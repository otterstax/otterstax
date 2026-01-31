// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "postgres_connection.hpp"
#include "utility/logger.hpp"

using namespace components;
using namespace components::sql;

namespace frontend::postgres {
    postgres_connection::postgres_connection(std::pmr::memory_resource* resource,
                                             boost::asio::io_context& ctx,
                                             uint32_t connection_id,
                                             actor_zeta::address_t scheduler,
                                             std::function<void()> on_close)
        : frontend_connection(ctx, connection_id, std::move(on_close))
        , resource_(resource)
        , statement_name_map_(resource_)
        , portals_(resource_)
        , scheduler_(scheduler)
        , transaction_man_()
        , pipeline_()
        , use_protocol_3_2_(false)
        , log_(get_logger(logger_tag::POSTGRES_CONNECTION)) {
        assert(log_.is_valid());
        assert(resource_ != nullptr && "memory resource must not be null");
        assert(static_cast<bool>(scheduler_) && "scheduler address must not be null");
    }

    postgres_connection::prepared_stmt_meta::prepared_stmt_meta(std::pmr::memory_resource* resource,
                                                                session_hash_t stmt_session,
                                                                uint32_t parameter_count,
                                                                components::types::complex_logical_type schema,
                                                                std::pmr::vector<field_type> specified_types)
        : stmt_session(stmt_session)
        , parameter_count(parameter_count)
        , schema(std::move(schema))
        , specified_types(std::move(specified_types))
        , portal_names(resource)
        , format()
        , is_schema_known_(false) {}

    std::vector<uint8_t> postgres_connection::build_too_many_connections_error() {
        packet_writer writer;
        return build_error_response(writer, sql_state::TOO_MANY_CONNECTIONS, "Too many connections");
    }

    void postgres_connection::start_impl() { read_initial_message(); }

    log_t& postgres_connection::get_logger_impl() { return log_; }

    uint32_t postgres_connection::get_header_size() const { return PACKET_HEADER_SIZE; }

    uint32_t postgres_connection::get_packet_size(const std::vector<uint8_t>& header) const {
        return merge_data_bytes<uint32_t, endian::BIG>(header, 1); // signed int by protocol, length cannot be negative
    }

    bool postgres_connection::validate_payload_size(uint32_t& size) {
        if (size < 4) {
            handle_network_read_error("Size of packet did not include it's length");
            return false;
        }
        size -= 4;

        return true;
    }

    void postgres_connection::handle_network_read_error(std::string description) {
        send_error_response(sql_state::IO_ERROR, std::move(description), error_severity::fatal());
    }

    void postgres_connection::handle_out_of_resources_error(std::string description) {
        send_error_response(sql_state::INSUFFICIENT_RESOURCES, std::move(description), error_severity::fatal());
    }

    void postgres_connection::read_initial_message() {
        // read packet size first
        auto timer = std::make_shared<boost::asio::steady_timer>(socket_.get_executor(),
                                                                 std::chrono::seconds(CONNECTION_TIMEOUT_SEC));
        timer->async_wait([this](boost::system::error_code ec) {
            if (!ec) {
                log_->warn("[Connection {}] READ: timeout, disconnecting", connection_id_);
                finish();
            }
        });

        // size is 4, without char noting message type
        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_buffer_.data(), 4),
            safe_callback([this, timer](boost::system::error_code ec, std::size_t length) {
                timer->cancel();
                if (ec || length != 4) {
                    send_error_response(sql_state::IO_ERROR, "Failed to read initial message", error_severity::fatal());
                    return;
                }

                auto msg_length = merge_data_bytes<uint32_t, endian::BIG>(read_buffer_, 0);
                std::cout << "Initial message length: " + std::to_string(msg_length) << std::endl;

                if (msg_length < 4 || msg_length > MAX_PACKET_SIZE) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION,
                                        "Invalid message length: " + std::to_string(length),
                                        error_severity::fatal());
                    return;
                }
                msg_length -= 4; // length includes itself

                boost::asio::async_read(
                    socket_,
                    boost::asio::buffer(read_buffer_.data(), msg_length),
                    safe_callback([this, msg_length](boost::system::error_code ec, std::size_t length) {
                        if (ec || length != msg_length) {
                            send_error_response(sql_state::IO_ERROR,
                                                "Failed to read initial message",
                                                error_severity::fatal());
                            return;
                        }

                        std::vector<uint8_t> payload(std::make_move_iterator(read_buffer_.begin()),
                                                     std::make_move_iterator(read_buffer_.begin() + length));
                        packet_reader reader(std::move(payload));

                        auto code = reader.read_int32();
                        if (code == message_code::SSL_REQUEST_CODE) {
                            handle_ssl_decline(reader);
                        } else if (code == message_code::PROTOCOL_VERSION_3_0) {
                            handle_startup_message(reader);
                        } else {
                            send_error_response(sql_state::PROTOCOL_VIOLATION,
                                                "Unsupported protocol version: " + std::to_string(code),
                                                error_severity::fatal());
                        }
                    }));
            }));
    }

    void postgres_connection::handle_packet(std::vector<uint8_t> header, std::vector<uint8_t> payload) {
        assert(header.size() == 5);
        auto message_type = static_cast<char>(header[0]);
        switch (message_type) {
            case message_type::frontend::QUERY: {
                if (payload.size() < 2) {
                    // empty query detected
                    // should be ok in C++20
                    send_packet_merged({build_empty_query_response(writer_),
                                        build_ready_for_query(writer_, transaction_man_.get_transaction_status())});
                    break;
                }

                std::string query(std::make_move_iterator(payload.begin()), std::make_move_iterator(payload.end() - 1));
                log_->info("[Connection {}] QUERY message: '{}'", connection_id_, query);
                handle_query(std::move(query));
                break;
            }
            case message_type::frontend::PARSE: {
                pipeline_.begin_pipeline();
                packet_reader reader(
                    {std::make_move_iterator(payload.begin()), std::make_move_iterator(payload.end())});

                if (!reader.remaining()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message statement name");
                    return;
                }

                auto stmt = reader.read_string_null();
                if (!reader.remaining()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message query");
                    return;
                }

                auto query = reader.read_string_null();
                if (reader.remaining() < 2) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message parameters");
                    return;
                }

                auto num_params = reader.read_int16();
                if (reader.remaining() < 4 * num_params) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message parameter types");
                    return;
                }

                handle_parse(std::move(stmt), std::move(query), num_params, std::move(reader));
                break;
            }
            case message_type::frontend::BIND: {
                pipeline_.begin_pipeline();
                packet_reader reader(
                    {std::make_move_iterator(payload.begin()), std::make_move_iterator(payload.end())});

                if (!reader.remaining()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND portal name");
                    return;
                }

                auto portal_name = reader.read_string_null();
                if (!reader.remaining()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION,
                                        "Truncated BIND message prepared statement name");
                    return;
                }

                auto stmt = reader.read_string_null();
                if (reader.remaining() < 2) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message parameter format codes");
                    return;
                }

                auto format_len = reader.read_int16();
                if (reader.remaining() < 2 * format_len) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message parameter format codes");
                    return;
                }

                std::vector<result_encoding> format;
                format.reserve(format_len);
                for (int16_t i = 0; i < format_len; ++i) {
                    format.emplace_back(static_cast<result_encoding>(reader.read_int16()));
                }

                if (reader.remaining() < 2) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message parameter values");
                    return;
                }

                auto num_params = reader.read_int16();
                handle_bind(std::move(stmt), std::move(portal_name), std::move(format), num_params, std::move(reader));
                break;
            }
            case message_type::frontend::EXECUTE: {
                pipeline_.begin_pipeline();
                packet_reader reader(
                    {std::make_move_iterator(payload.begin()), std::make_move_iterator(payload.end())});

                if (!reader.remaining()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated EXECUTE message statement name");
                    return;
                }

                auto portal = reader.read_string_null();
                if (reader.remaining() < 4) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated EXECUTE message row limit");
                    return;
                }

                auto limit = reader.read_int32();
                handle_execute(std::move(portal), limit);
                break;
            }
            case message_type::frontend::CLOSE: // fall-through
            case message_type::frontend::DESCRIBE: {
                packet_reader reader(
                    {std::make_move_iterator(payload.begin()), std::make_move_iterator(payload.end())});

                bool is_close = message_type == message_type::frontend::CLOSE;
                std::string str = is_close ? "CLOSE" : "DESCRIBE";
                if (!reader.remaining()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated " + str + " message type");
                    return;
                }

                char type = reader.read_uint8();
                if (type != static_cast<char>(describe_close_arg::PORTAL) &&
                    type != static_cast<char>(describe_close_arg::STATEMENT)) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION,
                                        std::string("Unknown ") + str +
                                            " statement/portal type: " + std::to_string(type));
                    return;
                }

                if (!reader.remaining()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION,
                                        std::string("Truncated ") + str + "statement/portal type");
                    return;
                }

                auto arg = static_cast<describe_close_arg>(type);
                auto name = reader.read_string_null();
                is_close ? handle_close(arg, std::move(name)) : handle_describe(arg, std::move(name));
                break;
            }
            case message_type::frontend::SYNC: {
                bool err = pipeline_.has_error();
                pipeline_.end_pipeline();
                send_packet(build_ready_for_query(writer_,
                                                  err ? transaction_status::TRANSACTION_ERROR
                                                      : transaction_man_.get_transaction_status()));
                break;
            }
            case message_type::frontend::FLUSH:
                read_packet(); // no-op
                break;
            case message_type::frontend::TERMINATE:
                finish();
                break;
            default:
                send_error_response(sql_state::PROTOCOL_VIOLATION,
                                    std::string("Unknown message type: '") + message_type + "'");
                break;
        }
    }

    void postgres_connection::send_error_response(const char* sqlstate, std::string message, error_severity severity) {
        log_->warn("[Connection {}] ERROR: sqlstate={} msg='{}'", connection_id_, sqlstate, message);
        pipeline_.set_error();
        if (severity.tag == error_severity::fatal().tag) {
            send_packet(build_error_response(writer_, sqlstate, std::move(message), severity), false);
            finish(); // connection in postgres is closed after FATAL ErrorResponse
        } else {
            send_packet_merged({build_error_response(writer_, sqlstate, std::move(message), severity),
                                build_ready_for_query(writer_, transaction_man_.get_transaction_status())});
        }
    }
} // namespace frontend::postgres
