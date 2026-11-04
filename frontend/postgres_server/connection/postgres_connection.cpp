// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "postgres_connection.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

using namespace components;
using namespace components::sql;

namespace frontend::postgres {
    namespace {
        // The message types the frontend accepts after startup. PostgreSQL
        // also defines FunctionCall ('F'), which the frontend does not
        // implement; it is refused like any other type.
        bool is_frontend_message_type(char type) {
            switch (type) {
                case message_type::frontend::QUERY:
                case message_type::frontend::PARSE:
                case message_type::frontend::BIND:
                case message_type::frontend::EXECUTE:
                case message_type::frontend::DESCRIBE:
                case message_type::frontend::CLOSE:
                case message_type::frontend::SYNC:
                case message_type::frontend::FLUSH:
                case message_type::frontend::TERMINATE:
                case message_type::frontend::COPY_DATA:
                case message_type::frontend::COPY_DONE:
                case message_type::frontend::COPY_FAIL:
                    return true;
                default:
                    return false;
            }
        }
    } // namespace

    postgres_connection::postgres_connection(std::pmr::memory_resource* resource,
                                             boost::asio::ip::tcp::socket&& socket,
                                             uint32_t connection_id,
                                             otterstax::session::session_factory_t& sessions,
                                             connection_close_sink& close_sink,
                                             size_t slot,
                                             std::chrono::milliseconds read_timeout)
        : frontend_connection(std::move(socket), connection_id, close_sink, slot, read_timeout)
        , resource_(resource)
        , statement_name_map_(resource_)
        , portals_(resource_)
        , session_(sessions.open())
        , pipeline_()
        , use_protocol_3_2_(false)
        , log_(get_logger(logger_tag::POSTGRES_CONNECTION)) {
        assert(log_.is_valid());
        assert(resource_ != nullptr && "memory resource must not be null");
        assert(session_ != nullptr && "every connection has a query session");
    }

    postgres_connection::statement_run_t::statement_run_t(std::pmr::memory_resource* resource,
                                                          otterstax::session::statement_id_t stmt_session,
                                                          std::string_view sql,
                                                          components::types::complex_logical_type schema,
                                                          NodeTag tag)
        : stmt_session(stmt_session)
        , sql(sql, resource)
        , schema(std::move(schema))
        , tag(tag) {}

    postgres_connection::statement_run_t::statement_run_t(const statement_run_t& other,
                                                          std::pmr::memory_resource* resource)
        : stmt_session(other.stmt_session)
        , sql(other.sql, resource)
        , schema(other.schema)
        , tag(other.tag)
        , deallocate(other.deallocate)
        , protocol_setting(other.protocol_setting)
        , show(other.show)
        , resets_client_parameters(other.resets_client_parameters) {}

    postgres_connection::prepared_stmt_meta::prepared_stmt_meta(statement_run_t run,
                                                                uint32_t parameter_count,
                                                                std::pmr::vector<field_type> specified_types)
        : run(std::move(run))
        , parameter_count(parameter_count)
        , specified_types(std::move(specified_types)) {}

    postgres_connection::portal_meta::portal_meta(portal_t params,
                                                  std::string statement_name,
                                                  statement_run_t run,
                                                  bool statement_described,
                                                  std::vector<result_encoding> format)
        : params(std::move(params))
        , statement_name(std::move(statement_name))
        , run(std::move(run))
        , statement_described(statement_described)
        , format(std::move(format)) {}

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

    bool postgres_connection::validate_payload_size(const std::vector<uint8_t>& header, uint32_t& size) {
        // As in PostgreSQL, a type byte the protocol does not define means the
        // message boundaries are lost: FATAL and close, without waiting for
        // the body the length announces.
        if (!is_frontend_message_type(static_cast<char>(header[0]))) {
            send_error_response(sql_state::PROTOCOL_VIOLATION,
                                "invalid frontend message type " + std::to_string(header[0]),
                                error_severity::fatal());
            return false;
        }

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
        arm_read_timeout("initial message");

        // size is 4, without char noting message type
        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_buffer_.data(), 4),
            safe_callback([this](boost::system::error_code ec, std::size_t length) {
                cancel_read_timeout();
                if (closed()) {
                    return;
                }
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

                // The startup body is bounded by MAX_BUFFER_SIZE, not by the
                // initial read buffer; it is never read past the buffer's end.
                if (!ensure_read_buffer(msg_length)) {
                    return;
                }

                arm_read_timeout("initial message body");
                boost::asio::async_read(
                    socket_,
                    boost::asio::buffer(read_buffer_.data(), msg_length),
                    safe_callback([this, msg_length](boost::system::error_code ec, std::size_t length) {
                        cancel_read_timeout();
                        if (closed()) {
                            return;
                        }
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
                        if (!reader.ok()) {
                            send_error_response(sql_state::PROTOCOL_VIOLATION,
                                                "Truncated startup message: no protocol version",
                                                error_severity::fatal());
                        } else if (code == message_code::SSL_REQUEST_CODE) {
                            handle_ssl_decline(reader);
                        } else if (code == message_code::PROTOCOL_VERSION_3_0) {
                            run_handler(handle_startup_message(code, std::move(reader)));
                        } else {
                            send_error_response(sql_state::PROTOCOL_VIOLATION,
                                                "Unsupported protocol version: " + std::to_string(code),
                                                error_severity::fatal());
                        }
                    }));
            }));
    }

    boost::asio::awaitable<void> postgres_connection::handle_packet(std::vector<uint8_t> header,
                                                                    std::vector<uint8_t> payload) {
        assert(header.size() == 5);
        auto message_type = static_cast<char>(header[0]);
        {
            // A zone never spans a co_await (the coroutine resumes on any
            // thread of the pool): the handlers below carry their own.
            OTX_ZONE_N("pg::handle_packet");
            // A failed pipeline discards every message up to its Sync, which sends
            // the single ReadyForQuery; Terminate still ends the connection.
            if (pipeline_.has_error() && message_type != message_type::frontend::SYNC &&
                message_type != message_type::frontend::TERMINATE) {
                log_->debug("[Connection {}] '{}' message discarded: the pipeline failed, waiting for Sync",
                            connection_id_,
                            message_type);
                read_packet();
                co_return;
            }
        }
        // `payload` is the whole body the length prefix declared
        // (read_packet_payload reads exactly that many bytes), so a body
        // shorter than its fields is a protocol error of this message alone:
        // the next message starts at the next header. Like PostgreSQL, it is
        // answered with ErrorResponse 08P01 of severity ERROR — inside a
        // pipeline the ReadyForQuery waits for the Sync, a simple Query gets it
        // at once — and the connection stays usable. A body longer than its
        // fields (bytes after the last one) is the same ERROR 08P01, "invalid
        // message format", checked before the message is acted on. Only a
        // malformed StartupMessage (no session yet) and a message type the
        // protocol does not define (validate_payload_size) are FATAL.
        switch (message_type) {
            case message_type::frontend::QUERY: {
                std::string query;
                {
                    OTX_ZONE_N("pg::handle_packet::query");
                    // The simple protocol answers ReadyForQuery itself, after the
                    // result or the error.
                    pipeline_.end_pipeline();
                    packet_reader reader(
                        {std::make_move_iterator(payload.begin()), std::make_move_iterator(payload.end())});
                    query = reader.read_string_null();
                    // The query text without its terminator.
                    if (!reader.ok()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION,
                                            "Truncated QUERY message: unterminated query");
                        co_return;
                    }
                    // Nothing may follow the terminator.
                    if (reader.remaining() != 0) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
                        co_return;
                    }

                    if (query.empty()) {
                        std::vector<std::vector<uint8_t>> response;
                        response.emplace_back(build_empty_query_response(writer_));
                        append_ready_for_query(response);
                        send_packet_merged(std::move(response));
                        co_return;
                    }

                    log_->info("[Connection {}] QUERY message: '{}'", connection_id_, query);
                }
                co_await handle_query(std::move(query));
                break;
            }
            case message_type::frontend::PARSE: {
                std::string stmt;
                std::string query;
                int16_t num_params = 0;
                std::optional<packet_reader> reader;
                {
                    OTX_ZONE_N("pg::handle_packet::parse");
                    pipeline_.begin_pipeline();
                    reader.emplace(std::vector<uint8_t>{std::make_move_iterator(payload.begin()),
                                                        std::make_move_iterator(payload.end())});

                    if (!reader->remaining()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message statement name");
                        co_return;
                    }

                    stmt = reader->read_string_null();
                    if (!reader->remaining()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message query");
                        co_return;
                    }

                    query = reader->read_string_null();
                    if (reader->remaining() < 2) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message parameters");
                        co_return;
                    }

                    num_params = reader->read_int16();
                    if (reader->remaining() < 4 * num_params) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message parameter types");
                        co_return;
                    }

                    // A string above without its terminator.
                    if (!reader->ok()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message");
                        co_return;
                    }
                }
                co_await handle_parse(std::move(stmt), std::move(query), num_params, std::move(*reader));
                break;
            }
            case message_type::frontend::BIND: {
                std::string portal_name;
                std::string stmt;
                std::vector<result_encoding> format;
                int16_t num_params = 0;
                std::optional<packet_reader> reader;
                {
                    OTX_ZONE_N("pg::handle_packet::bind");
                    pipeline_.begin_pipeline();
                    reader.emplace(std::vector<uint8_t>{std::make_move_iterator(payload.begin()),
                                                        std::make_move_iterator(payload.end())});

                    if (!reader->remaining()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND portal name");
                        co_return;
                    }

                    portal_name = reader->read_string_null();
                    if (!reader->remaining()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION,
                                            "Truncated BIND message prepared statement name");
                        co_return;
                    }

                    stmt = reader->read_string_null();
                    if (reader->remaining() < 2) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION,
                                            "Truncated BIND message parameter format codes");
                        co_return;
                    }

                    auto format_len = reader->read_int16();
                    if (reader->remaining() < 2 * format_len) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION,
                                            "Truncated BIND message parameter format codes");
                        co_return;
                    }

                    format.reserve(format_len);
                    for (int16_t i = 0; i < format_len; ++i) {
                        format.emplace_back(static_cast<result_encoding>(reader->read_int16()));
                    }

                    if (reader->remaining() < 2) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message parameter values");
                        co_return;
                    }

                    num_params = reader->read_int16();
                    // A string above without its terminator.
                    if (!reader->ok()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message");
                        co_return;
                    }
                }
                co_await handle_bind(std::move(stmt),
                                     std::move(portal_name),
                                     std::move(format),
                                     num_params,
                                     std::move(*reader));
                break;
            }
            case message_type::frontend::EXECUTE: {
                std::string portal;
                int32_t limit = 0;
                {
                    OTX_ZONE_N("pg::handle_packet::execute");
                    pipeline_.begin_pipeline();
                    packet_reader reader(
                        {std::make_move_iterator(payload.begin()), std::make_move_iterator(payload.end())});

                    if (!reader.remaining()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated EXECUTE message statement name");
                        co_return;
                    }

                    portal = reader.read_string_null();
                    if (reader.remaining() < 4) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated EXECUTE message row limit");
                        co_return;
                    }

                    limit = reader.read_int32();
                    // The portal name without its terminator.
                    if (!reader.ok()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated EXECUTE message");
                        co_return;
                    }
                    // Nothing may follow the row limit.
                    if (reader.remaining() != 0) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
                        co_return;
                    }
                }
                co_await handle_execute(std::move(portal), limit);
                break;
            }
            case message_type::frontend::CLOSE: // fall-through
            case message_type::frontend::DESCRIBE: {
                const bool is_close = message_type == message_type::frontend::CLOSE;
                auto arg = describe_close_arg::STATEMENT;
                std::string name;
                {
                    OTX_ZONE_N("pg::handle_packet::close_describe");
                    pipeline_.begin_pipeline();
                    packet_reader reader(
                        {std::make_move_iterator(payload.begin()), std::make_move_iterator(payload.end())});

                    std::string str = is_close ? "CLOSE" : "DESCRIBE";
                    if (!reader.remaining()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated " + str + " message type");
                        co_return;
                    }

                    char type = reader.read_uint8();
                    if (type != static_cast<char>(describe_close_arg::PORTAL) &&
                        type != static_cast<char>(describe_close_arg::STATEMENT)) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION,
                                            std::string("Unknown ") + str +
                                                " statement/portal type: " + std::to_string(type));
                        co_return;
                    }

                    if (!reader.remaining()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated " + str + " message name");
                        co_return;
                    }

                    arg = static_cast<describe_close_arg>(type);
                    name = reader.read_string_null();
                    // The name without its terminator.
                    if (!reader.ok()) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated " + str + " message");
                        co_return;
                    }
                    // Nothing may follow the name.
                    if (reader.remaining() != 0) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
                        co_return;
                    }
                }
                if (is_close) {
                    co_await handle_close(arg, std::move(name));
                } else {
                    co_await handle_describe(arg, std::move(name));
                }
                break;
            }
            case message_type::frontend::SYNC: {
                // The Sync ends the pipeline whatever happened inside it. Its
                // ReadyForQuery reports the transaction block — an error inside
                // a block has already failed it — not the pipeline.
                pipeline_.end_pipeline();
                // Sync has no fields. Outside the pipeline now, the error gets
                // its ReadyForQuery at once.
                if (!payload.empty()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
                    co_return;
                }
                // Outside a block the Sync ends the implicit transaction, and
                // with it every portal (PostgreSQL drops them at the
                // transaction's end): a window one of them left open is closed
                // (it holds the statement's snapshot until it ends).
                if (session_->transaction_status() == components::session::transaction_status_t::idle) {
                    co_await release_portals(true);
                }
                std::vector<std::vector<uint8_t>> response;
                append_ready_for_query(response);
                send_packet_merged(std::move(response));
                break;
            }
            case message_type::frontend::FLUSH:
                // An extended-query message: an error here waits for the Sync.
                pipeline_.begin_pipeline();
                // Flush has no fields.
                if (!payload.empty()) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
                    co_return;
                }
                read_packet(); // every answer is already written: nothing to flush
                break;
            case message_type::frontend::TERMINATE:
                // As in PostgreSQL, the connection ends whatever the body holds.
                finish();
                break;
            case message_type::frontend::COPY_DATA: // fall-through
            case message_type::frontend::COPY_DONE: // fall-through
            case message_type::frontend::COPY_FAIL:
                // Accepted and ignored outside COPY, as PostgreSQL does: a
                // client may still be sending data for a COPY that failed.
                read_packet();
                break;
            default:
                // validate_payload_size refused every other type before its body.
                assert(false && "handle_packet: a message type validate_payload_size does not accept");
                finish();
                break;
        }
    }

    void postgres_connection::send_error_response(const char* sqlstate,
                                                  std::string message,
                                                  error_severity severity,
                                                  std::string hint) {
        log_->warn("[Connection {}] ERROR: sqlstate={} msg='{}'", connection_id_, sqlstate, message);
        const bool ready_for_query_at_sync = pipeline_.set_error();
        if (severity.tag == error_severity::fatal().tag) {
            // FATAL closes the connection: a broken startup, a message type the
            // protocol does not define, a transport error, exhausted resources —
            // never a malformed message of a known type after startup.
            send_packet(build_error_response(writer_, sqlstate, std::move(message), severity, std::move(hint)), false);
            finish();
            return;
        }

        // An error inside an explicit transaction block fails the block, as in
        // PostgreSQL: every ReadyForQuery reports 'E' until ROLLBACK or COMMIT
        // ends it, and the engine refuses the block's statements. A statement's
        // own failure has failed it already (the session reported it to the
        // engine); one the frontend found itself — a malformed message, a value
        // it cannot bind, a statement it stopped waiting for — is reported to
        // the session here. Not awaited: it is queued behind whatever the
        // session runs and ahead of the client's next statement, and until an
        // answer shows it applied ReadyForQuery reports 'E' (failure_pending_).
        // Outside a block there is nothing to fail and the status stays 'I'.
        if (session_ != nullptr && !failure_pending_ &&
            session_->transaction_status() == components::session::transaction_status_t::in_transaction) {
            [[maybe_unused]] auto [needs_sched, fut] =
                actor_zeta::send(session_->address(),
                                 &otterstax::session::query_session_t::fail_transaction,
                                 core::error_code_t::protocol_violation,
                                 std::string{message});
            failure_pending_ = true;
        }
        if (ready_for_query_at_sync) {
            // Inside an extended-query pipeline the ErrorResponse goes alone:
            // the Sync that ends the pipeline answers the ReadyForQuery.
            send_packet(build_error_response(writer_, sqlstate, std::move(message), severity, std::move(hint)));
        } else {
            std::vector<std::vector<uint8_t>> response;
            response.emplace_back(build_error_response(writer_, sqlstate, std::move(message), severity, std::move(hint)));
            append_ready_for_query(response);
            send_packet_merged(std::move(response));
        }
    }
} // namespace frontend::postgres
