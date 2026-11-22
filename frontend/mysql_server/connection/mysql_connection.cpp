// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_connection.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

using namespace components;

namespace frontend::mysql {
    mysql_connection::mysql_connection(std::pmr::memory_resource* resource,
                                       boost::asio::ip::tcp::socket&& socket,
                                       uint32_t connection_id,
                                       otterstax::session::session_factory_t& sessions,
                                       connection_close_sink& close_sink,
                                       size_t slot,
                                       connection_timeouts_t timeouts)
        : frontend_connection(std::move(socket), connection_id, close_sink, slot, timeouts)
        , resource_(resource)
        , statement_id_map_(resource_)
        , sequence_id_(0)
        , next_statement_id_(0)
        , session_(sessions.open(otterstax::session::database_term_t::schema))
        , state_(connection_state::HANDSHAKE)
        , log_(get_logger(logger_tag::MYSQL_CONNECTION)) {
        assert(log_.is_valid());
        assert(resource_ != nullptr && "memory resource must not be null");
        assert(session_ != nullptr && "every connection has a query session");
    }

    mysql_connection::prepared_stmt_meta::prepared_stmt_meta(std::pmr::memory_resource* resource,
                                                             otterstax::session::statement_id_t stmt_session,
                                                             uint32_t parameter_count)
        : stmt_session(stmt_session)
        , parameter_count(parameter_count)
        , param_wire_types(resource)
        , engine_types(resource) {}

    std::vector<uint8_t> mysql_connection::build_too_many_connections_error() {
        packet_writer writer;
        return build_error(writer, 0, mysql_error::ER_CON_COUNT_ERROR, "Too many connections");
    }

    void mysql_connection::start_impl() { send_handshake(); }

    log_t& mysql_connection::get_logger_impl() { return log_; }

    uint32_t mysql_connection::get_header_size() const { return PACKET_HEADER_SIZE; }

    uint32_t mysql_connection::get_packet_size(const std::vector<uint8_t>& header) const {
        return merge_n_bytes<uint32_t, 3, endian::LITTLE>(header, 0);
    }

    bool mysql_connection::validate_payload_size(const std::vector<uint8_t>& header, uint32_t& size) { return true; }

    void mysql_connection::handle_network_read_error(std::string description) {
        send_error(mysql_error::ER_NET_READ_ERROR, std::move(description));
    }

    void mysql_connection::handle_out_of_resources_error(std::string description) {
        send_error(mysql_error::ER_OUT_OF_RESOURCES, std::move(description));
    }

    void mysql_connection::send_handshake() {
        auto scramble = mysql_scramble();
        if (!scramble) {
            // An ERR packet in place of the handshake is how the server refuses a client.
            log_->error("[Connection {}] HANDSHAKE: no secure random bytes for the scramble", connection_id_);
            send_packet(build_error(writer_,
                                    sequence_id_++,
                                    mysql_error::ER_HANDSHAKE_ERROR,
                                    "the server could not generate the authentication scramble"),
                        false);
            finish();
            return;
        }
        // The packet is written from send_buffer_, which outlives the write.
        send_buffer_ = build_handshake_10(writer_, connection_id_, std::move(*scramble), status_flags());
        sequence_id_++;

        // todo: count rounds & break if too many
        boost::asio::async_write(
            socket_,
            boost::asio::buffer(send_buffer_),
            safe_callback([this](boost::system::error_code ec, std::size_t bytes_sent) {
                if (ec) {
                    if (!closed()) {
                        log_->error("[Connection {}] HANDSHAKE failed, disconnecting: {}",
                                    connection_id_,
                                    ec.message());
                    }
                    finish();
                } else {
                    log_->info("[Connection {}] HANDSHAKE: sent {} bytes", connection_id_, bytes_sent);
                    state_ = connection_state::AUTH;
                    read_packet();
                }
            }));
    }

    boost::asio::awaitable<void> mysql_connection::handle_packet(std::vector<uint8_t> header,
                                                                 std::vector<uint8_t> payload) {
        assert(header.size() == 4);
        uint8_t seq_id = header[3];

        bool sequence_valid = false;
        switch (state_) {
            case connection_state::AUTH:
                sequence_valid = (seq_id == 1); // server sent auth packet
                sequence_id_ = 2;
                break;
            case connection_state::COMMAND:
                if (command_.empty()) {
                    sequence_valid = (seq_id == 0);
                    sequence_id_ = 1;
                } else {
                    sequence_valid = (seq_id == command_seq_);
                }
                break;
            case connection_state::HANDSHAKE:
                // The server writes the handshake and moves to AUTH before it
                // reads anything, so no packet can be handled in this state.
                send_error(mysql_error::ER_SEQUENCE_ERROR, "Packet received before the server handshake");
                co_return;
        }

        if (!sequence_valid) {
            if (!command_.empty()) {
                // Mid-command the two sides are in different commands, and a
                // stream cannot resync inside one: the connection ends.
                sequence_id_ = seq_id + 1;
                send_fatal_error(mysql_error::ER_SEQUENCE_ERROR,
                                 "Packet sequence error in COMMAND got: " + std::to_string(seq_id));
                co_return;
            }
            std::string state_str = state_ == connection_state::AUTH ? "AUTH" : "COMMAND";
            send_error(mysql_error::ER_SEQUENCE_ERROR,
                       "Packet sequence error in " + std::move(state_str) + " got: " + std::to_string(seq_id));
            co_return;
        }

        switch (state_) {
            case connection_state::AUTH:
                co_await handle_auth(std::move(payload));
                break;
            case connection_state::COMMAND:
                co_await handle_command_packet(seq_id, std::move(payload));
                break;
            case connection_state::HANDSHAKE:
                break; // refused by the first switch above
        }
    }

    boost::asio::awaitable<void> mysql_connection::handle_command_packet(uint8_t seq_id, std::vector<uint8_t> payload) {
        { OTX_ZONE_N("mysql::handle_command_packet"); }
        if (command_.empty()) {
            if (payload.size() < MAX_PACKET_SIZE) {
                // The whole command in one wire packet.
                sequence_id_ = seq_id + 1;
                co_await handle_command(std::move(payload));
                co_return;
            }
            if (payload.size() > MAX_ALLOWED_PACKET) {
                sequence_id_ = seq_id + 1;
                send_fatal_error(mysql_error::ER_PACKET_TOO_LARGE, "Command exceeds max_allowed_packet");
                co_return;
            }
            command_ = std::move(payload);
            command_seq_ = seq_id + 1;
            read_packet(); // the next piece, once this handler ends
            co_return;
        }

        if (command_.size() + payload.size() > MAX_ALLOWED_PACKET) {
            command_.clear();
            command_.shrink_to_fit();
            sequence_id_ = seq_id + 1;
            send_fatal_error(mysql_error::ER_PACKET_TOO_LARGE, "Command exceeds max_allowed_packet");
            co_return;
        }
        command_.insert(command_.end(), payload.begin(), payload.end());
        if (payload.size() == MAX_PACKET_SIZE) {
            // Still a full packet: the command continues.
            command_seq_ = seq_id + 1;
            read_packet();
            co_return;
        }
        // A piece shorter than a full packet ends the command, an empty one too
        sequence_id_ = seq_id + 1;
        std::vector<uint8_t> whole = std::move(command_);
        co_await handle_command(std::move(whole));
    }

    void mysql_connection::send_error(mysql_error error_code, std::string message) {
        log_->warn("[Connection {}] ERROR: code={} msg='{}' seq={}",
                   connection_id_,
                   static_cast<uint16_t>(error_code),
                   message,
                   static_cast<int>(sequence_id_));
        send_packet(build_error(writer_, sequence_id_++, error_code, std::move(message)));
    }

    void mysql_connection::send_fatal_error(mysql_error error_code, std::string message) {
        // The ERR write is initiated before the close is posted, so the packet
        // reaches the client ahead of the FIN (the same order the PG frontend
        // relies on for a FATAL ErrorResponse).
        send_error(error_code, std::move(message));
        finish();
    }

    void mysql_connection::send_malformed_packet_error(std::string message) {
        send_fatal_error(mysql_error::ER_MALFORMED_PACKET, std::move(message));
    }
} // namespace frontend::mysql
