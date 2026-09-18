// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_connection.hpp"
#include "utility/logger.hpp"

using namespace components;
using namespace components::sql;

namespace frontend::mysql {
    mysql_connection::mysql_connection(std::pmr::memory_resource* resource,
                                       boost::asio::io_context& ctx,
                                       uint32_t connection_id,
                                       actor_zeta::address_t scheduler,
                                       connection_close_sink& close_sink,
                                       size_t slot,
                                       std::chrono::milliseconds read_timeout)
        : frontend_connection(ctx, connection_id, close_sink, slot, read_timeout)
        , resource_(resource)
        , statement_id_map_(resource_)
        , sequence_id_(0)
        , next_statement_id_(0)
        , client_max_packet_size_(DEFAULT_MAX_PACKET_SIZE)
        , scheduler_(scheduler)
        , state_(connection_state::HANDSHAKE)
        , log_(get_logger(logger_tag::MYSQL_CONNECTION)) {
        assert(log_.is_valid());
        assert(resource_ != nullptr && "memory resource must not be null");
        assert(static_cast<bool>(scheduler_) && "scheduler address must not be null");
    }

    mysql_connection::prepared_stmt_meta::prepared_stmt_meta(std::pmr::memory_resource* resource,
                                                             session_hash_t stmt_session,
                                                             std::string sql,
                                                             uint32_t parameter_count)
        : stmt_session(stmt_session)
        , sql(sql.c_str(), sql.size(), resource)
        , consumed(false)
        , parameter_count(parameter_count)
        , param_types(resource) {}

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
        // The packet is written from send_buffer_, which outlives the write.
        send_buffer_ = build_handshake_10(writer_, connection_id_, generate_backend_key(AUTH_DATA_FULL_LENGTH));
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

    void mysql_connection::handle_packet(std::vector<uint8_t> header, std::vector<uint8_t> payload) {
        assert(header.size() == 4);
        uint8_t seq_id = header[3];

        bool sequence_valid = false;
        switch (state_) {
            case connection_state::AUTH:
                sequence_valid = (seq_id == 1); // server sent auth packet
                sequence_id_ = 2;
                break;
            case connection_state::COMMAND:
                sequence_valid = (seq_id == 0); // start with 0
                sequence_id_ = 1;
                break;
            case connection_state::HANDSHAKE:
                // The server writes the handshake and moves to AUTH before it
                // reads anything, so no packet can be handled in this state.
                send_error(mysql_error::ER_SEQUENCE_ERROR, "Packet received before the server handshake");
                return;
        }

        if (!sequence_valid) {
            std::string state_str = state_ == connection_state::AUTH ? "AUTH" : "COMMAND";
            send_error(mysql_error::ER_SEQUENCE_ERROR,
                       "Packet sequence error in " + std::move(state_str) + " got: " + std::to_string(seq_id));
            return;
        }

        if (payload.size() > client_max_packet_size_) {
            send_error(mysql_error::ER_PACKET_TOO_LARGE, "Packet exceeds max_allowed_packet");
            return;
        }

        switch (state_) {
            case connection_state::AUTH:
                handle_auth(std::move(payload));
                break;
            case connection_state::COMMAND:
                handle_command(std::move(payload));
                break;
        }
    }

    void mysql_connection::send_error(mysql_error error_code, std::string message) {
        log_->warn("[Connection {}] ERROR: code={} msg='{}' seq={}",
                   connection_id_,
                   static_cast<uint16_t>(error_code),
                   message,
                   static_cast<int>(sequence_id_));
        send_packet(build_error(writer_, sequence_id_++, error_code, std::move(message)));
    }

    void mysql_connection::send_malformed_packet_error(std::string message) {
        log_->warn("[Connection {}] ERROR: malformed packet, disconnecting: msg='{}' seq={}",
                   connection_id_,
                   message,
                   static_cast<int>(sequence_id_));
        // The write is initiated before the close is posted, so the ERR packet
        // reaches the client ahead of the FIN (the same order the PG frontend
        // relies on for a FATAL ErrorResponse).
        send_packet(build_error(writer_, sequence_id_++, mysql_error::ER_MALFORMED_PACKET, std::move(message)),
                    false);
        finish();
    }
} // namespace frontend::mysql
