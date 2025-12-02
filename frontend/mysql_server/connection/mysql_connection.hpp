// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "../mysql_defs/capabilities.hpp"
#include "../mysql_defs/character_set.hpp"
#include "../mysql_defs/error.hpp"
#include "../mysql_defs/server_command.hpp"
#include "../mysql_defs/server_status.hpp"
#include "../packet/packet_reader.hpp"
#include "../packet/packet_utils.hpp"
#include "../packet/packet_writer.hpp"
#include "../protocol_const.hpp"
#include "../resultset/mysql_resultset.hpp"
#include "../utils.hpp"

#include "../../../routes/scheduler.hpp"
#include "../../../utility/session.hpp"
#include "../../../utility/shared_flight_data.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <iostream>
#include <random>
#include <regex>

namespace mysql_front {
    class mysql_connection {
    public:
        mysql_connection(std::pmr::memory_resource* resource,
                         boost::asio::io_context& ctx,
                         uint32_t connection_id,
                         actor_zeta::address_t scheduler,
                         std::function<void()> on_close);

        boost::asio::ip::tcp::socket& socket();
        void start();
        void finish();

        struct prepared_stmt_meta {
            prepared_stmt_meta(std::pmr::memory_resource* resource,
                               session_hash_t stmt_session,
                               uint32_t parameter_count);

            session_hash_t stmt_session;
            uint32_t parameter_count;
            std::pmr::vector<uint16_t> param_types;
        };

    private:
        static constexpr size_t READ_BUFFER_SIZE = 4096;
        static constexpr size_t TRY_RESEND_RESULTSET_ATTEMPTS = 3;

        enum class State
        {
            HANDSHAKE,
            AUTH,
            COMMAND
        };

        template<typename Callable>
        auto safe_callback(Callable&& callback) {
            return [this, callback = std::forward<Callable>(callback)](auto&&... args) {
                try {
                    callback(std::forward<decltype(args)>(args)...);
                } catch (const std::exception& e) {
                    std::cerr << "[Connection " << connection_id_ << "] EXCEPTION: " << e.what() << ", disconnecting..."
                              << std::endl;
                    finish();
                } catch (...) {
                    std::cerr << "[Connection " << connection_id_
                              << "] EXCEPTION: unknown callback exception, disconnecting..." << std::endl;
                    finish();
                }
            };
        }

        void send_handshake();
        void read_packet();
        void read_packet_payload(uint32_t payload_length, uint8_t seq_id);

        void handle_packet(std::vector<uint8_t> payload, uint8_t seq_id);
        void handle_auth(std::vector<uint8_t> payload);
        void handle_command(std::vector<uint8_t> payload);
        void handle_query(std::string query);
        void try_fix_variable_set_query(std::string_view query, std::string error);

        void handle_prepared_stmt(std::string query);
        void try_fix_prepared_stmt(std::string_view query, std::string error);

        std::pmr::vector<components::types::logical_value_t>
        handle_execute_params(uint32_t stmt_id,
                              size_t num_params,
                              std::pmr::vector<uint16_t>& param_types,
                              packet_reader& reader);
        void handle_execute_stmt(session_hash_t id, std::pmr::vector<components::types::logical_value_t> param_values);

        void send_resultset(mysql_resultset&& result);
        void send_packet_sequence(std::vector<std::vector<uint8_t>> packets, size_t index, size_t attempt = 0);

        void send_packet(std::vector<uint8_t> packet);
        void send_error(mysql_error error_code, std::string message);

        void reset_packet_sequence();

        std::pmr::memory_resource* resource_;
        boost::asio::ip::tcp::socket socket_;
        uint32_t connection_id_;
        std::function<void()> close_callback_;
        std::vector<uint8_t> read_buffer_;
        std::pmr::unordered_map<uint32_t, prepared_stmt_meta> statement_id_map_;
        packet_writer writer_;
        uint8_t sequence_id_;
        uint8_t expected_sequence_id_;
        uint32_t next_statement_id_;
        uint32_t client_max_packet_size_;
        actor_zeta::address_t scheduler_;
        State state_;
    };
} // namespace mysql_front