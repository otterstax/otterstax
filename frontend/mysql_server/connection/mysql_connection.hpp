// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/frontend_connection.hpp"
#include "../mysql_defs/capabilities.hpp"
#include "../mysql_defs/character_set.hpp"
#include "../mysql_defs/error.hpp"
#include "../mysql_defs/server_command.hpp"
#include "../mysql_defs/server_status.hpp"
#include "../packet/packet_reader.hpp"
#include "../packet/packet_utils.hpp"
#include "../packet/packet_writer.hpp"
#include "../resultset/mysql_resultset.hpp"

#include "routes/scheduler.hpp"
#include "utility/session.hpp"
#include "utility/shared_flight_data.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <iostream>
#include <random>
#include <regex>

namespace frontend::mysql {
    class mysql_connection : public frontend_connection {
    public:
        mysql_connection(std::pmr::memory_resource* resource,
                         boost::asio::io_context& ctx,
                         uint32_t connection_id,
                         actor_zeta::address_t scheduler,
                         std::function<void()> on_close);

        struct prepared_stmt_meta {
            prepared_stmt_meta(std::pmr::memory_resource* resource,
                               session_hash_t stmt_session,
                               uint32_t parameter_count);

            session_hash_t stmt_session;
            uint32_t parameter_count;
            std::pmr::vector<uint16_t> param_types;
        };

        static std::vector<uint8_t> build_too_many_connections_error();

    protected:
        void start_impl() override;

        log_t& get_logger_impl() override;
        uint32_t get_header_size() const override;
        uint32_t get_packet_size(const std::vector<uint8_t>& header) const override;
        bool validate_payload_size(uint32_t& size) override;

        void handle_packet(std::vector<uint8_t> header, std::vector<uint8_t> payload) override;
        void handle_network_read_error(std::string description) override;
        void handle_out_of_resources_error(std::string description) override;

    private:
        enum class connection_state
        {
            HANDSHAKE,
            AUTH,
            COMMAND
        };

        void send_handshake();

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
                              packet_reader&& reader);
        void handle_execute_stmt(session_hash_t id, std::pmr::vector<components::types::logical_value_t> param_values);

        void send_resultset(mysql_resultset&& result);
        void send_error(mysql_error error_code, std::string message);

        void reset_packet_sequence();

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<uint32_t, prepared_stmt_meta> statement_id_map_;
        packet_writer writer_;
        uint8_t sequence_id_;
        uint8_t expected_sequence_id_;
        uint32_t next_statement_id_;
        uint32_t client_max_packet_size_;
        actor_zeta::address_t scheduler_;
        connection_state state_;
        log_t log_;
    };
} // namespace frontend::mysql
