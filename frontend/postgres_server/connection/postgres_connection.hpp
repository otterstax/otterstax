// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/frontend_connection.hpp"
#include "../packet/packet_reader.hpp"
#include "../packet/packet_utils.hpp"
#include "../packet/packet_writer.hpp"
#include "../postgres_defs/error.hpp"
#include "../postgres_defs/field_type.hpp"
#include "../postgres_defs/message_type.hpp"
#include "../resultset/postgres_resultset.hpp"
#include "pipeline_state.hpp"
#include "transaction_manager.hpp"

#include "routes/scheduler.hpp"
#include "utility/session.hpp"
#include "utility/shared_flight_data.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <iostream>
#include <random>
#include <regex>

namespace frontend::postgres {
    class postgres_connection : public frontend_connection {
    public:
        using portal_t = std::pmr::vector<components::types::logical_value_t>;

        postgres_connection(std::pmr::memory_resource* resource,
                            boost::asio::io_context& ctx,
                            uint32_t connection_id,
                            actor_zeta::address_t scheduler,
                            std::function<void()> on_close);

        struct prepared_stmt_meta {
            prepared_stmt_meta(std::pmr::memory_resource* resource,
                               session_hash_t stmt_session,
                               uint32_t parameter_count,
                               components::types::complex_logical_type schema,
                               std::pmr::vector<field_type> specified_types);

            session_hash_t stmt_session;
            uint32_t parameter_count;
            components::types::complex_logical_type schema;
            std::pmr::vector<field_type> specified_types;
            std::pmr::vector<std::string> portal_names;
            std::vector<result_encoding> format;
            bool is_schema_known_;
        };

        struct portal_meta {
            portal_t portal;
            std::reference_wrapper<prepared_stmt_meta> statement;
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

        void read_initial_message();
        void handle_startup_message(packet_reader& reader);
        void handle_ssl_decline(packet_reader& reader);
        void handle_query(std::string query);
        void try_handle_transaction(std::string query, std::string error);

        void handle_parse(std::string stmt, std::string query, int16_t num_params, packet_reader&& reader);
        void handle_bind(std::string stmt,
                         std::string portal_name,
                         std::vector<result_encoding> format,
                         int16_t num_params,
                         packet_reader&& reader);
        void handle_execute(std::string portal_name, int32_t limit);
        void handle_close(describe_close_arg type, std::string name);
        void handle_describe(describe_close_arg type, std::string name);

        void do_close(describe_close_arg type, std::string name);
        void do_describe(components::types::complex_logical_type schema);
        void send_error_response(const char* sqlstate,
                                 std::string message,
                                 error_severity severity = error_severity::error());

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<std::string, prepared_stmt_meta> statement_name_map_;
        std::pmr::unordered_map<std::string, portal_meta> portals_;
        packet_writer writer_;
        actor_zeta::address_t scheduler_;
        std::vector<uint8_t> backend_secret_key_;
        transaction_manager transaction_man_;
        pipeline_state pipeline_;
        bool use_protocol_3_2_;
        log_t log_;
    };
} // namespace frontend::postgres
