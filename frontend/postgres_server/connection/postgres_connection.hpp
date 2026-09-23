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

#include "frontend/common/asio_future_bridge.hpp"
#include "scheduler/session_data.hpp"
#include "scheduler/scheduler.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <iostream>
#include <optional>
#include <random>
#include <regex>

namespace frontend::postgres {
    class postgres_connection : public frontend_connection {
    public:
        using portal_t = std::pmr::vector<components::types::logical_value_t>;

        postgres_connection(std::pmr::memory_resource* resource,
                            boost::asio::ip::tcp::socket&& socket,
                            uint32_t connection_id,
                            actor_zeta::address_t scheduler,
                            connection_close_sink& close_sink,
                            size_t slot,
                            std::chrono::milliseconds read_timeout);

        struct prepared_stmt_meta {
            prepared_stmt_meta(std::pmr::memory_resource* resource,
                               session_hash_t stmt_session,
                               std::string sql,
                               uint32_t parameter_count,
                               components::types::complex_logical_type schema,
                               std::pmr::vector<field_type> specified_types,
                               NodeTag tag);

            session_hash_t stmt_session;
            // The worker keeps a prepared statement for exactly one execution,
            // successful or not; every later Execute prepares this text again
            // under a fresh session before executing.
            std::pmr::string sql;
            bool consumed;
            uint32_t parameter_count;
            components::types::complex_logical_type schema;
            // The kind of statement the prepare answered for. Describe(portal)
            // decides by it whether the portal may be RUN to be described:
            // running a statement that answers rows costs the execution the
            // Execute would do anyway (the portal keeps the result), while
            // running an INSERT / UPDATE / DELETE would write — twice, for a
            // client that describes a portal and then binds it again.
            NodeTag tag;
            std::pmr::vector<field_type> specified_types;
            std::pmr::vector<std::string> portal_names;
            // Describe(statement) already gave the client a RowDescription.
            bool described;
        };

        // Result format codes belong to the Bind that created the portal, and so
        // does the executed result: an Execute with a row limit stops at the
        // limit (PortalSuspended) and the next Execute on the same portal
        // continues from `emitted` without running the statement again.
        struct portal_meta {
            portal_meta(portal_t params, prepared_stmt_meta& statement, std::vector<result_encoding> format);

            portal_t params;
            std::reference_wrapper<prepared_stmt_meta> statement;
            std::vector<result_encoding> format;
            // Describe(portal) already gave the client a RowDescription.
            bool described;
            std::optional<session_payload> result;
            size_t emitted;
        };

        static std::vector<uint8_t> build_too_many_connections_error();

    protected:
        void start_impl() override;
        // Closes on the Worker every prepared statement the client left
        // unexecuted (Terminate, socket drop, FATAL error, timeout).
        void finish_impl() override;

        log_t& get_logger_impl() override;
        uint32_t get_header_size() const override;
        uint32_t get_packet_size(const std::vector<uint8_t>& header) const override;
        // A message type the protocol does not define is FATAL here, before
        // its body is read.
        bool validate_payload_size(const std::vector<uint8_t>& header, uint32_t& size) override;

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
        // `reader` stands after the protocol version the caller dispatched on.
        void handle_startup_message(int32_t protocol_version, packet_reader& reader);
        void handle_ssl_decline(packet_reader& reader);
        void handle_query(std::string query);
        // Answers a transaction statement of a simple Query from
        // transaction_manager: one the engine ran (`error` empty: BEGIN,
        // COMMIT, ROLLBACK) or one it refused as "Unsupported node type"
        // (SAVEPOINT, ROLLBACK TO, RELEASE). Any other refusal goes out as the
        // engine's error.
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
        // Appends a RowDescription (with the given result format codes) for a
        // resolved schema, NoData otherwise; `described` is set to whether a
        // RowDescription went out — only then does the client know the row
        // shape. Returns false after sending an ErrorResponse for a column the
        // formats cannot carry; nothing is appended then.
        [[nodiscard]] bool do_describe(const components::types::complex_logical_type& schema,
                                       const std::vector<result_encoding>& format,
                                       std::vector<std::vector<uint8_t>>& packets,
                                       bool& described);
        // Prepares the statement text again once the worker has consumed it.
        // Returns false after sending the error to the client.
        bool reprepare_stmt(prepared_stmt_meta& stmt);
        // Runs the portal's statement and stores the result in the portal.
        // Returns false after sending the error to the client.
        bool run_portal(portal_meta& portal);
        // PostgreSQL's text for a message with bytes after its last field.
        static constexpr const char* INVALID_MESSAGE_FORMAT = "invalid message format";
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
