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

#include "frontend/common/asio_future_bridge.hpp"
#include "session/session_factory.hpp"
#include "session/session_payload.hpp"
#include "session/statement_kind.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <iostream>
#include <memory>
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
                            otterstax::session::session_factory_t& sessions,
                            connection_close_sink& close_sink,
                            size_t slot,
                            std::chrono::milliseconds read_timeout);

        struct prepared_stmt_meta {
            prepared_stmt_meta(std::pmr::memory_resource* resource,
                               otterstax::session::statement_id_t stmt_session,
                               std::string sql,
                               uint32_t parameter_count,
                               components::types::complex_logical_type schema,
                               std::pmr::vector<field_type> specified_types,
                               NodeTag tag);

            // The statement's id in the connection's query session.
            otterstax::session::statement_id_t stmt_session;
            // The session keeps a prepared statement for exactly one execution,
            // successful or not; every later Execute prepares this text again
            // under a fresh statement id before executing.
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
            // A DEALLOCATE prepared through Parse (psycopg's pipeline sends
            // DEALLOCATE ALL so): the frontend runs it at Execute, the session
            // never holds it (`stmt_session` names nothing there).
            std::optional<otterstax::session::deallocate_statement_t> deallocate;
            // COMMIT / ROLLBACK (session::is_transaction_exit_statement): the only
            // statement a Bind takes in an aborted block, as in PostgreSQL.
            bool transaction_exit = false;
        };
        // Shared with the portals bound from it: DEALLOCATE drops the statement
        // and leaves its portals open, as PostgreSQL does.
        using statement_ptr = std::shared_ptr<prepared_stmt_meta>;

        // Result format codes belong to the Bind that created the portal, and so
        // does the executed result: an Execute with a row limit stops at the
        // limit (PortalSuspended) and the next Execute on the same portal
        // continues from `emitted` without running the statement again.
        struct portal_meta {
            portal_meta(portal_t params, statement_ptr statement, std::vector<result_encoding> format);

            portal_t params;
            statement_ptr statement;
            std::vector<result_encoding> format;
            // Describe(portal) already gave the client a RowDescription.
            bool described;
            std::optional<session_payload> result;
            size_t emitted;
        };

        static std::vector<uint8_t> build_too_many_connections_error();

    protected:
        void start_impl() override;
        // Closes the connection's query session (Terminate, socket drop, FATAL
        // error, timeout): its prepared statements go with it.
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
        // DEALLOCATE [PREPARE] <name> | ALL on the simple-query route: run_deallocate,
        // then CommandComplete DEALLOCATE / DEALLOCATE ALL and ReadyForQuery.
        void handle_deallocate(otterstax::session::deallocate_statement_t statement);
        // Drops the named statement, or every named one, as PostgreSQL's
        // DEALLOCATE does: the session entry is released, the portals bound
        // from it stay open. An unknown name is 26000 and a failed block refuses
        // it with 25P02, as PostgreSQL does. Returns false after sending the
        // ErrorResponse.
        bool run_deallocate(const otterstax::session::deallocate_statement_t& statement);

        // Every session call is awaited here: the bridge, plus the end of a
        // pending failure once an answer shows the session ran it.
        otterstax::awaited_t<session_payload>
        await_session(actor_zeta::unique_future<otterstax::session::query_session_t::session_result> fut);
        // The block is failed ('E'): the session's, or a failure sent to it and
        // not yet seen applied.
        bool block_aborted() const;
        // 25P02 for what an aborted block refuses before it reaches the session
        // (PostgreSQL's exec_parse_message / exec_bind_message): every statement
        // but a transaction exit.
        void refuse_in_aborted_block();
        // The ReadyForQuery status: the session's transaction block ('I' / 'T' /
        // 'E'), 'E' too while a failure sent to it is pending. One source for
        // the simple and the extended route alike.
        transaction_status ready_status() const;
        // The answer a COMMIT of a failed block is given
        // (query_session_t::ended_failed_block): the engine rolled the block
        // back and it has ended, so no rows and the ROLLBACK tag, as PostgreSQL
        // answers it.
        session_payload rollback_payload() const;
        // A session failure as an ErrorResponse: the bridge's timeout (told by
        // the await's status, never by a code) 57014,
        // a statement refused in a failed block 25P02, one the engine does not
        // support (SAVEPOINT among them) 0A000, any other `prefix` + the text as
        // 42601.
        void send_session_error(const otterstax::awaited_t<session_payload>& answer, std::string_view prefix);

        void handle_parse(std::string stmt, std::string query, int16_t num_params, packet_reader&& reader);
        void handle_bind(std::string stmt,
                         std::string portal_name,
                         std::vector<result_encoding> format,
                         int16_t num_params,
                         packet_reader&& reader);
        void handle_execute(std::string portal_name, int32_t limit);
        void handle_close(describe_close_arg type, std::string name);
        void handle_describe(describe_close_arg type, std::string name);

        // Close / a Parse replacing a statement: the statement with its portals,
        // or the portal.
        void do_close(describe_close_arg type, std::string name);
        // Forgets the named statement and releases its session entry; its
        // portals are left alone (they hold the statement).
        void drop_statement(const std::string& name);
        // Appends a RowDescription (with the given result format codes) for a
        // resolved schema, NoData otherwise; `described` is set to whether a
        // RowDescription went out — only then does the client know the row
        // shape. Returns false after sending an ErrorResponse for a column the
        // formats cannot carry; nothing is appended then.
        [[nodiscard]] bool do_describe(const components::types::complex_logical_type& schema,
                                       const std::vector<result_encoding>& format,
                                       std::vector<std::vector<uint8_t>>& packets,
                                       bool& described);
        // Prepares the statement text again once the session has consumed it.
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
        std::pmr::unordered_map<std::string, statement_ptr> statement_name_map_;
        std::pmr::unordered_map<std::string, portal_meta> portals_;
        packet_writer writer_;
        // The connection's query session: one engine session, its transaction
        // and its prepared statements. Closed in finish_impl.
        otterstax::session::session_ptr session_;
        std::vector<uint8_t> backend_secret_key_;
        // A failure reported to the session (fail_transaction) and not yet seen
        // applied: an open block is reported failed meanwhile.
        bool failure_pending_{false};
        pipeline_state pipeline_;
        bool use_protocol_3_2_;
        log_t log_;
    };
} // namespace frontend::postgres
