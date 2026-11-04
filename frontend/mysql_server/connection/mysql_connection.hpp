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
#include "../protocol/lexer.hpp"
#include "../protocol/protocol_statement.hpp"
#include "../resultset/mysql_resultset.hpp"

#include "frontend/common/asio_future_bridge.hpp"
#include "session/session_factory.hpp"
#include "session/session_payload.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <optional>
#include <random>

namespace frontend::mysql {
    class mysql_connection : public frontend_connection {
    public:
        mysql_connection(std::pmr::memory_resource* resource,
                         boost::asio::ip::tcp::socket&& socket,
                         uint32_t connection_id,
                         otterstax::session::session_factory_t& sessions,
                         connection_close_sink& close_sink,
                         size_t slot,
                         std::chrono::milliseconds read_timeout);

        struct prepared_stmt_meta {
            prepared_stmt_meta(std::pmr::memory_resource* resource,
                               otterstax::session::statement_id_t stmt_session,
                               uint32_t parameter_count);

            // The statement's id in the connection's query session, which keeps
            // it for every COM_STMT_EXECUTE until COM_STMT_CLOSE.
            otterstax::session::statement_id_t stmt_session;
            uint32_t parameter_count;
            std::pmr::vector<uint16_t> param_types;
            // A protocol statement (SELECT @@x, SET @@x = ?, SHOW VARIABLES, ...)
            // prepared by a driver: answered by the frontend at every
            // COM_STMT_EXECUTE as COM_QUERY answers it, never stored in the
            // session (`stmt_session` names nothing there).
            std::optional<protocol_statement_t> protocol;
        };

        static std::vector<uint8_t> build_too_many_connections_error();

    protected:
        void start_impl() override;
        // Closes the connection's query session (COM_QUIT, socket drop, protocol
        // error, timeout): its prepared statements go with it.
        void finish_impl() override;

        log_t& get_logger_impl() override;
        uint32_t get_header_size() const override;
        uint32_t get_packet_size(const std::vector<uint8_t>& header) const override;
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

        void send_handshake();

        void handle_auth(std::vector<uint8_t> payload);
        void handle_command(std::vector<uint8_t> payload);
        // COM_QUERY: the text is read by MySQL's lexical rules and written in
        // the engine's form (protocol/lexer.hpp, to_engine_text) before the
        // session sees it; a `?` in it is ER_PARSE_ERROR, as MySQL answers it.
        void handle_query(std::string query);
        // A text to_engine_text refused: the ERR packet, with an open block
        // failed behind it as an engine parse error fails it.
        void refuse_text(text_refusal_t refusal);
        // The MySQL protocol statements (protocol/protocol_statement.hpp), whose
        // forms the engine's grammar cannot read: no `@@variable`, no SHOW
        // VARIABLES / WARNINGS, no MySQL SET forms, no USE. A SELECT of system
        // variables, SHOW VARIABLES and SHOW WARNINGS are answered here, whatever
        // the session's block (a refusal of driver housekeeping would fail an
        // open block behind an answer the client never asked to fail); a SET and
        // a USE go to the engine as its SET and the engine answers them
        // (answer_setting, answer_use). True when the answer went out.
        bool serve_protocol_statement(std::string_view query);
        // Every assignment of `set` in ONE engine statement
        // (engine_setting_plan), as MySQL applies a SET: all or none. A
        // refusal — the frontend's, the engine's parser's or the engine's — is
        // the ERR packet with nothing applied, else an OK packet; only then is
        // what the engine accepted remembered (autocommit_, time_zone_).
        void answer_setting(const set_variables_t& set);
        // SET search_path = '<database>.public' on the engine: an OK packet and
        // database_ remembered, or the ERR packet — ER_BAD_DB_ERROR for a
        // database or schema that does not exist, any other refusal as
        // send_statement_error answers a statement. False when the engine
        // refused (the ERR packet went out).
        bool answer_use(const std::string& database, bool answer_ok);
        // Awaits a statement of the frontend's own (a SET, a USE) on the
        // session; false after the ERR packet went out: the bridge's own
        // failures (answer_await_failure), ER_BAD_DB_ERROR for a missing
        // database / schema when `names_database`, else send_statement_error.
        bool await_own_statement(
            actor_zeta::unique_future<otterstax::session::query_session_t::session_result> future,
            bool names_database);
        // What the protocol statements' answers are derived from.
        protocol_context_t protocol_context() const;
        // A session error as an ERR packet (COM_QUERY, COM_STMT_PREPARE,
        // COM_STMT_EXECUTE, a SET, a USE that names an existing place): `code`
        // a statement the engine does not support is ER_NOT_SUPPORTED_YET,
        // anything else ER_SYNTAX_ERROR.
        void send_statement_error(core::error_code_t code, std::string error);
        // The status flags of an OK / EOF packet: SERVER_STATUS_AUTOCOMMIT
        // while the last SET autocommit the engine accepted left it on, and
        // SERVER_STATUS_IN_TRANS while the engine stamps the session inside a
        // transaction (open or failed: the block holds until ROLLBACK / COMMIT
        // ends it; with autocommit off, after any statement).
        server_status_flags_t status_flags() const;
        // The bridge's own failures (asio_future_bridge.hpp, by status, never by
        // code): ER_QUERY_TIMEOUT for a statement it stopped waiting for,
        // ER_UNKNOWN_ERROR with the bridge's message for one that got no
        // answer; an open block is failed behind either. True when the answer
        // went out (the session answered nothing).
        bool answer_await_failure(const otterstax::awaited_t<session_payload>& r);
        // Inside an open block (the engine's status in_transaction), fails it
        // behind the statement in flight (query_session_t::fail_transaction, not
        // awaited).
        void fail_open_block(std::string reason);

        // COM_STMT_PREPARE: the text is written in the engine's form, its `?`
        // placeholders as `$n` (protocol/lexer.hpp, to_engine_text), before the
        // session ever sees it — the engine's grammar reads a bare `?` as an
        // operator. A protocol statement is prepared by the frontend itself
        // (prepare_protocol_statement).
        void handle_prepared_stmt(std::string_view query);
        // COM_STMT_PREPARE of a protocol statement: STMT_PREPARE_OK with its
        // columns (computed now, as MySQL resolves a system variable at prepare:
        // an unknown one is refused here, failing no block) and
        // `parameter_count` parameters. A SET the frontend refuses is refused
        // here, an open block failed behind it, as COM_QUERY refuses it.
        void prepare_protocol_statement(protocol_statement_t statement, uint32_t parameter_count);
        // COM_STMT_EXECUTE of a prepared protocol statement: its answer as a
        // binary resultset or an OK / ERR packet (a SET / USE: the engine's); the
        // parameters are not read.
        void execute_protocol_statement(const protocol_statement_t& statement);

        // The COM_STMT_EXECUTE values, `$<i+1>` at i, each a one-row vector.
        std::pmr::vector<components::vector::vector_t>
        handle_execute_params(uint32_t stmt_id,
                              size_t num_params,
                              std::pmr::vector<uint16_t>& param_types,
                              packet_reader&& reader);
        // Drops an unexecuted statement in the session; a failure is logged only.
        void close_stmt_in_session(otterstax::session::statement_id_t stmt_session, const char* reason);
        void handle_execute_stmt(prepared_stmt_meta& stmt,
                                 std::pmr::vector<components::vector::vector_t> param_values);

        void send_resultset(mysql_resultset&& result);
        void send_error(mysql_error error_code, std::string message);
        // ERR 1835 (ER_MALFORMED_PACKET) followed by the close of the
        // connection: a packet shorter than the fields its command carries
        // leaves nothing this client sends trustworthy.
        void send_malformed_packet_error(std::string message);

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<uint32_t, prepared_stmt_meta> statement_id_map_;
        packet_writer writer_;
        uint8_t sequence_id_;
        uint32_t next_statement_id_;
        uint32_t client_max_packet_size_;
        // The connection's query session: one engine session, its transaction
        // and its prepared statements. Closed in finish_impl.
        otterstax::session::session_ptr session_;
        // The user named in the HandshakeResponse (USER(), CURRENT_USER()).
        std::string user_;
        // What the last SET / USE the engine accepted set, as @@autocommit,
        // @@time_zone and DATABASE() answer it (the engine has no SHOW);
        // a refused SET changes none of them. A SET a ROLLBACK
        // later undoes is not followed here.
        bool autocommit_{true};
        std::string time_zone_{"SYSTEM"};
        std::optional<std::string> database_;
        connection_state state_;
        log_t log_;
    };
} // namespace frontend::mysql
