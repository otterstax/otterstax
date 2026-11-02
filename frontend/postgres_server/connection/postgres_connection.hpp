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
#include "deallocate_statement.hpp"
#include "pipeline_state.hpp"
#include "protocol_parameters.hpp"

#include "frontend/common/asio_future_bridge.hpp"
#include "session/session_factory.hpp"
#include "session/session_payload.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <iostream>
#include <optional>

namespace frontend::postgres {
    class postgres_connection : public frontend_connection {
    public:
        // The Bind values, `$<i+1>` at i, each a one-row vector.
        using portal_t = std::pmr::vector<components::vector::vector_t>;

        postgres_connection(std::pmr::memory_resource* resource,
                            boost::asio::ip::tcp::socket&& socket,
                            uint32_t connection_id,
                            otterstax::session::session_factory_t& sessions,
                            connection_close_sink& close_sink,
                            size_t slot,
                            std::chrono::milliseconds read_timeout);

        // What a statement runs: the part of it a portal bound from it keeps
        // (a copy, taken at Bind), so a portal outlives the statement's
        // DEALLOCATE (PostgreSQL's DEALLOCATE touches no portal).
        struct statement_run_t {
            statement_run_t(std::pmr::memory_resource* resource,
                            otterstax::session::statement_id_t stmt_session,
                            std::string_view sql,
                            components::types::complex_logical_type schema,
                            NodeTag tag);
            // A copy whose text lives on `resource`.
            statement_run_t(const statement_run_t& other, std::pmr::memory_resource* resource);

            // The statement's id in the connection's query session, which keeps
            // it (its text, its described schema) for every execution until the
            // statement is closed.
            otterstax::session::statement_id_t stmt_session;
            // Prepared again under a fresh id by a portal whose statement is
            // gone (portal_entry).
            std::pmr::string sql;
            components::types::complex_logical_type schema;
            NodeTag tag;
            // A DEALLOCATE prepared through Parse (psycopg's pipeline sends
            // DEALLOCATE ALL so): the frontend runs it at Execute, the session
            // never holds it (`stmt_session` names nothing there).
            std::optional<deallocate_statement_t> deallocate;
            // A SET of a protocol parameter prepared through Parse: the frontend
            // checks it at Execute, the session never holds it either.
            std::optional<protocol_setting_t> protocol_setting;
            // SHOW of a client parameter prepared through Parse: the frontend
            // answers it at Execute (`schema` is its one text column).
            std::optional<client_parameter_t> show;
            // RESET ALL / DISCARD ALL: the engine runs it, the frontend resets
            // the client parameters behind its success.
            bool resets_client_parameters{false};
            // DEALLOCATE, a protocol SET or a SHOW of a client parameter: kept
            // by the frontend alone.
            bool frontend_only() const noexcept {
                return deallocate.has_value() || protocol_setting.has_value() || show.has_value();
            }
        };

        struct prepared_stmt_meta {
            prepared_stmt_meta(statement_run_t run,
                               uint32_t parameter_count,
                               std::pmr::vector<field_type> specified_types);

            statement_run_t run;
            uint32_t parameter_count;
            // The type each `$n` is bound by: the client's (Parse), else the
            // engine's (the describe's parameter types), else text.
            std::pmr::vector<field_type> specified_types;
            // Describe(statement) already gave the client a RowDescription.
            bool described{false};
            // COMMIT / ROLLBACK / ROLLBACK TO (the describe's transaction
            // statement): the only statement a Bind takes in an aborted block,
            // as in PostgreSQL's exec_bind_message.
            bool transaction_exit{false};
        };

        // Result format codes belong to the Bind that created the portal. An
        // Execute with a row limit runs the statement under an engine window
        // (query_session_t::execute_window): `window` names it while more rows
        // may follow (PortalSuspended), and the next Execute on the portal
        // fetches on from it; a portal closed, replaced or left at an
        // autocommit Sync closes its window.
        struct portal_meta {
            portal_meta(portal_t params,
                        std::string statement_name,
                        statement_run_t run,
                        bool statement_described,
                        std::vector<result_encoding> format);

            portal_t params;
            // The statement it was bound from, by name (Close(statement) and a
            // replacing Parse close the portals of the statement still of that
            // name and entry); its run, as Bind found it.
            std::string statement_name;
            statement_run_t run;
            // The statement gave the client a RowDescription before Bind: the
            // shape an Execute is held to (same_wire_shape).
            bool statement_described;
            std::vector<result_encoding> format;
            // Describe(portal) already gave the client a RowDescription, or an
            // Execute sent one in front of the first rows.
            bool described{false};
            uint64_t window{0};
            // Ran to its end: a later Execute answers no rows (a statement the
            // frontend keeps: 55000, its portal cannot run again).
            bool done{false};
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
        // Every parameter is either the protocol's (protocol_parameters.hpp:
        // checked against what OtterStax does, any other value FATAL 22023;
        // a client parameter kept as the client gives it — application_name
        // cleaned, extra_float_digits checked, FATAL 22023) or a
        // setting of the engine session: `database` is SET search_path =
        // '<database>.public' (a database the engine does not have: FATAL
        // 3D000), the `options` switches (-c name=value, --name=value) and every
        // other parameter SET in the engine, in that order (PostgreSQL's), a
        // refusal FATAL with the engine's text. `user` names nobody to
        // authenticate; `_pq_.*` are protocol extensions.
        void handle_startup_message(int32_t protocol_version, packet_reader& reader);
        // The startup's engine settings in order; false after the FATAL error.
        // Each one the engine applied is followed (follow_parameters).
        [[nodiscard]] bool apply_startup_settings(const std::optional<std::string>& database,
                                                  const std::vector<std::pair<std::string, std::string>>& settings);
        // The reported engine settings as a fresh session has them, the
        // default every session shares: the engine's built-in values with the
        // stored pg_catalog.pg_settings rows over them (the last row of a
        // setting, as the engine reads it at its start), each through the
        // engine's own set_setting. ALTER SYSTEM never reaches the engine
        // (the parser refuses it), so they stay what the engine started with;
        // still they are read per connection: the frontend has no per-server
        // slot (frontend_server constructs the connections) and the read is
        // one catalog scan on the connection's own engine session.
        void read_default_settings();
        // A SET of a protocol parameter (Query, or an Execute of one Parse
        // kept): a failed block refuses it (25P02), a value OtterStax does not
        // give is 22023, a SET of a client parameter changes the connection's
        // (set_client_parameter); nothing reaches the engine. False after the
        // error.
        [[nodiscard]] bool run_protocol_setting(const protocol_setting_t& setting);
        // A SET / RESET of a client parameter as PostgreSQL's GUC applies it
        // (client_setting_t): a value extra_float_digits does not take is 22023
        // (inside a block it fails it). False after the error.
        [[nodiscard]] bool set_client_parameter(const protocol_setting_t& setting);
        // ReadyForQuery with the block's status, behind a ParameterStatus for
        // every reported parameter (application_name, client_encoding,
        // TimeZone, search_path) whose value is not the one last reported —
        // PostgreSQL's ReportChangedGUCOptions, which runs in front of every
        // ReadyForQuery: a SET, a RESET, the end of a block (a ROLLBACK takes a
        // value back), a ROLLBACK TO, inside a block too. extra_float_digits is
        // not reported (no GUC_REPORT).
        void append_ready_for_query(std::vector<std::vector<uint8_t>>& response);
        // What an answered statement did to the parameters the frontend
        // follows: the engine settings it assigned (session_payload::
        // setting_changes), a savepoint it set, released or rolled back to, the
        // end of the block (COMMIT keeps, anything else drops). Every awaited
        // answer goes through here (await_session).
        void follow_parameters(const otterstax::awaited_t<session_payload>& answer);
        // RESET ALL: every client parameter back to its StartupMessage value,
        // as a SET (inside a block until it ends).
        void reset_client_parameters();
        // CommandComplete of a SET of the frontend's: RESET for RESET (utility.c).
        command_complete_tag setting_tag(const protocol_setting_t& setting) const;
        // The answer of SHOW <client parameter>: a RowDescription unless
        // `row_shape_known`, one DataRow in `format`, CommandComplete SHOW.
        std::vector<std::vector<uint8_t>>
        answer_show(client_parameter_t parameter, const std::vector<result_encoding>& format, bool row_shape_known);
        void handle_ssl_decline(packet_reader& reader);
        void handle_query(std::string query);
        // DEALLOCATE [PREPARE] <name> | ALL on the simple-query route: run_deallocate,
        // then CommandComplete DEALLOCATE / DEALLOCATE ALL and ReadyForQuery.
        void handle_deallocate(const deallocate_statement_t& statement, const std::string& sql);
        // A name of the connection's statement map (Parse) is dropped there, as
        // PostgreSQL's DEALLOCATE does — the session entry released, the portals
        // bound from it left open; a failed block refuses it with 25P02. Any
        // other name is the engine's (SQL PREPARE): `sql` runs on the session,
        // the engine's answer; DEALLOCATE ALL drops the map's and runs on the
        // engine. Returns false after sending the ErrorResponse.
        bool run_deallocate(const deallocate_statement_t& statement, const std::string& sql);

        // Every session call is awaited here: the bridge, plus the end of a
        // pending failure once an answer shows the session ran it.
        otterstax::awaited_t<session_payload>
        await_session(actor_zeta::unique_future<otterstax::session::query_session_t::session_result> fut);
        // The block is failed ('E'): the session's, or a failure sent to it and
        // not yet seen applied.
        bool block_aborted() const;
        // 25P02 for what an aborted block refuses before it reaches the session
        // (PostgreSQL's exec_bind_message: a Bind of anything but a transaction
        // exit; a Parse of a DEALLOCATE, which the frontend keeps).
        void refuse_in_aborted_block();
        // The ReadyForQuery status: where the engine stamped the session after
        // its last statement ('I' / 'T' / 'E', query_session_t::transaction_status),
        // 'E' too while a failure sent to it is pending. One source for the
        // simple and the extended route alike.
        transaction_status ready_status() const;
        // A session failure as an ErrorResponse: the bridge's timeout (told by
        // the await's status, never by a code) 57014, else the SQLSTATE of the
        // engine's code (sqlstate_of; 42601 with `prefix` for a code without
        // one).
        void send_session_error(const otterstax::awaited_t<session_payload>& answer,
                                std::string_view prefix,
                                error_severity severity = error_severity::error());

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
        // portals are left alone (each keeps its statement's run, and prepares
        // the text again at its Execute: portal_entry).
        void drop_statement(const std::string& name);
        // The session entry a portal runs: its statement's while that statement
        // is still the one of its name, else its text prepared again under a
        // fresh id (`temporary`: closed by the caller after the run, through
        // close_session_statement). std::nullopt after the ErrorResponse.
        std::optional<otterstax::session::statement_id_t> portal_entry(const portal_meta& portal, bool& temporary);
        // Appends a RowDescription (with the given result format codes) for a
        // resolved schema, NoData otherwise; `described` is set to whether a
        // RowDescription went out — only then does the client know the row
        // shape. Returns false after sending an ErrorResponse for a column the
        // formats cannot carry; nothing is appended then.
        [[nodiscard]] bool do_describe(const components::types::complex_logical_type& schema,
                                       const std::vector<result_encoding>& format,
                                       std::vector<std::vector<uint8_t>>& packets,
                                       bool& described);
        // Closes the portal's engine window, if one is open (Close, a replacing
        // Bind, an autocommit Sync); a failure is logged only.
        void close_window(portal_meta& portal);
        using portal_iterator = std::pmr::unordered_map<std::string, portal_meta>::iterator;
        // Ends a portal: its window closed when `close_windows` (a window the
        // transaction's end ended is not), the portal forgotten. Answers the
        // next portal.
        portal_iterator release_portal(portal_iterator it, bool close_windows);
        // Every portal, as release_portal ends one: at the transaction's end.
        void release_portals(bool close_windows);
        // close_statement of the session's entry; a failure is logged only.
        void close_session_statement(otterstax::session::statement_id_t stmt_session);
        // PostgreSQL's text for a message with bytes after its last field.
        static constexpr const char* INVALID_MESSAGE_FORMAT = "invalid message format";
        // `hint`: the ErrorResponse's H field, empty for none.
        void send_error_response(const char* sqlstate,
                                 std::string message,
                                 error_severity severity = error_severity::error(),
                                 std::string hint = {});

        std::pmr::memory_resource* resource_;
        // The one owner of every prepared statement (a portal keeps a copy of
        // its run, never the statement).
        std::pmr::unordered_map<std::string, prepared_stmt_meta> statement_name_map_;
        std::pmr::unordered_map<std::string, portal_meta> portals_;
        packet_writer writer_;
        // The connection's query session: one engine session, its transaction
        // and its prepared statements. Closed in finish_impl.
        otterstax::session::session_ptr session_;
        std::vector<uint8_t> backend_secret_key_;
        // A failure reported to the session (fail_transaction) and not yet seen
        // applied: an open block is reported failed meanwhile.
        bool failure_pending_{false};
        // The client parameters as PostgreSQL's GUC keeps them
        // (protocol_parameters.hpp): the frontend's, never the engine's. A block
        // their SET was made in ends in follow_parameters.
        client_setting_t<std::string> application_name_{};
        // PostgreSQL's value when the client names none: the database's
        // encoding.
        client_setting_t<std::string> client_encoding_{"UTF8", "UTF8"};
        // How this connection's float cells are written as text.
        client_setting_t<int> extra_float_digits_{default_extra_float_digits, default_extra_float_digits};
        // The engine settings PostgreSQL reports, followed as the engine keeps
        // them (follow_parameters): `reset` the engine's default
        // (read_default_settings), the startup's SET its session value.
        client_setting_t<std::string> timezone_{};
        client_setting_t<std::string> search_path_{};
        // The value of the last ParameterStatus of each reported parameter.
        struct reported_t {
            std::string application_name;
            std::string client_encoding;
            std::string timezone;
            std::string search_path;
        };
        reported_t reported_{};
        pipeline_state pipeline_;
        bool use_protocol_3_2_;
        log_t log_;
    };
} // namespace frontend::postgres
