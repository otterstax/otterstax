// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "postgres_connection.hpp"

#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <bit>

using namespace components;
using namespace components::sql;

namespace frontend::postgres {
    constexpr size_t INITIAL_MESSAGES_COUNT = 1 + 6 + 1 + 1; // auth_ok + 6 param + backend_key + ready_for_query
    constexpr size_t SECRET_KEY_3_2_SIZE = 32;
    constexpr size_t SECRET_KEY_SIZE = 4;

    void postgres_connection::handle_startup_message(int32_t protocol_version, packet_reader& reader) {
        OTX_ZONE_N("pg::handle_startup_message");
        log_->info("[Connection {}]: Client protocol version: {}", connection_id_, protocol_version);
        while (reader.remaining()) {
            std::string key = reader.read_string_null();

            // An unterminated key reads as empty: the fault, not the emptiness,
            // is what ends the loop then.
            if (!reader.ok() || key.empty()) {
                break;
            }

            if (!reader.remaining()) {
                send_error_response(sql_state::PROTOCOL_VIOLATION,
                                    "Malformed StartupMessage: key without value",
                                    error_severity::fatal());
                return;
            }

            std::string value = reader.read_string_null();
            if (!reader.ok()) {
                break;
            }

            if (key == "_pq_.protocol_extensions") {
                if (value.find("variable_length_keys") != std::string::npos ||
                    value.find("protocol_3_2") != std::string::npos) {
                    use_protocol_3_2_ = true;
                    log_->info("[Connection {}]: Postgres client supports Protocol 3.2+", connection_id_);
                }
            }
        }

        // A key or value without its terminator.
        if (!reader.ok()) {
            send_error_response(sql_state::PROTOCOL_VIOLATION,
                                "Malformed StartupMessage: unterminated parameter",
                                error_severity::fatal());
            return;
        }

        std::vector<std::vector<uint8_t>> msg;
        msg.reserve(INITIAL_MESSAGES_COUNT);
        msg.emplace_back(build_auth_ok(writer_));

        msg.emplace_back(build_parameter_status(writer_, "server_version", "16.0 (Mock)"));
        msg.emplace_back(build_parameter_status(writer_, "server_encoding", "UTF8"));
        msg.emplace_back(build_parameter_status(writer_, "client_encoding", "UTF8"));
        msg.emplace_back(build_parameter_status(writer_, "DateStyle", "ISO, MDY"));
        msg.emplace_back(build_parameter_status(writer_, "TimeZone", "UTC"));
        msg.emplace_back(build_parameter_status(writer_, "integer_datetimes", "on"));

        auto secret_key = secure_random_bytes(use_protocol_3_2_ ? SECRET_KEY_3_2_SIZE : SECRET_KEY_SIZE);
        if (!secret_key) {
            send_error_response(sql_state::INTERNAL_ERROR,
                                "the server could not generate the cancel key",
                                error_severity::fatal());
            return;
        }
        backend_secret_key_ = std::move(*secret_key);
        msg.emplace_back(build_backend_key_data(writer_, connection_id_, backend_secret_key_));

        log_->debug("[Connection {}] Generated BackendKeyData: key_size={} bytes",
                    connection_id_,
                    backend_secret_key_.size());
        msg.emplace_back(build_ready_for_query(writer_, transaction_status::IDLE));
        send_packet_merged(std::move(msg)); // send packet & begin message-handling loop
    }

    void postgres_connection::handle_ssl_decline(frontend::postgres::packet_reader& reader) {
        OTX_ZONE_N("pg::handle_ssl_decline");
        std::vector<uint8_t> negative(1);
        negative[0] = 'N';

        log_->info("[Connection {}] Sent SSL decline ('N'), waiting for StartupMessage", connection_id_);
        send_packet(std::move(negative), false);
        read_initial_message();
    }

    namespace {
        // The CommandComplete tag of a statement's answer: an executed BEGIN /
        // COMMIT / ROLLBACK by the transaction statement the session says it
        // was, every other statement by its kind.
        command_complete_tag tag_of(const session_payload& payload, int64_t rows) {
            switch (payload.transaction) {
                case otterstax::session::transaction_command_t::begin:
                    return command_complete_tag::begin();
                case otterstax::session::transaction_command_t::commit:
                    return command_complete_tag::commit();
                case otterstax::session::transaction_command_t::rollback:
                    return command_complete_tag::rollback();
                case otterstax::session::transaction_command_t::none:
                    break;
            }
            return command_complete_tag::simple_command(payload.tag, rows);
        }

        bool ends_block(const session_payload& payload) {
            return payload.transaction == otterstax::session::transaction_command_t::commit ||
                   payload.transaction == otterstax::session::transaction_command_t::rollback;
        }
    } // namespace

    otterstax::awaited_t<session_payload>
    postgres_connection::await_session(actor_zeta::unique_future<otterstax::session::query_session_t::session_result> fut) {
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        // Any answer but the bridge's timeout comes after every message sent
        // before the statement, a fail_transaction included: the session's block
        // is current again.
        if (!r.timed_out()) {
            failure_pending_ = false;
        }
        return r;
    }

    session_payload postgres_connection::rollback_payload() const {
        session_payload payload(resource_);
        payload.tag = T_TransactionStmt;
        payload.transaction = otterstax::session::transaction_command_t::rollback;
        return payload;
    }

    void postgres_connection::send_session_error(const otterstax::awaited_t<session_payload>& answer,
                                                 std::string_view prefix) {
        const auto& error = answer.error();
        if (answer.timed_out()) {
            // The statement goes on running; send_error_response fails an open
            // block behind it, so a COMMIT cannot publish what the client was told
            // is cancelled.
            send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit");
        } else if (error.type == core::error_code_t::transaction_finalized) {
            send_error_response(sql_state::IN_FAILED_SQL_TRANSACTION, std::string{error.what.c_str()});
        } else if (error.type == core::error_code_t::unimplemented_yet) {
            // SAVEPOINT / RELEASE / ROLLBACK TO among them: the engine has no
            // savepoints, and like every error inside a block the refusal fails it.
            send_error_response(sql_state::FEATURE_NOT_SUPPORTED, std::string{error.what.c_str()});
        } else {
            send_error_response(sql_state::SYNTAX_ERROR, std::string{prefix} + error.what.c_str());
        }
    }

    transaction_status postgres_connection::ready_status() const {
        switch (session_->transaction_state()) {
            case otterstax::session::transaction_state_t::idle:
                return transaction_status::IDLE;
            case otterstax::session::transaction_state_t::in_block:
                // A failure the session has been told of and not run yet.
                return failure_pending_ ? transaction_status::TRANSACTION_ERROR : transaction_status::IN_TRANSACTION;
            case otterstax::session::transaction_state_t::failed:
                return transaction_status::TRANSACTION_ERROR;
        }
        return transaction_status::IDLE;
    }

    bool postgres_connection::block_aborted() const {
        return session_->transaction_state() == otterstax::session::transaction_state_t::failed || failure_pending_;
    }

    void postgres_connection::refuse_in_aborted_block() {
        send_error_response(sql_state::IN_FAILED_SQL_TRANSACTION,
                            "current transaction is aborted, commands ignored until end of transaction block");
    }

    bool postgres_connection::run_deallocate(const otterstax::session::deallocate_statement_t& statement) {
        OTX_ZONE_N("pg::run_deallocate");
        if (block_aborted()) {
            refuse_in_aborted_block();
            return false;
        }
        if (!statement.name.has_value()) {
            // The named statements; the unnamed one is the protocol's, not DEALLOCATE's.
            std::vector<std::string> names;
            names.reserve(statement_name_map_.size());
            for (const auto& entry : statement_name_map_) {
                if (!entry.first.empty()) {
                    names.push_back(entry.first);
                }
            }
            for (const auto& name : names) {
                drop_statement(name);
            }
            return true;
        }
        if (statement.name->empty() || statement_name_map_.find(*statement.name) == statement_name_map_.end()) {
            send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME,
                                "prepared statement \"" + *statement.name + "\" does not exist");
            return false;
        }
        drop_statement(*statement.name);
        return true;
    }

    void postgres_connection::handle_deallocate(otterstax::session::deallocate_statement_t statement) {
        OTX_ZONE_N("pg::handle_deallocate");
        if (!run_deallocate(statement)) {
            return;
        }
        std::vector<std::vector<uint8_t>> response;
        response.emplace_back(
            build_command_complete(writer_, command_complete_tag::deallocate(!statement.name.has_value())));
        response.emplace_back(build_ready_for_query(writer_, ready_status()));
        send_packet_merged(std::move(response));
    }

    void postgres_connection::handle_query(std::string query) {
        OTX_ZONE_N("pg::handle_query");
        if (auto deallocate = otterstax::session::deallocate_statement(resource_, query)) {
            handle_deallocate(std::move(*deallocate));
            return;
        }
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::execute,
                                                                    session_->next_statement(),
                                                                    query);
        auto r = await_session(std::move(fut));

        session_payload sdata_result(resource_);
        if (!r.has_error()) {
            sdata_result = std::move(r.value());
        } else if (session_->ended_failed_block(r.error())) {
            // COMMIT of a failed block: CommandComplete ROLLBACK and status I,
            // as PostgreSQL answers it.
            sdata_result = rollback_payload();
        } else {
            send_session_error(r, "");
            return;
        }

        if (ends_block(sdata_result)) {
            portals_.clear(); // portals are released at the transaction's end
        }
        int64_t rows_cnt = static_cast<int64_t>(sdata_result.reported_rows());
        std::vector<std::vector<uint8_t>> response;
        if (sdata_result.column_count() > 0) {
            // A column the text encoder cannot carry is an ErrorResponse, not a
            // dropped connection.
            if (auto bad = find_unsupported_column<frontend_type::POSTGRES>(sdata_result.chunks.front(),
                                                                           {result_encoding::TEXT})) {
                send_error_response(sql_state::FEATURE_NOT_SUPPORTED,
                                    unsupported_column_message<frontend_type::POSTGRES>(*bad));
                return;
            }

            postgres_resultset result(writer_);
            result.add_chunk_columns(sdata_result.chunks.front()); // default text encoding
            for (auto& ch : sdata_result.chunks) {
                for (size_t i = 0; i < ch.size(); ++i) {
                    result.add_row(ch, i);
                }
            }
            response = postgres_resultset::build_packets(std::move(result));
        }
        response.emplace_back(build_command_complete(writer_, tag_of(sdata_result, rows_cnt)));
        response.emplace_back(build_ready_for_query(writer_, ready_status()));
        send_packet_merged(std::move(response));
    }

    void
    postgres_connection::handle_parse(std::string stmt, std::string query, int16_t num_params, packet_reader&& reader) {
        OTX_ZONE_N("pg::handle_parse");
        log_->info("[Connection {}] PARSE stmt: \"{}\", query: \"{}\"", connection_id_, stmt, query);
        std::pmr::vector<field_type> specified_types(resource_);
        for (int i = 0; i < num_params; ++i) {
            auto oid = reader.read_int32();
            if (get_field_type(oid) == field_type::NA) {
                specified_types.emplace_back(field_type::TEXT);
            } else {
                specified_types.emplace_back(static_cast<field_type>(oid));
            }
        }

        if (!reader.ok()) {
            send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message parameter types");
            return;
        }
        // Nothing may follow the last parameter type.
        if (reader.remaining() != 0) {
            send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
            return;
        }

        // PostgreSQL refuses a Parse in an aborted block before it parses the
        // statement for anything (exec_parse_message), unless the statement ends
        // the block or the text is empty.
        const bool transaction_exit = otterstax::session::is_transaction_exit_statement(resource_, query);
        if (block_aborted() && !transaction_exit && query.find_first_not_of(" \t\r\n\f\v;") != std::string::npos) {
            refuse_in_aborted_block();
            return;
        }

        const auto id = session_->next_statement();
        // DEALLOCATE is the frontend's (session/statement_kind.hpp): kept here
        // and run at Execute, never prepared by the session.
        if (auto deallocate = otterstax::session::deallocate_statement(resource_, query)) {
            if (!specified_types.empty()) {
                send_error_response(sql_state::UNDEFINED_PARAMETER,
                                    "Parameter type left unspecified: specified " +
                                        std::to_string(specified_types.size()) + " out of 0");
                return;
            }
            do_close(describe_close_arg::STATEMENT, stmt);
            auto meta = std::allocate_shared<prepared_stmt_meta>(std::pmr::polymorphic_allocator<prepared_stmt_meta>(resource_),
                                                                 resource_,
                                                                 id,
                                                                 std::move(query),
                                                                 0,
                                                                 types::complex_logical_type{},
                                                                 std::move(specified_types),
                                                                 T_DeallocateStmt);
            meta->consumed = true; // nothing in the session to close or execute
            meta->transaction_exit = false;
            meta->deallocate = std::move(*deallocate);
            statement_name_map_.emplace(std::move(stmt), std::move(meta));
            send_packet(build_parse_complete(writer_));
            return;
        }
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::prepare_schema,
                                                                    id,
                                                                    query);
        auto r = await_session(std::move(fut));

        if (r.has_error()) {
            send_session_error(r, "Syntax error: ");
            return;
        }

        session_payload result = std::move(r.value());
        log_->debug("[Connection {}] PARSE stmt: query: \"{}\", param_cnt={}",
                    connection_id_,
                    query,
                    result.parameter_count);

        if (result.parameter_count != specified_types.size()) {
            send_error_response(sql_state::UNDEFINED_PARAMETER,
                                "Parameter type left unspecified: specified " + std::to_string(specified_types.size()) +
                                    " out of " + std::to_string(result.parameter_count));
            return;
        }

        // statements with identical name replace each other, together with their portals
        do_close(describe_close_arg::STATEMENT, stmt);
        auto meta = std::allocate_shared<prepared_stmt_meta>(std::pmr::polymorphic_allocator<prepared_stmt_meta>(resource_),
                                                             resource_,
                                                             id,
                                                             std::move(query),
                                                             result.parameter_count,
                                                             std::move(result.schema),
                                                             std::move(specified_types),
                                                             result.tag);
        meta->transaction_exit = transaction_exit;
        statement_name_map_.emplace(std::move(stmt), std::move(meta));
        send_packet(build_parse_complete(writer_));
    }

    void postgres_connection::handle_bind(std::string stmt,
                                          std::string portal_name,
                                          std::vector<result_encoding> format,
                                          int16_t num_params,
                                          packet_reader&& reader) {
        OTX_ZONE_N("pg::handle_bind");
        log_->info("[Connection {}] BIND stmt: \"{}\", portal: \"{}\"", connection_id_, stmt, portal_name);
        auto it = statement_name_map_.find(stmt);
        if (it == statement_name_map_.end()) {
            send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME, "Unknown prepared statement in BIND: " + stmt);
            return;
        }

        auto& statement = *it->second;
        // As PostgreSQL's exec_bind_message: in an aborted block only a
        // transaction exit without parameters is bound.
        if (block_aborted() && (!statement.transaction_exit || num_params != 0)) {
            refuse_in_aborted_block();
            return;
        }
        if (num_params != statement.parameter_count) {
            send_error_response(sql_state::UNDEFINED_PARAMETER,
                                "Missing parameters in BIND to statement " + stmt + ": received " +
                                    std::to_string(num_params) + " out of required " +
                                    std::to_string(statement.parameter_count));
            return;
        }

        portal_t params(resource_);
        params.reserve(num_params);
        for (int16_t i = 0; i < num_params; ++i) {
            result_encoding encoding;
            if (auto encoding_opt = get_format_code(format, i); !encoding_opt) {
                send_error_response(sql_state::UNDEFINED_PARAMETER,
                                    "Missing parameters format codes in BIND to statement " + stmt + ": received " +
                                        std::to_string(format.size()) + " out of required " +
                                        std::to_string(statement.parameter_count));
                return;
            } else {
                encoding = *encoding_opt;
            }

            if (reader.remaining() < 4) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, "Missing BIND message parameter value length");
                return;
            }

            int32_t len = reader.read_int32();
            if (len == -1) {
                params.emplace_back(resource_, nullptr);
                continue;
            }

            if (reader.remaining() < len) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND parameter value");
                return;
            }

            const field_type type = statement.specified_types[i];
            if (encoding == result_encoding::TEXT) {
                std::string str(len, '\0');
                for (int j = 0; j < len; str[j++] = static_cast<char>(reader.read_uint8()))
                    ;

                auto parsed = parse_text_parameter(resource_, type, std::move(str));
                if (parsed.has_error()) {
                    // The portal is incomplete: nothing may follow the error, in
                    // particular no BindComplete.
                    send_error_response(parsed.error().type == core::error_code_t::unimplemented_yet
                                            ? sql_state::FEATURE_NOT_SUPPORTED
                                            : sql_state::INVALID_TEXT_REPRESENTATION,
                                        std::string{parsed.error().what.c_str()});
                    return;
                }
                params.emplace_back(std::move(parsed.value()));
                continue;
            }

            switch (type) {
                case field_type::BOOL:
                    if (len != 1) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid BOOL binary length");
                        return;
                    }
                    params.emplace_back(resource_, reader.read_uint8() != 0);
                    break;
                case field_type::INT2:
                    if (len != 2) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT2 binary length");
                        return;
                    }
                    params.emplace_back(resource_, reader.read_int16());
                    break;
                case field_type::INT4:
                    if (len != 4) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT4 binary length");
                        return;
                    }
                    params.emplace_back(resource_, reader.read_int32());
                    break;
                case field_type::INT8:
                    if (len != 8) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT8 binary length");
                        return;
                    }
                    params.emplace_back(resource_, reader.read_int64());
                    break;
                case field_type::FLOAT4: {
                    if (len != 4) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid FLOAT4 binary length");
                        return;
                    }
                    uint32_t raw = reader.read_uint32();
                    params.emplace_back(resource_, std::bit_cast<float>(raw));
                    break;
                }
                case field_type::FLOAT8: {
                    if (len != 8) {
                        send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid FLOAT8 binary length");
                        return;
                    }
                    uint64_t raw = reader.read_uint64();
                    params.emplace_back(resource_, std::bit_cast<double>(raw));
                    break;
                }
                case field_type::TEXT: {
                    std::string str(len, '\0');
                    for (int j = 0; j < len; str[j++] = static_cast<char>(reader.read_uint8()))
                        ;
                    params.emplace_back(resource_, std::move(str));
                    break;
                }
                default:
                    send_error_response(sql_state::FEATURE_NOT_SUPPORTED, "Unsupported parameter type");
                    return;
            }
        }

        if (reader.remaining() < 2) {
            send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message result format codes");
            return;
        }

        auto out_format_len = reader.read_int16();
        if (reader.remaining() < 2 * out_format_len) {
            send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message result format codes");
            return;
        }

        std::vector<result_encoding> out_format;
        out_format.reserve(out_format_len);
        for (int16_t i = 0; i < out_format_len; ++i) {
            out_format.emplace_back(static_cast<result_encoding>(reader.read_int16()));
        }

        // Every fixed-size field above was pre-checked; this is the reader's
        // own verdict on the message, checked before anything is kept from it.
        if (!reader.ok()) {
            send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message");
            return;
        }
        // Nothing may follow the last result format code.
        if (reader.remaining() != 0) {
            send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
            return;
        }

        // add & create portal
        statement.portal_names.emplace_back(portal_name);
        portals_.erase(portal_name); // portals with identical name replace each other
        portals_.emplace(std::move(portal_name), portal_meta(std::move(params), it->second, std::move(out_format)));

        send_packet(build_bind_complete(writer_));
    }

    bool postgres_connection::reprepare_stmt(prepared_stmt_meta& stmt) {
        OTX_ZONE_N("pg::reprepare_stmt");
        const auto id = session_->next_statement();
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::prepare_schema,
                                                                    id,
                                                                    std::string{stmt.sql.c_str()});
        auto r = await_session(std::move(fut));

        if (r.has_error()) {
            send_session_error(r, "Syntax error: ");
            return false;
        }

        if (r.value().parameter_count != stmt.parameter_count) {
            send_error_response(sql_state::UNDEFINED_PARAMETER,
                                "Prepared statement changed its parameter count on re-prepare: " +
                                    std::to_string(r.value().parameter_count) + " out of " +
                                    std::to_string(stmt.parameter_count));
            return false;
        }

        stmt.stmt_session = id;
        stmt.consumed = false;
        return true;
    }

    bool postgres_connection::run_portal(portal_meta& portal) {
        OTX_ZONE_N("pg::run_portal");
        auto& stmt = *portal.statement;
        if (stmt.consumed && !reprepare_stmt(stmt)) {
            return false;
        }
        // The session drops the statement on every exit path of the execution.
        stmt.consumed = true;

        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(session_->address(),
                             &otterstax::session::query_session_t::execute_prepared_statement,
                             stmt.stmt_session,
                             std::move(portal.params));
        auto r = await_session(std::move(fut));

        if (r.has_error()) {
            if (!session_->ended_failed_block(r.error())) {
                send_session_error(r, "Syntax error: ");
                return false;
            }
            // COMMIT of a failed block: CommandComplete ROLLBACK, as PostgreSQL
            // answers it; the Sync then reports the block ended.
            portal.result.emplace(rollback_payload());
            portal.emitted = 0;
            return true;
        }

        portal.result.emplace(std::move(r.value()));
        portal.emitted = 0;
        return true;
    }

    void postgres_connection::handle_execute(std::string portal_name, int32_t limit) {
        OTX_ZONE_N("pg::handle_execute");
        log_->info("[Connection {}] EXECUTE portal: \"{}\"", connection_id_, portal_name);
        auto it = portals_.find(portal_name);
        if (it == portals_.end()) {
            send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME,
                                "Unknown portal in EXECUTE: \"" + portal_name + "\"");
            return;
        }

        auto& portal = it->second;
        // The portal holds its statement: a DEALLOCATE ALL run below drops the
        // statement from the map, never from under this Execute.
        const statement_ptr statement = portal.statement;
        auto& stmt = *statement;
        if (stmt.deallocate) {
            if (run_deallocate(*stmt.deallocate)) {
                send_packet(build_command_complete(writer_,
                                                   command_complete_tag::deallocate(!stmt.deallocate->name.has_value())));
            }
            return;
        }
        if (!portal.result && !run_portal(portal)) {
            return;
        }
        session_payload& payload = *portal.result;

        // PostgreSQL's guarantee: the row shape a client was handed before Bind
        // is the shape the rows arrive in. A statement described then was
        // described from what a prepare could know — a parameterized
        // statement's projection carries no types at all — so the executed
        // result may not be that shape, and a client that decodes by the
        // description it holds would read one type's bytes as another's.
        // PostgreSQL answers that divergence with 0A000 "cached plan must not
        // change result type" instead of streaming rows of another shape
        // (plancache.c, RevalidateCachedQuery), and so does this. A portal the
        // client described itself was described from this very result and is
        // not held against the statement's older answer.
        if (stmt.described && !portal.described) {
            assert(stmt.schema.type() == types::logical_type::STRUCT &&
                   "a described statement was described from a STRUCT schema");
            if (!same_wire_shape(stmt.schema.child_types(), payload.chunks.front())) {
                log_->warn("[Connection {}] EXECUTE portal \"{}\": described {} column(s), executed {}",
                           connection_id_,
                           portal_name,
                           stmt.schema.child_types().size(),
                           payload.column_count());
                send_error_response(sql_state::FEATURE_NOT_SUPPORTED, "cached plan must not change result type");
                return;
            }
        }

        std::vector<std::vector<uint8_t>> response;
        // CommandComplete reports the rows of THIS Execute: the rows sent for a
        // query, the affected count for a DML statement (which has no result
        // columns, so the count comes from the payload and is reported once).
        int64_t rows_cnt = 0;
        if (payload.column_count() > 0) {
            // The result formats are the portal's; the executed columns may
            // differ from the described schema, so every Execute checks them.
            if (auto bad =
                    find_unsupported_column<frontend_type::POSTGRES>(payload.chunks.front(), portal.format)) {
                send_error_response(sql_state::FEATURE_NOT_SUPPORTED,
                                    unsupported_column_message<frontend_type::POSTGRES>(*bad));
                return;
            }

            const size_t total = payload.size();
            const size_t stop = limit > 0 ? std::min(total, portal.emitted + static_cast<size_t>(limit)) : total;

            // Execute itself never carries the row shape; a client that skipped
            // Describe gets it once, in front of the first rows of the portal.
            const bool row_shape_known = stmt.described || portal.described || portal.emitted > 0;
            postgres_resultset result(writer_, row_shape_known);
            if (!row_shape_known) {
                result.add_chunk_columns(payload.chunks.front());
            }
            result.add_encoding(portal.format);

            size_t row = 0;
            for (auto& ch : payload.chunks) {
                for (size_t i = 0; i < ch.size() && row < stop; ++i, ++row) {
                    if (row >= portal.emitted) {
                        result.add_row(ch, i);
                    }
                }
                if (row >= stop) {
                    break;
                }
            }
            rows_cnt = static_cast<int64_t>(stop - portal.emitted);
            portal.emitted = stop;
            response = postgres_resultset::build_packets(std::move(result));

            if (portal.emitted < total) {
                response.emplace_back(build_portal_suspended(writer_));
                send_packet_merged(std::move(response));
                return;
            }
        } else {
            rows_cnt = portal.emitted == 0 ? static_cast<int64_t>(payload.affected_rows) : 0;
            portal.emitted = payload.size();
        }

        response.emplace_back(build_command_complete(writer_, tag_of(payload, rows_cnt)));
        const bool block_ended = ends_block(payload);
        send_packet_merged(std::move(response));
        if (block_ended) {
            portals_.clear(); // portals are released at the transaction's end
        }
    }

    void postgres_connection::handle_close(describe_close_arg type, std::string name) {
        OTX_ZONE_N("pg::handle_close");
        log_->info("[Connection {}] CLOSE name: \"{}\" type:\"{}\"", connection_id_, name, static_cast<char>(type));
        do_close(type, std::move(name));
        send_packet(build_close_complete(writer_));
    }

    void postgres_connection::do_close(frontend::postgres::describe_close_arg type, std::string name) {
        if (type == describe_close_arg::STATEMENT) {
            if (auto it = statement_name_map_.find(name); it != statement_name_map_.end()) {
                // Closing a statement closes the portals bound from it (protocol).
                for (auto&& p : it->second->portal_names) {
                    if (auto portal = portals_.find(p);
                        portal != portals_.end() && portal->second.statement == it->second) {
                        portals_.erase(portal);
                    }
                }
                drop_statement(name);
            }
        } else {
            portals_.erase(std::move(name));
        }
    }

    void postgres_connection::drop_statement(const std::string& name) {
        auto it = statement_name_map_.find(name);
        if (it == statement_name_map_.end()) {
            return;
        }
        auto& stmt = *it->second;
        // The session keeps an unexecuted statement until it is closed; an
        // executed one it has already dropped (consumed).
        if (!stmt.consumed && session_ != nullptr) {
            // a query session schedules itself: needs_sched is always false
            [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                        &otterstax::session::query_session_t::close_statement,
                                                                        stmt.stmt_session);
            auto r = await_session(std::move(fut));
            if (r.has_error()) {
                log_->warn("[Connection {}] CLOSE stmt: \"{}\": close_statement failed: {}",
                           connection_id_,
                           name,
                           r.error().what);
            }
        }
        // A portal still bound from it (DEALLOCATE leaves them) prepares the
        // text again at its Execute.
        stmt.consumed = true;
        statement_name_map_.erase(it);
    }

    void postgres_connection::handle_describe(describe_close_arg type, std::string name) {
        OTX_ZONE_N("pg::handle_describe");
        log_->info("[Connection {}] DESCRIBE name: \"{}\" type:\"{}\"", connection_id_, name, static_cast<char>(type));

        std::vector<std::vector<uint8_t>> packets;
        if (type == describe_close_arg::STATEMENT) {
            auto it = statement_name_map_.find(name);
            if (it == statement_name_map_.end()) {
                send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME, "Statement " + name + " does not exist");
                return;
            }
            // A statement has no result formats yet (they come with Bind), so its
            // RowDescription is text; ParameterDescription always precedes it.
            auto& statement = *it->second;
            packets.emplace_back(build_parameter_description(writer_, statement.specified_types));
            if (!do_describe(statement.schema, {result_encoding::TEXT}, packets, statement.described)) {
                return;
            }
        } else {
            auto it = portals_.find(name);
            if (it == portals_.end()) {
                send_error_response(sql_state::INVALID_NAME, "Portal " + name + " does not exist");
                return;
            }
            // A portal is a BOUND statement: every parameter has a value, so a
            // statement that answers rows can be described by the result it
            // actually answers — exactly, and at no extra cost, since the
            // portal keeps that result and its Execute streams it rather than
            // running the statement again. This is the only description that is
            // right for a parameterized statement: what a prepare could say
            // before Bind is the plan's projection, which carries no types.
            //
            // Only a row-producing statement is run here, and the statement
            // KIND decides it — not the columns of a result nobody has yet. An
            // INSERT / UPDATE / DELETE described this way would write, and
            // write a second time for a client that binds the portal again, so
            // a DML is described from the prepared schema and nothing runs. For
            // a DML with RETURNING that schema names the RETURNING columns,
            // resolved from the plan at prepare (query_session_t::prepare_schema) rather
            // than by executing anything; for one without it there are no
            // result columns and the answer is NoData.
            auto& portal = it->second;
            if (portal.statement->tag == T_SelectStmt) {
                if (!portal.result && !run_portal(portal)) {
                    return;
                }
                // types() answers a copy; the STRUCT is what do_describe reads
                // its columns from, exactly as for a prepared schema.
                const auto executed = portal.result->chunks.front().types();
                if (!do_describe(types::complex_logical_type::create_struct("", executed),
                                 portal.format,
                                 packets,
                                 portal.described)) {
                    return;
                }
            } else if (!do_describe(portal.statement->schema, portal.format, packets, portal.described)) {
                return;
            }
        }
        send_packet_merged(std::move(packets));
    }

    bool postgres_connection::do_describe(const types::complex_logical_type& schema,
                                          const std::vector<result_encoding>& format,
                                          std::vector<std::vector<uint8_t>>& packets,
                                          bool& described) {
        if (schema.type() != types::logical_type::STRUCT) {
            // No schema was resolved at prepare time. NoData is the honest answer
            // here; Execute then sends the RowDescription alongside the rows if it
            // turns out there are any.
            packets.emplace_back(build_no_data(writer_));
            described = false;
            return true;
        }

        // A column the requested formats cannot carry has no RowDescription:
        // the client gets the refusal now, not a dropped connection at Execute.
        if (auto bad = find_unsupported_column<frontend_type::POSTGRES>(schema.child_types(), format)) {
            send_error_response(sql_state::FEATURE_NOT_SUPPORTED,
                                unsupported_column_message<frontend_type::POSTGRES>(*bad));
            return false;
        }

        std::vector<field_description> field_desc;
        for (const auto& column : schema.child_types()) {
            auto wire_type = get_field_type(column.type());
            assert(wire_type.has_value() && "Describe: column types were checked above");
            // A column without a name (SELECT 1) has no alias in the prepared
            // schema, and alias() has no null guard for such a type; it goes out
            // with the empty name an executed result gives it.
            field_desc.emplace_back(column.has_alias() ? column.alias() : std::string{}, *wire_type);
        }

        packets.emplace_back(build_row_description(writer_, std::move(field_desc), format));
        described = true;
        return true;
    }

    void postgres_connection::finish_impl() {
        OTX_ZONE_N("pg::finish_impl");
        // The prepared statements live in the connection's session and go with
        // it. Closing the session never waits: a statement still running (the
        // client stopped waiting for it) finishes, then the session frees itself.
        portals_.clear();
        statement_name_map_.clear();
        session_.reset();
    }
} // namespace frontend::postgres
