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

        backend_secret_key_ = generate_backend_key(use_protocol_3_2_ ? SECRET_KEY_3_2_SIZE : SECRET_KEY_SIZE);
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

    void postgres_connection::handle_query(std::string query) {
        OTX_ZONE_N("pg::handle_query");
        session_id id;
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(scheduler_, &Scheduler::execute, id.hash(), query);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

        if (r.has_error()) {
            if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
                send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit");
            } else {
                // may be a transaction block, handle them separately
                // todo: psycopg2's PREPARE & EXECUTE
                try_handle_transaction(std::move(query), std::string{r.error().what.c_str()});
            }
            return;
        }

        // handle Ok
        session_payload sdata_result = std::move(r.value());
        if (sdata_result.tag == T_TransactionStmt) {
            // The engine runs BEGIN / COMMIT / ROLLBACK itself and answers
            // without a result; the transaction block ReadyForQuery reports is
            // kept here.
            try_handle_transaction(std::move(query), std::string{});
            return;
        }
        int64_t rows_cnt = sdata_result.size();
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
        response.emplace_back(
            build_command_complete(writer_, command_complete_tag::simple_command(sdata_result.tag, rows_cnt)));
        response.emplace_back(build_ready_for_query(writer_, transaction_man_.get_transaction_status()));
        send_packet_merged(std::move(response));
    }

    void postgres_connection::try_handle_transaction(std::string query, std::string error) {
        OTX_ZONE_N("pg::try_handle_transaction");
        // `error` is empty for a transaction statement the engine ran (BEGIN,
        // COMMIT, ROLLBACK); the savepoint statements reach here as the
        // engine's "Unsupported node type".
        if (error.empty() || error.find("Unsupported node type") != std::string::npos) {
            try {
                tracy_memory_resource arena_mr(resource_, "pg::parse_arena");
                std::pmr::monotonic_buffer_resource arena_resource(&arena_mr);
                auto res = linitial(raw_parser(&arena_resource, query.c_str()));
                if (nodeTag(res) == T_TransactionStmt) { // handle transactions
                    auto tr = transform::pg_ptr_cast<TransactionStmt>(res);
                    switch (tr->kind) {
                        case TRANS_STMT_START:
                        case TRANS_STMT_BEGIN:
                            send_packet_merged(transaction_man_.handle_begin(writer_));
                            return;
                        case TRANS_STMT_COMMIT:
                            portals_.clear(); // portals are released at transaction's end
                            send_packet_merged(transaction_man_.handle_commit(writer_));
                            return;
                        case TRANS_STMT_ROLLBACK:
                            portals_.clear();
                            send_packet_merged(transaction_man_.handle_rollback(writer_));
                            return;
                        case TRANS_STMT_SAVEPOINT: {
                            auto def = transform::pg_ptr_cast<DefElem>(linitial(tr->options));
                            send_packet_merged(
                                transaction_man_.handle_savepoint(writer_,
                                                                  strVal(transform::pg_ptr_cast<Value>(def->arg))));
                            return;
                        }
                        case TRANS_STMT_ROLLBACK_TO: {
                            auto def = transform::pg_ptr_cast<DefElem>(linitial(tr->options));
                            send_packet_merged(transaction_man_.handle_rollback_to_savepoint(
                                writer_,
                                strVal(transform::pg_ptr_cast<Value>(def->arg))));
                            return;
                        }
                        case TRANS_STMT_RELEASE: {
                            auto def = transform::pg_ptr_cast<DefElem>(linitial(tr->options));
                            send_packet_merged(transaction_man_.handle_release_savepoint(
                                writer_,
                                strVal(transform::pg_ptr_cast<Value>(def->arg))));
                            return;
                        }
                        case TRANS_STMT_PREPARE:
                        case TRANS_STMT_COMMIT_PREPARED:
                        case TRANS_STMT_ROLLBACK_PREPARED:
                            send_error_response(sql_state::PROTOCOL_VIOLATION, "Unable to prepare transaction");
                            return;
                    }
                }
            } catch (const std::exception& e) {
                // do nothing, first error message will be sent
            }
        }

        // The engine's own refusal; a statement the engine ran that the parse
        // above does not recognize as a transaction statement is named instead.
        send_error_response(sql_state::SYNTAX_ERROR,
                            error.empty() ? "Unrecognized transaction statement: " + query : std::move(error));
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

        session_id id;
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::prepare_schema, id.hash(), query);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

        if (r.has_error()) {
            if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
                send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit");
            } else {
                send_error_response(sql_state::SYNTAX_ERROR, "Syntax error: " + std::string{r.error().what.c_str()});
            }
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
        statement_name_map_.emplace(std::move(stmt),
                                    prepared_stmt_meta(resource_,
                                                       id.hash(),
                                                       std::move(query),
                                                       result.parameter_count,
                                                       std::move(result.schema),
                                                       std::move(specified_types),
                                                       result.tag));
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

        if (num_params != it->second.parameter_count) {
            send_error_response(sql_state::UNDEFINED_PARAMETER,
                                "Missing parameters in BIND to statement " + stmt + ": received " +
                                    std::to_string(num_params) + " out of required " +
                                    std::to_string(it->second.parameter_count));
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
                                        std::to_string(it->second.parameter_count));
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

            const field_type type = it->second.specified_types[i];
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
        it->second.portal_names.emplace_back(portal_name);
        portals_.erase(portal_name); // portals with identical name replace each other
        portals_.emplace(std::move(portal_name), portal_meta(std::move(params), it->second, std::move(out_format)));

        send_packet(build_bind_complete(writer_));
    }

    bool postgres_connection::reprepare_stmt(prepared_stmt_meta& stmt) {
        OTX_ZONE_N("pg::reprepare_stmt");
        session_id id;
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::prepare_schema, id.hash(), std::string{stmt.sql.c_str()});
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

        if (r.has_error()) {
            if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
                send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit");
            } else {
                send_error_response(sql_state::SYNTAX_ERROR, "Syntax error: " + std::string{r.error().what.c_str()});
            }
            return false;
        }

        if (r.value().parameter_count != stmt.parameter_count) {
            send_error_response(sql_state::UNDEFINED_PARAMETER,
                                "Prepared statement changed its parameter count on re-prepare: " +
                                    std::to_string(r.value().parameter_count) + " out of " +
                                    std::to_string(stmt.parameter_count));
            return false;
        }

        stmt.stmt_session = id.hash();
        stmt.consumed = false;
        return true;
    }

    bool postgres_connection::run_portal(portal_meta& portal) {
        OTX_ZONE_N("pg::run_portal");
        auto& stmt = portal.statement.get();
        if (stmt.consumed && !reprepare_stmt(stmt)) {
            return false;
        }
        // The worker drops the statement on every exit path of the execution.
        stmt.consumed = true;

        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(scheduler_,
                                                                    &Scheduler::execute_prepared_statement,
                                                                    stmt.stmt_session,
                                                                    std::move(portal.params));
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

        if (r.has_error()) {
            if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
                send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit");
            } else {
                send_error_response(sql_state::SYNTAX_ERROR, "Syntax error: " + std::string{r.error().what.c_str()});
            }
            return false;
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
        auto& stmt = portal.statement.get();
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
            rows_cnt = portal.emitted == 0 ? static_cast<int64_t>(payload.size()) : 0;
            portal.emitted = payload.size();
        }

        response.emplace_back(
            build_command_complete(writer_, command_complete_tag::simple_command(payload.tag, rows_cnt)));
        send_packet_merged(std::move(response));
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
                for (auto&& p : it->second.portal_names) {
                    do_close(describe_close_arg::PORTAL, std::move(p));
                }
                // The worker keeps an unexecuted statement until it is closed;
                // an executed one it has already dropped (consumed).
                if (!it->second.consumed) {
                    // sending to the Scheduler event-loop always returns needs_sched=false
                    [[maybe_unused]] auto [needs_sched, fut] =
                        actor_zeta::send(scheduler_, &Scheduler::close_statement, it->second.stmt_session);
                    auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
                    if (r.has_error()) {
                        log_->warn("[Connection {}] CLOSE stmt: \"{}\": close_statement failed: {}",
                                   connection_id_,
                                   name,
                                   r.error().what);
                    }
                }
                statement_name_map_.erase(it);
            }
        } else {
            portals_.erase(std::move(name));
        }
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
            packets.emplace_back(build_parameter_description(writer_, it->second.specified_types));
            if (!do_describe(it->second.schema, {result_encoding::TEXT}, packets, it->second.described)) {
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
            // resolved from the plan at prepare (Worker::prepare_schema) rather
            // than by executing anything; for one without it there are no
            // result columns and the answer is NoData.
            auto& portal = it->second;
            if (portal.statement.get().tag == T_SelectStmt) {
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
            } else if (!do_describe(portal.statement.get().schema, portal.format, packets, portal.described)) {
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
        // do_close releases an unexecuted statement on the Worker and forgets
        // the statement with its portals; the loop drains the map through it.
        while (!statement_name_map_.empty()) {
            do_close(describe_close_arg::STATEMENT, statement_name_map_.begin()->first);
        }
        portals_.clear();
    }
} // namespace frontend::postgres
