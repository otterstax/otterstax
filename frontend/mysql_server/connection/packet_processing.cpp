// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_connection.hpp"

#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

using namespace components;
using namespace components::sql;

namespace {
    // see https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_binary_resultset.html
    constexpr size_t null_bitmap_size(size_t column_count) { return (column_count + 7) / 8; }
} // namespace

namespace frontend::mysql {
    constexpr uint8_t MIN_AUTH_PAYLOAD_SIZE = 32;
    constexpr uint8_t AUTH_FILLER_SIZE = 23;

    void mysql_connection::handle_auth(std::vector<uint8_t> payload) {
        OTX_ZONE_N("mysql::handle_auth");
        if (payload.size() < MIN_AUTH_PAYLOAD_SIZE) {
            reject_authentication();
            return;
        }

        const auto payload_size = payload.size();
        packet_reader reader(std::move(payload));

        uint32_t client_flags = reader.read_uint32();
        if ((client_flags & CLIENT_SSL) && !encrypted()) {
            if (payload_size != MIN_AUTH_PAYLOAD_SIZE || !tls_available()) { reject_authentication(); return; }
            start_tls([this] { read_packet(); });
            return;
        }
        if (authentication_required() && (!encrypted() || !(client_flags & CLIENT_PLUGIN_AUTH))) {
            reject_authentication();
            return;
        }

        if (!(client_flags & CLIENT_PROTOCOL_41)) {
            send_error(mysql_error::ER_NOT_SUPPORTED_AUTH_MODE,
                       "Client does not support authentication protocol requested by server");
            return;
        }

        uint32_t max_packet_size = reader.read_uint32();
        //            if (max_packet_size > MAX_PACKET_SIZE) {
        //                std::cerr << "[Connection " << connection_id_ << "] AUTH: client max packet size is" << max_packet_size
        //                          << " while max allowed is 0xFFFFFF" << std::endl;
        //                send_error(mysql_error::ER_ACCESS_DENIED_ERROR, "max_packet_size exceeds maximum allowed (0xFFFFFF)");
        //                return;
        //            }
        client_max_packet_size_ = max_packet_size == 0 ? DEFAULT_MAX_PACKET_SIZE : max_packet_size;

        uint8_t charset = reader.read_uint8();
        log_->info("[Connection {}] AUTH: charset={}", connection_id_, static_cast<int>(charset));

        if (reader.remaining() < AUTH_FILLER_SIZE) {
            send_error(mysql_error::ER_ACCESS_DENIED_ERROR, "Access denied - malformed auth packet");
            return;
        }

        reader.skip_bytes(AUTH_FILLER_SIZE); // filler
        reader.read_string_null(); // User identity comes from the verified token.
        if (client_flags & (CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_SECURE_CONNECTION)) {
            const auto auth_length = (client_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                ? reader.read_length_encoded_integer() : reader.read_uint8();
            if (!reader.ok() || auth_length > reader.remaining()) {
                send_malformed_packet_error("Malformed HandshakeResponse authentication data");
                return;
            }
            reader.skip_bytes(static_cast<size_t>(auth_length));
        } else {
            reader.read_string_null();
        }

        std::string requested_database;
        if ((client_flags & CLIENT_CONNECT_WITH_DB) && reader.remaining() > 0) {
            // todo: support
            requested_database = reader.read_string_null();
            log_->info("[Connection {}] AUTH: database='{}'", connection_id_, requested_database);
        }

        std::string auth_plugin;
        if ((client_flags & CLIENT_PLUGIN_AUTH) && reader.remaining() > 0) {
            auth_plugin = reader.read_string_null();
            log_->info("[Connection {}] AUTH: plugin='{}'", connection_id_, auth_plugin);
        }

        // A string above without its terminator: none of the values read is data.
        if (!reader.ok()) {
            send_malformed_packet_error("Malformed HandshakeResponse packet");
            return;
        }

        if (authentication_required()) {
            writer_.reserve_payload(32);
            writer_.write_uint8(0xfe); // AuthSwitchRequest
            writer_.write_string_null("mysql_clear_password");
            writer_.write_uint8(0); // No challenge data for bearer tokens.
            state_ = connection_state::AUTH_TOKEN;
            send_packet(writer_.build_from_payload(sequence_id_++));
            return;
        }
        state_ = connection_state::COMMAND;
        log_->info("[Connection {}] AUTH: Success -> COMMAND state", connection_id_);
        send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
    }

    void mysql_connection::reject_authentication() {
        send_then(build_error(writer_, sequence_id_++, mysql_error::ER_ACCESS_DENIED_ERROR,
                              "A valid bearer access token over TLS is required"), [this] { finish(); });
    }

    void mysql_connection::handle_auth_token(std::vector<uint8_t> payload) {
        packet_reader reader(std::move(payload));
        auto token = reader.read_string_null();
        if (!reader.ok() || reader.remaining() != 0 ||
            verify_token(token).status != otterstax::security::token_status::accepted) {
            reject_authentication();
            return;
        }
        access_token_ = std::move(token);
        state_ = connection_state::COMMAND;
        send_packet(build_ok(writer_, sequence_id_++, 0, status_flags()));
    }

    void mysql_connection::handle_command(std::vector<uint8_t> payload) {
        OTX_ZONE_N("mysql::handle_command");
        if (payload.empty()) {
            send_error(mysql_error::ER_MALFORMED_PACKET, "Empty command packet");
            return;
        }

        auto cmd = static_cast<server_command>(payload[0]);
        if (cmd != COM_QUIT && authentication_required() &&
            verify_token(access_token_).status != otterstax::security::token_status::accepted) {
            reject_authentication();
            return;
        }
        switch (cmd) {
            case COM_QUIT:
                log_->info("[Connection {}] COM_QUIT", connection_id_);
                finish();
                break;
            case COM_PING: {
                log_->info("[Connection {}] COM_PING", connection_id_);
                send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
                break;
            }
            case COM_INIT_DB: {
                // todo: support
                if (payload.size() < 2) {
                    send_error(mysql_error::ER_MALFORMED_PACKET, "Malformed COM_INIT_DB packet");
                    break;
                }

                std::string db_name(std::make_move_iterator(payload.begin() + 1),
                                    std::make_move_iterator(payload.end()));
                log_->info("[Connection {}] COM_INIT_DB: '{}'", connection_id_, std::move(db_name));
                send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
                break;
            }
            case COM_STMT_PREPARE: // fall-through
            case COM_QUERY: {
                if (payload.size() < 2) {
                    send_error(mysql_error::ER_MALFORMED_PACKET, "Malformed COM_QUERY packet");
                    break;
                }

                std::string query(std::make_move_iterator(payload.begin() + 1), std::make_move_iterator(payload.end()));
                log_->info("[Connection {}] {}: '{}'",
                           connection_id_,
                           (cmd == COM_QUERY ? "COM_QUERY" : "COM_STMT_PREPARE"),
                           query);

                if (query.find_first_not_of(" \t\r\n") == std::string::npos) {
                    send_error(mysql_error::ER_EMPTY_QUERY, "Query was empty");
                    break;
                }

                cmd == COM_QUERY ? handle_query(std::move(query)) : handle_prepared_stmt(std::move(query));
                break;
            }
            case COM_STMT_EXECUTE: {
                packet_reader reader(std::move(payload));
                reader.read_uint8(); // skip [0x17] - COM_STMT_EXECUTE
                uint32_t stmt_id = reader.read_uint32();
                uint8_t flags = reader.read_uint8();
                reader.read_uint32(); // iteration count - always 1, ignore
                if (!reader.ok()) {
                    send_malformed_packet_error("Malformed COM_STMT_EXECUTE packet");
                    break;
                }

                auto it_stmt = statement_id_map_.find(stmt_id);
                if (it_stmt == statement_id_map_.end()) {
                    send_error(mysql_error::ER_UNKNOWN_STMT_HANDLER, "Unknown statement id " + std::to_string(stmt_id));
                    break;
                }

                size_t num_params = it_stmt->second.parameter_count;
                log_->info("[Connection {}] COM_STMT_EXECUTE stmt_id={} flags={} num_params={}",
                           connection_id_,
                           stmt_id,
                           static_cast<int>(flags),
                           num_params);

                if (num_params == 0) {
                    handle_execute_stmt(it_stmt->second, std::pmr::vector<types::logical_value_t>{resource_});
                    break;
                }

                if (auto param_values =
                        handle_execute_params(stmt_id, num_params, it_stmt->second.param_types, std::move(reader));
                    !param_values.empty()) {
                    handle_execute_stmt(it_stmt->second, std::move(param_values));
                }
                break;
            }
            case COM_STMT_CLOSE: {
                packet_reader reader(std::move(payload));
                reader.read_uint8(); // skip [0x25] - COM_STMT_CLOSE
                uint32_t stmt_id = reader.read_uint32();
                if (!reader.ok()) {
                    // The command has no response by protocol; the ERR packet
                    // stands for the close that follows it.
                    send_malformed_packet_error("Malformed COM_STMT_CLOSE packet");
                    break;
                }
                if (auto it = statement_id_map_.find(stmt_id); it != statement_id_map_.end()) {
                    // The worker keeps an unexecuted statement until it is closed;
                    // an executed one it has already dropped (consumed).
                    if (!it->second.consumed) {
                        close_stmt_on_worker(it->second.stmt_session, "COM_STMT_CLOSE");
                    }
                    statement_id_map_.erase(it);
                }
                read_packet(); // nothing is sent to client, read next
                break;
            }
            case COM_STMT_RESET:
                // no-op
                send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
                break;
            default:
                send_error(mysql_error::ER_UNKNOWN_COM_ERROR, "Unknown command: 0x" + std::to_string(cmd));
                break;
        }
    }

    void mysql_connection::handle_query(std::string query) {
        OTX_ZONE_N("mysql::handle_query");
        const auto id = next_request_id();
        // todo: one execute() call for simplicity - use computed schema for text_resultset columns
        auto r = sql_session_.execute(id, query);

        if (r.has_error()) {
            if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
                send_error(mysql_error::ER_QUERY_TIMEOUT, "Query exceeded execution limit");
            } else {
                // all connectors send "SET NAMES utf8mb4" and "SET AUTOCOMMIT=0" after auth, handle them separately
                try_fix_variable_set_query(query, std::string{r.error().what.c_str()});
            }
            return;
        }

        // handle Ok
        session_payload sdata_result = std::move(r.value());
        // empty db & table in metadata (not critical, but may be improved)
        if (sdata_result.column_count() == 0) {
            send_packet(build_ok(writer_, sequence_id_, sdata_result.size(), status_flags()));
            return;
        }

        // A column the text resultset cannot encode is an error packet, not a
        // dropped connection.
        if (auto bad = find_unsupported_column<frontend_type::MYSQL>(sdata_result.chunks.front(),
                                                                    {result_encoding::TEXT})) {
            send_error(mysql_error::ER_NOT_SUPPORTED_YET, unsupported_column_message<frontend_type::MYSQL>(*bad));
            return;
        }

        mysql_resultset result(writer_, result_encoding::TEXT);
        result.add_chunk_columns(sdata_result.chunks.front());
        for (auto& ch : sdata_result.chunks) {
            for (size_t i = 0; i < ch.size(); ++i) {
                result.add_row(ch, i);
            }
        }

        send_resultset(std::move(result));
    }

    void mysql_connection::try_fix_variable_set_query(std::string_view query, std::string error) {
        std::string fixed_query(query);

        if (error.find("syntax") != std::string::npos) {
            // currently, SET NAMES ... queries from both connectors are producing syntax errors, try fixing them here
            // in postgres "SET NAMES utf8mb4" is a syntax error ("SET NAMES 'utf8mb4'" is correct)
            // the rest after encoding is being cut because postgres grammar rejects it
            static std::string_view set_names = "set names ";

            std::transform(fixed_query.begin(), fixed_query.end(), fixed_query.begin(), ::tolower);
            auto pos = fixed_query.find(set_names);
            if (pos != std::string::npos) {
                auto start = pos + set_names.size();
                auto end = std::min(query.find_first_of(" \t\n\r;", start), query.size());

                std::string_view encoding = query.substr(start, end - start);
                if ((encoding.front() == '\'' || encoding.front() == '"') && encoding.front() == encoding.back()) {
                    encoding = encoding.substr(1, encoding.size() - 2);
                }

                fixed_query = std::string(set_names) + "'" + std::string(encoding) + "'";
            } else {
                send_error(mysql_error::ER_SYNTAX_ERROR, std::move(error));
                return;
            }
        }

        try {
            tracy_memory_resource arena_mr(resource_, "mysql::parse_arena");
            std::pmr::monotonic_buffer_resource arena_resource(&arena_mr);
            auto res = linitial(raw_parser(&arena_resource, fixed_query.c_str()));
            if (nodeTag(res) == T_VariableSetStmt) { // handle SET statements
                auto set = transform::pg_ptr_cast<VariableSetStmt>(res);
                if (std::string(set->name) == "client_encoding") {
                    std::string encoding(strVal(&transform::pg_ptr_cast<A_Const>(linitial(set->args))->val));
                    if (encoding == "utf8mb4" || encoding == "utf8mb3") {
                        send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
                    } else {
                        send_error(mysql_error::ER_HANDSHAKE_ERROR, "Only utf-8 encodings are supported");
                    }
                    return;
                }


            }
        } catch (const std::exception& e) {
            // do nothing, first error message will be sent
        }
        send_error(mysql_error::ER_SYNTAX_ERROR, std::move(error));
    }

    void mysql_connection::handle_prepared_stmt(std::string query) {
        OTX_ZONE_N("mysql::handle_prepared_stmt");
        const auto id = next_request_id();
        auto r = sql_session_.prepare(id, query);

        if (r.has_error()) {
            if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
                send_error(mysql_error::ER_QUERY_TIMEOUT, "Query exceeded execution limit");
            } else {
                // ? case - postgres will not allow
                try_fix_prepared_stmt(query, std::string{r.error().what.c_str()});
            }
            return;
        }

        session_payload result = std::move(r.value());
        std::vector<std::vector<uint8_t>> packets;

        uint16_t column_cnt = result.schema != types::logical_type::NA ? result.schema.child_types().size() : 0;

        // COM_STMT_EXECUTE answers a binary resultset: a column the binary
        // encoder cannot carry is refused here, and the statement the worker
        // has just stored is released — no execute will ever consume it.
        if (column_cnt) {
            if (auto bad = find_unsupported_column<frontend_type::MYSQL>(result.schema.child_types(),
                                                                        {result_encoding::BINARY})) {
                close_stmt_on_worker(id, "COM_STMT_PREPARE refused");
                send_error(mysql_error::ER_NOT_SUPPORTED_YET,
                           unsupported_column_message<frontend_type::MYSQL>(*bad));
                return;
            }
        }

        packets.reserve(4 + column_cnt + result.parameter_count);
        log_->info("[Connection {}] COM_STMT_PREPARE: id={} column_cnt={} param_cnt={}",
                   connection_id_,
                   next_statement_id_,
                   column_cnt,
                   result.parameter_count);
        packets.push_back(
            build_stmt_prepare_ok(writer_, sequence_id_++, next_statement_id_, column_cnt, result.parameter_count));
        // `query` is the text the worker accepted (a `?` statement arrives here
        // already rewritten to `$n`), so it is the text a re-prepare must use.
        statement_id_map_.emplace(next_statement_id_++,
                                  prepared_stmt_meta(resource_, id, std::move(query), result.parameter_count));

        // params
        if (result.parameter_count) {
            for (size_t i = 0; i < result.parameter_count; ++i) {
                column_definition_41 param("?", field_type::MYSQL_TYPE_STRING);
                packets.push_back(column_definition_41::write_packet(std::move(param), writer_, sequence_id_++));
            }
            packets.push_back(build_eof(writer_, sequence_id_++, 0, status_flags()));
        }

        // columns
        if (column_cnt) {
            for (auto& column : result.schema.child_types()) {
                auto wire_type = get_field_type(column.type());
                assert(wire_type.has_value() && "COM_STMT_PREPARE: column types were checked above");
                // A column without a name (SELECT 1) has no alias in the prepared
                // schema, and alias() has no null guard for such a type; it goes
                // out with the empty name an executed result gives it.
                column_definition_41 col(column.has_alias() ? column.alias() : std::string{}, *wire_type);
                // A DECIMAL column announces its own scale and display length.
                apply_decimal_metadata(col, column);
                packets.push_back(column_definition_41::write_packet(std::move(col), writer_, sequence_id_++));
            }
            packets.push_back(build_eof(writer_, sequence_id_++, 0, status_flags()));
        }

        send_packet_sequence(std::move(packets), 0);
    }

    void mysql_connection::try_fix_prepared_stmt(std::string_view query, std::string error) {
        if (error.find("syntax") == std::string::npos) {
            send_error(mysql_error::ER_UNKNOWN_ERROR, std::move(error));
            return;
        }

        bool single_quoted = false, double_quoted = false;
        uint32_t next_param_idx = 1;
        std::string result;
        result.reserve(query.size() + 16);

        for (size_t i = 0; i < query.size(); ++i) {
            char c = query[i];
            if (c == '\'' && (i == 0 || query[i - 1] != '\\')) {
                single_quoted = !single_quoted;
            } else if (c == '"' && (i == 0 || query[i - 1] != '\\')) {
                double_quoted = !double_quoted;
            }

            if (!single_quoted && !double_quoted && c == '?') {
                result.push_back('$');
                result.append(std::to_string(next_param_idx++));
            } else {
                result.push_back(c);
            }
        }

        if (next_param_idx > 1) {
            handle_prepared_stmt(std::move(result));
        } else {
            send_error(mysql_error::ER_SYNTAX_ERROR, std::move(error));
        }
    }

    std::pmr::vector<components::types::logical_value_t>
    mysql_connection::handle_execute_params(uint32_t stmt_id,
                                            size_t num_params,
                                            std::pmr::vector<uint16_t>& param_types,
                                            packet_reader&& reader) {
        std::vector<uint8_t> null_bitmap(null_bitmap_size(num_params));
        for (auto& b : null_bitmap) {
            b = reader.read_uint8();
        }

        // WARN: MariaDB does not have such flag! in case of MariaDB parameters will follow immediately
        // todo: identify session type (if possible)
        uint8_t new_params_bound_flag = reader.read_uint8();
        if (new_params_bound_flag) {
            param_types.clear();
            param_types.reserve(num_params);
            for (size_t i = 0; i < num_params; ++i) {
                uint16_t t = reader.read_uint16();
                param_types.push_back(t);
            }
        }

        // Checked before the types are used: a type read past the end is 0
        // (MYSQL_TYPE_DECIMAL), which would be refused as unsupported instead.
        if (!reader.ok()) {
            param_types.clear();
            send_malformed_packet_error("Malformed COM_STMT_EXECUTE packet: parameter types");
            return {};
        }

        if (param_types.size() != num_params) {
            send_error(mysql_error::ER_UNKNOWN_STMT_HANDLER,
                       "Missing parameter types for statement with id=" + std::to_string(stmt_id) + ": " +
                           std::to_string(param_types.size()) + " parameters passed out of " +
                           std::to_string(num_params));
            return {};
        }

        // values are always sent if num_params > 0, regardless of new_params_bound_flag
        std::pmr::vector<types::logical_value_t> param_values(resource_);
        for (size_t i = 0; i < num_params; ++i) {
            bool is_null = (null_bitmap[i / 8] >> (i % 8)) & 1;
            if (is_null) {
                param_values.emplace_back(resource_, nullptr);
                continue;
            }

            uint16_t raw_type = param_types[i];
            uint8_t type = raw_type & 0xFF;
            bool is_unsigned = (raw_type & 0x8000) != 0;
            switch (static_cast<field_type>(type)) {
                case field_type::MYSQL_TYPE_TINY: {
                    auto val = reader.read_uint8();
                    if (is_unsigned) {
                        param_values.emplace_back(resource_, val);
                    } else {
                        param_values.emplace_back(resource_, static_cast<int8_t>(val));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_SHORT: {
                    auto val = reader.read_uint16();
                    if (is_unsigned) {
                        param_values.emplace_back(resource_, val);
                    } else {
                        param_values.emplace_back(resource_, static_cast<int16_t>(val));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_LONG: {
                    auto val = reader.read_uint32();
                    if (is_unsigned) {
                        param_values.emplace_back(resource_, val);
                    } else {
                        param_values.emplace_back(resource_, static_cast<int32_t>(val));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_LONGLONG: {
                    auto val = reader.read_uint64();
                    if (is_unsigned) {
                        param_values.emplace_back(resource_, val);
                    } else {
                        param_values.emplace_back(resource_, static_cast<int64_t>(val));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_FLOAT: {
                    auto f = std::bit_cast<float>(reader.read_uint32());
                    param_values.emplace_back(resource_, f);
                    break;
                }
                case field_type::MYSQL_TYPE_DOUBLE: {
                    auto d = std::bit_cast<double>(reader.read_uint64());
                    param_values.emplace_back(resource_, d);
                    break;
                }
                case field_type::MYSQL_TYPE_VAR_STRING:
                case field_type::MYSQL_TYPE_STRING:
                case field_type::MYSQL_TYPE_BLOB: {
                    std::string s = reader.read_length_encoded_string();
                    param_values.emplace_back(resource_, std::move(s));
                    break;
                }
                default:
                    send_error(mysql_error::ER_SYNTAX_ERROR, "Unsupported parameter type " + std::to_string(type));
                    return {};
            }
        }

        if (!reader.ok()) {
            send_malformed_packet_error("Malformed COM_STMT_EXECUTE packet: parameter values");
            return {};
        }

        return param_values;
    }

    bool mysql_connection::reprepare_stmt(prepared_stmt_meta& stmt) {
        OTX_ZONE_N("mysql::reprepare_stmt");
        const auto id = next_request_id();
        auto r = sql_session_.prepare(id, std::string{stmt.sql.c_str()});

        if (r.has_error()) {
            if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
                send_error(mysql_error::ER_QUERY_TIMEOUT, "Query exceeded execution limit");
            } else {
                send_error(mysql_error::ER_UNKNOWN_ERROR, std::string{r.error().what.c_str()});
            }
            return false;
        }

        if (r.value().parameter_count != stmt.parameter_count) {
            send_error(mysql_error::ER_UNKNOWN_STMT_HANDLER,
                       "Prepared statement changed its parameter count on re-prepare: " +
                           std::to_string(r.value().parameter_count) + " out of " +
                           std::to_string(stmt.parameter_count));
            return false;
        }

        stmt.stmt_session = id;
        stmt.consumed = false;
        return true;
    }

    void mysql_connection::close_stmt_on_worker(session_hash_t stmt_session, const char* reason) {
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::close_statement, stmt_session);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            log_->warn("[Connection {}] {}: close_statement failed: {}", connection_id_, reason, r.error().what);
        }
    }

    void mysql_connection::finish_impl() {
        auto rollback = sql_session_.close();
        if (rollback.has_error()) {
            log_->error("[Connection {}] Transaction cleanup failed: {}", connection_id_, rollback.error().what);
        }
        OTX_ZONE_N("mysql::finish_impl");
        // The worker keeps an unexecuted statement until it is closed; an
        // executed one it has already dropped (consumed).
        for (const auto& entry : statement_id_map_) {
            if (!entry.second.consumed) {
                close_stmt_on_worker(entry.second.stmt_session, "connection finish");
            }
        }
        statement_id_map_.clear();
    }

    void mysql_connection::handle_execute_stmt(prepared_stmt_meta& stmt,
                                               std::pmr::vector<types::logical_value_t> param_values) {
        OTX_ZONE_N("mysql::handle_execute_stmt");
        if (stmt.consumed && !reprepare_stmt(stmt)) {
            return;
        }
        // The worker drops the statement on every exit path of the execution.
        stmt.consumed = true;

        auto r = sql_session_.execute_prepared(stmt.stmt_session, std::string{stmt.sql.c_str()},
                                                   std::move(param_values));

        if (r.has_error()) {
            if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
                send_error(mysql_error::ER_QUERY_TIMEOUT, "Query exceeded execution limit");
            } else {
                send_error(mysql_error::ER_SYNTAX_ERROR, std::string{r.error().what.c_str()});
            }
            return;
        }

        session_payload sdata_result = std::move(r.value());

        if (sdata_result.column_count() == 0) {
            send_packet(build_ok(writer_, sequence_id_, sdata_result.size(), status_flags()));
            return;
        }

        // The executed columns may differ from the prepared schema (a
        // parameterized statement has none at prepare time): checked again.
        if (auto bad = find_unsupported_column<frontend_type::MYSQL>(sdata_result.chunks.front(),
                                                                    {result_encoding::BINARY})) {
            send_error(mysql_error::ER_NOT_SUPPORTED_YET, unsupported_column_message<frontend_type::MYSQL>(*bad));
            return;
        }

        mysql_resultset result(writer_, result_encoding::BINARY);
        result.add_chunk_columns(sdata_result.chunks.front());
        for (auto& ch : sdata_result.chunks) {
            for (size_t i = 0; i < ch.size(); ++i) {
                result.add_row(ch, i);
            }
        }

        send_resultset(std::move(result));
    }

    server_status_flags_t mysql_connection::status_flags() const {
        server_status_flags_t flags = sql_session_.autocommit()
                                         ? static_cast<uint16_t>(server_status::SERVER_STATUS_AUTOCOMMIT) : 0;
        if (sql_session_.status() != frontend::sql_session::state::idle) {
            flags |= static_cast<uint16_t>(server_status::SERVER_STATUS_IN_TRANS);
        }
        return flags;
    }

    void mysql_connection::send_resultset(mysql_resultset&& result) {
        send_packet_merged(mysql_resultset::build_packets(std::move(result), sequence_id_, status_flags()));
    }
} // namespace frontend::mysql
