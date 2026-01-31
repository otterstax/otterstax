// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_connection.hpp"

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
        if (payload.size() < MIN_AUTH_PAYLOAD_SIZE) {
            log_->info("[Connection {}] AUTH: Payload too small", connection_id_);
            send_error(mysql_error::ER_ACCESS_DENIED_ERROR, "Access denied for user (using password: NO)");
            return;
        }

        packet_reader reader(std::move(payload));

        uint32_t client_flags = reader.read_uint32();
        log_->info("[Connection {}] AUTH: flags=0x{:x}", connection_id_, client_flags);

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
        client_max_packet_size_ = max_packet_size;

        uint8_t charset = reader.read_uint8();
        log_->info("[Connection {}] AUTH: charset={}", connection_id_, static_cast<int>(charset));

        if (reader.remaining() < AUTH_FILLER_SIZE) {
            send_error(mysql_error::ER_ACCESS_DENIED_ERROR, "Access denied - malformed auth packet");
            return;
        }

        reader.skip_bytes(AUTH_FILLER_SIZE); // filler
        log_->info("[Connection {}] AUTH: user='{}'", connection_id_, reader.read_string_null());

        std::vector<uint8_t> password;
        if (reader.remaining() > 0) {
            uint8_t auth_length = reader.read_uint8();
            log_->info("[Connection {}] AUTH: auth_len={}", connection_id_, static_cast<int>(auth_length));

            if (auth_length > 0 && auth_length <= reader.remaining()) {
                password.resize(auth_length);
                for (int i = 0; i < auth_length; ++i) {
                    password[i] = reader.read_uint8();
                }
            }
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

        // skip auth
        state_ = connection_state::COMMAND;
        log_->info("[Connection {}] AUTH: Success -> COMMAND state", connection_id_);
        send_packet(build_ok(writer_, sequence_id_));
    }

    void mysql_connection::handle_command(std::vector<uint8_t> payload) {
        if (payload.empty()) {
            send_error(mysql_error::ER_MALFORMED_PACKET, "Empty command packet");
            return;
        }

        auto cmd = static_cast<server_command>(payload[0]);
        switch (cmd) {
            case COM_QUIT:
                log_->info("[Connection {}] COM_QUIT", connection_id_);
                finish();
                break;
            case COM_PING: {
                log_->info("[Connection {}] COM_PING", connection_id_);
                send_packet(build_ok(writer_, sequence_id_));
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
                send_packet(build_ok(writer_, sequence_id_));
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
                    handle_execute_stmt(it_stmt->second.stmt_session, {});
                    break;
                }

                if (auto param_values =
                        handle_execute_params(stmt_id, num_params, it_stmt->second.param_types, std::move(reader));
                    !param_values.empty()) {
                    handle_execute_stmt(it_stmt->second.stmt_session, std::move(param_values));
                }
                break;
            }
            case COM_STMT_CLOSE: {
                packet_reader reader(std::move(payload));
                reader.read_uint8(); // skip [0x25] - COM_STMT_CLOSE
                uint32_t stmt_id = reader.read_uint32();
                statement_id_map_.erase(stmt_id);
                read_packet(); // nothing is sent to client, read next
                break;
            }
            case COM_STMT_RESET:
                // no-op
                send_packet(build_ok(writer_, sequence_id_));
                break;
            default:
                send_error(mysql_error::ER_UNKNOWN_COM_ERROR, "Unknown command: 0x" + std::to_string(cmd));
                break;
        }
    }

    void mysql_connection::handle_query(std::string query) {
        auto shared_data = create_cv_wrapper(flight_data(resource_));
        session_id id;
        // todo: one execute() call for simplicity - use computed schema for text_resultset columns
        actor_zeta::send(scheduler_->address(),
                         scheduler_->address(),
                         scheduler::handler_id(scheduler::route::execute),
                         id.hash(),
                         shared_data,
                         query);
        shared_data->wait_for(cv_wrapper::DEFAULT_TIMEOUT);

        switch (shared_data->status()) {
            case cv_wrapper::Status::Ok:
                if (!shared_data->result.chunk.empty()) {
                    break;
                }
                // fallthrough otherwise
            case cv_wrapper::Status::Empty:
                send_packet(build_ok(writer_, sequence_id_, 0));
                return;
            case cv_wrapper::Status::Timeout:
            case cv_wrapper::Status::Unknown:
                send_error(mysql_error::ER_QUERY_TIMEOUT, "Query exceeded execution limit");
                return;
            case cv_wrapper::Status::Error:
                // all connectors send "SET NAMES utf8mb4" and "SET AUTOCOMMIT=0" after auth, handle them separately
                try_fix_variable_set_query(query, shared_data->error_message());
                return;
        }

        // handle Ok
        // empty db & table in metadata (not critical, but may be improved)
        mysql_resultset result(writer_, result_encoding::TEXT);
        result.add_chunk_columns(shared_data->result.chunk);
        for (size_t i = 0; i < shared_data->result.chunk.size(); i++) {
            result.add_row(shared_data->result.chunk, i);
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
            std::pmr::monotonic_buffer_resource arena_resource(resource_);
            auto res = linitial(raw_parser(&arena_resource, fixed_query.c_str()));
            if (nodeTag(res) == T_VariableSetStmt) { // handle SET statements
                auto set = transform::pg_ptr_cast<VariableSetStmt>(res);
                if (std::string(set->name) == "client_encoding") {
                    std::string encoding(strVal(&transform::pg_ptr_cast<A_Const>(linitial(set->args))->val));
                    if (encoding == "utf8mb4" || encoding == "utf8mb3") {
                        send_packet(build_ok(writer_, sequence_id_, 0));
                    } else {
                        send_error(mysql_error::ER_HANDSHAKE_ERROR, "Only utf-8 encodings are supported");
                    }
                    return;
                }

                if (std::string(set->name) == "autocommit") {
                    // just ok, no transactions
                    send_packet(build_ok(writer_, sequence_id_, 0));
                    return;
                }
            }
        } catch (const std::exception& e) {
            // do nothing, first error message will be sent
        }
        send_error(mysql_error::ER_SYNTAX_ERROR, std::move(error));
    }

    void mysql_connection::handle_prepared_stmt(std::string query) {
        auto shared_data = create_cv_wrapper(flight_data(resource_));
        session_id id;
        actor_zeta::send(scheduler_->address(),
                         scheduler_->address(),
                         scheduler::handler_id(scheduler::route::prepare_schema),
                         id.hash(),
                         shared_data,
                         query);
        shared_data->wait_for(cv_wrapper::DEFAULT_TIMEOUT);

        switch (shared_data->status()) {
            case cv_wrapper::Status::Ok:
            case cv_wrapper::Status::Empty:
                break;
            case cv_wrapper::Status::Timeout:
            case cv_wrapper::Status::Unknown:
                send_error(mysql_error::ER_QUERY_TIMEOUT, "Query exceeded execution limit");
                return;
            case cv_wrapper::Status::Error:
                // ? case - postgres will not allow
                try_fix_prepared_stmt(query, shared_data->error_message());
                return;
        }

        auto& result = shared_data->result;
        std::vector<std::vector<uint8_t>> packets;

        uint16_t column_cnt = result.schema != types::logical_type::NA ? result.schema.child_types().size() : 0;
        packets.reserve(4 + column_cnt + result.parameter_count);
        log_->info("[Connection {}] COM_STMT_PREPARE: id={} column_cnt={} param_cnt={}",
                   connection_id_,
                   next_statement_id_,
                   column_cnt,
                   result.parameter_count);
        packets.push_back(
            build_stmt_prepare_ok(writer_, sequence_id_++, next_statement_id_, column_cnt, result.parameter_count));
        statement_id_map_.emplace(next_statement_id_++,
                                  prepared_stmt_meta(resource_, id.hash(), result.parameter_count));

        // params
        if (result.parameter_count) {
            for (size_t i = 0; i < result.parameter_count; ++i) {
                column_definition_41 param("?", field_type::MYSQL_TYPE_STRING);
                packets.push_back(column_definition_41::write_packet(std::move(param), writer_, sequence_id_++));
            }
            packets.push_back(build_eof(writer_, sequence_id_++));
        }

        // columns
        if (column_cnt) {
            for (auto& column : result.schema.child_types()) {
                column_definition_41 col(column.alias(), get_field_type(column.type()));
                packets.push_back(column_definition_41::write_packet(std::move(col), writer_, sequence_id_++));
            }
            packets.push_back(build_eof(writer_, sequence_id_++));
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
                param_values.emplace_back(nullptr);
                continue;
            }

            uint16_t raw_type = param_types[i];
            uint8_t type = raw_type & 0xFF;
            bool is_unsigned = (raw_type & 0x8000) != 0;
            switch (static_cast<field_type>(type)) {
                case field_type::MYSQL_TYPE_TINY: {
                    auto val = reader.read_uint8();
                    if (is_unsigned) {
                        param_values.emplace_back(val);
                    } else {
                        param_values.emplace_back(static_cast<int8_t>(val));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_SHORT: {
                    auto val = reader.read_uint16();
                    if (is_unsigned) {
                        param_values.emplace_back(val);
                    } else {
                        param_values.emplace_back(static_cast<int16_t>(val));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_LONG: {
                    auto val = reader.read_uint32();
                    if (is_unsigned) {
                        param_values.emplace_back(val);
                    } else {
                        param_values.emplace_back(static_cast<int32_t>(val));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_LONGLONG: {
                    auto val = reader.read_uint64();
                    if (is_unsigned) {
                        param_values.emplace_back(val);
                    } else {
                        param_values.emplace_back(static_cast<int64_t>(val));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_FLOAT: {
                    auto f = std::bit_cast<float>(reader.read_uint32());
                    param_values.emplace_back(f);
                    break;
                }
                case field_type::MYSQL_TYPE_DOUBLE: {
                    auto d = std::bit_cast<double>(reader.read_uint64());
                    param_values.emplace_back(d);
                    break;
                }
                case field_type::MYSQL_TYPE_VAR_STRING:
                case field_type::MYSQL_TYPE_STRING:
                case field_type::MYSQL_TYPE_BLOB: {
                    std::string s = reader.read_length_encoded_string();
                    param_values.emplace_back(std::move(s));
                    break;
                }
                default:
                    send_error(mysql_error::ER_SYNTAX_ERROR, "Unsupported parameter type " + std::to_string(type));
                    return {};
            }
        }

        return param_values;
    }

    void mysql_connection::handle_execute_stmt(session_hash_t id,
                                               std::pmr::vector<types::logical_value_t> param_values) {
        auto shared_data = create_cv_wrapper(flight_data(resource_));
        actor_zeta::send(scheduler_->address(),
                         scheduler_->address(),
                         scheduler::handler_id(scheduler::route::execute_prepared_statement),
                         id,
                         std::move(param_values),
                         shared_data);
        shared_data->wait_for(cv_wrapper::DEFAULT_TIMEOUT);

        switch (shared_data->status()) {
            case cv_wrapper::Status::Ok:
                if (!shared_data->result.chunk.empty()) {
                    break;
                }
                // fallthrough otherwise
            case cv_wrapper::Status::Empty:
                send_packet(build_ok(writer_, sequence_id_, 0));
                return;
            case cv_wrapper::Status::Timeout:
            case cv_wrapper::Status::Unknown:
                send_error(mysql_error::ER_QUERY_TIMEOUT, "Query exceeded execution limit");
                return;
            case cv_wrapper::Status::Error:
                send_error(mysql_error::ER_SYNTAX_ERROR, shared_data->error_message());
                return;
        }

        mysql_resultset result(writer_, result_encoding::BINARY);
        result.add_chunk_columns(shared_data->result.chunk);
        for (size_t i = 0; i < shared_data->result.chunk.size(); i++) {
            result.add_row(shared_data->result.chunk, i);
        }

        send_resultset(std::move(result));
    }

    void mysql_connection::send_resultset(mysql_resultset&& result) {
        send_packet_merged(mysql_resultset::build_packets(std::move(result), sequence_id_));
    }
} // namespace frontend::mysql