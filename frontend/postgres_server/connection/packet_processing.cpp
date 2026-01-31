// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "postgres_connection.hpp"

using namespace components;
using namespace components::sql;

namespace frontend::postgres {
    constexpr size_t INITIAL_MESSAGES_COUNT = 1 + 6 + 1 + 1; // auth_ok + 6 param + backend_key + ready_for_query
    constexpr size_t SECRET_KEY_3_2_SIZE = 32;
    constexpr size_t SECRET_KEY_SIZE = 4;

    void postgres_connection::handle_startup_message(packet_reader& reader) {
        log_->info("[Connection {}]: Client protocol version: {}", connection_id_, reader.read_int32());
        while (reader.remaining()) {
            std::string key = reader.read_string_null();

            if (key.empty()) {
                break;
            }

            if (!reader.remaining()) {
                send_error_response(sql_state::PROTOCOL_VIOLATION,
                                    "Malformed StartupMessage: key without value",
                                    error_severity::fatal());
                return;
            }

            std::string value = reader.read_string_null();

            if (key == "_pq_.protocol_extensions") {
                if (value.find("variable_length_keys") != std::string::npos ||
                    value.find("protocol_3_2") != std::string::npos) {
                    use_protocol_3_2_ = true;
                    log_->info("[Connection {}]: Postgres client supports Protocol 3.2+", connection_id_);
                }
            }
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
        std::vector<uint8_t> negative(1);
        negative[0] = 'N';

        log_->info("[Connection {}] Sent SSL decline ('N'), waiting for StartupMessage", connection_id_);
        send_packet(std::move(negative), false);
        read_initial_message();
    }

    void postgres_connection::handle_query(std::string query) {
        auto shared_data = create_cv_wrapper(flight_data(resource_));
        session_id id;
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
                send_packet_merged(
                    {build_command_complete(writer_, command_complete_tag::simple_command(shared_data->result.tag)),
                     build_ready_for_query(writer_, transaction_man_.get_transaction_status())});
                return;
            case cv_wrapper::Status::Timeout:
            case cv_wrapper::Status::Unknown:
                send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit");
                return;
            case cv_wrapper::Status::Error:
                // may be a transaction block, handle them separately
                // todo: psycopg2's PREPARE & EXECUTE
                try_handle_transaction(std::move(query), shared_data->error_message());
                return;
        }

        // handle Ok
        int32_t rows_cnt = shared_data->result.chunk.size();
        postgres_resultset result(writer_);
        result.add_chunk_columns(shared_data->result.chunk); // default text encoding
        for (size_t i = 0; i < rows_cnt; i++) {
            result.add_row(shared_data->result.chunk, i);
        }

        auto response = postgres_resultset::build_packets(std::move(result));
        response.emplace_back(build_command_complete(writer_, command_complete_tag::select(rows_cnt)));
        response.emplace_back(build_ready_for_query(writer_, transaction_man_.get_transaction_status()));
        send_packet_merged(std::move(response));
    }

    void postgres_connection::try_handle_transaction(std::string query, std::string error) {
        if (error.find("Unsupported node type") != std::string::npos) {
            try {
                std::pmr::monotonic_buffer_resource arena_resource(resource_);
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

        send_error_response(sql_state::SYNTAX_ERROR, std::move(error));
    }

    void
    postgres_connection::handle_parse(std::string stmt, std::string query, int16_t num_params, packet_reader&& reader) {
        if (pipeline_.has_error()) {
            log_->error("[Connection {}] PARSE stmt: \"{}\", query: \"{}\" IGNORED DUE TO PIPELINE ERROR, reading next "
                        "packet...",
                        connection_id_,
                        stmt,
                        query);
            read_packet();
            return;
        }

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
                send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit");
                return;
            case cv_wrapper::Status::Error:
                send_error_response(sql_state::SYNTAX_ERROR, "Syntax error: " + shared_data->error_message());
                return;
        }

        auto& result = shared_data->result;
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

        statement_name_map_.erase(stmt); // statements with identical name replace each other
        statement_name_map_.emplace(std::move(stmt),
                                    prepared_stmt_meta(resource_,
                                                       id.hash(),
                                                       result.parameter_count,
                                                       std::move(result.schema),
                                                       std::move(specified_types)));
        send_packet(build_parse_complete(writer_));
    }

    void postgres_connection::handle_bind(std::string stmt,
                                          std::string portal_name,
                                          std::vector<result_encoding> format,
                                          int16_t num_params,
                                          packet_reader&& reader) {
        if (pipeline_.has_error()) {
            log_->error("[Connection {}] BIND stmt: \"{}\", portal: \"{}\" IGNORED DUE TO PIPELINE ERROR, reading next "
                        "packet...",
                        connection_id_,
                        stmt,
                        portal_name);
            read_packet();
            return;
        }

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

        portal_t portal(resource_);
        portal.reserve(num_params);
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
                portal.emplace_back(nullptr);
                continue;
            }

            if (reader.remaining() < len) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND parameter value");
                return;
            }

            std::string str(len, '\0');
            if (encoding == result_encoding::TEXT) {
                for (int j = 0; j < len; str[j++] = static_cast<char>(reader.read_uint8()))
                    ;
            }

            try {
                switch (it->second.specified_types[i]) {
                    case field_type::BOOL:
                        if (encoding == result_encoding::TEXT) {
                            if (str != "t" && str != "f") {
                                send_error_response(sql_state::INVALID_TEXT_REPRESENTATION,
                                                    "Invalid boolean literal: " + str);
                                return;
                            }
                            portal.emplace_back(str == "t");
                        } else {
                            if (len != 1) {
                                send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid BOOL binary length");
                                return;
                            }
                            uint8_t v = reader.read_uint8();
                            portal.emplace_back(v != 0);
                        }
                        break;
                    case field_type::INT2:
                        if (encoding == result_encoding::TEXT) {
                            portal.emplace_back(static_cast<int16_t>(std::stoi(str)));
                        } else {
                            if (len != 2) {
                                send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT2 binary length");
                                return;
                            }
                            portal.emplace_back(reader.read_int16());
                        }
                        break;
                    case field_type::INT4:
                        if (encoding == result_encoding::TEXT) {
                            portal.emplace_back(static_cast<int32_t>(std::stol(str)));
                        } else {
                            if (len != 4) {
                                send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT4 binary length");
                                return;
                            }
                            portal.emplace_back(reader.read_int32());
                        }
                        break;
                    case field_type::INT8:
                        if (encoding == result_encoding::TEXT) {
                            portal.emplace_back(static_cast<int64_t>(std::stoll(str)));
                        } else {
                            if (len != 8) {
                                send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT8 binary length");
                                return;
                            }
                            portal.emplace_back(reader.read_int64());
                        }
                        break;
                    case field_type::FLOAT4:
                        if (encoding == result_encoding::TEXT) {
                            portal.emplace_back(std::stof(str));
                        } else {
                            if (len != 4) {
                                send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid FLOAT4 binary length");
                                return;
                            }
                            uint32_t raw = reader.read_uint32();
                            portal.emplace_back(std::bit_cast<float>(raw));
                        }
                        break;
                    case field_type::FLOAT8:
                        if (encoding == result_encoding::TEXT) {
                            portal.emplace_back(std::stod(str));
                        } else {
                            if (len != 8) {
                                send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid FLOAT8 binary length");
                                return;
                            }
                            uint64_t raw = reader.read_uint64();
                            portal.emplace_back(std::bit_cast<double>(raw));
                        }
                        break;
                    case field_type::TEXT: {
                        portal.emplace_back(std::move(str));
                        break;
                    }
                    default:
                        send_error_response(sql_state::FEATURE_NOT_SUPPORTED, "Unsupported parameter type");
                        return;
                }
            } catch (const std::exception& e) {
                // string conversions may throw, catch here
                send_error_response(sql_state::INVALID_TEXT_REPRESENTATION,
                                    "Invalid literal of type with oid: " +
                                        std::to_string(static_cast<oid_t>(it->second.specified_types[i])) + " - " +
                                        str);
            }
        }

        if (reader.remaining() < 2) {
            send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND parameter value");
            return;
        }

        auto out_format_len = reader.read_int16();
        if (reader.remaining() < 2 * out_format_len) {
            send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message parameter format codes");
            return;
        }

        std::vector<result_encoding>& out_format = it->second.format;
        out_format.reserve(out_format_len);
        for (int16_t i = 0; i < out_format_len; ++i) {
            out_format.emplace_back(static_cast<result_encoding>(reader.read_int16()));
        }

        // add & create portal
        it->second.portal_names.emplace_back(portal_name);
        portals_.erase(portal_name); // portals with identical name replace each other
        portals_.emplace(std::move(portal_name), portal_meta(std::move(portal), std::ref(it->second)));

        send_packet(build_bind_complete(writer_));
    }

    void postgres_connection::handle_execute(std::string portal_name, int32_t limit) {
        if (pipeline_.has_error()) {
            log_->error("[Connection {}] EXECUTE portal: \"{}\" IGNORED DUE TO PIPELINE ERROR, reading next "
                        "packet...",
                        connection_id_,
                        portal_name);
            read_packet();
            return;
        }

        log_->info("[Connection {}] EXECUTE portal: \"{}\"", connection_id_, portal_name);
        auto it = portals_.find(portal_name);
        if (it == portals_.end()) {
            send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME,
                                "Unknown portal in EXECUTE: \"" + portal_name + "\"");
            return;
        }

        auto& portal_meta = it->second;
        auto& stmt = portal_meta.statement.get();
        auto shared_data = create_cv_wrapper(flight_data(resource_));
        actor_zeta::send(scheduler_->address(),
                         scheduler_->address(),
                         scheduler::handler_id(scheduler::route::execute_prepared_statement),
                         stmt.stmt_session,
                         portal_meta.portal,
                         shared_data);
        shared_data->wait_for(cv_wrapper::DEFAULT_TIMEOUT);

        switch (shared_data->status()) {
            case cv_wrapper::Status::Ok:
                if (!shared_data->result.chunk.empty()) {
                    break;
                }
                // fallthrough otherwise
            case cv_wrapper::Status::Empty:
                send_packet(
                    build_command_complete(writer_, command_complete_tag::simple_command(shared_data->result.tag)));
                return;
            case cv_wrapper::Status::Timeout:
            case cv_wrapper::Status::Unknown:
                send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit");
                return;
            case cv_wrapper::Status::Error:
                send_error_response(sql_state::SYNTAX_ERROR, "Syntax error: " + shared_data->error_message());
                return;
        }

        // TODO: PortalSuspended
        int32_t rows_cnt = shared_data->result.chunk.size();
        if (limit != 0) {
            rows_cnt = std::min(static_cast<size_t>(limit), shared_data->result.chunk.size());
        }

        postgres_resultset result(writer_, stmt.is_schema_known_);
        if (!stmt.is_schema_known_) {
            result.add_chunk_columns(shared_data->result.chunk);
        }
        result.add_encoding(stmt.format);

        for (size_t i = 0; i < rows_cnt; i++) {
            result.add_row(shared_data->result.chunk, i);
        }

        auto response = postgres_resultset::build_packets(std::move(result));
        response.emplace_back(build_command_complete(writer_, command_complete_tag::select(rows_cnt)));
        send_packet_merged(std::move(response));
    }

    void postgres_connection::handle_close(describe_close_arg type, std::string name) {
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
                statement_name_map_.erase(it);
            }
        } else {
            portals_.erase(std::move(name));
        }
    }

    void postgres_connection::handle_describe(describe_close_arg type, std::string name) {
        log_->info("[Connection {}] DESCRIBE name: \"{}\" type:\"{}\"", connection_id_, name, static_cast<char>(type));

        if (type == describe_close_arg::STATEMENT) {
            if (auto it = statement_name_map_.find(name); it != statement_name_map_.end()) {
                it->second.is_schema_known_ = true;
                do_describe(it->second.schema);
            } else {
                send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME, "Statement " + name + " does not exist");
            }
        } else {
            if (auto it = portals_.find(name); it != portals_.end()) {
                it->second.statement.get().is_schema_known_ = true;
                do_describe(it->second.statement.get().schema);
            } else {
                send_error_response(sql_state::INVALID_NAME, "Portal " + name + " does not exist");
            }
        }
    }

    void postgres_connection::do_describe(types::complex_logical_type schema) {
        if (schema.type() == types::logical_type::NA || schema.type() != types::logical_type::STRUCT) {
            send_packet(build_no_data(writer_));
            return;
        }

        std::vector<field_description> field_desc;
        for (const auto& column : schema.child_types()) {
            field_desc.emplace_back(column.alias(), get_field_type(column.type()));
        }

        send_packet(build_row_description(writer_, std::move(field_desc), {result_encoding::TEXT}));
    }
} // namespace frontend::postgres
