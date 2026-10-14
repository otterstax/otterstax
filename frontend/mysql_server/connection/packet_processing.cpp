// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_connection.hpp"

#include "../protocol/lexer.hpp"

#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

using namespace components;

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
        user_ = reader.read_string_null();
        log_->info("[Connection {}] AUTH: user='{}'", connection_id_, user_);

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

        // A string above without its terminator: none of the values read is data.
        if (!reader.ok()) {
            send_malformed_packet_error("Malformed HandshakeResponse packet");
            return;
        }

        // skip auth
        state_ = connection_state::COMMAND;
        log_->info("[Connection {}] AUTH: Success -> COMMAND state", connection_id_);
        send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
    }

    void mysql_connection::handle_command(std::vector<uint8_t> payload) {
        OTX_ZONE_N("mysql::handle_command");
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

                cmd == COM_QUERY ? handle_query(std::move(query)) : handle_prepared_stmt(query);
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
                    // The session keeps an unexecuted statement until it is
                    // closed; an executed one it has already dropped (consumed).
                    if (!it->second.consumed && !it->second.protocol) {
                        close_stmt_in_session(it->second.stmt_session, "COM_STMT_CLOSE");
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

    server_status_flags_t mysql_connection::status_flags() const {
        // Never SERVER_STATUS_NO_BACKSLASH_ESCAPES: drivers then escape a string
        // value with a backslash, which is what the frontend reads
        // (protocol/lexer.hpp), as @@sql_mode says.
        auto flags = static_cast<server_status_flags_t>(server_status::SERVER_STATUS_AUTOCOMMIT);
        if (session_ != nullptr &&
            session_->transaction_state() != otterstax::session::transaction_state_t::idle) {
            flags |= static_cast<server_status_flags_t>(server_status::SERVER_STATUS_IN_TRANS);
        }
        return flags;
    }

    bool mysql_connection::serve_protocol_statement(std::string_view query) {
        OTX_ZONE_N("mysql::serve_protocol_statement");
        auto statement = classify_protocol_statement(query);
        if (!statement) {
            return false;
        }
        if (std::holds_alternative<set_variables_t>(*statement)) {
            answer_setting();
            return true;
        }
        auto answer = answer_protocol_statement(resource_, *statement, protocol_context());
        if (auto* refusal = std::get_if<protocol_refusal_t>(&answer)) {
            send_error(refusal->code, std::move(refusal->message));
        } else if (auto* rows = std::get_if<vector::data_chunk_t>(&answer)) {
            mysql_resultset result(writer_, result_encoding::TEXT);
            result.add_chunk_columns(*rows);
            for (size_t i = 0; i < rows->size(); ++i) {
                result.add_row(*rows, i);
            }
            send_resultset(std::move(result));
        } else {
            send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
        }
        return true;
    }

    void mysql_connection::answer_setting() {
        OTX_ZONE_N("mysql::answer_setting");
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(session_->address(), &otterstax::session::query_session_t::execute_setting);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            if (!answer_await_failure(r)) {
                send_statement_error(r.error().type, std::string{r.error().what.c_str()});
            }
            return;
        }
        send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
    }

    protocol_context_t mysql_connection::protocol_context() const {
        boost::system::error_code ec;
        const auto peer = socket_.remote_endpoint(ec);
        return protocol_context_t{std::chrono::duration_cast<std::chrono::seconds>(read_timeout_),
                                  connection_id_,
                                  user_,
                                  ec ? std::string{"localhost"} : peer.address().to_string()};
    }

    void mysql_connection::refuse_text(text_refusal_t refusal) {
        // The engine refusing a text it cannot parse fails an open block; a text
        // the frontend refuses for the engine does the same.
        fail_open_block(refusal.message);
        send_error(refusal.code, std::move(refusal.message));
    }

    void mysql_connection::handle_query(std::string query) {
        OTX_ZONE_N("mysql::handle_query");
        // Protocol statements first: they never reach the engine, so their text
        // is never written in its form (SET NAMES utf8mb4 COLLATE
        // utf8mb4_0900_ai_ci is answered, where a COLLATE the engine cannot
        // keep is refused in a statement it runs).
        if (serve_protocol_statement(query)) {
            return;
        }
        auto translated = to_engine_text(query, text_command_t::query);
        if (auto* refusal = std::get_if<text_refusal_t>(&translated)) {
            refuse_text(std::move(*refusal));
            return;
        }
        auto engine_text = std::move(std::get<engine_text_t>(translated).sql);
        // Nothing but comments (or an executable comment for a newer server).
        if (engine_text.find_first_not_of(" \t\r\n\f\v") == std::string::npos) {
            send_error(mysql_error::ER_EMPTY_QUERY, "Query was empty");
            return;
        }
        // todo: one execute() call for simplicity - use computed schema for text_resultset columns
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::execute,
                                                                    session_->next_statement(),
                                                                    std::move(engine_text));
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

        if (r.has_error()) {
            if (!answer_await_failure(r) && !answer_failed_commit(r.error())) {
                send_statement_error(r.error().type, std::string{r.error().what.c_str()});
            }
            return;
        }

        // handle Ok
        session_payload sdata_result = std::move(r.value());
        // empty db & table in metadata (not critical, but may be improved)
        if (sdata_result.column_count() == 0) {
            send_packet(build_ok(writer_, sequence_id_, sdata_result.reported_rows(), status_flags()));
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

    bool mysql_connection::answer_failed_commit(const core::error_t& error) {
        if (!session_->ended_failed_block(error)) {
            return false;
        }
        send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
        return true;
    }

    bool mysql_connection::answer_await_failure(const otterstax::awaited_t<session_payload>& r) {
        switch (r.status()) {
            case otterstax::await_status_t::answered:
                return false;
            case otterstax::await_status_t::timed_out:
                // The statement goes on running after the client stopped waiting
                // for it. Inside an explicit transaction the block is failed
                // behind it, so a COMMIT cannot publish what the client was told
                // is cancelled.
                fail_open_block(*otterstax::unanswered_block_failure(r.status()));
                send_error(mysql_error::ER_QUERY_TIMEOUT, "Query exceeded execution limit");
                return true;
            case otterstax::await_status_t::broken:
                // No answer came back (an exception escaped the engine's
                // executor, say): the session never settled the statement, so
                // an open block is failed behind it as for a timeout.
                fail_open_block(*otterstax::unanswered_block_failure(r.status()));
                send_error(mysql_error::ER_UNKNOWN_ERROR, std::string{r.error().what.c_str()});
                return true;
        }
        return false;
    }

    void mysql_connection::fail_open_block(std::string reason) {
        // The failure is queued after the statement and ahead of the client's
        // next one (not awaited — it runs once the statement ended).
        if (session_->transaction_state() == otterstax::session::transaction_state_t::in_block) {
            [[maybe_unused]] auto [needs_sched, fut] =
                actor_zeta::send(session_->address(),
                                 &otterstax::session::query_session_t::fail_transaction,
                                 core::error_code_t::other_error,
                                 std::move(reason));
        }
    }

    void mysql_connection::send_statement_error(core::error_code_t code, std::string error) {
        // A statement the engine does not support (SAVEPOINT / RELEASE /
        // ROLLBACK TO among them: it has no savepoints) is ER_NOT_SUPPORTED_YET,
        // by COM_QUERY and COM_STMT_PREPARE alike.
        send_error(code == core::error_code_t::unimplemented_yet ? mysql_error::ER_NOT_SUPPORTED_YET
                                                                 : mysql_error::ER_SYNTAX_ERROR,
                   std::move(error));
    }

    void mysql_connection::handle_prepared_stmt(std::string_view query) {
        OTX_ZONE_N("mysql::handle_prepared_stmt");
        // What drivers prepare on their own (SELECT @@x, SET @@x = ?) never
        // reaches the engine, whose grammar reads none of it: one parameter per `?`.
        if (auto statement = classify_protocol_statement(query, true)) {
            uint32_t placeholders = 0;
            for (const auto& token : tokenize(query).value_or(std::vector<token_t>{})) {
                placeholders += token.kind == token_kind::punct && token.value == "?" ? 1 : 0;
            }
            prepare_protocol_statement(std::move(*statement), placeholders);
            return;
        }
        // The session and the engine only ever see the engine's form, `$n`
        // among it: a bare `?` is an operator in the engine's grammar.
        auto translated = to_engine_text(query, text_command_t::prepare);
        if (auto* refusal = std::get_if<text_refusal_t>(&translated)) {
            refuse_text(std::move(*refusal));
            return;
        }
        auto rewritten = std::move(std::get<engine_text_t>(translated));
        if (rewritten.sql.find_first_not_of(" \t\r\n\f\v") == std::string::npos) {
            send_error(mysql_error::ER_EMPTY_QUERY, "Query was empty");
            return;
        }
        const auto id = session_->next_statement();
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::prepare_schema,
                                                                    id,
                                                                    rewritten.sql);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

        if (r.has_error()) {
            if (!answer_await_failure(r)) {
                send_statement_error(r.error().type, std::string{r.error().what.c_str()});
            }
            return;
        }

        session_payload result = std::move(r.value());
        std::vector<std::vector<uint8_t>> packets;

        // The client binds one value per `?` it wrote; the engine must take
        // exactly as many, or the values would land on other parameters.
        if (result.parameter_count != rewritten.placeholders) {
            close_stmt_in_session(id, "COM_STMT_PREPARE refused");
            send_error(mysql_error::ER_NOT_SUPPORTED_YET,
                       "the statement has " + std::to_string(rewritten.placeholders) +
                           " '?' placeholder(s), and the engine takes " + std::to_string(result.parameter_count) +
                           " parameter(s)");
            return;
        }

        uint16_t column_cnt = result.schema != types::logical_type::NA ? result.schema.child_types().size() : 0;

        // COM_STMT_EXECUTE answers a binary resultset: a column the binary
        // encoder cannot carry is refused here, and the statement the session
        // has just stored is released — no execute will ever consume it.
        if (column_cnt) {
            if (auto bad = find_unsupported_column<frontend_type::MYSQL>(result.schema.child_types(),
                                                                        {result_encoding::BINARY})) {
                close_stmt_in_session(id, "COM_STMT_PREPARE refused");
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
        // The text the session accepted (`?` already written as `$n`) is the
        // text a re-prepare uses.
        statement_id_map_.emplace(next_statement_id_++,
                                  prepared_stmt_meta(resource_, id, std::move(rewritten.sql), result.parameter_count));

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

    void mysql_connection::prepare_protocol_statement(protocol_statement_t statement, uint32_t parameter_count) {
        OTX_ZONE_N("mysql::prepare_protocol_statement");
        // The columns the execution will answer; a SET answers an OK packet.
        std::pmr::vector<types::complex_logical_type> columns(resource_);
        if (!std::holds_alternative<set_variables_t>(statement)) {
            auto answer = answer_protocol_statement(resource_, statement, protocol_context());
            if (auto* refusal = std::get_if<protocol_refusal_t>(&answer)) {
                // Driver housekeeping never fails a block (serve_protocol_statement).
                send_error(refusal->code, std::move(refusal->message));
                return;
            }
            if (auto* rows = std::get_if<vector::data_chunk_t>(&answer)) {
                columns = rows->types();
            }
        }

        const auto column_cnt = static_cast<uint16_t>(columns.size());
        std::vector<std::vector<uint8_t>> packets;
        packets.reserve(4 + column_cnt + parameter_count);
        packets.push_back(
            build_stmt_prepare_ok(writer_, sequence_id_++, next_statement_id_, column_cnt, parameter_count));
        prepared_stmt_meta meta(resource_, session_->next_statement(), std::string{}, parameter_count);
        meta.consumed = true; // nothing in the session to close or execute
        meta.protocol = std::move(statement);
        statement_id_map_.emplace(next_statement_id_++, std::move(meta));

        if (parameter_count) {
            for (uint32_t i = 0; i < parameter_count; ++i) {
                column_definition_41 param("?", field_type::MYSQL_TYPE_STRING);
                packets.push_back(column_definition_41::write_packet(std::move(param), writer_, sequence_id_++));
            }
            packets.push_back(build_eof(writer_, sequence_id_++, 0, status_flags()));
        }
        if (column_cnt) {
            for (const auto& column : columns) {
                auto wire_type = get_field_type(column.type());
                assert(wire_type.has_value() && "a protocol answer is BIGINT and text columns only");
                column_definition_41 col(column.has_alias() ? column.alias() : std::string{}, *wire_type);
                packets.push_back(column_definition_41::write_packet(std::move(col), writer_, sequence_id_++));
            }
            packets.push_back(build_eof(writer_, sequence_id_++, 0, status_flags()));
        }
        send_packet_sequence(std::move(packets), 0);
    }

    void mysql_connection::execute_protocol_statement(const protocol_statement_t& statement) {
        OTX_ZONE_N("mysql::execute_protocol_statement");
        if (std::holds_alternative<set_variables_t>(statement)) {
            answer_setting();
            return;
        }
        auto answer = answer_protocol_statement(resource_, statement, protocol_context());
        if (auto* refusal = std::get_if<protocol_refusal_t>(&answer)) {
            send_error(refusal->code, std::move(refusal->message));
        } else if (auto* rows = std::get_if<vector::data_chunk_t>(&answer)) {
            mysql_resultset result(writer_, result_encoding::BINARY);
            result.add_chunk_columns(*rows);
            for (size_t i = 0; i < rows->size(); ++i) {
                result.add_row(*rows, i);
            }
            send_resultset(std::move(result));
        } else {
            send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
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
        const auto id = session_->next_statement();
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::prepare_schema,
                                                                    id,
                                                                    std::string{stmt.sql.c_str()});
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

        if (r.has_error()) {
            // Refused as the COM_STMT_PREPARE of the same text would be.
            if (!answer_await_failure(r)) {
                send_statement_error(r.error().type, std::string{r.error().what.c_str()});
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

    void mysql_connection::close_stmt_in_session(otterstax::session::statement_id_t stmt_session, const char* reason) {
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::close_statement,
                                                                    stmt_session);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            log_->warn("[Connection {}] {}: close_statement failed: {}", connection_id_, reason, r.error().what);
        }
    }

    void mysql_connection::finish_impl() {
        OTX_ZONE_N("mysql::finish_impl");
        // The prepared statements live in the connection's session and go with
        // it. Closing the session never waits: a statement still running (the
        // client stopped waiting for it) finishes, then the session frees itself.
        statement_id_map_.clear();
        session_.reset();
    }

    void mysql_connection::handle_execute_stmt(prepared_stmt_meta& stmt,
                                               std::pmr::vector<types::logical_value_t> param_values) {
        OTX_ZONE_N("mysql::handle_execute_stmt");
        if (stmt.protocol) {
            execute_protocol_statement(*stmt.protocol);
            return;
        }
        if (stmt.consumed && !reprepare_stmt(stmt)) {
            return;
        }
        // The session drops the statement on every exit path of the execution.
        stmt.consumed = true;

        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(session_->address(),
                             &otterstax::session::query_session_t::execute_prepared_statement,
                             stmt.stmt_session,
                             std::move(param_values));
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

        if (r.has_error()) {
            if (!answer_await_failure(r) && !answer_failed_commit(r.error())) {
                send_statement_error(r.error().type, std::string{r.error().what.c_str()});
            }
            return;
        }

        session_payload sdata_result = std::move(r.value());

        if (sdata_result.column_count() == 0) {
            send_packet(build_ok(writer_, sequence_id_, sdata_result.reported_rows(), status_flags()));
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

    void mysql_connection::send_resultset(mysql_resultset&& result) {
        send_packet_merged(mysql_resultset::build_packets(std::move(result), sequence_id_, status_flags()));
    }
} // namespace frontend::mysql