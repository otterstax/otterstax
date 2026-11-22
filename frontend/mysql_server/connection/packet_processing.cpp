// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_connection.hpp"

#include "../protocol/lexer.hpp"

#include "../../common/utils.hpp"
#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/casts/kernels/string_cast.hpp>
#include <components/vector/vector_builders.hpp>

using namespace components;

namespace {
    // see https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_binary_resultset.html
    constexpr size_t null_bitmap_size(size_t column_count) { return (column_count + 7) / 8; }

    namespace decimal_parse = components::casts::kernels::detail;
} // namespace

namespace frontend::mysql {
    constexpr uint8_t MIN_AUTH_PAYLOAD_SIZE = 32;
    constexpr uint8_t AUTH_FILLER_SIZE = 23;

    boost::asio::awaitable<void> mysql_connection::handle_auth(std::vector<uint8_t> payload) {
        std::string requested_database;
        {
            OTX_ZONE_N("mysql::handle_auth");
            if (payload.size() < MIN_AUTH_PAYLOAD_SIZE) {
                log_->info("[Connection {}] AUTH: Payload too small", connection_id_);
                send_error(mysql_error::ER_ACCESS_DENIED_ERROR, "Access denied for user (using password: NO)");
                co_return;
            }

            packet_reader reader(std::move(payload));

            uint32_t client_flags = reader.read_uint32();
            log_->info("[Connection {}] AUTH: flags=0x{:x}", connection_id_, client_flags);

            if (!(client_flags & CLIENT_PROTOCOL_41)) {
                send_error(mysql_error::ER_NOT_SUPPORTED_AUTH_MODE,
                           "Client does not support authentication protocol requested by server");
                co_return;
            }

            std::ignore = reader.read_uint32(); // all mysql clients ignore MAX_PACKET_SIZE, which is read here

            uint8_t charset = reader.read_uint8();
            log_->info("[Connection {}] AUTH: charset={}", connection_id_, static_cast<int>(charset));

            if (reader.remaining() < AUTH_FILLER_SIZE) {
                send_error(mysql_error::ER_ACCESS_DENIED_ERROR, "Access denied - malformed auth packet");
                co_return;
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

            if ((client_flags & CLIENT_CONNECT_WITH_DB) && reader.remaining() > 0) {
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
                co_return;
            }
            // The client's character set is the protocol's: one the wire does not
            // carry refuses the login, as SET NAMES refuses it.
            if (auto refusal = handshake_charset_refusal(charset)) {
                send_error(refusal->code, std::move(refusal->message));
                finish();
                co_return;
            }

            // skip auth
            state_ = connection_state::COMMAND;
            mark_established();
            // A client may name this connection now (CONNECTION_ID(), the handshake's thread id): KILL reaches it.
            cancels().add(connection_id_, &statement_cancel_, this);
            log_->info("[Connection {}] AUTH: Success -> COMMAND state", connection_id_);
        }
        // The database the client connects to is the session's place; one the
        // engine refuses ends the connection, as MySQL refuses the login.
        if (requested_database.empty()) {
            send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
        } else if (!co_await answer_use(std::move(requested_database), true)) {
            finish();
        }
    }

    boost::asio::awaitable<void> mysql_connection::handle_command(std::vector<uint8_t> payload) {
        {
            // The handlers it dispatches to carry their own zones; a zone never
            // spans a co_await (the coroutine resumes on any thread of the pool).
            OTX_ZONE_N("mysql::handle_command");
            if (payload.empty()) {
                send_error(mysql_error::ER_MALFORMED_PACKET, "Empty command packet");
                co_return;
            }
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
                if (payload.size() < 2) {
                    send_error(mysql_error::ER_MALFORMED_PACKET, "Malformed COM_INIT_DB packet");
                    break;
                }

                std::string db_name(payload.begin() + 1, payload.end());
                log_->info("[Connection {}] COM_INIT_DB: '{}'", connection_id_, db_name);
                [[maybe_unused]] const bool used = co_await answer_use(std::move(db_name), true);
                break;
            }
            case COM_STMT_PREPARE: // fall-through
            case COM_QUERY: {
                std::string query;
                {
                    OTX_ZONE_N("mysql::handle_command::query");
                    if (payload.size() < 2) {
                        send_error(mysql_error::ER_MALFORMED_PACKET, "Malformed COM_QUERY packet");
                        co_return;
                    }

                    query.assign(payload.begin() + 1, payload.end());
                    log_->info("[Connection {}] {}: '{}'",
                               connection_id_,
                               (cmd == COM_QUERY ? "COM_QUERY" : "COM_STMT_PREPARE"),
                               query);

                    if (query.find_first_not_of(" \t\r\n") == std::string::npos) {
                        send_error(mysql_error::ER_EMPTY_QUERY, "Query was empty");
                        co_return;
                    }
                }
                if (cmd == COM_QUERY) {
                    co_await handle_query(std::move(query));
                } else {
                    co_await handle_prepared_stmt(std::move(query));
                }
                break;
            }
            case COM_STMT_EXECUTE: {
                // What the execution runs, copied out of the statement map.
                otterstax::session::statement_id_t stmt_session{};
                std::optional<protocol_statement_t> protocol;
                std::pmr::vector<components::vector::vector_t> param_values(resource_);
                {
                    OTX_ZONE_N("mysql::handle_command::stmt_execute");
                    packet_reader reader(std::move(payload));
                    reader.read_uint8(); // skip [0x17] - COM_STMT_EXECUTE
                    uint32_t stmt_id = reader.read_uint32();
                    uint8_t flags = reader.read_uint8();
                    reader.read_uint32(); // iteration count - always 1, ignore
                    if (!reader.ok()) {
                        send_malformed_packet_error("Malformed COM_STMT_EXECUTE packet");
                        co_return;
                    }

                    auto it_stmt = statement_id_map_.find(stmt_id);
                    if (it_stmt == statement_id_map_.end()) {
                        send_error(mysql_error::ER_UNKNOWN_STMT_HANDLER,
                                   "Unknown statement id " + std::to_string(stmt_id));
                        co_return;
                    }

                    size_t num_params = it_stmt->second.parameter_count;
                    log_->info("[Connection {}] COM_STMT_EXECUTE stmt_id={} flags={} num_params={}",
                               connection_id_,
                               stmt_id,
                               static_cast<int>(flags),
                               num_params);

                    if (num_params != 0) {
                        param_values = handle_execute_params(stmt_id,
                                                             num_params,
                                                             it_stmt->second.param_wire_types,
                                                             it_stmt->second.engine_types,
                                                             std::move(reader));
                        if (param_values.empty()) {
                            co_return; // refused
                        }
                    }
                    stmt_session = it_stmt->second.stmt_session;
                    protocol = it_stmt->second.protocol;
                }
                co_await handle_execute_stmt(stmt_session, std::move(protocol), std::move(param_values));
                break;
            }
            case COM_STMT_CLOSE: {
                std::optional<otterstax::session::statement_id_t> in_session;
                {
                    OTX_ZONE_N("mysql::handle_command::stmt_close");
                    packet_reader reader(std::move(payload));
                    reader.read_uint8(); // skip [0x25] - COM_STMT_CLOSE
                    uint32_t stmt_id = reader.read_uint32();
                    if (!reader.ok()) {
                        // The command has no response by protocol; the ERR packet
                        // stands for the close that follows it.
                        send_malformed_packet_error("Malformed COM_STMT_CLOSE packet");
                        co_return;
                    }
                    if (auto it = statement_id_map_.find(stmt_id); it != statement_id_map_.end()) {
                        // The session keeps the statement until it is closed.
                        if (!it->second.protocol) {
                            in_session = it->second.stmt_session;
                        }
                        statement_id_map_.erase(it);
                    }
                }
                if (in_session) {
                    co_await close_stmt_in_session(*in_session, "COM_STMT_CLOSE");
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
        server_status_flags_t flags = 0;
        if (autocommit_) {
            flags |= static_cast<server_status_flags_t>(server_status::SERVER_STATUS_AUTOCOMMIT);
        }
        if (session_ != nullptr &&
            session_->transaction_status() != otterstax::session::query_session_t::transaction_status_t::idle) {
            flags |= static_cast<server_status_flags_t>(server_status::SERVER_STATUS_IN_TRANS);
        }
        return flags;
    }

    boost::asio::awaitable<bool> mysql_connection::serve_protocol_statement(std::string_view query) {
        std::optional<protocol_statement_t> statement;
        {
            OTX_ZONE_N("mysql::serve_protocol_statement");
            statement = classify_protocol_statement(query);
            if (!statement) {
                co_return false;
            }
        }
        if (auto* set = std::get_if<set_variables_t>(&*statement)) {
            co_await answer_setting(std::move(*set));
            co_return true;
        }
        if (auto* use = std::get_if<use_database_t>(&*statement)) {
            [[maybe_unused]] const bool used = co_await answer_use(std::move(use->database), true);
            co_return true;
        }
        if (auto* show = std::get_if<show_create_table_t>(&*statement)) {
            co_await answer_show_create_table(std::move(*show));
            co_return true;
        }
        if (auto* describe = std::get_if<describe_table_t>(&*statement)) {
            co_await answer_describe(std::move(*describe));
            co_return true;
        }
        if (auto* tables = std::get_if<show_tables_t>(&*statement)) {
            co_await answer_show_tables(std::move(*tables));
            co_return true;
        }
        if (auto* databases = std::get_if<show_databases_t>(&*statement)) {
            co_await answer_show_databases(std::move(*databases));
            co_return true;
        }
        if (auto* kill = std::get_if<kill_t>(&*statement)) {
            answer_kill(*kill);
            co_return true;
        }
        {
            OTX_ZONE_N("mysql::serve_protocol_statement::answer");
            auto answer = answer_protocol_statement(resource_, *statement, protocol_context());
            if (auto* refusal = std::get_if<protocol_refusal_t>(&answer)) {
                send_error(refusal->code, std::move(refusal->message));
            } else {
                auto& rows = std::get<vector::data_chunk_t>(answer);
                mysql_resultset result(writer_, result_encoding::TEXT);
                result.add_chunk_columns(rows);
                for (size_t i = 0; i < rows.size(); ++i) {
                    result.add_row(rows, i);
                }
                send_resultset(std::move(result));
            }
            co_return true;
        }
    }

    boost::asio::awaitable<bool> mysql_connection::await_own_statement(session_future future, bool names_database) {
        auto r = co_await otterstax::async_await_future<session_payload>(std::move(future), resource_, otterstax::DEFAULT_TIMEOUT, &statement_cancel_);
        {
            OTX_ZONE_N("mysql::await_own_statement");
            if (!r.has_error()) {
                co_return true;
            }
            if (answer_await_failure(r)) {
                co_return false;
            }
            const auto code = r.error().type;
            // A place that does not exist is MySQL's ER_BAD_DB_ERROR; any other
            // refusal (a failed block, a malformed place) is the statement's.
            if (names_database &&
                (code == core::error_code_t::database_not_exists || code == core::error_code_t::schema_not_exists)) {
                send_error(mysql_error::ER_BAD_DB_ERROR, std::string{r.error().what.c_str()});
            } else {
                send_statement_error(code, std::string{r.error().what.c_str()});
            }
            co_return false;
        }
    }

    boost::asio::awaitable<void> mysql_connection::answer_setting(set_variables_t set) {
        session_future fut;
        {
            OTX_ZONE_N("mysql::answer_setting");
            // A value the engine's SET cannot take is refused before any
            // assignment runs; like an engine refusal it fails an open block.
            if (set.refusal) {
                fail_open_block(set.refusal->message);
                send_error(set.refusal->code, set.refusal->message);
                co_return;
            }
            if (!set.settings.empty()) {
                // MySQL applies a SET whole or not at all: every assignment goes to
                // the engine in one statement.
                auto plan = engine_setting_plan(resource_, set.settings);
                if (plan.has_error()) {
                    // The engine's parser refused an assignment, nothing was
                    // sent: refused as the session refuses a text that does not
                    // parse, an open block failed behind it.
                    std::string message{plan.error().what.c_str()};
                    fail_open_block(message);
                    send_statement_error(plan.error().type, std::move(message));
                    co_return;
                }
                // a query session schedules itself: needs_sched is always false
                [[maybe_unused]] auto [needs_sched, sent] =
                    actor_zeta::send(session_->address(),
                                     &otterstax::session::query_session_t::execute_plan,
                                     session_->next_statement(),
                                     std::move(plan.value()),
                                     otterstax::session::query_session_t::rebuild_fn{});
                fut = std::move(sent);
            }
        }
        if (!set.settings.empty()) { // its SET went to the engine
            if (!co_await await_own_statement(std::move(fut), false)) {
                co_return; // nothing of the statement applied, wait_timeout neither
            }
            // Every assignment was applied: what the engine now holds.
            const bool autocommit_was_on = autocommit_;
            for (const auto& setting : set.settings) {
                if (setting.name == "autocommit") {
                    autocommit_ = !setting.value || *setting.value != "off";
                } else if (setting.name == "timezone") {
                    time_zone_ = setting.value.value_or("SYSTEM");
                }
            }
            // MySQL: autocommit turned from 0 to 1 commits the open transaction. The engine keeps the block
            // (the SET itself ran inside the implicit one autocommit off opened), so the frontend ends it — else a
            // later error fails a block the client, back in autocommit, never sees (pymysql's connect sends
            // autocommit 0, Django and SQLAlchemy then 1). A failed block's COMMIT rolls it back, as MySQL's
            // statement errors left nothing of it to keep.
            if (!autocommit_was_on && autocommit_ &&
                session_->transaction_status() != otterstax::session::query_session_t::transaction_status_t::idle) {
                session_future commit;
                {
                    OTX_ZONE_N("mysql::answer_setting::implicit_commit");
                    [[maybe_unused]] auto [needs_sched, sent] =
                        actor_zeta::send(session_->address(),
                                         &otterstax::session::query_session_t::execute,
                                         session_->next_statement(),
                                         std::string{"COMMIT;"});
                    commit = std::move(sent);
                }
                if (!co_await await_own_statement(std::move(commit), false)) {
                    co_return;
                }
            }
        }
        if (set.wait_timeout) {
            if (*set.wait_timeout == 0) {
                wait_timeout_.reset();
            } else {
                wait_timeout_ = std::chrono::seconds(*set.wait_timeout);
            }
        }
        send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
    }

    std::chrono::milliseconds mysql_connection::idle_timeout() const {
        return wait_timeout_ ? std::chrono::milliseconds(*wait_timeout_) : default_idle_timeout();
    }

    namespace {
        // A standard SQL literal of `text` for the catalog reads: a quote doubled.
        std::string catalog_literal(std::string_view text) {
            std::string out{"'"};
            for (char c : text) {
                out.push_back(c);
                if (c == '\'') {
                    out.push_back(c);
                }
            }
            out.push_back('\'');
            return out;
        }

        // The database a name's schema belongs to: a three-part name's connection (or local), else the session's
        // current database.
        std::string database_condition(const std::vector<std::string>& name) {
            if (name.size() == 3) {
                return name[0] == "local" ? " AND d.datname = 'local' AND d.datconnection = ''"
                                          : " AND d.datconnection = " + catalog_literal(name[0]);
            }
            return " AND d.datname = current_database() AND d.datconnection = current_connection()";
        }

        constexpr std::string_view catalog_joins =
            " FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid JOIN "
            "pg_catalog.pg_database d ON n.nspdatabase = d.oid";
    } // namespace

    boost::asio::awaitable<otterstax::awaited_t<session_payload>> mysql_connection::run_catalog_query(std::string sql) {
        session_future fut;
        {
            OTX_ZONE_N("mysql::run_catalog_query");
            [[maybe_unused]] auto [needs_sched, sent] = actor_zeta::send(session_->address(),
                                                                         &otterstax::session::query_session_t::execute,
                                                                         session_->next_statement(),
                                                                         std::move(sql));
            fut = std::move(sent);
        }
        co_return co_await otterstax::async_await_future<session_payload>(std::move(fut), resource_, otterstax::DEFAULT_TIMEOUT, &statement_cancel_);
    }

    bool mysql_connection::answer_catalog_failure(const otterstax::awaited_t<session_payload>& answer) {
        if (!answer.has_error()) {
            return false;
        }
        if (!answer_await_failure(answer)) {
            send_statement_error(answer.error().type, std::string{answer.error().what.c_str()});
        }
        return true;
    }

    void mysql_connection::send_text_rows(const text_rows_t& rows) {
        OTX_ZONE_N("mysql::send_text_rows");
        std::pmr::vector<types::complex_logical_type> fields(resource_);
        for (const auto& name : rows.names) {
            fields.emplace_back(types::logical_type::TEXT, name);
        }
        vector::data_chunk_t chunk(resource_, fields, std::max<std::size_t>(rows.rows.size(), 1));
        for (std::size_t row = 0; row < rows.rows.size(); ++row) {
            for (std::size_t column = 0; column < rows.names.size(); ++column) {
                const auto& cell = rows.rows[row][column];
                if (cell) {
                    chunk.set_value(column, row, std::string_view{*cell});
                } else {
                    chunk.data[column].set_null(row, true);
                }
            }
        }
        chunk.set_cardinality(rows.rows.size());
        mysql_resultset result(writer_, result_encoding::TEXT);
        result.add_chunk_columns(chunk);
        for (std::size_t row = 0; row < rows.rows.size(); ++row) {
            result.add_row(chunk, row);
        }
        send_resultset(std::move(result));
    }

    boost::asio::awaitable<std::optional<mysql_connection::catalog_table_t>>
    mysql_connection::read_catalog_table(const std::vector<std::string>& name) {
        catalog_table_t table;
        std::string where;
        std::string written;
        {
            OTX_ZONE_N("mysql::read_catalog_table");
            table.table = name.back();
            for (std::size_t i = 0; i < name.size(); ++i) {
                written += (i > 0 ? "." : "") + std::string{"\""} + name[i] + "\"";
            }
            // The place a name of one, two or three parts resolves into, as a query's would.
            where = "c.relname = " + catalog_literal(table.table);
            where += name.size() == 1 ? std::string{" AND n.nspname = current_schema()"}
                                      : " AND n.nspname = " + catalog_literal(name[name.size() - 2]);
            where += database_condition(name);
        }
        auto columns = co_await run_catalog_query(
            "SELECT a.attname, a.attnotnull, a.attoid, c.oid FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c "
            "ON a.attrelid = c.oid JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid JOIN pg_catalog.pg_database "
            "d ON n.nspdatabase = d.oid WHERE " + where + " AND NOT a.attisdropped ORDER BY a.attnum;");
        if (answer_catalog_failure(columns)) {
            co_return std::nullopt;
        }
        std::vector<std::pair<uint32_t, std::string>> attoids;
        uint32_t table_oid = 0;
        for (const auto& chunk : columns.value().chunks) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                create_table_column_t column;
                column.name = std::string{chunk.data[0].get_value<std::string_view>(row)};
                column.not_null = chunk.data[1].get_value<bool>(row);
                attoids.emplace_back(chunk.data[2].get_value<uint32_t>(row), column.name);
                table_oid = chunk.data[3].get_value<uint32_t>(row);
                table.columns.push_back(std::move(column));
            }
        }
        if (table.columns.empty()) {
            send_error(mysql_error::ER_NO_SUCH_TABLE, "Table '" + table.table + "' doesn't exist");
            co_return std::nullopt;
        }
        // The engine's types of the columns, by position: its describe of SELECT *, which asks no backend.
        session_future prepared;
        const auto statement = session_->next_statement();
        {
            [[maybe_unused]] auto [needs_sched, sent] =
                actor_zeta::send(session_->address(),
                                 &otterstax::session::query_session_t::prepare_schema,
                                 statement,
                                 "SELECT * FROM " + written + ";");
            prepared = std::move(sent);
        }
        auto schema = co_await otterstax::async_await_future<session_payload>(std::move(prepared), resource_, otterstax::DEFAULT_TIMEOUT, &statement_cancel_);
        co_await close_stmt_in_session(statement, "a catalog statement");
        if (answer_catalog_failure(schema)) {
            co_return std::nullopt;
        }
        const auto& schema_type = schema.value().schema;
        if (schema_type.type() == types::logical_type::STRUCT &&
            schema_type.child_types().size() == table.columns.size()) {
            for (std::size_t i = 0; i < table.columns.size(); ++i) {
                table.columns[i].type = schema_type.child_types()[i];
            }
        }
        // The PRIMARY KEY's columns: pg_constraint's conkey, the attoids of its columns in key order.
        auto key = co_await run_catalog_query("SELECT conkey FROM pg_catalog.pg_constraint WHERE conrelid = " +
                                              std::to_string(table_oid) + " AND contype = 'p';");
        if (!key.has_error()) {
            const auto name_of = [&](uint32_t attoid) {
                for (const auto& [oid, column] : attoids) {
                    if (oid == attoid) {
                        table.primary_key.push_back(column);
                    }
                }
            };
            for (const auto& chunk : key.value().chunks) {
                if (chunk.size() == 0 || chunk.column_count() != 1) {
                    continue;
                }
                const auto& cell = chunk.data[0];
                if (cell.type().type() == types::logical_type::TEXT) {
                    std::string_view text = cell.get_value<std::string_view>(0);
                    while (!text.empty()) {
                        const auto comma = text.find(',');
                        name_of(static_cast<uint32_t>(std::stoul(std::string{text.substr(0, comma)})));
                        text = comma == std::string_view::npos ? std::string_view{} : text.substr(comma + 1);
                    }
                } else {
                    for (const auto& attoid : cell.get_value<std::vector<uint32_t>>(0)) {
                        name_of(attoid);
                    }
                }
            }
        }
        co_return table;
    }

    boost::asio::awaitable<void> mysql_connection::answer_show_create_table(show_create_table_t show) {
        auto table = co_await read_catalog_table(show.name);
        if (!table) {
            co_return; // answered
        }
        OTX_ZONE_N("mysql::answer_show_create_table");
        send_text_rows(text_rows_t{{"Table", "Create Table"},
                                   {{table->table, create_table_text(table->table, table->columns, table->primary_key)}}});
    }

    boost::asio::awaitable<void> mysql_connection::answer_describe(describe_table_t describe) {
        auto table = co_await read_catalog_table(describe.name);
        if (!table) {
            co_return; // answered
        }
        OTX_ZONE_N("mysql::answer_describe");
        send_text_rows(describe_rows(table->columns, table->primary_key, describe.full));
    }

    boost::asio::awaitable<void> mysql_connection::answer_show_tables(show_tables_t show) {
        std::string sql;
        {
            OTX_ZONE_N("mysql::answer_show_tables");
            if (!show.from && !database_) {
                send_error(mysql_error::ER_NO_DB_ERROR, "No database selected");
                co_return;
            }
            sql = "SELECT c.relname, c.relkind" + std::string{catalog_joins} +
                  " WHERE c.relkind IN ('r', 'g', 'v', 'm') AND n.nspname = " +
                  (show.from ? catalog_literal(*show.from) : std::string{"current_schema()"}) +
                  database_condition({}) + " ORDER BY c.relname;";
        }
        auto tables = co_await run_catalog_query(std::move(sql));
        if (answer_catalog_failure(tables)) {
            co_return;
        }
        OTX_ZONE_N("mysql::answer_show_tables::answer");
        text_rows_t rows;
        rows.names.push_back("Tables_in_" + show.from.value_or(database_.value_or("")));
        if (show.full) {
            rows.names.emplace_back("Table_type");
        }
        for (const auto& chunk : tables.value().chunks) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                std::string name{chunk.data[0].get_value<std::string_view>(row)};
                if (show.like && !mysql_like(name, *show.like)) {
                    continue;
                }
                const auto kind = chunk.data[1].get_value<std::string_view>(row);
                std::vector<std::optional<std::string>> cells{std::move(name)};
                if (show.full) {
                    cells.emplace_back(kind == "v" || kind == "m" ? "VIEW" : "BASE TABLE");
                }
                rows.rows.push_back(std::move(cells));
            }
        }
        send_text_rows(rows);
    }

    boost::asio::awaitable<void> mysql_connection::answer_show_databases(show_databases_t show) {
        auto schemas = co_await run_catalog_query(
            "SELECT n.nspname FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_database d ON n.nspdatabase = d.oid "
            "WHERE d.datname = current_database() AND d.datconnection = current_connection() ORDER BY n.nspname;");
        if (answer_catalog_failure(schemas)) {
            co_return;
        }
        OTX_ZONE_N("mysql::answer_show_databases");
        text_rows_t rows{{"Database"}, {}};
        for (const auto& chunk : schemas.value().chunks) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                std::string name{chunk.data[0].get_value<std::string_view>(row)};
                if (!show.like || mysql_like(name, *show.like)) {
                    rows.rows.push_back({std::move(name)});
                }
            }
        }
        send_text_rows(rows);
    }

    void mysql_connection::answer_kill(const kill_t& kill) {
        OTX_ZONE_N("mysql::answer_kill");
        // MySQL: an unknown id is ER_NO_SUCH_THREAD; KILL QUERY of a connection waiting for nothing is OK and does
        // nothing. The cancelled connection answers its own client ER_QUERY_INTERRUPTED (answer_await_failure).
        // KILL <own id> closes this connection after the OK.
        if (kill.query_only) {
            if (cancels().cancel(kill.connection_id) == cancel_registry_t::outcome_t::no_such_connection) {
                send_error(mysql_error::ER_NO_SUCH_THREAD, "Unknown thread id: " + std::to_string(kill.connection_id));
                return;
            }
            send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
            return;
        }
        if (kill.connection_id == connection_id_) {
            send_packet(build_ok(writer_, sequence_id_, 0, status_flags()), false);
            finish();
            return;
        }
        if (!cancels().close(kill.connection_id)) {
            send_error(mysql_error::ER_NO_SUCH_THREAD, "Unknown thread id: " + std::to_string(kill.connection_id));
            return;
        }
        send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
    }

    boost::asio::awaitable<bool> mysql_connection::answer_use(std::string database, bool answer_ok) {
        session_future fut;
        {
            OTX_ZONE_N("mysql::answer_use");
            // a query session schedules itself: needs_sched is always false
            [[maybe_unused]] auto [needs_sched, sent] = actor_zeta::send(session_->address(),
                                                                         &otterstax::session::query_session_t::use,
                                                                         session_->next_statement(),
                                                                         database);
            fut = std::move(sent);
        }
        if (!co_await await_own_statement(std::move(fut), true)) {
            co_return false;
        }
        // DATABASE() is MySQL's current database: the namespace a short name resolves into, which is the
        // engine's current_schema() of the place just set (USE local → public, USE my.bill → bill, USE my → its
        // configured database) — what information_schema's table_schema names, never the text the client sent.
        session_future schema_future;
        {
            OTX_ZONE_N("mysql::answer_use::current_schema");
            [[maybe_unused]] auto [needs_sched, sent] = actor_zeta::send(session_->address(),
                                                                         &otterstax::session::query_session_t::execute,
                                                                         session_->next_statement(),
                                                                         std::string{"SELECT current_schema();"});
            schema_future = std::move(sent);
        }
        auto schema = co_await otterstax::async_await_future<session_payload>(std::move(schema_future), resource_, otterstax::DEFAULT_TIMEOUT, &statement_cancel_);
        database_.reset();
        if (!schema.has_error() && !schema.value().chunks.empty() && schema.value().chunks.front().size() == 1 &&
            schema.value().chunks.front().column_count() == 1 && !schema.value().chunks.front().data[0].is_null(0)) {
            database_ = std::string{schema.value().chunks.front().data[0].get_value<std::string_view>(0)};
        } else if (schema.has_error()) {
            log_->warn("[Connection {}] USE {}: current_schema() could not be read: {}",
                       connection_id_,
                       database,
                       schema.error().what.c_str());
        }
        if (answer_ok) {
            send_packet(build_ok(writer_, sequence_id_, 0, status_flags()));
        }
        co_return true;
    }

    protocol_context_t mysql_connection::protocol_context() const {
        boost::system::error_code ec;
        const auto peer = socket_.remote_endpoint(ec);
        return protocol_context_t{.wait_timeout = std::chrono::duration_cast<std::chrono::seconds>(idle_timeout()),
                                  .connection_id = connection_id_,
                                  .user = user_,
                                  .host = ec ? std::string{"localhost"} : peer.address().to_string(),
                                  .autocommit = autocommit_,
                                  .time_zone = time_zone_,
                                  .database = database_,
                                  .interactive_timeout =
                                      std::chrono::duration_cast<std::chrono::seconds>(default_idle_timeout())};
    }

    core::date::time_zone_t mysql_connection::session_time_zone() const {
        // @@time_zone as the last SET the engine accepted wrote it; SYSTEM is the engine's default, UTC.
        core::date::time_zone_t zone;
        if (time_zone_ == "SYSTEM" || !core::date::setting_time_zone(time_zone_, &zone)) {
            return core::date::time_zone_t{};
        }
        return zone;
    }

    void mysql_connection::refuse_text(text_refusal_t refusal) {
        // The engine refusing a text it cannot parse fails an open block; a text
        // the frontend refuses for the engine does the same.
        fail_open_block(refusal.message);
        send_error(refusal.code, std::move(refusal.message));
    }

    boost::asio::awaitable<void> mysql_connection::handle_query(std::string query) {
        // Protocol statements first: their text is never written in the
        // engine's form (a SET / USE reaches the engine as its own SET, so SET
        // NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci is never refused for its
        // collation, which the engine's SET does not carry).
        if (co_await serve_protocol_statement(query)) {
            co_return;
        }
        session_future fut;
        {
            OTX_ZONE_N("mysql::handle_query");
            // A SELECT mixing system variables / session functions with what the engine evaluates.
            if (auto substituted = substitute_session_values(query, protocol_context())) {
                if (auto* refusal = std::get_if<protocol_refusal_t>(&*substituted)) {
                    send_error(refusal->code, std::move(refusal->message));
                    co_return;
                }
                query = std::move(std::get<std::string>(*substituted));
            }
            auto translated = to_engine_text(query, text_command_t::query);
            if (auto* refusal = std::get_if<text_refusal_t>(&translated)) {
                refuse_text(std::move(*refusal));
                co_return;
            }
            auto engine_text = std::move(std::get<engine_text_t>(translated).sql);
            // Nothing but comments (or an executable comment for a newer server).
            if (engine_text.find_first_not_of(" \t\r\n\f\v") == std::string::npos) {
                send_error(mysql_error::ER_EMPTY_QUERY, "Query was empty");
                co_return;
            }
            // todo: one execute() call for simplicity - use computed schema for text_resultset columns
            // a query session schedules itself: needs_sched is always false
            [[maybe_unused]] auto [needs_sched, sent] = actor_zeta::send(session_->address(),
                                                                         &otterstax::session::query_session_t::execute,
                                                                         session_->next_statement(),
                                                                         std::move(engine_text));
            fut = std::move(sent);
        }
        auto r = co_await otterstax::async_await_future<session_payload>(std::move(fut), resource_, otterstax::DEFAULT_TIMEOUT, &statement_cancel_);
        {
            OTX_ZONE_N("mysql::handle_query::answer");
            if (r.has_error()) {
                if (!answer_await_failure(r)) {
                    send_statement_error(r.error().type, std::string{r.error().what.c_str()});
                }
                co_return;
            }

            // handle Ok
            session_payload sdata_result = std::move(r.value());
            // empty db & table in metadata (not critical, but may be improved)
            if (sdata_result.column_count() == 0) {
                send_packet(build_ok(writer_, sequence_id_, sdata_result.reported_rows(), status_flags(), sdata_result.generated_id));
                co_return;
            }

            // A column the text resultset cannot encode is an error packet, not a
            // dropped connection.
            if (auto bad = find_unsupported_column<frontend_type::MYSQL>(sdata_result.chunks.front(),
                                                                         {result_encoding::TEXT})) {
                send_error(mysql_error::ER_NOT_SUPPORTED_YET, unsupported_column_message<frontend_type::MYSQL>(*bad));
                co_return;
            }

            mysql_resultset result(writer_, result_encoding::TEXT);
            result.set_time_zone(session_time_zone());
            result.add_chunk_columns(sdata_result.chunks.front());
            for (auto& ch : sdata_result.chunks) {
                for (size_t i = 0; i < ch.size(); ++i) {
                    result.add_row(ch, i);
                }
            }

            send_resultset(std::move(result));
        }
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
            case otterstax::await_status_t::cancelled:
                // KILL QUERY from another connection: the statement runs on (the engine has no cancel), an open
                // block is failed behind it as for a timeout.
                fail_open_block(*otterstax::unanswered_block_failure(r.status()));
                send_error(mysql_error::ER_QUERY_INTERRUPTED, "Query execution was interrupted");
                return true;
        }
        return false;
    }

    void mysql_connection::fail_open_block(std::string reason) {
        // The failure is queued after the statement and ahead of the client's
        // next one (not awaited — it runs once the statement ended).
        if (session_->transaction_status() ==
            otterstax::session::query_session_t::transaction_status_t::in_transaction) {
            [[maybe_unused]] auto [needs_sched, fut] =
                actor_zeta::send(session_->address(),
                                 &otterstax::session::query_session_t::fail_transaction,
                                 core::error_code_t::protocol_violation,
                                 std::move(reason));
        }
    }

    void mysql_connection::send_statement_error(core::error_code_t code, std::string error) {
        // A statement the engine does not support is ER_NOT_SUPPORTED_YET, by
        // COM_QUERY and COM_STMT_PREPARE alike; every other code is
        // ER_SYNTAX_ERROR until the engine maps its codes.
        send_error(code == core::error_code_t::unimplemented_yet ? mysql_error::ER_NOT_SUPPORTED_YET
                                                                 : mysql_error::ER_SYNTAX_ERROR,
                   std::move(error));
    }

    boost::asio::awaitable<void> mysql_connection::handle_prepared_stmt(std::string query) {
        session_future fut;
        otterstax::session::statement_id_t id{};
        uint32_t written_placeholders = 0; // the `?` the client binds
        {
            OTX_ZONE_N("mysql::handle_prepared_stmt");
            // What drivers prepare on their own (SELECT @@x, SET @@x = ?) never
            // reaches the engine, whose grammar reads none of it: one parameter per `?`.
            if (auto statement = classify_protocol_statement(query, true)) {
                uint32_t placeholders = 0;
                for (const auto& token : tokenize(query).value_or(std::vector<token_t>{})) {
                    placeholders += token.kind == token_kind::punct && token.value == "?" ? 1 : 0;
                }
                prepare_protocol_statement(std::move(*statement), placeholders);
                co_return;
            }
            if (auto substituted = substitute_session_values(query, protocol_context())) {
                if (auto* refusal = std::get_if<protocol_refusal_t>(&*substituted)) {
                    send_error(refusal->code, std::move(refusal->message));
                    co_return;
                }
                query = std::move(std::get<std::string>(*substituted));
            }
            // The session and the engine only ever see the engine's form, `$n`
            // among it: a bare `?` is an operator in the engine's grammar.
            auto translated = to_engine_text(query, text_command_t::prepare);
            if (auto* refusal = std::get_if<text_refusal_t>(&translated)) {
                refuse_text(std::move(*refusal));
                co_return;
            }
            auto rewritten = std::move(std::get<engine_text_t>(translated));
            if (rewritten.sql.find_first_not_of(" \t\r\n\f\v") == std::string::npos) {
                send_error(mysql_error::ER_EMPTY_QUERY, "Query was empty");
                co_return;
            }
            written_placeholders = rewritten.placeholders;
            id = session_->next_statement();
            // a query session schedules itself: needs_sched is always false
            [[maybe_unused]] auto [needs_sched, sent] =
                actor_zeta::send(session_->address(),
                                 &otterstax::session::query_session_t::prepare_schema,
                                 id,
                                 rewritten.sql);
            fut = std::move(sent);
        }
        auto r = co_await otterstax::async_await_future<session_payload>(std::move(fut), resource_, otterstax::DEFAULT_TIMEOUT, &statement_cancel_);

        if (r.has_error()) {
            if (!answer_await_failure(r)) {
                send_statement_error(r.error().type, std::string{r.error().what.c_str()});
            }
            co_return;
        }

        session_payload result = std::move(r.value());

        // The client binds one value per `?` it wrote; the engine must take
        // exactly as many, or the values would land on other parameters.
        // A prepare the frontend refuses fails an open block, as the engine's
        // own refusal of a prepare does (and as COM_QUERY's refusals do).
        if (result.parameter_count != written_placeholders) {
            co_await close_stmt_in_session(id, "COM_STMT_PREPARE refused");
            std::string message = "the statement has " + std::to_string(written_placeholders) +
                                  " '?' placeholder(s), and the engine takes " +
                                  std::to_string(result.parameter_count) + " parameter(s)";
            fail_open_block(message);
            send_error(mysql_error::ER_NOT_SUPPORTED_YET, std::move(message));
            co_return;
        }

        uint16_t column_cnt = result.schema != types::logical_type::NA ? result.schema.child_types().size() : 0;

        // COM_STMT_EXECUTE answers a binary resultset: a column the binary
        // encoder cannot carry is refused here, and the statement the session
        // has just stored is released — no execute will ever consume it.
        if (column_cnt) {
            if (auto bad = find_unsupported_column<frontend_type::MYSQL>(result.schema.child_types(),
                                                                         {result_encoding::BINARY})) {
                co_await close_stmt_in_session(id, "COM_STMT_PREPARE refused");
                auto message = unsupported_column_message<frontend_type::MYSQL>(*bad);
                fail_open_block(message);
                send_error(mysql_error::ER_NOT_SUPPORTED_YET, std::move(message));
                co_return;
            }
        }

        {
            OTX_ZONE_N("mysql::handle_prepared_stmt::answer");
            std::vector<std::vector<uint8_t>> packets;

            packets.reserve(4 + column_cnt + result.parameter_count);
            log_->info("[Connection {}] COM_STMT_PREPARE: id={} column_cnt={} param_cnt={}",
                       connection_id_,
                       next_statement_id_,
                       column_cnt,
                       result.parameter_count);
            packets.push_back(
                build_stmt_prepare_ok(writer_, sequence_id_++, next_statement_id_, column_cnt, result.parameter_count));
            // The session keeps the statement (`?` already written as `$n`) for
            // every COM_STMT_EXECUTE until COM_STMT_CLOSE, with the describe's
            // parameter types: the engine type of every `$n` a decimal bind
            // reads.
            prepared_stmt_meta meta(resource_, id, result.parameter_count);
            meta.engine_types = std::move(result.engine_types);
            statement_id_map_.emplace(next_statement_id_++, std::move(meta));

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
                    // The flags of its type; a DECIMAL column its own scale and
                    // display length.
                    apply_type_metadata(col, column);
                    packets.push_back(column_definition_41::write_packet(std::move(col), writer_, sequence_id_++));
                }
                packets.push_back(build_eof(writer_, sequence_id_++, 0, status_flags()));
            }

            send_packet_sequence(std::move(packets), 0);
        }
    }

    void mysql_connection::prepare_protocol_statement(protocol_statement_t statement, uint32_t parameter_count) {
        OTX_ZONE_N("mysql::prepare_protocol_statement");
        // The columns the execution will answer; a SET / USE answers an OK
        // packet, and a SET whose value the engine cannot take is refused now.
        std::pmr::vector<types::complex_logical_type> columns(resource_);
        // A SET refused now is refused as COM_QUERY refuses it (answer_setting):
        // a statement refused, an open block failed behind it — as the prepare
        // of any other statement the frontend refuses fails it.
        if (const auto* set = std::get_if<set_variables_t>(&statement); set != nullptr && set->refusal) {
            fail_open_block(set->refusal->message);
            send_error(set->refusal->code, set->refusal->message);
            return;
        }
        if (!std::holds_alternative<set_variables_t>(statement) && !std::holds_alternative<use_database_t>(statement)) {
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
        // `engine_types` stays empty: no engine describe answers a protocol
        // statement, and every value bound to one rides as the string it came
        // as.
        prepared_stmt_meta meta(resource_, session_->next_statement(), parameter_count);
        meta.protocol = std::move(statement); // nothing in the session to close or execute
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
                // The flags of its type, as the executed resultset announces them.
                apply_type_metadata(col, column);
                packets.push_back(column_definition_41::write_packet(std::move(col), writer_, sequence_id_++));
            }
            packets.push_back(build_eof(writer_, sequence_id_++, 0, status_flags()));
        }
        send_packet_sequence(std::move(packets), 0);
    }

    boost::asio::awaitable<void> mysql_connection::execute_protocol_statement(protocol_statement_t statement) {
        if (auto* set = std::get_if<set_variables_t>(&statement)) {
            co_await answer_setting(std::move(*set));
            co_return;
        }
        if (auto* use = std::get_if<use_database_t>(&statement)) {
            [[maybe_unused]] const bool used = co_await answer_use(std::move(use->database), true);
            co_return;
        }
        {
            OTX_ZONE_N("mysql::execute_protocol_statement");
            auto answer = answer_protocol_statement(resource_, statement, protocol_context());
            if (auto* refusal = std::get_if<protocol_refusal_t>(&answer)) {
                send_error(refusal->code, std::move(refusal->message));
            } else {
                auto& rows = std::get<vector::data_chunk_t>(answer);
                mysql_resultset result(writer_, result_encoding::BINARY);
                result.add_chunk_columns(rows);
                for (size_t i = 0; i < rows.size(); ++i) {
                    result.add_row(rows, i);
                }
                send_resultset(std::move(result));
            }
        }
    }

    std::pmr::vector<components::vector::vector_t> mysql_connection::handle_execute_params(
        uint32_t stmt_id,
        size_t num_params,
        std::pmr::vector<uint16_t>& param_wire_types,
        const std::pmr::vector<components::types::complex_logical_type>& engine_types,
        packet_reader&& reader) {
        std::vector<uint8_t> null_bitmap(null_bitmap_size(num_params));
        for (auto& b : null_bitmap) {
            b = reader.read_uint8();
        }

        // WARN: MariaDB does not have such flag! in case of MariaDB parameters will follow immediately
        // todo: identify session type (if possible)
        uint8_t new_params_bound_flag = reader.read_uint8();
        if (new_params_bound_flag) {
            param_wire_types.clear();
            param_wire_types.reserve(num_params);
            for (size_t i = 0; i < num_params; ++i) {
                uint16_t t = reader.read_uint16();
                param_wire_types.push_back(t);
            }
        }

        // Checked before the types are used: a type read past the end is 0
        // (MYSQL_TYPE_DECIMAL), which would be refused as unsupported instead.
        if (!reader.ok()) {
            param_wire_types.clear();
            send_malformed_packet_error("Malformed COM_STMT_EXECUTE packet: parameter types");
            return {};
        }

        if (param_wire_types.size() != num_params) {
            send_error(mysql_error::ER_UNKNOWN_STMT_HANDLER,
                       "Missing parameter types for statement with id=" + std::to_string(stmt_id) + ": " +
                           std::to_string(param_wire_types.size()) + " parameters passed out of " +
                           std::to_string(num_params));
            return {};
        }

        // values are always sent if num_params > 0, regardless of new_params_bound_flag
        std::pmr::vector<components::vector::vector_t> param_values(resource_);
        for (size_t i = 0; i < num_params; ++i) {
            bool is_null = (null_bitmap[i / 8] >> (i % 8)) & 1;
            if (is_null) {
                param_values.push_back(components::vector::make_null(
                    resource_,
                    components::types::complex_logical_type{components::types::logical_type::NA}));
                continue;
            }

            uint16_t raw_type = param_wire_types[i];
            uint8_t type = raw_type & 0xFF;
            bool is_unsigned = (raw_type & 0x8000) != 0;
            switch (static_cast<field_type>(type)) {
                case field_type::MYSQL_TYPE_TINY: {
                    auto val = reader.read_uint8();
                    if (is_unsigned) {
                        param_values.push_back(components::vector::make_scalar(resource_, val));
                    } else {
                        param_values.push_back(components::vector::make_scalar(resource_, static_cast<int8_t>(val)));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_SHORT: {
                    auto val = reader.read_uint16();
                    if (is_unsigned) {
                        param_values.push_back(components::vector::make_scalar(resource_, val));
                    } else {
                        param_values.push_back(components::vector::make_scalar(resource_, static_cast<int16_t>(val)));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_LONG: {
                    auto val = reader.read_uint32();
                    if (is_unsigned) {
                        param_values.push_back(components::vector::make_scalar(resource_, val));
                    } else {
                        param_values.push_back(components::vector::make_scalar(resource_, static_cast<int32_t>(val)));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_LONGLONG: {
                    auto val = reader.read_uint64();
                    if (is_unsigned) {
                        param_values.push_back(components::vector::make_scalar(resource_, val));
                    } else {
                        param_values.push_back(components::vector::make_scalar(resource_, static_cast<int64_t>(val)));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_FLOAT: {
                    auto f = std::bit_cast<float>(reader.read_uint32());
                    param_values.push_back(components::vector::make_scalar(resource_, f));
                    break;
                }
                case field_type::MYSQL_TYPE_DOUBLE: {
                    auto d = std::bit_cast<double>(reader.read_uint64());
                    param_values.push_back(components::vector::make_scalar(resource_, d));
                    break;
                }
                case field_type::MYSQL_TYPE_VAR_STRING:
                case field_type::MYSQL_TYPE_STRING:
                case field_type::MYSQL_TYPE_BLOB: {
                    std::string s = reader.read_length_encoded_string();
                    param_values.push_back(components::vector::make_scalar(resource_, std::string_view{s}));
                    break;
                }
                // MySQL's binary DATE / DATETIME / TIMESTAMP (a length 0, 4, 7 or 11 and the year, month, day,
                // hour, minute, second, microseconds it covers) and TIME (0, 8 or 12: the sign, days, hours,
                // minutes, seconds, microseconds) as the engine's DATE / TIMESTAMP / TIME — a client sends a
                // TIMESTAMP as its wall clock, a TIMESTAMP the engine reads in the session's zone where it meets
                // an instant. A zero date and a TIME outside one day have no engine value: refused.
                case field_type::MYSQL_TYPE_DATE:
                case field_type::MYSQL_TYPE_DATETIME:
                case field_type::MYSQL_TYPE_TIMESTAMP: {
                    const uint8_t length = reader.read_uint8();
                    unsigned year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
                    uint32_t micro = 0;
                    if (length >= 4) {
                        year = reader.read_uint16();
                        month = reader.read_uint8();
                        day = reader.read_uint8();
                    }
                    if (length >= 7) {
                        hour = reader.read_uint8();
                        minute = reader.read_uint8();
                        second = reader.read_uint8();
                    }
                    if (length >= 11) {
                        micro = reader.read_uint32();
                    }
                    if (!reader.ok() || (length != 0 && length != 4 && length != 7 && length != 11)) {
                        send_malformed_packet_error("Malformed COM_STMT_EXECUTE packet: a date/time parameter");
                        return {};
                    }
                    const std::chrono::year_month_day ymd{std::chrono::year{static_cast<int>(year)},
                                                          std::chrono::month{month},
                                                          std::chrono::day{day}};
                    if (!ymd.ok() || hour > 23 || minute > 59 || second > 59 || micro > 999999) {
                        send_error(mysql_error::ER_SYNTAX_ERROR,
                                   "parameter " + std::to_string(i + 1) + " is no date the server can hold");
                        return {};
                    }
                    const auto date = core::date::from_sys_days(std::chrono::sys_days{ymd});
                    if (static_cast<field_type>(type) == field_type::MYSQL_TYPE_DATE) {
                        param_values.push_back(components::vector::make_scalar(resource_, date));
                    } else {
                        const core::date::microseconds time_of_day{
                            int64_t{hour} * 3'600'000'000 + int64_t{minute} * 60'000'000 + int64_t{second} * 1'000'000 +
                            micro};
                        param_values.push_back(components::vector::make_scalar(
                            resource_,
                            core::date::timestamp_t{core::date::from_sys_days_us(std::chrono::sys_days{ymd}, time_of_day)}));
                    }
                    break;
                }
                case field_type::MYSQL_TYPE_TIME: {
                    const uint8_t length = reader.read_uint8();
                    bool negative = false;
                    uint32_t days = 0;
                    unsigned hour = 0, minute = 0, second = 0;
                    uint32_t micro = 0;
                    if (length >= 8) {
                        negative = reader.read_uint8() != 0;
                        days = reader.read_uint32();
                        hour = reader.read_uint8();
                        minute = reader.read_uint8();
                        second = reader.read_uint8();
                    }
                    if (length >= 12) {
                        micro = reader.read_uint32();
                    }
                    if (!reader.ok() || (length != 0 && length != 8 && length != 12)) {
                        send_malformed_packet_error("Malformed COM_STMT_EXECUTE packet: a time parameter");
                        return {};
                    }
                    if (negative || days != 0 || hour > 23 || minute > 59 || second > 59 || micro > 999999) {
                        send_error(mysql_error::ER_SYNTAX_ERROR,
                                   "parameter " + std::to_string(i + 1) + " is no time of day the server can hold");
                        return {};
                    }
                    param_values.push_back(components::vector::make_scalar(
                        resource_,
                        core::date::time_t{core::date::microseconds{int64_t{hour} * 3'600'000'000 +
                                                                    int64_t{minute} * 60'000'000 +
                                                                    int64_t{second} * 1'000'000 + micro}}));
                    break;
                }
                case field_type::MYSQL_TYPE_DECIMAL:
                case field_type::MYSQL_TYPE_NEWDECIMAL: {
                    // 246 rides as the length-encoded string MySQL prints a decimal
                    // with — type 0, the pre-5.0 DECIMAL, the same (COM_STMT_EXECUTE:
                    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html).
                    std::string s = reader.read_length_encoded_string();
                    // A value cut short by the packet's end is no data: the
                    // malformed-packet refusal, before any reading of it.
                    if (!reader.ok()) {
                        send_malformed_packet_error("Malformed COM_STMT_EXECUTE packet: parameter values");
                        return {};
                    }
                    const types::complex_logical_type* engine_type =
                        i < engine_types.size() ? &engine_types[i] : nullptr;
                    if (engine_type == nullptr || engine_type->type() == types::logical_type::TEXT ||
                        engine_type->type() == types::logical_type::STRING_LITERAL ||
                        engine_type->type() == types::logical_type::NA) {
                        param_values.push_back(components::vector::make_scalar(resource_, std::string_view{s}));
                        break;
                    }
                    if (engine_type->type() != types::logical_type::DECIMAL) {
                        send_error(mysql_error::ER_SYNTAX_ERROR,
                                   "parameter " + std::to_string(i + 1) + " takes a value of type " +
                                       std::string(logical_type_name(engine_type->type())) + ", not a decimal");
                        return {};
                    }
                    const auto* decimal = engine_type->extension_as<types::decimal_logical_type_extension>();
                    types::int128_t unscaled = 0;
                    if (const auto outcome =
                            decimal_parse::parse_decimal(s, decimal->width(), decimal->scale(), &unscaled);
                        outcome != decimal_parse::parse_outcome::parsed) {
                        const auto expected = "DECIMAL(" + std::to_string(decimal->width()) + ", " +
                                              std::to_string(decimal->scale()) + ")";
                        if (outcome == decimal_parse::parse_outcome::out_of_range) {
                            send_error(mysql_error::ER_PARAM_PAST_PRECISION,
                                       "value for parameter " + std::to_string(i + 1) + " is past the precision of " +
                                           expected);
                        } else {
                            send_error(mysql_error::ER_PARAM_NOT_A_DECIMAL,
                                       "incorrect decimal value for parameter " + std::to_string(i + 1) + ", " +
                                           expected + " expected");
                        }
                        return {};
                    }
                    // The described type carries a name (the column's); a bound
                    // value's type keeps none, as parse_decimal_parameter keeps none.
                    auto bound = *engine_type;
                    bound.set_alias(std::string{});
                    param_values.push_back(components::vector::make_decimal(resource_, bound, unscaled));
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

    boost::asio::awaitable<void>
    mysql_connection::close_stmt_in_session(otterstax::session::statement_id_t stmt_session, const char* reason) {
        session_future fut;
        {
            OTX_ZONE_N("mysql::close_stmt_in_session");
            // a query session schedules itself: needs_sched is always false
            fut = actor_zeta::send(session_->address(),
                                   &otterstax::session::query_session_t::close_statement,
                                   stmt_session)
                      .second;
        }
        auto r = co_await otterstax::async_await_future<session_payload>(std::move(fut), resource_, otterstax::DEFAULT_TIMEOUT, &statement_cancel_);
        if (r.has_error()) {
            log_->warn("[Connection {}] {}: close_statement failed: {}", connection_id_, reason, r.error().what);
        }
    }

    void mysql_connection::finish_impl() {
        OTX_ZONE_N("mysql::finish_impl");
        // Before the connection is destroyed: no KILL reaches it afterwards (idempotent for one never added).
        cancels().remove(connection_id_);
        // The prepared statements live in the connection's session and go with
        // it. Closing the session never waits: a statement still running (the
        // client stopped waiting for it) finishes, then the session frees itself.
        statement_id_map_.clear();
        session_.reset();
    }

    boost::asio::awaitable<void>
    mysql_connection::handle_execute_stmt(otterstax::session::statement_id_t stmt_session,
                                          std::optional<protocol_statement_t> protocol,
                                          std::pmr::vector<components::vector::vector_t> param_values) {
        if (protocol) {
            co_await execute_protocol_statement(std::move(*protocol));
            co_return;
        }
        session_future fut;
        {
            OTX_ZONE_N("mysql::handle_execute_stmt");
            // a query session schedules itself: needs_sched is always false
            [[maybe_unused]] auto [needs_sched, sent] =
                actor_zeta::send(session_->address(),
                                 &otterstax::session::query_session_t::execute_prepared_statement,
                                 stmt_session,
                                 otterstax::session::parameter_row(resource_, std::move(param_values)),
                                 uint64_t{0});
            fut = std::move(sent);
        }
        auto r = co_await otterstax::async_await_future<session_payload>(std::move(fut), resource_, otterstax::DEFAULT_TIMEOUT, &statement_cancel_);
        {
            OTX_ZONE_N("mysql::handle_execute_stmt::answer");
            if (r.has_error()) {
                if (!answer_await_failure(r)) {
                    send_statement_error(r.error().type, std::string{r.error().what.c_str()});
                }
                co_return;
            }

            session_payload sdata_result = std::move(r.value());

            if (sdata_result.column_count() == 0) {
                send_packet(build_ok(writer_, sequence_id_, sdata_result.reported_rows(), status_flags(), sdata_result.generated_id));
                co_return;
            }

            // The executed columns may differ from the prepared schema (a
            // parameterized statement has none at prepare time): checked again.
            if (auto bad = find_unsupported_column<frontend_type::MYSQL>(sdata_result.chunks.front(),
                                                                         {result_encoding::BINARY})) {
                send_error(mysql_error::ER_NOT_SUPPORTED_YET, unsupported_column_message<frontend_type::MYSQL>(*bad));
                co_return;
            }

            mysql_resultset result(writer_, result_encoding::BINARY);
            result.set_time_zone(session_time_zone());
            result.add_chunk_columns(sdata_result.chunks.front());
            for (auto& ch : sdata_result.chunks) {
                for (size_t i = 0; i < ch.size(); ++i) {
                    result.add_row(ch, i);
                }
            }

            send_resultset(std::move(result));
        }
    }

    void mysql_connection::send_resultset(mysql_resultset&& result) {
        send_packet_merged(mysql_resultset::build_packets(std::move(result), sequence_id_, status_flags()));
    }
} // namespace frontend::mysql