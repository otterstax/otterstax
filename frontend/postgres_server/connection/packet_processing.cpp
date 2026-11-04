// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "postgres_connection.hpp"

#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/catalog/settings.hpp>
#include <components/logical_plan/node_cursor.hpp>
#include <components/vector/vector_builders.hpp>

#include <algorithm>
#include <array>
#include <bit>

using namespace components;
using namespace components::sql;

namespace frontend::postgres {
    constexpr size_t SECRET_KEY_3_2_SIZE = 32;
    constexpr size_t SECRET_KEY_SIZE = 4;

    namespace {
        // An engine SET of `name` to `value`: the name a quoted identifier, the
        // value a standard string literal (the engine takes a SET value as a
        // string constant only).
        std::string engine_set_statement(std::string_view name, std::string_view value) {
            std::string sql = "SET \"";
            for (char c : name) {
                sql += c;
                if (c == '"') {
                    sql += '"';
                }
            }
            sql += "\" = '";
            for (char c : value) {
                sql += c;
                if (c == '\'') {
                    sql += '\'';
                }
            }
            sql += '\'';
            return sql;
        }

        // The engine setting a pg_settings row is of (its `name` column is the
        // setting's catalog name), nullptr for any other row.
        const components::catalog::setting_def_t* setting_of_row(std::string_view name) {
            for (const auto& setting : components::catalog::all_settings()) {
                if (setting.catalog_name == name) {
                    return &setting;
                }
            }
            return nullptr;
        }
    } // namespace

    boost::asio::awaitable<void> postgres_connection::handle_startup_message(int32_t protocol_version,
                                                                             packet_reader reader) {
        std::optional<std::string> database;
        std::vector<std::pair<std::string, std::string>> options;
        {
            OTX_ZONE_N("pg::handle_startup_message");
            log_->info("[Connection {}]: Client protocol version: {}", connection_id_, protocol_version);
            std::vector<std::pair<std::string, std::string>> parameters;
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
                    co_return;
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
                } else if (key.starts_with("_pq_.") || key == "user") {
                    // A protocol extension; the user: no authentication.
                } else if (key == "database") {
                    database = std::move(value);
                } else if (key == "options") {
                    auto switches = startup_options(value);
                    if (auto* refusal = std::get_if<std::string>(&switches)) {
                        send_error_response(sql_state::SYNTAX_ERROR, std::move(*refusal), error_severity::fatal());
                        co_return;
                    }
                    auto& settings = std::get<0>(switches);
                    options.insert(options.end(),
                                   std::make_move_iterator(settings.begin()),
                                   std::make_move_iterator(settings.end()));
                } else {
                    parameters.emplace_back(std::move(key), std::move(value));
                }
            }

            // A key or value without its terminator.
            if (!reader.ok()) {
                send_error_response(sql_state::PROTOCOL_VIOLATION,
                                    "Malformed StartupMessage: unterminated parameter",
                                    error_severity::fatal());
                co_return;
            }

            // PostgreSQL applies the `options` switches first, the parameters after.
            options.insert(options.end(),
                           std::make_move_iterator(parameters.begin()),
                           std::make_move_iterator(parameters.end()));
        }
        // The defaults first: a RESET of a reported engine setting goes back to
        // them, not to the startup's value (the startup's SET is an ordinary
        // SET in the engine).
        co_await read_default_settings();
        if (!co_await apply_startup_settings(database, options)) {
            co_return;
        }
        OTX_ZONE_N("pg::handle_startup_message::answer"); // no co_await follows

        std::vector<std::vector<uint8_t>> msg;
        msg.emplace_back(build_auth_ok(writer_));
        // The client's encoding, then the protocol's parameters: what OtterStax
        // does, fixed.
        reported_.client_encoding = client_encoding_.current();
        msg.emplace_back(build_parameter_status(writer_, "client_encoding", reported_.client_encoding));
        for (const auto& parameter : protocol_parameters()) {
            msg.emplace_back(
                build_parameter_status(writer_, std::string{parameter.name}, std::string{parameter.value}));
        }
        // Read-only facts of the server: the PostgreSQL version it speaks, the
        // encoding the engine stores text in, the timestamps' microsecond
        // integers (core/date/date_types.hpp).
        msg.emplace_back(build_parameter_status(writer_, "server_version", "18.0"));
        msg.emplace_back(build_parameter_status(writer_, "server_encoding", "UTF8"));
        msg.emplace_back(build_parameter_status(writer_, "integer_datetimes", "on"));
        // The engine session's, as the engine has them.
        reported_.timezone = timezone_.current();
        reported_.search_path = search_path_.current();
        msg.emplace_back(build_parameter_status(writer_, "TimeZone", reported_.timezone));
        msg.emplace_back(build_parameter_status(writer_, "search_path", reported_.search_path));
        // The client's own, reported empty when it named none (PostgreSQL's
        // default).
        log_->info("[Connection {}] application_name: \"{}\", client_encoding: {}, extra_float_digits: {}",
                   connection_id_,
                   application_name_.session,
                   client_encoding_.session,
                   extra_float_digits_.session);
        reported_.application_name = application_name_.current();
        msg.emplace_back(build_parameter_status(writer_, "application_name", reported_.application_name));

        auto secret_key = secure_random_bytes(use_protocol_3_2_ ? SECRET_KEY_3_2_SIZE : SECRET_KEY_SIZE);
        if (!secret_key) {
            send_error_response(sql_state::INTERNAL_ERROR,
                                "the server could not generate the cancel key",
                                error_severity::fatal());
            co_return;
        }
        backend_secret_key_ = std::move(*secret_key);
        msg.emplace_back(build_backend_key_data(writer_, connection_id_, backend_secret_key_));

        log_->debug("[Connection {}] Generated BackendKeyData: key_size={} bytes",
                    connection_id_,
                    backend_secret_key_.size());
        msg.emplace_back(build_ready_for_query(writer_, transaction_status::IDLE));
        send_packet_merged(std::move(msg)); // send packet & begin message-handling loop
    }

    boost::asio::awaitable<bool>
    postgres_connection::apply_startup_settings(const std::optional<std::string>& database,
                                                const std::vector<std::pair<std::string, std::string>>& settings) {
        {
            OTX_ZONE_N("pg::apply_startup_settings");
            // The protocol's first: nothing reaches the engine unless all are true.
            // A client parameter is the client's: the last one given is kept, and
            // RESET goes back to it.
            for (const auto& [name, value] : settings) {
                if (const auto client = find_client_parameter(name)) {
                    switch (*client) {
                        case client_parameter_t::application_name:
                            application_name_.reset = clean_application_name(value);
                            application_name_.session = application_name_.reset;
                            continue;
                        case client_parameter_t::client_encoding: {
                            auto encoding = client_encoding_value(value);
                            if (!encoding) {
                                send_error_response(sql_state::INVALID_PARAMETER_VALUE,
                                                    client_encoding_refusal(value),
                                                    error_severity::fatal());
                                co_return false;
                            }
                            client_encoding_.reset = std::move(*encoding);
                            client_encoding_.session = client_encoding_.reset;
                            continue;
                        }
                        case client_parameter_t::extra_float_digits: {
                            auto digits = parse_extra_float_digits(value);
                            if (auto* refusal = std::get_if<std::string>(&digits)) {
                                send_error_response(sql_state::INVALID_PARAMETER_VALUE,
                                                    std::move(*refusal),
                                                    error_severity::fatal());
                                co_return false;
                            }
                            extra_float_digits_.reset = std::get<int>(digits);
                            extra_float_digits_.session = extra_float_digits_.reset;
                            continue;
                        }
                    }
                } else if (const auto* parameter = find_protocol_parameter(name)) {
                    if (auto refusal = check_protocol_parameter(*parameter, value)) {
                        send_error_response(sql_state::INVALID_PARAMETER_VALUE,
                                            std::move(*refusal),
                                            error_severity::fatal());
                        co_return false;
                    }
                }
            }
        }
        // Each SET the engine applied is followed by await_session.
        if (database && !database->empty()) {
            auto r = co_await execute_own(engine_set_statement("search_path", *database + ".public"));
            if (r.has_error()) {
                const auto code = r.error().type;
                if (!r.timed_out() && code == core::error_code_t::database_not_exists) {
                    // The database a client that names none is placed in
                    // (otterbrix's built-in search_path, main.public).
                    send_error_response(sql_state::INVALID_CATALOG_NAME,
                                        "database \"" + *database + "\" does not exist",
                                        error_severity::fatal(),
                                        "The default database is \"main\".");
                } else if (!r.timed_out() && code == core::error_code_t::schema_not_exists) {
                    // The database is there, its public schema (the place a
                    // StartupMessage's database names) is not. PostgreSQL never
                    // refuses a connection for a search_path schema, but the
                    // engine cannot place a session in one that does not exist:
                    // the refusal names the schema, never "the database does not
                    // exist" for a database that does.
                    send_error_response(sql_state::INVALID_SCHEMA_NAME,
                                        "schema \"" + *database + ".public\" does not exist",
                                        error_severity::fatal());
                } else {
                    send_session_error(r, "", error_severity::fatal());
                }
                co_return false;
            }
        }
        for (const auto& [name, value] : settings) {
            if (find_protocol_parameter(name) != nullptr || find_client_parameter(name)) {
                continue;
            }
            auto r = co_await execute_own(engine_set_statement(name, value));
            if (r.has_error()) {
                send_session_error(r, "", error_severity::fatal());
                co_return false;
            }
        }
        co_return true;
    }

    boost::asio::awaitable<void> postgres_connection::read_default_settings() {
        // A fresh session's value is the default every session shares: the
        // built-in one with the stored rows of pg_settings over it, the last
        // row of a setting winning (the table is append-only; the engine reads
        // the last one at its start), each through the engine's set_setting.
        auto r = co_await execute_own(std::string{"SELECT name, setting FROM pg_catalog.pg_settings"});
        OTX_ZONE_N("pg::read_default_settings"); // after the only co_await
        components::catalog::session_catalog_t defaults;
        if (r.has_error()) {
            log_->warn("[Connection {}] pg_settings could not be read: {}", connection_id_, r.error().what.c_str());
        } else {
            for (const auto& chunk : r.value().chunks) {
                // Two text columns, or the rows are not what this reads.
                if (chunk.column_count() < 2 ||
                    chunk.data[0].type().type() != types::logical_type::STRING_LITERAL ||
                    chunk.data[1].type().type() != types::logical_type::STRING_LITERAL) {
                    log_->warn("[Connection {}] pg_settings answered columns other than (name, setting): ignored",
                               connection_id_);
                    continue;
                }
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    if (chunk.data[0].is_null(row) || chunk.data[1].is_null(row)) {
                        continue;
                    }
                    const auto* setting = setting_of_row(chunk.data[0].data<std::string_view>()[row]);
                    if (setting == nullptr) {
                        continue;
                    }
                    const auto value = chunk.data[1].data<std::string_view>()[row];
                    if (auto refused = components::catalog::set_setting(defaults, setting->id, value, resource_);
                        refused.contains_error()) {
                        log_->warn("[Connection {}] pg_settings {} = '{}' refused: {}",
                                   connection_id_,
                                   setting->sql_name,
                                   value,
                                   refused.what.c_str());
                    }
                }
            }
        }
        const auto value_of = [&](components::catalog::setting_id id) {
            return std::string{components::catalog::current_setting_value(defaults, id, resource_)};
        };
        timezone_.reset = value_of(components::catalog::setting_id::timezone);
        timezone_.session = timezone_.reset;
        search_path_.reset = value_of(components::catalog::setting_id::search_path);
        search_path_.session = search_path_.reset;
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
        // The CommandComplete tag of a statement's answer (`sql` its text): an
        // executed transaction statement by what the session says it was (a
        // COMMIT that ended a failed block is ROLLBACK), every other statement
        // by its kind.
        command_complete_tag tag_of(const session_payload& payload, int64_t rows, std::string_view sql) {
            switch (payload.transaction) {
                case otterstax::session::transaction_command_t::begin:
                    return command_complete_tag::begin();
                case otterstax::session::transaction_command_t::commit:
                    return command_complete_tag::commit();
                case otterstax::session::transaction_command_t::rollback:
                case otterstax::session::transaction_command_t::rollback_to:
                    return command_complete_tag::rollback();
                case otterstax::session::transaction_command_t::savepoint:
                    return command_complete_tag::savepoint();
                case otterstax::session::transaction_command_t::release:
                    return command_complete_tag::release();
                case otterstax::session::transaction_command_t::none:
                    break;
            }
            // RESET and RESET ALL are tagged RESET (utility.c), SET ... TO
            // DEFAULT SET: one VariableSetStmt, told by its first keyword.
            if (payload.tag == T_VariableSetStmt && keyword_is(first_keyword(sql), "reset")) {
                return command_complete_tag::reset();
            }
            return command_complete_tag::simple_command(payload.tag, rows);
        }

        bool ends_block(const session_payload& payload) {
            return payload.transaction == otterstax::session::transaction_command_t::commit ||
                   payload.transaction == otterstax::session::transaction_command_t::rollback;
        }

        // The SQLSTATE of an engine code, where the tests read one; the full
        // table is otterbrix's to give. nullptr: none.
        const char* sqlstate_of(core::error_code_t code) {
            switch (code) {
                case core::error_code_t::transaction_finalized:
                    return sql_state::IN_FAILED_SQL_TRANSACTION;
                case core::error_code_t::unimplemented_yet:
                    return sql_state::FEATURE_NOT_SUPPORTED;
                case core::error_code_t::undefined_prepared_statement:
                    return sql_state::INVALID_SQL_STATAMENT_NAME;
                case core::error_code_t::no_active_sql_transaction:
                    return sql_state::NO_ACTIVE_SQL_TRANSACTION;
                default:
                    return nullptr;
            }
        }

        // The Bind values copied: Describe(portal) binds them into a describe
        // and leaves the portal its own for the Execute.
        components::vector::data_chunk_t copy_row(std::pmr::memory_resource* resource,
                                                  const postgres_connection::portal_t& params) {
            std::pmr::vector<components::vector::vector_t> values(resource);
            values.reserve(params.size());
            for (const auto& value : params) {
                values.push_back(components::vector::make_constant_copy(resource, value, 0));
            }
            return otterstax::session::parameter_row(resource, std::move(values));
        }
    } // namespace

    boost::asio::awaitable<otterstax::awaited_t<session_payload>>
    postgres_connection::await_session(session_future fut) {
        auto r = co_await otterstax::async_await_future<session_payload>(std::move(fut), resource_);
        // Any answer but the bridge's timeout comes after every message sent
        // before the statement, a fail_transaction included: the session's block
        // is current again.
        if (!r.timed_out()) {
            failure_pending_ = false;
        }
        follow_parameters(r);
        co_return r;
    }

    boost::asio::awaitable<otterstax::awaited_t<session_payload>> postgres_connection::execute_own(std::string sql) {
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::execute,
                                                                    session_->next_statement(),
                                                                    std::move(sql));
        co_return co_await await_session(std::move(fut));
    }

    void postgres_connection::follow_parameters(const otterstax::awaited_t<session_payload>& answer) {
        OTX_ZONE_N("pg::follow_parameters");
        if (session_ == nullptr || answer.timed_out()) {
            return; // nothing the statement did is known yet
        }
        const bool inside_block =
            session_->transaction_status() != components::session::transaction_status_t::idle;
        if (!answer.has_error()) {
            const auto& payload = answer.value();
            for (const auto& change : payload.setting_changes) {
                client_setting_t<std::string>* followed = nullptr;
                switch (change.id) {
                    case components::catalog::setting_id::timezone:
                        followed = &timezone_;
                        break;
                    case components::catalog::setting_id::search_path:
                        followed = &search_path_;
                        break;
                    case components::catalog::setting_id::autocommit:
                        break; // not reported
                }
                if (followed != nullptr) {
                    followed->set(change.value ? *change.value : followed->reset, change.local, inside_block);
                }
            }
            if (payload.savepoint) {
                const auto each = [&](auto&& apply) {
                    apply(application_name_);
                    apply(client_encoding_);
                    apply(extra_float_digits_);
                    apply(timezone_);
                    apply(search_path_);
                };
                switch (payload.transaction) {
                    case otterstax::session::transaction_command_t::savepoint:
                        each([&](auto& setting) { setting.savepoint(*payload.savepoint); });
                        break;
                    case otterstax::session::transaction_command_t::release:
                        each([&](auto& setting) { setting.release(*payload.savepoint); });
                        break;
                    case otterstax::session::transaction_command_t::rollback_to:
                        each([&](auto& setting) { setting.rollback_to(*payload.savepoint); });
                        break;
                    default:
                        break;
                }
            }
        }
        // The block ended with this statement (or none is open): kept by a
        // COMMIT (not a COMMIT of a failed block, answered as a rollback),
        // dropped otherwise; a SET LOCAL ends either way.
        if (!inside_block) {
            const bool committed =
                !answer.has_error() && answer.value().transaction == otterstax::session::transaction_command_t::commit;
            application_name_.end_block(committed);
            client_encoding_.end_block(committed);
            extra_float_digits_.end_block(committed);
            timezone_.end_block(committed);
            search_path_.end_block(committed);
        }
    }

    void postgres_connection::reset_client_parameters() {
        OTX_ZONE_N("pg::reset_client_parameters");
        const bool inside_block =
            session_->transaction_status() != components::session::transaction_status_t::idle;
        application_name_.set(application_name_.reset, false, inside_block);
        client_encoding_.set(client_encoding_.reset, false, inside_block);
        extra_float_digits_.set(extra_float_digits_.reset, false, inside_block);
    }

    command_complete_tag postgres_connection::setting_tag(const protocol_setting_t& setting) const {
        return setting.reset ? command_complete_tag::reset()
                             : command_complete_tag::simple_command(T_VariableSetStmt, 0);
    }

    void postgres_connection::append_ready_for_query(std::vector<std::vector<uint8_t>>& response) {
        const auto report = [&](const char* name, const std::string& current, std::string& reported) {
            if (current == reported) {
                return;
            }
            reported = current;
            response.emplace_back(build_parameter_status(writer_, name, reported));
        };
        report("application_name", application_name_.current(), reported_.application_name);
        report("client_encoding", client_encoding_.current(), reported_.client_encoding);
        report("TimeZone", timezone_.current(), reported_.timezone);
        report("search_path", search_path_.current(), reported_.search_path);
        response.emplace_back(build_ready_for_query(writer_, ready_status()));
    }

    void postgres_connection::send_session_error(const otterstax::awaited_t<session_payload>& answer,
                                                 std::string_view prefix,
                                                 error_severity severity) {
        const auto& error = answer.error();
        if (answer.timed_out()) {
            // The statement goes on running; send_error_response fails an open
            // block behind it, so a COMMIT cannot publish what the client was told
            // is cancelled.
            send_error_response(sql_state::QUERY_CANCELED, "Query exceeded execution limit", severity);
        } else if (const char* state = sqlstate_of(error.type)) {
            send_error_response(state, std::string{error.what.c_str()}, severity);
        } else {
            send_error_response(sql_state::SYNTAX_ERROR, std::string{prefix} + error.what.c_str(), severity);
        }
    }

    bool postgres_connection::run_protocol_setting(const protocol_setting_t& setting) {
        OTX_ZONE_N("pg::run_protocol_setting");
        // As the engine refuses every statement of a failed block.
        if (block_aborted()) {
            refuse_in_aborted_block();
            return false;
        }
        // flatten_set_variable_args: a parameter that is no list takes one value.
        if (setting.several_values && (setting.parameter == nullptr || !setting.parameter->list_input)) {
            send_error_response(sql_state::INVALID_PARAMETER_VALUE, "SET " + setting.name + " takes only one argument");
            return false;
        }
        if (setting.client) {
            return set_client_parameter(setting);
        }
        if (setting.value) {
            if (auto refusal = check_protocol_parameter(*setting.parameter, *setting.value)) {
                // Inside a block the error fails it (send_error_response).
                send_error_response(sql_state::INVALID_PARAMETER_VALUE, std::move(*refusal));
                return false;
            }
        }
        // The value is what OtterStax gives already: nothing changes, so no
        // ParameterStatus follows (PostgreSQL reports a changed value only).
        return true;
    }

    bool postgres_connection::set_client_parameter(const protocol_setting_t& setting) {
        OTX_ZONE_N("pg::set_client_parameter");
        // A failed block was refused before: the session is idle or in an open
        // block.
        const bool inside_block =
            session_->transaction_status() != components::session::transaction_status_t::idle;
        switch (*setting.client) {
            case client_parameter_t::application_name: {
                auto& name = application_name_;
                std::string value = setting.value    ? clean_application_name(*setting.value)
                                    : setting.current ? name.current()
                                                      : name.reset;
                name.set(std::move(value), setting.local, inside_block);
                return true;
            }
            case client_parameter_t::client_encoding: {
                auto& encoding = client_encoding_;
                std::string value = setting.current ? encoding.current() : encoding.reset;
                if (setting.value) {
                    auto kept = client_encoding_value(*setting.value);
                    if (!kept) {
                        // Inside a block the error fails it (send_error_response).
                        send_error_response(sql_state::INVALID_PARAMETER_VALUE,
                                            client_encoding_refusal(*setting.value));
                        return false;
                    }
                    value = std::move(*kept);
                }
                encoding.set(std::move(value), setting.local, inside_block);
                return true;
            }
            case client_parameter_t::extra_float_digits: {
                auto& digits = extra_float_digits_;
                int value = setting.current ? digits.current() : digits.reset;
                if (setting.value) {
                    auto parsed = parse_extra_float_digits(*setting.value);
                    if (auto* refusal = std::get_if<std::string>(&parsed)) {
                        // Inside a block the error fails it (send_error_response).
                        send_error_response(sql_state::INVALID_PARAMETER_VALUE, std::move(*refusal));
                        return false;
                    }
                    value = std::get<int>(parsed);
                }
                digits.set(value, setting.local, inside_block);
                return true;
            }
        }
        return true;
    }

    std::vector<std::vector<uint8_t>> postgres_connection::answer_show(client_parameter_t parameter,
                                                                       const std::vector<result_encoding>& format,
                                                                       bool row_shape_known) {
        OTX_ZONE_N("pg::answer_show");
        std::pmr::vector<types::complex_logical_type> columns(resource_);
        columns.emplace_back(types::logical_type::STRING_LITERAL, std::string{client_parameter_name(parameter)});
        components::vector::data_chunk_t chunk(resource_, columns);
        std::string value;
        switch (parameter) {
            case client_parameter_t::application_name:
                value = application_name_.current();
                break;
            case client_parameter_t::client_encoding:
                value = client_encoding_.current();
                break;
            case client_parameter_t::extra_float_digits:
                value = std::to_string(extra_float_digits_.current());
                break;
        }
        chunk.set_value(0, 0, std::string_view{value});
        chunk.set_cardinality(1);
        postgres_resultset result(writer_, extra_float_digits_t{extra_float_digits_.current()}, row_shape_known);
        if (!row_shape_known) {
            result.add_chunk_columns(chunk);
        }
        result.add_encoding(format);
        result.add_row(chunk, 0);
        auto response = postgres_resultset::build_packets(std::move(result));
        response.emplace_back(
            build_command_complete(writer_, command_complete_tag::simple_command(T_VariableShowStmt, 1)));
        return response;
    }

    transaction_status postgres_connection::ready_status() const {
        switch (session_->transaction_status()) {
            case components::session::transaction_status_t::idle:
                return transaction_status::IDLE;
            case components::session::transaction_status_t::in_transaction:
                // A failure the session has been told of and not run yet.
                return failure_pending_ ? transaction_status::TRANSACTION_ERROR : transaction_status::IN_TRANSACTION;
            case components::session::transaction_status_t::failed:
                return transaction_status::TRANSACTION_ERROR;
        }
        return transaction_status::IDLE;
    }

    bool postgres_connection::block_aborted() const {
        return session_->transaction_status() == components::session::transaction_status_t::failed || failure_pending_;
    }

    void postgres_connection::refuse_in_aborted_block() {
        send_error_response(sql_state::IN_FAILED_SQL_TRANSACTION,
                            "current transaction is aborted, commands ignored until end of transaction block");
    }

    boost::asio::awaitable<bool> postgres_connection::run_deallocate(const deallocate_statement_t& statement,
                                                                     const std::string& sql) {
        bool own = false;
        {
            OTX_ZONE_N("pg::run_deallocate");
            own = statement.name.has_value() && !statement.name->empty() &&
                  statement_name_map_.find(*statement.name) != statement_name_map_.end();
            if (own && block_aborted()) {
                refuse_in_aborted_block();
                co_return false;
            }
        }
        if (own) {
            co_await drop_statement(*statement.name);
            co_return true;
        }
        // The engine's store (SQL PREPARE): its answer, 26000 for a name it
        // does not have either, 25P02 in a failed block.
        auto r = co_await execute_own(sql);
        if (r.has_error()) {
            send_session_error(r, "");
            co_return false;
        }
        if (!statement.name.has_value()) {
            // DEALLOCATE ALL: the named statements of the map too; the unnamed
            // one is the protocol's, not DEALLOCATE's.
            std::vector<std::string> names;
            names.reserve(statement_name_map_.size());
            for (const auto& entry : statement_name_map_) {
                if (!entry.first.empty()) {
                    names.push_back(entry.first);
                }
            }
            for (const auto& name : names) {
                co_await drop_statement(name);
            }
        }
        co_return true;
    }

    boost::asio::awaitable<void> postgres_connection::handle_deallocate(const deallocate_statement_t& statement,
                                                                        const std::string& sql) {
        if (!co_await run_deallocate(statement, sql)) {
            co_return;
        }
        OTX_ZONE_N("pg::handle_deallocate"); // no co_await follows
        std::vector<std::vector<uint8_t>> response;
        response.emplace_back(
            build_command_complete(writer_, command_complete_tag::deallocate(!statement.name.has_value())));
        append_ready_for_query(response);
        send_packet_merged(std::move(response));
    }

    boost::asio::awaitable<void> postgres_connection::handle_query(std::string query) {
        if (auto deallocate = deallocate_statement(resource_, query)) {
            co_await handle_deallocate(*deallocate, query);
            co_return;
        }
        // RESET ALL resets the engine's settings (the session runs it) and the
        // client parameters behind its success; DISCARD ALL too, which
        // PostgreSQL runs only outside a block (PreventInTransactionBlock).
        bool resets_client_parameters = false;
        {
            OTX_ZONE_N("pg::handle_query");
            if (auto setting = protocol_setting_statement(resource_, query)) {
                if (setting->all) {
                    resets_client_parameters = true;
                } else {
                    if (run_protocol_setting(*setting)) {
                        std::vector<std::vector<uint8_t>> response;
                        response.emplace_back(build_command_complete(writer_, setting_tag(*setting)));
                        append_ready_for_query(response);
                        send_packet_merged(std::move(response));
                    }
                    co_return;
                }
            } else if (discard_all_statement(resource_, query)) {
                if (block_aborted()) {
                    refuse_in_aborted_block();
                    co_return;
                }
                if (session_->transaction_status() != components::session::transaction_status_t::idle) {
                    send_error_response(sql_state::ACTIVE_SQL_TRANSACTION,
                                        "DISCARD ALL cannot run inside a transaction block");
                    co_return;
                }
                resets_client_parameters = true;
            } else if (const auto shown = show_client_parameter(resource_, query)) {
                // As the engine refuses every statement of a failed block.
                if (block_aborted()) {
                    refuse_in_aborted_block();
                    co_return;
                }
                auto response = answer_show(*shown, {result_encoding::TEXT}, false);
                append_ready_for_query(response);
                send_packet_merged(std::move(response));
                co_return;
            }
        }
        auto r = co_await execute_own(query);
        if (r.has_error()) {
            send_session_error(r, "");
            co_return;
        }
        session_payload sdata_result = std::move(r.value());
        if (resets_client_parameters) {
            reset_client_parameters();
        }

        if (ends_block(sdata_result)) {
            co_await release_portals(false); // portals are released at the transaction's end
        }
        OTX_ZONE_N("pg::handle_query::answer"); // no co_await follows
        int64_t rows_cnt = static_cast<int64_t>(sdata_result.reported_rows());
        std::vector<std::vector<uint8_t>> response;
        if (sdata_result.column_count() > 0) {
            // A column the text encoder cannot carry is an ErrorResponse, not a
            // dropped connection.
            if (auto bad = find_unsupported_column<frontend_type::POSTGRES>(sdata_result.chunks.front(),
                                                                           {result_encoding::TEXT})) {
                send_error_response(sql_state::FEATURE_NOT_SUPPORTED,
                                    unsupported_column_message<frontend_type::POSTGRES>(*bad));
                co_return;
            }

            postgres_resultset result(writer_, extra_float_digits_t{extra_float_digits_.current()});
            result.add_chunk_columns(sdata_result.chunks.front()); // default text encoding
            for (auto& ch : sdata_result.chunks) {
                for (size_t i = 0; i < ch.size(); ++i) {
                    result.add_row(ch, i);
                }
            }
            response = postgres_resultset::build_packets(std::move(result));
        }
        response.emplace_back(build_command_complete(writer_, tag_of(sdata_result, rows_cnt, query)));
        // A reported parameter the statement changed (a SET, the end of the
        // block a SET was in), as PostgreSQL sends it before the ReadyForQuery.
        append_ready_for_query(response);
        send_packet_merged(std::move(response));
    }

    boost::asio::awaitable<void>
    postgres_connection::handle_parse(std::string stmt, std::string query, int16_t num_params, packet_reader&& reader) {
        std::pmr::vector<int32_t> oids(resource_);
        std::pmr::vector<std::optional<field_type>> declared(resource_);
        {
            OTX_ZONE_N("pg::handle_parse");
            log_->info("[Connection {}] PARSE stmt: \"{}\", query: \"{}\"", connection_id_, stmt, query);
            // oid 0 (or one the frontend has no type for) leaves the type to the
            // engine's describe.
            for (int i = 0; i < num_params; ++i) {
                auto oid = reader.read_int32();
                oids.push_back(oid);
                if (get_field_type(oid) == field_type::NA) {
                    declared.emplace_back(std::nullopt);
                } else {
                    declared.emplace_back(static_cast<field_type>(oid));
                }
            }

            if (!reader.ok()) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated PARSE message parameter types");
                co_return;
            }
            // Nothing may follow the last parameter type.
            if (reader.remaining() != 0) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
                co_return;
            }
        }

        const auto id = session_->next_statement();
        // A statement the frontend keeps (DEALLOCATE, a protocol SET, SHOW of a
        // client parameter): run at Execute, never prepared by the session. It
        // uses no parameter, and PostgreSQL takes a Parse that declares types
        // for parameters the statement does not use: they are its parameters
        // (described, bound) all the same — but an undeclared one (oid 0) has
        // no type to be described by (42P18).
        // A coroutine awaited where it is called: `keep` and what it refers to
        // outlive it.
        const auto keep = [&](statement_run_t run) -> boost::asio::awaitable<void> {
            if (block_aborted()) {
                refuse_in_aborted_block();
                co_return;
            }
            std::pmr::vector<field_type> types(resource_);
            for (std::size_t i = 0; i < oids.size(); ++i) {
                if (oids[i] == 0) {
                    send_error_response(sql_state::INDETERMINATE_DATATYPE,
                                        "could not determine data type of parameter $" + std::to_string(i + 1));
                    co_return;
                }
                types.push_back(static_cast<field_type>(oids[i]));
            }
            co_await do_close(describe_close_arg::STATEMENT, stmt);
            const auto count = static_cast<uint32_t>(types.size());
            statement_name_map_.emplace(std::move(stmt), prepared_stmt_meta(std::move(run), count, std::move(types)));
            send_packet(build_parse_complete(writer_));
        };
        if (auto deallocate = deallocate_statement(resource_, query)) {
            statement_run_t run(resource_, id, query, types::complex_logical_type{}, T_DeallocateStmt);
            run.deallocate = std::move(*deallocate);
            co_await keep(std::move(run));
            co_return;
        }
        // So is a SET of a protocol parameter: checked at Execute, as
        // PostgreSQL applies a SET when it runs. RESET ALL is the engine's,
        // the client parameters reset behind it.
        auto setting = protocol_setting_statement(resource_, query);
        if (setting && !setting->all) {
            statement_run_t run(resource_, id, query, types::complex_logical_type{}, T_VariableSetStmt);
            run.protocol_setting = std::move(*setting);
            co_await keep(std::move(run));
            co_return;
        }
        // And SHOW of a client parameter: answered at Execute, described by
        // its one text column.
        if (const auto shown = show_client_parameter(resource_, query)) {
            std::pmr::vector<types::complex_logical_type> columns(resource_);
            columns.emplace_back(types::logical_type::STRING_LITERAL, std::string{client_parameter_name(*shown)});
            statement_run_t run(resource_,
                                id,
                                query,
                                types::complex_logical_type::create_struct("", columns),
                                T_VariableShowStmt);
            run.show = *shown;
            co_await keep(std::move(run));
            co_return;
        }
        // The engine describes the statement (a failed block refuses it, but
        // a COMMIT / ROLLBACK / ROLLBACK TO, with 25P02, as PostgreSQL's Parse).
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::prepare_schema,
                                                                    id,
                                                                    query);
        auto r = co_await await_session(std::move(fut));

        if (r.has_error()) {
            send_session_error(r, "Syntax error: ");
            co_return;
        }

        session_payload result = std::move(r.value());
        log_->debug("[Connection {}] PARSE stmt: query: \"{}\", param_cnt={}",
                    connection_id_,
                    query,
                    result.parameter_count);

        if (declared.size() > result.parameter_count) {
            co_await close_session_statement(id);
            send_error_response(sql_state::UNDEFINED_PARAMETER,
                                "Parameter types specified for " + std::to_string(declared.size()) +
                                    " parameter(s), the statement has " + std::to_string(result.parameter_count));
            co_return;
        }
        // The client's type, else the engine's, else text.
        std::pmr::vector<field_type> parameter_types(resource_);
        parameter_types.reserve(result.parameter_count);
        for (size_t i = 0; i < result.parameter_count; ++i) {
            if (i < declared.size() && declared[i]) {
                parameter_types.push_back(*declared[i]);
                continue;
            }
            std::optional<field_type> engine;
            if (i < result.parameter_types.size()) {
                engine = get_field_type(result.parameter_types[i].type());
            }
            parameter_types.push_back(engine.value_or(field_type::TEXT));
        }

        // statements with identical name replace each other, together with their portals
        co_await do_close(describe_close_arg::STATEMENT, stmt);
        statement_run_t run(resource_, id, query, std::move(result.schema), result.tag);
        run.resets_client_parameters = setting.has_value(); // RESET ALL
        prepared_stmt_meta meta(std::move(run), result.parameter_count, std::move(parameter_types));
        meta.transaction_exit = result.transaction == otterstax::session::transaction_command_t::commit ||
                                result.transaction == otterstax::session::transaction_command_t::rollback ||
                                result.transaction == otterstax::session::transaction_command_t::rollback_to;
        statement_name_map_.emplace(std::move(stmt), std::move(meta));
        send_packet(build_parse_complete(writer_));
    }

    boost::asio::awaitable<void> postgres_connection::handle_bind(std::string stmt,
                                                                  std::string portal_name,
                                                                  std::vector<result_encoding> format,
                                                                  int16_t num_params,
                                                                  packet_reader&& reader) {
        auto it = statement_name_map_.end();
        portal_t params(resource_);
        std::vector<result_encoding> out_format;
        {
            OTX_ZONE_N("pg::handle_bind");
            log_->info("[Connection {}] BIND stmt: \"{}\", portal: \"{}\"", connection_id_, stmt, portal_name);
            it = statement_name_map_.find(stmt);
            if (it == statement_name_map_.end()) {
                send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME,
                                    "Unknown prepared statement in BIND: " + stmt);
                co_return;
            }

            auto& statement = it->second;
            // As PostgreSQL's exec_bind_message: in an aborted block only a
            // transaction exit without parameters is bound.
            if (block_aborted() && (!statement.transaction_exit || num_params != 0)) {
                refuse_in_aborted_block();
                co_return;
            }
            if (num_params != statement.parameter_count) {
                send_error_response(sql_state::UNDEFINED_PARAMETER,
                                    "Missing parameters in BIND to statement " + stmt + ": received " +
                                        std::to_string(num_params) + " out of required " +
                                        std::to_string(statement.parameter_count));
                co_return;
            }

            params.reserve(num_params);
            for (int16_t i = 0; i < num_params; ++i) {
                result_encoding encoding;
                if (auto encoding_opt = get_format_code(format, i); !encoding_opt) {
                    send_error_response(sql_state::UNDEFINED_PARAMETER,
                                        "Missing parameters format codes in BIND to statement " + stmt + ": received " +
                                            std::to_string(format.size()) + " out of required " +
                                            std::to_string(statement.parameter_count));
                    co_return;
                } else {
                    encoding = *encoding_opt;
                }

                if (reader.remaining() < 4) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Missing BIND message parameter value length");
                    co_return;
                }

                int32_t len = reader.read_int32();
                if (len == -1) {
                    params.push_back(components::vector::make_null(
                        resource_, components::types::complex_logical_type{components::types::logical_type::NA}));
                    continue;
                }

                if (reader.remaining() < len) {
                    send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND parameter value");
                    co_return;
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
                        co_return;
                    }
                    params.emplace_back(std::move(parsed.value()));
                    continue;
                }

                switch (type) {
                    case field_type::BOOL:
                        if (len != 1) {
                            send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid BOOL binary length");
                            co_return;
                        }
                        params.push_back(components::vector::make_scalar(resource_, reader.read_uint8() != 0));
                        break;
                    case field_type::INT2:
                        if (len != 2) {
                            send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT2 binary length");
                            co_return;
                        }
                        params.push_back(components::vector::make_scalar(resource_, reader.read_int16()));
                        break;
                    case field_type::INT4:
                        if (len != 4) {
                            send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT4 binary length");
                            co_return;
                        }
                        params.push_back(components::vector::make_scalar(resource_, reader.read_int32()));
                        break;
                    case field_type::INT8:
                        if (len != 8) {
                            send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid INT8 binary length");
                            co_return;
                        }
                        params.push_back(components::vector::make_scalar(resource_, reader.read_int64()));
                        break;
                    case field_type::FLOAT4: {
                        if (len != 4) {
                            send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid FLOAT4 binary length");
                            co_return;
                        }
                        uint32_t raw = reader.read_uint32();
                        params.push_back(components::vector::make_scalar(resource_, std::bit_cast<float>(raw)));
                        break;
                    }
                    case field_type::FLOAT8: {
                        if (len != 8) {
                            send_error_response(sql_state::PROTOCOL_VIOLATION, "Invalid FLOAT8 binary length");
                            co_return;
                        }
                        uint64_t raw = reader.read_uint64();
                        params.push_back(components::vector::make_scalar(resource_, std::bit_cast<double>(raw)));
                        break;
                    }
                    case field_type::TEXT: {
                        std::string str(len, '\0');
                        for (int j = 0; j < len; str[j++] = static_cast<char>(reader.read_uint8()))
                            ;
                        params.push_back(components::vector::make_scalar(resource_, std::string_view{str}));
                        break;
                    }
                    default:
                        send_error_response(sql_state::FEATURE_NOT_SUPPORTED, "Unsupported parameter type");
                        co_return;
                }
            }

            if (reader.remaining() < 2) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message result format codes");
                co_return;
            }

            auto out_format_len = reader.read_int16();
            if (reader.remaining() < 2 * out_format_len) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message result format codes");
                co_return;
            }

            out_format.reserve(out_format_len);
            for (int16_t i = 0; i < out_format_len; ++i) {
                out_format.emplace_back(static_cast<result_encoding>(reader.read_int16()));
            }

            // Every fixed-size field above was pre-checked; this is the reader's
            // own verdict on the message, checked before anything is kept from it.
            if (!reader.ok()) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, "Truncated BIND message");
                co_return;
            }
            // Nothing may follow the last result format code.
            if (reader.remaining() != 0) {
                send_error_response(sql_state::PROTOCOL_VIOLATION, INVALID_MESSAGE_FORMAT);
                co_return;
            }

        }

        // add & create portal; portals with identical name replace each other
        if (auto replaced = portals_.find(portal_name); replaced != portals_.end()) {
            co_await release_portal(replaced, true);
        }
        // Nothing else of this connection runs while the window closes: `it` holds.
        const auto& statement = it->second;
        portals_.emplace(std::move(portal_name),
                         portal_meta(std::move(params),
                                     it->first,
                                     statement_run_t(statement.run, resource_),
                                     statement.described,
                                     std::move(out_format)));

        send_packet(build_bind_complete(writer_));
    }

    boost::asio::awaitable<void> postgres_connection::close_window(portal_meta& portal) {
        session_future fut;
        {
            OTX_ZONE_N("pg::close_window");
            if (portal.window == 0 || session_ == nullptr) {
                co_return;
            }
            fut = actor_zeta::send(session_->address(),
                                   &otterstax::session::query_session_t::close_window,
                                   portal.window)
                      .second;
        }
        auto r = co_await await_session(std::move(fut));
        if (r.has_error()) {
            log_->warn("[Connection {}] the window of a portal could not be closed: {}", connection_id_, r.error().what);
        }
        portal.window = 0;
        portal.done = true;
    }

    boost::asio::awaitable<postgres_connection::portal_iterator>
    postgres_connection::release_portal(portal_iterator it, bool close_windows) {
        if (close_windows) {
            co_await close_window(it->second);
        }
        OTX_ZONE_N("pg::release_portal"); // after the only co_await
        co_return portals_.erase(it);
    }

    boost::asio::awaitable<void> postgres_connection::release_portals(bool close_windows) {
        for (auto it = portals_.begin(); it != portals_.end();) {
            it = co_await release_portal(it, close_windows);
        }
    }

    boost::asio::awaitable<void>
    postgres_connection::close_session_statement(otterstax::session::statement_id_t stmt_session) {
        if (session_ == nullptr) {
            co_return;
        }
        // a query session schedules itself: needs_sched is always false
        [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session_->address(),
                                                                    &otterstax::session::query_session_t::close_statement,
                                                                    stmt_session);
        auto r = co_await await_session(std::move(fut));
        if (r.has_error()) {
            log_->warn("[Connection {}] close_statement failed: {}", connection_id_, r.error().what);
        }
    }

    boost::asio::awaitable<std::optional<otterstax::session::statement_id_t>>
    postgres_connection::portal_entry(const portal_meta& portal, bool& temporary) {
        temporary = false;
        otterstax::session::statement_id_t id{};
        session_future fut;
        {
            OTX_ZONE_N("pg::portal_entry");
            if (auto it = statement_name_map_.find(portal.statement_name);
                it != statement_name_map_.end() && it->second.run.stmt_session == portal.run.stmt_session) {
                co_return portal.run.stmt_session;
            }
            // The statement was DEALLOCATEd since the Bind: the portal prepares its
            // text again, as PostgreSQL's portal keeps its plan.
            id = session_->next_statement();
            fut = actor_zeta::send(session_->address(),
                                   &otterstax::session::query_session_t::prepare_schema,
                                   id,
                                   std::string{portal.run.sql})
                      .second;
        }
        auto r = co_await await_session(std::move(fut));
        if (r.has_error()) {
            send_session_error(r, "Syntax error: ");
            co_return std::nullopt;
        }
        temporary = true;
        co_return id;
    }

    boost::asio::awaitable<void> postgres_connection::handle_execute(std::string portal_name, int32_t limit) {
        auto it = portals_.end();
        {
            OTX_ZONE_N("pg::handle_execute");
            log_->info("[Connection {}] EXECUTE portal: \"{}\"", connection_id_, portal_name);
            it = portals_.find(portal_name);
            if (it == portals_.end()) {
                send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME,
                                    "Unknown portal in EXECUTE: \"" + portal_name + "\"");
                co_return;
            }
            const auto& portal = it->second;
            if (portal.done && (portal.run.deallocate || portal.run.protocol_setting)) {
                // PortalRun: a portal of a utility statement (PORTAL_MULTI_QUERY)
                // that ran is done, and MarkPortalActive refuses to run it again.
                send_error_response(sql_state::OBJECT_NOT_IN_PREREQUISITE_STATE,
                                    "portal \"" + portal_name + "\" cannot be run");
                co_return;
            }
        }

        auto& portal = it->second;
        const auto& run = portal.run;
        if (run.deallocate) {
            // run_deallocate may drop statements, never the portal this runs.
            const auto statement = *run.deallocate;
            const std::string sql{run.sql};
            portal.done = true;
            if (co_await run_deallocate(statement, sql)) {
                send_packet(build_command_complete(writer_, command_complete_tag::deallocate(!statement.name.has_value())));
            }
            co_return;
        }
        if (run.protocol_setting) {
            portal.done = true;
            if (run_protocol_setting(*run.protocol_setting)) {
                send_packet(build_command_complete(writer_, setting_tag(*run.protocol_setting)));
            }
            co_return;
        }
        if (portal.done) {
            // A portal that ran to its end answers no more rows.
            send_packet(build_command_complete(writer_, command_complete_tag::simple_command(run.tag, 0)));
            co_return;
        }
        if (run.show) {
            // PostgreSQL's exec_execute_message: a failed block runs nothing but
            // a transaction exit.
            if (block_aborted()) {
                refuse_in_aborted_block();
                co_return;
            }
            const bool row_shape_known = portal.statement_described || portal.described;
            portal.described = true;
            portal.done = true;
            send_packet_merged(answer_show(*run.show, portal.format, row_shape_known));
            co_return;
        }

        // A row limit runs the statement under an engine window; the next
        // Execute fetches on from it.
        const uint64_t count = limit > 0 ? static_cast<uint64_t>(limit) : components::logical_plan::fetch_all;
        // The session entry this Execute prepared for itself (its statement was
        // DEALLOCATEd since the Bind), closed once it ran.
        std::optional<otterstax::session::statement_id_t> temporary_entry;
        actor_zeta::unique_future<otterstax::session::query_session_t::session_result> fut;
        if (portal.window != 0) {
            fut = actor_zeta::send(session_->address(),
                                   &otterstax::session::query_session_t::fetch_window,
                                   portal.window,
                                   count)
                      .second;
        } else {
            bool temporary = false;
            const auto entry = co_await portal_entry(portal, temporary);
            if (!entry) {
                co_await release_portal(it, false);
                co_return;
            }
            if (temporary) {
                temporary_entry = entry;
            }
            if (limit > 0) {
                fut = actor_zeta::send(session_->address(),
                                       &otterstax::session::query_session_t::execute_window,
                                       *entry,
                                       otterstax::session::parameter_row(resource_, std::move(portal.params)),
                                       uint64_t{0},
                                       count)
                          .second;
            } else {
                fut = actor_zeta::send(session_->address(),
                                       &otterstax::session::query_session_t::execute_prepared_statement,
                                       *entry,
                                       otterstax::session::parameter_row(resource_, std::move(portal.params)),
                                       uint64_t{0})
                          .second;
            }
        }
        auto r = co_await await_session(std::move(fut));
        if (temporary_entry) {
            co_await close_session_statement(*temporary_entry);
        }
        if (r.has_error()) {
            // A failed Execute ends the portal: a fetch that fails ends the
            // window (the engine's rule), and the values were handed to the run
            // — an Execute again finds no portal rather than running the
            // statement once more without them.
            portal.window = 0;
            co_await release_portal(it, false);
            send_session_error(r, "Syntax error: ");
            co_return;
        }
        bool block_ended = false;
        {
            OTX_ZONE_N("pg::handle_execute::answer");
            session_payload& payload = r.value();
            // A fetch answers rows of the statement: CommandComplete names it.
            payload.tag = run.tag;
            portal.window = payload.has_more ? payload.portal : 0;
            portal.done = !payload.has_more;

            // PostgreSQL's guarantee: the row shape a client was handed before Bind
            // is the shape the rows arrive in; a divergence is 0A000 "cached plan
            // must not change result type" (plancache.c, RevalidateCachedQuery). A
            // portal the client described itself is not held against the
            // statement's older answer.
            if (portal.statement_described && !portal.described && payload.column_count() > 0) {
                assert(run.schema.type() == types::logical_type::STRUCT &&
                       "a described statement was described from a STRUCT schema");
                if (!same_wire_shape(run.schema.child_types(), payload.chunks.front())) {
                    log_->warn("[Connection {}] EXECUTE portal \"{}\": described {} column(s), executed {}",
                               connection_id_,
                               portal_name,
                               run.schema.child_types().size(),
                               payload.column_count());
                    send_error_response(sql_state::FEATURE_NOT_SUPPORTED, "cached plan must not change result type");
                    co_return;
                }
            }

            std::vector<std::vector<uint8_t>> response;
            // CommandComplete reports the rows of THIS Execute: the rows sent for a
            // query, the affected count for a DML statement.
            int64_t rows_cnt = static_cast<int64_t>(payload.reported_rows());
            if (payload.column_count() > 0) {
                // The result formats are the portal's; the executed columns may
                // differ from the described schema, so every Execute checks them.
                if (auto bad =
                        find_unsupported_column<frontend_type::POSTGRES>(payload.chunks.front(), portal.format)) {
                    send_error_response(sql_state::FEATURE_NOT_SUPPORTED,
                                        unsupported_column_message<frontend_type::POSTGRES>(*bad));
                    co_return;
                }
                // Execute itself never carries the row shape; a client that skipped
                // Describe gets it once, in front of the first rows of the portal.
                const bool row_shape_known = portal.statement_described || portal.described;
                postgres_resultset result(writer_,
                                          extra_float_digits_t{extra_float_digits_.current()},
                                          row_shape_known);
                if (!row_shape_known) {
                    result.add_chunk_columns(payload.chunks.front());
                    portal.described = true;
                }
                result.add_encoding(portal.format);
                for (auto& ch : payload.chunks) {
                    for (size_t i = 0; i < ch.size(); ++i) {
                        result.add_row(ch, i);
                    }
                }
                response = postgres_resultset::build_packets(std::move(result));
                if (payload.has_more) {
                    response.emplace_back(build_portal_suspended(writer_));
                    send_packet_merged(std::move(response));
                    co_return;
                }
            }

            response.emplace_back(build_command_complete(writer_, tag_of(payload, rows_cnt, run.sql)));
            // RESET ALL: the client parameters too. What changed is reported in
            // front of the Sync's ReadyForQuery, as PostgreSQL reports it.
            if (run.resets_client_parameters) {
                reset_client_parameters();
            }
            block_ended = ends_block(payload);
            send_packet_merged(std::move(response));
        }
        if (block_ended) {
            co_await release_portals(false); // portals are released at the transaction's end
        }
    }

    boost::asio::awaitable<void> postgres_connection::handle_close(describe_close_arg type, std::string name) {
        {
            OTX_ZONE_N("pg::handle_close");
            log_->info("[Connection {}] CLOSE name: \"{}\" type:\"{}\"",
                       connection_id_,
                       name,
                       static_cast<char>(type));
        }
        co_await do_close(type, std::move(name));
        send_packet(build_close_complete(writer_));
    }

    boost::asio::awaitable<void> postgres_connection::do_close(frontend::postgres::describe_close_arg type,
                                                               std::string name) {
        if (type == describe_close_arg::STATEMENT) {
            if (auto it = statement_name_map_.find(name); it != statement_name_map_.end()) {
                // Closing a statement closes the portals bound from it
                // (protocol): those of its name and its session entry — a
                // portal of an earlier statement of the name stays.
                const auto entry = it->second.run.stmt_session;
                for (auto portal = portals_.begin(); portal != portals_.end();) {
                    if (portal->second.statement_name == name && portal->second.run.stmt_session == entry) {
                        portal = co_await release_portal(portal, true);
                    } else {
                        portal = std::next(portal);
                    }
                }
                co_await drop_statement(name);
            }
        } else if (auto portal = portals_.find(name); portal != portals_.end()) {
            co_await release_portal(portal, true);
        }
    }

    boost::asio::awaitable<void> postgres_connection::drop_statement(std::string name) {
        auto it = statement_name_map_.find(name);
        if (it == statement_name_map_.end()) {
            co_return;
        }
        // The session keeps the statement until it is closed. A portal still
        // bound from it (DEALLOCATE leaves them) prepares its text again at its
        // Execute (portal_entry). The entry is forgotten before the close is
        // awaited.
        const bool in_session = !it->second.run.frontend_only();
        const auto stmt_session = it->second.run.stmt_session;
        statement_name_map_.erase(it);
        if (in_session) {
            co_await close_session_statement(stmt_session);
        }
    }

    boost::asio::awaitable<void> postgres_connection::handle_describe(describe_close_arg type, std::string name) {
        log_->info("[Connection {}] DESCRIBE name: \"{}\" type:\"{}\"", connection_id_, name, static_cast<char>(type));

        std::vector<std::vector<uint8_t>> packets;
        if (type == describe_close_arg::STATEMENT) {
            auto it = statement_name_map_.find(name);
            if (it == statement_name_map_.end()) {
                send_error_response(sql_state::INVALID_SQL_STATAMENT_NAME, "Statement " + name + " does not exist");
                co_return;
            }
            // A statement has no result formats yet (they come with Bind), so its
            // RowDescription is text; ParameterDescription always precedes it.
            auto& statement = it->second;
            packets.emplace_back(build_parameter_description(writer_, statement.specified_types));
            if (!do_describe(statement.run.schema, {result_encoding::TEXT}, packets, statement.described)) {
                co_return;
            }
            // A portal already bound from it (Parse, Bind, Describe(statement),
            // Execute): the client has the row shape now too.
            for (auto& [portal_name, portal] : portals_) {
                if (portal.statement_name == name && portal.run.stmt_session == statement.run.stmt_session) {
                    portal.statement_described = statement.described;
                }
            }
        } else {
            auto it = portals_.find(name);
            if (it == portals_.end()) {
                send_error_response(sql_state::INVALID_NAME, "Portal " + name + " does not exist");
                co_return;
            }
            // A portal is a BOUND statement: the engine describes it with the
            // Bind's values in it (a `$n` takes its value's type), and runs
            // nothing — a DML described this way writes nothing.
            auto& portal = it->second;
            if (portal.run.frontend_only()) {
                // A SHOW's one column, NoData for the others.
                if (!do_describe(portal.run.schema, portal.format, packets, portal.described)) {
                    co_return;
                }
            } else {
                bool temporary = false;
                const auto entry = co_await portal_entry(portal, temporary);
                if (!entry) {
                    co_return;
                }
                [[maybe_unused]] auto [needs_sched, fut] =
                    actor_zeta::send(session_->address(),
                                     &otterstax::session::query_session_t::describe_bound,
                                     *entry,
                                     copy_row(resource_, portal.params),
                                     uint64_t{0});
                auto r = co_await await_session(std::move(fut));
                if (temporary) {
                    co_await close_session_statement(*entry);
                }
                if (r.has_error()) {
                    send_session_error(r, "");
                    co_return;
                }
                if (!do_describe(r.value().schema, portal.format, packets, portal.described)) {
                    co_return;
                }
            }
        }
        OTX_ZONE_N("pg::handle_describe"); // after every co_await
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
