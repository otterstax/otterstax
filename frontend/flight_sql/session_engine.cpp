// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "session_engine.hpp"

#include "frontend/common/asio_future_bridge.hpp"
#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/translators/input/arrow_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "otterbrix/translators/output/writable_columns.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

using namespace components::types;

namespace flight::engine {
    namespace {
        // Every error leaving the adapter is an EngineError: the Flight SQL
        // core maps it to INVALID_ARGUMENT with the text attached.
        // `::core` is the global namespace; a bare `core` here would resolve
        // to our own flight::core.
        [[noreturn]] void engine_error(const otterstax::awaited_t<session_payload>& answer, const char* stage) {
            if (answer.timed_out()) {
                throw core::EngineError(std::string{"Timeout while "} + stage);
            }
            throw core::EngineError(std::string{"Error while "} + stage + ": " + answer.error().what.c_str());
        }

        // The schema handed out is the stream's contract: a row-producing
        // statement whose columns the batch stream cannot encode (a nested
        // STRUCT/LIST) is refused before a ticket exists, exactly where the
        // old Arrow-based frontend refused it.
        void check_stream_encodable(const arrow::Schema& schema) {
            for (const auto& field : schema.fields()) {
                if (arrow::is_nested(field->type()->id())) {
                    throw core::EngineError("the record batch stream encodes scalar columns only: column '" +
                                            field->name() + "' has nested type " + field->type()->ToString());
                }
            }
        }

        core::QueryResult to_query_result(std::pmr::memory_resource* resource, session_payload& payload) {
            core::QueryResult result;
            auto schema = to_arrow_schema(resource, payload.schema);
            if (schema.has_error()) {
                throw core::EngineError(schema.error().what.c_str());
            }
            check_stream_encodable(*schema.value());
            result.schema = std::move(schema.value());
            for (auto& chunk : payload.chunks) {
                if (chunk.empty()) {
                    // an empty chunk is not end-of-stream — keep scanning
                    continue;
                }
                auto batch = chunk_to_record_batch(resource, chunk);
                if (batch.has_error()) {
                    throw core::EngineError(batch.error().what.c_str());
                }
                result.batches.push_back(std::move(batch.value()));
            }
            return result;
        }

        // A SQL string literal / quoted identifier of `text`: an embedded quote doubled.
        std::string enquote(std::string_view text, char quote) {
            std::string out(1, quote);
            for (const char c : text) {
                if (c == quote) {
                    out.push_back(quote);
                }
                out.push_back(c);
            }
            out.push_back(quote);
            return out;
        }
        std::string literal(std::string_view text) { return enquote(text, '\''); }

        // The schemas a client may list (n), with their database (d): every
        // one but the system schemas and the database `federation` OtterStax
        // keeps its own tables in.
        std::string listed_schemas_where() {
            return " WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND NOT (d.datconnection = '' AND "
                   "d.datname = " +
                   literal(otterstax::federation::metadata_database) + ")";
        }
        constexpr std::string_view schemas_from =
            " FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_database d ON n.nspdatabase = d.oid";

        // The database `catalog` names, as catalog_of writes it: `d` local,
        // `c.d` of the connection c (an alias has no dot, the first one splits).
        std::string catalog_filter(const std::optional<std::string>& catalog) {
            if (!catalog) {
                return {};
            }
            std::string filter = " AND ((d.datconnection = '' AND d.datname = " + literal(*catalog) + ")";
            if (const auto dot = catalog->find('.'); dot != std::string::npos) {
                filter += " OR (d.datconnection = " + literal(catalog->substr(0, dot)) +
                          " AND d.datname = " + literal(catalog->substr(dot + 1)) + ")";
            }
            return filter + ")";
        }

        std::string like_filter(std::string_view column, const std::string& pattern) {
            return pattern.empty() ? std::string{} : " AND " + std::string{column} + " LIKE " + literal(pattern);
        }

        // Every row of `payload` as its text cells (the listing selects text
        // columns only; a NULL is empty).
        std::vector<std::vector<std::string>> text_rows(const session_payload& payload) {
            std::vector<std::vector<std::string>> rows;
            for (const auto& chunk : payload.chunks) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    auto& cells = rows.emplace_back();
                    for (const auto& column : chunk.data) {
                        if (column.is_null(row) || column.type().type() != logical_type::STRING_LITERAL) {
                            cells.emplace_back();
                        } else {
                            cells.emplace_back(column.get_value<std::string_view>(row));
                        }
                    }
                }
            }
            return rows;
        }
    } // namespace

    SessionEngine::SessionEngine(otterstax::session::session_factory_t& sessions, std::pmr::memory_resource* resource)
        : sessions_(sessions)
        , resource_(resource) {
        assert(resource_ != nullptr && "memory resource must not be null");
    }

    template<typename Session, typename Send>
    session_payload SessionEngine::call(const Session& session, Send send, const char* stage) {
        query_session_t::unique_future<query_session_t::session_result> future;
        if (!session([&](query_session_t& target) { future = send(target); })) {
            throw core::EngineError(std::string{"Error while "} + stage + ": the prepared statement was closed");
        }
        auto r = otterstax::await_future_blocking<session_payload>(std::move(future), resource_);
        if (r.has_error()) {
            engine_error(r, stage);
        }
        return std::move(r.value());
    }

    template<typename Session>
    session_payload SessionEngine::run_query(const Session& session, const std::string& sql) {
        OTX_ZONE_N("flight::SessionEngine::run_query");
        // a query session schedules itself: needs_sched is always false
        return call(
            session,
            [&](query_session_t& target) {
                return actor_zeta::send(target.address(), &query_session_t::execute, target.next_statement(), sql)
                    .second;
            },
            "executing the statement");
    }

    template<typename Session>
    std::pair<otterstax::session::statement_id_t, session_payload>
    SessionEngine::prepare_fresh(const Session& session, const std::string& sql) {
        OTX_ZONE_N("flight::SessionEngine::prepare_fresh");
        otterstax::session::statement_id_t id{};
        auto payload = call(
            session,
            [&](query_session_t& target) {
                id = target.next_statement();
                return actor_zeta::send(target.address(), &query_session_t::prepare_schema, id, sql).second;
            },
            "preparing the statement");
        return {id, std::move(payload)};
    }

    std::pmr::vector<components::vector::data_chunk_t> SessionEngine::to_param_chunks(const core::BoundParams& params) {
        std::pmr::vector<components::vector::data_chunk_t> chunks(resource_);
        for (const auto& batch : params) {
            if (!batch || batch->num_rows() == 0) {
                continue;
            }
            // The project's own reader: an arrow batch -> engine chunk, whose
            // rows are what the binder takes.
            auto converted = tsl::arrow_to_chunk(resource_, batch);
            if (converted.has_error()) {
                throw core::EngineError(converted.error().what.c_str());
            }
            for (auto& chunk : converted.value()) {
                chunks.push_back(std::move(chunk));
            }
        }
        return chunks;
    }

    template<typename Session>
    session_payload SessionEngine::run_prepared(const Session& session,
                                                const core::Prepared& prepared,
                                                const core::BoundParams& params) {
        OTX_ZONE_N("flight::SessionEngine::run_prepared");
        auto parameters = to_param_chunks(params);
        std::size_t rows = 0;
        for (const auto& chunk : parameters) {
            rows += chunk.size();
        }
        if (prepared.parameter_count > 0 && rows != 1 && rows != 0) {
            // executemany goes row-by-row upstream; a single run takes one row
            throw core::EngineError("prepared statement takes one parameter row per execution, got " +
                                    std::to_string(rows));
        }
        const std::size_t columns = parameters.empty() ? 0 : parameters.front().column_count();
        if (prepared.parameter_count > 0 && (rows == 0 || columns != prepared.parameter_count)) {
            throw core::EngineError("prepared statement takes " + std::to_string(prepared.parameter_count) +
                                    " parameter(s), got " + std::to_string(rows == 0 ? 0 : columns));
        }
        auto bound =
            parameters.empty()
                ? components::vector::data_chunk_t(resource_,
                                                   std::pmr::vector<components::types::complex_logical_type>{resource_},
                                                   1)
                : std::move(parameters.front());
        // The entry CreatePreparedStatement left: every execution parses its text again and binds
        // its own values there (query_session_t::execute_prepared_statement).
        return call(
            session,
            [&](query_session_t& target) {
                return actor_zeta::send(target.address(),
                                        &query_session_t::execute_prepared_statement,
                                        prepared.statement,
                                        std::move(bound),
                                        uint64_t{0})
                    .second;
            },
            "executing the prepared statement");
    }

    template<typename Session>
    void SessionEngine::refuse_open_transaction(const Session& session) {
        auto status = query_session_t::transaction_status_t::idle;
        if (!session([&](query_session_t& target) { status = target.transaction_status(); })) {
            throw core::EngineError("the prepared statement was closed");
        }
        if (status == query_session_t::transaction_status_t::idle) {
            return;
        }
        // No Flight SQL scope outlives what it runs: a statement by text closes
        // its session with the RPC, and a prepared statement's session runs that
        // one statement only. A transaction opened in it could never be ended
        // by the client (its COMMIT would run on another session), so it is
        // rolled back here and the statement refused.
        try {
            run_query(session, "ROLLBACK");
        } catch (const core::EngineError& e) {
            get_logger(logger_tag::FLIGHTSQL_SERVER)
                ->warn("ROLLBACK of a transaction opened over Flight SQL failed: {}", e.what());
        }
        throw core::EngineError("a transaction cannot be opened over Flight SQL: every statement runs on a session "
                                "of its own (autocommit), so the transaction was rolled back");
    }

    template<typename Session>
    void SessionEngine::close_quietly(const Session& session, otterstax::session::statement_id_t id) {
        try {
            call(
                session,
                [&](query_session_t& target) {
                    return actor_zeta::send(target.address(), &query_session_t::close_statement, id).second;
                },
                "closing the prepared statement");
        } catch (const core::EngineError& e) {
            get_logger(logger_tag::FLIGHTSQL_SERVER)
                ->warn("close_statement of a prepared statement failed: {}", e.what());
        }
    }

    core::QueryResult SessionEngine::execute(const std::string& query) {
        OTX_ZONE_N("flight::SessionEngine::execute");
        // The RPC's own session, closed when it returns.
        auto scope = sessions_.open();
        auto session = [&scope](auto&& use) {
            use(*scope);
            return true;
        };
        // Two-phase: execute hands back the rows but an EMPTY payload schema
        // (only the prepare path resolves one), so the statement is prepared
        // first — its payload carries the result schema — and executed under the
        // same statement id. A statement that is not a SELECT (DDL/DML) takes the
        // same path and answers an empty dataset: the python flightsql client
        // drives EVERYTHING through the query RPC.
        const auto [id, prepared_payload] = prepare_fresh(session, query);
        if (prepared_payload.tag == NodeTag::T_SelectStmt) {
            if (prepared_payload.parameter_count > 0) {
                throw core::EngineError("the statement has " + std::to_string(prepared_payload.parameter_count) +
                                        " unbound parameter(s); bind them through a prepared statement");
            }
            // The schema handed out is the stream's contract: a row-producing
            // statement whose prepare left it unresolved cannot be served.
            if (prepared_payload.schema.type() != logical_type::STRUCT) {
                throw core::EngineError("the result schema of the statement could not be resolved");
            }
        }

        session_payload payload = call(
            session,
            [&, id = id](query_session_t& target) {
                return actor_zeta::send(target.address(), &query_session_t::execute_statement, id).second;
            },
            "executing the statement");
        refuse_open_transaction(session);

        if (payload.column_count() == 0) {
            // DDL / DML over the query RPC: no result set, no rows. Always a
            // VALID (empty) schema: a null one would fail downstream.
            return core::QueryResult{arrow::schema({}), {}};
        }
        return to_query_result(resource_, payload);
    }

    std::int64_t SessionEngine::execute_update(const std::string& query) {
        OTX_ZONE_N("flight::SessionEngine::execute_update");
        auto scope = sessions_.open();
        auto session = [&scope](auto&& use) {
            use(*scope);
            return true;
        };
        const auto affected = static_cast<std::int64_t>(run_query(session, query).reported_rows());
        refuse_open_transaction(session);
        return affected;
    }

    std::pair<core::Prepared, otterstax::session::session_ptr> SessionEngine::prepare(const std::string& query) {
        OTX_ZONE_N("flight::SessionEngine::prepare");
        auto scope = sessions_.open();
        auto session = [&scope](auto&& use) {
            use(*scope);
            return true;
        };
        // The session's entry of the statement, closed with the session at ClosePreparedStatement:
        // every execution runs it.
        auto [id, payload] = prepare_fresh(session, query);

        core::Prepared prepared;
        prepared.query = query;
        prepared.statement = id;
        prepared.parameter_count = payload.parameter_count;
        if (payload.tag == NodeTag::T_SelectStmt) {
            // The schema handed out at CreatePreparedStatement is the
            // statement's contract; a row-producing statement whose prepare
            // left it unresolved cannot be served — its DoGet would stream rows
            // of no columns.
            if (payload.schema.type() != logical_type::STRUCT) {
                throw core::EngineError("the result schema of the statement could not be resolved");
            }
            auto converted = to_arrow_schema(resource_, payload.schema);
            if (converted.has_error()) {
                throw core::EngineError(converted.error().what.c_str());
            }
            check_stream_encodable(*converted.value());
            prepared.dataset_schema = std::move(converted.value());
        }
        // int64 "$N" parameters: the model the reference drivers bind against
        // (arrow-go refuses to coerce a typed value into a utf8 field).
        if (payload.parameter_count > 0) {
            std::vector<std::shared_ptr<arrow::Field>> fields;
            fields.reserve(payload.parameter_count);
            for (std::size_t i = 0; i < payload.parameter_count; ++i) {
                fields.push_back(arrow::field("$" + std::to_string(i + 1), arrow::int64(), true));
            }
            prepared.parameter_schema = arrow::schema(std::move(fields));
        }
        // The statement keeps this session until ClosePreparedStatement.
        return {std::move(prepared), std::move(scope)};
    }

    core::QueryResult SessionEngine::execute_prepared(core::FlightSqlCore& statements,
                                                      const std::string& handle,
                                                      const core::Prepared& prepared,
                                                      const core::BoundParams& params) {
        OTX_ZONE_N("flight::SessionEngine::execute_prepared");
        auto session = [&](auto&& use) { return statements.use_prepared(handle, use); };
        // The first bound row answers; an executemany of a SELECT has no single
        // result to stream.
        auto payload = run_prepared(session, prepared, params);
        refuse_open_transaction(session);
        if (payload.column_count() == 0) {
            // DDL/DML over the prepared path — an empty dataset
            return core::QueryResult{arrow::schema({}), {}};
        }
        return to_query_result(resource_, payload);
    }

    std::int64_t SessionEngine::execute_update_prepared(core::FlightSqlCore& statements,
                                                        const std::string& handle,
                                                        const core::Prepared& prepared,
                                                        const core::BoundParams& params) {
        OTX_ZONE_N("flight::SessionEngine::execute_update_prepared");
        auto session = [&](auto&& use) { return statements.use_prepared(handle, use); };
        std::int64_t affected = 0;
        if (params.empty()) {
            affected = static_cast<std::int64_t>(run_prepared(session, prepared, {}).reported_rows());
            refuse_open_transaction(session);
        } else {
            // executemany: one execution per bound row, the affected counts sum
            // (the ADBC executemany contract for updates).
            for (const auto& batch : params) {
                affected += static_cast<std::int64_t>(run_prepared(session, prepared, {batch}).reported_rows());
                refuse_open_transaction(session);
            }
        }
        return affected;
    }

    std::vector<std::string> SessionEngine::catalogs() {
        OTX_ZONE_N("flight::SessionEngine::catalogs");
        auto scope = sessions_.open();
        auto session = [&scope](auto&& use) {
            use(*scope);
            return true;
        };
        // Every database of a listed schema (a local database has at least
        // public; a connection's, the schemas registered of it).
        auto listed = run_query(session,
                                "SELECT DISTINCT d.datconnection, d.datname" + std::string{schemas_from} +
                                    listed_schemas_where() + ";");
        std::vector<std::string> out;
        for (const auto& row : text_rows(listed)) {
            out.push_back(core::catalog_of(row.at(0), row.at(1)));
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    std::vector<core::DbSchema> SessionEngine::db_schemas(const std::optional<std::string>& catalog,
                                                          const std::string& schema_pattern) {
        OTX_ZONE_N("flight::SessionEngine::db_schemas");
        auto scope = sessions_.open();
        auto session = [&scope](auto&& use) {
            use(*scope);
            return true;
        };
        auto listed = run_query(session,
                                "SELECT d.datconnection, d.datname, n.nspname" + std::string{schemas_from} +
                                    listed_schemas_where() + catalog_filter(catalog) +
                                    like_filter("n.nspname", schema_pattern) + ";");
        std::vector<core::DbSchema> out;
        for (const auto& row : text_rows(listed)) {
            out.push_back(core::DbSchema{core::catalog_of(row.at(0), row.at(1)), row.at(2)});
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    std::pmr::vector<table_info> SessionEngine::tables(const std::optional<std::string>& catalog,
                                                       const std::string& schema_pattern,
                                                       const std::string& table_pattern,
                                                       bool include_schema) {
        OTX_ZONE_N("flight::SessionEngine::tables");
        auto scope = sessions_.open();
        auto session = [&scope](auto&& use) {
            use(*scope);
            return true;
        };
        // Tables and views (r / g / v / m); no index, sequence or type.
        auto listed =
            run_query(session,
                      "SELECT d.datconnection, d.datname, n.nspname, c.relname FROM pg_catalog.pg_class c "
                      "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid JOIN pg_catalog.pg_database "
                      "d ON n.nspdatabase = d.oid" +
                          listed_schemas_where() + " AND c.relkind IN ('r', 'g', 'v', 'm')" + catalog_filter(catalog) +
                          like_filter("n.nspname", schema_pattern) + like_filter("c.relname", table_pattern) + ";");
        std::pmr::vector<table_info> out(resource_);
        for (auto& row : text_rows(listed)) {
            out.push_back(table_info{qualified_name_t{::core::connection_name_t{row.at(0)},
                                                      ::core::dbname_t{row.at(1)},
                                                      ::core::schema_t{row.at(2)},
                                                      ::core::relname_t{row.at(3)}},
                                     {}});
        }
        std::sort(out.begin(), out.end(), [](const table_info& a, const table_info& b) {
            return std::tuple{core::catalog_of(a.name.connection.t, a.name.database.t),
                              a.name.schema.t,
                              a.name.collection.t} <
                   std::tuple{core::catalog_of(b.name.connection.t, b.name.database.t),
                              b.name.schema.t,
                              b.name.collection.t};
        });
        if (!include_schema) {
            return out;
        }
        // The columns: the engine's describe of each table's `SELECT *` (no
        // backend is asked). A table dropped since the listing keeps no schema.
        for (auto& table : out) {
            // As a client names it: connection.namespace.table, or a local database's.
            const auto& first = table.name.connection.t.empty() ? table.name.database.t : table.name.connection.t;
            std::string sql = "SELECT * FROM " + enquote(first, '"') + "." + enquote(table.name.schema.t, '"') + "." +
                              enquote(table.name.collection.t, '"') + ";";
            try {
                auto [id, described] = prepare_fresh(session, sql);
                close_quietly(session, id);
                table.schema = std::move(described.schema);
            } catch (const core::EngineError& e) {
                get_logger(logger_tag::FLIGHTSQL_SERVER)->warn("GetTables: describing {} failed: {}", sql, e.what());
            }
        }
        return out;
    }

    std::string SessionEngine::dialect_name() const { return "otterstax"; }
} // namespace flight::engine
