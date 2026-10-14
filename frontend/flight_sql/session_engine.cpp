// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "session_engine.hpp"

#include "catalog/introspection.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "otterbrix/translators/input/arrow_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "otterbrix/translators/output/writable_columns.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <string>
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
                                            field->name() + "' has nested type " +
                                            field->type()->ToString());
                }
            }
        }

        core::QueryResult to_query_result(std::pmr::memory_resource* resource,
                                          session_payload& payload) {
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

    std::pmr::vector<std::pmr::vector<components::types::logical_value_t>>
    SessionEngine::to_param_rows(const core::BoundParams& params) {
        std::pmr::vector<std::pmr::vector<components::types::logical_value_t>> rows(resource_);
        for (const auto& batch : params) {
            if (!batch || batch->num_rows() == 0) {
                continue;
            }
            // The project's own reader: an arrow batch -> engine chunk; each
            // chunk cell IS a logical_value_t, which is what the binder takes.
            auto converted = tsl::arrow_to_chunk(resource_, batch);
            if (converted.has_error()) {
                throw core::EngineError(converted.error().what.c_str());
            }
            const auto& chunk = converted.value();
            for (std::size_t r = 0; r < chunk.size(); ++r) {
                std::pmr::vector<components::types::logical_value_t> row(resource_);
                row.reserve(chunk.column_count());
                for (std::size_t c = 0; c < chunk.column_count(); ++c) {
                    row.push_back(chunk.value(c, r));
                }
                rows.push_back(std::move(row));
            }
        }
        return rows;
    }

    template<typename Session>
    session_payload
    SessionEngine::run_prepared(const Session& session, const std::string& sql, const core::BoundParams& params) {
        OTX_ZONE_N("flight::SessionEngine::run_prepared");
        auto [id, prepared_payload] = prepare_fresh(session, sql);
        auto parameters = to_param_rows(params);
        if (prepared_payload.parameter_count > 0 && parameters.size() != 1 && !parameters.empty()) {
            // executemany goes row-by-row upstream; a single run takes one row
            close_quietly(session, id);
            throw core::EngineError("prepared statement takes one parameter row per execution, got " +
                                    std::to_string(parameters.size()));
        }
        if (prepared_payload.parameter_count > 0 &&
            (parameters.empty() || parameters.front().size() != prepared_payload.parameter_count)) {
            close_quietly(session, id);
            throw core::EngineError("prepared statement takes " + std::to_string(prepared_payload.parameter_count) +
                                    " parameter(s), got " +
                                    std::to_string(parameters.empty() ? 0 : parameters.front().size()));
        }
        auto bound = parameters.empty() ? std::pmr::vector<components::types::logical_value_t>{resource_}
                                        : std::move(parameters.front());
        return call(
            session,
            [&, id = id](query_session_t& target) {
                return actor_zeta::send(target.address(),
                                        &query_session_t::execute_prepared_statement,
                                        id,
                                        std::move(bound))
                    .second;
            },
            "executing the prepared statement");
    }

    template<typename Session>
    void SessionEngine::refuse_open_transaction(const Session& session) {
        auto state = otterstax::session::transaction_state_t::idle;
        if (!session([&](query_session_t& target) { state = target.transaction_state(); })) {
            throw core::EngineError("the prepared statement was closed");
        }
        if (state == otterstax::session::transaction_state_t::idle) {
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
            get_logger(logger_tag::FLIGHTSQL_SERVER)->warn("close_statement of a prepared statement failed: {}", e.what());
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
        auto [id, payload] = prepare_fresh(session, query);
        // Every execution re-prepares the text on this session; the entry the
        // prepare left is not needed.
        close_quietly(session, id);

        core::Prepared prepared;
        prepared.query = query;
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
        auto payload = run_prepared(session, prepared.query, params);
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
            affected = static_cast<std::int64_t>(run_prepared(session, prepared.query, {}).reported_rows());
            refuse_open_transaction(session);
        } else {
            // executemany: one execution per bound row, the affected counts sum
            // (the ADBC executemany contract for updates).
            for (const auto& batch : params) {
                affected += static_cast<std::int64_t>(run_prepared(session, prepared.query, {batch}).reported_rows());
                refuse_open_transaction(session);
            }
        }
        return affected;
    }

    core::EngineMetadata SessionEngine::metadata() {
        OTX_ZONE_N("flight::SessionEngine::metadata");
        core::EngineMetadata metadata;

        // One listing pass — the engine's own tables and the federation's remote
        // ones — on a session of its own; every metadata command (Catalogs /
        // DbSchemas / Tables / TableTypes) reads this answer.
        auto scope = sessions_.open();
        auto session = [&scope](auto&& use) {
            use(*scope);
            return true;
        };
        auto classes = run_query(session, std::string{otterstax::catalog::classes_sql});
        auto namespaces = run_query(session, std::string{otterstax::catalog::namespaces_sql});
        auto attributes = run_query(session, std::string{otterstax::catalog::attributes_sql});
        auto remote_tables = run_query(session, std::string{otterstax::catalog::remote_tables_sql});
        auto remote_columns = run_query(session, std::string{otterstax::catalog::remote_columns_sql});
        otterstax::catalog::get_tables_command_t command;
        command.include_schema = true;
        auto listed = otterstax::catalog::list_tables(resource_,
                                                      command,
                                                      namespaces.chunks,
                                                      classes.chunks,
                                                      &attributes.chunks,
                                                      remote_tables.chunks,
                                                      &remote_columns.chunks);
        if (listed.has_error()) {
            throw core::EngineError(std::string{"Error while reading table metadata: "} + listed.error().what.c_str());
        }

        // The tables ride as the project's own table_info (qualified name +
        // engine schema); the arrow schema of a table is built where the
        // metadata batches are assembled (commands.cpp).
        // GetCatalogs / GetDbSchemas answer each catalog and each (catalog,
        // schema) pair once, ordered as the spec asks: catalog_name, then
        // db_schema_name. A schema keeps the catalog that holds it.
        for (const auto& table : listed.value()) {
            metadata.catalogs.emplace_back(table.name.database.t.c_str());
            metadata.db_schemas.push_back(core::DbSchema{table.name.database.t.c_str(), table.name.schema.t.c_str()});
            metadata.tables.emplace_back(table);
        }
        std::sort(metadata.catalogs.begin(), metadata.catalogs.end());
        metadata.catalogs.erase(std::unique(metadata.catalogs.begin(), metadata.catalogs.end()),
                                metadata.catalogs.end());
        std::sort(metadata.db_schemas.begin(), metadata.db_schemas.end());
        metadata.db_schemas.erase(std::unique(metadata.db_schemas.begin(), metadata.db_schemas.end()),
                                  metadata.db_schemas.end());
        metadata.table_types.emplace_back(otterstax::catalog::table_type_name);
        return metadata;
    }

    std::string SessionEngine::dialect_name() const {
        return "otterstax";
    }

} // namespace flight::engine
