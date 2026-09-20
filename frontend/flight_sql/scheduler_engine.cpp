// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "scheduler_engine.hpp"

#include "chunk_to_ipc.hpp"

#include "catalog/catalog_manager.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "scheduler/scheduler.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <charconv>
#include <memory_resource>
#include <string>
#include <vector>

using namespace components::types;

namespace flight::engine {

    namespace {

        // Every error leaving the adapter is an EngineError: the Flight SQL
        // core maps it to INVALID_ARGUMENT with the text attached.
        // `::core` — the global namespace; a bare `core` here would resolve
        // to our own flight::core.
        [[noreturn]] void engine_error(const ::core::error_t& error, const char* stage) {
            if (error.type == otterstax::AWAIT_TIMEOUT_CODE) {
                throw core::EngineError(std::string{"Timeout while "} + stage);
            }
            throw core::EngineError(std::string{"Error while "} + stage + ": " + error.what.c_str());
        }

        // One bound parameter row -> engine values. The parameter schema the
        // server hands out is utf8 (parameter types are not resolved at
        // prepare time), so a string value is re-typed the way a text protocol
        // frontend types its literals: an integer it parses as one, then a
        // double, then a bool, else it stays text.
        std::pmr::vector<logical_value_t> to_engine_params(std::pmr::memory_resource* resource,
                                                           const core::BoundParams& rows) {
            std::pmr::vector<logical_value_t> params(resource);
            if (rows.empty()) {
                return params;
            }
            const auto& row = rows.front();
            params.reserve(row.size());
            for (const auto& value : row) {
                switch (value.index()) {
                    case 0: // monostate = NULL
                        params.emplace_back(resource, nullptr);
                        break;
                    case 1:
                        params.emplace_back(resource, std::get<bool>(value));
                        break;
                    case 2:
                        params.emplace_back(resource, std::get<std::int64_t>(value));
                        break;
                    case 3:
                        params.emplace_back(resource, std::get<std::uint64_t>(value));
                        break;
                    case 4:
                        params.emplace_back(resource, std::get<double>(value));
                        break;
                    default: {
                        const std::string& text = std::get<std::string>(value);
                        std::int64_t i = 0;
                        auto [int_end, int_ec] = std::from_chars(text.data(), text.data() + text.size(), i);
                        if (int_ec == std::errc{} && int_end == text.data() + text.size()) {
                            params.emplace_back(resource, i);
                            break;
                        }
                        double d = 0;
                        auto [dbl_end, dbl_ec] = std::from_chars(text.data(), text.data() + text.size(), d);
                        if (dbl_ec == std::errc{} && dbl_end == text.data() + text.size()) {
                            params.emplace_back(resource, d);
                            break;
                        }
                        if (text == "true" || text == "t") {
                            params.emplace_back(resource, true);
                            break;
                        }
                        if (text == "false" || text == "f") {
                            params.emplace_back(resource, false);
                            break;
                        }
                        params.emplace_back(resource, std::string{text});
                        break;
                    }
                }
            }
            return params;
        }

        // The prepared-statement session the CreatePreparedStatement prepare
        // left on a Worker, closed when the statement is executed (the
        // execution re-prepares under its own fresh session) or closed
        // outright; 0 when the engine holds nothing.
        session_hash_t handle_of(const core::Prepared& prepared) {
            if (prepared.engine_handle.empty()) {
                return 0;
            }
            try {
                return static_cast<session_hash_t>(std::stoull(prepared.engine_handle));
            } catch (const std::exception&) {
                return 0;
            }
        }

    } // namespace

    SchedulerEngine::SchedulerEngine(actor_zeta::address_t scheduler,
                                     actor_zeta::address_t catalog,
                                     std::pmr::memory_resource* resource)
        : scheduler_(std::move(scheduler))
        , catalog_(std::move(catalog))
        , resource_(resource) {
        assert(resource_ != nullptr && "memory resource must not be null");
    }

    session_payload SchedulerEngine::run_query(const std::string& sql) {
        OTX_ZONE_N("flight::SchedulerEngine::run_query");
        session_id id;
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::execute, id.hash(), sql);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            engine_error(r.error(), "executing the statement");
        }
        return std::move(r.value());
    }

    std::pair<session_hash_t, session_payload> SchedulerEngine::prepare_fresh(const std::string& sql) {
        OTX_ZONE_N("flight::SchedulerEngine::prepare_fresh");
        session_id id;
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::prepare_schema, id.hash(), sql);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            engine_error(r.error(), "preparing the statement");
        }
        return {id.hash(), std::move(r.value())};
    }

    session_payload SchedulerEngine::run_prepared(const std::string& sql, const core::BoundParams& params) {
        OTX_ZONE_N("flight::SchedulerEngine::run_prepared");
        auto [id, prepared_payload] = prepare_fresh(sql);
        auto parameters = to_engine_params(resource_, params);
        if (prepared_payload.parameter_count > 0 && parameters.size() != prepared_payload.parameter_count) {
            close_quietly(id);
            throw core::EngineError("prepared statement takes " +
                                    std::to_string(prepared_payload.parameter_count) +
                                    " parameter(s), got " + std::to_string(parameters.size()));
        }
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::execute_prepared_statement, id, std::move(parameters));
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            engine_error(r.error(), "executing the prepared statement");
        }
        return std::move(r.value());
    }

    void SchedulerEngine::close_quietly(session_hash_t id) {
        if (id == 0) {
            return;
        }
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::close_statement, id);
        auto closed = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (closed.has_error()) {
            get_logger(logger_tag::FLIGHTSQL_SERVER)
                ->warn("close_statement after a finished prepared statement failed: {}",
                       closed.error().what.c_str());
        }
    }

    core::QueryResult SchedulerEngine::execute(const std::string& query) {
        OTX_ZONE_N("flight::SchedulerEngine::execute");
        // Two-phase: `Scheduler::execute` hands back the rows but an EMPTY
        // payload schema (only the prepare path resolves one), so the
        // statement is prepared first — its payload carries the result schema
        // — and executed under the same session id. A statement that is not a
        // SELECT (DDL/DML) takes the same path and answers an empty dataset:
        // the python flightsql client drives EVERYTHING through the query
        // RPC.
        const auto [id, prepared_payload] = prepare_fresh(query);
        if (prepared_payload.tag == NodeTag::T_SelectStmt) {
            if (prepared_payload.parameter_count > 0) {
                close_quietly(id);
                throw core::EngineError("the statement has " +
                                        std::to_string(prepared_payload.parameter_count) +
                                        " unbound parameter(s); bind them through a prepared statement");
            }
            // The schema handed out is the stream's contract: a row-producing
            // statement whose prepare left it unresolved cannot be served.
            if (prepared_payload.schema.type() != logical_type::STRUCT) {
                close_quietly(id);
                throw core::EngineError("the result schema of the statement could not be resolved");
            }
        }

        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::execute_statement, id);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            engine_error(r.error(), "executing the statement");
        }
        session_payload payload = std::move(r.value());

        core::QueryResult result;
        // Always a VALID (possibly empty) schema: a null SchemaPtr would
        // dereference in the IPC serializer when the ticket is registered.
        result.schema = conv::schema_to_ipc(payload.schema);
        if (payload.column_count() > 0) {
            result.batches = conv::chunks_to_ipc(payload, result.schema);
        }
        // no columns: DDL / DML over the query RPC — no result set, no rows
        return result;
    }

    std::int64_t SchedulerEngine::execute_update(const std::string& query) {
        OTX_ZONE_N("flight::SchedulerEngine::execute_update");
        return static_cast<std::int64_t>(run_query(query).size());
    }

    core::Prepared SchedulerEngine::prepare(const std::string& query) {
        OTX_ZONE_N("flight::SchedulerEngine::prepare");
        auto [id, payload] = prepare_fresh(query);

        core::Prepared prepared;
        prepared.query = query;
        prepared.engine_handle = std::to_string(id);
        if (payload.tag == NodeTag::T_SelectStmt) {
            // The schema handed out at CreatePreparedStatement is the
            // statement's contract; a row-producing statement whose prepare
            // left it unresolved cannot be served — its DoGet would stream
            // rows of no columns.
            if (payload.schema.type() != logical_type::STRUCT) {
                close_quietly(id);
                throw core::EngineError("the result schema of the statement could not be resolved");
            }
            prepared.dataset_schema = conv::schema_to_ipc(payload.schema);
        }
        prepared.parameter_schema = conv::parameter_ipc_schema(payload.parameter_count);
        return prepared;
    }

    core::QueryResult SchedulerEngine::execute_prepared(const core::Prepared& prepared,
                                                        const core::BoundParams& params) {
        OTX_ZONE_N("flight::SchedulerEngine::execute_prepared");
        // The first bound row answers; an executemany of a SELECT has no
        // single result to stream.
        core::QueryResult result;
        try {
            auto payload = run_prepared(prepared.query, params);
            // Always a VALID (possibly empty) schema; see execute().
            result.schema = conv::schema_to_ipc(payload.schema);
            if (payload.column_count() > 0) {
                result.batches = conv::chunks_to_ipc(payload, result.schema);
            }
            // no columns: DDL/DML over the prepared path — an empty dataset
        } catch (...) {
            close_quietly(handle_of(prepared));
            throw;
        }
        close_quietly(handle_of(prepared));
        return result;
    }

    std::int64_t SchedulerEngine::execute_update_prepared(const core::Prepared& prepared,
                                                          const core::BoundParams& params) {
        OTX_ZONE_N("flight::SchedulerEngine::execute_update_prepared");
        std::int64_t affected = 0;
        try {
            if (params.empty()) {
                affected = static_cast<std::int64_t>(run_prepared(prepared.query, {}).size());
            } else {
                // executemany: one execution per bound row, the affected
                // counts sum (the ADBC executemany contract for updates).
                for (const auto& row : params) {
                    affected += static_cast<std::int64_t>(run_prepared(prepared.query, {row}).size());
                }
            }
        } catch (...) {
            close_quietly(handle_of(prepared));
            throw;
        }
        close_quietly(handle_of(prepared));
        return affected;
    }

    void SchedulerEngine::close_prepared(const core::Prepared& prepared) {
        OTX_ZONE_N("flight::SchedulerEngine::close_prepared");
        close_quietly(handle_of(prepared));
    }

    core::EngineMetadata SchedulerEngine::metadata() {
        OTX_ZONE_N("flight::SchedulerEngine::metadata");
        core::EngineMetadata metadata;

        // One discovery pass over the catalog mirror; every metadata command
        // (Catalogs / DbSchemas / Tables / TableTypes) reads this answer.
        catalog_ext::get_tables_command_t command;
        command.include_schema = true;
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(catalog_, &mysql::CatalogManager::get_tables, std::move(command));
        auto r = otterstax::await_future_blocking<std::pmr::vector<table_info>>(std::move(fut), resource_);
        if (r.has_error()) {
            engine_error(r.error(), "reading table metadata");
        }

        // The tables ride as the project's own table_info (qualified name +
        // engine schema); the IPC schema of a table is built where the
        // metadata batches are assembled (commands.cpp).
        for (const auto& table : r.value()) {
            metadata.catalogs.push_back(table.name.database.c_str());
            metadata.db_schemas.push_back(table.name.schema.c_str());
            metadata.tables.emplace_back(table);
        }
        metadata.table_types.emplace_back(catalog_ext::table_type_name);
        return metadata;
    }

    std::string SchedulerEngine::dialect_name() const {
        return "otterstax";
    }

} // namespace flight::engine
