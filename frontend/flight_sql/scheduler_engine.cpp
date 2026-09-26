// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "scheduler_engine.hpp"

#include "catalog/catalog_manager.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "otterbrix/translators/input/arrow_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "otterbrix/translators/output/writable_columns.hpp"
#include "scheduler/scheduler.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <string>
#include <vector>

using namespace components::types;

namespace flight::engine {

    namespace {

        // Every error leaving the adapter is an EngineError: the Flight SQL
        // core maps it to INVALID_ARGUMENT with the text attached.
        // `::core` is the global namespace; a bare `core` here would resolve
        // to our own flight::core.
        [[noreturn]] void engine_error(const ::core::error_t& error, const char* stage) {
            if (error.type == otterstax::AWAIT_TIMEOUT_CODE) {
                throw core::EngineError(std::string{"Timeout while "} + stage);
            }
            throw core::EngineError(std::string{"Error while "} + stage + ": " + error.what.c_str());
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

        // The prepared session the CreatePreparedStatement prepare left on a
        // Worker, closed when the statement is executed (the execution
        // re-prepares under its own fresh session) or closed outright; 0 when
        // the engine holds nothing.
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
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::prepare_schema, id.hash(), sql);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            engine_error(r.error(), "preparing the statement");
        }
        return {id.hash(), std::move(r.value())};
    }

    std::pmr::vector<std::pmr::vector<components::types::logical_value_t>>
    SchedulerEngine::to_param_rows(const core::BoundParams& params) {
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

    session_payload SchedulerEngine::run_prepared(const std::string& sql, const core::BoundParams& params) {
        OTX_ZONE_N("flight::SchedulerEngine::run_prepared");
        auto [id, prepared_payload] = prepare_fresh(sql);
        auto parameters = to_param_rows(params);
        if (prepared_payload.parameter_count > 0 &&
            parameters.size() != 1 && !parameters.empty()) {
            // executemany goes row-by-row upstream; a single run takes one row
            close_quietly(id);
            throw core::EngineError("prepared statement takes one parameter row per execution, got " +
                                    std::to_string(parameters.size()));
        }
        if (prepared_payload.parameter_count > 0 &&
            (parameters.empty() || parameters.front().size() != prepared_payload.parameter_count)) {
            close_quietly(id);
            throw core::EngineError("prepared statement takes " +
                                    std::to_string(prepared_payload.parameter_count) +
                                    " parameter(s), got " +
                                    std::to_string(parameters.empty() ? 0 : parameters.front().size()));
        }
        auto bound = parameters.empty()
                         ? std::pmr::vector<components::types::logical_value_t>{resource_}
                         : std::move(parameters.front());
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::execute_prepared_statement, id, std::move(bound));
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
        // sending to the Scheduler event-loop always returns needs_sched=false
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

        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(scheduler_, &Scheduler::execute_statement, id);
        auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);
        if (r.has_error()) {
            engine_error(r.error(), "executing the statement");
        }
        session_payload payload = std::move(r.value());

        if (payload.column_count() == 0) {
            // DDL / DML over the query RPC: no result set, no rows. Always a
            // VALID (empty) schema: a null one would fail downstream.
            return core::QueryResult{arrow::schema({}), {}};
        }
        return to_query_result(resource_, payload);
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
            auto converted = to_arrow_schema(resource_, payload.schema);
            if (converted.has_error()) {
                close_quietly(id);
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
        return prepared;
    }

    core::QueryResult SchedulerEngine::execute_prepared(const core::Prepared& prepared,
                                                        const core::BoundParams& params) {
        OTX_ZONE_N("flight::SchedulerEngine::execute_prepared");
        // The first bound row answers; an executemany of a SELECT has no
        // single result to stream.
        try {
            auto payload = run_prepared(prepared.query, params);
            if (payload.column_count() == 0) {
                // DDL/DML over the prepared path — an empty dataset
                return core::QueryResult{arrow::schema({}), {}};
            }
            return to_query_result(resource_, payload);
        } catch (...) {
            close_quietly(handle_of(prepared));
            throw;
        }
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
                for (const auto& batch : params) {
                    affected += static_cast<std::int64_t>(
                        run_prepared(prepared.query, {batch}).size());
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
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [needs_sched, fut] =
            actor_zeta::send(catalog_, &mysql::CatalogManager::get_tables, std::move(command));
        auto r = otterstax::await_future_blocking<std::pmr::vector<table_info>>(std::move(fut), resource_);
        if (r.has_error()) {
            engine_error(r.error(), "reading table metadata");
        }

        // The tables ride as the project's own table_info (qualified name +
        // engine schema); the arrow schema of a table is built where the
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
