// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "catalog/discovery.hpp"
#include "connectors/clickhouse/manager.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <mutex>
#include <string>
#include <unordered_map>

namespace db {
    // ClickhouseManager ONLY fetches data from ClickHouse, NO JOIN operations
    class ClickhouseManager final : public actor_zeta::actor::actor_mixin<ClickhouseManager> {
    public:
        using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        ClickhouseManager(std::pmr::memory_resource* res, ch::ConnectorManager* connector_manager);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        /// handler coroutine — ONLY fetches data, does NOT perform JOIN
        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>> execute(session_hash_t id,
                                                                                      ParsedQueryDataPtr data);

        // Schema discovery for the catalog: `scope.database` is the ClickHouse
        // database the probes run against (empty is missing_field); empty
        // `scope.collection` → every table of it (system.tables listing, then
        // one probe per table), non-empty → that single table. Each table's
        // named-type overrides (system.columns) are fetched first — a failed
        // metadata query fails the discovery, so a table is never registered
        // with degraded column types — and the probe's STRUCT is built with
        // them, so the registered schema matches what a query later decodes.
        // Every connector future is consumed here, on this actor, so the
        // connector manager has one driver.
        actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
        discover(qualified_name_t scope);

        // The prepared schema of the statement's ClickHouse slots, answered by
        // the backend. For every schema_node_t stub the catalog made of an
        // aggregate slot, the statement execute would run for it — the same
        // generator call — is wrapped as `SELECT * FROM (<statement>) LIMIT 0`
        // and run: ClickHouse answers the header block (the result's columns
        // and wire types) and the LIMIT closes the pipeline before a row is
        // read. The header goes through ch_to_struct under the slot's table's
        // named types, exactly as execute's blocks go through ch_to_chunk, so
        // the prepared column types are the executed chunk's by construction
        // (count() UInt64, `x + 1` Int64, a function aliased as a column its
        // own type). The stub is filled with that STRUCT, which
        // Worker::prepare_schema reads. Sent by the Worker after
        // CatalogManager::get_catalog_schema classified the statement.
        // A raw-SQL subquery stub — the text the parser lifted out of the
        // statement, which no plan describes — is probed the same way, from the
        // statement execute generates for it (its qualifiers rewritten for
        // ClickHouse); its schema is filled in place, so the text and qualifiers
        // survive for execute, and schema_utils reads it as the input relation
        // of the aggregate above it. A
        // parameterized statement cannot be described before Bind — the binder
        // holds every parameter, literals included, until finalize, so no SQL
        // exists to probe with — and is unimplemented_yet, never a plan-derived
        // guess. A probe the backend refuses or does not answer is the
        // connector's error, code kept; an answer without a header block is
        // schema_error. Nothing is cached: every prepare asks the backend.
        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>> describe(session_hash_t id,
                                                                                       ParsedQueryDataPtr data);

        using dispatch_traits = actor_zeta::
            dispatch_traits<&ClickhouseManager::execute, &ClickhouseManager::discover, &ClickhouseManager::describe>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        // column → ClickHouse type string (e.g. "Tuple(channel String, ...)"),
        // the shape tsl::ch_to_chunk / ch_to_struct take as overrides.
        using named_types_t = std::unordered_map<std::string, std::string>;

        std::pmr::memory_resource* resource_;
        // Non-owning: ComponentManager owns it and outlives this actor. This
        // actor is its only driver after startup (execute and discover both run
        // here, serialised by mutex_).
        ch::ConnectorManager* connector_manager_;
        log_t log_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "ClickhouseManager::mutex");
        actor_zeta::behavior_t current_behavior_;
        // uid → table → named-type overrides. Actor-confined: assigned by
        // discover from the value the io-thread query handed back (a
        // rediscovered table replaces its entry), read by execute — both under
        // mutex_. A table without an entry (a DDL target, never probed) has no
        // overrides.
        std::pmr::unordered_map<std::pmr::string, std::pmr::unordered_map<std::pmr::string, named_types_t>>
            named_types_;

        // Runs the system.columns query for one table and returns the decoded
        // overrides as a value; nothing is cached before the outcome is known.
        core::result_wrapper_t<named_types_t>
        fetch_named_types(const std::string& uuid, const std::string& database, const std::string& table);
        const named_types_t* named_types_for(const std::string& uuid, const std::string& table) const;

        // The statement one slot sends to ClickHouse and the base table whose
        // named types shape its result columns — shared by execute (which runs
        // it) and describe (which probes its header), so both see one text.
        struct slot_statement_t {
            std::string sql;
            std::string table;
        };
        core::result_wrapper_t<slot_statement_t>
        generate_slot_statement(const components::logical_plan::node_ptr& node,
                                const otterstax::names::resolved_target_t& target,
                                const std::pmr::vector<external_entry_t>& batch,
                                const components::logical_plan::storage_parameters* parameters);
    };
} // namespace db
