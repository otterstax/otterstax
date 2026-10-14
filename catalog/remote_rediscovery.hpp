// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/discovery.hpp"
#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/federation/provider.hpp"

#include <components/logical_plan/execution_plan.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <span>
#include <vector>

// The session's recovery when a statement names a remote table the metadata does not hold (yet), or
// holds with a schema the backend no longer has. need / decide never reach a backend, so a table
// created on the backend after startup, or changed since, is unknown to the engine until it is
// rediscovered. The flow, run by the query session between engine round trips:
//
//   1. run the statement;
//   2. it failed: `rediscovery_targets(error, names, registry())` answers what to rediscover — empty
//      means "report the error as it is" (not a missing / stale remote table);
//   3. per target: read the table's backend spelling out of federation.remote_tables
//      (make_remote_table_read / with_remote_spelling) — when the metadata holds the table, ask
//      the backend for that spelling, else for the statement's; then
//      call `discover(target.discovery_scope)` on the backend the federation registry holds for
//      the target's uid (federation::backend_t: db::MySQLManager / PostgressManager /
//      ClickhouseManager);
//   4. `make_rediscovery_plans(resource, target, reply)` → run the plans on one engine session inside
//      BEGIN ... COMMIT; an error here means the table cannot be rediscovered: report the
//      statement's ORIGINAL error. Two sessions rediscovering one table at once may both write its
//      rows; decide reads a column repeated that way once (federation/name_resolution.hpp), and
//      the next rediscovery of the table replaces them all;
//   5. parse the statement again (a plan is single-use) and run it once more. Whatever this
//      second run answers is the statement's answer: the retry is never rediscovered again.
namespace otterstax::catalog {

    // A remote table to rediscover.
    struct rediscovery_target_t {
        backend_type_t type;
        federation::table_key_t key;
        // The name the backend's discover takes: uid (config.yaml's spelling), database
        // (MySQL / ClickHouse) or schema (PostgreSQL), table — as the statement wrote them, or
        // as the backend spells them once with_remote_spelling was applied. Never folded.
        qualified_name_t discovery_scope;
    };

    // The statement's remote names worth a rediscovery: every name in `statement_names` the
    // federation answers for (federation::resolve_written_name), deduplicated — when the error is
    // a missing relation (the engine's answer for a name nobody resolved: "relation ... does not
    // exist" table_not_exists, "database ... does not exist" database_not_exists, or the refusal of
    // a schema slot under an existing database: invalid_parameter "... does not exist ...") or a
    // stale remote schema (the federation's answer for rows that no longer match the declared
    // columns, federation::conform_to_result: schema_error "... changed since it was discovered").
    // Nothing otherwise.
    std::vector<rediscovery_target_t> rediscovery_targets(const core::error_t& error,
                                                          std::span<const qualified_name_t> statement_names,
                                                          const federation::registry_t& registry);

    // Ask the backend for the table by its own spelling rather than the statement's: the remote_*
    // columns of the federation.remote_tables rows make_remote_table_read read (`rows`). The target
    // is left as it is when the metadata does not hold the table (or the rows are not that table's
    // shape).
    void with_remote_spelling(rediscovery_target_t& target,
                              const std::pmr::vector<components::vector::data_chunk_t>& rows);

    // The plans that apply a discover reply for `target`: its rows replaced by the reply's
    // (make_discovery_upsert under the target's scope). A reply that is an error, or holds no table,
    // is answered with that error / table_not_exists: there is nothing to retry with.
    core::result_wrapper_t<std::pmr::vector<components::logical_plan::execution_plan_t>>
    make_rediscovery_plans(std::pmr::memory_resource* resource,
                           const rediscovery_target_t& target,
                           const core::result_wrapper_t<federation::discovered_tables_t>& reply);

    // At most one rediscovery of a table writes its rows at a time in this process: the session that
    // claims the key rediscovers it, a session that cannot claim it (another one is at it) skips the
    // write and retries its statement on whatever the other leaves. The claim is released when the
    // object goes.
    class rediscovery_claim_t {
    public:
        static std::optional<rediscovery_claim_t> try_claim(const federation::table_key_t& key);
        rediscovery_claim_t(rediscovery_claim_t&& other) noexcept;
        rediscovery_claim_t& operator=(rediscovery_claim_t&& other) noexcept;
        rediscovery_claim_t(const rediscovery_claim_t&) = delete;
        rediscovery_claim_t& operator=(const rediscovery_claim_t&) = delete;
        ~rediscovery_claim_t();

    private:
        explicit rediscovery_claim_t(std::string key);
        void release() noexcept;
        std::string key_;
    };

} // namespace otterstax::catalog
