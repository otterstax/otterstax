// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The IEngine adapter over the Scheduler/Worker pool: what the old
// Arrow-based FlightSQL frontend did with Scheduler::execute /
// prepare_schema / execute_prepared_statement, expressed against the
// synchronous IEngine contract of the custom Flight SQL core.
//
// IEngine is called from inside an asio-grpc coroutine, and every Scheduler
// call is awaited with frontend/common/asio_future_bridge.hpp's
// await_future_blocking — the same bridge the wire frontends use. The gRPC
// server runs its GrpcContext on N threads (see server.cpp), so one blocked
// query does not stall the others.

#include "core/engine.hpp"

#include "scheduler/session_data.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>

#include <cstddef>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <utility>

class Scheduler;

namespace mysql {
    class CatalogManager;
}

namespace flight::engine {

    class SchedulerEngine final : public core::IEngine {
    public:
        SchedulerEngine(actor_zeta::address_t scheduler,
                        actor_zeta::address_t catalog,
                        std::pmr::memory_resource* resource);

        // IEngine — SELECT-like queries and DML by text.
        core::QueryResult execute(const std::string& query) override;
        std::int64_t execute_update(const std::string& query) override;

        // IEngine — prepared statements. A Worker's stored statement is
        // single-use, so every execution re-prepares under a fresh session
        // id and binds the parameters on it (the pattern the MySQL and PG
        // frontends follow); the session the CreatePreparedStatement prepare
        // left on the Worker is closed right after.
        core::Prepared prepare(const std::string& query) override;
        core::QueryResult execute_prepared(const core::Prepared& prepared,
                                           const core::BoundParams& params) override;
        std::int64_t execute_update_prepared(const core::Prepared& prepared,
                                             const core::BoundParams& params) override;
        void close_prepared(const core::Prepared& prepared) override;

        // IEngine — metadata (Catalogs / DbSchemas / Tables / TableTypes).
        core::EngineMetadata metadata() override;

        std::string dialect_name() const override;

    private:
        // Scheduler::execute + await; throws core::EngineError.
        session_payload run_query(const std::string& sql);
        // Scheduler::prepare_schema under a fresh session id + await; throws.
        std::pair<session_hash_t, session_payload> prepare_fresh(const std::string& sql);
        // Re-prepare + execute_prepared_statement with one parameter row.
        session_payload run_prepared(const std::string& sql, const core::BoundParams& params);
        // Best-effort Scheduler::close_statement (idempotent on the Worker).
        void close_quietly(session_hash_t id);

        actor_zeta::address_t scheduler_;
        actor_zeta::address_t catalog_;
        std::pmr::memory_resource* resource_;
    };

} // namespace flight::engine
