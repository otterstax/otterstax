// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The engine side of the Flight SQL server: Scheduler::execute /
// prepare_schema / execute_prepared_statement wrapped as the one engine
// FlightSqlCore owns.
//
// The engine is called from inside an asio-grpc coroutine, and every
// Scheduler call is awaited with frontend/common/asio_future_bridge.hpp's
// await_future_blocking — the same bridge the wire frontends use. The gRPC
// server runs its GrpcContext on N threads (see server.cpp), so one blocked
// query does not stall the others.

#include "core/core.hpp"

#include "scheduler/session_data.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>

#include <memory_resource>
#include <string>
#include <utility>

class Scheduler;

namespace mysql {
    class CatalogManager;
}

namespace flight::engine {

    class SchedulerEngine final {
    public:
        SchedulerEngine(actor_zeta::address_t scheduler,
                        actor_zeta::address_t catalog,
                        std::pmr::memory_resource* resource);

        // SELECT-like queries and DML by text.
        core::QueryResult execute(const std::string& query);
        std::int64_t execute_update(const std::string& query);

        // Prepared statements. A Worker's stored statement is single-use, so
        // every execution re-prepares under a fresh session id and binds the
        // parameters on it (the pattern the MySQL and PG frontends follow);
        // the session the CreatePreparedStatement prepare left on the Worker
        // is closed right after.
        core::Prepared prepare(const std::string& query);
        core::QueryResult execute_prepared(const core::Prepared& prepared,
                                           const core::BoundParams& params);
        std::int64_t execute_update_prepared(const core::Prepared& prepared,
                                             const core::BoundParams& params);
        void close_prepared(const core::Prepared& prepared);

        // Metadata (Catalogs / DbSchemas / Tables / TableTypes).
        core::EngineMetadata metadata();

        std::string dialect_name() const;

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
