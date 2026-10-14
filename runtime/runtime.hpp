// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "config/config.hpp"

#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>
#include <services/engine/engine.hpp>

#include <cstddef>
#include <memory>
#include <memory_resource>

namespace otterstax::runtime {

    // Every pool is an actor-zeta work-sharing scheduler (otterbrix calls the
    // same type actor_zeta::shared_work).
    using pool_t = actor_zeta::scheduler::sharing_scheduler;

    // A configured thread count: 0 is "size to the machine", max(2, hardware threads).
    [[nodiscard]] std::size_t resolve_threads(std::size_t configured) noexcept;

    // The first module of the process (runtime_t → remote_backends_t → engine_t,
    // then the actors, the session factory and the servers — all main.cpp's
    // locals): the memory resource and every thread pool, by
    // role. It is built first and destroyed last, so every pool outlives the
    // actors and the jobs on it.
    //
    //  - general / exec / disk — the engine's pools (services::engine::schedulers_t).
    //    runtime_t builds them and never runs them: open_engine starts them and the
    //    engine's shutdown stops them. An actor-zeta pool cannot be restarted
    //    (scheduler_t::start appends a second set of workers, stop() of a pool
    //    never started indexes an empty vector), so they serve ONE engine: lent by
    //    engine_t::open, spent once that engine is destroyed or its open was
    //    refused. A second engine needs a second runtime_t.
    //  - sessions — one query session actor per client connection.
    //  - backends — the federation's statements of the three backends
    //    (remote_backends_t: the connection checkout and the wait for a
    //    backend's answer, off the engine's executors; integration/
    //    statement_executor.hpp). Their discover / execute_ddl run on the
    //    calling thread, and the connectors do their I/O on the
    //    asio io pools their connector managers own (sized by
    //    service.runtime.connector_io; not a runtime_t pool). A statement
    //    holds one of its threads for the whole remote call, so the server
    //    sizes it against the remote connections before it builds the runtime
    //    (backends::backends_threads); a 0 that reaches here is resolve_threads.
    //    Both started here, at construction; sessions is stopped by
    //    stop_sessions or the destructor, backends by the destructor only,
    //    sessions first. The server stops sessions before its sessions are
    //    freed and backends only after the backends are gone (~runtime_t): they
    //    wait for the jobs they posted, which a stopped scheduler would drop.
    //
    // Not thread-safe: built, lent and stopped by the thread that assembles the
    // process (main, a test).
    class runtime_t final {
    public:
        explicit runtime_t(const config::RuntimeConfig& config = {});
        runtime_t(const runtime_t&) = delete;
        runtime_t& operator=(const runtime_t&) = delete;
        runtime_t(runtime_t&&) = delete;
        runtime_t& operator=(runtime_t&&) = delete;
        // Stops the sessions pool, then the backends pool; an engine still
        // running on the engine pools here is a programming error and aborts
        // (its threads would outlive their pools).
        ~runtime_t();

        // The process-wide arena (core::pmr::otterbrix_resource, as the engine used
        // before): the engine and every actor allocate from it.
        [[nodiscard]] std::pmr::memory_resource* resource() noexcept { return &resource_; }

        [[nodiscard]] pool_t* sessions_scheduler() noexcept { return sessions_.get(); }
        [[nodiscard]] pool_t* backends_scheduler() noexcept { return backends_.get(); }

        // Joins the sessions pool's threads; idempotent. Call before the
        // sessions are destroyed: once it returns, no pool thread resumes one
        // of them (~session_factory_t calls it in its shutdown).
        void stop_sessions() noexcept;

    private:
        friend class engine_t;

        // Three states, not a flag: a second lend is refused once the pools were
        // lent (lent or spent), and ~runtime_t aborts only while an engine still
        // runs on them (lent, not spent).
        enum class engine_pools_state_t
        {
            fresh, // never started; an engine may take them
            lent,  // an engine runs on them
            spent, // that engine stopped them, or its open was refused; they cannot run another
        };

        // The three engine pools for open_engine, or invalid_parameter when an
        // engine has them or already spent them; the message lives on new_delete_resource.
        [[nodiscard]] core::result_wrapper_t<services::engine::schedulers_t> lend_engine_pools();
        // The engine's shutdown stopped them, or open_engine refused before
        // starting them: no engine runs on them and none may again.
        void engine_pools_spent() noexcept;

        // Declared first, so destroyed last: every actor, engine structure and
        // queued message the pools may still reference was allocated from it.
        core::pmr::otterbrix_resource resource_;
        std::unique_ptr<pool_t> general_;
        std::unique_ptr<pool_t> exec_;
        std::unique_ptr<pool_t> disk_;
        std::unique_ptr<pool_t> sessions_;
        std::unique_ptr<pool_t> backends_;
        bool sessions_running_{false};
        engine_pools_state_t engine_pools_state_{engine_pools_state_t::fresh};
    };

} // namespace otterstax::runtime
