// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "runtime.hpp"

#include "config/config.hpp"

#include <actor-zeta.hpp>
#include <components/configuration/configuration.hpp>
#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/planner/host_hooks.hpp>
#include <components/session/session.hpp>
#include <core/result_wrapper.hpp>
#include <services/engine/engine.hpp>

#include <memory>
#include <memory_resource>
#include <optional>

namespace otterstax::runtime {

    // `service.engine` → the engine's configuration::config: wal and disk
    // straight into data_dir (the single-directory layout), the log into
    // config::log_directory, and main_path — the directory the engine locks —
    // data_dir too; the log level is SPDLOG_ACTIVE_LEVEL, otterstax's own;
    // everything else is otterbrix's default.
    [[nodiscard]] configuration::config make_engine_configuration(const config::EngineConfig& engine);

    // The second module: the embedded otterbrix engine, services::engine::open_engine
    // over the runtime's resource and its general / exec / disk pools.
    //
    // Opening validates the configuration, takes `<main_path>/.lock`, replays the
    // WAL, bootstraps the catalog and starts the three pools; destroying it is the
    // shutdown — the dispatcher refuses new work, a final CHECKPOINT folds the
    // journal into storage, the loops and the three pools stop. The runtime_t must
    // outlive it (its destructor aborts otherwise).
    //
    // Requests go straight to dispatcher_address() as actor messages
    // (manager_dispatcher_t::execute_plan, ...), awaited by actor coroutines.
    class engine_t final {
    public:
        using engine_ptr = std::unique_ptr<engine_t>;

        // The configuration from make_engine_configuration and the log from
        // make_engine_log (engine.cpp) at its level, written through the
        // sinks of otterstax's Main logger. main.cpp creates every logger under
        // config::log_directory (log_dir, else data_dir) before it opens
        // anything, so the engine's lines land in Main's file there; a caller
        // that created no Main (a test) gets one created under that directory.
        //
        // A refused start — a log that cannot be made (make_engine_log's
        // error), a held `.lock` (already_exists), an unreadable WAL (io_error),
        // a bad config.execution (invalid_parameter), the runtime's engine pools
        // already used (invalid_parameter) — is the error; its message lives on
        // new_delete_resource, so it outlives the runtime. A refusal once the
        // pools were lent spends them: the next engine needs a new runtime_t.
        // `primitives` are the host's optimizer rules and name resolution
        // need/decide; empty is otterbrix's "resolve nothing".
        [[nodiscard]] static core::result_wrapper_t<engine_ptr> open(runtime_t& runtime,
                                                                      const config::EngineConfig& engine,
                                                                      components::planner::primitives_t primitives = {});

        engine_t(const engine_t&) = delete;
        engine_t& operator=(const engine_t&) = delete;
        engine_t(engine_t&&) = delete;
        engine_t& operator=(engine_t&&) = delete;
        ~engine_t();

        // services::dispatcher::manager_dispatcher_t: execute_plan, refuse_statement, txn_*.
        [[nodiscard]] actor_zeta::address_t dispatcher_address() const noexcept;
        // The runtime's resource, the one every engine structure lives on.
        [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return runtime_->resource(); }

        // Sends `plan` to the dispatcher and blocks the calling thread until the
        // cursor lands (actor_zeta::otterbrix::wait_ready). For startup work that
        // runs before any frontend accepts a client, and for tests. NEVER call it
        // from an actor or a pool thread: it parks that thread on a sleep loop.
        [[nodiscard]] components::cursor::cursor_t_ptr
        execute_plan_blocking(components::session::session_id_t session,
                              components::logical_plan::execution_plan_t plan);

    private:
        engine_t(runtime_t& runtime, configuration::config configuration, log_t log);

        runtime_t* runtime_;
        configuration::config configuration_;
        // Owned here: open_engine takes the log by reference (the engine keeps a clone).
        log_t log_;
        std::optional<services::engine::engine_t> engine_;
    };

} // namespace otterstax::runtime
