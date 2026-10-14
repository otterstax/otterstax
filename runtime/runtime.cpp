// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "runtime.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

namespace otterstax::runtime {

    namespace {
        // Messages an actor handles before it yields its thread (actor-zeta has no default).
        constexpr std::size_t max_throughput = 1000;

        std::unique_ptr<pool_t> make_pool(std::size_t threads) { return std::make_unique<pool_t>(threads, max_throughput); }
    } // namespace

    std::size_t resolve_threads(std::size_t configured) noexcept {
        if (configured != 0) {
            return configured;
        }
        return std::max<std::size_t>(2, std::thread::hardware_concurrency());
    }

    runtime_t::runtime_t(const config::RuntimeConfig& config)
        // The engine pools have no auto size (the config layer refuses 0); a 0
        // reaching here from code is read the same way rather than as no threads.
        : general_(make_pool(resolve_threads(config.general.threads)))
        , exec_(make_pool(resolve_threads(config.exec.threads)))
        , disk_(make_pool(resolve_threads(config.disk.threads)))
        , sessions_(make_pool(resolve_threads(config.sessions.threads)))
        , backends_(make_pool(resolve_threads(config.backends.threads))) {
        OTX_ZONE_N("runtime::runtime_t");
        // Backends first: the sessions an accepted connection spawns talk to
        // them, never the other way round. Stopped in the reverse order.
        backends_->start();
        sessions_->start();
        sessions_running_ = true;
    }

    runtime_t::~runtime_t() {
        OTX_ZONE_N("runtime::~runtime_t");
        stop_sessions();
        // Started in the constructor, stopped once, here.
        backends_->stop();
        if (engine_pools_state_ == engine_pools_state_t::lent) {
            // The engine's threads would outlive the pools they run on.
            std::fputs("otterstax::runtime::runtime_t destroyed while an engine_t still runs on its engine pools: "
                       "destroy the engine_t first\n",
                       stderr);
            std::abort();
        }
        // fresh: never started, nothing to join (stop() of an unstarted pool is
        // undefined). spent: the engine joined them while shutting down, or a
        // refused open never started them.
    }

    void runtime_t::stop_sessions() noexcept {
        OTX_ZONE_N("runtime::stop_sessions");
        if (sessions_running_) {
            sessions_->stop();
            sessions_running_ = false;
        }
    }

    core::result_wrapper_t<services::engine::schedulers_t> runtime_t::lend_engine_pools() {
        switch (engine_pools_state_) {
            case engine_pools_state_t::fresh:
                engine_pools_state_ = engine_pools_state_t::lent;
                return services::engine::schedulers_t{general_.get(), exec_.get(), disk_.get()};
            case engine_pools_state_t::lent:
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"engine startup REFUSED, this runtime's engine pools already "
                                                      "run an engine: one runtime_t hosts one engine_t",
                                                      std::pmr::new_delete_resource()});
            case engine_pools_state_t::spent:
                break;
        }
        return core::error_t(core::error_code_t::invalid_parameter,
                             std::pmr::string{"engine startup REFUSED, this runtime's engine pools were spent by "
                                              "the engine that ran on them (or by a refused open) and an actor-zeta "
                                              "pool cannot be restarted: open the next engine on a new runtime_t",
                                              std::pmr::new_delete_resource()});
    }

    void runtime_t::engine_pools_spent() noexcept { engine_pools_state_ = engine_pools_state_t::spent; }

} // namespace otterstax::runtime
