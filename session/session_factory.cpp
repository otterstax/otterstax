// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "session/session_factory.hpp"

#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <cassert>
#include <utility>
#include <vector>

namespace otterstax::session {

    namespace {
        // How long the shutdown waits for the sessions of closed connections to
        // finish the statement in flight before it stops the sessions pool.
        constexpr std::chrono::milliseconds kSessionDrainTimeout = std::chrono::seconds(30);
    } // namespace

    session_factory_t::session_factory_t(runtime::runtime_t& runtime,
                                         parser_factory_fn parser_factory,
                                         session_services_t services)
        : runtime_(runtime)
        , resource_(runtime.resource())
        , scheduler_(runtime.sessions_scheduler())
        , parser_factory_(parser_factory)
        , services_(std::move(services)) {
        assert(resource_ != nullptr);
        assert(scheduler_ != nullptr);
        assert(parser_factory_ != nullptr);
    }

    session_factory_t::~session_factory_t() {
        OTX_ZONE_N("session_factory::~session_factory");
        // Sessions: a closed session frees itself once the statement it runs is
        // done; that statement still talks to the backends and the engine, which
        // outlive the factory.
        if (!wait_closed(kSessionDrainTimeout)) {
            // A session dropped by the stopped pool does not run the ROLLBACK
            // its close queued: its open block stays with the engine (which has
            // no close-session call) until the engine itself stops.
            get_logger(logger_tag::Main)
                ->warn("shutdown: {} query session(s) still running a statement after {} ms ({} inside a "
                       "transaction block); stopping the pool",
                       live_sessions(),
                       kSessionDrainTimeout.count(),
                       sessions_in_block());
        }
        // No pool thread resumes a session after this (idempotent: a second
        // stop of the pool itself would hang); reap() frees what the pool left
        // (a session whose job it dropped).
        runtime_.stop_sessions();
        reap();
    }

    session_ptr session_factory_t::open() {
        OTX_ZONE_N("session_factory::open");
        auto spawned = actor_zeta::spawn<query_session_t>(resource_,
                                                          scheduler_,
                                                          parser_factory_(resource_),
                                                          services_,
                                                          this);
        add(spawned.get());
        // The session frees itself (query_session_t::destroy); the owner's handle
        // only closes it.
        return session_ptr{spawned.release()};
    }

    void session_factory_t::add(query_session_t* session) {
        std::lock_guard lock(mutex_);
        sessions_.insert(session);
    }

    void session_factory_t::forget(query_session_t* session) noexcept {
        std::lock_guard lock(mutex_);
        sessions_.erase(session);
        if (sessions_.empty()) {
            empty_.notify_all();
        }
    }

    std::size_t session_factory_t::live_sessions() const {
        std::lock_guard lock(mutex_);
        return sessions_.size();
    }

    std::size_t session_factory_t::sessions_in_block() const {
        // A session leaves the set (forget) under this lock before its
        // memory goes, so every one read here is alive.
        std::lock_guard lock(mutex_);
        std::size_t count = 0;
        for (const auto* session : sessions_) {
            if (session->transaction_state() != transaction_state_t::idle) {
                ++count;
            }
        }
        return count;
    }

    bool session_factory_t::wait_closed(std::chrono::milliseconds timeout) {
        OTX_ZONE_N("session_factory::wait_closed");
        std::unique_lock lock(mutex_);
        return empty_.wait_for(lock, timeout, [this] { return sessions_.empty(); });
    }

    void session_factory_t::reap() noexcept {
        OTX_ZONE_N("session_factory::reap");
        // Taken out under the lock and freed outside it: a session's destroy()
        // calls forget(), which locks.
        std::vector<query_session_t*> left;
        {
            std::lock_guard lock(mutex_);
            left.assign(sessions_.begin(), sessions_.end());
            sessions_.clear();
        }
        for (auto* session : left) {
            // The pool has stopped, so no job of it can run again; its entry is
            // already gone, so the free below does not call back here.
            actor_zeta::pmr::deleter_t{session->resource()}(session);
        }
    }

} // namespace otterstax::session
