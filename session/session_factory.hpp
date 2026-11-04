// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"
#include "runtime/runtime.hpp"
#include "session/query_session.hpp"

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <unordered_set>

namespace otterstax::session {

    // The parser one session owns (otterbrix/parser/parser.hpp make_parser in
    // the server; tests pass mock parsers).
    using parser_factory_fn = parser_ptr (*)(std::pmr::memory_resource*);

    // Opens one query session per client connection (a wire connection, a Flight
    // SQL RPC or prepared statement, a Spark session) with its own parser, its
    // own engine session and a copy of the services, on the runtime's sessions
    // pool; main.cpp owns it and hands it to every server.
    //
    // It also holds the sessions that exist: a session enters at open() and
    // leaves when it frees itself (query_session_t::destroy → forget()).
    // Thread-safe: owners close sessions from frontend threads, sessions free
    // themselves on pool threads.
    //
    // Shutdown: the servers, declared after the factory, are destroyed first
    // (each stops in its destructor; their connections close their sessions).
    // Then the destructor waits for the closed sessions to free themselves (up
    // to 30 s), stops the runtime's sessions pool (runtime_t::stop_sessions) and
    // frees, through reap(), a closed session the stopped pool left behind. The
    // backends and the engine, declared before the factory, are what those
    // statements still talk to.
    class session_factory_t {
    public:
        session_factory_t(runtime::runtime_t& runtime, parser_factory_fn parser_factory, session_services_t services);
        session_factory_t(const session_factory_t&) = delete;
        session_factory_t& operator=(const session_factory_t&) = delete;
        // wait_closed (30 s; a warning names what is left), then
        // runtime_t::stop_sessions, then reap(): every owner of a session
        // (the servers) must be gone before.
        ~session_factory_t();

        // A new connection's session; dropping the handle closes it.
        [[nodiscard]] session_ptr open();

        [[nodiscard]] std::size_t live_sessions() const;
        // The live sessions inside a transaction block (open or failed) after
        // their last statement: what the shutdown leaves to the rollback of each
        // closing session (the destructor logs it).
        [[nodiscard]] std::size_t sessions_in_block() const;
        // Waits for every closed session to free itself (its statement in flight
        // finished); true when none is left.
        [[nodiscard]] bool wait_closed(std::chrono::milliseconds timeout);

    private:
        // forget() is the session's, as it frees itself.
        friend class query_session_t;

        void add(query_session_t* session);
        // Called by the session as it frees itself; nothing of it is read here.
        void forget(query_session_t* session) noexcept;
        // Frees every session still here. Valid only once the sessions pool has
        // stopped (no pool thread can run one) and every owner has closed its
        // session: what is left is a closed session whose job the stopped pool
        // dropped.
        void reap() noexcept;

        runtime::runtime_t& runtime_;
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        parser_factory_fn parser_factory_;
        session_services_t services_;
        mutable std::mutex mutex_;
        std::condition_variable empty_;
        std::unordered_set<query_session_t*> sessions_;
    };

} // namespace otterstax::session
