// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The Spark sessions the service holds (idle_session_registry_t): opened on a
// session's first RPC, reached under the registry's lock by every step of an
// RPC, closed — by the registry, their only owner — after the idle timeout or
// a release, their ids then refused rather than handed a fresh session — the
// way Spark Connect's own SessionManager expires sessions.

#include "frontend/spark_connect_server/spark_sessions.hpp"

#include <catch2/catch_all.hpp>

#include <chrono>
#include <memory>
#include <string>

namespace {

    using namespace std::chrono_literals;

    // The owning handle, as session_ptr is: dropping it closes the session.
    struct closer_t {
        int* closed;
        void operator()(int* session) const {
            ++*closed;
            delete session;
        }
    };
    using handle_t = std::unique_ptr<int, closer_t>;
    using registry_t = frontend::spark::idle_session_registry_t<handle_t>;
    using clock = registry_t::clock;

    struct sessions_t {
        int opened = 0;
        int closed = 0;

        auto opener() {
            return [this] { return handle_t{new int(++opened), closer_t{&closed}}; };
        }
    };

    // The session `id` names as an RPC's first step reaches it (opened on the
    // session's first RPC); 0 for a closed id.
    int first_step(registry_t& registry, sessions_t& sessions, const std::string& id, clock::time_point now) {
        int seen = 0;
        registry.open_or_use(id, now, [&](int& session) { seen = session; }, sessions.opener());
        return seen;
    }

    // A later step of an RPC: never opens a session.
    int later_step(registry_t& registry, const std::string& id, clock::time_point now) {
        int seen = 0;
        registry.use(id, now, [&](int& session) { seen = session; });
        return seen;
    }

} // namespace

TEST_CASE("spark sessions: one session per id, opened on its first RPC") {
    registry_t registry(60min);
    sessions_t sessions;
    const auto now = clock::now();
    const int a = first_step(registry, sessions, "a", now);
    const int again = first_step(registry, sessions, "a", now);
    const int b = first_step(registry, sessions, "b", now);
    CHECK(a != 0);
    CHECK(a == again);
    CHECK(b != a);
    CHECK(sessions.opened == 2);
    CHECK(registry.size() == 2);
    // A later step reaches the session the first one opened, and opens none.
    CHECK(later_step(registry, "a", now) == a);
    CHECK(later_step(registry, "never-seen", now) == 0);
    CHECK(sessions.opened == 2);
}

TEST_CASE("spark sessions: an idle session is closed and its id refused afterwards") {
    registry_t registry(10min);
    sessions_t sessions;
    const auto start = clock::now();
    first_step(registry, sessions, "a", start);
    first_step(registry, sessions, "b", start);
    // b had an RPC since; a did not.
    first_step(registry, sessions, "b", start + 8min);

    CHECK(registry.evict_idle(start + 9min) == 0);
    CHECK(registry.evict_idle(start + 10min) == 1);
    CHECK(sessions.closed == 1);
    CHECK(registry.size() == 1);

    // The expired id is refused, by a first step and a later one alike; the
    // other lives on until its own timeout.
    CHECK(first_step(registry, sessions, "a", start + 11min) == 0);
    CHECK(later_step(registry, "a", start + 11min) == 0);
    CHECK(first_step(registry, sessions, "b", start + 11min) == 2);
    CHECK(sessions.opened == 2);
}

// The registry is the session's only owner: an RPC holds nothing, so the idle
// clock is the last step of an RPC — every step restarts it — and a session
// whose RPC sent its last step longer ago than the timeout is closed (the
// statement it took runs to its end; the RPC awaits its answer, not the
// session).
TEST_CASE("spark sessions: every step of an RPC restarts the idle clock") {
    registry_t registry(1min);
    sessions_t sessions;
    const auto start = clock::now();
    first_step(registry, sessions, "a", start);
    CHECK(later_step(registry, "a", start + 50s) == 1);
    CHECK(registry.evict_idle(start + 100s) == 0);
    CHECK(registry.evict_idle(start + 110s) == 1);
    CHECK(sessions.closed == 1);
    CHECK(later_step(registry, "a", start + 110s) == 0);
}

// A step whose answer its RPC awaits (begin ... finish) keeps the session
// from expiring however long the wait; its finish restarts the idle clock.
// A release closes it all the same.
TEST_CASE("spark sessions: a session with a step in work is not closed as idle") {
    registry_t registry(1min);
    sessions_t sessions;
    const auto start = clock::now();
    first_step(registry, sessions, "a", start);
    int seen = 0;
    CHECK(registry.begin("a", start, [&](int& session) { seen = session; }));
    CHECK(seen == 1);
    CHECK(registry.begin("a", start, [](int&) {}));
    CHECK(registry.evict_idle(start + 10min) == 0);
    registry.finish("a", start + 10min);
    CHECK(registry.evict_idle(start + 20min) == 0);
    registry.finish("a", start + 20min);
    CHECK(registry.evict_idle(start + 20min + 59s) == 0);
    CHECK(registry.evict_idle(start + 21min) == 1);
    CHECK(sessions.closed == 1);
    // A closed session begins nothing, and a finish of it changes nothing.
    CHECK_FALSE(registry.begin("a", start + 21min, [&](int&) { seen = 0; }));
    CHECK(seen == 1);
    registry.finish("a", start + 21min);

    first_step(registry, sessions, "b", start);
    CHECK(registry.begin("b", start, [](int&) {}));
    CHECK(registry.release("b"));
    CHECK(sessions.closed == 2);
    registry.finish("b", start + 1min);
    CHECK(registry.size() == 0);
}

TEST_CASE("spark sessions: a zero timeout keeps sessions until released") {
    registry_t registry(0ms);
    sessions_t sessions;
    const auto start = clock::now();
    first_step(registry, sessions, "a", start);
    CHECK(registry.evict_idle(start + 24h) == 0);
    CHECK(registry.release("a"));
    CHECK(sessions.closed == 1);
    CHECK(registry.size() == 0);
    CHECK(sessions.opened == 1);
}

// ReleaseSession closes the session the way an expiry does: its id is
// remembered, so a request that comes back with it is refused
// (INVALID_HANDLE.SESSION_CLOSED, as Spark answers it) rather than handed a
// fresh session that knows nothing of its transaction.
TEST_CASE("spark sessions: a released id is refused afterwards") {
    registry_t registry(1min);
    sessions_t sessions;
    const auto start = clock::now();
    first_step(registry, sessions, "a", start);
    CHECK(registry.release("a"));
    CHECK(sessions.closed == 1);
    CHECK(first_step(registry, sessions, "a", start) == 0);
    // Released again: nothing to close, still refused.
    CHECK_FALSE(registry.release("a"));
    CHECK(first_step(registry, sessions, "a", start) == 0);
    // An id released before any request is refused too.
    CHECK_FALSE(registry.release("never-seen"));
    CHECK(first_step(registry, sessions, "never-seen", start) == 0);
    CHECK(sessions.opened == 1);
}

// The server's stop closes every session.
TEST_CASE("spark sessions: clear closes every session") {
    registry_t registry(1min);
    sessions_t sessions;
    const auto start = clock::now();
    for (const char* id : {"a", "b"}) {
        first_step(registry, sessions, id, start);
    }
    registry.clear();
    CHECK(sessions.closed == 2);
    CHECK(registry.size() == 0);
}

TEST_CASE("spark sessions: the expired ids remembered are bounded") {
    registry_t registry(1min, /*tombstones=*/2);
    sessions_t sessions;
    const auto start = clock::now();
    for (const char* id : {"a", "b", "c"}) {
        first_step(registry, sessions, id, start);
    }
    CHECK(registry.evict_idle(start + 2min) == 3);
    // Two of the three are remembered; the oldest one fell out.
    int refused = 0;
    for (const char* id : {"a", "b", "c"}) {
        refused += first_step(registry, sessions, id, start + 3min) == 0 ? 1 : 0;
    }
    CHECK(refused == 2);
}
