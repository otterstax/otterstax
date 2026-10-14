// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <chrono>
#include <cstddef>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace frontend::spark {

    // The Spark sessions the server holds, by the client's session_id, and their
    // end the way Spark Connect's own SessionManager ends them: a session that
    // received no RPC for `idle_timeout` is closed, and so is one ReleaseSession
    // names; either way its id is remembered (the most recent `tombstones` of
    // them, Spark's closedSessionsTombstonesSize) so a client that comes back
    // with it is refused rather than silently handed a fresh session — one that
    // would know nothing of its transaction or its prepared state. Thread-safe.
    //
    // `Session` is the owning handle of what one Spark session owns (the
    // service's session_ptr): the registry is its only owner, and dropping it —
    // under the lock — closes it. An RPC never holds it: each of its steps
    // reaches the session under the lock (`open_or_use`, `use`, `begin`), where it only queues
    // its statement, and awaits the answer outside; a statement the session has
    // taken runs to its end after the close (query_session_t's close protocol).
    // A step whose answer the RPC awaits (`begin` … `finish`) keeps the session
    // from expiring meanwhile; a release closes it all the same.
    template<typename Session>
    class idle_session_registry_t {
    public:
        using clock = std::chrono::steady_clock;

        // `idle_timeout` zero: sessions are kept until released.
        explicit idle_session_registry_t(std::chrono::milliseconds idle_timeout, std::size_t tombstones = 1000)
            : idle_timeout_(idle_timeout)
            , tombstone_limit_(tombstones) {}

        std::chrono::milliseconds idle_timeout() const noexcept { return idle_timeout_; }

        // Calls `step` with the session `id` names, under the lock, and restarts
        // its idle clock at `now`. False — `step` not called — for an id not
        // held (never opened, expired or released).
        template<typename Step>
        bool use(const std::string& id, clock::time_point now, Step&& step) {
            std::lock_guard lock(mutex_);
            auto it = sessions_.find(id);
            if (it == sessions_.end()) {
                return false;
            }
            run_step(it->second, now, step);
            return true;
        }

        // `use`, and the session's first RPC opens it (`open()`). False — `step`
        // not called, nothing opened — for an id that was closed (expired or
        // released).
        template<typename Step, typename Open>
        bool open_or_use(const std::string& id, clock::time_point now, Step&& step, Open&& open) {
            std::lock_guard lock(mutex_);
            auto it = sessions_.find(id);
            if (it == sessions_.end()) {
                if (closed_.contains(id)) {
                    return false;
                }
                it = sessions_.emplace(id, entry_t{open(), now}).first;
            }
            run_step(it->second, now, step);
            return true;
        }

        // A step whose answer the RPC awaits: `use`, and the step stays in work
        // — the session is not closed as idle — until the RPC calls `finish`
        // (whatever the wait ended with). Counted only once `step` returned.
        // False — `step` not called, nothing to finish — as for `use`.
        template<typename Step>
        bool begin(const std::string& id, clock::time_point now, Step&& step) {
            std::lock_guard lock(mutex_);
            auto it = sessions_.find(id);
            if (it == sessions_.end()) {
                return false;
            }
            run_step(it->second, now, step);
            ++it->second.steps_in_work;
            return true;
        }

        // The end of a step `begin` counted, at `now`, which restarts the idle
        // clock. Nothing for a session closed meanwhile (released).
        void finish(const std::string& id, clock::time_point now) {
            std::lock_guard lock(mutex_);
            auto it = sessions_.find(id);
            if (it == sessions_.end() || it->second.steps_in_work == 0) {
                return;
            }
            --it->second.steps_in_work;
            it->second.last_rpc = now;
        }

        // ReleaseSession: the session leaves the registry — closed here, under
        // the lock — and its id is remembered as closed, so a later RPC with it
        // is refused. An id never seen is remembered too (Spark refuses it the
        // same way after a release). True when a session was closed.
        bool release(const std::string& id) {
            std::lock_guard lock(mutex_);
            const bool released = sessions_.erase(id) != 0;
            remember_closed(id);
            return released;
        }

        // Closes the sessions that received no RPC within the timeout at `now`
        // and have no step in work, and remembers their ids as closed; answers
        // how many.
        std::size_t evict_idle(clock::time_point now) {
            if (idle_timeout_.count() == 0) {
                return 0;
            }
            std::lock_guard lock(mutex_);
            std::size_t evicted = 0;
            for (auto it = sessions_.begin(); it != sessions_.end();) {
                if (it->second.steps_in_work == 0 && now - it->second.last_rpc >= idle_timeout_) {
                    remember_closed(it->first);
                    it = sessions_.erase(it);
                    ++evicted;
                } else {
                    ++it;
                }
            }
            return evicted;
        }

        // Closes every session: the server's stop, which closes the query
        // sessions of every Spark session it served.
        void clear() {
            std::lock_guard lock(mutex_);
            sessions_.clear();
        }

        std::size_t size() const {
            std::lock_guard lock(mutex_);
            return sessions_.size();
        }

    private:
        struct entry_t {
            Session session;
            clock::time_point last_rpc;
            std::size_t steps_in_work = 0; // begun and not finished
        };

        // A step on a held session, under the lock: its idle clock restarted.
        template<typename Step>
        static void run_step(entry_t& entry, clock::time_point now, Step& step) {
            entry.last_rpc = now;
            step(*entry.session);
        }

        void remember_closed(const std::string& id) {
            if (tombstone_limit_ == 0 || closed_.contains(id)) {
                return;
            }
            if (closed_order_.size() == tombstone_limit_) {
                closed_.erase(closed_order_.front());
                closed_order_.pop_front();
            }
            closed_.insert(id);
            closed_order_.push_back(id);
        }

        const std::chrono::milliseconds idle_timeout_;
        const std::size_t tombstone_limit_;
        mutable std::mutex mutex_;
        std::unordered_map<std::string, entry_t> sessions_;
        std::unordered_set<std::string> closed_;
        std::deque<std::string> closed_order_;
    };

} // namespace frontend::spark
