// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "pool_config.hpp"

#include "utility/logger.hpp"
#include "utility/pmr_text.hpp"
#include "utility/tracy_profiler.hpp"

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace conn {

    // A queue of requests served by min_size..max_size threads, each owning one Session. Any free thread
    // takes the next request. Every request is answered through an actor_zeta::promise, so the awaiting
    // actor resumes on its own scheduler thread and never blocks.
    //
    // Session provides: using request_t, outcome_t (constructible from core::error_t);
    // core::error_t open() (connect, on the session's own thread except the min_size from open());
    // outcome_t run(const request_t&) (may block, must not throw); void close() noexcept.
    //
    // Sizing (pool_config_t): open() opens min_size sessions on the caller's thread; a failure means no pool.
    // The pool grows by one connect at a time, on the new session's thread, when requests queue past the
    // idle sessions, and not within connect_backoff of a failed connect. Sessions above min_size close
    // after idle_timeout. A request queued past queue_timeout is answered io_error: an expiry thread does
    // it while all sessions are busy, and the check at take-time covers the race between the two.
    // Destruction answers queued requests io_error, then stops and joins every session thread; a running
    // request finishes first.
    //
    // `resource` must be thread-safe: promise states and errors are allocated on session threads and
    // released on the awaiting actor's.
    template<typename Session>
    class work_pool_t {
    public:
        using request_t = typename Session::request_t;
        using outcome_t = typename Session::outcome_t;
        using future_t = actor_zeta::unique_future<outcome_t>;
        using session_factory_t = std::function<std::unique_ptr<Session>()>;
        using clock = std::chrono::steady_clock;

        static core::result_wrapper_t<std::unique_ptr<work_pool_t>> open(std::pmr::memory_resource* resource,
                                                                          std::string_view name,
                                                                          session_factory_t factory,
                                                                          pool_config_t config) {
            OTX_ZONE_N("conn::work_pool::open");
            std::unique_ptr<work_pool_t> pool{new work_pool_t(resource, name, std::move(factory), config)};
            try {
                pool->expirer_ = std::thread([raw = pool.get()] { raw->expire(); });
            } catch (const std::exception& e) {
                return pool->error(core::error_code_t::io_error, "cannot start its expiry thread: ", e.what());
            }
            for (std::size_t i = 0; i < pool->config_.min_size; ++i) {
                auto session = pool->factory_();
                if (auto err = session->open(); err.contains_error()) {
                    return std::move(err); // the pool's destructor stops what already started
                }
                std::lock_guard guard(pool->mutex_);
                ++pool->open_;
                pool->launch(*pool->free_slot(), std::move(session));
            }
            return std::move(pool);
        }

        ~work_pool_t() {
            std::pmr::deque<job_t> left(resource_); // the same resource as queue_: swap needs it
            {
                std::lock_guard guard(mutex_);
                stopping_ = true;
                left.swap(queue_);
            }
            ready_.notify_all();
            expiry_.notify_all();
            // An unanswered actor_zeta::promise stores an error, and awaiting it asserts.
            for (auto& job : left) {
                job.promise.set_value(outcome_t{error(core::error_code_t::io_error, "is shutting down")});
            }
            if (expirer_.joinable()) {
                expirer_.join();
            }
            for (auto& slot : slots_) {
                if (slot.thread.joinable()) {
                    slot.thread.join();
                }
            }
        }

        work_pool_t(const work_pool_t&) = delete;
        work_pool_t& operator=(const work_pool_t&) = delete;

        // Any thread; never blocks. `waker` (may be empty) is called on the session thread once the
        // answer is settled. A request refused because the pool is stopping is settled before submit
        // returns; shutdown answers queued requests without waking anyone, as their awaiters are gone.
        future_t submit(request_t request, waker_t waker = {}) {
            actor_zeta::promise<outcome_t> promise{resource_};
            auto future = promise.get_future();
            {
                std::lock_guard guard(mutex_);
                if (!stopping_) {
                    const bool was_empty = queue_.empty();
                    queue_.push_back(job_t{std::move(request), std::move(promise), clock::now(), std::move(waker)});
                    maybe_grow();
                    ready_.notify_one();
                    if (was_empty) {
                        expiry_.notify_one(); // the expiry thread waits for the oldest request only
                    }
                    return future;
                }
            }
            promise.set_value(outcome_t{error(core::error_code_t::io_error, "is shutting down")});
            return future;
        }

        std::string_view name() const noexcept { return name_; }
        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Sessions open or connecting, and requests waiting; for tests and plots.
        std::size_t open_sessions() const {
            std::lock_guard guard(mutex_);
            return open_;
        }
        std::size_t queued() const {
            std::lock_guard guard(mutex_);
            return queue_.size();
        }

    private:
        struct job_t {
            request_t request;
            actor_zeta::promise<outcome_t> promise;
            clock::time_point queued_at;
            waker_t waker;
        };

        // Answers `job`, releases its promise — the awaiter polls for the release, not for
        // the value — and only then wakes the awaiter.
        static void settle(job_t job, outcome_t outcome) {
            auto waker = std::move(job.waker);
            job.promise.set_value(std::move(outcome)); // noexcept; never resumes the waiter
            { auto released = std::move(job.promise); }
            if (waker) {
                waker();
            }
        }

        enum class slot_state
        {
            free,
            running,
            finished // the thread is past its last use of the pool; join() returns at once
        };

        struct slot_t {
            slot_state state{slot_state::free};
            std::unique_ptr<Session> session; // owned by the slot's thread while it runs
            std::thread thread;
        };

        work_pool_t(std::pmr::memory_resource* resource,
                    std::string_view name,
                    session_factory_t factory,
                    pool_config_t config)
            : resource_(resource)
            , name_(name, resource)
            , factory_(std::move(factory))
            , config_(config)
            , log_(get_logger(logger_tag::CONNECTOR_MANAGER))
            , queue_(resource)
            , slots_(resource) {
            assert(resource_ != nullptr);
            assert(factory_);
            config_.max_size = std::max<std::size_t>(config_.max_size, 1);
            config_.min_size = std::min(config_.min_size, config_.max_size);
            slots_.resize(config_.max_size); // never reallocated: threads hold slot references
        }

        // "pool '<name>' <what><detail>"
        core::error_t error(core::error_code_t code, std::string_view what, std::string_view detail = {}) const {
            return core::error_t(code, otterstax::concat(resource_, {"pool '", name_, "' ", what, detail}));
        }

        // Under mutex_. A finished slot's thread no longer touches the pool, so it joins at once.
        slot_t* free_slot() {
            for (auto& slot : slots_) {
                if (slot.state == slot_state::finished) {
                    slot.thread.join();
                    slot.state = slot_state::free;
                }
                if (slot.state == slot_state::free) {
                    return &slot;
                }
            }
            return nullptr;
        }

        // Under mutex_; open_ already counts the session. A null session connects on the new
        // thread.
        void launch(slot_t& slot, std::unique_ptr<Session> session) {
            slot.state = slot_state::running;
            slot.session = std::move(session);
            try {
                slot.thread = std::thread([this, &slot] { serve(slot); });
            } catch (const std::exception& e) {
                slot.state = slot_state::free;
                slot.session.reset();
                --open_;
                connecting_ = false;
                if (log_.is_valid()) {
                    log_->error("pool '{}': cannot start a session thread: {}", name_, e.what());
                }
            }
        }

        // Under mutex_. Grow only when the queue holds more than the idle sessions will take,
        // below the cap, one connect at a time, and not right after a failed connect.
        void maybe_grow() {
            if (stopping_ || connecting_ || open_ >= config_.max_size || queue_.size() <= idle_) {
                return;
            }
            if (clock::now() < next_connect_attempt_) {
                return;
            }
            slot_t* slot = free_slot();
            if (slot == nullptr) {
                return;
            }
            connecting_ = true;
            ++open_;
            launch(*slot, nullptr);
        }

        outcome_t busy() const {
            return outcome_t{error(core::error_code_t::io_error, "is busy: the request waited past queue_timeout")};
        }

        // The expiry thread. The queue is in arrival order, so its front is the oldest
        // request: sleep until that one is due, answer every request that is, repeat. A
        // request a session takes first is simply gone from the queue.
        void expire() {
            std::unique_lock lock(mutex_);
            while (!stopping_) {
                if (queue_.empty()) {
                    expiry_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
                    continue;
                }
                const auto due = queue_.front().queued_at + config_.queue_timeout;
                if (clock::now() <= due) {
                    expiry_.wait_until(lock, due);
                    continue;
                }
                std::pmr::vector<job_t> expired(resource_);
                const auto now = clock::now();
                while (!queue_.empty() && now - queue_.front().queued_at > config_.queue_timeout) {
                    expired.push_back(std::move(queue_.front()));
                    queue_.pop_front();
                }
                lock.unlock();
                for (auto& job : expired) {
                    settle(std::move(job), busy()); // wakes its awaiter, which is alive: not a shutdown
                }
                lock.lock();
            }
        }

        outcome_t run(Session& session, job_t& job) {
            if (clock::now() - job.queued_at > config_.queue_timeout) {
                return busy();
            }
            try {
                return session.run(job.request);
            } catch (const std::exception& e) {
                return outcome_t{error(core::error_code_t::io_error, "session failed: ", e.what())};
            } catch (...) {
                return outcome_t{error(core::error_code_t::io_error, "session failed")};
            }
        }

        void serve(slot_t& slot) {
            // A grown session connects here, on its own thread: a connect can take seconds.
            const bool grown = !slot.session;
            std::unique_ptr<Session> fresh;
            core::error_t opened = core::error_t::no_error();
            if (grown) {
                fresh = factory_();
                opened = fresh->open();
                if (opened.contains_error()) {
                    fresh.reset(); // torn down before the slot reads as finished: free_slot joins it under mutex_
                }
            }

            std::unique_lock lock(mutex_);
            if (grown) {
                connecting_ = false;
                if (opened.contains_error()) {
                    if (log_.is_valid()) {
                        log_->warn("pool '{}': opening another session failed: {}", name_, opened.what.c_str());
                    }
                    next_connect_attempt_ = clock::now() + config_.connect_backoff;
                    --open_;
                    slot.state = slot_state::finished;
                    return; // the queued requests stay for the open sessions
                }
                slot.session = std::move(fresh);
                maybe_grow(); // a burst may need more than this one session
            }

            for (;;) {
                ++idle_;
                const bool has_work =
                    ready_.wait_for(lock, config_.idle_timeout, [this] { return stopping_ || !queue_.empty(); });
                --idle_;
                if (stopping_) {
                    break;
                }
                if (!has_work) {
                    // A session still connecting may yet fail, so it does not count toward the floor.
                    if (open_ - (connecting_ ? 1 : 0) > config_.min_size) {
                        break; // idle too long, above the floor
                    }
                    continue;
                }
                job_t job = std::move(queue_.front());
                queue_.pop_front();
                lock.unlock();
                auto outcome = run(*slot.session, job);
                settle(std::move(job), std::move(outcome));
                lock.lock();
            }

            --open_;
            lock.unlock();
            slot.session->close(); // may take a round trip: not under the pool's lock
            lock.lock();
            slot.session.reset();
            slot.state = slot_state::finished;
        }

        std::pmr::memory_resource* resource_;
        std::pmr::string name_;
        session_factory_t factory_; // called at open() and when the pool grows, never per request
        pool_config_t config_;
        log_t log_;

        mutable std::mutex mutex_;
        std::condition_variable ready_;
        std::condition_variable expiry_; // the expiry thread: a request queued into an empty queue, or stop
        std::pmr::deque<job_t> queue_;
        std::size_t open_{0};     // sessions open or connecting
        std::size_t idle_{0};     // sessions waiting for a request
        bool connecting_{false};  // at most one connect in flight
        clock::time_point next_connect_attempt_{};
        bool stopping_{false};
        std::pmr::vector<slot_t> slots_;
        std::thread expirer_; // runs expire(); joined by the destructor
    };

} // namespace conn
