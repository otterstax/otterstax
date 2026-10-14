// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta/scheduler/resumable.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>

#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <string_view>
#include <utility>

namespace db {

    // Where a backend runs the blocking part of the federation's statements (`scan` / `write`:
    // the connection checkout and the wait for the backend's answer), off the engine's executors.
    // The threads are not the backend's: the pool is one otterstax creates and sizes in runtime_t from
    // config.yaml (`service.runtime.backends`, runtime_t::backends_scheduler()) and every backend
    // shares. A job is a one-shot resumable on that actor-zeta work-sharing scheduler: it runs once
    // on a pool thread and reports `done`.
    //
    // The pool must keep running until every executor posting to it is destroyed — an actor-zeta
    // scheduler drops the jobs still queued at stop() without running them. The destructor waits
    // for every job posted through this executor to finish, so a statement still queued completes
    // (and fulfils its operator's promise) before the connector manager it uses goes. The wait is
    // bounded by the jobs themselves: the backends make their connector manager refuse new
    // statements and abort the running ones first (ConnectorManager::shutdown).
    //
    // A job that throws is contained: the exception is swallowed on the pool thread, the job still
    // counts as finished, and whatever it owned (its promise included — an actor-zeta promise
    // destroyed unfulfilled settles its future with broken_pipe) is destroyed. Jobs convert their
    // own failures into the value their promise carries (answer_of below); this is the last guard.
    //
    // Thread safety: post() may be called from any thread.
    class statement_executor_t {
    public:
        using pool_t = actor_zeta::scheduler::sharing_scheduler;

        explicit statement_executor_t(pool_t* pool) noexcept
            : pool_(pool) {
            assert(pool_ != nullptr);
        }

        statement_executor_t(const statement_executor_t&) = delete;
        statement_executor_t& operator=(const statement_executor_t&) = delete;

        ~statement_executor_t() {
            std::unique_lock lock(mutex_);
            idle_.wait(lock, [this] { return pending_ == 0; });
        }

        // Runs `job()` once on a pool thread. Throws only what allocating the job throws
        // (std::bad_alloc), and then nothing is counted and nothing runs.
        template<typename Job>
        void post(Job job) {
            auto task = std::make_unique<task_t<Job>>(this, std::move(job));
            {
                std::lock_guard lock(mutex_);
                ++pending_;
            }
            try {
                pool_->enqueue(task.get());
            } catch (...) {
                finished();
                throw;
            }
            static_cast<void>(task.release());
        }

    private:
        template<typename Job>
        struct task_t {
            task_t(statement_executor_t* owner, Job job)
                : owner(owner)
                , job(std::move(job)) {}

            // The scheduler's resumable protocol (actor_zeta::scheduler::detail::resumable_type).
            actor_zeta::scheduler::resume_info resume(std::size_t) noexcept {
                auto* executor = owner;
                try {
                    job();
                } catch (...) {
                    // Contained (see the class comment): the job's owned state goes with the task.
                }
                delete this;
                executor->finished();
                return actor_zeta::scheduler::resume_info{actor_zeta::scheduler::resume_result::done, 1};
            }

            statement_executor_t* owner;
            Job job;
        };

        void finished() noexcept {
            std::lock_guard lock(mutex_);
            if (--pending_ == 0) {
                idle_.notify_all();
            }
        }

        pool_t* pool_;
        std::mutex mutex_;
        std::condition_variable idle_;
        std::size_t pending_{0};
    };

    // The value `body()` answers, or `on_exception(what)` when it throws: a statement job hands its
    // promise one value either way, so the operator waiting on it gets the failure as an error
    // value instead of a broken promise.
    template<typename Body, typename OnException>
    auto answer_of(Body&& body, OnException&& on_exception) -> decltype(body()) {
        try {
            return body();
        } catch (const std::exception& e) {
            return on_exception(std::string_view{e.what()});
        } catch (...) {
            return on_exception(std::string_view{"unknown exception"});
        }
    }

} // namespace db
