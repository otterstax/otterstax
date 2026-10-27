// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "pool_config.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <memory_resource>

namespace conn {

    // One request an owner may suspend on. The pool thread settles it — after the answer is
    // set and its promise released — and only then calls the request's waker.
    struct wait_ticket_t {
        std::atomic<bool> settled{false};
    };
    using wait_ticket_ptr = std::shared_ptr<wait_ticket_t>;

    // Lets a Worker park on a pool answer instead of being polled: actor-zeta 1.2.0 re-runs an
    // actor awaiting an unsettled future, so a ticket lets it leave and its waker brings it back.
    // Per request: watch(ticket) builds the waker, suspending_on(ticket) before the suspend, resumed() after.
    class wait_owner_t {
    public:
        virtual ~wait_owner_t() = default;

        // The waker of one request: settles `ticket`, then reschedules the owner.
        virtual waker_t watch(wait_ticket_ptr ticket) = 0;
        virtual void suspending_on(wait_ticket_ptr ticket) = 0;
        virtual void resumed() noexcept = 0;
    };

    // Used by every awaiter of a pool request: watch, submit with the waker, suspended_on_t, co_await.
    // A null owner gives an empty waker. The ticket lives on `resource`, which must be thread-safe:
    // the pool thread may release it last.
    struct watched_t {
        waker_t waker;          // the request is submitted with it
        wait_ticket_ptr ticket; // what the owner suspends on
    };

    inline watched_t watch(wait_owner_t* owner, std::pmr::memory_resource* resource) {
        if (owner == nullptr) {
            return {};
        }
        auto ticket = std::allocate_shared<wait_ticket_t>(std::pmr::polymorphic_allocator<wait_ticket_t>(resource));
        auto waker = owner->watch(ticket);
        return {std::move(waker), std::move(ticket)};
    }

    // The owner of a thread that drives its own coroutine to completion (CatalogManager::enqueue_impl):
    // instead of spinning while a pool answer is outstanding, it sleeps until the answer's waker counts it.
    // Read answers() before checking readiness, then wait_after() it, so an answer in between is not missed.
    class blocking_owner_t final : public wait_owner_t {
    public:
        waker_t watch(wait_ticket_ptr ticket) override {
            return [state = state_, ticket = std::move(ticket)] {
                ticket->settled.store(true);
                {
                    std::lock_guard guard(state->mutex);
                    ++state->answers;
                }
                state->answered.notify_all();
            };
        }
        void suspending_on(wait_ticket_ptr ticket) override {
            std::lock_guard guard(state_->mutex);
            state_->waiting_on = std::move(ticket);
        }
        void resumed() noexcept override {
            std::lock_guard guard(state_->mutex);
            state_->waiting_on.reset();
        }

        std::uint64_t answers() const {
            std::lock_guard guard(state_->mutex);
            return state_->answers;
        }

        // Until an answer arrives after `seen`. Suspended on a watched pool answer, its waker ends the
        // wait (1 s is only a safety net); a wait on anything else is polled every `poll`.
        void wait_after(std::uint64_t seen, std::chrono::microseconds poll) const {
            std::unique_lock lock(state_->mutex);
            const std::chrono::microseconds limit = state_->waiting_on ? std::chrono::seconds(1) : poll;
            state_->answered.wait_for(lock, limit, [&] { return state_->answers != seen; });
        }

    private:
        // Shared with the wakers, so one that outlives the owner stays valid.
        struct state_t {
            std::mutex mutex;
            std::condition_variable answered;
            std::uint64_t answers{0};
            wait_ticket_ptr waiting_on; // the watched answer the coroutine is suspended on, if any
        };
        std::shared_ptr<state_t> state_ = std::make_shared<state_t>();
    };

    // Reports the owner suspended on `ticket` while it lives, when the answer is not ready:
    // constructed before the co_await, destroyed after it resumes or with the coroutine frame,
    // so an owner whose coroutine is destroyed mid-wait is not left waiting on a ticket.
    class suspended_on_t {
    public:
        suspended_on_t(wait_owner_t* owner, const wait_ticket_ptr& ticket, bool ready)
            : owner_(owner != nullptr && ticket != nullptr && !ready ? owner : nullptr) {
            if (owner_ != nullptr) {
                owner_->suspending_on(ticket);
            }
        }
        ~suspended_on_t() {
            if (owner_ != nullptr) {
                owner_->resumed();
            }
        }

        suspended_on_t(const suspended_on_t&) = delete;
        suspended_on_t& operator=(const suspended_on_t&) = delete;

    private:
        wait_owner_t* owner_;
    };

} // namespace conn
