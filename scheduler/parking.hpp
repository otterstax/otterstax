// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/common/wait.hpp"

#include <actor-zeta.hpp>

#include <atomic>
#include <memory>
#include <mutex>

// Parks a cooperative actor while its behavior is suspended on a pool answer. Without it actor-zeta
// (1.2.0, and 2.0.0 by design) re-runs the actor in a busy loop, a core per waiting actor, because
// settling a promise touches neither the mailbox nor the scheduler.
//
// This is the conn::wait_owner_t of an actor whose requests go to a work_pool_t:
//   - every request is submitted with watch(ticket)'s waker;
//   - the coroutine calls suspending_on(ticket) just before it suspends on the answer and resumed()
//     just after (db::backend_t::answer);
//   - the actor hides cooperative_actor::resume with a version that asks park() first:
//
//         resume_info resume(size_t n) noexcept {
//             if (parking_.park()) return resume_info(resume_result::awaiting, 0);
//             return actor_zeta::basic_actor<Actor>::resume(n);
//         }
//
// park() is true when the behavior waits on an unsettled ticket. The job is dropped but the actor
// stays marked scheduled, so no message enqueues it twice, and the ticket's waker enqueues it again.
// Waits on anything but a watched pool answer run the base and are polled.
//
// A waker that finds the actor not parked sends it the no-op `wake` message, since actor-zeta 1.2.0
// may have blocked the actor on its mailbox. At most one wake is queued: a queued wake keeps the
// mailbox non-empty, so the actor is never blocked on it. wake() calls woken() first; the next waker
// sends again.
//
// Actor needs `unique_future<void> wake()` in its dispatch_traits, calling woken() first. Stop the
// scheduler before destroying the actor; the destructor detaches the wakers of requests in flight.
template<class Actor>
class actor_parking_t final : public conn::wait_owner_t {
public:
    actor_parking_t(Actor* actor, actor_zeta::scheduler_raw scheduler)
        : target_(std::make_shared<target_t>(actor, scheduler)) {}

    ~actor_parking_t() override {
        std::lock_guard guard(target_->mutex);
        target_->actor = nullptr;
    }

    actor_parking_t(const actor_parking_t&) = delete;
    actor_parking_t& operator=(const actor_parking_t&) = delete;

    // On the pool thread, after the answer is settled and its promise released.
    conn::waker_t watch(conn::wait_ticket_ptr ticket) override {
        return [target = target_, ticket = std::move(ticket)] {
            ticket->settled.store(true);
            std::lock_guard guard(target->mutex);
            Actor* actor = target->actor;
            if (actor == nullptr) {
                return;
            }
            if (target->parked.exchange(false)) {
                // Still marked scheduled: a message would not schedule it.
                target->scheduler->enqueue(actor);
                return;
            }
            if (target->wake_pending.exchange(true)) {
                return; // a wake is already queued, and the actor cannot block on its mailbox before taking it
            }
            auto [needs_sched, future] = actor_zeta::send(actor->address(), &Actor::wake);
            if (needs_sched) {
                target->scheduler->enqueue(actor);
            }
        };
    }

    // From the actor's wake(), first thing: the queued wake has been taken, so the next waker
    // that finds the actor not parked sends another. An exchange, so it reads the last
    // skipping waker's write and everything that waker settled before it.
    void woken() noexcept { target_->wake_pending.exchange(false); }

    void suspending_on(conn::wait_ticket_ptr ticket) override {
        std::lock_guard guard(waiting_mutex_);
        waiting_on_ = std::move(ticket);
    }

    void resumed() noexcept override {
        std::lock_guard guard(waiting_mutex_);
        waiting_on_.reset();
    }

    // The flags pair like Dekker's, all seq_cst: park() stores `parked` then loads `settled`, the waker
    // stores `settled` then exchanges `parked`. Either park() sees the answer or the waker sees the park.
    bool park() noexcept {
        conn::wait_ticket_ptr ticket;
        {
            std::lock_guard guard(waiting_mutex_);
            ticket = waiting_on_;
        }
        if (ticket == nullptr || ticket->settled.load()) {
            return false;
        }
        target_->parked.store(true);
        if (!ticket->settled.load()) {
            return true; // the ticket's waker enqueues the actor
        }
        // Settled in between: take the park back — unless its waker already took it, and
        // its enqueue is what resumes the actor.
        return !target_->parked.exchange(false);
    }

private:
    // Shared with every waker in flight, so a waker that outlives the actor finds it gone.
    struct target_t {
        target_t(Actor* a, actor_zeta::scheduler_raw s)
            : actor(a)
            , scheduler(s) {}

        std::mutex mutex;
        Actor* actor; // null once the actor is destroyed
        actor_zeta::scheduler_raw scheduler;
        // Set by park() when the actor leaves the scheduler; taken by the waker that
        // enqueues it again.
        std::atomic<bool> parked{false};
        // Set by the waker that sends `wake`; cleared by the actor's wake() (woken()).
        std::atomic<bool> wake_pending{false};
    };

    std::shared_ptr<target_t> target_;
    std::mutex waiting_mutex_;
    conn::wait_ticket_ptr waiting_on_; // the request the behavior is suspended on
};
