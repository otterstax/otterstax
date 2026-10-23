// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/common/wait.hpp"

#include <actor-zeta.hpp>

#include <atomic>
#include <memory>
#include <mutex>

// Parks a cooperative actor (actor_zeta::basic_actor) while its behavior is suspended on a
// pool answer, instead of letting actor-zeta re-run it until the answer arrives.
//
// actor-zeta (1.2.0, and 2.0.0 by design) keeps an actor whose behavior awaits an unsettled
// future scheduled: cooperative_actor::resume_impl answers `resume` with keep_scheduled, and
// the scheduler puts the job straight back on its queue — a busy loop through the queue's
// mutex, a core per waiting actor. Readiness is a flag on the future; settling a promise
// touches neither the mailbox nor the scheduler, so nothing else could wake a parked actor.
//
// This is the conn::wait_owner_t of an actor whose requests go to a work_pool_t:
//   - every request is submitted with watch(ticket)'s waker;
//   - the actor's coroutine calls suspending_on(ticket) right before it suspends on the
//     answer and resumed() right after (db::backend_t::answer);
//   - the actor hides cooperative_actor::resume with one that asks park() first — the
//     scheduler calls it as scheduler::detail::resume_impl<Actor>, statically typed:
//
//         resume_info resume(size_t n) noexcept {
//             if (parking_.park()) return resume_info(resume_result::awaiting, 0);
//             return actor_zeta::basic_actor<Actor>::resume(n);
//         }
//
// park() is true when the behavior is suspended on a ticket not yet settled. The actor then
// answers `awaiting` without entering the base: the job is dropped, the actor stays marked
// scheduled — so a message sent meanwhile does not enqueue it a second time — and the
// ticket's waker enqueues it again. A spurious run (another request's waker) parks again. A
// wait on anything that is not a watched pool answer runs the base, polled as before.
//
// The waker of a request that finds the actor NOT parked sends it the no-op `wake` message
// instead: the actor may be asleep on its mailbox (actor-zeta 1.2.0 blocks it when, after
// resuming one awaited answer, its handler suspends on another and no message is queued).
// At most one wake is in the mailbox at a time: while one is queued the mailbox is not empty,
// so resume_impl never blocks the actor on it (try_block is reached only on an empty
// mailbox), and a second wake would only be one more dispatch of nothing. A queued wake is
// taken only when no handler is suspended, so a waker that skips its send meanwhile has no
// handler waiting on its answer. The actor's wake() calls woken() first, and the next waker
// sends again.
//
// Actor needs `actor_zeta::unique_future<void> wake()` in its dispatch_traits, calling
// woken() before anything else. The scheduler
// must be stopped before the actor is destroyed (as for any actor); the destructor detaches
// the wakers of requests still in flight.
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

    // The flags pair like Dekker's: park() stores `parked` then loads `settled`, the waker
    // stores `settled` then exchanges `parked` (all sequentially consistent), so either
    // park() sees the answer or the waker sees the park — a parked actor is never left
    // without its enqueue.
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
