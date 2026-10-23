// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "pool_config.hpp"

#include <atomic>
#include <memory>

namespace conn {

    // One request an owner may suspend on. The pool thread settles it — after the answer is
    // set and its promise released — and only then calls the request's waker.
    struct wait_ticket_t {
        std::atomic<bool> settled{false};
    };
    using wait_ticket_ptr = std::shared_ptr<wait_ticket_t>;

    // An owner that parks while it is suspended on a pool answer instead of being polled: a
    // Worker. actor-zeta 1.2.0 keeps an actor whose behavior awaits an unsettled future
    // scheduled and re-runs it until the future settles — a busy loop through the
    // scheduler's queue. An owner told which request it is suspended on can leave the
    // scheduler instead, and the request's waker brings it back.
    //
    // The protocol, per request: `watch(ticket)` makes the waker the request is submitted
    // with; right before the owner's coroutine suspends on the answer it calls
    // `suspending_on(ticket)`, right after it resumed `resumed()`. An owner that polls on its
    // own (CatalogManager's enqueue_impl, otterstax::test::drive) needs none of this.
    class wait_owner_t {
    public:
        virtual ~wait_owner_t() = default;

        // The waker of one request: settles `ticket`, then reschedules the owner.
        virtual waker_t watch(wait_ticket_ptr ticket) = 0;
        virtual void suspending_on(wait_ticket_ptr ticket) = 0;
        virtual void resumed() noexcept = 0;
    };

    // The protocol above, for every awaiter of a pool request (db::backend_t, the Worker's
    // transfers):
    //
    //     auto watched = conn::watch(owner);
    //     auto answer = pool.submit(request, std::move(watched.waker));
    //     conn::suspended_on_t suspended(owner, watched.ticket, answer.is_ready());
    //     auto outcome = co_await std::move(answer);
    //
    // Without an owner (null) the waker is empty and nothing is reported.
    struct watched_t {
        waker_t waker;          // the request is submitted with it
        wait_ticket_ptr ticket; // what the owner suspends on
    };

    inline watched_t watch(wait_owner_t* owner) {
        if (owner == nullptr) {
            return {};
        }
        auto ticket = std::make_shared<wait_ticket_t>();
        auto waker = owner->watch(ticket);
        return {std::move(waker), std::move(ticket)};
    }

    // Reports the owner suspended on `ticket` while it lives, when the answer is not there
    // yet: constructed right before the co_await, destroyed right after it resumes — or with
    // the coroutine frame, so an owner whose coroutine is destroyed mid-wait is not left
    // waiting on a ticket.
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
