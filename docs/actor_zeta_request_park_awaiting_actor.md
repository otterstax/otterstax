# A cooperative actor awaiting a future settled on another thread is re-run until it settles: one core per waiting actor

**Version:** actor-zeta 1.2.0 (the Conan package OtterStax builds with). The behaviour is
unchanged by design on `main` at `9009d5b` ("Fix the lost wakeup in resume_impl", the 2.0.0
line); line references below are to that commit.

## Summary

- A `basic_actor` whose behavior is suspended on `co_await` of a `unique_future` stays
  scheduled until the future settles, and the scheduler re-runs it in a loop.
- When the promise is settled by a non-actor thread (a connection pool, an I/O thread),
  that is a busy loop through the scheduler queue's mutex and condition variable for the
  whole wait: a full core per waiting actor.
- 1.2.0 also loses a wakeup when a handler, after resuming one awaited future, suspends on a
  second one. 2.0.0 fixes that.
- Parking needs three additions to actor-zeta: an actor that knows its scheduler, a future
  state that can name the actor waiting on it, and a `resume_impl` that parks instead of
  re-queuing.

## Reproduction

An actor on a 4-thread `sharing_scheduler` awaits a statement answered by a pool thread
after `wait_ms`. CPU is the whole process (`getrusage`):

| Statement time × actors | Re-run (today): CPU per await | Cores busy | Parked: CPU per await | Cores busy | Wall time, re-run / parked |
|---|---|---|---|---|---|
| 0 ms × 1 | 0.067 ms | 1.28 | 0.053 ms | 1.06 | 52 µs / 49 µs |
| 2 ms × 1 | 3.87 ms | 1.29 | 0.17 ms | 0.06 | 3.00 ms / 3.05 ms |
| 20 ms × 1 | 31.4 ms | 1.29 | 0.28 ms | 0.01 | 24.3 ms / 23.9 ms |
| 20 ms × 4 | 20.9 ms | 3.28 | 0.22 ms | 0.03 | 25.6 ms / 24.7 ms |

"Parked" is the workaround OtterStax ships (see the last section). It shows that parking
costs no latency. The benchmark is `BM_actor_await_{spin,park}` in OtterStax's
`benchmark/microbench/bench_connection_pool.cpp`.

## Root cause

**Settling a promise only sets flags** (`detail/future.hpp`). `promise::settle()` sets
`promise_finalizing`, calls `shared_state::release_promise()` (a `fetch_or` of
`promise_released`, `detail/shared_state.hpp`), and returns. It never touches a mailbox or a
scheduler; the comment says so: *"The continuation is NOT taken or resumed here -- the
consumer's own resume_impl() does that, on its own thread."* Nothing in the state records
who is waiting.

**So `resume_impl` keeps a busy behavior scheduled** (`actor/cooperative_actor.hpp`):

```cpp
// A behavior on a co_await must report `resume`, never `awaiting` (awaiting pairs with
// keep_scheduled = false: nothing would ever wake it). Readiness is flag-only --
// release_promise() touches neither mailbox nor scheduler -- so the mailbox cannot speak for it.
if (current_behavior_.is_busy()) {
    if (current_behavior_.is_awaited_ready()) { ... cont.resume(); }
    if (current_behavior_.is_busy()) {
        return finalize(scheduler::resume_result::resume, 0, true);   // re-queued: the spin
    }
}
```

The same branch repeats inside the message loop. `park()` also rules out `(awaiting,
keep_scheduled = true)`: `~resume_guard` would set `scheduled` with no job in any queue, and
nothing would claim the bit.

**The actor cannot reschedule itself.** A `basic_actor` does not know its scheduler.
Whoever gets `needs_sched == true` from `actor_zeta::send` must call
`scheduler->enqueue(actor)`, and a thread that settles a promise has no scheduler to
enqueue on.

**1.2.0 only: the lost wakeup.** The entry path resumes a ready continuation and then falls
through to `mailbox().empty()` → `try_block()` without re-checking `is_busy()`. A handler
that suspends on a second future inside that `cont.resume()` leaves the actor blocked on its
mailbox with a live behavior, and nothing wakes it. `9009d5b` fixes this by re-checking
`is_busy()` after `cont.resume()`.

## Request

**A. Ship the lost-wakeup fix where 1.2.x users get it.** Backport `9009d5b`'s re-check to a
1.2.x release, or publish 2.0.0 to the Conan remote. Note that 2.0.0 removed the blocking
future API.

**B. An actor knows the scheduler that runs it.** For example,
`work_sharing::enqueue(Resumable*)` (`scheduler/policy/work_sharing.hpp`) records the
scheduler in the actor the first time it enqueues it. That needs a `scheduler_raw` member,
written before the first `resume` publishes the job and readable from any thread. The
alternative is a `spawn` that takes the scheduler.

**C. A future's state can name the actor waiting on it.** An optional waiter slot in
`detail::shared_state<T>`, plus one of the two free bits in the flag byte:

```cpp
struct future_waiter_t {
    void (*notify)(void* ctx) noexcept;   // reschedules the waiting actor
    void* ctx;                            // the actor, or a handle the actor detaches on destruction
};
inline constexpr std::uint8_t waiter_parked = 0b0100'0000;
```

- **Producer** (`promise::settle`): after `release_promise()`, if `waiter_parked` was set,
  clear it with an atomic read-modify-write and call `notify(ctx)`. That operation decides
  the race against the consumer taking its park back.
- **Consumer** (`release_future`): a future destroyed while parked clears `waiter_parked`
  first, so the producer never calls into a destroyed actor.
- **Nested coroutine chains** already carry the deepest awaited state's flags and
  continuation up to the behavior (`detail/future_awaiters.hpp`). The waiter slot should be
  carried the same way, so `resume_impl` registers on the state the behavior is actually
  blocked on.

**D. `resume_impl` parks a busy behavior instead of re-queuing it.** In both `is_busy()`
branches, for a future that isn't ready:

1. Write the waiter (`notify` = enqueue on the scheduler from B, `ctx` = the actor) on the
   deepest awaited state.
2. Set `waiter_parked`.
3. Re-check readiness. If the future became ready, take the park back by clearing
   `waiter_parked` with a read-modify-write:
   - the bit is still set: resume the continuation now, as today;
   - the bit is already gone: the producer took it, and its `notify` will enqueue the
     actor; return the new verdict without resuming.
4. Otherwise return a new verdict, for example `resume_result::parked`.
   - Like `awaiting`, the scheduler drops the job.
   - Unlike `awaiting`, the `scheduled` bit stays set and belongs to the pending `notify`,
     so a `send()` arriving meanwhile gets `needs_sched == false` and only queues its
     message.
   - `~resume_guard` accepts `keep_scheduled` with no job queued for this verdict only.

Steps 2 and 3, together with the producer's `promise_released` write followed by its
`waiter_parked` read-modify-write, are the usual Dekker pairing. Either the consumer sees
the result, or the producer sees the park. A parked actor is never left without its
enqueue, and is never enqueued twice.

**E. Tests.**
- An actor awaiting a promise settled 200 ms later by a plain `std::thread`: process CPU
  stays a small fraction of the wait, and the result is correct.
- A message sent while the actor is parked: handled after the continuation, once, with
  `needs_sched == false`.
- A handler awaiting two futures in sequence, the second created inside the first's
  continuation (the 1.2.0 lost-wakeup shape).
- Settle racing park in a tight loop, under TSAN: no lost wakeup and no double enqueue.
- The actor destroyed while parked, then the promise settled: no call into freed memory
  (ASAN).
- Non-actor awaiters (`run_until_complete`, polling drivers) register no waiter and behave
  as today.

## What OtterStax does in the meantime

OtterStax parks its Workers on the unmodified 1.2.0 package (`scheduler/parking.hpp`):

- **Tickets.** Every pool request carries a waker and a `wait_ticket_t`
  (`connectors/common/wait.hpp`). The coroutine reports the ticket it suspends on and clears
  it once resumed.
- **Parking.** `Worker::resume` hides `cooperative_actor::resume`, which the scheduler calls
  statically typed as `scheduler::detail::resume_impl<Worker>`. While the Worker is
  suspended on an unsettled ticket, it returns `awaiting` without entering the base, which
  leaves `scheduled` set exactly as D(4) would.
- **Waking.** The ticket's waker enqueues the Worker again. When the Worker isn't parked, it
  sends a no-op `wake` message instead, for 1.2.0's lost wakeup.

Its limits:
- It covers only futures OtterStax watches, which are its pool requests. Any other future
  settled on another thread is still polled.
- It depends on the name-hiding of a non-virtual `resume`, and on actor-zeta's internal
  `scheduled` bit semantics, either of which an actor-zeta upgrade can change silently.

With A–D, OtterStax deletes `scheduler/parking.hpp`, `connectors/common/wait.hpp`,
`Worker::resume` and `Worker::wake`.
