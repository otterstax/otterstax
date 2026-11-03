# Embedding actor-zeta in a long-lived host: what the host needs from the library

> **Status:** proposal for the maintainers of `otterbrix/actor-zeta`.
>
> **Base:** tag `2.0.0` (`9009d5b`; OtterStax and otterbrix `61aa43d6` run on it, recipe `actor-zeta/2.0.0#887bc992`)
> and the open PR #185 "Actor as a pull-driven generator" (branch `actor-generator`, head `c54a96c`). Requests to
> otterbrix are in [`otterbrix-embedding.md`](otterbrix-embedding.md).

References:

- `az:<path>:<line>` — `header/actor-zeta/<path>` at tag `2.0.0`;
- `az185:<path>:<line>` — the same path at `c54a96c` (PR #185); `az185-doc:` — `docs/LIFECYCLE.md`, `az185-cl:` —
  `CHANGELOG.md`, `az185-test:` — `test/<path>`, all at `c54a96c`;
- `ob:<path>:<line>` — otterbrix `61aa43d6` (PR #850); `otx:<path>[:<line>]` — OtterStax, the port branch
  (`e4ef465`), for illustration only.

Each open request has: **Request**, **Why** (the host scenario), **Evidence**, **Host workaround** (when there is one),
**Acceptance criteria**. Priorities: **P1** — the host works correctly but pays (CPU, latency, an unclear error) or
keeps a workaround; **P2** — nice to have.

---

## 1. Summary

| ID | P | What | Issue / PR |
|---|---|---|---|
| AZ-waker | P1 | settling a future wakes its waiter: a waker on `unique_future` | #186 |
| AZ-park | P1 | a cooperative actor on a pending future parks instead of spinning its pool | follow-up of #186 (item 5) |
| AZ-answer-later | P1 | a cooperative actor's method answers later without holding the turn (a reply promise) | no issue yet |
| AZ-await-outcome | P1 | await a future's outcome (value / code / exception) instead of aborting on an error code | no issue yet |
| AZ-pool-lifecycle | P1 | `stop()` in any state; restart a pool (or refuse it explicitly) | no issue yet |
| AZ-thread-start | P1 | an OS refusal to create a thread is a `start()` error, not `std::system_error` | #188 |
| AZ-exception | P2 | the consumer sees a method's exception without a rethrow; an own code instead of `errc::interrupted` | no issue yet |
| AZ-implements-extra | P2 | `implements<Contract, …>` with actor methods beyond the contract | no issue yet |
| AZ-release | — | `actor-zeta/2.0.0` on the conan remote | done (§3) |
| AZ-teardown | — | destroy an actor while its pool runs | done in #185: `close()` (§3) |

Related, not requested: #189 (a public "someone awaits this future" predicate, `unique_future::is_awaited()` /
`promise::is_awaited()`); it is compatible with AZ-waker (the same `continuation_`). Issue state as of 2026-11-02: PRs
#185, #186, #187, #188, #189 are open.

**Context.** OtterStax is a federated SQL server; the embedded otterbrix runs every query and reaches remote tables
through a backend actor OtterStax implements. In one process actor-zeta runs otterbrix's dispatcher, executors
(`executor_t`, a cooperative actor), disk, WAL and index actors on `sharing_scheduler` pools, and OtterStax's
per-connection sessions (cooperative actors on a sessions pool) and backend actors. The wait chain of a statement is:
protocol thread (asio / gRPC, not an actor; polls the session's future on a timer) → session → dispatcher → executor →
backend actor; every hop waits on a `unique_future`. The engine starts and stops its own pools
(`ob:services/engine/engine.cpp:906-908`, `:962-964`) and refuses one pool for two roles
(`ob:services/engine/engine.cpp:191-198`); engine and host pools stay separate.

---

## 2. Requests

### AZ-waker — settling wakes the waiter (#186) (P1)

**Request** (as in #186). `unique_future<T>::set_waker(...)`: one object with `void wake() noexcept` that only signals
(a semaphore, a scheduler enqueue, `asio::post`) and never reads the value. Registration uses the same
"register, then re-check" CAS as `co_await`: if the future is already ready, the caller sees it at once and `wake()`
is not called. `settle()` takes the waker before publishing the flags, then calls `wake()` once on the producer's
thread. A waiter destroyed earlier gets a harmless late `wake()`.

**Why.** Whoever waits for a future outside an actor trades latency for CPU. OtterStax's protocol threads poll the
session's future on a timer: a 1 ms step (MySQL, PostgreSQL, Flight SQL), 16 µs to 1 ms for Spark
(`otx:frontend/common/asio_future_bridge.hpp`, `otx:frontend/spark_connect_server/await_future.hpp`). otterbrix's
dispatcher, disk and WAL loops poll readiness on their own intervals (otterbrix #693, stage 2; `otterbrix-embedding.md`,
OTB-notify). AZ-park needs the waker too.

**Evidence.**
- `settle()` only sets flags: "The continuation is NOT taken or resumed here" (`az:detail/future.hpp:112-126`,
  `az185:detail/future.hpp:116-130`); `set_value` — "Producer side; never resumes the continuation"
  (`az:detail/shared_state.hpp:60-66`, `az185:detail/shared_state.hpp:66-83`); `release_promise` sets flags
  (`az185:detail/shared_state.hpp:145-159`).
- Everything `send()` returns completes by flags alone (`az185:detail/future_awaiters.hpp:21-23`); only a method
  coroutine's `final_suspend` wakes anyone (`az185:detail/future.hpp:284-326`). PR #185 states the rule: "Results never
  wake anyone" (`az185-doc:15-16`).
- No blocking `get()` / `wait()` and no `co_await` outside an actor — by design, and we do not ask to bring them back:
  with a waker the host builds both a blocking wait and a bridge into its event loop.
- PR #185 removes `intrusive_ptr` and `ref_counted` (`az185-cl:17-20`), which #186 uses to hold the waker: ownership
  needs another form (§5, question 2).

**Acceptance criteria.** A waker registered before or after `set_value` / `error` / an exception fires exactly once,
and only once `is_ready()` is true; after the future is released `wake()` is not called (ASAN). An external thread
waiting through a waker on a semaphore uses no CPU and wakes within microseconds of `set_value`.

### AZ-park — an actor on a pending future parks (P1)

**Request.** A cooperative actor whose behavior waits in `co_await` on a pending future registers a waker on it
(AZ-waker) whose `wake()` re-enqueues the actor by taking its `scheduled` bit (as a new message does), and answers its
pool "waiting" instead of "re-enqueue me". A result that arrived before the registration resumes the actor at once
(the same CAS). Nested coroutines register the waker on the innermost awaited future.

**Why.** In OtterStax two actors wait like this for every statement — the session (the engine's answer) and
`executor_t` (remote sources) — and both spin their pools the whole time.

- A statement held on the backend for 1 s (hidden test `[.engine-defect-otb-notify]`,
  `otx:tests/session/test_session_lifetime.cpp`; macOS, 18 cores; sessions pool 4 threads, executors pool 2): **≈ 3.1
  CPU-seconds** per second of waiting on otterbrix `61aa43d6` (0.03 with nothing in flight; the test's limit is 0.5).
- Cross-database JOIN benchmark (Docker, 18 CPUs, medians of 5 runs, otterbrix `46fba865` — the same wait model as
  `61aa43d6` and PR #185): in a perf profile of the ClickHouse JOINs **40 % of samples** are the scheduler
  (`pthread_cond_signal` in `work_sharing::enqueue` ← `resume_job_later` 24.1 %, `pthread_cond_wait` in `dequeue`
  10.2 %, `sched_yield` in `worker::run` 5.8 %); the baseline (OtterStax before execution moved to the engine, with no
  future waits) has 0 %.

Medians, ms. "Now": actor-zeta 2.0.0. "No spin": an experiment (not merged) where a session that handled no message on
`resume` sleeps 50 µs instead of re-enqueueing at once.

| JOIN | baseline | now | no spin | now vs no spin | now vs baseline |
|---|---:|---:|---:|---:|---:|
| ClickHouse ⋈ MySQL | 890.9 | 1085.7 | 875.9 | +24 % | +22 % |
| ClickHouse ⋈ PostgreSQL | 878.7 | 1081.4 | 879.0 | +23 % | +23 % |
| MySQL ⋈ ClickHouse | 898.0 | 1088.2 | 867.2 | +25 % | +21 % |
| PostgreSQL ⋈ ClickHouse | 893.7 | 1082.1 | 870.3 | +24 % | +21 % |
| MySQL ⋈ PostgreSQL | 24.7 | 16.2 | 22.1 | −27 % | −34 % |
| PostgreSQL ⋈ MySQL | 22.8 | 17.3 | 25.5 | −32 % | −24 % |
| three databases, variant 1 | 445.6 | 523.0 | 494.0 | +6 % | +17 % |
| three databases, variant 2 | 162.9 | 194.3 | 160.7 | +21 % | +19 % |

Without the spin the long ClickHouse JOINs return to the baseline; the short ones show that polling with a pause is no
fix (16.2 → 22.1 ms): a wake-up is needed, not another interval.

**Evidence.**
- 2.0.0: on a pending future `resume_impl` answers `resume` with `keep_scheduled`
  (`az:actor/cooperative_actor.hpp:345-356`); "on `resume` the caller must re-enqueue this actor; nobody else will"
  (`:204-205`).
- PR #185: the default verdict is `resume` (`az185:actor/cooperative_actor.hpp:198`), "a suspended behavior keeps the
  turn" (`:201`), the behavior loop on a busy `behavior` yields `co_yield step{handled, true}` (`:288-307`, `:304`);
  `az185-doc:59-60`: "A behavior suspended on a `co_await` keeps its actor in the run queue".
- A worker given `resume` with nothing handled yields and re-enqueues the job (`az:scheduler/worker.hpp:72-83`,
  `az185:scheduler/worker.hpp:73-83`) → `enqueue`: a mutex, `push_back`, `notify_one`
  (`az185:scheduler/policy/work_sharing.hpp:81-87`, `:99-102`). Each `notify_one` wakes another worker, which sees the
  pending future again.
- There is no other policy: `sharing_scheduler = scheduler_t<work_sharing>` (`az:scheduler/sharing_scheduler.hpp:8`,
  the same at `c54a96c`).

**Host workaround.** None correct: the ready flag is written by `settle()` inside the library.

**Acceptance criteria.** An actor waits for a future completed after 1 s: the pool runs O(1) `resume()` of that actor
in that second, and the actor continues within microseconds of `set_value`. A future completed between the check and
the waker registration resumes the actor (TSAN stress, 100 runs). In the host: `[.engine-defect-otb-notify]` passes
(under 0.5 CPU-seconds per second of waiting), ClickHouse JOINs at the baseline, short JOINs no slower than now.

### AZ-answer-later — answer later without holding the turn (P1)

**Request.** A public way for a cooperative actor's method to answer later: the method gets the reply promise ("I will
answer"), stores it and returns the turn; the actor takes the next message; the promise is completed later, from any
thread or another method. With lifetime rules: a promise destroyed without a value answers `broken_pipe`, as now;
`close()` waits for or refuses unfulfilled promises.

**Why.** OtterStax's backend actor (the otterbrix storage contract, `otterbrix-embedding.md`, §2) receives messages from
every engine executor and must answer without blocking the sender (`ob:docs/embedding-host-api.md:127-140`). Each
message is a remote query of tens of milliseconds or more. If a cooperative actor's method `co_await`s it, the actor
holds the turn until the answer, and every query to that backend runs one at a time.

**Evidence.**
- `dispatch()` takes the reply promise, waits for the method's future, and only then completes the promise:
  `az185:actor/dispatch.hpp:100-103` (`get_result_promise`, `transfer_ownership`), `:114-115`
  (`detail::settled<value_type> outcome{...}; co_await outcome;`), `:116-124`; on 2.0.0 `az:actor/dispatch.hpp:98-116`.
  While it waits the behavior is busy and the actor holds the turn (`az185:actor/cooperative_actor.hpp:288-307`).
- There is no `response_promise` / `make_response_promise` in any revision. A low-level "router" path exists — the
  public `message::get_result_promise<T>()` and `transfer_ownership()` (`az185:mailbox/message.hpp:84`, `:87-89`,
  `:177-181`; `az185-cl:87-89`) — but it is not described or tested for a cooperative actor, and
  `promise(state_type*)` is called a "Non-owning view" (`az185:detail/future.hpp:43`).

**Host workaround.** An `actor_mixin` with its own loop thread: `enqueue_impl` pushes the message into an inbox, the
loop starts behaviors and resumes the coroutines whose reply is ready — as otterbrix's disk
(`ob:services/disk/manager_disk.hpp:293`), otterbrix's test backend (`ob:integration/cpp/test/test_host_actor.hpp:110`,
`ob:docs/embedding-host-api.md:130-132`) and OtterStax's backend actor
(`otx:otterbrix/federation/backend_actor.hpp:134`). It costs a thread per actor and a hand-written loop.

**Acceptance criteria.** A cooperative actor takes 100 messages, promises each answer and answers after 10 ms from
another thread: all 100 answers arrive after ≈ 10 ms, not 1 s; a promise destroyed without a value answers
`broken_pipe`; `close()` with an unfulfilled promise follows the documented rule (ASAN / TSAN clean).

### AZ-await-outcome — await the outcome (P1)

**Request.** A public way to await a future inside an actor without taking the value and without aborting on an error
code: make `settled<U>` from PR #185 public, or `co_await std::move(f).outcome()` → value | code | exception.

**Why.** Host actors wait for the engine's answers and for each other. A message destroyed unhandled (mailbox closed,
actor dying) completes its promise with `operation_canceled`; a promise without a value with `broken_pipe`. An actor
awaiting such an answer with a plain `co_await` takes down the whole server with every connection. It should become
the statement's error.

**Evidence.** `co_await` on a future holding an error code without an exception →
`refuse_valueless_extraction("co_await")` (`az:detail/future_awaiters.hpp:87-90`, `az185:detail/future_awaiters.hpp:104-107`)
→ `std::abort()` (`az:detail/result_storage.hpp:18-26`, the same at `c54a96c`). `settled<U>` in PR #185 is in
`namespace detail` (`az185:detail/future_awaiters.hpp:49-57`, `await_transform` `:118-141`) and only `dispatch()` uses
it (`az185:actor/dispatch.hpp:114-115`).

**Host workaround.** A strict shutdown order: connections and their sessions close before the engine (`otx:main.cpp`);
any send to the engine after its shutdown began is `abort()`.

**Acceptance criteria.** Actor A awaits the outcome of actor B's answer; B is closed before handling the message; A
gets `operation_canceled` and the process lives. Values and exceptions arrive intact. A plain `co_await` is unchanged.

### AZ-pool-lifecycle — `stop()` in any state, restart (P1)

**Request.**
1. `stop()` of a never-started pool and a second `stop()` are no-ops.
2. `start()` after `stop()` brings up exactly N threads without a leak, or refuses explicitly (an assert in debug, an
   error in release).
3. Document that `stop()` drops queued jobs without `resume()`: accepted messages never run, their futures wait until
   the actor is destroyed.
4. Optionally: `bool running() const noexcept`.

**Why.** OtterStax runs its own sessions and backends pools and wants simple rules: `stop()` may be called on an error
path where the pool never started, and a second `stop()` does nothing. Today a wrong call order is UB or a hang at
process exit. The same limit makes otterbrix refuse one pool for two roles (`ob:services/engine/engine.cpp:191-198`:
"it would be started and stopped twice").

**Evidence.**
- 2.0.0: `stop()` of a never-started pool indexes an empty vector — `worker_by_id(i)` → `workers_[x].get()`
  (`az:scheduler/scheduler.hpp:40`, `:85-87`, the vector `:117`); a second `stop()` queues a shutdown job in the
  central queue and waits forever (`:90-93`); `start()` after `stop()` adds N workers to the old ones and starts all 2N
  (`:52-58`), the next `stop()` sends N jobs and `join` (`:97-99`) hangs.
- PR #185 (`c54a96c`): `workers_` is a raw array, `nullptr` before `start()` (`az185:scheduler/scheduler.hpp:120`);
  `stop()` of a never-started pool dereferences null (`:95-96`, `workers_[i]->external_enqueue(&sh)`); a second
  `stop()` hangs in `wait_for_completion` with no live threads (`:96-97`); `start()` after `stop()` overwrites
  `workers_` with a new array (`:62-63`) and the old N workers leak (`~scheduler_t` frees only the current ones,
  `:35-42`). `stop()` drops the queue without `resume()` (`:104-108`) and without a word on the semantics; the rule
  "`start()` once, `stop()` exactly once" is only written (`az185-cl:327-328`), not checked.
- No revision can tell whether a pool runs.

**Host workaround.** A "started" flag per pool; `stop()` only when it is set (`otx:runtime/runtime.cpp`).

**Acceptance criteria.** `stop()` of a created pool and `stop(); stop();` return at once (ASAN / UBSAN clean);
`start(); stop(); start();` gives exactly N threads, no leak; five cycles in a row grow neither threads nor memory.

### AZ-thread-start — the OS refuses a thread (#188) (P1)

**Request** (as in #188). A pool's `start()` returns an error when the OS does not create a thread: the threads already
started are stopped and joined; a test pins the path.

**Why.** OtterStax and otterbrix build without exceptions on their error paths. Because of this, otterbrix's C ABI
terminates the process when a scheduler thread cannot be created (`ob:docs/embedding-host-api.md:601-603`), and the
host cannot answer "out of resources" at startup.

**Evidence.** `worker::start()` → `launch_thread` → `std::thread{...}` (`az:scheduler/worker.hpp:28-31`,
`az185:scheduler/worker.hpp:28-31`, `az185:detail/launch_thread.hpp:14-21`): the constructor throws
`std::system_error`, nothing catches it. In PR #185 threads start one by one (`az185:scheduler/scheduler.hpp:68-70`):
after a throw the started ones stay joinable and `~scheduler_t` calls `std::terminate` (comment `:33-34`).

**Acceptance criteria.** A test with a thread launcher that refuses at the k-th worker: `start()` returns an error, no
thread is left, the pool's destructor does not call `terminate`.

### AZ-exception — the consumer sees the exception (P2)

**Request.** Non-throwing access to a `unique_future`'s exception: `has_exception()` and `exception()` (false / empty
in a build without exceptions); an own error code (actor-zeta category, `unhandled_exception`) instead of
`std::errc::interrupted`.

**Why.** P2 because otterbrix on `61aa43d6` does not throw (CI "no exceptions" check,
`ob:scripts/CI/run-check-no-exceptions.sh`), so an exception rarely reaches OtterStax's protocol threads. If one does,
a consumer following the documented polling protocol sees only "Interrupted system call", which cannot be told from a
producer's `EINTR`.

**Evidence.** `set_exception` stores the `exception_ptr` and sets `errc::interrupted` (`az:detail/shared_state.hpp:79-84`,
`az185:detail/shared_state.hpp:86-90`); there is no exception getter — the public future API is `take_ready`,
`is_ready`, `failed`, `error`, `detach` (`az185:detail/future.hpp:182-239`); the exception is reachable only by a rethrow
from `take_ready()` (`az185-doc:123`).

**Host workaround.** On `errc::interrupted` the protocol threads use their own text "an exception escaped the actor
running it" (`otx:frontend/common/asio_future_bridge.hpp`).

**Acceptance criteria.** A method throws `runtime_error("boom")`: the consumer sees `failed()`, `has_exception()`, and
`rethrow_exception(f.exception())` gives `boom` without `take_ready()`; `promise::error(errc::interrupted)` and an
exception are distinguishable; the no-exceptions build compiles.

### AZ-implements-extra — methods beyond the contract (P2)

**Request.** `implements<Contract, &A::m1, …>` checks that the actor's first methods are the contract in order and by
signature, and allows actor methods after them (their message ids follow the contract's); or a separate
`extends<Contract, …>`.

**Why.** OtterStax's backend actor answers the 18 messages of otterbrix's `storage_contract` plus a 19th of its own
(`discover`). `implements<>` requires exactly as many methods as the contract, so the contract check and
`dispatch_traits` have to be written as two lists.

**Evidence.** `methods_count_matches` compares the list lengths (`az:actor/implements.hpp:36-38`, the same at
`c54a96c`); `implements` requires `valid_implementation` (`:60-67`, `:77-79`).

**Host workaround.** `contract_check = implements<storage_contract, …18…>` plus a separate `dispatch_traits` with the
same 18 and `discover` last (`otx:otterbrix/federation/backend_actor.hpp:270-308`).

**Acceptance criteria.** An actor with a contract of N methods and M of its own compiles with `implements<>`, and
`msg_id` of the first N equals the contract's; an extra method in the middle or a different signature is a compile
error.

---

## 3. Done

- **AZ-release** — `actor-zeta/2.0.0#887bc992cac0ead59e5bdf5c90c844f4` on `conan.otterbrix.com`; OtterStax pins this
  revision (`otx:conanfile.py`). Please do not re-release `2.0.0` with another recipe revision.
- **AZ-teardown (#187)** — PR #185 `close()` (`az185:actor/cooperative_actor.hpp:231-259`, test
  `az185-test:close/main.cpp:194-251`): "Once one is ready, no job for this actor exists or can appear: delete is safe
  while the scheduler runs". Enough for the host. Not requested: `delete` without `close()` with a queued job is still
  a use-after-free (`az185-doc:92`), and the `close()` future may never become ready if a behavior awaits a producer
  nobody runs (`az185-doc:77-78`).

## 4. Checked and not requested

- Blocking `get()` / `wait()` — by design; with a waker the host builds a blocking wait itself.
- `co_await` on `unique_future` outside an actor — by design; the bridge to the host's event loop is `asio::post` from
  `wake()`.
- An exception from `behavior()` is printed and dropped — documented; the host puts work into methods.
- The `shared_state` layout depends on `-fexceptions` — documented; the host builds actor-zeta in the same mode.
- Shared engine and host pools — otterbrix starts and stops its own pools (`ob:services/engine/engine.cpp:906-908`,
  `:962-964`); OtterStax keeps separate ones.

## 5. Open questions

1. **AZ-waker / AZ-park.** Register a waker on every `co_await`, or only when the future is not ready at the first
   check?
2. **AZ-waker.** Without `intrusive_ptr` (PR #185), who owns the waker — host reference counting, or `shared_state`
   holding it by value?
3. **AZ-answer-later.** A reply promise as a public type next to `dispatch()`, or a documented
   `get_result_promise` / `transfer_ownership` path for cooperative actors?
4. **AZ-pool-lifecycle.** Support restarting a pool, or explicitly forbid `start()` after `stop()`?
5. **AZ-await-outcome / AZ-exception.** One outcome type for `co_await` and external polling, or two APIs?
