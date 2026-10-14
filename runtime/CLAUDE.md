# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`runtime/` (CMake target `runtime`) holds the first and the third module of the process
assembly `runtime_t → remote_backends_t → engine_t` (then the S3 / file / Kafka actors, the
session factory and the servers, all `main.cpp`'s locals). `main.cpp` builds them
in that order and destroys them in reverse; `service.engine` / `service.runtime`
of `config.yaml` (`config/CLAUDE.md`) size them.

```
runtime/
├── runtime.{hpp,cpp}  — runtime_t: the memory resource + every thread pool, by role
└── engine.{hpp,cpp}   — engine_t: services::engine::open_engine over the runtime's pools;
                         make_engine_configuration (service.engine → configuration::config);
                         the engine's log_t over otterstax's spdlog sinks (engine.cpp, file-local)
```

## `runtime_t`

```cpp
explicit runtime_t(const config::RuntimeConfig& config = {});   // service.runtime
std::pmr::memory_resource* resource();                          // core::pmr::otterbrix_resource
pool_t* sessions_scheduler();
pool_t* backends_scheduler();
void stop_sessions();                                           // idempotent; ~session_factory_t's shutdown
// the destructor: stop_sessions, then the backends pool (stopped there, once)
```

`pool_t` is `actor_zeta::scheduler::sharing_scheduler` (otterbrix's `actor_zeta::shared_work`).

- **general / exec / disk** belong to the engine (`services::engine::schedulers_t`).
  `runtime_t` builds them and never starts or stops them: `open_engine` starts them,
  and the engine's shutdown stops them. An actor-zeta pool cannot be restarted.
  `scheduler_t::start` appends a second set of workers, and `stop()` on a pool that
  was never started indexes an empty vector (asked of actor-zeta as AZ-pool-lifecycle;
  the engine stopping its pools at all waits for AZ-teardown, actor-zeta#187 —
  `docs/proposals/actor-zeta.md`). So the three pools serve **one** engine.
  `engine_t::open` lends them (`fresh → lent`) and the engine's destructor spends them
  (`lent → spent`); a refused open spends them too, unstarted (`lent → spent`). A
  second engine — after a reopen or after a refused open — needs a second `runtime_t`:
  a second lend is `invalid_parameter`. Destroying a `runtime_t` while its engine runs
  is a programming error and aborts. The state is a private three-value enum, not a
  flag: the lend refusal needs "ever lent" (lent or spent), the abort needs "lent and
  not spent".
- **sessions** runs one `query_session_t` actor per client connection
  (`session/CLAUDE.md`). **backends** runs the federation's statements of the
  three backends (`remote_backends_options_t::statements` =
  `backends_scheduler()`: the connection checkout and the wait for a backend's
  answer, off the engine's executors; `integration/statement_executor.hpp`); their
  `discover` / `execute_ddl` run on the calling thread and the
  connectors do their I/O on the asio io pools their connector managers own — not
  a `runtime_t` pool: `remote_backends_t` sizes them from
  `service.runtime.connector_io` (`config/CLAUDE.md`). Both are started in the
  constructor, backends first; `stop_sessions` (public) and the destructor (the
  backends pool, stopped once there) join them, and the two are stopped at opposite
  ends of the shutdown:
  - **sessions** before its actors are freed: `~session_factory_t` stops it once
    the closed sessions drained and before it reaps what is left (no pool thread
    may resume a freed session);
  - **backends** after the backends are gone: a backend's statement executor
    waits, when destroyed, for every job it posted, and a scheduler drops queued
    jobs at `stop()`, so the pool runs until `remote_backends_t` is destroyed —
    after the engine — and stops in `~runtime_t`.

  The destructor stops whatever is still running.
- Sizes come from `service.runtime`. `threads: 0` means `max(2, hardware threads)`
  (`resolve_threads`) for sessions. For backends `main.cpp` resolves 0 before it
  builds the runtime (`backends::backends_threads`): at least that and at least
  the sum of `pool_size` over every remote connection of `connections:`. **A
  statement holds a backends thread for its whole remote call** (the connection
  checkout, then the wait for the backend's answer), so the pool is what isolates
  the connections from each other: with fewer threads than connections the
  surplus connections idle, and one slow alias whose statements hold every thread
  stalls the others — even those with free connections. An explicit
  `backends.threads` is taken as is; below the sum of `pool_size`
  `backends_threads` logs a warning saying so (logger `BACKENDS`). The engine pools default to 3 threads; every pool takes
  1000 messages per actor before it yields its thread (`max_throughput`, a constant of
  `runtime.cpp`).
- Not thread-safe: the assembling thread (`main`, a test) builds, lends and stops it.

## `engine_t`

```cpp
static core::result_wrapper_t<std::unique_ptr<engine_t>>
    open(runtime_t&, const config::EngineConfig&, components::planner::primitives_t = {});
~engine_t();                                   // the shutdown
actor_zeta::address_t dispatcher_address() const;  // manager_dispatcher_t: execute_plan, refuse_statement, txn_*
std::pmr::memory_resource* resource() const;   // == runtime.resource()
components::cursor::cursor_t_ptr execute_plan_blocking(session_id_t, execution_plan_t);  // startup + tests only
configuration::config make_engine_configuration(const config::EngineConfig&);
```

- **Open** = `make_engine_configuration(engine)`, `make_engine_log` over its log
  path and level (a log that cannot be made is the error, before the pools are
  lent), then `open_engine(runtime.resource(), {general, exec, disk}, config, log, primitives)`.
  It validates `config.execution`, takes `<main_path>/.lock`, replays the WAL,
  bootstraps the catalog and starts the three pools. A refusal is returned as the error,
  with its message re-homed onto `new_delete_resource`:
  - a held lock → `already_exists`, "another engine already owns …";
  - an unreadable WAL → `io_error`;
  - the runtime's engine pools already lent or spent → `invalid_parameter`;
  - no log directory (`make_engine_log`) → `invalid_parameter`, a log file that
    cannot be created → `io_error`; the pools are not lent. `make_engine_log`
    answers a valid log or the error: the engine would dereference an invalid one;
  - an exception thrown by any step under `open_engine` (the directory lock, the
    WAL read, allocation, the log) → `other_error` "engine startup REFUSED,
    open_engine threw: …". It is caught in `engine_t::open`, so the pools are
    spent unstarted (`lent → spent`) like for any refusal — an exception
    escaping would leave them lent and `~runtime_t` would abort.

  `main` logs the error and exits, as it does for a bad config.
- **Shutdown** = the destructor, i.e. `services::engine::engine_t::~engine_t`. The
  dispatcher's `begin_shutdown` runs first, then the final CHECKPOINT (a failure is
  logged, the next start replays the journal), the manager loops stop, and general /
  exec / disk stop. The runtime then marks its engine pools spent.
- **`primitives`** are the host's optimizer rules and name-resolution `need`/`decide`.
  `open_engine` reads them once, and every executor copies them. Empty means otterbrix's
  defaults: `no_name_reads` / `no_storages`, no rules. The server opens the engine with
  the federation's (`otterstax::federation::make_primitives()`: `need`/`decide` and the
  pushdown rule), after `remote_backends_t::open` installed the registry the hooks read;
  the test stacks and `tests/federation/` open it the same way.
- **`make_engine_configuration`**: `main_path` (the locked directory), the WAL and
  the tables are `data_dir`; the log is `config::log_directory` (`log_dir`, else
  `data_dir`). This is the single-directory layout the server always used, with one
  fix: `main_path` is no longer the process's working directory. The log level is
  `SPDLOG_ACTIVE_LEVEL`, the compile-time level of otterstax's own loggers
  (`utility/logger.hpp`; default `ERROR`), not otterbrix's `trace`. Everything else —
  the execution knobs — is otterbrix's own default.
- **`execute_plan_blocking`** does `send` + `actor_zeta::otterbrix::wait_ready`, a
  100 µs sleep loop. Use it only on a non-actor thread before frontends accept clients
  (startup bootstrap, reconciliation) and in tests. Everything else awaits
  `dispatcher_address()` from an actor coroutine.

## Engine log (`runtime/engine.cpp`, anonymous namespace)

- `make_engine_log(directory, level)` builds an `spdlog::logger` named `otterbrix`.
  It writes over the **sinks of otterstax's `Main` logger**: stdout and
  `Main-<seconds>.txt`. In the server `Main` already exists, created by `main.cpp`
  under `service.engine.log_dir` (`config::log_directory`) right after the config was
  read — the same directory `make_engine_configuration` gives the engine — so the
  engine's lines land in that file. If nobody initialized `Main` (a test), the
  function initializes it under `directory` first.
- The logger is never registered with spdlog. Each engine owns its own logger, and
  `spdlog::set_level` / `initialize_all_loggers` never touch it. This matches the
  migration doc, decision 9.
- Failures come back as `io_error` / `invalid_parameter` on `new_delete_resource`.
  No exception leaves it. It is local to `engine.cpp`: `engine_t::open` is its
  one caller, and the tests reach it through it.
- Before this, the engine wrote through `make_log("otterbrix", log.path)`, which had
  its own stdout sink and its own `otterbrix-<seconds>.txt`.

## Startup / shutdown order (`main.cpp`)

```text
command line, config::load_config → ServiceConfig{ engine, runtime, … }   (failures → stderr)
initialize_all_loggers(config::log_directory(server_config.engine))   — log_dir, else data_dir
runtime_t runtime(server_config.runtime, backends.threads resolved by backends::backends_threads)
remote_backends_t(runtime.resource(), {.statements = runtime.backends_scheduler()}).open(connections, retry)
                                     → connections discovered, registry installed
auto engine = engine_t::open(runtime, server_config.engine, federation::make_primitives())   → error: log, return 1
s3 connector, FileManager, S3Manager, KafkaManager spawned (main's locals, after the engine)
backends::register_s3_aliases, backends::publish_federation_metadata, kafka->recover()
session_factory_t(runtime, &make_parser, make_session_services(…))   → query sessions on runtime.sessions_scheduler()
the four servers over the factory; start, flight->run()
── shutdown (the reverse of the declarations) ──
servers destroyed (each stops in its destructor; their sessions closed)
~session_factory_t: sessions drain (30 s) → runtime.stop_sessions() → reap
Kafka, S3, file, s3 connector destroyed (Kafka first)
engine destroyed (final CHECKPOINT, engine pools stopped)
remote backends destroyed (registry reset, the backends' statement jobs on the backends pool drained, connector managers)
runtime destroyed (backends pool stopped, memory)
```

## Tests

`tests/runtime/` (`test_runtime`, discovered with `otterstax_discover_tests`):

- runtime start (`stop_sessions` idempotent, the destructor stopping the rest) with no engine, the pool sizes, the resolved auto size;
- engine reopen on the same directory, data present;
- a second engine on a locked directory refused as `already_exists`, not abort; the
  refusal spends that runtime's pools (a second open on it refused) and the directory
  opens afterwards on a new runtime;
- one runtime hosts one engine, then its pools are spent;
- a log that cannot be made (no directory) refused before the pools are lent: the
  same runtime then opens an engine;
- shutdown with nothing in flight;
- the sessions pool stopping independently of the engine (it runs on with it stopped);
- `make_engine_configuration` defaults (the log level `SPDLOG_ACTIVE_LEVEL`, otterbrix's own past the directories and the level) and `data_dir` / `log_dir`;
- the engine log sharing Main's sinks and staying unregistered, two engines open in
  one process (through `engine_t::open`; at the default `ERROR` level the line it reads is
  a refused open's error).

`service.engine` / `service.runtime` parsing is tested in
`tests/unit/config/test_engine_config.cpp`.

## Tracy

`runtime_t` ctor/dtor, `stop_sessions`, `engine_t::open`,
`~engine_t`, `execute_plan_blocking`, `make_engine_configuration`, `make_engine_log`
carry `OTX_ZONE_N`. None of them is a coroutine.
