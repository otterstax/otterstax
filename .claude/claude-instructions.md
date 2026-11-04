# Instructions for OtterStax

The project map — what OtterStax is, `config.yaml`, the startup in `main.cpp`, the architecture, the
names, the build and Docker commands, the Known Constraints and the dependency versions — is the
root `CLAUDE.md`; each module's details are in its own `CLAUDE.md`. This file holds only the working
rules that are not written there.

## Before building

Size the jobs to RAM first (root `CLAUDE.md`, "Build resources"): `JOBS = min(nCPU,
floor(RAM_MB / 1536))`, and for Docker the VM's RAM (`docker info --format '{{.NCPU}} CPUs /
{{.MemTotal}} B'`), not the host's. On this machine `-j nproc` OOM-kills the Docker build partway
through (`ResourceExhausted: cannot allocate memory`), so the test steps never run and the failure
looks unrelated to the change; `-j 4` builds cleanly:
`docker build --build-arg BUILD_JOBS=4 -f Dockerfile.test -t otterstax-test .`,
`./docker-run-tests.sh -j 4`. If `conanfile.py` changed, remove the build directory before
`conan install`.

## Code conventions

- **Actors.** Talk to an actor only through `actor_zeta::send(address, &Actor::method, args...)` and
  `co_await` the future inside an actor coroutine; a frontend awaits through
  `frontend/common/asio_future_bridge.hpp` (Spark: `spark_connect_server/await_future.hpp`). A new
  handler: the coroutine method (returns `unique_future<T>`), its pointer in `dispatch_traits<...>`,
  its branch in `behavior()`, a Tracy zone (root `CLAUDE.md`).
- **Memory.** Allocate on the `std::pmr::memory_resource*` you are given (the runtime's); actors are
  `actor_zeta::spawn`ed and held by `std::unique_ptr<T, actor_zeta::pmr::deleter_t>`.
- **Ownership.** One owner per object; no new `std::shared_ptr` (Arrow's own handles stay at the
  Arrow call site, carried between actors by `tsl::arrow_stream_t`). A `query_session_t` is closed by
  dropping its `session_ptr`, never destroyed directly (`session/CLAUDE.md`, the close protocol).
- **Errors.** Nothing throws across a module boundary: `core::error_t` / `core::result_wrapper_t<T>`,
  `[[nodiscard]]`; a third-party throw is caught where it is made.
- **otterbrix first.** What otterbrix lacks is a request to otterbrix, not a workaround in
  OtterStax; a claim about otterbrix is checked in its sources
  (`~/CLionProjects/otterbrix`).
- **Namespaces.** `otterstax::{runtime, backends, federation, catalog, session, engine, kafka}`;
  `frontend::{mysql, postgres, spark}`, `flight::*`; the connectors `mysql::` / `pg::` / `ch::`,
  `conn::s3` / `conn::file`; `db::S3Manager`; `sql_gen`, `tsl` (translators); `config::`.

## Anti-patterns

1. `actor_zeta::spawn` outside the owner of the actor (`remote_backends_t` for the backend actors
   and agents, `main.cpp` / the test stack for the S3 / file / Kafka actors, `session_factory_t`
   for the sessions).
2. `std::make_unique` for an actor (use `actor_zeta::spawn` with the pmr deleter).
3. A blocking `get()` / `wait()` on an actor-zeta future.
4. Three parts for a remote table: `c.d.s.t` is a connection's table, `d.s.t` always a local one.
5. Weakening or hiding a test to make a run green (`tests/CLAUDE.md`, "Engine-defect tags").
