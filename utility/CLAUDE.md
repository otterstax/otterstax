# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`utility/` is a header-only collection of shared primitives used across all layers. There are no `.cpp` files; headers are compiled directly into `lib_otterstax` via the root `CMakeLists.txt`.

## Key Types

### `cv_wrapper.hpp`

`cv_wrapper_t<T>` — a mutex + condition variable wrapper for passing a single result between a producer (actor pipeline) and a consumer (frontend thread). Status values: `Ok`, `Empty`, `Timeout`, `Error`.

```cpp
auto sdata = create_cv_wrapper(session_payload{resource});
// producer:
sdata->set_result(payload);          // or release_on_error("msg") / release_empty()
// consumer:
sdata->wait_for(cv_wrapper::DEFAULT_TIMEOUT);   // 90 s default
if (sdata->status() == cv_wrapper::Status::Ok) { auto p = sdata->get_result(); }
```

`shared_data<T>` = `shared_ptr<cv_wrapper_t<T>>` — the type passed through the actor chain.

### `session_payload.hpp` / `shared_flight_data.hpp`

`session_payload` holds the output of a completed query: `schema` (`complex_logical_type`), `chunk` (`data_chunk_t`), `parameter_count`, and `NodeTag`. `shared_session_payload` = `shared_data<session_payload>`.

`shared_flight_data` is the older alias for the same pattern (now `shared_session_payload`); both exist during migration.

### `result.hpp`

`otterstax::result<T>` = `result_t<pipeline_error, T>` — a move-only Either type. Construct with a value for success or a `pipeline_error` for failure. Use `result.has_error()` / `result.value()` / `result.take_value()`. Use `convert_error<U>()` to propagate an error into a different result type without touching the value.

### `pipeline_error.hpp`

`pipeline_error` — the error type used throughout the actor pipeline. Implements `basic_error<ErrorCode, ErrorTag>`.

### `session.hpp`

`session_hash_t` — the type alias for query session IDs threaded through all actors.

### `thread_pool_manager.hpp`

`thread_pool_manager` — wraps a `boost::asio::io_context` + `std::vector<std::thread>`. Used by each `ConnectorManager` to host async DB operations.

### `logger.hpp`

`log_t` and `initialize_all_loggers()` — spdlog wrappers. Log level is compile-time via `SPDLOG_ACTIVE_LEVEL` (default `ERROR`; override with `-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG`).

### `tsan_helper.hpp`

TSAN annotation helpers (`ANNOTATE_HAPPENS_BEFORE` / `ANNOTATE_HAPPENS_AFTER`). Used to suppress false positives when actor message delivery provides implicit synchronization that TSAN cannot observe.
