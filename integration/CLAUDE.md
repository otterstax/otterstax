# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`integration/` (library `integration`) holds the actors between the query sessions and the non-SQL
sources:

| Directory | Class | What |
|-----------|-------|------|
| `s3/` | `db::S3Manager` | S3 object loads / exports over the s3 connector and the file manager (below) |
| `kafka/` | `otterstax::kafka::KafkaManager` | librdkafka consumers / producers + Kafka SOURCE / STREAM objects (own library `kafka_runtime`; see `kafka/CLAUDE.md`) |

Remote SQL backends are not here: a remote table's statements are the federation's backend actor and
its agents (`otterbrix/federation/CLAUDE.md`) over the connectors (`connectors/CLAUDE.md`).
`main.cpp` owns the S3 / file actors and Kafka (spawned after the engine, destroyed before it). The
engine has no actor of OtterStax's own: every engine request is a free function of
`otterbrix/operators/engine_requests.hpp` sent straight to the engine dispatcher
(`runtime::engine_t::dispatcher_address()`).

## S3Manager (`s3/`)

`db::S3Manager` orchestrates object I/O by bridging the raw S3 connector (`conn::s3::ConnectorManager`)
and the file connector (`conn::file::FileManager`). It moves bytes only; the query session does the
engine work on the connection's engine session.

- `download` — opens the object (`ConnectorManager::open_input`) and hands its stream to
  `FileManager::load_file`, which reads it into a run of chunks; nothing is staged on local disk. The
  query session loads the run (`CREATE EXTERNAL TABLE`).
- `upload` — writes `rows` (the cursor of a COPY's inner query, which the query session ran) to S3: it
  opens the object (`open_output`) and hands rows and stream to `FileManager::dump_file`, which writes
  the chunks into the stream and, holding it last, publishes it (`close()`) only when the whole result
  was written; every other exit aborts it (`tsl::output_stream_t`), so a COPY whose write fails
  publishes no object.
- `ls` — lists objects for an alias.

Both take the statement's `WITH (format = ...)` (empty → the object key's extension decides), so an
object key without an extension can be loaded and written.

Every handler forwards the callee's `core::error_t` code unchanged (`undefined_object` for an unknown
alias, `invalid_parameter`, `io_error`) and only prefixes the message, so the session reports the real
cause. Its own checks (null rows, no format option and an unrecognised object-key extension) are
`invalid_parameter`; a failed publish is `io_error`. Covered by `tests/system/test_s3_errors.cpp` and,
against RustFS, `tests/test_mysql_s3_stream.py`.

Its handlers `co_await` the cross-actor sends rather than `.take_ready()`, so they stay correct if those
callees move off the synchronous `enqueue_impl` path. End-to-end coverage lives in the python
integration suite (`tests/test_{schema_}mysql_s3.py`, `tests/test_mysql_join_sql_s3_to_s3.py`,
`tests/test_mysql_join_otb_local_s3.py`, `tests/test_mysql_join_otb_local_s3_file.py` — s3 parquet ⋈
file csv ⋈ otterbrix-internal), driven by `docker-run-tests.sh`.
