# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`integration/kafka/` is the Kafka **runtime**: the `KafkaManager` actor plus its
implementation. It owns the live Kafka objects (SOURCE / STREAM), their backing
Otterbrix tables, and the librdkafka consumer/producer threads. The Kafka DDL
**grammar** is separate — it lives in `otterbrix/parser/grammar_extension/kafka/`
(library `kafka_grammar`, namespace `otterstax::kafka`).

Library: `kafka_runtime` (links `kafka_grammar` + `otterbrix_local`).

## Namespace convention

- `otterstax::kafka` — the public actor (`KafkaManager`) and the grammar
  (`kafka_node_t`, `kafka_op`, `kafka_write_target`, `kafka_stream_source`,
  `kafka_find_aggregate`, `kafka_column_t`).
- `otterstax::kafka::detail` — the implementation in `detail/` (the RAII
  consumer/producer, the poller/stream threads, and the reader helpers). The
  manager sources (`detail/kafka_manager*.cpp`) do `using namespace detail;`; `kafka_manager.hpp` qualifies
  members as `detail::kafka_*_t`. External code (the query session, tests) reaches the
  helpers as `otterstax::kafka::detail::json_to_chunk` etc.

## KafkaManager (the actor)

`kafka_manager.{hpp,cpp}` — `actor_zeta::actor::actor_mixin<KafkaManager>`.

- **Names.** The Kafka objects live in the schema `kafka` of otterbrix's default
  database: `main.kafka.<name>` (`kafka_table_name`), so `kafka.<name>` from the
  default place `main.public` names them. The bookkeeping lives in the
  schema `kafka` of OtterStax's service database `federation`:
  `federation.kafka.__sources` (`kafka_service_table_name`) and
  `federation.kafka.<source>__offsets` (`kafka_offsets_table_name`) — hidden from
  the listings with the rest of `federation`, never a name a client writes. The
  schemas and `__sources` are made by `storage_plans()` (CREATE DATABASE IF NOT
  EXISTS federation, CREATE SCHEMA IF NOT EXISTS federation.kafka / main.kafka as
  `node_create_schema` plans — the parser takes no `CREATE SCHEMA d.s` —, CREATE
  TABLE IF NOT EXISTS `__sources`): at startup by `recover()`, and once per
  process by `ensure_storage()` before a CREATE persists anything.
- `execute(session_hash_t, kafka_node_ptr)` — runs a parsed Kafka DDL node
  (CREATE/DROP SOURCE/STREAM). SOURCE → `ensure_storage()` + create the backing
  table `main.kafka.<name>` (+ `federation.kafka.<name>__offsets`) + register + (if
  `start_pollers_`) launch a poller. STREAM → its output columns are the engine's
  describe (`explain_type::describe`) of the plan the worker runs over each batch
  (`stream_batch_plan` over no rows of the source's columns, `describe_stream`):
  the engine's own types (SUM over DOUBLE is DOUBLE, COUNT UBIGINT), no table +
  register + launch the continuous worker.
- `object_names()` — the registered objects' names (`kafka_object_names_t`, a set
  under its own mutex, kept in step with `registry_` by `register_object` /
  `forget_object`): what the query session reads (no message) before it completes
  an INSERT target with its place. The KafkaManager outlives every session, which
  holds a pointer to it (`session_services_t::kafka_objects`).
- `produce(session_hash_t, relname, source)` — routed here for `INSERT INTO
  kafka.<obj> VALUES`: evaluate the insert's source subplan, **validate the chunk
  against the object's declared columns** (`detail::chunk_matches_columns`), then
  publish to the topic (one-shot). Durability is the Kafka log itself — there is
  **no engine staging table** for writes.
- `add_stream_insert(session_hash_t, stream, insert_sql)` — routed here for
  `INSERT INTO kafka.<stream> SELECT … FROM kafka.<src>`: a continuous
  ksqlDB **INSERT INTO query** (persistent fan-in — several sources merged into one
  stream's output topic). Registers an anonymous stream-like object named
  `<stream>__insert__<n>` (its own `registry_`/`streams_`/`__sources` entry,
  consumer group + producer txn id) and `launch_stream`s it, so it is driven
  exactly like a `CREATE STREAM`. Target must be a STREAM (a SELECT into a SOURCE
  is rejected); exactly-once is inherited from the stream's `TRANSACTIONAL`.
- Owns `registry_` (name → `kafka_object_t{op, columns, options, as_select,
  source}` — `source` is the object a stream's / INSERT query's SELECT reads) plus
  `pollers_` / `producers_` / `streams_` maps, and `stream_insert_queries_`
  (stream → its INSERT-query names, for DROP-time cleanup). `start_pollers_` ctor
  flag is FALSE by default so broker-free unit tests stay thread-free;
  `main.cpp` spawns it with TRUE (the tests' stack with `stack_options_t::kafka_pollers`, FALSE by default). `recover()` (called
  by `main.cpp` once the s3 aliases and the remote tables are registered, before any client) makes the storage exist, then (with `start_pollers_`) replays `federation.kafka.__sources` in three passes — sources, streams, then INSERT
  INTO queries (`kind='insert'`, sink + source re-derived from the persisted SQL).
  Streams are recovered in rounds until none becomes resolvable, so a stream over
  another stream comes back whatever the row order. A stream or INSERT query that
  is **dangling** — its source (or, for an INSERT query, its target stream) has no
  `__sources` row, or is a source whose backing table is gone, or is itself a
  dangling stream — has its `__sources` row deleted (logged at warn) instead of
  being skipped on every restart and silently re-attached to an unrelated object
  re-created under the name. One whose source merely failed to recover (a probe
  error, a VIEW under the name) is only skipped; so is a source row
  whose backing table is gone.
  A source's columns are not persisted: they are the engine's describe of
  `SELECT * FROM main.kafka.<src>` (typed from the catalog, so an empty table
  recovers with the same schema as a populated one); a missing backing table is
  skipped, a VIEW under the source's name (its `pg_class.relkind`, asked through
  `pg_database` / `pg_namespace` / `pg_class`) is refused. A stream's and an INSERT
  INTO query's columns are the describe of their batch plan, as at CREATE.

## DROP semantics (`DROP SOURCE|STREAM [IF EXISTS] <name> [CASCADE | RESTRICT]`)

RESTRICT is the default, as in PostgreSQL/Materialize. The grammar carries the
choice as `kafka_node_t::cascade()`; `CASCADE`/`RESTRICT` are unreserved (still
usable as object/column names, keeping their source text like any identifier).
Dependents come from two places:

- **Kafka side** — streams and INSERT INTO queries read a source's (or a stream's)
  *topic*, so the engine does not know them. `blocking_readers(name)` lists the
  registry entries whose `source` is the object or one of its own INSERT queries;
  `cascade_closure(name)` follows readers transitively (each dropped stream takes
  its own INSERT queries).
- **Engine side** — VIEWs (and other normal `pg_depend` dependents) over
  `main.kafka.<source>`; the backing table is dropped with `drop_behavior_t::restrict_`
  or `cascade_`.

`DROP SOURCE s` (RESTRICT):
0. a name registered as a stream or INSERT INTO query → `invalid_parameter`
   `kafka: 'st' is a stream, not a source; use DROP STREAM` (with or without IF
   EXISTS — a stream has no table, so the absent-table path would forget it);
1. any Kafka-side reader → `dependent_objects_still_exist` `kafka: cannot drop source 's': insert
   query 'm__insert__0', stream 'st' depend on it; use DROP SOURCE s CASCADE`,
   before anything is stopped (`IF EXISTS` does not forgive it);
2. the poller is stopped and `main.kafka.s` dropped RESTRICT. A refusal relaunches the
   poller and keeps registry, `__sources` row and the offsets table untouched; the
   engine's DETAIL oid is resolved through `pg_catalog.pg_class` +
   `pg_namespace` (`describe_restrict_refusal`, the oid spelled as a number into
   `kafka_query`) → `kafka: cannot drop source 's': view kafka.v depends on it
   (and possibly other objects); use DROP SOURCE s CASCADE` — the engine reports
   only its first dependent. If the oid cannot be resolved, or the engine's wording
   is not recognised, or the drop failed for another reason, the answer is the
   engine's own message after the same `kafka: cannot drop source 's': ` prefix,
   with the engine's code (`dependent_objects_still_exist` for the refusal). A missing table without
   IF EXISTS keeps the engine's bare answer (`table_not_exists`);
3. only after `main.kafka.s` is gone (or absent) is `federation.kafka.s__offsets` dropped — always
   CASCADE: it is internal, and a VIEW someone put over it must not strand a
   source whose table is already gone. A failed offsets drop is logged, not
   answered (a later `DROP SOURCE IF EXISTS s` removes the leftover);
4. registry / producer / poller (a concurrent refused DROP of the same source may
   have relaunched it meanwhile) / `__sources` row cleanup.

`DROP SOURCE s CASCADE`: the same, minus the refusals; `main.kafka.s` is dropped CASCADE
(dependent views go, recursively), then `__offsets`, then every stream / INSERT
query of `cascade_closure` is stopped and forgotten (`forget_stream_objects`:
worker, registry, producer, INSERT bookkeeping, then their `__sources` rows), then
the source's own cleanup. **Mid-way failure:** the backing-table drop is the only
step that can refuse; until it succeeds nothing is changed (poller relaunched).
After it, the in-memory steps cannot fail; a failed `__offsets` drop or
`__sources` row delete is logged and the DROP stands — `recover()` deletes a
leftover reader row (dangling) and skips a leftover source row.

`DROP STREAM x`: `undefined_object` unless `IF EXISTS`; a SOURCE name is
`invalid_parameter`. RESTRICT refuses (`dependent_objects_still_exist`, `kafka: cannot drop stream
'x': stream 'y' depends on it; use DROP STREAM x CASCADE`) when another stream or
INSERT query reads FROM it (a stream's output topic can be another stream's
source). The stream's own INSERT INTO queries (it is their sink) go with it in both
modes. CASCADE drops `cascade_closure(x)`.

## detail/ (the implementation)

| File | Wraps / does |
|------|--------------|
| `kafka_consumer.{hpp,cpp}` | RAII over `RdKafka::KafkaConsumer`. `poll_batch`, `commit` (broker-group resume), and the exactly-once helpers `send_offsets_to_transaction(producer&, batch, …)` + `seek_to_batch_start` (rewind on abort). All RdKafka pointer ownership is confined here. |
| `kafka_producer.{hpp,cpp}` | RAII over `RdKafka::Producer` (`acks=all` + `enable.idempotence`). Optional `transactional_id` ⇒ exactly-once (`init_transactions` + `begin/send_offsets_to_transaction/commit/abort`). Used by INSERT→topic and STREAM output. |
| `kafka_poller.{hpp,cpp}` | Per-SOURCE `std::thread`: poll → `json_to_chunk` → `ingest_at_least_once` / `ingest_transactional` (free functions returning the engine's `core::error_t`, testable broker-free) → commit only on success. A failed batch is logged with its code, the consumer is rewound to the batch start and the batch is retried after a capped exponential pause (`batch_retry_backoff`). |
| `kafka_stream.{hpp,cpp}` | Per-STREAM continuous-query `std::thread`: poll → the batch plan (`stream_batch_plan`: `aggregate(empty)+[raw_data, <operators>]`) → `chunk_matches_columns` against the stream's declared output columns → produce. `TRANSACTIONAL=true` ⇒ the producer runs each batch in a transaction (exactly-once). A batch failing at any step (transform error, schema mismatch, produce) is never committed: rewind + capped backoff, like the poller. |
| `kafka_reader.{hpp,cpp}` | Pure/broker-free conversions + engine helpers: `json_to_chunk` / `chunk_to_json`, `chunk_matches_columns` (write-path schema guard — round-trips the rows through the declared columns), `kafka_insert`, `kafka_query` / `kafka_query_session` (parse SQL + send to the dispatcher; the session variant shares ONE engine transaction across calls — used by exactly-once), `kafka_parse_plan` / `kafka_execute`, `write_offsets` / `parse_offsets` (every write helper takes the table's full name), `stream_batch_plan` (the worker's per-batch plan), `describe_stream` / `described_columns` (a STREAM's output columns: the engine's describe of the batch plan over no rows). |

## Routing (who calls this)

The query session (`session/query_session.cpp`) detects Kafka work after parsing:
a `kafka_node_t` plan root → `KafkaManager::execute`; else `kafka_write_target(node)`
lets through an INSERT whose target, as written, can name a Kafka object (no
connection, a written database `main`, a written schema `kafka`). It is a Kafka
write when `object_names().contains(<last part>)` and the name, its empty slots
completed with the session's place (`SELECT current_database(), current_schema()`
on the connection's engine session, asked only then; a three-part name needs
none), is `main.kafka.<obj>` (`kafka_target_named`): `kafka.s` from `main.public`,
`s` from `main.kafka`, `main.kafka.s` from anywhere. Any other INSERT is the
engine's. The write splits on `source_is_select` — a `VALUES` write
(`node_raw_data` source) → `KafkaManager::produce` (one-shot), a `SELECT` source →
`KafkaManager::add_stream_insert` (continuous). All are sent as actor messages and
`co_await`ed.

## Constraints / conventions

- The grammar reserves `partition` → the offsets column is `partition_id`.
- `INSERT INTO kafka.<obj>` produces to the object's topic (SOURCE and STREAM
  alike); the produced rows are validated against the object's schema first.
- A session whose place is a connection's (`c.d.s`) completes `kafka.s` to
  `c.d.kafka.s`, but `current_database()` does not say the connection: such an
  INSERT is taken for `main.kafka.s` when `d` is `main` (otterbrix has no
  `current_connection()`).
- Declarable column types are the JSON-ingestible five (`INT`, `BIGINT`, `DOUBLE`,
  `BOOLEAN`, `VARCHAR`/`TEXT`/`STRING`); `map_column_type` rejects the rest at parse
  time. A JSON `null` ingests as SQL NULL; an integer outside the column's range
  or a wrong-kind value rejects the message with a logged reason.
- DDL validates its options (`TRANSACTIONAL`) before any side effect; CREATE fails
  (and rolls its tables back) when the `__sources` metadata row cannot be
  written; DROP is RESTRICT by default and refuses over Kafka-side readers or
  engine dependents before changing anything, drops `<name>__offsets` only after
  the backing table is gone, and forgets the object only then (`IF EXISTS`
  forgives only a missing object, never a dependent) — see "DROP semantics".
- **Known races.** The manager's loop interleaves
  coroutines at every `co_await`, so DDL on related objects is not serialised:
  a `CREATE STREAM` / `INSERT INTO … SELECT` over a source that a concurrent
  `DROP SOURCE … CASCADE` is dropping can register after the closure was taken and
  outlive its source (until `recover()` deletes it as dangling); a `__sources` row
  write/delete is not atomic with the in-memory registry (a crash or failed delete
  between them leaves a row `recover()` must reconcile); and `recover()` pass 3
  launches an INSERT INTO query whose target stream has a row but failed to
  recover. Two concurrent DROPs of one source are safe: the success path erases
  any poller the refused one relaunched.
- Integration tests are Python `--local` scripts in `tests/` (`test_kafka_*.py`);
  the broker is the `kafka` service in `compose.test.yml` (`127.0.0.1:19092`).
