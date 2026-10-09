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
  manager `.cpp` does `using namespace detail;`; `kafka_manager.hpp` qualifies
  members as `detail::kafka_*_t`. External code (scheduler, tests) reaches the
  helpers as `otterstax::kafka::detail::json_to_chunk` etc.

## KafkaManager (the actor)

`kafka_manager.{hpp,cpp}` — `actor_zeta::actor::actor_mixin<KafkaManager>`.

- `execute(session_hash_t, kafka_node_ptr)` — runs a parsed Kafka DDL node
  (CREATE/DROP SOURCE/STREAM). SOURCE → `ensure_database("kafka")` + create the
  backing table `kafka.<name>` (+ `kafka.<name>__offsets`) + register + (if
  `start_pollers_`) launch a poller. STREAM → compute the output schema from the
  SELECT (no table) + register + launch the continuous worker.
- `produce(session_hash_t, relname, source)` — routed here for `INSERT INTO
  kafka.<obj> VALUES`: evaluate the insert's source subplan, **validate the chunk
  against the object's declared columns** (`detail::chunk_matches_columns`), then
  publish to the topic (one-shot). Durability is the Kafka log itself — there is
  **no engine staging table** for writes.
- `add_stream_insert(session_hash_t, stream, insert_sql)` — routed here for
  `INSERT INTO kafka.<stream> SELECT … FROM kafka.<src>`: a continuous
  ksqlDB **INSERT INTO query** (persistent fan-in — several sources merged into one
  stream's output topic). Registers an anonymous stream-like object named
  `<stream>__insert__<n>` (its own `registry_`/`streams_`/`kafka.__sources` entry,
  consumer group + producer txn id) and `launch_stream`s it, so it is driven
  exactly like a `CREATE STREAM`. Target must be a STREAM (a SELECT into a SOURCE
  is rejected); exactly-once is inherited from the stream's `TRANSACTIONAL`.
- Owns `registry_` (name → `kafka_object_t{op, columns, options, as_select,
  source}` — `source` is the object a stream's / INSERT query's SELECT reads) plus
  `pollers_` / `producers_` / `streams_` maps, and `stream_insert_queries_`
  (stream → its INSERT-query names, for DROP-time cleanup). `start_pollers_` ctor
  flag is FALSE by default so broker-free unit tests stay thread-free;
  component_manager passes TRUE. `recover()` (called by component_manager after
  init) replays `kafka.__sources` in three passes — sources, streams, then INSERT
  INTO queries (`kind='insert'`, sink + source re-derived from the persisted SQL).
  Streams are recovered in rounds until none becomes resolvable, so a stream over
  another stream comes back whatever the row order. A stream or INSERT query that
  is **dangling** — its source (or, for an INSERT query, its target stream) has no
  `__sources` row, or is a source whose backing table is gone, or is itself a
  dangling stream — has its `__sources` row deleted (logged at warn) instead of
  being skipped on every restart and silently re-attached to an unrelated object
  re-created under the name. One whose source merely failed to recover (a probe
  error, a VIEW under the name) is only skipped, as before; so is a source row
  whose backing table is gone.
  A source's columns are not persisted: they are re-read from its backing table
  through the shared engine-side schema probe (`otterbrix/operators/schema_probe.hpp`,
  the same helper behind `OtterbrixDataManager::get_schema`), so an empty table
  recovers with the same schema as a populated one; a missing backing table is
  skipped, a VIEW under the source's name is refused.

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
  `kafka.<source>`; the backing table is dropped with `drop_behavior_t::restrict_`
  or `cascade_`.

`DROP SOURCE s` (RESTRICT):
0. a name registered as a stream or INSERT INTO query → `invalid_parameter`
   `kafka: 'st' is a stream, not a source; use DROP STREAM` (with or without IF
   EXISTS — a stream has no table, so the absent-table path would forget it);
1. any Kafka-side reader → `other_error` `kafka: cannot drop source 's': insert
   query 'm__insert__0', stream 'st' depend on it; use DROP SOURCE s CASCADE`,
   before anything is stopped (`IF EXISTS` does not forgive it);
2. the poller is stopped and `kafka.s` dropped RESTRICT. A refusal relaunches the
   poller and keeps registry, `__sources` row and `kafka.s__offsets` untouched; the
   engine's DETAIL oid is resolved through `pg_catalog.pg_class` +
   `pg_namespace` (`describe_restrict_refusal`, the oid spelled as a number into
   `kafka_query`) → `kafka: cannot drop source 's': view kafka.v depends on it
   (and possibly other objects); use DROP SOURCE s CASCADE` — the engine reports
   only its first dependent. If the oid cannot be resolved, or the engine's wording
   is not recognised, or the drop failed for another reason, the answer is the
   engine's own message after the same `kafka: cannot drop source 's': ` prefix,
   with the engine's code (`other_error` for the refusal). A missing table without
   IF EXISTS keeps the engine's bare `collection does not exist: kafka.s`
   (`table_not_exists`);
3. only after `kafka.s` is gone (or absent) is `kafka.s__offsets` dropped — always
   CASCADE: it is internal, and a VIEW someone put over it must not strand a
   source whose table is already gone. A failed offsets drop is logged, not
   answered (a later `DROP SOURCE IF EXISTS s` removes the leftover);
4. registry / producer / poller (a concurrent refused DROP of the same source may
   have relaunched it meanwhile) / `__sources` row cleanup.

`DROP SOURCE s CASCADE`: the same, minus the refusals; `kafka.s` is dropped CASCADE
(dependent views go, recursively), then `__offsets`, then every stream / INSERT
query of `cascade_closure` is stopped and forgotten (`forget_stream_objects`:
worker, registry, producer, INSERT bookkeeping, then their `__sources` rows), then
the source's own cleanup. **Mid-way failure:** the backing-table drop is the only
step that can refuse; until it succeeds nothing is changed (poller relaunched).
After it, the in-memory steps cannot fail; a failed `__offsets` drop or
`__sources` row delete is logged and the DROP stands — `recover()` deletes a
leftover reader row (dangling) and skips a leftover source row.

`DROP STREAM x`: `do_not_exists` unless `IF EXISTS`; a SOURCE name is
`invalid_parameter`. RESTRICT refuses (`other_error`, `kafka: cannot drop stream
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
| `kafka_stream.{hpp,cpp}` | Per-STREAM continuous-query `std::thread`: poll → node-swap transform (`aggregate(empty)+[raw_data, <operators>]`) → `chunk_matches_columns` against the stream's declared output columns → produce. `TRANSACTIONAL=true` ⇒ the producer runs each batch in a transaction (exactly-once). A batch failing at any step (transform error, schema mismatch, produce) is never committed: rewind + capped backoff, like the poller. |
| `kafka_reader.{hpp,cpp}` | Pure/broker-free conversions + engine helpers: `json_to_chunk` / `chunk_to_json`, `chunk_matches_columns` (write-path schema guard — round-trips the rows through the declared columns), `kafka_insert`, `kafka_query` / `kafka_query_session` (parse SQL + send to the dispatcher; the session variant shares ONE engine transaction across calls — used by exactly-once), `kafka_parse_plan` / `kafka_execute`, `write_offsets` / `parse_offsets`, `stream_output_schema` (computes a STREAM's projected schema via `schema_utils::aggregate_filter_schema`, since the engine drops the schema of a projection over an empty result). |

## Routing (who calls this)

The `Worker` (`scheduler/worker.cpp`) detects Kafka work after parsing:
a `kafka_node_t` plan root → `KafkaManager::execute`; else `kafka_write_target(node)`
(an `INSERT INTO kafka.<obj>`) splits on `source_is_select` — a `VALUES` write
(`node_raw_data` source) → `KafkaManager::produce` (one-shot), a `SELECT` source →
`KafkaManager::add_stream_insert` (continuous). All are sent as actor messages and
`co_await`ed.

## Constraints / conventions

- The grammar reserves `partition` → the offsets column is `partition_id`.
- `INSERT INTO kafka.<obj>` produces to the object's topic (SOURCE and STREAM
  alike); the produced rows are validated against the object's schema first.
- Declarable column types are the JSON-ingestible five (`INT`, `BIGINT`, `DOUBLE`,
  `BOOLEAN`, `VARCHAR`/`TEXT`/`STRING`); `map_column_type` rejects the rest at parse
  time. A JSON `null` ingests as SQL NULL; an integer outside the column's range
  or a wrong-kind value rejects the message with a logged reason.
- DDL validates its options (`TRANSACTIONAL`) before any side effect; CREATE fails
  (and rolls its tables back) when the `kafka.__sources` metadata row cannot be
  written; DROP is RESTRICT by default and refuses over Kafka-side readers or
  engine dependents before changing anything, drops `<name>__offsets` only after
  the backing table is gone, and forgets the object only then (`IF EXISTS`
  forgives only a missing object, never a dependent) — see "DROP semantics".
- **Known races (pre-existing, not addressed).** The manager's loop interleaves
  coroutines at every `co_await`, so DDL on related objects is not serialised:
  a `CREATE STREAM` / `INSERT INTO … SELECT` over a source that a concurrent
  `DROP SOURCE … CASCADE` is dropping can register after the closure was taken and
  outlive its source (until `recover()` deletes it as dangling); a `__sources` row
  write/delete is not atomic with the in-memory registry (a crash or failed delete
  between them leaves a row `recover()` must reconcile); and `recover()` pass 3
  launches an INSERT INTO query whose target stream has a row but failed to
  recover. Two concurrent DROPs of one source are safe: the success path erases
  any poller the refused one relaunched.
- **Actor-isolation exception: `recover()` reads the probe's plan after the engine
  replied.** For each SOURCE, `recover()` sends `probe.execution_plan(resource_)` to
  the engine's dispatcher actor and waits for the reply future (`drive`). Then
  `schema_probe::read_columns` reads `probe.aggregate->output_types()`, falling back
  to the resolve nodes' catalog metadata; `read_table_oid` reads the same metadata.
  Those nodes belong to the Kafka actor, but the engine wrote them.
  - **Why it is safe.** The engine stamps the types during validation
    (`validate_schema`), before it builds the physical plan and before it replies.
    `manager_dispatcher_t::execute_plan` takes the plan by value and answers with the
    cursor only, so after the reply the nodes belong to the caller alone. The engine
    may still release its node references on its own thread: the
    `intrusive_ref_counter` count is atomic, and `output_types_` is allocated on the
    caller's resource.
  - **What it rests on.** The guarantee depends on that ordering inside the engine; a
    continuation that writes to the plan after the reply, or a plan cache, would break
    it silently. `OtterbrixDataManager::get_schema` and `describe_collection` rely on
    the same protocol.
  - **Otterbrix rc-3.** The resolve metadata moves off the resolve nodes into
    `execution_plan_t::catalog_resolves`, and view expansion moves to
    `components::planner::expand_view_body`. `otterbrix/operators/schema_probe.hpp`
    (`read_columns`, `read_table_oid`, `make_view_probe`) is rewritten for that
    migration anyway, and that rewrite is where the column types should come from the
    engine's reply instead of being read back off the plan.
- Integration tests are Python `--local` scripts in `tests/` (`test_kafka_*.py`);
  the broker is the `kafka` service in `compose.test.yml` (`127.0.0.1:19092`).
