# Kafka streaming demo act

A step-by-step tour of every Kafka feature OtterStax implements, layered on top
of the federated-SQL demo. Each **step** is a numbered folder here
(`1_ingestion/`, `2_join/`, …); inside it the **sub-steps** are individual
files — bring up a topic (`*.sh`), a SQL statement (`*.sql`), or drain a topic
(`*.sh`). A step's `run.sh` walks its sub-steps in order, pausing for `[Enter]`
before each so you can narrate.

## What each step shows

| Step | Feature | Sub-steps |
|------|---------|-----------|
| `1_ingestion` | A Kafka topic becomes a queryable SQL table, **exactly-once** | seed topic → `CREATE SOURCE` → `SELECT`/`GROUP BY` |
| `2_join` | **The selling point:** `kafka ⋈ ClickHouse ⋈ Postgres` in one query | the federated `JOIN` |
| `3_produce` | `INSERT INTO kafka.<obj> VALUES` **produces** to the topic (round-trips back) | `INSERT` → round-trip `SELECT` |
| `4_stream` | `CREATE STREAM … AS SELECT` — continuous, **exactly-once** transform to a topic | seed output topic → `CREATE STREAM` → consume |
| `5_fanin` | `INSERT INTO <stream> SELECT` — continuous **fan-in / union** of a 2nd feed | seed 2nd feed → `CREATE SOURCE` → fan-in `INSERT` → consume |
| `6_teardown` | Input validation (`TRANSACTIONAL=maybe` rejected) + `DROP` cleanup | validation → `DROP` |

Steps are **ordered and build on each other** (step 2 needs step 1's source,
step 5 needs step 4's stream). Walk them front to back.

## Prerequisites

Bring the demo stack up first — it starts the backends **and** the `demo-kafka`
broker, and (in docker mode) registers the pg/ch connections the JOIN needs:

```bash
examples/demo/up.sh            # full docker
# — or —
examples/demo/up.sh --local    # bench mode; then start the server yourself and:
examples/demo/connections/add_connections.sh --local
```

## Run it — step by step (for a live demo)

```bash
examples/demo/kafka/1_ingestion/run.sh   [--local]
examples/demo/kafka/2_join/run.sh        [--local]
examples/demo/kafka/3_produce/run.sh     [--local]
examples/demo/kafka/4_stream/run.sh      [--local]
examples/demo/kafka/5_fanin/run.sh       [--local]
examples/demo/kafka/6_teardown/run.sh    [--local]
```

Add `--local` when otterstax runs as a local binary (the broker the SQL embeds
is then `127.0.0.1:19093` instead of the in-network `demo-kafka:9092`). You can
also run a single sub-step file by hand — the `*.sh` ones are self-contained;
for a `*.sql` one pass the broker, e.g.
`psql … -v broker=127.0.0.1:19093 -f 1_ingestion/02_create_source.sql`.

## Run it — all at once (smoke test)

```bash
examples/demo/kafka/run_all.sh [--local]        # no pauses
```

## Layout

```
kafka/
  README.md
  run_all.sh                 # chain every step, non-interactive
  lib/
    _common.sh               # broker addressing + psql/pause/wait helpers
    seed.py    --topic T [--fixture F] [--reset]   # bring up / reset a topic
    consume.py --topic T [--count N] [--timeout S] # drain a topic, print it
  <n>_<name>/                # one folder per step (1_ingestion, 2_join, …)
    run.sh                   # walk this step's sub-steps (pausing for [Enter])
    NN_*.sql | NN_*.sh       # the individual sub-steps
```

The order-event fixtures (`orders_live.ndjson`, `orders_intl.ndjson`) are
written into `examples/demo/init/kafka/` by `generate_data.py`; their
`customer_id`s are real customers that also have ClickHouse sessions, so the
step-2 JOIN returns rows.
