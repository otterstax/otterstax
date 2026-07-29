# Demo Runbook (for an LLM agent)

This document is a self-contained instruction set. Read it top-to-bottom and execute the commands as written. The goal is to bring up the federated-SQL demo and run six progressive SQL files against it.

Everything demo-related lives in this folder (`examples/demo/`). Paths below are given relative to the project root unless noted.

## What the demo shows

OtterStax is a federated SQL engine. The demo registers three real backends, one
S3-compatible object store, and a Kafka broker —

- **MariaDB** (`mysql.bill`) — orders, invoices (ACID transactions)
- **PostgreSQL** (`pg.shop`) — customers (with ENUM tier), products
- **ClickHouse** (`ch.ev`) — sessions (with nested struct columns, columnar OLAP)
- **MinIO** (`s3_alias = 'demo_s3'`) — single bucket `demo-bucket` for the
  external-table demo (steps 7-9)
- **Kafka / redpanda** (`demo-kafka`, host `127.0.0.1:19093`) — live order-event
  topics for the streaming act (`kafka/`, separate from steps 1-9)

— and runs nine SQL files (`sql/step_1.sql` … `sql/step_9.sql`) that all go through OtterStax via the **PostgreSQL wire protocol on port 8817**. Each file is a single SQL statement that JOINs across two or three backends, defines local engine tables, or exercises the s3 external-table path. Step 3 defines local types and a local `otter.warehouses` table inside the engine; steps 7-9 load two files from MinIO into the engine and dump a JOIN result back out as CSV.

The **Kafka streaming act** is a separate, step-by-step tour under `kafka/`
(source ingestion, the `kafka ⋈ ClickHouse ⋈ Postgres` federated JOIN, the
write path, continuous streams, and fan-in) — see `kafka/README.md`. It needs
the `demo-kafka` broker (started by `up.sh`) and the pg/ch connections.

Demo SQL uses the **3-part** qualifier form `<alias>.<db>.<tbl>` (e.g. `mysql.bill.orders`, `pg.shop.customers`, `ch.ev.sessions`). The parser promotes 3-part to its internal 4-part shape automatically, then `sql_gen::table_reference` emits backend-native qualifiers (`db.tbl` for MySQL/CH, `schema.tbl` for PG). External tables live under the `otter` engine database (`otter.regions`, `otter.promos`) — same database created by step_3a, so the cleanup script tears them down together.

## Pick a mode

Two ways to run, pick **one**:

| Mode | When | Otterstax | Command |
|---|---|---|---|
| **Full docker** | One-shot demo, no local build | Built inside docker (image `otterstax_app`) | `examples/demo/up.sh` |
| **Bench** (local) | You already have a built `./build/server` and want to iterate on the engine | Local binary, you start it yourself | `examples/demo/up.sh --local` |

Full docker is the default; choose `--local` only if the user explicitly asks to iterate on otterstax itself.

## Prerequisites

- Docker daemon running. If the current user can't reach `/var/run/docker.sock`, the scripts auto-fall-back to `sudo -n docker`. If `sudo -n` is not configured, ask the user to either add themselves to the `docker` group or enable passwordless sudo.
- Python 3 with `faker` (always) and `pyarrow` (for `step_8` — written via `pq.write_table` in `generate_data.py`). If pyarrow is missing the script prints a warning and skips `init/s3/promos.parquet`; step_8 will then fail with "External table is not registered". The scripts auto-`source ../../.venv/bin/activate` if a project venv exists.
- `psql` client on the host (used to drive the demo SQL files).
- Ports 3201, 3202, 3204, 3205, 3206, 3207, 8815, 8816, 8817 free on the host. 3206/3207 are the MinIO S3 API + console.

## Mode A: full docker (`examples/demo/up.sh`)

```bash
examples/demo/up.sh
```

This does, in order:

1. `python examples/demo/generate_data.py` — writes `examples/demo/init/{mariadb,postgres,clickhouse}/init.sql` with seeded data (correlated IDs across backends, time windows aligned to demo predicates) **and** writes `examples/demo/init/s3/{regions.csv,promos.parquet}` (6 rows + 12 rows) for steps 7-9.
2. `docker compose -f examples/demo/compose.yml --profile full up -d --build` — starts MariaDB + PostgreSQL + ClickHouse + MinIO + the `demo-minio-init` one-shot (which `mc mb demo-bucket && mc cp /fixtures/ m/demo-bucket/`) + the `otterstax_app` container.
3. Polls each container's healthcheck (up to 3 minutes). `demo-minio-init` exits cleanly once seeding finishes; it's not polled.
4. Connections require no registration step: `examples/demo/config.yaml` (docker-DNS hostnames: `demo-mariadb`, `demo-minio`, …) is mounted into the `demo-otterstax` container by `compose.yml` and read once at startup — server settings, the mysql/pg/ch backends **and** the `demo_s3` alias all come from that one file.
5. Prints the `psql` invocation lines.

The three wire ports are host-published:

- PG wire: `localhost:8817` ← the demo uses this
- MySQL wire: `localhost:8816`
- FlightSQL: `localhost:8815`

## Mode B: bench (`examples/demo/up.sh --local`)

```bash
examples/demo/up.sh --local
# in another terminal — a single config.yaml with the host-published ports:
./build/server --config examples/demo/config_local.yaml
# connections (mysql/pg/ch/s3) are read from that same file at startup — no separate step
# and, for the Kafka act:
examples/demo/kafka/1_ingestion/run.sh --local   # … then 2_join, 3_produce, …
```

Bench mode starts the three backends + MinIO + `demo-minio-init` + `demo-kafka` (no `otterstax_app` container). You run the engine binary yourself, pointing `--config` at `examples/demo/config_local.yaml` — its `connections:` section uses the `localhost:3201/3202/3204/3206` (host-published backend + minio ports) variant, which is what a local server can reach. The Kafka act's `--local` points the SQL at the broker's published listener `127.0.0.1:19093`.

## Run the demo steps

Once the server is up (connections load from the config file at startup), run each step against the PG wire:

```bash
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_1.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_2.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_3a_ddl.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_3b_insert.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_3c_select.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_3d_main.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_4.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_5.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_6.sql
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_7.sql   # s3 csv     → otter.regions
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_8.sql   # s3 parquet → otter.promos
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_9.sql   # JOIN + COPY back to s3
```

Or all at once with `examples/demo/run-queries.sh` (`for f in sql/step_*.sql; do psql … -f $f; done` — lexicographic sort matches the intended order).

Each step is a single SELECT (or DDL/INSERT for 3a/3b, DDL/COPY for 7/8/9). Expected output:

| Step | Backends touched | Demonstrates | Expected rows |
|---|---|---|---|
| 1 | mysql, pg | Cross-source JOIN + derived subquery with date filter | up to 10 |
| 2 | mysql, pg | Derived aggregates + HAVING alias + NOT LIKE | up to 20 |
| 3a | local (engine) | `CREATE TYPE` (ENUM + composite STRUCTs) + `CREATE TABLE otter.warehouses` | no rows, structural |
| 3b | local | INSERT 3 rows with `ROW(...)` composite values | 3 rows affected |
| 3c | local | `(struct).field` projection + `IS NULL` on STRUCT field | 1 row (TLV-1) |
| 3d | mysql, pg, ch | Three-backend JOIN with `(ch.props).channel` struct access | varies |
| 4 | pg (ENUM), local (STRUCT) | **Backend ⋈ otterbrix-internal in a single statement** — canonical positive example for this shape (see top-level `CLAUDE.md` "Working JOIN shapes" and `FIX_JOIN.md`). Backend manager fetches the customers slice, otterbrix engine JOINs that against engine-resident warehouses. Plus ENUM cast `'gold'::tier_t` + struct field JOIN key | 14 rows |
| 5 | pg, ch, mysql | DISTINCT + nested `((s.props).geo).ip` | varies |
| 6 | mysql, pg, ch | CASE WHEN inside SUM + HAVING + LEFT JOIN inside subquery | a few rows |
| 7 | s3 | `CREATE EXTERNAL TABLE otter.regions` from `s3://demo-bucket/regions.csv` (csv) | no rows, structural |
| 8 | s3 | `CREATE EXTERNAL TABLE otter.promos`  from `s3://demo-bucket/promos.parquet` (parquet) | no rows, structural |
| 9 | local (s3-sourced) | `COPY (SELECT … JOIN …)` → `s3://demo-bucket/exports/promos_by_region.csv` | no rows in psql; CSV appears in MinIO |

## Re-running

`CREATE DATABASE otter` from step 3a and the `CREATE EXTERNAL TABLE` calls
from steps 7-8 fail the second time because the engine state persists.
`cleanup.sql` drops the s3-loaded tables (`otter.regions`, `otter.promos`,
`otter.promos_by_region_rt`) and then the whole `otter` database — run it
before redoing the demo:

```bash
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/cleanup.sql
```

The `s3://demo-bucket/exports/promos_by_region.csv` object that step_9 writes
is **not** cleaned by `cleanup.sql` (it's outside the engine). Subsequent
runs of step_9 overwrite it. A full reset with `examples/demo/down.sh` wipes
the MinIO container along with the rest (the bucket is recreated by
`demo-minio-init` on the next `up`).

The Kafka act is re-runnable in place — each step's `DROP SOURCE IF EXISTS …
CREATE SOURCE …` (and `DROP STREAM …`) re-creates its objects cleanly, so you can
replay `run_all.sh` (or an individual step) without recreating otterstax.

## Tear down

```bash
examples/demo/down.sh
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `docker: permission denied` | User not in `docker` group | The scripts try `sudo -n docker` automatically. If sudo asks for password, fix by adding user to `docker` group or enabling NOPASSWD. |
| `python: No module named faker` | venv not active | `python -m pip install faker`, or activate the project venv first. |
| `Connection refused` on port 8817 | otterstax not ready yet | The healthcheck loop waits up to 180s. If it timed out, check `docker logs demo-otterstax`. |
| `CREATE DATABASE otter ... already exists` on rerun | Step 3a persists state | Run `examples/demo/sql/cleanup.sql` or `examples/demo/down.sh && examples/demo/up.sh`. |
| `psql: error: server closed the connection unexpectedly` | otterstax crashed mid-query | `docker logs demo-otterstax | tail -50`. Cross-backend ARRAY queries and some struct cases are known to crash; demo SQL avoids these but custom queries may hit them. |
| `er_table_exists_error` on test data | State from previous run leaked into backend volumes | `examples/demo/down.sh` then bring up fresh. |
| `Otterbrix execution failed: database does not exist` | Query routed to local engine that doesn't know the external alias | Check the config file (`config.yaml` / `config_local.yaml`) `connections:` section has all three backends and restart the server — connections load only at startup. |
| `External table is not registered: …` from step 7/8/9 | The `demo_s3` alias was not in the config file this server read | Add the `s3:` `demo_s3` entry under `connections:` and restart the server. |
| Step 8 fails with `External table is not registered` and `init/s3/promos.parquet` is missing | `pyarrow` not installed when `generate_data.py` ran | `pip install pyarrow` and re-run `python examples/demo/generate_data.py`, then `docker compose -f examples/demo/compose.yml up -d demo-minio-init` to re-seed. |
| Step 9 hangs or returns `Cannot reach minio:9000` from inside the server | s3 endpoint mismatch (docker vs local config) | In docker mode the alias points at `demo-minio:9000`; in bench mode it points at `localhost:3206`. Use the matching config file (`config.yaml` vs `config_local.yaml`) and restart the server. |

## File reference

All paths relative to `examples/demo/`:

| Path | What |
|---|---|
| `compose.yml` | docker-compose for the demo stack (no-profile = backends + MinIO; `--profile full` = + otterstax) |
| `up.sh [--local]` | main entry point — full docker (default) or bench mode (`--local`) |
| `down.sh` | tear down containers and wipe volumes (incl. the MinIO bucket) |
| `run-queries.sh` | run all `sql/step_*.sql` files in order against the PG wire (works for both modes) |
| `generate_data.py` | seeds init SQL files into `init/{mariadb,postgres,clickhouse}/`, s3 fixtures into `init/s3/`, AND kafka fixtures into `init/kafka/` |
| `init/{mariadb,postgres,clickhouse}/init.sql` | generated init scripts (gitignored) |
| `init/s3/{regions.csv,promos.parquet}` | generated S3 fixtures (gitignored); seeded into `demo-bucket` by the `demo-minio-init` compose service |
| `init/kafka/{orders_live,orders_intl}.ndjson` | generated Kafka fixtures (gitignored); produced onto topics by the `seed` helper in `kafka/lib/_common.sh` (rpk inside the demo-kafka container) |
| `kafka/` | the Kafka streaming act — step folders (`1_ingestion`…`6_teardown`), `run_all.sh`, `lib/` helpers; see `kafka/README.md` |
| `config.yaml` | single config file for docker mode (server settings + connections; docker-DNS hostnames `demo-mariadb`, `demo-minio`, …); mounted into the container, read at startup |
| `config_local.yaml` | single config file for bench/local mode (host-published ports: 3201/3202/3204/3206) — pass with `--config` |
| `sql/step_*.sql` | the actual demo queries — `step_7` / `step_8` load from s3, `step_9` JOINs and dumps back |
| `sql/cleanup.sql` | drops the s3-loaded tables and the `otter` engine database |

## Hand-off note

When running this autonomously: run **steps in order**, report the row count or first row from each step's output, and stop on the first error. Do NOT skip step 3a/3b — later steps depend on `otter.warehouses` existing. Step 9 depends on `otter.regions` (step 7) and `otter.promos` (step 8); after step 9 completes, the JOIN result is at `s3://demo-bucket/exports/promos_by_region.csv` (browse it via the MinIO console at `http://localhost:3207`, login `minioadmin`/`minioadmin`).
