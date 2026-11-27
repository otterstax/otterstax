# Demo Runbook (for an LLM agent)

This document is a self-contained instruction set. Read it top-to-bottom and execute the commands as written. The goal is to bring up the federated-SQL demo and run nine progressive SQL files against it.

Everything demo-related lives in this folder (`examples/demo/`). Paths below are given relative to the project root unless noted.

## What the demo shows

OtterStax is a federated SQL engine. The demo registers three real backends, one
S3-compatible object store, and a Kafka broker —

- **MariaDB** (`mysql.bill`) — orders, invoices (ACID transactions)
- **PostgreSQL** (`pg.shop`) — customers (with ENUM tier), products
- **ClickHouse** (`ch.ev`) — sessions (with nested struct columns, columnar OLAP)
- **RustFS** (`s3_alias = 'demo_s3'`) — single bucket `demo-bucket` for the
  external-table demo (steps 7-9)
- **Kafka / redpanda** (`demo-kafka`, host `127.0.0.1:19093`) — live order-event
  topics for the streaming act (`examples/demo/kafka/`, separate from steps 1-9)

— and runs nine SQL files (`examples/demo/sql/step_1.sql` … `examples/demo/sql/step_9.sql`) that all go through OtterStax via the **PostgreSQL wire protocol on port 8817**. Each file is a single SQL statement that JOINs across two or three backends, defines local engine tables, or exercises the s3 external-table path. Step 3 defines local types and a local `otter.warehouses` table inside the engine; steps 7-9 load two files from RustFS into the engine and dump a JOIN result back out as CSV.

The **Kafka streaming act** is a separate, step-by-step tour under `examples/demo/kafka/`
(source ingestion, the `kafka ⋈ ClickHouse ⋈ Postgres` federated JOIN, the
write path, continuous streams, and fan-in) — see `examples/demo/kafka/README.md`. It needs
the `demo-kafka` broker (started by `examples/demo/up.sh`) and the pg/ch connections.

Demo SQL names every remote table in **3 parts** `<alias>.<namespace>.<table>` (e.g. `mysql.bill.orders`, `pg.shop.customers`, `ch.ev.sessions`): the namespace is a database of the MySQL / ClickHouse server and a schema of the PostgreSQL connection's `database:` (`shop` here). Four parts are refused; local tables are `local.<schema>.<table>`. The psql clients connect to the database `local`, so the session's place is `local.public` (a name that is neither `local[.ns]` nor a connection `uid[.ns]` is refused, `FATAL 3D000`). Local tables live in the schema `otter` of `local` (`local.otter.warehouses`, and the external tables `local.otter.regions`, `local.otter.promos`) — the schema created by step_3a (`CREATE SCHEMA otter` always lands in `local`), so the cleanup script tears them down.

## Pick a mode

Two ways to run, pick **one**:

| Mode | When | Otterstax | Command |
|---|---|---|---|
| **Full docker** | One-shot demo, no local build | The published image `ghcr.io/otterstax/otterstax:0.2.0` | `examples/demo/up.sh` |
| **Bench** (local) | You already have a built `./build/Release/server` and want to iterate on the engine | Local binary, you start it yourself | `examples/demo/up.sh --local` |

Full docker is the default; choose `--local` only if the user explicitly asks to iterate on otterstax itself.

## Prerequisites

- Docker daemon running. If the current user can't reach `/var/run/docker.sock`, the scripts auto-fall-back to `sudo -n docker`. If `sudo -n` is not configured, ask the user to either add themselves to the `docker` group or enable passwordless sudo.
- Python 3 with `faker` (always) and `pyarrow` (to generate `promos.parquet` for step 8). Without `pyarrow`, `examples/demo/generate_data.py` warns and skips writing the file. The scripts auto-`source ../../.venv/bin/activate` if a project venv exists.
- `psql` client on the host (used to drive the demo SQL files).
- Ports 3201, 3202, 3204, 3205, 3206, 3207, 8815, 8816, 8817, 19093 free on the host. 3206/3207 are the RustFS S3 API + console; 19093 is the Kafka broker's host-published listener.

## Mode A: full docker (`examples/demo/up.sh`)

```bash
examples/demo/up.sh
```

This does, in order:

1. `python3 examples/demo/generate_data.py` — writes `examples/demo/init/{mariadb,postgres,clickhouse}/init.sql` with seeded data (correlated IDs across backends, time windows aligned to demo predicates) **and** writes `examples/demo/init/s3/{regions.csv,promos.parquet}` (6 rows + 12 rows) for steps 7-9.
2. `docker compose -f examples/demo/compose.yml --profile full up -d` — starts the backends, RustFS, Kafka, the one-shot `demo-rustfs-init` job and `demo-otterstax` from the published image.
3. Polls the long-running containers' health checks (up to 3 minutes each), then waits for `demo-rustfs-init` to exit successfully. The seed job uploads the fixtures and verifies that RustFS has both `regions.csv` and `promos.parquet`.
4. Connections require no registration step: `examples/demo/config.yaml` (docker-DNS hostnames: `demo-mariadb`, `demo-rustfs`, …) is mounted into the `demo-otterstax` container at `/otterstax/config.yaml` by `compose.yml` and read once at startup — server settings, the mysql/pg/ch backends **and** the `demo_s3` alias all come from that one file.
5. Prints the `psql` invocation lines.

The public image is published for Linux amd64. To use a locally built runtime
image, see [Running with Docker Compose](../../readme.md#running-with-docker-compose)
and run `OTTERSTAX_IMAGE=otterstax:local examples/demo/up.sh`. If the script uses
sudo for Docker, set the variable in the demo directory's Compose environment
file (`.env`).

The three wire ports are host-published:

- PG wire: `localhost:8817` ← the demo uses this
- MySQL wire: `localhost:8816`
- FlightSQL: `localhost:8815`

## Mode B: bench (`examples/demo/up.sh --local`)

```bash
examples/demo/up.sh --local
# in another terminal — a single config.yaml with the host-published ports:
./build/Release/server --config examples/demo/config_local.yaml
# connections (mysql/pg/ch/s3) are read from that same file at startup — no separate step
# and, for the Kafka act:
examples/demo/kafka/1_ingestion/run.sh --local   # … then 2_join, 3_produce, …
```

Bench mode starts the three backends, RustFS and Kafka, and waits for the same S3 seed job. You run the engine binary yourself, pointing `--config` at `examples/demo/config_local.yaml` — its `connections:` section uses the `localhost:3201/3202/3204/3206` (host-published backend + RustFS ports) variant, which is what a local server can reach. The Kafka act's `--local` points the SQL at the broker's published listener `127.0.0.1:19093`.

## Run the demo steps

Start with a ready PG wire and a successful S3 seed. Both `examples/demo/up.sh` modes wait
for the seed job. If you started the containers directly with Compose, run
`docker wait demo-rustfs-init` before step 7, or before starting the all-steps
runner. The command must print `0`, which means the job verified both objects in
`demo-bucket`. For any other result, check the seed logs and follow *Step 8 cannot
read `promos.parquet`* below.

Run each step against the PG wire and stop if a statement fails:

```bash
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_1.sql
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_2.sql
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local -f examples/demo/sql/step_3a_ddl.sql
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_3b_insert.sql
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_3c_select.sql
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_3d_main.sql
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_4.sql
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_5.sql
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_6.sql
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_7.sql   # s3 csv     → otter.regions
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_8.sql   # s3 parquet → otter.promos
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/step_9.sql   # JOIN + COPY back to s3
```

On a fresh engine, `examples/demo/run-queries.sh` runs all the files in order
and exits on the first SQL error. To replay on the same engine after cleanup,
follow *Re-running* below and execute the steps individually.

Each step is a single SELECT (or DDL/INSERT for 3a/3b, DDL/COPY for 7/8/9). Expected output:

| Step | Backends touched | Demonstrates | Expected rows |
|---|---|---|---|
| 1 | mysql, pg | Cross-source JOIN + derived subquery with date filter | up to 10 |
| 2 | mysql, pg | Derived aggregates + HAVING alias + NOT LIKE | up to 20 |
| 3a | local (engine) | `CREATE TYPE` (ENUM + composite STRUCTs) + `CREATE TABLE otter.warehouses` | no rows, structural |
| 3b | local | INSERT 3 rows with `ROW(...)` composite values | 3 rows affected |
| 3c | local | `(struct).field` projection + `IS NULL` on STRUCT field | 1 row (TLV-1) |
| 3d | mysql, pg, ch | Three-backend JOIN with `(ch.props).channel` struct access | varies |
| 4 | pg (ENUM), local (STRUCT) | **Backend ⋈ otterbrix-internal in a single statement** — demonstrated by `examples/demo/sql/step_4.sql`. Backend manager fetches the customers slice, otterbrix engine JOINs that against engine-resident warehouses. Plus ENUM cast `'gold'::tier_t` + struct field JOIN key | 14 rows |
| 5 | pg, ch, mysql | DISTINCT + nested `((s.props).geo).ip` | varies |
| 6 | mysql, pg, ch | CASE WHEN inside SUM + HAVING + LEFT JOIN inside subquery | a few rows |
| 7 | s3 | `CREATE EXTERNAL TABLE otter.regions` from `s3://demo-bucket/regions.csv` (csv) | no rows, structural |
| 8 | s3 | `CREATE EXTERNAL TABLE otter.promos`  from `s3://demo-bucket/promos.parquet` (parquet) | no rows, structural |
| 9 | local (s3-sourced) | `COPY (SELECT … JOIN …)` → `s3://demo-bucket/exports/promos_by_region.csv` | no rows in psql; CSV appears in RustFS |

## Re-running

`CREATE SCHEMA otter` / `CREATE TYPE` from step 3a and the `CREATE EXTERNAL
TABLE` calls from steps 7-8 fail the second time because the engine state
persists. `examples/demo/sql/cleanup.sql` is a **full reset of `local.otter`**.
Its `DROP SCHEMA … CASCADE` can delete
user-created objects in the schema and dependent objects in other schemas.
Use it only when you intend that reset:

```bash
psql -h localhost -p 8817 -U demo local -f examples/demo/sql/cleanup.sql
```

To keep the schema, first verify that the named objects belong to the demo.
Run these statements in order and stop if a dependency blocks a drop.

```sql
DROP TABLE IF EXISTS local.otter.promos_by_region_rt;
DROP TABLE IF EXISTS local.otter.regions;
DROP TABLE IF EXISTS local.otter.promos;
DROP TABLE IF EXISTS local.otter.warehouses;
```

Both cleanup recipes keep the demo types. Replay the steps individually and
execute only the applicable statements from step 3a. Skip
`CREATE SCHEMA otter` if the schema still exists and skip `CREATE TYPE` for each
demo type already created successfully. After a complete first run, replay
only the `CREATE TABLE` from step 3a if you kept the schema. After a full schema
reset, run `CREATE SCHEMA otter` first.

An older `examples/demo/sql/cleanup.sql` issued `DROP TYPE IF EXISTS` for the composite types.
On rc-7 that can leave `address_t` or `spec_t` unusable while a catalog entry
still blocks recreation. If step 3a hits that state, use a fresh local engine
data directory or a fresh disposable demo stack.

The `s3://demo-bucket/exports/promos_by_region.csv` object that step_9 writes
is **not** cleaned by `examples/demo/sql/cleanup.sql` (it's outside the engine). Subsequent
runs of step_9 overwrite it. A full reset with `examples/demo/down.sh` wipes
the RustFS container along with the rest (the bucket is recreated by
`demo-rustfs-init` on the next `up`).

You can rerun `examples/demo/kafka/run_all.sh` or an individual step on the same
server. Steps 1, 4 and 5 recreate their topics and discard existing records.
`DROP SOURCE … CASCADE` also removes dependents of the source, including any
later demo stream or fan-in query that reads it. See `examples/demo/kafka/README.md`
for the topics affected by each step.
Every Kafka step script exits non-zero on its first failed sub-step. `examples/demo/kafka/run_all.sh`
checks the PG wire first and stops at the first failed step, so its
`✅ Kafka act complete` means every step succeeded (details in `examples/demo/kafka/README.md`).

## Pause or reset

### Full Docker mode

To free port 8817 while preserving the running demo's data, use
`docker stop -t 60 demo-otterstax`. Resume with `docker start demo-otterstax`
when the port is free again.

The following command is a **full reset**. It removes the demo containers
and their volumes, including local OtterStax data, backend data, Kafka state
and RustFS objects.

```bash
examples/demo/down.sh
```

### Local mode (`--local`)

To free port 8817, stop the host `server` process. If it is running in a
terminal, press Ctrl+C there. Resume by starting it with the same config.

`examples/demo/down.sh` removes the Docker backends, Kafka and RustFS and their
volumes. The host engine's data directory stays on disk. For a clean local
engine, stop the server, copy `examples/demo/config_local.yaml`, set
`service.engine.data_dir` in that copy to a new empty directory, then start
`./build/Release/server --config` with the copied config. The shipped local config explicitly sets
`data_dir` to `/tmp/test_collection_sql/base`.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `docker: permission denied` | User not in `docker` group | The scripts try `sudo -n docker` automatically. If sudo asks for password, fix by adding user to `docker` group or enabling NOPASSWD. |
| `python: No module named faker` | venv not active | `python -m pip install faker`, or activate the project venv first. |
| `Connection refused` on port 8817 | otterstax not ready yet | The healthcheck loop waits up to 180s. If it timed out, check `docker logs demo-otterstax`. |
| `CREATE SCHEMA otter ... already exists` on rerun | Step 3a persists state | Use the scoped cleanup above and skip schema creation, or follow the full-reset procedure if you intend to discard the schema and its dependents. |
| `psql: error: server closed the connection unexpectedly` | the server died or the connection was lost mid-query | `docker logs demo-otterstax \| tail -50` for the cause; a restart is the normal recovery. If a query shape reproducibly kills the server, keep the statement and the log — no such shape is currently registered |
| `er_table_exists_error` on test data | State from previous run leaked into backend volumes | `examples/demo/down.sh` then bring up fresh. |
| A remote table written `mysql.bill.public.orders` is refused | Four parts would be a second full name of the table | Write it in three parts: `mysql.bill.orders`, `pg.shop.customers`, `ch.ev.sessions`. |
| `FATAL 3D000 database "demo" does not exist` at connect | The client named a place OtterStax does not have | Connect to `local` (`psql -h localhost -p 8817 -U demo local`), or to a connection (`mysql`, `pg.shop`). |
| `Otterbrix execution failed: database does not exist` | Query routed to local engine that doesn't know the external alias | Check the config file (`config.yaml` / `examples/demo/config_local.yaml`) `connections:` section has all three backends and restart the server — connections load only at startup. |
| Unknown S3 alias `demo_s3` from step 7/8/9 | The server's config did not register the alias | Check the `demo_s3` S3 entry under `connections:` in the config used by the server, then restart it. |
| Step 8 reports `CREATE EXTERNAL TABLE failed: … open_input …` for `promos.parquet` | The seed job may still be running, the file may be missing, or the object may be unreadable | Check the seed job and object as described below. |
| Step 9 hangs or returns `Cannot reach rustfs:9000` from inside the server | s3 endpoint mismatch (docker vs local config) | In docker mode the alias points at `demo-rustfs:9000`; in bench mode it points at `localhost:3206`. Use the matching config file (`config.yaml` vs `examples/demo/config_local.yaml`) and restart the server. |

### Step 8 cannot read `promos.parquet`

Older instructions recorded `External table is not registered` as the symptom
for step 8. Use the current error details to distinguish an unknown alias from
an `open_input` failure. Check the seed job's status and logs first. If
`init/s3/promos.parquet` under `examples/demo/` is missing, install `pyarrow` in the
environment used by the generator (`python3 -m pip install pyarrow`) and run
`python3 examples/demo/generate_data.py` from the repository root.

To re-seed the bucket from the repository root, run the following commands.
The seed job overwrites objects with matching keys.

```bash
docker compose -f examples/demo/compose.yml up -d demo-rustfs-init
docker compose -f examples/demo/compose.yml wait demo-rustfs-init
docker compose -f examples/demo/compose.yml logs demo-rustfs-init
```

Check that the job completed successfully and that `regions.csv` and
`promos.parquet` are present in `demo-bucket` in the RustFS console at
`http://localhost:3207/rustfs/console/` before retrying SQL.

## File reference

All paths relative to `examples/demo/`:

| Path | What |
|---|---|
| `compose.yml` | docker-compose for the demo stack (no-profile = backends + RustFS; `--profile full` = + otterstax) |
| `up.sh [--local]` | main entry point — full docker (default) or bench mode (`--local`) |
| `examples/demo/down.sh` | tear down containers and wipe volumes (incl. the RustFS bucket) |
| `examples/demo/run-queries.sh` | run all SQL steps on a fresh engine in either mode, stopping at the first error |
| `examples/demo/generate_data.py` | seeds init SQL files into `examples/demo/init/{mariadb,postgres,clickhouse}/`, s3 fixtures into `examples/demo/init/s3/`, AND kafka fixtures into `examples/demo/init/kafka/` |
| `examples/demo/init/{mariadb,postgres,clickhouse}/init.sql` | generated init scripts (gitignored) |
| `examples/demo/init/s3/{regions.csv,promos.parquet}` | generated S3 fixtures (gitignored); seeded into `demo-bucket` by the `demo-rustfs-init` compose service |
| `examples/demo/init/kafka/{orders_live,orders_intl}.ndjson` | generated Kafka fixtures (gitignored); produced onto topics by the `seed` helper in `examples/demo/kafka/lib/_common.sh` (rpk inside the demo-kafka container) |
| `examples/demo/kafka/` | the Kafka streaming act — step folders (`1_ingestion`…`6_teardown`), `examples/demo/kafka/run_all.sh`, `examples/demo/kafka/lib/` helpers; see `examples/demo/kafka/README.md` |
| `config.yaml` | single config file for docker mode (server settings + connections; docker-DNS hostnames `demo-mariadb`, `demo-rustfs`, …); mounted into the container, read at startup |
| `examples/demo/config_local.yaml` | single config file for bench/local mode (host-published ports: 3201/3202/3204/3206) — pass with `--config` |
| `examples/demo/sql/step_*.sql` | the actual demo queries — `step_7` / `step_8` load from s3, `step_9` JOINs and dumps back |
| `examples/demo/sql/cleanup.sql` | full reset of `local.otter` and its dependents, retaining the step-3a types |

## Hand-off note

When running this autonomously: run **steps in order**, report the row count or first row from each step's output, and stop on the first error. Do NOT skip step 3a/3b — later steps depend on `otter.warehouses` existing. Step 9 depends on `otter.regions` (step 7) and `otter.promos` (step 8); after step 9 completes, the JOIN result is at `s3://demo-bucket/exports/promos_by_region.csv` (browse it via the RustFS console at `http://localhost:3207/rustfs/console/`, login `rustfsadmin`/`rustfsadmin`).
