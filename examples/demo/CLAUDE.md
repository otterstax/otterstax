# Demo Runbook (for an LLM agent)

This document is a self-contained instruction set. Read it top-to-bottom and execute the commands as written. The goal is to bring up the federated-SQL demo and run six progressive SQL files against it.

Everything demo-related lives in this folder (`examples/demo/`). Paths below are given relative to the project root unless noted.

## What the demo shows

OtterStax is a federated SQL engine. The demo registers three real backends —

- **MariaDB** (`mysql.bill`) — orders, invoices (ACID transactions)
- **PostgreSQL** (`pg.shop`) — customers (with ENUM tier), products
- **ClickHouse** (`ch.ev`) — sessions (with nested struct columns, columnar OLAP)

— and runs six SQL files (`sql/step_1.sql` … `sql/step_6.sql`) that all go through OtterStax via the **PostgreSQL wire protocol on port 8817**. Each file is a single SQL statement that JOINs across two or three backends. Step 3 also defines local types and a local `otter.warehouses` table inside the engine itself.

Demo SQL uses the **3-part** qualifier form `<alias>.<db>.<tbl>` (e.g. `mysql.bill.orders`, `pg.shop.customers`, `ch.ev.sessions`). The parser promotes 3-part to its internal 4-part shape automatically, then `sql_gen::table_reference` emits backend-native qualifiers (`db.tbl` for MySQL/CH, `schema.tbl` for PG).

## Pick a mode

Two ways to run, pick **one**:

| Mode | When | Otterstax | Command |
|---|---|---|---|
| **Full docker** | One-shot demo, no local build | Built inside docker (image `otterstax_app`) | `examples/demo/up.sh` |
| **Bench** (local) | You already have a built `./build/server` and want to iterate on the engine | Local binary, you start it yourself | `examples/demo/up.sh --local` |

Full docker is the default; choose `--local` only if the user explicitly asks to iterate on otterstax itself.

## Prerequisites

- Docker daemon running. If the current user can't reach `/var/run/docker.sock`, the scripts auto-fall-back to `sudo -n docker`. If `sudo -n` is not configured, ask the user to either add themselves to the `docker` group or enable passwordless sudo.
- Python 3 with the `faker` package (for `examples/demo/generate_data.py`). The scripts auto-`source ../../.venv/bin/activate` if a project venv exists.
- `psql` client on the host (used to drive the demo SQL files).
- Ports 3201, 3202, 3204, 3205, 8085, 8815, 8816, 8817 free on the host.

## Mode A: full docker (`examples/demo/up.sh`)

```bash
examples/demo/up.sh
```

This does, in order:

1. `python examples/demo/generate_data.py` — writes `examples/demo/init/{mariadb,postgres,clickhouse}/init.sql` with seeded data (correlated IDs across backends, time windows aligned to demo predicates).
2. `docker compose -f examples/demo/compose.yml --profile full up -d --build` — starts MariaDB + PostgreSQL + ClickHouse + the `otterstax_app` container.
3. Polls each container's healthcheck (up to 3 minutes).
4. POSTs the three connection JSONs (`examples/demo/connections/connection_{mysql,pg,ch}.json`) to `http://localhost:8085`. Those JSONs use docker-DNS hostnames (`demo-mariadb` etc), which is what otterstax-in-docker needs.
5. Prints the `psql` invocation lines.

The OtterStax HTTP API and the three wire ports are host-published:

- HTTP (connection mgmt): `http://localhost:8085`
- PG wire: `localhost:8817` ← the demo uses this
- MySQL wire: `localhost:8816`
- FlightSQL: `localhost:8815`

## Mode B: bench (`examples/demo/up.sh --local`)

```bash
examples/demo/up.sh --local
# in another terminal:
./build/server --port-flight 8815 --port-mysql 8816 --port-postgres 8817 --port-http 8085
# then:
examples/demo/connections/add_connections.sh --local
```

Bench mode starts only the three backends (no `otterstax_app` container). You run the engine binary yourself. The `--local` flag selects the `_local` JSON variants, which point to `localhost:3201/3202/3204` (host-published backend ports), which is what a local server can reach.

## Run the demo steps

Once connections are registered, run each step against the PG wire:

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
```

Or all at once with `examples/demo/run-queries.sh`.

Each step is a single SELECT (or DDL/INSERT for 3a/3b). Expected output:

| Step | Backends touched | Demonstrates | Expected rows |
|---|---|---|---|
| 1 | mysql, pg | Cross-source JOIN + derived subquery with date filter | up to 10 |
| 2 | mysql, pg | Derived aggregates + HAVING alias + NOT LIKE | up to 20 |
| 3a | local (engine) | `CREATE TYPE` (ENUM + composite STRUCTs) + `CREATE TABLE otter.warehouses` | no rows, structural |
| 3b | local | INSERT 3 rows with `ROW(...)` composite values | 3 rows affected |
| 3c | local | `(struct).field` projection + `IS NULL` on STRUCT field | 1 row (TLV-1) |
| 3d | mysql, pg, ch | Three-backend JOIN with `(ch.props).channel` struct access | varies |
| 4 | pg (ENUM), local (STRUCT) | ENUM cast `'gold'::tier_t` + struct field JOIN key | a few rows |
| 5 | pg, ch, mysql | DISTINCT + nested `((s.props).geo).ip` | varies |
| 6 | mysql, pg, ch | CASE WHEN inside SUM + HAVING + LEFT JOIN inside subquery | a few rows |

## Re-running

`CREATE DATABASE otter` from step 3a fails the second time because the database persists. Before re-running steps 3a/3b, run:

```bash
psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/cleanup.sql
```

Or do a full reset with `examples/demo/down.sh` (wipes docker volumes — ClickHouse init only re-runs on a clean volume).

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
| `Otterbrix execution failed: database does not exist` | Query routed to local engine that doesn't know the external alias | Make sure all three connections were registered — re-run `examples/demo/connections/add_connections.sh` (or `--local` in bench mode). |

## File reference

All paths relative to `examples/demo/`:

| Path | What |
|---|---|
| `compose.yml` | docker-compose for the demo stack (no-profile = backends only; `--profile full` = + otterstax) |
| `up.sh [--local]` | main entry point — full docker (default) or bench mode (`--local`) |
| `down.sh` | tear down containers and wipe volumes |
| `run-queries.sh` | run all `sql/step_*.sql` files in order against the PG wire (works for both modes) |
| `generate_data.py` | seeds init SQL files into `init/` |
| `init/{mariadb,postgres,clickhouse}/init.sql` | generated init scripts (gitignored) |
| `connections/connection_*.json` | backend connection payloads (docker-DNS hostnames) |
| `connections/connection_*_local.json` | backend connection payloads (host-published ports for local binary) |
| `connections/add_connections.sh [--local]` | POST all three connections to otterstax HTTP API |
| `sql/step_*.sql` | the actual demo queries |
| `sql/cleanup.sql` | `DROP DATABASE IF EXISTS otter;` |

## Hand-off note

When running this autonomously: run **steps in order**, report the row count or first row from each step's output, and stop on the first error. Do NOT skip step 3a/3b — later steps depend on `otter.warehouses` existing.
