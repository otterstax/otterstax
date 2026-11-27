# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## What This Is

The public one-command quick start: `docker compose up --wait` brings up 2 PostgreSQL +
1 MariaDB + 1 ClickHouse + RustFS (S3) + Kafka (redpanda) behind one OtterStax
server, and the user runs `psql` snippets over the PostgreSQL wire on
`localhost:8817`. `examples/quick_start/README.md` is the user-facing walkthrough (examples 1–9 and
K1–K4, each also a file in `examples/quick_start/sql/`); this file holds the maintainer facts it
does not.

## Invariants — keep them when editing

- **Nothing but `docker compose` on the run path.** Every input is a committed
  static file: `examples/quick_start/init/{postgres_shop,postgres_catalog,mariadb,clickhouse}/init.sql`,
  `examples/quick_start/init/s3/{regions.csv,product_costs.parquet,promotions.ndjson}`,
  `examples/quick_start/init/kafka/orders.ndjson`. Compose init services load the committed
  fixtures during startup.
- **`examples/quick_start/fixtures/generate.py` is maintainer-only.** It regenerates the `examples/quick_start/init/**`
  files from a fixed seed (needs `faker` + `pyarrow`); re-run it only to change the
  data, then commit the output. The row counts quoted in `examples/quick_start/README.md` come from it.
- **Every cross-backend JOIN key in these fixtures is a string** (`customer_id`,
  `product_id`, `order_id`). Keep the types consistent across the seed files.
- **Seed backend tables are read-only in the examples.** Example 9 creates,
  updates and drops an engine table (`local.qs.scratch`). `examples/quick_start/sql/cleanup.sql`
  resets all of `local.qs` with CASCADE, including user-created objects. To remove
  only the three demo tables, use the three DROP TABLE commands in the README. Never add a
  `DROP` / `UPDATE` against `pgshop`, `pgcat`, `sales` or `web` — it would change
  the backend's real data, which persists (see below).
- **Flat schemas.** No ENUM / composite / tuple columns — those belong to
  `examples/demo/`.

## Stack facts

- Aliases (`examples/quick_start/config.yaml`, mounted at `/otterstax/config.yaml`):
  the file sets `service.engine.data_dir` explicitly to `/tmp/test_collection_sql/base`.
  The aliases are `pgshop` (db `shop`), `pgcat` (db `catalog`), `sales` (MariaDB, db `ops`),
  `web` (ClickHouse, db `analytics`), `qs_s3` (RustFS, bucket `quickstart-bucket`).
- Host ports: backends and RustFS on 3310–3317 (console
  `http://localhost:3317/rustfs/console/`, `rustfsadmin` / `rustfsadmin`), Kafka
  19094, OtterStax PostgreSQL wire 8817 only. 8817 is also the demo's port: the
  quick start and `examples/demo/ --profile full` cannot run at once.
- **Persistence:** `stop` preserves containers and their data. `down` removes
  OtterStax's local catalog, tables and Kafka definitions and offsets, as well
  as the unmounted broker state. The named SQL backend and RustFS volumes
  survive `down`. `down -v` removes those volumes too, and the next `up`
  rebuilds the fixtures from `examples/quick_start/init/**`.
- OtterStax startup depends on both init jobs completing successfully. When run,
  `qs-rustfs-init` creates the bucket if needed, deletes `exports/` and
  overwrites the three source objects. `qs-kafka-init` recreates the two demo
  topics and produces the committed input. A seed failure fails startup.
  Use a targeted restart or the pause/resume commands to keep existing
  fixtures. See `examples/quick_start/README.md`.
