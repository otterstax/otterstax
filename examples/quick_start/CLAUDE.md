# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## What This Is

The public one-command quick start: `docker compose up` brings up 2 PostgreSQL +
1 MariaDB + 1 ClickHouse + RustFS (S3) + Kafka (redpanda) behind one OtterStax
server, and the user runs `psql` snippets over the PostgreSQL wire on
`localhost:8817`. `README.md` is the user-facing walkthrough (examples 1–9 and
K1–K4, each also a file in `sql/`); this file holds the maintainer facts it
does not.

## Invariants — keep them when editing

- **Nothing but `docker compose` on the run path.** Every input is a committed
  static file: `init/{postgres_shop,postgres_catalog,mariadb,clickhouse}/init.sql`,
  `init/s3/{regions.csv,product_costs.parquet,promotions.ndjson}`,
  `init/kafka/orders.ndjson`. No script runs at `up`.
- **`fixtures/generate.py` is maintainer-only.** It regenerates the `init/**`
  files from a fixed seed (needs `faker` + `pyarrow`); re-run it only to change the
  data, then commit the output. The row counts quoted in `README.md` come from it.
- **Every cross-backend JOIN key is a string** (`customer_id`, `product_id`,
  `order_id`). An int32 ⋈ int64 equi-JOIN silently returns zero rows in the
  engine (root `CLAUDE.md`, "JOIN-key type widths must agree"); string keys keep
  the examples independent of each backend's integer width.
- **Seed backend tables are read-only in the examples.** Example 9 creates,
  updates and drops an engine table (`qs.scratch`); `sql/cleanup.sql` drops only
  the engine database `qs` and the tables example 6 loaded from S3. Never add a
  `DROP` / `UPDATE` against `pgshop`, `pgcat`, `sales` or `web` — it would change
  the backend's real data, which persists (see below).
- **Flat schemas.** No ENUM / composite / tuple columns — those belong to
  `examples/demo/`.

## Stack facts

- Aliases (`config.yaml`, mounted onto the server's default config path):
  `pgshop` (db `shop`), `pgcat` (db `catalog`), `sales` (MariaDB, db `ops`),
  `web` (ClickHouse, db `analytics`), `qs_s3` (RustFS, bucket `quickstart-bucket`).
- Host ports: backends and RustFS on 3310–3317 (console
  `http://localhost:3317/rustfs/console/`, `rustfsadmin` / `rustfsadmin`), Kafka
  19094, OtterStax PostgreSQL wire 8817 only. 8817 is also the demo's port: the
  quick start and `examples/demo/ --profile full` cannot run at once.
- **Persistence:** every backend and RustFS has a named volume, so data —
  including a user's `UPDATE`s — survives `restart` and `down`/`up`; `down -v`
  rebuilds it from the committed `init/**` files. Kafka has no volume and is
  re-seeded by `qs-kafka-init` on every `up`. Engine tables (`qs.*`, example 6)
  and Kafka sources/streams (K1/K4) are stored inside the OtterStax container (no
  volume): they survive `restart` — sources and streams resume — and are lost on
  `down`/`up`.
- `qs-rustfs-init` runs on every `up` and `restart`: creates the bucket if missing, deletes the
  `exports/` prefix (example 7's `COPY … TO` output), and re-uploads `init/s3/`.
