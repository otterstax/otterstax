# Quick Start Demo — Plan

> This file is the **build plan** for `examples/quick_start/`. It is written for a
> reviewer/implementer. The end-user-facing docs will be `README.md` (short, links
> into the wiki) and `WIKI.md` (the super-detailed walkthrough). Nothing here needs
> to survive into the shipped demo — delete or keep as design notes.

---

## 1. Goal & requirement mapping

A **zero-script**, copy-paste-friendly quick-start that shows OtterStax's *current*
federation capabilities. One `docker compose up`, then follow the wiki running
`psql` snippets. Kafka is explicitly **out of scope** (stage 2).

| Requirement | How this plan satisfies it |
|---|---|
| No `.sh`/python on the **run path** — only docker compose | `docker compose up` starts *everything* (backends + MinIO + s3 seed + otterstax). Init SQL and s3 objects are **pre-generated & committed** static files. A `fixtures/generate.py` exists only for maintainers to regenerate those static files; it is never needed to run the demo. |
| Predefined & pre-generated init files (db + s3) | Committed: `init/postgres_shop/init.sql`, `init/postgres_catalog/init.sql`, `init/mariadb/init.sql`, `init/clickhouse/init.sql`, `init/s3/{regions.csv,product_costs.parquet,promotions.ndjson}`. |
| Small data sets: 500–1000 rows, 2–5 columns | customers 800×4, products 500×4, orders 1000×5, pageviews 1000×4 (see §4). |
| **≥3 files on s3** (local dropped per updated scope) | s3: `regions.csv` (csv), `product_costs.parquet` (parquet), `promotions.ndjson` (ndjson) — **all three formats**. No local-file external table. |
| PSQL driver (like demo) | All examples run over the **PostgreSQL wire on `localhost:8817`** via `psql`. |
| **UPDATE examples + DROP tables** (new) | §8 examples 6 & 8: `UPDATE` on backend seed data + a full `CREATE→INSERT→UPDATE→DELETE→DROP` lifecycle on a scratch backend table, `UPDATE`/`DELETE` on s3-loaded engine tables, and `DROP TABLE`/`DROP DATABASE` in `cleanup.sql`. Verified supported by `test_flightsql_client_mysql_backend_mutable.py` (MySQL backend CRUD) and `test_pg_client_pg_backend.py` (PG `UPDATE`). |
| **Data intact after compose restart** (new) | Named volumes on every backend **and** MinIO (§6) → warm restart preserves data (incl. your `UPDATE`s). Committed init/s3 files → a full `down -v` cold rebuild reconstructs the same data from init files + re-seeds s3. See §9a for the warm-vs-cold matrix and the engine-table caveat. |
| Super-detailed wiki | `WIKI.md`: structure, per-backend data dictionary, start, per-backend health checks, MinIO console verification, persistence behavior, 10 progressive query examples as copyable snippets, teardown, troubleshooting. |
| ≥2 PG, 1 MySQL/MariaDB, 1 ClickHouse | 2 postgres services (`pgshop`, `pgcat`) + 1 mariadb (`sales`) + 1 clickhouse (`web`). |
| Federation: PG + ClickHouse + MariaDB + s3(in otterbrix) | Example 10 (the finale) joins all four live backends + an s3-loaded otterbrix table in one statement. |
| Super easy to run all examples from the wiki | Every example is a fenced, copyable `psql` snippet **and** a matching `sql/NN_*.sql` file (`psql … -f sql/NN_*.sql`). |

**Key-type decision (important):** every cross-backend JOIN key is a **string**
(`customer_id='C0001'`, `product_id='P0007'`, `order_id='O00042'`). This sidesteps
the int32/int64 silent-zero-row JOIN trap documented in `FIX_JOIN.md` /
top-level `CLAUDE.md`. `amount`/`price`/`unit_cost` are doubles; timestamps are
`DateTime`. No ENUM/struct/tuple columns in quick-start (those live in the full
`examples/demo/`); quick-start stays deliberately flat and robust.

---

## 2. What I reused from the existing demo/tests

- **Compose shape** — mirror `examples/demo/compose.yml`: per-backend healthchecks,
  a `*-minio-init` one-shot that does `mc mb` + `mc cp --recursive /fixtures/`, and
  the otterstax service built from the repo `Dockerfile` with `config.yaml` mounted
  at `/app/build/Release/config.yaml`. **Differences:** (a) no `--profile` gate —
  otterstax comes up by default so a single `docker compose up` is the whole demo;
  (b) **named data volumes** on every backend + MinIO for persistence (the demo is
  intentionally ephemeral; quick-start persists); (c) no Kafka/redpanda service.
- **Config shape** — mirror `examples/demo/config.yaml` / `tests/scripts/config.yaml`;
  the two-PG case is exactly the two-MariaDB pattern already in `tests/scripts/config.yaml`.
- **External-table SQL** — `CREATE EXTERNAL TABLE` / `COPY … TO` surface exactly as in
  `examples/demo/sql/step_7..9.sql` and `tests/external_helpers.py`.
- **Mutations** — `UPDATE`/`DELETE`/`DROP`/`CREATE TABLE` against backends are proven
  by `test_flightsql_client_mysql_backend_mutable.py` (full lifecycle on a MySQL
  backend: CREATE→INSERT→UPDATE→DELETE→CREATE INDEX→DROP INDEX→DROP TABLE) and
  `test_pg_client_pg_backend.py` / `test_mysql_client_mysql_backend.py` (`UPDATE … SET`).

---

## 3. System structure (what the wiki will diagram)

```text
                       ┌─────────────────────────── OtterStax ───────────────────────────┐
   psql :8817  ─────►  │  PostgreSQL wire (8817)   →  Scheduler → Worker → Catalog        │
                       │                                    │                              │
                       │        federated dispatch ─────────┼───────────────┐             │
                       └────────────────────────────────────┼───────────────┼─────────────┘
                                     │            │          │               │
              alias pgshop           alias pgcat  alias sales  alias web    alias qs_s3
        ┌──────────────────┐  ┌──────────────────┐  ┌────────────┐  ┌────────────┐  ┌──────────────┐
        │ PostgreSQL  #1   │  │ PostgreSQL  #2   │  │  MariaDB   │  │ ClickHouse │  │    MinIO     │
        │ db=shop          │  │ db=catalog       │  │  db=ops    │  │ db=analytics│  │ quickstart-  │
        │ customers (800)  │  │ products (500)   │  │ orders(1000)│  │ pageviews  │  │ bucket:      │
        │                  │  │                  │  │            │  │  (1000)    │  │  regions.csv │
        └──────────────────┘  └──────────────────┘  └────────────┘  └────────────┘  │  product_    │
              │ vol                 │ vol               │ vol            │ vol        │  costs.parquet│
        qs_pg_shop_data       qs_pg_catalog_data   qs_mariadb_data  qs_ch_data       │  promotions. │
                                                                                     │  ndjson      │
                                                                        qs_minio_data┤ (vol)        │
                                                                                     └──────────────┘
```

Backends (aliases used in SQL):

| Alias | Engine | db | Table(s) | Rows | Role |
|---|---|---|---|---|---|
| `pgshop` | PostgreSQL #1 | `shop` (schema `public`) | `customers` | 800 | who bought |
| `pgcat` | PostgreSQL #2 | `catalog` (schema `public`) | `products` | 500 | what's for sale |
| `sales` | MariaDB | `ops` | `orders` | 1000 | transactions |
| `web` | ClickHouse | `analytics` | `pageviews` | 1000 | OLAP clickstream |
| `qs_s3` | MinIO (s3) | bucket `quickstart-bucket` | `regions.csv`, `product_costs.parquet`, `promotions.ndjson` | 12 / 500 / 60 | external files |

---

## 4. Data model (all committed as static init files)

Join graph (all keys are **strings**):

```text
customers.customer_id ─┬─ orders.customer_id
                       └─ pageviews.customer_id
products.product_id  ──┬─ orders.product_id
                       ├─ pageviews.product_id
                       ├─ product_costs.product_id   (s3 parquet)
                       └─ promotions.product_id       (s3 ndjson)
customers.country    ──── regions.country            (s3 csv)
```

### PostgreSQL #1 — `pgshop.shop.customers` (800 rows, 4 cols)
| col | type | notes |
|---|---|---|
| `customer_id` | `TEXT` PK | `'C0001'..'C0800'` |
| `name` | `TEXT` | faker name |
| `country` | `CHAR(2)` | one of ~12 ISO codes, joins to `regions` |
| `tier` | `TEXT` | `bronze`/`silver`/`gold` (plain text, no ENUM) — the column example 6 `UPDATE`s |

init.sql: `CREATE DATABASE shop; \c shop; CREATE TABLE public.customers(...); INSERT …`.
Healthcheck target db = `shop`.

### PostgreSQL #2 — `pgcat.catalog.products` (500 rows, 4 cols)
| col | type | notes |
|---|---|---|
| `product_id` | `TEXT` PK | `'P0001'..'P0500'` |
| `name` | `TEXT` | faker two words |
| `category` | `TEXT` | 8 categories |
| `price` | `DOUBLE PRECISION` | 9.99–499.99 |

init.sql: `CREATE DATABASE catalog; \c catalog; CREATE TABLE public.products(...); INSERT …`.

### MariaDB — `sales.ops.orders` (1000 rows, 5 cols)
| col | type | notes |
|---|---|---|
| `order_id` | `VARCHAR(8)` PK | `'O00001'..'O01000'` |
| `customer_id` | `VARCHAR(8)` | FK→customers |
| `product_id` | `VARCHAR(8)` | FK→products |
| `amount` | `DOUBLE` | 10–500 |
| `status` | `VARCHAR(16)` | paid/shipped/pending/refunded (skew to paid) |

init.sql: `CREATE DATABASE IF NOT EXISTS ops; USE ops; CREATE TABLE orders(...); INSERT …`.
(compose `MYSQL_DATABASE: ops`, user `demo`/`demo`.)

### ClickHouse — `web.analytics.pageviews` (1000 rows, 4 cols)
| col | type | notes |
|---|---|---|
| `customer_id` | `String` | FK→customers |
| `product_id` | `String` | FK→products |
| `channel` | `String` | web/mobile/email/social |
| `event_ts` | `DateTime` | within a ~30-day window |

init.sql: `CREATE DATABASE IF NOT EXISTS analytics; CREATE TABLE analytics.pageviews(...) ENGINE=MergeTree ORDER BY (customer_id,event_ts); INSERT …`.
(Flat columns only — no `Tuple`, to keep it robust. No `UPDATE` demoed against CH —
CH mutations are non-standard `ALTER … UPDATE`; mutation examples stay on PG/MariaDB + engine.)

### S3 objects (bucket `quickstart-bucket`, seeded by minio-init)
- `regions.csv` — `country,region_name` — ~12 rows (one per country code used above).
- `product_costs.parquet` — `product_id (utf8), unit_cost (float64)` — 500 rows (one per product; `unit_cost` < `price`). **string product_id** → safe JOIN.
- `promotions.ndjson` — `{product_id, promo_code, discount_pct}` — ~60 rows (promos on a subset of products).

---

## 5. File tree to create

```text
examples/quick_start/
├── quick_start.md            ← THIS plan (design notes)
├── README.md                 ← short; "run docker compose up, then open WIKI.md"
├── WIKI.md                   ← the super-detailed walkthrough (§9)
├── compose.yml               ← single-command stack + named volumes (§6)
├── config.yaml               ← 2 pg + 1 mysql + 1 ch + 1 s3 (docker-DNS names)
├── fixtures/
│   └── generate.py           ← maintainer-only; regenerates everything under init/
├── sql/
│   ├── 01_select.sql
│   ├── 02_aggregate.sql
│   ├── 03_join_2backend.sql
│   ├── 04_join_3backend.sql
│   ├── 05_complex_join.sql
│   ├── 06_mutate_backend.sql      (UPDATE seed + CREATE/INSERT/UPDATE/DELETE/DROP lifecycle)
│   ├── 07_external_create.sql     (csv + parquet + ndjson → qs.*)
│   ├── 08_mutate_engine.sql       (UPDATE/DELETE on s3-loaded engine tables)
│   ├── 09_external_copy.sql       (COPY JOIN → s3)
│   ├── 10_federation_finale.sql   (PG+PG+MariaDB+CH+s3)
│   └── cleanup.sql                (DROP TABLE qs.*; DROP DATABASE qs)
└── init/
    ├── postgres_shop/init.sql        (committed, static)
    ├── postgres_catalog/init.sql     (committed, static)
    ├── mariadb/init.sql              (committed, static)
    ├── clickhouse/init.sql           (committed, static)
    └── s3/regions.csv                (committed, static)
        s3/product_costs.parquet      (committed, static)
        s3/promotions.ndjson          (committed, static)
```

> Note: unlike `examples/demo/` (which `.gitignore`s generated `init/`), quick-start
> **commits** every init/s3 file so `docker compose up` works with zero prep and a
> cold `down -v` rebuild reconstructs identical data. Confirm the repo `.gitignore`
> doesn't exclude `examples/quick_start/init/**` (add a negation if it does).

---

## 6. compose.yml design

Ports chosen to not collide with `examples/demo/` (3201–3207) or `compose.test.yml`.
Wire ports reuse 8815/8816/8817 (only one stack runs at a time).

| Service | Image | Host port(s) | Data volume | Notes |
|---|---|---|---|---|
| `qs-postgres-shop` | `postgres:15` | `3311:5432` | `qs_pg_shop_data:/var/lib/postgresql/data` | init.sql makes `shop`; healthcheck `pg_isready -d shop`. |
| `qs-postgres-catalog` | `postgres:15` | `3312:5432` | `qs_pg_catalog_data:/var/lib/postgresql/data` | init.sql makes `catalog`; healthcheck `pg_isready -d catalog`. |
| `qs-mariadb` | `mariadb:latest` | `3310:3306` | `qs_mariadb_data:/var/lib/mysql` | `MYSQL_DATABASE=ops`, user demo/demo. |
| `qs-clickhouse` | `clickhouse/clickhouse-server:23.8` | `3313:9000`,`3314:8123` | `qs_ch_data:/var/lib/clickhouse` | `CLICKHOUSE_DB=analytics`, user demo/demo. |
| `qs-minio` | `minio/minio:latest` | `3316:9000`,`3317:9001` | `qs_minio_data:/data` | root minioadmin/minioadmin. |
| `qs-minio-init` | `minio/mc:latest` | — | — | one-shot: `mc mb --ignore-existing quickstart-bucket` + `mc cp --recursive /fixtures/` (mounts `./init/s3:/fixtures:ro`). Runs every `up`; idempotent — re-seeds any object the volume lost. |
| `qs-otterstax` | build `../../Dockerfile` | `8815-8817` | — | depends_on all backends healthy; mounts `./config.yaml`→`/app/build/Release/config.yaml`. TCP healthcheck on 8817. **No `/fixtures` mount** (local-file path removed). |

All on a `quickstart_net` bridge. `qs-otterstax` `depends_on` each backend
`condition: service_healthy` (minio-init is not health-gated; otterstax retries s3
via `connection_retry`). No profiles → `docker compose up` brings the whole thing up.

Top-level `volumes:` declares the five named volumes above.

---

## 7. config.yaml design (docker-DNS hostnames)

```yaml
service:
  flight_sql: { host: "0.0.0.0", port: 8815 }
  mysql:      { port: 8816 }
  postgres:   { port: 8817 }
  connection_retry: { max_attempts: 15, delay_ms: 2000 }

connections:
  postgresql:
    - { alias: pgshop, host: qs-postgres-shop,    port: "5432", username: demo, password: demo, database: shop,    schema: public, table: "" }
    - { alias: pgcat,  host: qs-postgres-catalog, port: "5432", username: demo, password: demo, database: catalog, schema: public, table: "" }
  mysql:
    - { alias: sales,  host: qs-mariadb,          port: "3306", username: demo, password: demo, database: ops,     table: "" }
  clickhouse:
    - { alias: web,    host: qs-clickhouse,       port: "9000", username: demo, password: demo, database: analytics, table: "" }
  s3:
    - { alias: qs_s3,  access_key: minioadmin, secret_key: minioadmin, region: us-east-1, endpoint: qs-minio:9000 }
```

---

## 8. The 10 query examples (draft SQL — all over psql :8817)

Progressive, simple → advanced. Each becomes a `sql/NN_*.sql` file **and** a copyable
snippet in the wiki. Qualifier form: `alias.db.table` (3-part; parser fills PG schema).

### Wiki presentation contract (how EACH example renders in WIKI.md)

Every example in the wiki shows **three** things, in this order — so a reader can
copy one block, paste it, and immediately see the same result:

1. **One-line purpose** + backends touched.
2. **A full, copy-paste psql command** (not just the bare SQL):
   - **Single-statement** examples (1–5, 9, 10) → inline `-c` form, self-contained:
     ```bash
     psql -h localhost -p 8817 -U demo demo -c "SELECT ... ;"
     ```
   - **Multi-statement** examples (6, 7, 8, cleanup) → file form:
     ```bash
     psql -h localhost -p 8817 -U demo demo -f examples/quick_start/sql/06_mutate_backend.sql
     ```
3. **A small expected-result block** — the real `psql` table, **capped tiny**: every
   query keeps `LIMIT 10` (or is naturally ≤10 rows), and the wiki prints only the
   **first ~5 rows** + a `... (N rows)` footer so no output is a wall of text.

> Expected-result blocks below are **illustrative placeholders** — the exact rows/
> numbers get replaced with the actual `psql` output captured during the live-run
> step (§11.4). The *shape* (columns, ~row count) is what's fixed here.

The draft SQL for each example follows (LIMITs already trimmed to keep results small).

1. **Simple select (1 backend: PG#1)** — `sql/01_select.sql` — *fully worked template:*

   SQL:
   ```sql
   SELECT customer_id, name, country, tier
   FROM   pgshop.shop.customers
   ORDER BY customer_id
   LIMIT 10;
   ```
   Copy-paste command (this is the form the wiki shows for single-statement examples):
   ```bash
   psql -h localhost -p 8817 -U demo demo \
     -c "SELECT customer_id, name, country, tier FROM pgshop.shop.customers ORDER BY customer_id LIMIT 10;"
   ```
   Expected (first 5 of 10 rows — illustrative):
   ```text
    customer_id |      name       | country | tier
   -------------+-----------------+---------+--------
    C0001       | Allison Hill    | US      | gold
    C0002       | Robert Johnson  | DE      | gold
    C0003       | Maria Garcia    | IL      | bronze
    C0004       | James Smith     | FR      | silver
    C0005       | Linda Williams  | GB      | bronze
   ... (10 rows)
   ```
2. **Simple aggregate (1 backend)** — `sql/02_aggregate.sql`
   ```sql
   SELECT status, COUNT(*) AS n, ROUND(SUM(amount),2) AS total
   FROM   sales.ops.orders
   GROUP BY status
   ORDER BY total DESC;
   ```
3. **2-backend JOIN (MariaDB ⋈ PG#1)** — `sql/03_join_2backend.sql` — top-spending customers.
   ```sql
   SELECT c.name, c.country, COUNT(*) AS orders_cnt, ROUND(SUM(o.amount),2) AS spend
   FROM   sales.ops.orders o
   JOIN   pgshop.shop.customers c ON c.customer_id = o.customer_id
   WHERE  o.status = 'paid'
   GROUP BY c.name, c.country
   ORDER BY spend DESC
   LIMIT 10;
   ```
4. **3-backend JOIN — both PGs + MariaDB** — `sql/04_join_3backend.sql` — revenue by category × country (exercises the two independent PG backends in one statement).
   ```sql
   SELECT p.category, c.country, ROUND(SUM(o.amount),2) AS revenue
   FROM   sales.ops.orders o
   JOIN   pgshop.shop.customers c  ON c.customer_id = o.customer_id
   JOIN   pgcat.catalog.products p ON p.product_id  = o.product_id
   WHERE  o.status IN ('paid','shipped')
   GROUP BY p.category, c.country
   ORDER BY revenue DESC
   LIMIT 10;
   ```
5. **Complex 4-backend JOIN — + ClickHouse** — `sql/05_complex_join.sql` — clickstream (CH) ⋈ orders ⋈ customers ⋈ products, aggregated by channel.
   ```sql
   SELECT v.channel, p.category,
          COUNT(DISTINCT o.order_id) AS orders_cnt,
          ROUND(SUM(o.amount),2)     AS revenue
   FROM   web.analytics.pageviews v
   JOIN   sales.ops.orders     o  ON o.customer_id = v.customer_id
                                 AND o.product_id  = v.product_id
   JOIN   pgshop.shop.customers c ON c.customer_id = o.customer_id
   JOIN   pgcat.catalog.products p ON p.product_id = o.product_id
   WHERE  o.status = 'paid'
   GROUP BY v.channel, p.category
   ORDER BY revenue DESC
   LIMIT 10;
   ```
6. **Mutations on a backend — UPDATE / DELETE / DROP** — `sql/06_mutate_backend.sql`
   Two parts. **(a)** UPDATE seed data on PG#1 (this write lands in PostgreSQL and
   **persists across a warm restart** — see §9a):
   ```sql
   -- promote two customers; re-SELECT shows the change
   UPDATE pgshop.shop.customers SET tier = 'gold'
   WHERE  customer_id IN ('C0001','C0002');

   SELECT customer_id, name, tier FROM pgshop.shop.customers
   WHERE  customer_id IN ('C0001','C0002');
   ```
   **(b)** Full lifecycle on a **scratch** MariaDB table (non-destructive — created
   and dropped in the same file; mirrors `test_flightsql_client_mysql_backend_mutable.py`):
   ```sql
   CREATE TABLE sales.ops.scratch (id VARCHAR(8), note VARCHAR(32), qty INT);
   INSERT INTO sales.ops.scratch (id, note, qty) VALUES
     ('S1','alpha',1), ('S2','beta',2), ('S3','gamma',3);
   UPDATE sales.ops.scratch SET qty = qty * 10 WHERE id = 'S1';
   DELETE FROM sales.ops.scratch WHERE id = 'S3';
   SELECT * FROM sales.ops.scratch ORDER BY id;   -- S1 qty=10, S2 qty=2
   DROP TABLE sales.ops.scratch;
   ```
7. **External tables — load 3 s3 files (csv/parquet/ndjson) into otterbrix** — `sql/07_external_create.sql`
   ```sql
   CREATE DATABASE qs;

   CREATE EXTERNAL TABLE qs.regions
       WITH (s3_alias='qs_s3', location='s3://quickstart-bucket/regions.csv', format='csv');
   CREATE EXTERNAL TABLE qs.product_costs
       WITH (s3_alias='qs_s3', location='s3://quickstart-bucket/product_costs.parquet', format='parquet');
   CREATE EXTERNAL TABLE qs.promotions
       WITH (s3_alias='qs_s3', location='s3://quickstart-bucket/promotions.ndjson', format='ndjson');

   -- prove all three loaded, and JOIN an s3-in-otterbrix dimension to a live backend
   SELECT r.region_name, COUNT(*) AS customers
   FROM   pgshop.shop.customers c
   JOIN   qs.regions r ON r.country = c.country
   GROUP BY r.region_name
   ORDER BY customers DESC;
   ```
8. **Mutations on engine (otterbrix-internal) tables — UPDATE / DELETE** — `sql/08_mutate_engine.sql`
   The s3-loaded tables behave like any `CREATE TABLE` table and accept writes:
   ```sql
   -- bump a cost, drop one promo row; re-SELECT to confirm
   UPDATE qs.product_costs SET unit_cost = ROUND(unit_cost * 1.10, 2)
   WHERE  product_id = 'P0001';
   DELETE FROM qs.promotions WHERE discount_pct < 5;
   SELECT * FROM qs.product_costs WHERE product_id = 'P0001';
   ```
   > Verify live: if the engine rejects `UPDATE`/`DELETE` on a loaded external table,
   > fall back to demonstrating engine mutation on a plain `CREATE TABLE qs.tmp(...)`
   > and keep the backend `UPDATE` (example 6) as the primary mutation proof.
9. **Export a JOIN back to s3 (COPY … TO)** — `sql/09_external_copy.sql`
   ```sql
   COPY (
       SELECT pc.product_id, pc.unit_cost, pr.promo_code, pr.discount_pct
       FROM   qs.product_costs pc
       JOIN   qs.promotions   pr ON pr.product_id = pc.product_id
       ORDER  BY pc.product_id
   ) TO 's3://quickstart-bucket/exports/promo_costs.csv'
       WITH (s3_alias='qs_s3', format='csv');
   ```
10. **Federation finale — PG#1 + PG#2 + MariaDB + ClickHouse + s3(in otterbrix)** — `sql/10_federation_finale.sql`
    Margin per category × country × channel, combining all four live backends **and**
    the s3-loaded `qs.product_costs`.
    ```sql
    SELECT c.country, p.category, v.channel,
           COUNT(DISTINCT o.order_id)               AS orders_cnt,
           ROUND(SUM(o.amount), 2)                  AS revenue,
           ROUND(SUM(o.amount - pcst.unit_cost), 2) AS gross_margin
    FROM   sales.ops.orders       o                             -- MariaDB
    JOIN   pgshop.shop.customers  c    ON c.customer_id = o.customer_id   -- PostgreSQL #1
    JOIN   pgcat.catalog.products p    ON p.product_id  = o.product_id    -- PostgreSQL #2
    JOIN   web.analytics.pageviews v   ON v.customer_id = o.customer_id
                                      AND v.product_id  = o.product_id     -- ClickHouse
    JOIN   qs.product_costs       pcst ON pcst.product_id = o.product_id   -- s3→otterbrix
    WHERE  o.status = 'paid'
    GROUP BY c.country, p.category, v.channel
    ORDER BY gross_margin DESC
    LIMIT 10;
    ```

`sql/cleanup.sql` — the **DROP tables** demonstration + re-run enabler:
```sql
DROP TABLE IF EXISTS qs.regions;
DROP TABLE IF EXISTS qs.product_costs;
DROP TABLE IF EXISTS qs.promotions;
DROP DATABASE IF EXISTS qs;
```
Examples 1–5 are stateless/re-runnable; 7 must run before 8/9/10 (it creates `qs.*`).

**During implementation each example is run live and the SQL adjusted to whatever the
engine actually accepts** (`ROUND`, multi-key JOIN, `DISTINCT`, engine-table `UPDATE`)
— the demo's step SQL + the mutable test are the authority on supported syntax;
anything rejected gets simplified and the wiki records the real observed output.

---

## 9. WIKI.md outline (the "super-detailed, helpful wiki")

1. **What is this** — one-paragraph framing + the §3 structure diagram.
2. **System map** — the backend/alias/data table (§3) + s3 inventory + the §4 join-graph.
3. **Prerequisites** — Docker; `psql` client; free ports (list); `mysql` &
   `clickhouse-client` optional (for direct verification).
4. **Start it** — `cd examples/quick_start && docker compose up -d --build`, then
   `docker compose ps` to watch health.
5. **Verify the data is loaded** (copyable, per backend):
   - PG#1: `psql -h localhost -p 3311 -U demo shop -c 'SELECT COUNT(*) FROM customers;'`
   - PG#2: `psql -h localhost -p 3312 -U demo catalog -c 'SELECT COUNT(*) FROM products;'`
   - MariaDB: `mysql -h 127.0.0.1 -P 3310 -u demo -pdemo ops -e 'SELECT COUNT(*) FROM orders;'`
   - ClickHouse: `clickhouse-client --host 127.0.0.1 --port 3313 -u demo --password demo -q 'SELECT count() FROM analytics.pageviews'` (or HTTP `curl :3314`).
6. **Verify the s3 files** — MinIO console `http://localhost:3317`
   (minioadmin/minioadmin) → `quickstart-bucket` → `regions.csv`,
   `product_costs.parquet`, `promotions.ndjson`; after example 9, `exports/promo_costs.csv`.
7. **Connect to OtterStax** — `psql -h localhost -p 8817 -U demo demo`.
8. **The 10 examples** — each rendered per the **§8 presentation contract**: 1-line
   purpose + backends touched → **full copy-paste psql command** (inline `-c` for
   single-statement 1–5/9/10; `-f sql/NN_*.sql` for multi-statement 6/7/8) → **small
   expected-result block** (first ~5 rows + `... (N rows)`; every query `LIMIT 10`).
9. **(9a) Persistence — what survives a restart** (the new dedicated section; see below).
10. **Re-running / cleanup** — `psql … -f sql/cleanup.sql` before re-doing 7–10.
11. **Teardown** — `docker compose down` (keep data) vs `down -v` (wipe + fresh rebuild).
12. **Troubleshooting** — connection refused → still-waiting healthcheck; external table
    not registered → wrong s3 alias/config; empty JOIN → key-width note + FIX_JOIN.md;
    port clash with the demo stack; "my `qs.*` tables vanished after restart" → §9a.

### §9a Persistence matrix (wiki content)

| Action | Backends (customers/products/orders/pageviews) + your `UPDATE`s | s3 objects | otterbrix engine tables (`qs.*`, scratch) |
|---|---|---|---|
| `docker compose restart` / `stop`+`start` | **kept** (named volumes) | **kept** (minio volume) | **lost** (engine is in-memory; container restarted) |
| `docker compose down` then `up` (no `-v`) | **kept** (volumes persist) | **kept**; minio-init re-runs idempotently | **lost** — re-run example 7 |
| `docker compose down -v` then `up` | **rebuilt from `init/**` files** (identical data) | **re-seeded from `init/s3/**`** | absent until you re-run example 7 |

**Takeaway for the user:** *source* data (all four backends + the three s3 files) is
**intact after any restart** — preserved by named volumes on a warm restart, and
faithfully reconstructed from the committed init/s3 files on a cold `down -v`. The only
thing that needs re-running after an otterstax restart is the `CREATE EXTERNAL TABLE`
step (example 7), because engine-loaded tables live in the server process, not on a
backend. This is called out explicitly so "my `qs.*` disappeared" is never a surprise.

README.md is a ~20-line front door: one-command start + links to `WIKI.md` and its
example sections.

---

## 10. Decisions / defaults I'm taking (flag if you disagree)

- **One-command up (no `--profile`)**: otterstax starts by default.
- **Named volumes on every backend + MinIO** → data (incl. `UPDATE`s) persists across
  warm restart; committed init/s3 files reconstruct it on a cold `down -v`. (Demo is
  deliberately ephemeral and gitignores init; quick-start is the opposite.)
- **String join keys** everywhere (robust vs. FIX_JOIN int-width trap). No struct/ENUM.
- **Mutations demoed on PG/MariaDB + engine tables only** — not ClickHouse (its `UPDATE`
  is non-standard `ALTER … UPDATE`).
- **DROP shown on scratch + engine tables** (never on seed backend tables — a `DROP` there
  would really drop the source table).
- **Local-file external table removed** — 3 s3 files (csv/parquet/ndjson) cover the
  external-source story; the `/fixtures` mount and `targets.csv` are gone.
- **No `config_local.yaml` / `--local`** — compose-only.
- **Reuse wire ports 8815–8817** — can't run demo + quick-start simultaneously
  (documented in troubleshooting); backend host ports are unique (33xx).
- **`fixtures/generate.py` kept** as the reproducible source of the static fixtures, but
  the run path never invokes it (needs `faker` + `pyarrow`).

## 11. Build & verification checklist (implementation phase)

1. Write `fixtures/generate.py`; run once to emit all `init/**` static files; commit them.
2. Write `config.yaml`, `compose.yml` (incl. named volumes), `sql/*.sql`.
3. `docker compose up -d --build`; wait for `docker compose ps` all healthy.
4. Run each `sql/NN_*.sql` in order over `psql :8817`; capture actual row counts; fix any
   SQL the engine rejects (esp. example 8 engine `UPDATE`/`DELETE`); record expected
   output in the wiki.
5. Verify the s3 export object appears in MinIO console after example 9.
6. **Persistence check:** run example 6a (`UPDATE tier`), then `docker compose restart
   qs-postgres-shop` (or full `down` + `up`), reconnect, confirm `C0001/C0002` are still
   `gold` and all counts intact; then `down -v` + `up` and confirm data is rebuilt from
   init/s3 files. Record this as the §9a evidence.
7. Write `README.md` + `WIKI.md` with the *actual* observed outputs.
```
