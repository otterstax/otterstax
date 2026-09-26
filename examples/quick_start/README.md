# OtterStax Quick Start

A one-command federated-SQL demo. `docker compose up` brings up **2 PostgreSQL + 1
MariaDB + 1 ClickHouse + MinIO (S3) + Kafka** behind a single OtterStax server; you run
SQL that joins across all of them with `psql`. All data is pre-seeded from committed
files — no scripts.

The 9 core examples cover federated SQL over the databases + S3; the
[**Kafka (streaming)**](#kafka-streaming) section adds four streaming examples on top.

## Run it

```bash
cd examples/quick_start
docker compose up -d
```

First run builds/reuses the `otterstax_app:latest` image and seeds every backend from
`init/`. Wait until all report `healthy` (~30–60 s):

```bash
docker compose ps
```

```text
NAME                  STATUS
qs-clickhouse         Up (healthy)
qs-kafka              Up (healthy)
qs-mariadb            Up (healthy)
qs-minio              Up (healthy)
qs-otterstax          Up (healthy)
qs-postgres-catalog   Up (healthy)
qs-postgres-shop      Up (healthy)
```

Connect to OtterStax (no password):

```bash
psql -h localhost -p 8817 -U demo demo
```

> Uses the PostgreSQL wire on port **8817** — the same port as `examples/demo/`, so don't
> run both at once (`docker stop demo-otterstax` if needed).

## The 30-second tour

```bash
# federation across MariaDB + both PostgreSQL backends
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT p.category, c.country, SUM(o.amount) AS revenue
      FROM sales.ops.orders o
      JOIN pgshop.shop.customers c  ON c.customer_id = o.customer_id
      JOIN pgcat.catalog.products p ON p.product_id  = o.product_id
      WHERE o.status IN ('paid','shipped')
      GROUP BY p.category, c.country ORDER BY revenue DESC LIMIT 5;"
```

Then work through the examples below:

| # | Shows | # | Shows |
|---|-------|---|-------|
| [1](#example-1--simple-select-with-filters-1-backend-postgresql-2) | simple SELECT + filters | [6](#example-6--load-3-s3-files-into-the-engine-csv--parquet--ndjson) | load 3 S3 files (csv/parquet/ndjson) |
| [2](#example-2--aggregate-1-backend-mariadb) | aggregate | [7](#example-7--export-a-join-back-to-s3-copy--to) | COPY a JOIN back to S3 |
| [3](#example-3--2-backend-join-mariadb--postgresql-1) | 2-backend JOIN | [8](#example-8--federation-finale-pg-1--pg-2--mariadb--clickhouse--s3) | **federation finale** — all 4 backends + S3 |
| [4](#example-4--3-backend-join-both-postgresql-backends--mariadb) | 3-backend JOIN (2×PG + MariaDB) | [9](#example-9--mutations-insert--update--drop) | INSERT / UPDATE / DROP |
| [5](#example-5--complex-4-backend-join--clickhouse) | 4-backend JOIN (+ ClickHouse) | [K1–K4](#kafka-streaming) | Kafka: source → join → produce → stream |

---

## What you get

```text
                                psql  →  localhost:8817 (PostgreSQL wire)
                                              │
                                       ┌──────┴───────┐
                                       │  OtterStax   │   federated SQL engine
                                       └──────┬───────┘
        ┌──────────────┬───────────────┬──────┴───────┬───────────────┐
   alias pgshop   alias pgcat     alias sales     alias web       alias qs_s3
   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────┐
   │PostgreSQL│   │PostgreSQL│   │ MariaDB  │   │ClickHouse│   │    MinIO     │
   │ db=shop  │   │db=catalog│   │ db=ops   │   │db=       │   │quickstart-   │
   │customers │   │ products │   │ orders   │   │analytics │   │bucket        │
   │  (800)   │   │  (500)   │   │ (1000)   │   │pageviews │   │ regions.csv  │
   └──────────┘   └──────────┘   └──────────┘   │  (1000)  │   │ product_     │
                                                └──────────┘   │ costs.parquet│
   plus a Kafka broker (qs-kafka) seeded with 40 order events  │ promotions.  │
   on topic qs_orders — see the Kafka section.                 │ ndjson       │
                                                               └──────────────┘
```

### Backends & federation names

You reference a table in federated SQL as **`alias.database.table`**. The alias is
the connection name; OtterStax routes the query to the right engine.

| Alias | Engine | Database | Table (rows × cols) | Reference in SQL |
|-------|--------|----------|---------------------|------------------|
| `pgshop` | PostgreSQL #1 | `shop` | `customers` (800 × 4) | `pgshop.shop.customers` |
| `pgcat` | PostgreSQL #2 | `catalog` | `products` (500 × 4) | `pgcat.catalog.products` |
| `sales` | MariaDB | `ops` | `orders` (1000 × 5) | `sales.ops.orders` |
| `web` | ClickHouse | `analytics` | `pageviews` (1000 × 4) | `web.analytics.pageviews` |
| `qs_s3` | MinIO (S3) | bucket `quickstart-bucket` | `regions.csv`, `product_costs.parquet`, `promotions.ndjson` | loaded into `qs.*` (example 6) |

### Data dictionary

**`pgshop.shop.customers`** — who buys
| column | type | notes |
|--------|------|-------|
| `customer_id` | text (PK) | `C0001`…`C0800` |
| `name` | text | |
| `country` | char(2) | one of 12 ISO codes |
| `tier` | text | `bronze` / `silver` / `gold` |

**`pgcat.catalog.products`** — what's for sale
| column | type | notes |
|--------|------|-------|
| `product_id` | text (PK) | `P0001`…`P0500` |
| `name` | text | |
| `category` | text | 8 categories |
| `price` | double | 9.99–499.99 |

**`sales.ops.orders`** — transactions
| column | type | notes |
|--------|------|-------|
| `order_id` | varchar(8) (PK) | `O00001`…`O01000` |
| `customer_id` | varchar(8) | → customers |
| `product_id` | varchar(8) | → products |
| `amount` | double | 50–500 |
| `status` | varchar(16) | paid / shipped / pending / refunded |

**`web.analytics.pageviews`** — clickstream (OLAP)
| column | type | notes |
|--------|------|-------|
| `customer_id` | String | → customers |
| `product_id` | String | → products |
| `channel` | String | web / mobile / email / social |
| `event_ts` | DateTime | ~30-day window |

**S3 objects** (loaded into engine tables `qs.*` in example 6)
| object | format | columns |
|--------|--------|---------|
| `regions.csv` | csv | `country`, `region_name` (12 rows) |
| `product_costs.parquet` | parquet | `product_id`, `unit_cost` (500 rows) |
| `promotions.ndjson` | ndjson | `product_id`, `promo_code`, `discount_pct` (60 rows) |

---

## Prerequisites

- **Docker** (Desktop or Engine) running.
- **`psql`** on your host (the PostgreSQL client) — that's the only client you need.
- Free host ports: **8817** (OtterStax) and **3310–3317** (backends + MinIO). Port
  **19094** (Kafka) is published too but only needed for optional host-side `rpk`.
- `mysql` / `clickhouse-client` are **optional** — this guide uses `docker exec` for those.

---

## Verify the data is loaded

### Through OtterStax (federated)

A count against each backend, all through the one OtterStax connection on 8817:

```bash
psql -h localhost -p 8817 -U demo demo -c "SELECT count(*) FROM pgshop.shop.customers;"    # 800
psql -h localhost -p 8817 -U demo demo -c "SELECT count(*) FROM pgcat.catalog.products;"    # 500
psql -h localhost -p 8817 -U demo demo -c "SELECT count(*) FROM sales.ops.orders;"          # 1000
psql -h localhost -p 8817 -U demo demo -c "SELECT count(*) FROM web.analytics.pageviews;"   # 1000
```

### Directly against each backend (optional sanity check)

```bash
# PostgreSQL #1  (host port 3311, db shop, schema shop)
psql -h localhost -p 3311 -U demo shop -c "SELECT count(*) FROM shop.customers;"        # 800

# PostgreSQL #2  (host port 3312, db catalog, schema catalog)
psql -h localhost -p 3312 -U demo catalog -c "SELECT count(*) FROM catalog.products;"    # 500

# MariaDB  (no host client needed — exec into the container)
docker exec qs-mariadb mariadb -u demo -pdemo ops -e "SELECT COUNT(*) FROM orders;"      # 1000

# ClickHouse  (exec into the container)
docker exec qs-clickhouse clickhouse-client -u demo --password demo \
  -q "SELECT count() FROM analytics.pageviews"                                           # 1000
```
(The two `psql` calls will prompt for a password — it's `demo`. Or prefix with
`PGPASSWORD=demo`.)

### Verify the S3 files (MinIO console)

Open **http://localhost:3317** and log in with **`minioadmin` / `minioadmin`**.
Browse the `quickstart-bucket` — at a fresh start it holds exactly the three source
objects `regions.csv`, `product_costs.parquet` and `promotions.ndjson`.
[Example 7](#example-7--export-a-join-back-to-s3-copy--to) writes an
`exports/promo_costs.csv`; that `exports/` prefix is demo output, so the seed job
clears it on every `docker compose up` — you'll only see it after you run example 7.

---

## The examples

Nine progressive examples, simple → advanced. Each shows: **purpose**, a
**copy-paste command**, and the **expected result** (trimmed small). Every example is
also a file under `sql/` — run it with `-f examples/quick_start/sql/<file>`.

Run them from the repo root (or any directory — the commands are self-contained).

---

### Example 1 — Simple SELECT with filters (1 backend: PostgreSQL #2)

Premium products priced over 485, excluding the Clothing category (`>` and `<>`):

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT product_id, name, category, price
      FROM pgcat.catalog.products
      WHERE price > 485 AND category <> 'Clothing'
      ORDER BY price DESC;"
```

```text
 product_id |       name        | category |   price
------------+-------------------+----------+------------
 P0399      | Star Security     | Toys     | 498.590000
 P0446      | Station Determine | Books    | 495.930000
 P0184      | Suggest Player    | Beauty   | 491.030000
 P0269      | Good As           | Sports   | 490.570000
 P0335      | Such Green        | Toys     | 488.940000
 ... (7 rows)
```

> `WHERE` predicates (`>`, `<>`, `AND`, …) are pushed down to the backend. `LIMIT` /
> `OFFSET` are pushed down too, so `... LIMIT 10` returns 10 rows straight from the
> backend — this example just uses a filter to show comparison operators.

---

### Example 2 — Aggregate (1 backend: MariaDB)

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT status, COUNT(*) AS n, SUM(amount) AS total
      FROM sales.ops.orders
      GROUP BY status
      ORDER BY total DESC;"
```

```text
  status  |  n  |     total
----------+-----+---------------
 paid     | 684 | 190897.860000
 shipped  | 157 |  42808.090000
 pending  | 103 |  30693.640000
 refunded |  56 |  16341.030000
 (4 rows)
```

---

### Example 3 — 2-backend JOIN (MariaDB ⋈ PostgreSQL #1)

Top-spending customers on paid orders — `orders` (MariaDB) joined to `customers` (PG #1).

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT c.name, c.country, COUNT(*) AS orders_cnt, SUM(o.amount) AS spend
      FROM sales.ops.orders o
      JOIN pgshop.shop.customers c ON c.customer_id = o.customer_id
      WHERE o.status = 'paid'
      GROUP BY c.name, c.country
      ORDER BY spend DESC
      LIMIT 10;"
```

```text
      name       | country | orders_cnt |    spend
-----------------+---------+------------+-------------
 Connie Parker   | NL      |          4 | 1486.560000
 Daniel Cain     | FR      |          4 | 1437.210000
 Sandra Boyd     | DE      |          4 | 1343.680000
 James Rivera    | JP      |          5 | 1331.140000
 Anthony Preston | CA      |          4 | 1285.190000
 ... (10 rows)
```

---

### Example 4 — 3-backend JOIN (both PostgreSQL backends + MariaDB)

Revenue by product category and customer country — exercises **both independent
PostgreSQL backends** in one statement (`customers` from PG #1, `products` from PG #2).

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT p.category, c.country, SUM(o.amount) AS revenue
      FROM sales.ops.orders o
      JOIN pgshop.shop.customers c  ON c.customer_id = o.customer_id
      JOIN pgcat.catalog.products p ON p.product_id  = o.product_id
      WHERE o.status IN ('paid','shipped')
      GROUP BY p.category, c.country
      ORDER BY revenue DESC
      LIMIT 10;"
```

```text
 category | country |   revenue
----------+---------+-------------
 Toys     | FR      | 4950.610000
 Sports   | DE      | 4577.540000
 Books    | FR      | 4432.880000
 Food     | US      | 4338.800000
 Home     | JP      | 4111.880000
 ... (10 rows)
```

---

### Example 5 — Complex 4-backend JOIN (+ ClickHouse)

Clickstream joined to actual purchases. `pageviews` (ClickHouse) ⋈ `orders` (MariaDB)
on **both** `customer_id` and `product_id`, then to `customers` (PG #1) and `products`
(PG #2); aggregated by channel and category.

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT v.channel, p.category,
             COUNT(DISTINCT o.order_id) AS orders_cnt,
             SUM(o.amount) AS revenue
      FROM web.analytics.pageviews v
      JOIN sales.ops.orders o   ON o.customer_id = v.customer_id
                               AND o.product_id  = v.product_id
      JOIN pgshop.shop.customers c  ON c.customer_id = o.customer_id
      JOIN pgcat.catalog.products p ON p.product_id  = o.product_id
      WHERE o.status = 'paid'
      GROUP BY v.channel, p.category
      ORDER BY revenue DESC
      LIMIT 10;"
```

```text
 channel | category | orders_cnt |   revenue
---------+----------+------------+-------------
 social  | Books    |         19 | 6314.470000
 mobile  | Food     |         22 | 6242.420000
 social  | Home     |         16 | 6175.340000
 web     | Toys     |         16 | 5574.930000
 web     | Food     |         18 | 5522.360000
 ... (10 rows)
```

---

### Example 6 — Load 3 S3 files into the engine (csv + parquet + ndjson)

Loads three S3 objects — one of each format — into OtterStax-internal tables, then joins
one of them (`qs.regions`) to a live backend (`customers`). The `qs` database is
**auto-created** by the first `CREATE EXTERNAL TABLE`, and the **format is auto-detected**
from each file's extension (no `format` option needed):

```bash
psql -h localhost -p 8817 -U demo demo <<'SQL'
CREATE EXTERNAL TABLE qs.regions       WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/regions.csv');
CREATE EXTERNAL TABLE qs.product_costs WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/product_costs.parquet');
CREATE EXTERNAL TABLE qs.promotions    WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/promotions.ndjson');

SELECT r.region_name, COUNT(*) AS customers
FROM   pgshop.shop.customers c
JOIN   qs.regions r ON r.country = c.country
GROUP BY r.region_name
ORDER BY customers DESC;
SQL
```

```text
COMMAND
COMMAND
COMMAND
  region_name  | customers
---------------+-----------
 North America |       140
 EU South      |       137
 Asia Pacific  |       136
 EU West       |       123
 EU Central    |        72
 ... (8 rows)
```

---

### Example 7 — Export a JOIN back to S3 (COPY … TO)

Joins the two S3-sourced engine tables and writes the result as CSV to S3:

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "COPY (
        SELECT pc.product_id, pc.unit_cost, pr.promo_code, pr.discount_pct
        FROM   qs.product_costs pc
        JOIN   qs.promotions pr ON pr.product_id = pc.product_id
        ORDER  BY pc.product_id
      ) TO 's3://quickstart-bucket/exports/promo_costs.csv'
        WITH (s3_alias = 'qs_s3', format = 'csv');"
```

Output is just the command tag:

```text
COMMAND
```

Verify it landed — in the MinIO console (http://localhost:3317) browse
`quickstart-bucket/exports/promo_costs.csv`, or:

```bash
docker exec qs-minio ls /data/quickstart-bucket/exports/
```

---

### Example 8 — Federation finale (PG #1 + PG #2 + MariaDB + ClickHouse + S3)

The headline: **all four live backends and an S3-loaded engine table** in one
statement. Gross margin (`revenue − unit_cost`) per country, category and channel.

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT c.country, p.category, v.channel,
             COUNT(DISTINCT o.order_id)     AS orders_cnt,
             SUM(o.amount)                  AS revenue,
             SUM(o.amount - pcst.unit_cost) AS gross_margin
      FROM sales.ops.orders o
      JOIN pgshop.shop.customers  c    ON c.customer_id   = o.customer_id
      JOIN pgcat.catalog.products p    ON p.product_id    = o.product_id
      JOIN web.analytics.pageviews v   ON v.customer_id   = o.customer_id
                                      AND v.product_id    = o.product_id
      JOIN qs.product_costs       pcst ON pcst.product_id = o.product_id
      WHERE o.status = 'paid'
      GROUP BY c.country, p.category, v.channel
      ORDER BY gross_margin DESC
      LIMIT 10;"
```

```text
 country | category | channel | orders_cnt |   revenue   | gross_margin
---------+----------+---------+------------+-------------+--------------
 IL      | Food     | mobile  |          7 | 2135.550000 |  1515.310000
 ES      | Beauty   | web     |          3 | 1731.790000 |  1125.560000
 FR      | Books    | social  |          5 | 1928.670000 |  1084.870000
 US      | Clothing | mobile  |          7 | 1882.940000 |  1071.430000
 ES      | Food     | social  |          2 | 1689.110000 |  1037.890000
 ... (10 rows)
```

> Requires example 6 (`qs.product_costs`). MariaDB + PostgreSQL ×2 + ClickHouse +
> S3-in-otterbrix, joined and aggregated in a single SQL statement. That's the whole
> point of OtterStax.

---

### Example 9 — Mutations: INSERT / UPDATE / DROP

A full lifecycle on a small engine table. `UPDATE` echoes the changed rows with their
new values, so you can see exactly what it did (requires example 6 for the `qs` database):

```bash
psql -h localhost -p 8817 -U demo demo <<'SQL'
CREATE TABLE qs.scratch (id bigint, label string, qty bigint);
INSERT INTO qs.scratch (id, label, qty) VALUES (1,'alpha',1), (2,'beta',2), (3,'gamma',3);
UPDATE qs.scratch SET qty = qty * 10 WHERE id <= 2;
SELECT * FROM qs.scratch ORDER BY id;
DROP TABLE qs.scratch;
SQL
```

```text
CREATE TABLE
INSERT 0 3
 id | label | qty
----+-------+-----
  1 | alpha |  10
  2 | beta  |  20
(2 rows)

UPDATE 2
 id | label | qty
----+-------+-----
  1 | alpha |  10
  2 | beta  |  20
  3 | gamma |   3
(3 rows)

DROP
```

> The two-row block after `INSERT 0 3` is the `UPDATE` echoing the rows it changed;
> the three-row block is the final `SELECT`. `DELETE` works the same way and reports
> `DELETE N`. These run on **engine tables** (`qs.*`, incl. the S3-loaded ones), which
> are the reliably writable target — mutations directly against the pre-seeded backend
> tables (`customers`, `orders`, …) are not supported over this path.
>
> Engine tables live in the running server, so they **do not survive an OtterStax
> restart** — just re-run example 6 to recreate them. Backend data and S3 files always
> persist (see [Persistence](#persistence--what-survives-a-restart)).

---

## Kafka (streaming)

The stack also runs a **Kafka broker** (`qs-kafka`, redpanda), pre-seeded with 40 order
events on the `qs_orders` topic. These four examples turn a topic into a SQL table, join
it to the live backends, produce to it from SQL, and continuously transform it — all over
the same `psql` connection. `TRANSACTIONAL=true` makes ingest/produce **exactly-once**.

> **Ingestion is asynchronous.** After `CREATE SOURCE` a background poller fills the
> table over a few seconds — if a `SELECT` shows 0 rows, wait and re-run it.

### K1 — A Kafka topic as a SQL table

Declare the source (broker is the in-network `qs-kafka:9092`):

```bash
psql -h localhost -p 8817 -U demo demo <<'SQL'
DROP SOURCE IF EXISTS qs_orders;
CREATE SOURCE qs_orders (
    event_id VARCHAR, customer_id VARCHAR, product_id VARCHAR,
    amount DOUBLE, status VARCHAR, channel VARCHAR
) WITH (KAFKA_TOPIC       = 'qs_orders',
        VALUE_FORMAT      = 'JSON',
        BOOTSTRAP_SERVERS = 'qs-kafka:9092',
        OFFSET_RESET      = 'earliest',
        TRANSACTIONAL     = true);
SQL
```

Then **wait ~5 s** and query it like any table:

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT count(*) AS events FROM kafka.qs_orders;"
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT status, count(*) AS n, SUM(amount) AS total
      FROM kafka.qs_orders GROUP BY status ORDER BY n DESC;"
```

```text
 events
--------
     40

  status  | n  |    total
----------+----+-------------
 paid     | 27 | 6664.250000
 pending  |  6 |  888.410000
 shipped  |  4 |  955.640000
 refunded |  3 | 1082.510000
```

### K2 — Federated JOIN: live Kafka ⋈ PostgreSQL ×2

Each live order joined to its customer (PG #1: country, tier) and product (PG #2: category):

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT o.event_id, c.country, c.tier, p.category, o.amount
      FROM kafka.qs_orders o
      JOIN pgshop.shop.customers  c ON c.customer_id = o.customer_id
      JOIN pgcat.catalog.products p ON p.product_id  = o.product_id
      WHERE o.status = 'paid'
      ORDER BY o.amount DESC
      LIMIT 10;"
```

```text
 event_id | country |  tier  |  category   |   amount
----------+---------+--------+-------------+------------
 E0025    | US      | bronze | Food        | 488.980000
 E0030    | FR      | bronze | Electronics | 468.200000
 E0018    | AU      | bronze | Beauty      | 397.650000
 E0003    | BR      | silver | Toys        | 379.800000
 E0034    | IL      | bronze | Clothing    | 374.090000
 ... (10 rows)
```

> `customers` and `products` both have a `name` column; referencing `name` in this mixed
> 3-way join trips column resolution, so this projects `country`/`tier`/`category` instead.

### K3 — Produce to Kafka from SQL (round-trip)

`INSERT INTO` a kafka object writes to the topic; the poller ingests it straight back:

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "INSERT INTO kafka.qs_orders (event_id, customer_id, product_id, amount, status, channel)
      VALUES ('E9001','C0001','P0001',199.99,'paid','api'),
             ('E9002','C0002','P0002', 49.50,'pending','api');"
```

Then **wait a moment** and see the produced rows come back through the topic:

```bash
psql -h localhost -p 8817 -U demo demo \
  -c "SELECT event_id, customer_id, amount, status, channel
      FROM kafka.qs_orders WHERE channel = 'api' ORDER BY event_id;"
```

```text
 event_id | customer_id |   amount   | status  | channel
----------+-------------+------------+---------+---------
 E9001    | C0001       | 199.990000 | paid    | api
 E9002    | C0002       |  49.500000 | pending | api
```

### K4 — Continuous stream (`CREATE STREAM … AS SELECT`)

Spawn a persistent worker that filters `qs_orders` to paid rows and produces them to the
`qs_orders_paid` topic, forever:

```bash
psql -h localhost -p 8817 -U demo demo <<'SQL'
DROP STREAM IF EXISTS qs_orders_paid;
CREATE STREAM qs_orders_paid
    WITH (KAFKA_TOPIC       = 'qs_orders_paid',
          VALUE_FORMAT      = 'JSON',
          BOOTSTRAP_SERVERS = 'qs-kafka:9092',
          OFFSET_RESET      = 'earliest',
          TRANSACTIONAL     = true)
    AS SELECT event_id, customer_id, amount, channel
       FROM   kafka.qs_orders
       WHERE  status = 'paid';
SQL
```

Verify the output topic (drain it via `rpk` inside the broker; `-o :end` reads to the end
and exits):

```bash
docker exec qs-kafka rpk topic consume qs_orders_paid -o :end -f '%v\n'
```

```text
{"event_id":"E0001","customer_id":"C0540","amount":2.1553E2,"channel":"mobile"}
{"event_id":"E0002","customer_id":"C0539","amount":2.8616E2,"channel":"social"}
{"event_id":"E0003","customer_id":"C0343","amount":3.798E2,"channel":"email"}
... (28 paid records — the paid subset of qs_orders)
```

### Kafka cleanup / re-running

`DROP` the stream + source before replaying K1–K4:

```bash
psql -h localhost -p 8817 -U demo demo <<'SQL'
DROP STREAM IF EXISTS qs_orders_paid;
DROP SOURCE IF EXISTS qs_orders;
SQL
```

Like the engine tables, a source/stream lives in the running server and doesn't survive an
OtterStax restart — re-run K1 (and K4). The `qs_orders` topic itself is re-seeded to the
committed 40 events on every `docker compose up`.

---

## Re-running / cleanup of engine state

Examples 1–5 are read-only and always re-runnable. Examples 6–9 create the `qs.*`
engine tables. **Run example 6 once** — re-running `CREATE EXTERNAL TABLE` on a table
that already exists *appends* the file again (duplicate rows). Before replaying examples
6–9, drop the engine state:

```bash
psql -h localhost -p 8817 -U demo demo <<'SQL'
DROP TABLE IF EXISTS qs.regions;
DROP TABLE IF EXISTS qs.product_costs;
DROP TABLE IF EXISTS qs.promotions;
DROP DATABASE IF EXISTS qs;
SQL
```

This drops `qs.regions`, `qs.product_costs`, `qs.promotions` and the `qs` database.
Backend data and S3 files are untouched. (The `exports/promo_costs.csv` object from
example 7 lives in S3 and is simply overwritten on the next run.)

---

## Persistence — what survives a restart

Backend data lives in named Docker volumes and the S3 objects live in MinIO's volume,
so **your data is intact across restarts**. Verified behavior:

| Action | Backends (customers/products/orders/pageviews) | S3 source files | Engine tables (`qs.*`) |
|--------|-----------------------------------------------|-----------------|------------------------|
| `docker compose restart` / `stop`+`start` | **kept** (volumes) | **kept** | lost (in-memory; re-run ex. 6) |
| `docker compose down` then `up` (no `-v`) | **kept** (volumes) | **kept** (minio-init re-runs, idempotent) | lost — re-run ex. 6 |
| `docker compose down -v` then `up` | **rebuilt from `init/**`** (identical data) | **re-seeded from `init/s3/**`** | absent until ex. 6 |

So: source data (all four backends + the three S3 **source** files) is preserved on a warm
restart and faithfully **reconstructed from the committed init files** on a cold `down -v`.
The only thing you re-run after an OtterStax restart is `CREATE EXTERNAL TABLE` (example 6),
because engine-loaded tables live in the server process, not on a backend.

The `exports/` prefix (example 7's `COPY` output) is **not** persisted seed data — the
seed job clears it on every `up`, so a fresh start never shows a stale export. The Kafka
broker is ephemeral: `qs-kafka-init` re-seeds `qs_orders` to the committed 40 events on
every `up`.

---

## Teardown

```bash
docker compose down       # stop containers, KEEP data (volumes persist)
docker compose down -v    # stop and WIPE volumes (next `up` rebuilds from init/)
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `up` fails: *port is already allocated* (8817) | The `examples/demo/` stack (or another) holds 8817 | Stop it first: `docker stop demo-otterstax` (or `docker compose -f examples/demo/compose.yml down`). |
| `psql: connection refused` on 8817 | OtterStax still starting | Wait for `docker compose ps` to show `qs-otterstax` healthy (it waits for all backends first). |
| `External table is not registered: shop.customers` | Wrong qualifier — a PostgreSQL table needs `alias.db.table` where the tables live in a schema named like the db | Use the forms in [Backends & federation names](#backends--federation-names): `pgshop.shop.customers`, `pgcat.catalog.products`. |
| My `qs.*` tables vanished after a restart | Engine tables are in-memory | Re-run `sql/06_external_create.sql`. Backend + S3 data is still there (see [Persistence](#persistence--what-survives-a-restart)). |
| `ROUND(x, 2)` errors / returns wrong output | 2-arg `ROUND` isn't pushed to backends | Use plain `SUM()` / `AVG()` (as the examples do). |
| ClickHouse/MariaDB "client not found" on host | You don't have those CLIs | Use the `docker exec` forms in [Directly against each backend](#directly-against-each-backend-optional-sanity-check). |
| `kafka.qs_orders` shows 0 rows | The poller is still ingesting (async) | Wait a few seconds and re-run the `SELECT` (K1). |
| K2 join errors `path: 'name' was not found` | `name` exists in both `customers` and `products` — the mixed join can't resolve it | Project other columns (`country`/`tier`/`category`), as K2 does. |
| `qs_orders_paid` empty after `CREATE STREAM` | The stream worker needs a moment | Wait a few seconds; check `docker logs qs-otterstax`. Sources/streams don't survive an OtterStax restart — re-run K1/K4. |

---

## File reference

| Path | What |
|------|------|
| `compose.yml` | the whole stack (backends + MinIO + Kafka + seed jobs + OtterStax) with named volumes |
| `config.yaml` | OtterStax connection config (2×PG, MariaDB, ClickHouse, S3) — read at startup |
| `sql/01…09_*.sql` | the nine core examples; `sql/cleanup.sql` drops engine state |
| `sql/kafka_0{1..4}_*.sql` | the four Kafka examples; `sql/kafka_cleanup.sql` drops the source/stream |
| `init/postgres_shop/`, `init/postgres_catalog/`, `init/mariadb/`, `init/clickhouse/` | committed DB seed SQL (auto-run on first container init) |
| `init/s3/` | committed S3 fixtures (`regions.csv`, `product_costs.parquet`, `promotions.ndjson`) uploaded by `qs-minio-init` |
| `init/kafka/orders.ndjson` | committed Kafka fixture (40 events) produced to `qs_orders` by `qs-kafka-init` |
| `fixtures/generate.py` | **maintainer-only** regenerator for everything under `init/`. Not needed to run the demo (needs `faker` + `pyarrow`). |
