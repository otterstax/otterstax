# OtterStax Quick Start

A one-command federated-SQL demo. `docker compose up --wait` brings up **2 PostgreSQL + 1
MariaDB + 1 ClickHouse + RustFS (S3) + Kafka** behind a single OtterStax server; you run
SQL that joins across all of them with `psql`. Compose seeds the data from committed
files automatically.

The 9 core examples cover federated SQL over the databases + S3; the
[**Kafka (streaming)**](#kafka-streaming) section adds four streaming examples on top.

## Run it

```bash
cd examples/quick_start
docker compose up --wait
```

Use this command for initial setup or to load the demo fixtures again.
On an existing stack, a full `up --wait` can rerun the seed jobs even
without `down`. They recreate the demo Kafka topics and clear S3 exports.
To resume a paused stack with its data, use the [pause and resume commands](#teardown).

Compose pulls `ghcr.io/otterstax/otterstax:0.2.0` (Linux amd64) if it is missing.
To supply a locally built runtime image, see [Running with Docker Compose](../../readme.md#running-with-docker-compose).
OtterStax starts after both the S3 and Kafka seed jobs succeed. The command
returns once the server is healthy. Confirm the PostgreSQL wire answers a query:

```bash
psql -h localhost -p 8817 -U demo local -c "SELECT 1"
```

If startup reports a failed seed job, inspect its logs before running the
examples:

```bash
docker compose ps -a
docker compose logs qs-rustfs-init qs-kafka-init
```

Connect to OtterStax (no password; the database is the engine's `local` — a name that is
neither a connection nor `local` is refused with `FATAL 3D000`):

```bash
psql -h localhost -p 8817 -U demo local
```

> Uses the PostgreSQL wire on port **8817** — the same port as `examples/demo/`, so don't
> run both at once (`docker stop demo-otterstax` if needed).

## The 30-second tour

```bash
# federation across MariaDB + both PostgreSQL backends
psql -h localhost -p 8817 -U demo local \
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
   │PostgreSQL│   │PostgreSQL│   │ MariaDB  │   │ClickHouse│   │    RustFS    │
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

You reference a backend table in federated SQL in three parts,
**`alias.namespace.table`**. The alias is the connection name; OtterStax
routes the query to the right engine. The namespace is what one connection
can choose between: a PostgreSQL connection names a schema of its `database:` (the
`schema:` in `config.yaml`, `shop` / `catalog` here); MariaDB / ClickHouse
name one of their databases. Four-part names are refused. `local.*` names the
engine's own tables (`local.qs.regions`), so `local.shop.customers` is not the
backend table.

| Alias | Engine | Database | Table (rows × cols) | Reference in SQL |
|-------|--------|----------|---------------------|------------------|
| `pgshop` | PostgreSQL #1 | `shop` | `customers` (800 × 4) | `pgshop.shop.customers` |
| `pgcat` | PostgreSQL #2 | `catalog` | `products` (500 × 4) | `pgcat.catalog.products` |
| `sales` | MariaDB | `ops` | `orders` (1000 × 5) | `sales.ops.orders` |
| `web` | ClickHouse | `analytics` | `pageviews` (1000 × 4) | `web.analytics.pageviews` |
| `qs_s3` | RustFS (S3) | bucket `quickstart-bucket` | `regions.csv`, `product_costs.parquet`, `promotions.ndjson` | loaded into `local.qs.*` — the schema `qs` of the local database (example 6) |

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

**S3 objects** (loaded into engine tables `local.qs.*` in example 6)
| object | format | columns |
|--------|--------|---------|
| `regions.csv` | csv | `country`, `region_name` (12 rows) |
| `product_costs.parquet` | parquet | `product_id`, `unit_cost` (500 rows) |
| `promotions.ndjson` | ndjson | `product_id`, `promo_code`, `discount_pct` (60 rows) |

---

## Prerequisites

- **Docker** (Desktop or Engine) running.
- **`psql`** on your host (the PostgreSQL client) — that's the only client you need.
- Free host ports: **8817** (OtterStax), **3310–3317** (backends and RustFS),
  and **19094** (Kafka). Compose publishes 19094 even if you use `rpk` inside
  the broker container.
- `mysql` / `clickhouse-client` are **optional** — this guide uses `docker exec` for those.

---

## Verify the data is loaded

### Through OtterStax (federated)

A count against each backend, all through the one OtterStax connection on 8817:

```bash
psql -h localhost -p 8817 -U demo local -c "SELECT count(*) FROM pgshop.shop.customers;"    # 800
psql -h localhost -p 8817 -U demo local -c "SELECT count(*) FROM pgcat.catalog.products;"    # 500
psql -h localhost -p 8817 -U demo local -c "SELECT count(*) FROM sales.ops.orders;"          # 1000
psql -h localhost -p 8817 -U demo local -c "SELECT count(*) FROM web.analytics.pageviews;"   # 1000
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

### Verify the S3 files (RustFS console)

Before example 6, wait for the seed job to exit and inspect its logs:

```bash
docker compose ps -a qs-rustfs-init
docker compose logs qs-rustfs-init
```

Open **http://localhost:3317/rustfs/console/** and log in with **`rustfsadmin` / `rustfsadmin`**.
Verify that `quickstart-bucket` contains all three source objects:
`regions.csv`, `product_costs.parquet` and `promotions.ndjson`. An upload
error fails the startup command. Check the logs for the failure details.
[Example 7](#example-7--export-a-join-back-to-s3-copy--to) writes an
`exports/promo_costs.csv`. The seed job clears `exports/` whenever the job runs.

---

## The examples

Nine progressive examples, simple → advanced. Each shows: **purpose**, a
**copy-paste command**, and the **expected result** (trimmed small). Every example is
also a file under `examples/quick_start/sql/`. For example:

```bash
# From examples/quick_start
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local -f sql/01_select.sql
# From the repository root
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local -f examples/quick_start/sql/01_select.sql
```

Batch examples use `ON_ERROR_STOP=1` so `psql` stops and returns a nonzero
status on the first SQL error. Statements already committed remain in effect.

---

### Example 1 — Simple SELECT with filters (1 backend: PostgreSQL #2)

Premium products priced over 485, excluding the Clothing category (`>` and `<>`):

```bash
psql -h localhost -p 8817 -U demo local \
  -c "SELECT product_id, name, category, price
      FROM pgcat.catalog.products
      WHERE price > 485 AND category <> 'Clothing'
      ORDER BY price DESC;"
```

```text
 product_id |       name        | category | price
------------+-------------------+----------+--------
 P0399      | Star Security     | Toys     | 498.59
 P0446      | Station Determine | Books    | 495.93
 P0184      | Suggest Player    | Beauty   | 491.03
 P0269      | Good As           | Sports   | 490.57
 P0335      | Such Green        | Toys     | 488.94
 ... (7 rows)
```

> `WHERE` predicates (`>`, `<>`, `AND`, …) are pushed down to the backend. `LIMIT` /
> `OFFSET` are pushed down too, so `... LIMIT 10` returns 10 rows straight from the
> backend — this example just uses a filter to show comparison operators.

---

### Example 2 — Aggregate (1 backend: MariaDB)

```bash
psql -h localhost -p 8817 -U demo local \
  -c "SELECT status, COUNT(*) AS n, SUM(amount) AS total
      FROM sales.ops.orders
      GROUP BY status
      ORDER BY total DESC;"
```

```text
  status  |  n  |       total
----------+-----+--------------------
 paid     | 684 | 190897.86000000004
 shipped  | 157 |           42808.09
 pending  | 103 | 30693.639999999996
 refunded |  56 |           16341.03
 (4 rows)
```

---

### Example 3 — 2-backend JOIN (MariaDB ⋈ PostgreSQL #1)

Top-spending customers on paid orders — `orders` (MariaDB) joined to `customers` (PG #1).

```bash
psql -h localhost -p 8817 -U demo local \
  -c "SELECT c.name, c.country, COUNT(*) AS orders_cnt, SUM(o.amount) AS spend
      FROM sales.ops.orders o
      JOIN pgshop.shop.customers c ON c.customer_id = o.customer_id
      WHERE o.status = 'paid'
      GROUP BY c.name, c.country
      ORDER BY spend DESC
      LIMIT 10;"
```

```text
      name       | country | orders_cnt |       spend
-----------------+---------+------------+--------------------
 Connie Parker   | NL      |          4 | 1486.5600000000002
 Daniel Cain     | FR      |          4 |            1437.21
 Sandra Boyd     | DE      |          4 | 1343.6799999999998
 James Rivera    | JP      |          5 | 1331.1399999999999
 Anthony Preston | CA      |          4 |            1285.19
 ... (10 rows)
```

---

### Example 4 — 3-backend JOIN (both PostgreSQL backends + MariaDB)

Revenue by product category and customer country — exercises **both independent
PostgreSQL backends** in one statement (`customers` from PG #1, `products` from PG #2).

```bash
psql -h localhost -p 8817 -U demo local \
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
 category | country |      revenue
----------+---------+--------------------
 Toys     | FR      |            4950.61
 Sports   | DE      |  4577.540000000001
 Books    | FR      |  4432.880000000001
 Food     | US      |             4338.8
 Home     | JP      |  4111.879999999999
 ... (10 rows)
```

---

### Example 5 — Complex 4-backend JOIN (+ ClickHouse)

Clickstream joined to actual purchases. `pageviews` (ClickHouse) ⋈ `orders` (MariaDB)
on **both** `customer_id` and `product_id`, then to `customers` (PG #1) and `products`
(PG #2); aggregated by channel and category.

```bash
psql -h localhost -p 8817 -U demo local \
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
 channel | category | orders_cnt |      revenue
---------+----------+------------+-------------------
 social  | Books    |         19 | 6314.469999999999
 mobile  | Food     |         22 | 6242.419999999999
 social  | Home     |         16 |           6175.34
 web     | Toys     |         16 |           5574.93
 web     | Food     |         18 |           5522.36
 ... (10 rows)
```

---

### Example 6 — Load 3 S3 files into the engine (csv + parquet + ndjson)

First [verify the three S3 source objects](#verify-the-s3-files-rustfs-console).

Loads three S3 objects — one of each format — into OtterStax-internal tables, then joins
one of them (`local.qs.regions`) to a live backend (`customers`). The tables go into the
schema `qs` of the local database (`local.qs.regions`);
`CREATE EXTERNAL TABLE` never creates a schema, so the example creates it first. The
**format is auto-detected** from each file's extension (no `format` option needed):

```bash
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local <<'SQL'
CREATE SCHEMA IF NOT EXISTS qs;
CREATE EXTERNAL TABLE local.qs.regions       WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/regions.csv');
CREATE EXTERNAL TABLE local.qs.product_costs WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/product_costs.parquet');
CREATE EXTERNAL TABLE local.qs.promotions    WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/promotions.ndjson');

SELECT r.region_name, COUNT(*) AS customers
FROM   pgshop.shop.customers c
JOIN   local.qs.regions r ON r.country = c.country
GROUP BY r.region_name
ORDER BY customers DESC;
SQL
```

```text
CREATE SCHEMA
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
psql -h localhost -p 8817 -U demo local \
  -c "COPY (
        SELECT pc.product_id, pc.unit_cost, pr.promo_code, pr.discount_pct
        FROM   local.qs.product_costs pc
        JOIN   local.qs.promotions pr ON pr.product_id = pc.product_id
        ORDER  BY pc.product_id
      ) TO 's3://quickstart-bucket/exports/promo_costs.csv'
        WITH (s3_alias = 'qs_s3', format = 'csv');"
```

Output is just the command tag:

```text
COMMAND
```

Verify it landed — in the RustFS console (http://localhost:3317/rustfs/console/) browse
`quickstart-bucket/exports/promo_costs.csv`, or:

```bash
docker exec qs-rustfs ls /data/quickstart-bucket/exports/
```

---

### Example 8 — Federation finale (PG #1 + PG #2 + MariaDB + ClickHouse + S3)

The headline: **all four live backends and an S3-loaded engine table** in one
statement. Gross margin (`revenue − unit_cost`) per country, category and channel.

```bash
psql -h localhost -p 8817 -U demo local \
  -c "SELECT c.country, p.category, v.channel,
             COUNT(DISTINCT o.order_id)     AS orders_cnt,
             SUM(o.amount)                  AS revenue,
             SUM(o.amount - pcst.unit_cost) AS gross_margin
      FROM sales.ops.orders o
      JOIN pgshop.shop.customers  c    ON c.customer_id   = o.customer_id
      JOIN pgcat.catalog.products p    ON p.product_id    = o.product_id
      JOIN web.analytics.pageviews v   ON v.customer_id   = o.customer_id
                                      AND v.product_id    = o.product_id
      JOIN local.qs.product_costs       pcst ON pcst.product_id = o.product_id
      WHERE o.status = 'paid'
      GROUP BY c.country, p.category, v.channel
      ORDER BY gross_margin DESC
      LIMIT 10;"
```

```text
 country | category | channel | orders_cnt |      revenue       |    gross_margin
---------+----------+---------+------------+--------------------+--------------------
 IL      | Food     | mobile  |          7 | 2135.5499999999997 | 1515.3100000000002
 ES      | Beauty   | web     |          3 | 1731.7900000000002 |            1125.56
 FR      | Books    | social  |          5 |            1928.67 | 1084.8700000000001
 US      | Clothing | mobile  |          7 |            1882.94 |            1071.43
 ES      | Food     | social  |          2 |            1689.11 | 1037.8899999999999
 ... (10 rows)
```

> Requires example 6 (`local.qs.product_costs`). MariaDB + PostgreSQL ×2 + ClickHouse +
> S3-in-otterbrix, joined and aggregated in a single SQL statement. That's the whole
> point of OtterStax.

---

### Example 9 — Mutations: INSERT / UPDATE / DROP

A full lifecycle on a small engine table (requires example 6 for the `qs` schema):

```bash
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local <<'SQL'
CREATE TABLE local.qs.scratch (id bigint, label string, qty bigint);
INSERT INTO local.qs.scratch (id, label, qty) VALUES (1,'alpha',1), (2,'beta',2), (3,'gamma',3);
UPDATE local.qs.scratch SET qty = qty * 10 WHERE id <= 2;
SELECT * FROM local.qs.scratch ORDER BY id;
DROP TABLE local.qs.scratch;
SQL
```

```text
CREATE TABLE
INSERT 0 3
UPDATE 2
 id | label | qty
----+-------+-----
  1 | alpha |  10
  2 | beta  |  20
  3 | gamma |   3
(3 rows)

DROP
```

> `UPDATE 2` is the number of rows the `UPDATE` changed; the table is the final
> `SELECT`. `DELETE` works the same way and reports `DELETE N`. Example 9
> modifies only `local.qs.scratch`. Supported remote mutations run on the
> backend, so treat the demo backend tables (`customers`, `orders`, …) as
> read-only during the walkthrough.
>
> Engine tables live inside the OtterStax container. They survive restarting
> `qs-otterstax`, and `docker compose down` removes them. After a `down`/`up`,
> re-run example 6 to rebuild the demo tables. See [Persistence](#persistence--what-survives-a-restart)
> for what happens to backend volumes, S3 objects and Kafka.

---

## Kafka (streaming)

The stack also runs a **Kafka broker** (`qs-kafka`, redpanda), pre-seeded with 40 order
events on the `qs_orders` topic. These four examples turn a topic into a SQL table, join
it to the live backends, produce to it from SQL, and continuously transform it — all over
the same `psql` connection. `TRANSACTIONAL=true` makes ingest/produce **exactly-once**.

### Verify the Kafka input

Before K1, wait for the seed job to exit, inspect its log and check the input
topic itself:

```bash
docker compose ps -a qs-kafka-init
docker compose logs qs-kafka-init
docker compose exec -T qs-kafka rpk -X brokers=qs-kafka:9092 topic describe qs_orders -p
```

On a fresh stack, partition 0 should show log start offset 0 and high
watermark 40. If the topic is missing or the offsets differ on a fresh stack,
check the seed log for errors.

> **Ingestion is asynchronous.** After `CREATE SOURCE` a background poller fills the
> table over a few seconds — if a `SELECT` shows 0 rows, wait and re-run it.

If you re-seeded Kafka while keeping the OtterStax container, K1 can retain
rows and offsets from the previous topic. The transactional source resumes
from the latest offset stored in OtterStax for each partition:

```bash
psql -h localhost -p 8817 -U demo local \
  -c 'SELECT partition_id, max(committed_offset) AS next_offset
      FROM federation.kafka.qs_orders__offsets GROUP BY partition_id;'
```

Compare those offsets with the topic's log start and high watermark above.
For example, a saved offset of 43 is past the end of a re-seeded topic whose
high watermark is 40. Even matching offsets do not prove that the retained
rows came from the new topic. If you want K1 to read the freshly seeded data
from the beginning, use the [explicit Kafka cleanup](#kafka-cleanup--re-running),
then recreate the source with a fresh `GROUP_ID` and `OFFSET_RESET = 'earliest'`.

### K1 — A Kafka topic as a SQL table

On the first run, declare the source (broker is the in-network
`qs-kafka:9092`). If it already exists, use the SELECTs below to check it.

```bash
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local <<'SQL'
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
psql -h localhost -p 8817 -U demo local \
  -c "SELECT count(*) AS events FROM kafka.qs_orders;"
psql -h localhost -p 8817 -U demo local \
  -c "SELECT status, count(*) AS n, SUM(amount) AS total
      FROM kafka.qs_orders GROUP BY status ORDER BY n DESC;"
```

```text
 events
--------
     40

  status  | n  |       total
----------+----+-------------------
 paid     | 27 | 6664.249999999998
 pending  |  6 | 888.4100000000001
 shipped  |  4 | 955.6400000000001
 refunded |  3 |           1082.51
```

### K2 — Federated JOIN: live Kafka ⋈ PostgreSQL ×2

Each live order joined to its customer (PG #1: country, tier) and product (PG #2: category):

```bash
psql -h localhost -p 8817 -U demo local \
  -c "SELECT o.event_id, c.country, c.tier, p.category, o.amount
      FROM kafka.qs_orders o
      JOIN pgshop.shop.customers  c ON c.customer_id = o.customer_id
      JOIN pgcat.catalog.products p ON p.product_id  = o.product_id
      WHERE o.status = 'paid'
      ORDER BY o.amount DESC
      LIMIT 10;"
```

```text
 event_id | country |  tier  |  category   | amount
----------+---------+--------+-------------+--------
 E0025    | US      | bronze | Food        | 488.98
 E0030    | FR      | bronze | Electronics |  468.2
 E0018    | AU      | bronze | Beauty      | 397.65
 E0003    | BR      | silver | Toys        |  379.8
 E0034    | IL      | bronze | Clothing    | 374.09
 ... (10 rows)
```

> `customers` and `products` both have a `name` column; a bare `name` in this mixed
> 3-way join is ambiguous (`path: 'name' is ambiguous. Use aliases or full path`), so
> this projects `country`/`tier`/`category` instead — a qualified `c.name` works fine.

### K3 — Produce to Kafka from SQL (round-trip)

`INSERT INTO` a kafka object writes to the topic; the poller ingests it straight back:

```bash
psql -h localhost -p 8817 -U demo local \
  -c "INSERT INTO kafka.qs_orders (event_id, customer_id, product_id, amount, status, channel)
      VALUES ('E9001','C0001','P0001',199.99,'paid','api'),
             ('E9002','C0002','P0002', 49.50,'pending','api');"
```

Then **wait a moment** and see the produced rows come back through the topic:

```bash
psql -h localhost -p 8817 -U demo local \
  -c "SELECT event_id, customer_id, amount, status, channel
      FROM kafka.qs_orders WHERE channel = 'api' ORDER BY event_id;"
```

```text
 event_id | customer_id | amount | status  | channel
----------+-------------+--------+---------+---------
 E9001    | C0001       | 199.99 | paid    | api
 E9002    | C0002       |   49.5 | pending | api
```

### K4 — Continuous stream (`CREATE STREAM … AS SELECT`)

Spawn a persistent worker that filters `qs_orders` to paid rows and produces them to the
`qs_orders_paid` topic, forever:

```bash
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local <<'SQL'
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

Verify the output topic (drain it via `rpk` inside the broker; `-o :end` reads to the
current end and exits; `--read-committed` reads only committed records — a plain consume
can also show the records of an open or aborted transaction, and with
`TRANSACTIONAL = true` the stream commits its writes):

```bash
docker exec qs-kafka rpk topic consume qs_orders_paid -o :end -f '%v\n' --read-committed
```

`-o :end` is the end at the moment `rpk` starts, so a run soon after `CREATE STREAM`
can show a partial result — re-run it once the stream has worked through the input.
Expect 27 paid records on a fresh stack; one run of K3 adds `E9001` and makes it 28
(the sample below is that state):

```text
{"event_id":"E0001","customer_id":"C0540","amount":2.1553E2,"channel":"mobile"}
{"event_id":"E0002","customer_id":"C0539","amount":2.8616E2,"channel":"social"}
{"event_id":"E0003","customer_id":"C0343","amount":3.798E2,"channel":"email"}
... (28 paid records — the paid subset of qs_orders plus K3's E9001)
```

### Kafka cleanup / re-running

Use these commands to remove the SQL stream and source:

```bash
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local <<'SQL'
DROP STREAM IF EXISTS qs_orders_paid;
DROP SOURCE IF EXISTS qs_orders;
SQL
```

Dropping the source removes its materialized table and local read offsets.
The plain DROP refuses if other dependencies remain. For a full reset,
`DROP SOURCE IF EXISTS qs_orders CASCADE` also removes dependent views,
streams and continuous `INSERT … SELECT` queries, including ones you added.
Check where the replacement source should resume before recreating it.

Dropping the SQL objects leaves the broker's topics and committed group
offsets in place. Recreating K4 with the same stream name uses the same
default group, `otterstax_stream_qs_orders_paid`. `OFFSET_RESET = 'earliest'`
only applies when there is no usable committed offset.

For a clean K4 replay from the start of the retained input, choose a fresh
`GROUP_ID` and an empty output topic. For example, create
`qs_orders_paid_replay`, set K4's `KAFKA_TOPIC` to it, add
`GROUP_ID = 'qs_orders_paid_replay'` in the `WITH` clause and read that topic
when verifying the result. Choose new names for subsequent replays. A replay
reads the retained input, including rows added by K3 if you ran it.

```bash
docker compose exec -T qs-kafka rpk -X brokers=qs-kafka:9092 topic create qs_orders_paid_replay -p 1
```

Restarting only OtterStax preserves the SQL definitions and broker offsets.
After `down`/`up`, successful seeding restores the demonstration topics
from the fixtures. Re-run K1 and K4 to recreate the SQL objects.

---

## Re-running / cleanup of engine state

Examples 1–5 are read-only and always re-runnable. Examples 6–9 create the `local.qs.*`
engine tables. **Run example 6 once** — re-running `CREATE EXTERNAL TABLE` on a table
that already exists fails with `ERROR: CREATE EXTERNAL TABLE failed: collection already
exists` — the engine names the collection, not the table — and leaves the table as it
was. Before replaying examples 6–9, drop just the three demo tables:

```bash
psql -v ON_ERROR_STOP=1 -h localhost -p 8817 -U demo local <<'SQL'
DROP TABLE IF EXISTS local.qs.regions;
DROP TABLE IF EXISTS local.qs.product_costs;
DROP TABLE IF EXISTS local.qs.promotions;
SQL
```

If example 9 was interrupted, `local.qs.scratch` may still exist. Check that
it belongs to the walkthrough before running `DROP TABLE local.qs.scratch`.
If it is your own table, choose a different unused name throughout example 9.

For a **full reset of `local.qs`**, run `DROP SCHEMA IF EXISTS qs CASCADE` — that one
statement is all `examples/quick_start/sql/cleanup.sql` does. CASCADE removes the schema
and all its objects, including any you added, **and dependent objects in other schemas**:
a view that reads a `local.qs` table is dropped with it. The three plain `DROP TABLE`s
above are the surgical route and can themselves be refused while such a dependency
exists (with `ON_ERROR_STOP=1` the script stops there). SQL cleanup leaves backend data
and S3 objects in place. Example 7 overwrites its export when you run it again.

---

## Persistence — what survives a restart

| Action | Backend databases | RustFS | OtterStax and Kafka |
|--------|-------------------|--------|--------------------|
| `docker compose restart qs-otterstax` | Kept | Kept | Container state and broker offsets kept |
| `docker compose stop` | Volumes kept | Objects kept | Containers kept, including engine data and broker state |
| Full `docker compose up --wait` on an existing stack | Named volumes kept | Seed reruns overwrite the three source objects and delete `exports/` | Seed reruns recreate the demo topics. The local SQL objects can still exist if the OtterStax container is reused |
| `docker compose down` then `up` | Named volumes kept | Volume kept. The seed job overwrites the three source objects and deletes `exports/` | Local catalog, tables, Kafka SQL definitions and local offsets discarded. The broker is recreated and the demo topics re-seeded |
| `docker compose down -v` then `up` | Volumes deleted. Demo data rebuilt from fixtures | Volume deleted. Seed objects restored | Same engine and broker reset as `down` |

Whenever a seed job runs, it can change data even if the container or volume
survived. The Kafka seed job deletes and recreates `qs_orders` and
`qs_orders_paid`, then loads the 40 committed fixture records into `qs_orders`.
After a reset, re-run example 6 and K1/K4 to rebuild the demo's local state.

---

## Teardown

- Pause and keep the existing containers and their state with `docker compose stop`.
  To resume, bring the existing backends back to health and then start
  OtterStax separately. Use `--no-deps` to skip the seed jobs and
  `--no-recreate` to keep the existing containers:

  ```bash
  docker compose up --wait --no-deps --no-recreate \
    qs-postgres-shop qs-postgres-catalog qs-mariadb qs-clickhouse qs-rustfs qs-kafka &&
  docker compose up --wait --no-deps --no-recreate qs-otterstax
  ```

- Remove the containers with `docker compose down`. This discards the local
  engine and broker state and keeps the backend and RustFS volumes. The next
  `up` runs the seed jobs.
- Remove containers and all their named volumes with `docker compose down -v`.
  The next `up` rebuilds the demo fixtures.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `up` fails: *port is already allocated* (8817) | The `examples/demo/` stack (or another) holds 8817 | For the demo, see its [pause instructions](../demo/CLAUDE.md#pause-or-reset) for Docker and local mode. |
| `psql: connection refused` on 8817 | OtterStax still starting | Check `docker compose ps qs-otterstax` and retry `SELECT 1` once it is healthy. |
| `local.shop.customers` (or another `local.*` name of a backend table) does not exist | `local.*` names an engine table; a backend table is `alias.namespace.table` | Use the forms in [Backends & federation names](#backends--federation-names): `pgshop.shop.customers`, `pgcat.catalog.products`. |
| My `local.qs.*` tables vanished after `down`/`up` | Engine tables live in the OtterStax container, which `down` removes | Verify the S3 source objects, then run example 6. Use `-f sql/06_external_create.sql` from the quick start directory or `-f examples/quick_start/sql/06_external_create.sql` from the repository root, with `-v ON_ERROR_STOP=1`. |
| `ROUND(x, 2)` errors (`unrecognized function 'round'`) | the engine has no `round` function (rc-7), at any arity | Use plain `SUM()` / `AVG()` (as the examples do) and round in the client. |
| ClickHouse/MariaDB "client not found" on host | You don't have those CLIs | Use the `docker exec` forms in [Directly against each backend](#directly-against-each-backend-optional-sanity-check). |
| `kafka.qs_orders` shows 0 or stale rows | Ingestion is still running, the topic is empty, or K1 kept rows and offsets from before a re-seed | [Verify the Kafka input and K1's local offsets](#verify-the-kafka-input), then retry the `SELECT`. If ingestion still fails, check `docker compose logs qs-otterstax`. |
| K2 join errors `path: 'name' is ambiguous. Use aliases or full path` | `name` exists in both `customers` and `products` — the engine refuses the bare name | Qualify it (`c.name`), or project other columns (`country`/`tier`/`category`), as K2 does. |
| `qs_orders_paid` empty after `CREATE STREAM` | The stream worker is starting, failed, or resumed at a committed offset | Check `docker compose logs qs-otterstax` and [replay K4 with a fresh group](#kafka-cleanup--re-running) if you need to reread the input. |

---

## File reference

| Path | What |
|------|------|
| `compose.yml` | the whole stack (backends + RustFS + Kafka + seed jobs + OtterStax) with named volumes |
| `config.yaml` | OtterStax connection config (2×PG, MariaDB, ClickHouse, S3) — read at startup |
| `examples/quick_start/sql/0[1-9]_*.sql` | the nine core examples; `examples/quick_start/sql/cleanup.sql` resets all of `local.qs` |
| `examples/quick_start/sql/kafka_0[1-4]_*.sql` | the four Kafka examples; `examples/quick_start/sql/kafka_cleanup.sql` drops the source/stream |
| `examples/quick_start/init/postgres_shop/`, `examples/quick_start/init/postgres_catalog/`, `examples/quick_start/init/mariadb/`, `examples/quick_start/init/clickhouse/` | committed DB seed SQL (auto-run on first container init) |
| `examples/quick_start/init/s3/` | committed S3 fixtures (`regions.csv`, `product_costs.parquet`, `promotions.ndjson`) uploaded by `qs-rustfs-init` |
| `examples/quick_start/init/kafka/orders.ndjson` | committed Kafka fixture (40 events) produced to `qs_orders` by `qs-kafka-init` |
| `examples/quick_start/fixtures/generate.py` | **maintainer-only** regenerator for everything under `examples/quick_start/init/`. Not needed to run the demo (needs `faker` + `pyarrow`). |
