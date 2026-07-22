#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# Maintainer-only generator for the quick-start demo fixtures.
#
#   NOT part of the run path. `docker compose up` uses the committed output of
#   this script (examples/quick_start/init/**). Re-run it only when you want to
#   change the data — then `git add` the regenerated files.
#
#   Needs: faker, pyarrow.  From repo root:  .venv/bin/python \
#       examples/quick_start/fixtures/generate.py
#
# Emits (all under examples/quick_start/init/):
#   postgres_shop/init.sql      customers   (800 x 4)   PG #1  db=shop
#   postgres_catalog/init.sql   products    (500 x 4)   PG #2  db=catalog
#   mariadb/init.sql            orders      (1000 x 5)  MariaDB db=ops
#   clickhouse/init.sql         pageviews   (1000 x 4)  ClickHouse db=analytics
#   s3/regions.csv              regions     (12 x 2)    csv
#   s3/product_costs.parquet    costs       (500 x 2)   parquet
#   s3/promotions.ndjson        promotions  (60 x 3)    ndjson
#
# Every cross-backend JOIN key is a STRING (customer_id/product_id/order_id) to
# sidestep the int32/int64 silent zero-row JOIN trap (see repo FIX_JOIN.md).

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

SEED = 7
random.seed(SEED)
fake = Faker("en_US")
Faker.seed(SEED)

INIT = Path(__file__).resolve().parent.parent / "init"
for sub in ("postgres_shop", "postgres_catalog", "mariadb", "clickhouse", "s3", "kafka"):
    (INIT / sub).mkdir(parents=True, exist_ok=True)

# ── parameters ───────────────────────────────────────────────────────────────
N_CUSTOMERS = 800
N_PRODUCTS = 500
N_ORDERS = 1000
N_PAGEVIEWS = 1000

# country -> region (regions.csv is the s3 csv dimension; customers.country ⋈ it)
REGIONS = {
    "US": "North America",
    "CA": "North America",
    "BR": "South America",
    "DE": "EU Central",
    "FR": "EU West",
    "NL": "EU West",
    "IT": "EU South",
    "ES": "EU South",
    "GB": "UK & Ireland",
    "IL": "Middle East",
    "JP": "Asia Pacific",
    "AU": "Asia Pacific",
}
COUNTRIES = list(REGIONS.keys())
CATEGORIES = ["Electronics", "Clothing", "Food", "Home",
              "Sports", "Books", "Toys", "Beauty"]
CHANNELS = ["web", "mobile", "email", "social"]
ORDER_STATUS = ["paid"] * 70 + ["shipped"] * 15 + ["pending"] * 10 + ["refunded"] * 5


def sql_str(s) -> str:
    """Single-quoted, escaped SQL string literal (MySQL/PG/CH-safe)."""
    return "'" + str(s).replace("\\", "\\\\").replace("'", "''") + "'"


def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ── customers (PG #1) ────────────────────────────────────────────────────────
customers = []
for i in range(N_CUSTOMERS):
    cid = f"C{i + 1:04d}"
    tier = random.choices(["bronze", "silver", "gold"], weights=[60, 30, 10])[0]
    customers.append({
        "id": cid,
        "name": fake.name(),
        "country": random.choice(COUNTRIES),
        "tier": tier,
    })
# Make example 6's UPDATE visibly change something: C0001/C0002 start non-gold.
customers[0]["tier"] = "bronze"
customers[1]["tier"] = "silver"

# ── products (PG #2) ─────────────────────────────────────────────────────────
products = []
for i in range(N_PRODUCTS):
    pid = f"P{i + 1:04d}"
    price = round(random.uniform(9.99, 499.99), 2)
    products.append({
        "id": pid,
        "name": f"{fake.word().capitalize()} {fake.word().capitalize()}",
        "category": CATEGORIES[i % len(CATEGORIES)],
        "price": price,
    })

# ── orders (MariaDB) ─────────────────────────────────────────────────────────
orders = []
for i in range(N_ORDERS):
    oid = f"O{i + 1:05d}"
    c = random.choice(customers)
    p = random.choice(products)
    orders.append({
        "id": oid,
        "customer_id": c["id"],
        "product_id": p["id"],
        "amount": round(random.uniform(50, 500), 2),
        "status": random.choice(ORDER_STATUS),
    })

# ── pageviews (ClickHouse) ───────────────────────────────────────────────────
# ~75% derived from a real order so the (customer_id, product_id) 2-key JOIN in
# examples 5 & 10 matches; the rest are random noise (won't join — realistic).
base_ts = datetime(2026, 6, 1)
pageviews = []
for i in range(N_PAGEVIEWS):
    if random.random() < 0.75:
        o = random.choice(orders)
        cust_id, prod_id = o["customer_id"], o["product_id"]
    else:
        cust_id = random.choice(customers)["id"]
        prod_id = random.choice(products)["id"]
    ts = base_ts + timedelta(seconds=random.randint(0, 30 * 24 * 3600))
    pageviews.append({
        "customer_id": cust_id,
        "product_id": prod_id,
        "channel": random.choice(CHANNELS),
        "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
    })

# ── s3: regions.csv ──────────────────────────────────────────────────────────
with open(INIT / "s3" / "regions.csv", "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["country", "region_name"])
    for country, region in REGIONS.items():
        w.writerow([country, region])

# ── s3: product_costs.parquet (unit_cost < price → positive margin) ──────────
costs = [{"product_id": p["id"], "unit_cost": round(p["price"] * random.uniform(0.4, 0.7), 2)}
         for p in products]
import pyarrow as pa
import pyarrow.parquet as pq
pq.write_table(
    pa.table({
        "product_id": pa.array([c["product_id"] for c in costs], pa.string()),
        "unit_cost": pa.array([c["unit_cost"] for c in costs], pa.float64()),
    }),
    INIT / "s3" / "product_costs.parquet",
)

# ── s3: promotions.ndjson (some discount_pct < 5 so example 8's DELETE bites) ─
promos = []
for p in random.sample(products, 30):
    promos.append({"product_id": p["id"], "promo_code": f"{p['id']}-SPR", "discount_pct": round(random.uniform(2, 25), 1)})
    promos.append({"product_id": p["id"], "promo_code": f"{p['id']}-WIN", "discount_pct": round(random.uniform(2, 25), 1)})
with open(INIT / "s3" / "promotions.ndjson", "w") as fh:
    for row in promos:
        fh.write(json.dumps(row) + "\n")

# ── PG #1: customers ─────────────────────────────────────────────────────────
# Tables live in a schema whose name equals the database (shop.shop) so the
# clean 3-part federated form `pgshop.shop.customers` resolves — OtterStax
# promotes `alias.db.table` to `alias.db.<db-as-schema>.table` (matches the
# demo's db==schema==shop). Config `schema:` must agree.
pg1 = ["""\
CREATE DATABASE shop;
\\c shop

CREATE SCHEMA shop;

CREATE TABLE shop.customers (
  customer_id TEXT PRIMARY KEY,
  name        TEXT   NOT NULL,
  country     CHAR(2) NOT NULL,
  tier        TEXT   NOT NULL
);
"""]
for batch in chunked(customers, 500):
    vals = ",\n  ".join(
        f"({sql_str(c['id'])}, {sql_str(c['name'])}, {sql_str(c['country'])}, {sql_str(c['tier'])})"
        for c in batch)
    pg1.append(f"INSERT INTO shop.customers (customer_id, name, country, tier) VALUES\n  {vals};\n")
(INIT / "postgres_shop" / "init.sql").write_text("\n".join(pg1))

# ── PG #2: products ──────────────────────────────────────────────────────────
pg2 = ["""\
CREATE DATABASE catalog;
\\c catalog

CREATE SCHEMA catalog;

CREATE TABLE catalog.products (
  product_id TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  category   TEXT NOT NULL,
  price      DOUBLE PRECISION NOT NULL
);
"""]
for batch in chunked(products, 500):
    vals = ",\n  ".join(
        f"({sql_str(p['id'])}, {sql_str(p['name'])}, {sql_str(p['category'])}, {p['price']})"
        for p in batch)
    pg2.append(f"INSERT INTO catalog.products (product_id, name, category, price) VALUES\n  {vals};\n")
(INIT / "postgres_catalog" / "init.sql").write_text("\n".join(pg2))

# ── MariaDB: orders ──────────────────────────────────────────────────────────
my = ["""\
CREATE DATABASE IF NOT EXISTS ops;
USE ops;

CREATE TABLE orders (
  order_id    VARCHAR(8)  NOT NULL PRIMARY KEY,
  customer_id VARCHAR(8)  NOT NULL,
  product_id  VARCHAR(8)  NOT NULL,
  amount      DOUBLE      NOT NULL,
  status      VARCHAR(16) NOT NULL,
  INDEX idx_orders_customer (customer_id),
  INDEX idx_orders_product  (product_id)
);
"""]
for batch in chunked(orders, 500):
    vals = ",\n  ".join(
        f"({sql_str(o['id'])}, {sql_str(o['customer_id'])}, {sql_str(o['product_id'])}, "
        f"{o['amount']}, {sql_str(o['status'])})"
        for o in batch)
    my.append(f"INSERT INTO orders (order_id, customer_id, product_id, amount, status) VALUES\n  {vals};\n")
(INIT / "mariadb" / "init.sql").write_text("\n".join(my))

# ── ClickHouse: pageviews ────────────────────────────────────────────────────
ch = ["""\
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE analytics.pageviews (
  customer_id String,
  product_id  String,
  channel     String,
  event_ts    DateTime
) ENGINE = MergeTree()
ORDER BY (customer_id, event_ts);
"""]
# ClickHouse's init runner parses INSERT ... VALUES inline; newlines between
# rows terminate the statement early. Keep each multi-row INSERT on ONE line.
for batch in chunked(pageviews, 500):
    vals = ", ".join(
        f"({sql_str(v['customer_id'])}, {sql_str(v['product_id'])}, "
        f"{sql_str(v['channel'])}, {sql_str(v['event_ts'])})"
        for v in batch)
    ch.append(f"INSERT INTO analytics.pageviews (customer_id, product_id, channel, event_ts) VALUES {vals};")
(INIT / "clickhouse" / "init.sql").write_text("\n".join(ch))

# ── kafka: orders.ndjson (produced to topic qs_orders by qs-kafka-init) ───────
# Live order events. customer_id / product_id are drawn from the existing
# customers / products so the federated Kafka JOIN (example K2) matches real
# backend rows. One JSON object per line — rpk produces one message per line.
N_KAFKA = 40
kafka_orders = []
for i in range(N_KAFKA):
    c = random.choice(customers)
    p = random.choice(products)
    kafka_orders.append({
        "event_id": f"E{i + 1:04d}",
        "customer_id": c["id"],
        "product_id": p["id"],
        "amount": round(random.uniform(10, 500), 2),
        "status": random.choice(ORDER_STATUS),
        "channel": random.choice(CHANNELS),
    })
with open(INIT / "kafka" / "orders.ndjson", "w") as fh:
    for row in kafka_orders:
        fh.write(json.dumps(row) + "\n")

_kafka_paid = sum(1 for o in kafka_orders if o["status"] == "paid")
print(f"""✅ quick-start fixtures written under {INIT}
   postgres_shop/init.sql      customers={len(customers)}
   postgres_catalog/init.sql   products={len(products)}
   mariadb/init.sql            orders={len(orders)}
   clickhouse/init.sql         pageviews={len(pageviews)}
   s3/regions.csv              regions={len(REGIONS)}
   s3/product_costs.parquet    costs={len(costs)}
   s3/promotions.ndjson        promotions={len(promos)} (below-5% discounts: {sum(1 for p in promos if p['discount_pct'] < 5)})
   kafka/orders.ndjson         events={len(kafka_orders)} (paid={_kafka_paid})
""")
