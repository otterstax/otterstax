#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

# Deterministic output — tests can assert on shape.
SEED = 42
random.seed(SEED)
fake = Faker("en_US")
Faker.seed(SEED)

SCRIPT_DIR = Path(__file__).parent.resolve()
INIT_DIR = SCRIPT_DIR / "init"
(INIT_DIR / "mariadb").mkdir(parents=True, exist_ok=True)
(INIT_DIR / "postgres").mkdir(parents=True, exist_ok=True)
(INIT_DIR / "clickhouse").mkdir(parents=True, exist_ok=True)
# S3 fixtures — seeded into MinIO by the `demo-minio-init` compose service for
# steps 7-9. Living under `init/` keeps them covered by the existing .gitignore
# rule and means `up.sh` blows them away alongside the other backend init data.
S3_DIR = INIT_DIR / "s3"
S3_DIR.mkdir(parents=True, exist_ok=True)


# Generation parameters

N_CUSTOMERS = 200
N_PRODUCTS  = 50
N_ORDERS    = 1500
N_INVOICES  = 1000
N_SESSIONS  = 2000

CATEGORIES = ["Electronics", "Clothing", "Food", "Home",
              "Sports", "Books", "Toys", "Beauty"]
CHANNELS   = ["web", "mobile", "email", "social", "referral"]

WAREHOUSE_CITIES = [("Tel Aviv", "IL", "6701101"),
                    ("Berlin",   "DE", "10115"),
                    ("New York", "US", "10001")]

EU_LOCATIONS = [
    ("Berlin",   "DE", "10115"),
    ("Munich",   "DE", "80331"),
    ("Paris",    "FR", "75001"),
    ("Lyon",     "FR", "69001"),
    ("Rome",     "IT", "00118"),
    ("Milan",    "IT", "20121"),
    ("Madrid",   "ES", "28001"),
    ("Barcelona","ES", "08001"),
    ("Amsterdam","NL", "1011"),
    ("Rotterdam","NL", "3011"),
]
OTHER_LOCATIONS = [
    ("Tel Aviv", "IL", "6701101"),
    ("New York", "US", "10001"),
    ("San Francisco", "US", "94102"),
    ("London",   "GB", "EC1A"),
    ("Tokyo",    "JP", "100-0001"),
]
ALL_LOCATIONS = EU_LOCATIONS + OTHER_LOCATIONS


# Helpers

def new_uuid() -> str:
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


def sql_str(s: str) -> str:
    """Single-quoted SQL string with safe escaping for MySQL/Postgres."""
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


def random_ts(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


# Time windows
WINDOW_BACKGROUND = (datetime(2025, 11, 1),  datetime(2026, 4, 25))
WINDOW_MONTH      = (datetime(2026, 3, 20),  datetime(2026, 4, 25))
WINDOW_WEEK       = (datetime(2026, 4, 12),  datetime(2026, 4, 19))
WINDOW_SPIKE      = (datetime(2026, 4, 18),  datetime(2026, 4, 19))


# Generate customers

print(f"⚙️  generating {N_CUSTOMERS} customers...")

customers = []
# 60% bronze, 30% silver, 10% gold
tiers = (["bronze"] * int(N_CUSTOMERS * 0.60)
         + ["silver"] * int(N_CUSTOMERS * 0.30)
         + ["gold"]   * (N_CUSTOMERS - int(N_CUSTOMERS * 0.60) - int(N_CUSTOMERS * 0.30)))
random.shuffle(tiers)

# Step 4 needs gold customers in warehouse cities (Tel Aviv / Berlin / NYC).
# Force the first ~half of gold customers into those exact cities so the join
# returns ≥10 rows regardless of N_CUSTOMERS.
gold_quota_in_warehouses = max(10, sum(1 for t in tiers if t == "gold") // 2)
gold_placed_in_warehouse = 0

for i in range(N_CUSTOMERS):
    cid = new_uuid()
    name = fake.name()
    email_local = fake.user_name()
    # 25% emails on @test.* domain so step 2's NOT LIKE filter has work to do.
    if random.random() < 0.25:
        email = f"{email_local}@test.{random.choice(['internal','io','co'])}"
    else:
        email = f"{email_local}@{random.choice(['gmail.com','outlook.com','company.io','example.org'])}"
    tier = tiers[i]
    if tier == "gold" and gold_placed_in_warehouse < gold_quota_in_warehouses:
        city, country, zip_ = random.choice(WAREHOUSE_CITIES)
        gold_placed_in_warehouse += 1
    else:
        city, country, zip_ = random.choice(ALL_LOCATIONS)
    customers.append({
        "id": cid, "name": name, "email": email, "tier": tier,
        "city": city, "country": country, "zip": zip_,
    })


# Generate products

print(f"⚙️  generating {N_PRODUCTS} products...")

products = []
for i in range(N_PRODUCTS):
    pid = new_uuid()
    name = f"{fake.word().capitalize()} {fake.word().capitalize()}"
    category = CATEGORIES[i % len(CATEGORIES)]
    price = round(random.uniform(9.99, 499.99), 2)
    stock = random.randint(0, 1000)
    products.append({"id": pid, "name": name, "category": category,
                     "price": price, "stock": stock})

product_by_id = {p["id"]: p for p in products}


# Generate orders

print(f"⚙️  generating {N_ORDERS} orders...")

# Distribution across the three windows so demo predicates light up:
#   ~10% in spike day (step 1)
#   ~25% in week before, outside spike (steps 3d, 5)
#   ~40% in month back, outside week (step 2)
#   ~25% earlier background
N_SPIKE = int(N_ORDERS * 0.10)
N_WEEK  = int(N_ORDERS * 0.25)
N_MONTH = int(N_ORDERS * 0.40)
N_BG    = N_ORDERS - N_SPIKE - N_WEEK - N_MONTH

ORDER_STATUSES = ["paid"] * 70 + ["shipped"] * 15 + ["pending"] * 10 + ["refunded"] * 5

orders = []
def add_order(ts: datetime):
    oid = new_uuid()
    cust = random.choice(customers)
    prod = random.choice(products)
    amount = round(random.uniform(10, 500), 2)
    status = random.choice(ORDER_STATUSES)
    orders.append({"id": oid, "customer_id": cust["id"], "product_id": prod["id"],
                   "amount": amount, "status": status, "ts": ts,
                   "_category": prod["category"]})  # leak for session correlation

for _ in range(N_SPIKE): add_order(random_ts(*WINDOW_SPIKE))
for _ in range(N_WEEK):
    # Avoid the spike day inside the week window so the buckets stay distinct.
    ts = random_ts(WINDOW_WEEK[0], WINDOW_SPIKE[0])
    add_order(ts)
for _ in range(N_MONTH):
    ts = random_ts(WINDOW_MONTH[0], WINDOW_WEEK[0])
    add_order(ts)
for _ in range(N_BG):
    add_order(random_ts(WINDOW_BACKGROUND[0], WINDOW_MONTH[0]))

print("⚙️  topping up multi-order customers for step 2...")
heavy_customers = random.sample(customers, 50)
for cust in heavy_customers:
    for _ in range(random.randint(3, 6)):
        ts = random_ts(*WINDOW_MONTH)
        prod = random.choice(products)
        oid = new_uuid()
        amount = round(random.uniform(50, 800), 2)
        orders.append({"id": oid, "customer_id": cust["id"], "product_id": prod["id"],
                       "amount": amount, "status": "paid", "ts": ts,
                       "_category": prod["category"]})


# Generate invoices

print(f"⚙️  generating {N_INVOICES} invoices...")

INVOICE_STATUSES = ["paid"] * 80 + ["void"] * 10 + ["pending"] * 10

# Tier-correlated invoice amounts so step 6's HAVING mrr > 1000 splits the tiers.
TIER_AMOUNT = {
    "bronze": (10, 100),
    "silver": (100, 500),
    "gold":   (500, 2000),
}

invoices = []
# 60% in window_month (steps 2, 6 main content), 40% earlier
for i in range(N_INVOICES):
    iid = new_uuid()
    cust = random.choice(customers)
    lo, hi = TIER_AMOUNT[cust["tier"]]
    amount = round(random.uniform(lo, hi), 2)
    status = random.choice(INVOICE_STATUSES)
    if i < int(N_INVOICES * 0.60):
        ts = random_ts(*WINDOW_MONTH)
    else:
        ts = random_ts(WINDOW_BACKGROUND[0], WINDOW_MONTH[0])
    invoices.append({"id": iid, "customer_id": cust["id"],
                     "amount": amount, "status": status, "ts": ts})


# Generate sessions

print(f"⚙️  generating {N_SESSIONS} sessions...")

orders_by_cust = {}
for o in orders:
    orders_by_cust.setdefault(o["customer_id"], []).append(o)

N_S_WEEK  = int(N_SESSIONS * 0.30)
N_S_MONTH = int(N_SESSIONS * 0.30)
N_S_BG    = N_SESSIONS - N_S_WEEK - N_S_MONTH

sessions = []

def random_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def make_session(ts: datetime, want_step5_correlation: bool):
    cust = random.choice(customers)
    roll = random.random()
    bought_orders = [o for o in orders_by_cust.get(cust["id"], [])
                     if WINDOW_WEEK[0] <= o["ts"] < WINDOW_WEEK[1]]
    if roll < 0.30:
        browsed_category = random.choice(CATEGORIES)
        traffic_source = "campaign"
        order_id = random.choice(bought_orders)["id"] if bought_orders and random.random() < 0.5 \
                   else new_uuid()  # ghost order_id otherwise — JOIN drops them
    elif roll < 0.60:
        # Step 5 correlation: prefer a category the same user actually bought
        # in the week window, so JOIN+predicate has matches.
        if bought_orders:
            browsed_category = random.choice(bought_orders)["_category"]
            order_id = random.choice(bought_orders)["id"]
        else:
            browsed_category = random.choice(CATEGORIES)
            order_id = new_uuid()
        traffic_source = "campaign"
    else:
        browsed_category = random.choice(CATEGORIES)
        traffic_source = random.choice(["organic", "direct", "retargeting"])
        order_id = new_uuid()

    channel = random.choice(CHANNELS)  # always present so IS NOT NULL holds
    ip = random_ip()
    # ship_addr: 40% EU (so step_3d filter is meaningful), 60% global
    if random.random() < 0.40:
        ship_city, ship_country, ship_zip = random.choice(EU_LOCATIONS)
    else:
        ship_city, ship_country, ship_zip = random.choice(ALL_LOCATIONS)
    sessions.append({
        "user_id": cust["id"],
        "order_id": order_id,
        "ts": ts,
        "browsed_category": browsed_category,
        "traffic_source": traffic_source,
        "channel": channel,
        "ip": ip,
        "ship_city": ship_city,
        "ship_country": ship_country,
        "ship_zip": ship_zip,
    })

for _ in range(N_S_WEEK):  make_session(random_ts(*WINDOW_WEEK), want_step5_correlation=True)
for _ in range(N_S_MONTH): make_session(random_ts(WINDOW_MONTH[0], WINDOW_WEEK[0]), False)
for _ in range(N_S_BG):    make_session(random_ts(WINDOW_BACKGROUND[0], WINDOW_MONTH[0]), False)


# Emit MariaDB init.sql (mysql.bill)

print(f"📝 writing {INIT_DIR / 'mariadb' / 'init.sql'}...")

mysql_lines = ["""\
CREATE DATABASE IF NOT EXISTS bill;
USE bill;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  order_id    VARCHAR(36)  NOT NULL PRIMARY KEY,
  customer_id VARCHAR(36)  NOT NULL,
  product_id  VARCHAR(36)  NOT NULL,
  amount      DOUBLE NOT NULL,
  status      VARCHAR(16)  NOT NULL,
  ts          TIMESTAMP    NOT NULL,
  INDEX idx_orders_ts (ts),
  INDEX idx_orders_customer (customer_id),
  INDEX idx_orders_product (product_id)
);

DROP TABLE IF EXISTS invoices;
CREATE TABLE invoices (
  invoice_id  VARCHAR(36)  NOT NULL PRIMARY KEY,
  customer_id VARCHAR(36)  NOT NULL,
  amount      DOUBLE NOT NULL,
  status      VARCHAR(16)  NOT NULL,
  ts          TIMESTAMP    NOT NULL,
  INDEX idx_invoices_ts (ts),
  INDEX idx_invoices_customer (customer_id)
);
"""]

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

for batch in chunked(orders, 500):
    values = ",\n  ".join(
        f"({sql_str(o['id'])}, {sql_str(o['customer_id'])}, {sql_str(o['product_id'])}, "
        f"{o['amount']}, {sql_str(o['status'])}, {sql_str(fmt_ts(o['ts']))})"
        for o in batch
    )
    mysql_lines.append(f"INSERT INTO orders (order_id, customer_id, product_id, amount, status, ts) VALUES\n  {values};\n")

for batch in chunked(invoices, 500):
    values = ",\n  ".join(
        f"({sql_str(i['id'])}, {sql_str(i['customer_id'])}, "
        f"{i['amount']}, {sql_str(i['status'])}, {sql_str(fmt_ts(i['ts']))})"
        for i in batch
    )
    mysql_lines.append(f"INSERT INTO invoices (invoice_id, customer_id, amount, status, ts) VALUES\n  {values};\n")

(INIT_DIR / "mariadb" / "init.sql").write_text("\n".join(mysql_lines))


# Emit PostgreSQL init.sql (pg.shop)

print(f"📝 writing {INIT_DIR / 'postgres' / 'init.sql'}...")

pg_lines = ["""\
CREATE DATABASE shop;
\\c shop

CREATE SCHEMA shop;
SET search_path TO shop, public;

CREATE TYPE tier_t AS ENUM ('bronze','silver','gold');

CREATE TABLE customers (
  customer_id  UUID PRIMARY KEY,
  name         TEXT NOT NULL,
  email        TEXT NOT NULL,
  tier         tier_t NOT NULL,
  addr_city    TEXT,
  addr_country CHAR(2),
  addr_zip     TEXT
);
CREATE INDEX idx_customers_tier ON customers(tier);

CREATE TABLE products (
  product_id UUID PRIMARY KEY,
  name       TEXT NOT NULL,
  category   TEXT NOT NULL,
  price      DOUBLE PRECISION NOT NULL,
  stock      INT NOT NULL
);
CREATE INDEX idx_products_category ON products(category);
"""]

for batch in chunked(customers, 500):
    values = ",\n  ".join(
        f"({sql_str(c['id'])}, {sql_str(c['name'])}, {sql_str(c['email'])}, "
        f"{sql_str(c['tier'])}::tier_t, "
        f"{sql_str(c['city'])}, {sql_str(c['country'])}, {sql_str(c['zip'])})"
        for c in batch
    )
    pg_lines.append(f"INSERT INTO customers (customer_id, name, email, tier, addr_city, addr_country, addr_zip) VALUES\n  {values};\n")

for batch in chunked(products, 500):
    values = ",\n  ".join(
        f"({sql_str(p['id'])}, {sql_str(p['name'])}, {sql_str(p['category'])}, "
        f"{p['price']}, {p['stock']})"
        for p in batch
    )
    pg_lines.append(f"INSERT INTO products (product_id, name, category, price, stock) VALUES\n  {values};\n")

(INIT_DIR / "postgres" / "init.sql").write_text("\n".join(pg_lines))


# Emit ClickHouse init.sql (ch.ev)

print(f"📝 writing {INIT_DIR / 'clickhouse' / 'init.sql'}...")

ch_lines = ["""\
CREATE DATABASE IF NOT EXISTS ev;

-- storing as String keeps formats identical and JOINs trivial.
CREATE TABLE ev.sessions (
  user_id          String,
  order_id         String,
  ts               DateTime,
  browsed_category String,
  traffic_source   String,
  props            Tuple(channel String, geo Tuple(ip String)),
  ship_addr        Tuple(country String, city String, zip String)
) ENGINE = MergeTree()
ORDER BY (user_id, ts);
"""]

for s in sessions:
    ch_lines.append(
        f"INSERT INTO ev.sessions VALUES ("
        f"{sql_str(s['user_id'])}, {sql_str(s['order_id'])}, {sql_str(fmt_ts(s['ts']))}, "
        f"{sql_str(s['browsed_category'])}, {sql_str(s['traffic_source'])}, "
        f"({sql_str(s['channel'])}, tuple({sql_str(s['ip'])})), "
        f"({sql_str(s['ship_country'])}, {sql_str(s['ship_city'])}, {sql_str(s['ship_zip'])}));"
    )

(INIT_DIR / "clickhouse" / "init.sql").write_text("\n".join(ch_lines))


print(f"""
✅ Demo init SQL written:
   {INIT_DIR / 'mariadb' / 'init.sql'}      orders={len(orders)} invoices={len(invoices)}
   {INIT_DIR / 'postgres' / 'init.sql'}     customers={len(customers)} products={len(products)}
   {INIT_DIR / 'clickhouse' / 'init.sql'}   sessions={len(sessions)}

   gold customers in warehouse cities: {sum(1 for c in customers if c['tier']=='gold' and c['city'] in ('Tel Aviv','Berlin','New York'))}
   orders in spike day (04-18..04-19): {sum(1 for o in orders if WINDOW_SPIKE[0] <= o['ts'] < WINDOW_SPIKE[1])}
   sessions with traffic_source='campaign': {sum(1 for s in sessions if s['traffic_source']=='campaign')}
   sessions in EU ship_addr:                {sum(1 for s in sessions if s['ship_country'] in ('DE','FR','IT','ES','NL'))}
""")


# ── S3 fixtures for steps 7-9 ────────────────────────────────────────────────
# Two tiny tables seeded into MinIO at `s3://demo-bucket/`:
#   regions.csv   — 6-row dimension (region_id, country, region_name)
#   promos.parquet — 12-row fact     (region_id, promo_code, discount_pct)
# step_7 / step_8 load them as engine-internal tables; step_9 JOINs them and
# COPYs the result back out to s3://demo-bucket/exports/promos_by_region.csv.

import csv as _csv

regions = [
    (1, "US", "North America East"),
    (2, "US", "North America West"),
    (3, "DE", "EU Central"),
    (4, "IL", "Middle East"),
    (5, "JP", "Asia Pacific"),
    (6, "BR", "South America"),
]
with open(S3_DIR / "regions.csv", "w", newline="") as fh:
    w = _csv.writer(fh)
    w.writerow(["region_id", "country", "region_name"])
    for row in regions:
        w.writerow(row)

# Two promo codes per region, seeded discount percentages — deterministic so
# step_9's ORDER BY output is stable across reruns.
promos = []
for rid, country, _ in regions:
    promos.append((rid, f"{country}-SPRING25", round(5.0 + rid * 1.5, 2)))
    promos.append((rid, f"{country}-WINTER25", round(8.0 + rid * 1.0, 2)))

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "region_id":    pa.array([p[0] for p in promos], pa.int64()),
        "promo_code":   pa.array([p[1] for p in promos], pa.string()),
        "discount_pct": pa.array([p[2] for p in promos], pa.float64()),
    })
    pq.write_table(table, S3_DIR / "promos.parquet")
    parquet_note = f"   {S3_DIR / 'promos.parquet'}   promos={len(promos)} (pyarrow / snappy by default)"
except ImportError:
    parquet_note = (
        f"   ⚠️  pyarrow not installed — skipped {S3_DIR / 'promos.parquet'}.\n"
        f"      Install with `pip install pyarrow` (already a transitive dep of the "
        f"otterstax integration suite) so step_8 has something to read."
    )

print(f"""✅ S3 demo fixtures written:
   {S3_DIR / 'regions.csv'}        regions={len(regions)}
{parquet_note}
""")


# ── Kafka fixtures for the streaming demo (examples/demo/kafka/) ──────────────
# Two live order-event topics, seeded into redpanda by examples/demo/kafka/seed.py.
#   demo_orders_live  — the main event feed (features 1-6)
#   demo_orders_intl  — a second feed used only by the fan-in demo (feature 7)
# customer_id is drawn from customers that ALSO appear in ch.ev.sessions, so the
# kafka ⋈ ClickHouse ⋈ Postgres JOIN returns real rows. Every join key is a
# string (UUID) — a string key sidesteps the int32/int64 width footgun the
# top-level CLAUDE.md warns about.

KAFKA_DIR = INIT_DIR / "kafka"
KAFKA_DIR.mkdir(parents=True, exist_ok=True)

import json as _json

# Customers that have at least one session — guarantees the ClickHouse join
# (sessions.user_id = customer_id) matches for every kafka event.
_session_customer_ids = sorted({s["user_id"] for s in sessions})

# Skew toward 'paid' so both the WHERE status='paid' stream and the plain
# ingestion have plenty to show, while keeping a couple of other statuses.
KAFKA_STATUSES = ["paid"] * 6 + ["pending"] * 2 + ["refunded"] * 2
KAFKA_CHANNELS = ["web", "mobile", "email", "social"]


def make_events(n: int, rng_seed: int) -> list:
    rng = random.Random(SEED + rng_seed)
    out = []
    for _ in range(n):
        out.append({
            "event_id":    new_uuid(),
            "customer_id": rng.choice(_session_customer_ids),
            "amount":      round(rng.uniform(10, 500), 2),
            "qty":         rng.randint(1, 5),
            "status":      rng.choice(KAFKA_STATUSES),
            "channel":     rng.choice(KAFKA_CHANNELS),
        })
    return out


N_KAFKA_MAIN = 40
N_KAFKA_INTL = 25
orders_live = make_events(N_KAFKA_MAIN, 1)
orders_intl = make_events(N_KAFKA_INTL, 2)


def write_ndjson(path: Path, rows: list) -> None:
    with open(path, "w") as fh:
        for r in rows:
            fh.write(_json.dumps(r) + "\n")


write_ndjson(KAFKA_DIR / "orders_live.ndjson", orders_live)
write_ndjson(KAFKA_DIR / "orders_intl.ndjson", orders_intl)

_paid_main = sum(1 for e in orders_live if e["status"] == "paid")
print(f"""✅ Kafka demo fixtures written:
   {KAFKA_DIR / 'orders_live.ndjson'}   events={len(orders_live)} (paid={_paid_main})
   {KAFKA_DIR / 'orders_intl.ndjson'}   events={len(orders_intl)}
   distinct customer_ids referenced: {len({e['customer_id'] for e in orders_live + orders_intl})} (all have ch sessions)
""")
