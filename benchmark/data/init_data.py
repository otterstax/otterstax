#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""
Initialise all 6 benchmark databases.

Group A (mariadb1 / postgres1 / clickhouse1): campaigns, impressions, daily_stats
Group B (mariadb2 / postgres2 / clickhouse2): products, orders, events

campaign_id 1-1000 is shared across all 6 DBs so cross-group JOINs always match.
Date/timestamp columns are intentionally omitted until OtterStax supports the
date type translator across all frontends.

The rows are generated once into a saved dataset (``--dataset``, default
``data/dataset/``: ``<db>/<table>.json.gz``) and every run loads that same data,
so runs on different days and different images query identical tables.  The
dataset is generated again only when it is missing, when the bench.yaml sizes or
DATASET_VERSION changed, or with ``--regenerate`` (see dataset_stamp.py).
"""

import argparse
import gzip
import json
import random
import shutil
from pathlib import Path
from faker import Faker
import yaml

import dataset_stamp

# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
_cfg_path = Path(__file__).parent.parent / "bench.yaml"
_cfg = {}
if _cfg_path.exists():
    with _cfg_path.open() as _f:
        _cfg = yaml.safe_load(_f) or {}

# Support both old flat `tables:` key and new `group_a:` / `group_b:` keys.
_ga = _cfg.get("group_a", _cfg.get("tables", {}))
_gb = _cfg.get("group_b", _cfg.get("tables", {}))

NUM_CAMPAIGNS       = _ga.get("num_campaigns",            1_000)
IMPRESSIONS_PER_CAM = _ga.get("impressions_per_campaign",    60)
STATS_PER_CAM       = _ga.get("stats_per_campaign",          60)
BATCH_A             = _ga.get("batch_size",               1_000)

PRODUCTS_PER_CAM    = _gb.get("products_per_campaign",        5)
ORDERS_PER_PRODUCT  = _gb.get("orders_per_product",           1)
EVENTS_PER_CAM      = _gb.get("events_per_campaign",          4)
BATCH_B             = _gb.get("batch_size",               1_000)

BATCH = BATCH_A  # used for group-A inserts

STATUS_CHOICES   = ["active", "paused", "completed"]
CATEGORY_CHOICES = ["electronics", "clothing", "food", "books", "sports",
                    "home", "toys", "beauty"]
EVENT_TYPES = ["click", "view", "add_to_cart", "purchase"]
DEVICES     = ["desktop", "mobile", "tablet"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def batched(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# ---------------------------------------------------------------------------
# Group A data generators
# ---------------------------------------------------------------------------

def gen_campaigns(fake, seed):
    random.seed(seed)
    fake.seed_instance(seed)
    rows = []
    for cid in range(1, NUM_CAMPAIGNS + 1):
        rows.append((
            cid,
            fake.catch_phrase()[:200],
            round(random.uniform(5_000, 500_000), 2),
            random.choice(STATUS_CHOICES),
        ))
    return rows


def gen_impressions(seed):
    random.seed(seed)
    rows = []
    imp_id = 1
    for cid in range(1, NUM_CAMPAIGNS + 1):
        for _ in range(IMPRESSIONS_PER_CAM):
            views  = random.randint(1_000, 50_000)
            clicks = random.randint(10, min(5_000, views))
            rows.append((
                imp_id, cid,
                views, clicks,
                round(views * random.uniform(0.001, 0.01), 4),
            ))
            imp_id += 1
    return rows


def gen_daily_stats(seed):
    random.seed(seed + 1000)
    rows = []
    stat_id = 1
    for cid in range(1, NUM_CAMPAIGNS + 1):
        for _ in range(STATS_PER_CAM):
            spend       = round(random.uniform(10, 5_000), 2)
            revenue     = round(spend * random.uniform(0.5, 3.0), 2)
            conversions = random.randint(0, 500)
            clicks      = random.randint(10, 5_000)
            views       = random.randint(clicks, 50_000)
            rows.append((
                stat_id, cid,
                spend, revenue, conversions,
                round(clicks / views, 6) if views else 0.0,
                round(revenue / spend, 4) if spend else 0.0,
            ))
            stat_id += 1
    return rows

# ---------------------------------------------------------------------------
# Group B data generators
# ---------------------------------------------------------------------------

def gen_products(fake, seed):
    random.seed(seed + 2000)
    fake.seed_instance(seed + 2000)
    rows = []
    pid = 1
    for cid in range(1, NUM_CAMPAIGNS + 1):
        for _ in range(PRODUCTS_PER_CAM):
            rows.append((
                pid, cid,
                fake.catch_phrase()[:200],
                random.choice(CATEGORY_CHOICES),
                round(random.uniform(9.99, 999.99), 2),
                random.randint(0, 10_000),
            ))
            pid += 1
    return rows


def gen_orders(products, seed):
    random.seed(seed + 3000)
    rows = []
    oid = 1
    for pid, cid, _, _, price, _ in products:
        for _ in range(ORDERS_PER_PRODUCT):
            qty = random.randint(1, 20)
            rows.append((
                oid, pid, cid,
                f"user{random.randint(1, 100_000)}@example.com",
                qty, price,
                round(qty * price, 2),
            ))
            oid += 1
    return rows


def gen_events(products, seed):
    random.seed(seed + 4000)
    rows = []
    eid = 1
    prod_by_cam = {}
    for pid, cid, *_ in products:
        prod_by_cam.setdefault(cid, []).append(pid)
    for cid in range(1, NUM_CAMPAIGNS + 1):
        pids = prod_by_cam.get(cid, [1])
        for _ in range(EVENTS_PER_CAM):
            rows.append((
                eid, cid, random.choice(pids),
                random.choice(EVENT_TYPES),
                random.randint(1, 1_000_000),
                random.choice(DEVICES),
            ))
            eid += 1
    return rows

# ---------------------------------------------------------------------------
# MariaDB helpers
# ---------------------------------------------------------------------------

def mysql_conn(host, user, password, database):
    import pymysql
    return pymysql.connect(host=host, port=3306, user=user,
                           password=password, database=database,
                           autocommit=False)


def init_mariadb_group_a(host, user, password, db, tables):
    print(f"  MariaDB group-A  host={host} db={db}")
    conn = mysql_conn(host, user, password, db)
    cur  = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS impressions")
    cur.execute("DROP TABLE IF EXISTS daily_stats")
    cur.execute("DROP TABLE IF EXISTS campaigns")

    cur.execute("""
        CREATE TABLE campaigns (
            campaign_id   INT PRIMARY KEY,
            campaign_name VARCHAR(255),
            budget        DECIMAL(12,2),
            status        VARCHAR(16)
        )
    """)
    cur.execute("""
        CREATE TABLE impressions (
            impression_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            campaign_id   INT,
            views         INT,
            clicks        INT,
            cost          DECIMAL(10,4)
        )
    """)
    cur.execute("""
        CREATE TABLE daily_stats (
            stat_id          BIGINT PRIMARY KEY AUTO_INCREMENT,
            campaign_id      INT,
            total_spend      DECIMAL(12,2),
            total_revenue    DECIMAL(12,2),
            conversion_count INT,
            ctr              FLOAT,
            roas             FLOAT
        )
    """)

    camps = tables["campaigns"]
    for chunk in batched(camps, BATCH):
        cur.executemany(
            "INSERT INTO campaigns VALUES (%s,%s,%s,%s)", chunk)
    conn.commit()

    imps = tables["impressions"]
    for chunk in batched(imps, BATCH):
        cur.executemany(
            "INSERT INTO impressions (campaign_id,views,clicks,cost) "
            "VALUES (%s,%s,%s,%s)",
            [(r[1], r[2], r[3], r[4]) for r in chunk])
    conn.commit()

    stats = tables["daily_stats"]
    for chunk in batched(stats, BATCH):
        cur.executemany(
            "INSERT INTO daily_stats "
            "(campaign_id,total_spend,total_revenue,conversion_count,ctr,roas) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            [(r[1], r[2], r[3], r[4], r[5], r[6]) for r in chunk])
    conn.commit()
    conn.close()
    print(f"    campaigns={NUM_CAMPAIGNS} impressions={len(imps)} daily_stats={len(stats)}")


def init_mariadb_group_b(host, user, password, db, tables):
    print(f"  MariaDB group-B  host={host} db={db}")
    conn = mysql_conn(host, user, password, db)
    cur  = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS orders")
    cur.execute("DROP TABLE IF EXISTS events")
    cur.execute("DROP TABLE IF EXISTS products")
    cur.execute("DROP TABLE IF EXISTS bench_inserts")

    # Target of mysql_insert, empty here so startup discovery registers it. No key:
    # the benchmark reuses ids across insert sizes and frontends.
    cur.execute("""
        CREATE TABLE bench_inserts (
            id          BIGINT,
            campaign_id INT,
            note        VARCHAR(64),
            amount      DOUBLE
        )
    """)
    cur.execute("""
        CREATE TABLE products (
            product_id   INT PRIMARY KEY AUTO_INCREMENT,
            campaign_id  INT,
            product_name VARCHAR(255),
            category     VARCHAR(100),
            price        DECIMAL(10,2),
            stock_qty    INT
        )
    """)
    cur.execute("""
        CREATE TABLE orders (
            order_id       INT PRIMARY KEY AUTO_INCREMENT,
            product_id     INT,
            campaign_id    INT,
            customer_email VARCHAR(255),
            quantity       INT,
            unit_price     DECIMAL(10,2),
            total_price    DECIMAL(12,2)
        )
    """)
    cur.execute("""
        CREATE TABLE events (
            event_id    BIGINT PRIMARY KEY AUTO_INCREMENT,
            campaign_id INT,
            product_id  INT,
            event_type  VARCHAR(32),
            user_id     BIGINT,
            device      VARCHAR(16)
        )
    """)

    prods = tables["products"]
    for chunk in batched(prods, BATCH_B):
        cur.executemany(
            "INSERT INTO products (product_id,campaign_id,product_name,category,price,stock_qty) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            chunk)
    conn.commit()

    ords = tables["orders"]
    for chunk in batched(ords, BATCH_B):
        cur.executemany(
            "INSERT INTO orders "
            "(product_id,campaign_id,customer_email,quantity,unit_price,total_price) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            [(r[1], r[2], r[3], r[4], r[5], r[6]) for r in chunk])
    conn.commit()

    evts = tables["events"]
    for chunk in batched(evts, BATCH_B):
        cur.executemany(
            "INSERT INTO events (campaign_id,product_id,event_type,user_id,device) "
            "VALUES (%s,%s,%s,%s,%s)",
            [(r[1], r[2], r[3], r[4], r[5]) for r in chunk])
    conn.commit()
    conn.close()
    print(f"    products={len(prods)} orders={len(ords)} events={len(evts)}")

# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def pg_conn(host, user, password, dbname):
    import psycopg2
    return psycopg2.connect(host=host, port=5432, user=user,
                            password=password, dbname=dbname,
                            connect_timeout=30)


def _pg_terminate_other_connections(conn):
    """Kill any other backends connected to the same DB so DROP TABLE doesn't block."""
    cur = conn.cursor()
    cur.execute("""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = current_database() AND pid <> pg_backend_pid()
    """)
    conn.commit()


def init_postgres_group_a(host, user, password, db, tables):
    print(f"  PostgreSQL group-A  host={host} db={db}")
    conn = pg_conn(host, user, password, db)
    conn.autocommit = False
    _pg_terminate_other_connections(conn)
    cur  = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS impressions")
    cur.execute("DROP TABLE IF EXISTS daily_stats")
    cur.execute("DROP TABLE IF EXISTS campaigns")
    conn.commit()

    cur.execute("""
        CREATE TABLE campaigns (
            campaign_id   INT PRIMARY KEY,
            campaign_name VARCHAR(255),
            budget        NUMERIC(12,2),
            status        VARCHAR(16)
        )
    """)
    cur.execute("""
        CREATE TABLE impressions (
            impression_id BIGSERIAL PRIMARY KEY,
            campaign_id   INT,
            views         INT,
            clicks        INT,
            cost          NUMERIC(10,4)
        )
    """)
    cur.execute("""
        CREATE TABLE daily_stats (
            stat_id          BIGSERIAL PRIMARY KEY,
            campaign_id      INT,
            total_spend      NUMERIC(12,2),
            total_revenue    NUMERIC(12,2),
            conversion_count INT,
            ctr              FLOAT,
            roas             FLOAT
        )
    """)
    conn.commit()

    camps = tables["campaigns"]
    for chunk in batched(camps, BATCH):
        cur.executemany(
            "INSERT INTO campaigns VALUES (%s,%s,%s,%s)", chunk)
    conn.commit()

    imps = tables["impressions"]
    for chunk in batched(imps, BATCH):
        cur.executemany(
            "INSERT INTO impressions (campaign_id,views,clicks,cost) "
            "VALUES (%s,%s,%s,%s)",
            [(r[1], r[2], r[3], r[4]) for r in chunk])
    conn.commit()

    stats = tables["daily_stats"]
    for chunk in batched(stats, BATCH):
        cur.executemany(
            "INSERT INTO daily_stats "
            "(campaign_id,total_spend,total_revenue,conversion_count,ctr,roas) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            [(r[1], r[2], r[3], r[4], r[5], r[6]) for r in chunk])
    conn.commit()
    conn.close()
    print(f"    campaigns={NUM_CAMPAIGNS} impressions={len(imps)} daily_stats={len(stats)}")


def init_postgres_group_b(host, user, password, db, tables):
    print(f"  PostgreSQL group-B  host={host} db={db}")
    conn = pg_conn(host, user, password, db)
    conn.autocommit = False
    _pg_terminate_other_connections(conn)
    cur  = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS orders")
    cur.execute("DROP TABLE IF EXISTS events")
    cur.execute("DROP TABLE IF EXISTS products")
    cur.execute("DROP TABLE IF EXISTS bench_inserts")
    conn.commit()

    # Target of pg_insert, empty here so startup discovery registers it. No key:
    # the benchmark reuses ids across insert sizes and frontends.
    cur.execute("""
        CREATE TABLE bench_inserts (
            id          BIGINT,
            campaign_id INT,
            note        VARCHAR(64),
            amount      DOUBLE PRECISION
        )
    """)
    cur.execute("""
        CREATE TABLE products (
            product_id   SERIAL PRIMARY KEY,
            campaign_id  INT,
            product_name VARCHAR(255),
            category     VARCHAR(100),
            price        NUMERIC(10,2),
            stock_qty    INT
        )
    """)
    cur.execute("""
        CREATE TABLE orders (
            order_id       SERIAL PRIMARY KEY,
            product_id     INT,
            campaign_id    INT,
            customer_email VARCHAR(255),
            quantity       INT,
            unit_price     NUMERIC(10,2),
            total_price    NUMERIC(12,2)
        )
    """)
    cur.execute("""
        CREATE TABLE events (
            event_id    BIGSERIAL PRIMARY KEY,
            campaign_id INT,
            product_id  INT,
            event_type  VARCHAR(32),
            user_id     BIGINT,
            device      VARCHAR(16)
        )
    """)
    conn.commit()

    prods = tables["products"]
    for chunk in batched(prods, BATCH_B):
        cur.executemany(
            "INSERT INTO products (product_id,campaign_id,product_name,category,price,stock_qty) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            chunk)
    conn.commit()

    ords = tables["orders"]
    for chunk in batched(ords, BATCH_B):
        cur.executemany(
            "INSERT INTO orders "
            "(product_id,campaign_id,customer_email,quantity,unit_price,total_price) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            [(r[1], r[2], r[3], r[4], r[5], r[6]) for r in chunk])
    conn.commit()

    evts = tables["events"]
    for chunk in batched(evts, BATCH_B):
        cur.executemany(
            "INSERT INTO events (campaign_id,product_id,event_type,user_id,device) "
            "VALUES (%s,%s,%s,%s,%s)",
            [(r[1], r[2], r[3], r[4], r[5]) for r in chunk])
    conn.commit()
    conn.close()
    print(f"    products={len(prods)} orders={len(ords)} events={len(evts)}")

# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------

def ch_conn(host, user, password, database):
    from clickhouse_driver import Client
    return Client(host=host, port=9000, user=user,
                  password=password, database=database)


def init_clickhouse_group_a(host, user, password, db, tables):
    print(f"  ClickHouse group-A  host={host} db={db}")
    client = ch_conn(host, user, password, db)

    client.execute("DROP TABLE IF EXISTS impressions")
    client.execute("DROP TABLE IF EXISTS daily_stats")
    client.execute("DROP TABLE IF EXISTS campaigns")

    client.execute("""
        CREATE TABLE campaigns (
            campaign_id   UInt32,
            campaign_name String,
            budget        Float64,
            status        LowCardinality(String)
        ) ENGINE = MergeTree() ORDER BY campaign_id
    """)
    client.execute("""
        CREATE TABLE impressions (
            impression_id UInt64,
            campaign_id   UInt32,
            views         UInt32,
            clicks        UInt32,
            cost          Float64
        ) ENGINE = MergeTree() ORDER BY campaign_id
    """)
    client.execute("""
        CREATE TABLE daily_stats (
            stat_id          UInt64,
            campaign_id      UInt32,
            total_spend      Float64,
            total_revenue    Float64,
            conversion_count UInt32,
            ctr              Float64,
            roas             Float64
        ) ENGINE = MergeTree() ORDER BY campaign_id
    """)

    camps = tables["campaigns"]
    for chunk in batched(camps, BATCH):
        client.execute("INSERT INTO campaigns VALUES", chunk)

    imps = tables["impressions"]
    for chunk in batched(imps, BATCH):
        client.execute("INSERT INTO impressions VALUES", chunk)

    stats = tables["daily_stats"]
    for chunk in batched(stats, BATCH):
        client.execute("INSERT INTO daily_stats VALUES", chunk)

    print(f"    campaigns={NUM_CAMPAIGNS} impressions={len(imps)} daily_stats={len(stats)}")


def init_clickhouse_group_b(host, user, password, db, tables):
    print(f"  ClickHouse group-B  host={host} db={db}")
    client = ch_conn(host, user, password, db)

    client.execute("DROP TABLE IF EXISTS orders")
    client.execute("DROP TABLE IF EXISTS events")
    client.execute("DROP TABLE IF EXISTS products")
    client.execute("DROP TABLE IF EXISTS bench_inserts")

    client.execute("""
        CREATE TABLE products (
            product_id   UInt32,
            campaign_id  UInt32,
            product_name String,
            category     LowCardinality(String),
            price        Float64,
            stock_qty    UInt32
        ) ENGINE = MergeTree() ORDER BY (campaign_id, product_id)
    """)
    client.execute("""
        CREATE TABLE orders (
            order_id       UInt32,
            product_id     UInt32,
            campaign_id    UInt32,
            customer_email String,
            quantity       UInt32,
            unit_price     Float64,
            total_price    Float64
        ) ENGINE = MergeTree() ORDER BY campaign_id
    """)
    client.execute("""
        CREATE TABLE events (
            event_id    UInt64,
            campaign_id UInt32,
            product_id  UInt32,
            event_type  LowCardinality(String),
            user_id     UInt64,
            device      LowCardinality(String)
        ) ENGINE = MergeTree() ORDER BY campaign_id
    """)
    # Target of the ch_insert benchmark (benchmarks/clickhouse/ch_insert.py). Created
    # empty here, before OtterStax starts, so startup discovery registers it.
    # Signed columns: the benchmark's INSERT literals are plain integers.
    client.execute("""
        CREATE TABLE bench_inserts (
            id          Int64,
            campaign_id Int32,
            note        String,
            amount      Float64
        ) ENGINE = MergeTree() ORDER BY id
    """)

    prods = tables["products"]
    for chunk in batched(prods, BATCH_B):
        client.execute("INSERT INTO products VALUES", chunk)

    ords = tables["orders"]
    for chunk in batched(ords, BATCH_B):
        client.execute("INSERT INTO orders VALUES", chunk)

    evts = tables["events"]
    for chunk in batched(evts, BATCH_B):
        client.execute("INSERT INTO events VALUES", chunk)

    print(f"    products={len(prods)} orders={len(ords)} events={len(evts)}")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

# The six databases: name -> (host, user, password, group, seed, loader).
DATABASES = {
    "benchdb1": ("bench_mariadb1",    "user1",  "password1",  "a", 1, init_mariadb_group_a),
    "benchpg1": ("bench_postgres1",   "pguser", "pgpassword", "a", 2, init_postgres_group_a),
    "benchch1": ("bench_clickhouse1", "chuser", "chpassword", "a", 3, init_clickhouse_group_a),
    "benchdb2": ("bench_mariadb2",    "user2",  "password2",  "b", 4, init_mariadb_group_b),
    "benchpg2": ("bench_postgres2",   "pguser", "pgpassword", "b", 5, init_postgres_group_b),
    "benchch2": ("bench_clickhouse2", "chuser", "chpassword", "b", 6, init_clickhouse_group_b),
}
GROUP_TABLES = {
    "a": ("campaigns", "impressions", "daily_stats"),
    "b": ("products", "orders", "events"),
}

# Bump when a generator changes, so saved datasets are generated again.
DATASET_VERSION = 1
STAMP = "dataset"


def dataset_settings():
    return {
        "version": DATASET_VERSION,
        "seeds": {name: db[4] for name, db in DATABASES.items()},
        "num_campaigns": NUM_CAMPAIGNS,
        "impressions_per_campaign": IMPRESSIONS_PER_CAM,
        "stats_per_campaign": STATS_PER_CAM,
        "products_per_campaign": PRODUCTS_PER_CAM,
        "orders_per_product": ORDERS_PER_PRODUCT,
        "events_per_campaign": EVENTS_PER_CAM,
    }


def dataset_files():
    return [f"{name}/{table}.json.gz"
            for name, db in DATABASES.items() for table in GROUP_TABLES[db[3]]]


def generate_tables(group, seed):
    fake = Faker("en_US")
    if group == "a":
        return {"campaigns": gen_campaigns(fake, seed),
                "impressions": gen_impressions(seed),
                "daily_stats": gen_daily_stats(seed)}
    products = gen_products(fake, seed)
    return {"products": products,
            "orders": gen_orders(products, seed),
            "events": gen_events(products, seed)}


def generate_dataset(root):
    dataset_stamp.invalidate(root, STAMP)
    for name, db in DATABASES.items():
        shutil.rmtree(root / name, ignore_errors=True)
        (root / name).mkdir(parents=True)
        for table, rows in generate_tables(db[3], db[4]).items():
            with gzip.open(root / name / f"{table}.json.gz", "wt") as f:
                json.dump(rows, f)
    dataset_stamp.mark_current(root, STAMP, dataset_settings())


def load_tables(root, name):
    tables = {}
    for table in GROUP_TABLES[DATABASES[name][3]]:
        with gzip.open(root / name / f"{table}.json.gz", "rt") as f:
            tables[table] = [tuple(row) for row in json.load(f)]
    return tables


def main():
    p = argparse.ArgumentParser(description="Load the benchmark data into the six backend databases")
    p.add_argument("--dataset", type=Path, default=Path(__file__).parent / "dataset",
                   help="Saved dataset directory (default: data/dataset)")
    p.add_argument("--regenerate", action="store_true",
                   help="Generate the dataset again even when the saved one is current")
    args = p.parse_args()

    print("=== OtterStax Benchmark Data Initialisation ===\n")
    args.dataset.mkdir(parents=True, exist_ok=True)
    if not args.regenerate and dataset_stamp.is_current(args.dataset, STAMP, dataset_settings(), dataset_files()):
        print(f"Reusing the saved dataset in {args.dataset}")
    else:
        print(f"Generating the dataset into {args.dataset}")
        generate_dataset(args.dataset)

    for name, (host, user, password, group, _, init) in DATABASES.items():
        if name == "benchdb2":
            print("\nGroup B — products / orders / events")
        elif name == "benchdb1":
            print("\nGroup A — campaigns / impressions / daily_stats")
        init(host, user, password, name, load_tables(args.dataset, name))

    print("\n=== Initialisation complete ===")


if __name__ == "__main__":
    main()
