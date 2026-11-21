# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Cross-backend INNER JOIN tests via PostgreSQL wire protocol (port 8817).

Same query set as test_cross_backend_queries_mysql.py but issued over the PostgreSQL
wire protocol. Running both lets you isolate frontend-specific issues.

Covers:
  - MySQL + PostgreSQL JOINs
  - MySQL + ClickHouse JOINs
  - PostgreSQL + ClickHouse JOINs
  - MySQL + PostgreSQL + ClickHouse triple JOINs
  - triple JOINs in reverse order and SELECT * over same-named keys
"""

import sys
import argparse
import psycopg2

import config


def make_config(local=False):
    host = config.get_host(local)
    return {
        'host': host,
        'port': config.PG_PORT,
        'user': 'testuser',
        'password': 'testpass',
        'dbname': 'local',  # the session's place: the local database
    }


def run_query(cfg, query):
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()
    finally:
        conn.close()


def run_exec(cfg, query):
    """A statement with no result set (DDL / INSERT): remote DDL runs only in
    autocommit, so it takes its own autocommit connection."""
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(query)
    finally:
        conn.close()


def run_query_columns(cfg, query):
    """A SELECT read together with its column names (a JOIN's SELECT * carries
    each side's key, so names repeat)."""
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(query)
        return [d[0] for d in cur.description], cur.fetchall()
    finally:
        conn.close()


def test_mysql_pg_joins(cfg):
    print(f"\n{'='*70}")
    print("Cross-Backend INNER JOIN Tests — MySQL + PostgreSQL (PG wire)")
    print(f"{'='*70}")

    print("\n-- Test 1: products (PG) INNER JOIN campaigns (MySQL)")
    rows = run_query(cfg, """
        SELECT p.product_id, p.product_name, p.price, c.campaign_name
        FROM products.public.products p
        JOIN campaigns.db1.campaigns c ON p.campaign_id = c.campaign_id
        LIMIT 10
    """)
    assert len(rows) > 0, "PG JOIN MySQL returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    product_id={r[0]}, product={r[1]}, price={r[2]}, campaign={r[3]}")

    print("\n-- Test 2: campaigns (MySQL) INNER JOIN products (PG)")
    rows = run_query(cfg, """
        SELECT c.campaign_name, c.budget, p.product_name, p.price
        FROM campaigns.db1.campaigns c
        JOIN products.public.products p ON c.campaign_id = p.campaign_id
        LIMIT 10
    """)
    assert len(rows) > 0, "MySQL JOIN PG returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    campaign={r[0]}, budget={r[1]}, product={r[2]}, price={r[3]}")

    print("\n-- Test 3: INNER JOIN + WHERE (price > 100)")
    rows = run_query(cfg, """
        SELECT p.product_name, p.price, c.campaign_name, c.budget
        FROM products.public.products p
        JOIN campaigns.db1.campaigns c ON p.campaign_id = c.campaign_id
        WHERE p.price > 100
        LIMIT 10
    """)
    assert len(rows) > 0, "MySQL+PG JOIN with WHERE returned no rows"
    assert all(r[1] > 100 for r in rows), "WHERE price > 100 not respected"
    print(f"  ✓ {len(rows)} rows, all price > 100")

    # Cross-backend GROUP BY aggregation
    print("\n-- Test 4: INNER JOIN + GROUP BY aggregation")
    rows = run_query(cfg, """
        SELECT c.campaign_name,
               COUNT(p.product_id) as product_count,
               AVG(p.price) as avg_product_price
        FROM campaigns.db1.campaigns c
        INNER JOIN products.public.products p ON c.campaign_id = p.campaign_id
        GROUP BY c.campaign_name
        ORDER BY product_count DESC
        LIMIT 10
    """)
    assert len(rows) > 0, "MySQL+PG GROUP BY aggregation returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        avg_p = f"{r[2]:.2f}" if r[2] is not None else "NULL"
        print(f"    campaign={r[0]}, products={r[1]}, avg_price={avg_p}")

    print(f"\n✅ MySQL + PostgreSQL JOIN Tests PASSED")


def test_mysql_ch_joins(cfg):
    print(f"\n{'='*70}")
    print("Cross-Backend INNER JOIN Tests — MySQL + ClickHouse (PG wire)")
    print(f"{'='*70}")

    print("\n-- Test 1: campaigns (MySQL) INNER JOIN orders (CH)")
    rows = run_query(cfg, """
        SELECT c.campaign_name, o.order_id, o.customer_name, o.total_amount
        FROM campaigns.db1.campaigns c
        INNER JOIN chtest.chdb.orders o ON c.campaign_id = o.campaign_id
        LIMIT 20
    """)
    assert len(rows) > 0, "MySQL+CH JOIN returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    campaign={r[0]}, order_id={r[1]}, customer={r[2]}, amount={r[3]}")

    print(f"\n✅ MySQL + ClickHouse JOIN Tests PASSED")


def test_pg_ch_joins(cfg):
    print(f"\n{'='*70}")
    print("Cross-Backend INNER JOIN Tests — PostgreSQL + ClickHouse (PG wire)")
    print(f"{'='*70}")

    print("\n-- Test 1: products (PG) INNER JOIN orders (CH)")
    rows = run_query(cfg, """
        SELECT p.product_name, p.price, o.order_id, o.quantity, o.total_amount
        FROM products.public.products p
        INNER JOIN chtest.chdb.orders o ON p.product_id = o.product_id
        LIMIT 20
    """)
    assert len(rows) > 0, "PG+CH JOIN returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    product={r[0]}, price={r[1]}, order_id={r[2]}, qty={r[3]}, amount={r[4]}")

    print(f"\n✅ PostgreSQL + ClickHouse JOIN Tests PASSED")


def test_triple_join(cfg):
    print(f"\n{'='*70}")
    print("Cross-Backend INNER JOIN Tests — MySQL + PostgreSQL + ClickHouse (PG wire)")
    print(f"{'='*70}")

    print("\n-- Test 1: campaigns (MySQL) + products (PG) + orders (CH)")
    rows = run_query(cfg, """
        SELECT c.campaign_name, p.product_name, p.category,
               o.customer_name, o.total_amount
        FROM campaigns.db1.campaigns c
        INNER JOIN products.public.products p ON c.campaign_id = p.campaign_id
        INNER JOIN chtest.chdb.orders o ON p.product_id = o.product_id
        LIMIT 20
    """)
    assert len(rows) > 0, "MySQL+PG+CH triple JOIN returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    campaign={r[0]}, product={r[1]}, category={r[2]},"
              f" customer={r[3]}, amount={r[4]}")

    print(f"\n✅ Triple Backend JOIN Tests PASSED")


def test_triple_join_shapes(cfg):
    print(f"\n{'='*70}")
    print("Cross-Backend Triple JOIN Shapes — Same-Named Keys (PG wire)")
    print(f"{'='*70}")

    # One table per backend, every key spelled the same: the chain
    # triple_a ⋈ triple_b ⋈ triple_c keeps exactly the ids (10, 12) — every
    # table also holds a row an INNER JOIN drops (a4, b11, b13, c102).
    drops = []
    try:
        run_exec(cfg, "CREATE TABLE campaigns.db1.triple_a (id int, name string)")
        drops.append("DROP TABLE campaigns.db1.triple_a")
        run_exec(cfg, "CREATE TABLE products.public.triple_b (id int, a_id int)")
        drops.append("DROP TABLE products.public.triple_b")
        run_exec(cfg, "CREATE TABLE chtest.chdb.triple_c (id int, b_id int)")
        drops.append("DROP TABLE chtest.chdb.triple_c")
        run_exec(cfg, "INSERT INTO campaigns.db1.triple_a (id, name) VALUES "
                      "(1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta')")
        run_exec(cfg, "INSERT INTO products.public.triple_b (id, a_id) VALUES "
                      "(10, 1), (11, 1), (12, 2), (13, 3)")
        run_exec(cfg, "INSERT INTO chtest.chdb.triple_c (id, b_id) VALUES "
                      "(100, 10), (101, 12), (102, 99)")


        print("\n-- Test 2: the same three tables joined in the reversed order")
        rows = run_query(cfg, """
            SELECT products.public.triple_b.id
            FROM chtest.chdb.triple_c
            INNER JOIN products.public.triple_b ON chtest.chdb.triple_c.b_id = products.public.triple_b.id
            INNER JOIN campaigns.db1.triple_a ON products.public.triple_b.a_id = campaigns.db1.triple_a.id
        """)
        assert sorted(rows) == [(10,), (12,)], \
            f"reversed triple JOIN returned {sorted(rows)}, expected [(10,), (12,)]"
        print(f"  ✓ {len(rows)} rows, exactly ids 10 and 12")

        print("\n-- Test 3: SELECT * over the three sources: every side's key, exact row count")
        columns, rows = run_query_columns(cfg, """
            SELECT *
            FROM campaigns.db1.triple_a
            INNER JOIN products.public.triple_b ON campaigns.db1.triple_a.id = products.public.triple_b.a_id
            INNER JOIN chtest.chdb.triple_c ON products.public.triple_b.id = chtest.chdb.triple_c.b_id
        """)
        assert columns == ["id", "name", "id", "a_id", "id", "b_id"], \
            f"SELECT * carried columns {columns}, expected ['id', 'name', 'id', 'a_id', 'id', 'b_id']"
        assert len(rows) == 2, f"SELECT * returned {len(rows)} rows, expected 2"
        print(f"  ✓ columns {columns}, {len(rows)} rows")

    finally:
        for statement in reversed(drops):
            run_exec(cfg, statement)

    print(f"\n✅ Triple JOIN Shape Tests PASSED")


def main_test():
    parser = argparse.ArgumentParser(description='Cross-backend query tests via PostgreSQL wire')
    parser.add_argument('--local', action='store_true',
                        help='Use local host instead of test-otterstax')
    args = parser.parse_args()

    cfg = make_config(local=args.local)

    try:
        test_mysql_pg_joins(cfg)
        test_mysql_ch_joins(cfg)
        test_pg_ch_joins(cfg)
        test_triple_join(cfg)
        test_triple_join_shapes(cfg)

        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Cross-Backend Queries (PostgreSQL wire)\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - Cross-Backend Queries (PostgreSQL wire)\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
