# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Cross-backend INNER JOIN tests via MySQL wire protocol (port 8816).

Covers:
  - MySQL + PostgreSQL JOINs
  - MySQL + ClickHouse JOINs
  - PostgreSQL + ClickHouse JOINs
  - MySQL + PostgreSQL + ClickHouse triple JOINs

NOTE: LEFT/RIGHT/OUTER JOINs intentionally omitted — they crash otterbrix
vector engine (SIGABRT in vector_ops::copy, assertion source.type()==target.type()).
See memory/bugs.md for details.
"""

import sys
import argparse
import mysql.connector

import config


def make_config(local=False):
    host = config.get_host(local)
    return {
        'host': host,
        'port': config.MYSQL_PORT,
        'user': 'testuser',
        'password': 'testpass',
    }


def run_query(cfg, query):
    conn = mysql.connector.connect(**cfg)
    try:
        cur = conn.cursor(buffered=True)
        cur.execute(query)
        return cur.fetchall()
    finally:
        conn.close()


def test_mysql_pg_joins(cfg):
    print(f"\n{'='*70}")
    print("Cross-Backend INNER JOIN Tests — MySQL + PostgreSQL")
    print(f"{'='*70}")

    # PG table on the left, MySQL on the right
    print("\n-- Test 1: products (PG) INNER JOIN campaigns (MySQL)")
    rows = run_query(cfg, """
        SELECT p.product_id, p.product_name, p.price, c.campaign_name
        FROM products.pgdb.public.products p
        JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
        LIMIT 10
    """)
    assert len(rows) > 0, "PG JOIN MySQL returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    product_id={r[0]}, product={r[1]}, price={r[2]}, campaign={r[3]}")

    # MySQL table on the left, PG on the right
    print("\n-- Test 2: campaigns (MySQL) INNER JOIN products (PG)")
    rows = run_query(cfg, """
        SELECT c.campaign_name, c.budget, p.product_name, p.price
        FROM campaigns.db1.schema.campaigns c
        JOIN products.pgdb.public.products p ON c.campaign_id = p.campaign_id
        LIMIT 10
    """)
    assert len(rows) > 0, "MySQL JOIN PG returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    campaign={r[0]}, budget={r[1]}, product={r[2]}, price={r[3]}")

    # Cross-backend WHERE filter
    print("\n-- Test 3: INNER JOIN + WHERE (price > 100)")
    rows = run_query(cfg, """
        SELECT p.product_name, p.price, c.campaign_name, c.budget
        FROM products.pgdb.public.products p
        JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
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
        FROM campaigns.db1.schema.campaigns c
        INNER JOIN products.pgdb.public.products p ON c.campaign_id = p.campaign_id
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
    print("Cross-Backend INNER JOIN Tests — MySQL + ClickHouse")
    print(f"{'='*70}")

    print("\n-- Test 1: campaigns (MySQL) INNER JOIN orders (CH)")
    rows = run_query(cfg, """
        SELECT c.campaign_name, o.order_id, o.customer_name, o.total_amount
        FROM campaigns.db1.schema.campaigns c
        INNER JOIN chtest.chdb.schema.orders o ON c.campaign_id = o.campaign_id
        LIMIT 20
    """)
    assert len(rows) > 0, "MySQL+CH JOIN returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    campaign={r[0]}, order_id={r[1]}, customer={r[2]}, amount={r[3]}")

    print(f"\n✅ MySQL + ClickHouse JOIN Tests PASSED")


def test_pg_ch_joins(cfg):
    print(f"\n{'='*70}")
    print("Cross-Backend INNER JOIN Tests — PostgreSQL + ClickHouse")
    print(f"{'='*70}")

    print("\n-- Test 1: products (PG) INNER JOIN orders (CH)")
    rows = run_query(cfg, """
        SELECT p.product_name, p.price, o.order_id, o.quantity, o.total_amount
        FROM products.pgdb.public.products p
        INNER JOIN chtest.chdb.schema.orders o ON p.product_id = o.product_id
        LIMIT 20
    """)
    assert len(rows) > 0, "PG+CH JOIN returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    product={r[0]}, price={r[1]}, order_id={r[2]}, qty={r[3]}, amount={r[4]}")

    print(f"\n✅ PostgreSQL + ClickHouse JOIN Tests PASSED")


def test_triple_join(cfg):
    print(f"\n{'='*70}")
    print("Cross-Backend INNER JOIN Tests — MySQL + PostgreSQL + ClickHouse")
    print(f"{'='*70}")

    print("\n-- Test 1: campaigns (MySQL) + products (PG) + orders (CH)")
    rows = run_query(cfg, """
        SELECT c.campaign_name, p.product_name, p.category,
               o.customer_name, o.total_amount
        FROM campaigns.db1.schema.campaigns c
        INNER JOIN products.pgdb.public.products p ON c.campaign_id = p.campaign_id
        INNER JOIN chtest.chdb.schema.orders o ON p.product_id = o.product_id
        LIMIT 20
    """)
    assert len(rows) > 0, "MySQL+PG+CH triple JOIN returned no rows"
    print(f"  ✓ {len(rows)} rows")
    for r in rows[:3]:
        print(f"    campaign={r[0]}, product={r[1]}, category={r[2]},"
              f" customer={r[3]}, amount={r[4]}")

    print(f"\n✅ Triple Backend JOIN Tests PASSED")


def main_test():
    parser = argparse.ArgumentParser(description='Cross-backend query tests via MySQL wire')
    parser.add_argument('--local', action='store_true',
                        help='Use local host instead of test-otterstax')
    args = parser.parse_args()

    cfg = make_config(local=args.local)

    try:
        test_mysql_pg_joins(cfg)
        test_mysql_ch_joins(cfg)
        test_pg_ch_joins(cfg)
        test_triple_join(cfg)

        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Cross-Backend Queries (MySQL wire)\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - Cross-Backend Queries (MySQL wire)\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
