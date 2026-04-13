# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""PostgreSQL backend tests via MySQL wire protocol (port 8816)."""

import sys
import argparse
import mysql.connector
from contextlib import contextmanager

import config

_PRODUCTS = "products.pgdb.public.products"


class client:
    def __init__(self, local=False):
        host = config.get_host(local)
        self.proxy_config = {
            'host': host,
            'port': config.MYSQL_PORT,
            'user': 'testuser',
            'password': 'testpass',
        }
        print(f"Connecting to host: {host}")

    def assert_equal(self, a, b, msg=""):
        if a != b:
            raise AssertionError(f"Assertion failed: {a!r} != {b!r}. {msg}")

    def assert_floating_equal(self, a, b, tol=1e-6, msg=""):
        if abs(a - b) > tol:
            raise AssertionError(f"Assertion failed: {a!r} != {b!r} ± {tol}. {msg}")

    @contextmanager
    def mysql_connection(self):
        conn = None
        try:
            conn = mysql.connector.connect(**self.proxy_config)
            yield conn
        finally:
            if conn:
                conn.close()

    def test_pg_select(self):
        with self.mysql_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT product_id, product_name, price, category"
                f" FROM {_PRODUCTS} LIMIT 10"
            )
            rows = cur.fetchall()
            assert len(rows) > 0, "products table returned no rows"
            product_id, product_name, price, category = rows[0]
            assert isinstance(product_name, str), f"product_name: expected str, got {type(product_name)}"
            assert isinstance(price, (int, float)), f"price: expected numeric, got {type(price)}"
            assert isinstance(category, str), f"category: expected str, got {type(category)}"
            print(f"  Sample: product_id={product_id}, name={product_name},"
                  f" price={float(price):.2f}, category={category}")

    def test_pg_select_with_where(self):
        with self.mysql_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT product_id, product_name, price"
                f" FROM {_PRODUCTS}"
                f" WHERE price > 100 LIMIT 20"
            )
            rows = cur.fetchall()
            assert len(rows) > 0, "WHERE price > 100 returned no rows"
            for product_id, product_name, price in rows:
                assert float(price) > 100, f"WHERE filter not respected: price={price}"
            print(f"  Rows with price > 100: {len(rows)}")

    def test_pg_order_by(self):
        with self.mysql_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT product_id, price"
                f" FROM {_PRODUCTS}"
                f" ORDER BY price DESC LIMIT 10"
            )
            rows = cur.fetchall()
            assert len(rows) > 0, "ORDER BY returned no rows"
            prices = [float(r[1]) for r in rows]
            assert prices == sorted(prices, reverse=True), "ORDER BY DESC not respected"
            print(f"  Top price: {prices[0]:.2f}")

    def test_pg_aggregation(self):
        with self.mysql_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT category,
                       COUNT(product_id) AS product_count,
                       AVG(price)        AS avg_price,
                       MIN(price)        AS min_price,
                       MAX(price)        AS max_price
                FROM {_PRODUCTS}
                GROUP BY category
                ORDER BY product_count DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
            assert len(rows) > 0, "aggregation returned no rows"
            category, product_count, avg_price, min_price, max_price = rows[0]
            assert isinstance(category, str), f"category: expected str, got {type(category)}"
            assert product_count > 0, "product_count should be > 0"
            print(f"  Top category: {category}, count={product_count},"
                  f" avg_price={float(avg_price):.2f}")

    def run_all_tests(self):
        self.test_pg_select()
        self.test_pg_select_with_where()
        self.test_pg_order_by()
        self.test_pg_aggregation()


def main_test():
    parser = argparse.ArgumentParser(description='PostgreSQL backend tests via MySQL wire')
    parser.add_argument('--local', action='store_true',
                        help='Use local host instead of test-otterstax')
    args = parser.parse_args()

    tests = client(local=args.local)
    try:
        tests.run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - MySQL Client / PostgreSQL Backend\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - MySQL Client / PostgreSQL Backend\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
