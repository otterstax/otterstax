# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""ClickHouse backend tests via PostgreSQL wire protocol (port 8817)."""

import sys
import argparse
import psycopg2
from contextlib import contextmanager

import config

_ORDERS = f"{config.CH_ALIAS}.{config.CH_DATABASE}.schema.orders"


class client:
    def __init__(self, local=False):
        host = config.get_host(local)
        self.proxy_config = {
            'host': host,
            'port': config.PG_PORT,
            'user': 'testuser',
            'password': 'testpass',
            'dbname': config.CH_ALIAS,
        }
        print(f"Connecting to host: {host}")

    def assert_equal(self, a, b, msg=""):
        if a != b:
            raise AssertionError(f"Assertion failed: {a!r} != {b!r}. {msg}")

    def assert_floating_equal(self, a, b, tol=1e-6, msg=""):
        if abs(a - b) > tol:
            raise AssertionError(f"Assertion failed: {a!r} != {b!r} ± {tol}. {msg}")

    @contextmanager
    def pg_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(**self.proxy_config)
            conn.autocommit = True
            yield conn
        finally:
            if conn:
                conn.close()

    def test_ch_select(self):
        with self.pg_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT order_id, campaign_id, customer_name, total_amount"
                f" FROM {_ORDERS} LIMIT 10"
            )
            rows = cur.fetchall()
            assert len(rows) > 0, "orders table returned no rows"
            order_id, campaign_id, customer_name, total_amount = rows[0]
            assert isinstance(order_id, int), f"order_id: expected int, got {type(order_id)}"
            assert isinstance(campaign_id, int), f"campaign_id: expected int, got {type(campaign_id)}"
            assert isinstance(customer_name, str), f"customer_name: expected str, got {type(customer_name)}"
            print(f"  Sample: order_id={order_id}, campaign_id={campaign_id},"
                  f" customer={customer_name}, amount={float(total_amount):.2f}")

    def test_ch_select_with_where(self):
        with self.pg_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT order_id, quantity, total_amount"
                f" FROM {_ORDERS}"
                f" WHERE quantity > 5 LIMIT 20"
            )
            rows = cur.fetchall()
            assert len(rows) > 0, "WHERE quantity > 5 returned no rows"
            for order_id, quantity, total_amount in rows:
                assert quantity > 5, f"WHERE filter not respected: quantity={quantity}"
            print(f"  Rows with quantity > 5: {len(rows)}")

    def test_ch_order_by(self):
        with self.pg_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT order_id, total_amount"
                f" FROM {_ORDERS}"
                f" ORDER BY total_amount DESC LIMIT 10"
            )
            rows = cur.fetchall()
            assert len(rows) > 0, "ORDER BY returned no rows"
            amounts = [float(r[1]) for r in rows]
            assert amounts == sorted(amounts, reverse=True), "ORDER BY DESC not respected"
            print(f"  Top total_amount: {amounts[0]:.2f}")

    def test_ch_aggregation(self):
        with self.pg_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT campaign_id,
                       COUNT(order_id)   AS order_count,
                       AVG(total_amount) AS avg_amount,
                       SUM(quantity)     AS total_qty
                FROM {_ORDERS}
                GROUP BY campaign_id
                ORDER BY order_count DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
            assert len(rows) > 0, "aggregation returned no rows"
            campaign_id, order_count, avg_amount, total_qty = rows[0]
            assert isinstance(campaign_id, int), f"campaign_id: expected int, got {type(campaign_id)}"
            assert order_count > 0, "order_count should be > 0"
            print(f"  Top campaign: id={campaign_id}, orders={order_count},"
                  f" avg_amount={float(avg_amount):.2f}, total_qty={total_qty}")

    def run_all_tests(self):
        self.test_ch_select()
        self.test_ch_select_with_where()
        self.test_ch_order_by()
        self.test_ch_aggregation()


def main_test():
    parser = argparse.ArgumentParser(description='ClickHouse backend tests via PostgreSQL wire')
    parser.add_argument('--local', action='store_true',
                        help='Use local host instead of test-otterstax')
    args = parser.parse_args()

    tests = client(local=args.local)
    try:
        tests.run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - PostgreSQL Client / ClickHouse Backend\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - PostgreSQL Client / ClickHouse Backend\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
