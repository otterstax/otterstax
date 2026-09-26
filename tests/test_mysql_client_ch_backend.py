# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""ClickHouse backend tests via MySQL wire protocol (port 8816)."""

import sys
import time
import argparse
import mysql.connector
from contextlib import contextmanager

import config

_ORDERS = f"{config.CH_ALIAS}.{config.CH_DATABASE}.schema.orders"


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

    def test_ch_select(self):
        with self.mysql_connection() as conn:
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
            assert isinstance(total_amount, float), f"total_amount: expected float, got {type(total_amount)}"
            print(f"  Sample: order_id={order_id}, campaign_id={campaign_id},"
                  f" customer={customer_name}, amount={total_amount:.2f}")

    def test_ch_select_with_where(self):
        with self.mysql_connection() as conn:
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
        with self.mysql_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT order_id, total_amount"
                f" FROM {_ORDERS}"
                f" ORDER BY total_amount DESC LIMIT 10"
            )
            rows = cur.fetchall()
            assert len(rows) > 0, "ORDER BY returned no rows"
            amounts = [r[1] for r in rows]
            assert amounts == sorted(amounts, reverse=True), "ORDER BY DESC not respected"
            print(f"  Top total_amount: {amounts[0]:.2f}")

    def test_ch_aggregation(self):
        with self.mysql_connection() as conn:
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

    def test_ch_remote_dml_row_counts(self):
        """A remote DML on ClickHouse reports what the server can count, in the OK packet.

        The native protocol carries no affected-row count in any block: an
        INSERT's rows arrive only as Progress.written_rows, which the connector
        sums, so INSERT reports them. A lightweight DELETE and ALTER ... UPDATE
        are mutations applied outside the statement's pipeline and report 0 —
        the count is read, never invented. The rows themselves must still be
        gone / changed afterwards. Mirror of the PG-wire case in
        test_pg_client_ch_backend.py; here the count is the OK packet's
        affected_rows, which mysql.connector exposes as rowcount.
        """
        ids = [990101, 990102, 990103]
        # The generator has no IN list; a closed range names the same rows.
        counted = f"order_id >= {ids[0]} AND order_id <= {ids[-1]}"
        with self.mysql_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"INSERT INTO {_ORDERS}"
                    f" (order_id, campaign_id, product_id, customer_name, order_date, quantity, total_amount)"
                    f" VALUES"
                    f" ({ids[0]}, 1, 1, 'Counted One', '2026-01-01 00:00:00', 1, 1.5),"
                    f" ({ids[1]}, 1, 1, 'Counted Two', '2026-01-01 00:00:00', 2, 2.5),"
                    f" ({ids[2]}, 1, 1, 'Counted Three', '2026-01-01 00:00:00', 3, 3.5)"
                )
                self.assert_equal(cur.rowcount, 3, "remote ClickHouse INSERT affected_rows")

                cur.execute(f"SELECT order_id FROM {_ORDERS} WHERE {counted} ORDER BY order_id")
                self.assert_equal([r[0] for r in cur.fetchall()], ids, "inserted rows are readable")

                # ALTER TABLE ... UPDATE is a mutation: it reports 0 and lands
                # asynchronously, so the change is polled for, not assumed.
                cur.execute(f"UPDATE {_ORDERS} SET quantity = 99 WHERE order_id = {ids[0]}")
                self.assert_equal(cur.rowcount, 0, "remote ClickHouse UPDATE (mutation) affected_rows")
                deadline = time.monotonic() + 30
                quantity = None
                while time.monotonic() < deadline:
                    cur.execute(f"SELECT quantity FROM {_ORDERS} WHERE order_id = {ids[0]}")
                    quantity = cur.fetchall()[0][0]
                    if quantity == 99:
                        break
                    time.sleep(0.2)
                self.assert_equal(quantity, 99, "mutation must have been applied")

                cur.execute(f"DELETE FROM {_ORDERS} WHERE {counted}")
                self.assert_equal(cur.rowcount, 0, "remote ClickHouse lightweight DELETE affected_rows")

                cur.execute(f"SELECT COUNT(order_id) FROM {_ORDERS} WHERE {counted}")
                self.assert_equal(cur.fetchall()[0][0], 0, "lightweight DELETE hides the rows at once")
                print("  INSERT 3 / UPDATE 0 / DELETE 0 affected_rows on ClickHouse ✓")
            finally:
                cur.execute(f"DELETE FROM {_ORDERS} WHERE {counted}")

    def run_all_tests(self):
        self.test_ch_select()
        self.test_ch_select_with_where()
        self.test_ch_order_by()
        self.test_ch_aggregation()
        self.test_ch_remote_dml_row_counts()


def main_test():
    parser = argparse.ArgumentParser(description='ClickHouse backend tests via MySQL wire')
    parser.add_argument('--local', action='store_true',
                        help='Use local host instead of test-otterstax')
    args = parser.parse_args()

    tests = client(local=args.local)
    try:
        tests.run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - MySQL Client / ClickHouse Backend\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - MySQL Client / ClickHouse Backend\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
