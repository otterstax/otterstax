# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""ClickHouse backend tests via PostgreSQL wire protocol (port 8817)."""

import sys
import time
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

    def test_ch_offset_without_limit(self):
        """OFFSET with no LIMIT must skip exactly `skip` rows on ClickHouse.

        `limit_t` stores limit_ = -1 (unlimited) and offset_ independently, so a
        generator that keys on the limit alone drops the OFFSET with it. The
        dialect split matters here: MySQL needs a row_count before OFFSET and
        gets the 18446744073709551615 sentinel, which ClickHouse must NOT get —
        it overflows there and returns zero rows (ClickHouse#10470). ClickHouse
        gets a bare OFFSET; this case is the live proof that the server accepts
        it and applies it.
        """
        with self.pg_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT order_id FROM {_ORDERS} ORDER BY order_id")
            all_ids = [r[0] for r in cur.fetchall()]
            assert len(all_ids) > 20, f"need >20 orders to test OFFSET, have {len(all_ids)}"

            skip = 10
            cur.execute(f"SELECT order_id FROM {_ORDERS} ORDER BY order_id OFFSET {skip}")
            offset_ids = [r[0] for r in cur.fetchall()]

            # Not zero rows: that is the ClickHouse overflow signature if the
            # MySQL sentinel ever leaks into this dialect.
            assert len(offset_ids) > 0, "bare OFFSET returned no rows (sentinel overflow?)"
            self.assert_equal(len(offset_ids), len(all_ids) - skip,
                              "bare OFFSET must skip exactly `skip` rows")
            self.assert_equal(offset_ids, all_ids[skip:], "bare OFFSET must skip the FIRST rows")
            print(f"  bare OFFSET {skip}: {len(all_ids)} -> {len(offset_ids)} rows")

    def test_ch_limit_offset_window(self):
        """LIMIT + OFFSET must be an exact [offset, offset+limit) window.

        The node is replaced by raw data downstream, so nothing re-applies the
        window if the generator gets it wrong — an off-by-one here is silent.
        """
        with self.pg_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT order_id FROM {_ORDERS} ORDER BY order_id LIMIT 25")
            head = [r[0] for r in cur.fetchall()]
            assert len(head) == 25, f"LIMIT 25 returned {len(head)} rows"

            cur.execute(f"SELECT order_id FROM {_ORDERS} ORDER BY order_id LIMIT 5 OFFSET 20")
            window = [r[0] for r in cur.fetchall()]
            self.assert_equal(len(window), 5, "LIMIT 5 OFFSET 20 row count")
            self.assert_equal(window, head[20:25], "LIMIT/OFFSET window must be exact")
            print(f"  LIMIT 5 OFFSET 20 == rows[20:25] ✓")

    def test_ch_remote_dml_row_counts(self):
        """A remote DML on ClickHouse reports what the server can count.

        The native protocol carries no affected-row count in any block: an
        INSERT's rows arrive only as Progress.written_rows, which the connector
        sums, so INSERT reports them. A lightweight DELETE and ALTER ... UPDATE
        are mutations applied outside the statement's pipeline and report 0 —
        the count is read, never invented. The rows themselves must still be
        gone / changed afterwards.
        """
        ids = [990001, 990002, 990003]
        # The generator has no IN list; a closed range names the same rows.
        counted = f"order_id >= {ids[0]} AND order_id <= {ids[-1]}"
        with self.pg_connection() as conn:
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
                self.assert_equal(cur.rowcount, 3, "remote ClickHouse INSERT row count")
                self.assert_equal(cur.statusmessage, "INSERT 0 3", "remote ClickHouse INSERT tag")

                cur.execute(f"SELECT order_id FROM {_ORDERS} WHERE {counted} ORDER BY order_id")
                self.assert_equal([r[0] for r in cur.fetchall()], ids, "inserted rows are readable")

                # ALTER TABLE ... UPDATE is a mutation: it reports 0 and lands
                # asynchronously, so the change is polled for, not assumed.
                cur.execute(f"UPDATE {_ORDERS} SET quantity = 99 WHERE order_id = {ids[0]}")
                self.assert_equal(cur.rowcount, 0, "remote ClickHouse UPDATE (mutation) row count")
                self.assert_equal(cur.statusmessage, "UPDATE 0", "remote ClickHouse UPDATE (mutation) tag")
                deadline = time.monotonic() + 30
                quantity = None
                while time.monotonic() < deadline:
                    cur.execute(f"SELECT quantity FROM {_ORDERS} WHERE order_id = {ids[0]}")
                    quantity = cur.fetchone()[0]
                    if quantity == 99:
                        break
                    time.sleep(0.2)
                self.assert_equal(quantity, 99, "mutation must have been applied")

                cur.execute(f"DELETE FROM {_ORDERS} WHERE {counted}")
                self.assert_equal(cur.rowcount, 0, "remote ClickHouse lightweight DELETE row count")
                self.assert_equal(cur.statusmessage, "DELETE 0", "remote ClickHouse lightweight DELETE tag")

                cur.execute(f"SELECT COUNT(order_id) FROM {_ORDERS} WHERE {counted}")
                self.assert_equal(cur.fetchone()[0], 0, "lightweight DELETE hides the rows at once")
                print("  INSERT 0 3 / UPDATE 0 / DELETE 0 on ClickHouse ✓")
            finally:
                cur.execute(f"DELETE FROM {_ORDERS} WHERE {counted}")

    def run_all_tests(self):
        self.test_ch_select()
        self.test_ch_select_with_where()
        self.test_ch_order_by()
        self.test_ch_aggregation()
        self.test_ch_offset_without_limit()
        self.test_ch_limit_offset_window()
        self.test_ch_remote_dml_row_counts()


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
