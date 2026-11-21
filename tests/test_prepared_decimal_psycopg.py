# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 OtterStax

"""Real psycopg coverage for the PostgreSQL binary NUMERIC Bind parameter.

psycopg 3 sends a Decimal parameter in binary format (numeric_send) typed
NUMERIC; the server rebuilds its exact decimal and binds it at the described
DECIMAL. Hosts and ports come from the environment with the in-stack values
as defaults, so the file runs inside the compose network and against a hybrid
local-server stand unchanged.
"""

import decimal
import os

import psycopg

OTTERSTAX_HOST = os.environ.get("OTTERSTAX_HOST", "test-otterstax")
OTTERSTAX_PORT = int(os.environ.get("OTTERSTAX_PORT", "8817"))
TABLE = f"prepared_decimal_psycopg_{os.getpid()}"

D = decimal.Decimal


class PreparedDecimalPsycopgTest:
    def __init__(self):
        self.conn = psycopg.connect(host=OTTERSTAX_HOST, port=OTTERSTAX_PORT,
                                    user="testuser", password="testpass", dbname="local")
        self.conn.autocommit = False

    def assert_equal(self, actual, expected, what):
        assert actual == expected, f"{what}: {actual!r} != {expected!r}"

    def setup(self):
        cur = self.conn.cursor()
        cur.execute(f"CREATE TABLE {TABLE} (id bigint, price decimal(38, 2))")
        cur.execute(
            f"INSERT INTO {TABLE} (id, price) VALUES (1, 1.25), (2, -3.50), (3, 0.10)")
        self.conn.commit()

    def cleanup(self):
        try:
            cur = self.conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
            self.conn.commit()
        finally:
            self.conn.close()

    def test_binary_numeric_selects_its_exact_row(self):
        """A Decimal parameter sent binary matches the row, sign and scale kept."""
        cur = self.conn.cursor()
        cur.execute(f"SELECT id, price FROM {TABLE} WHERE price = %s", [D("1.25")])
        self.assert_equal(cur.fetchall(), [(1, D("1.25"))], "the binary decimal matched")
        cur.execute(f"SELECT id, price FROM {TABLE} WHERE price = %s", [D("-3.50")])
        self.assert_equal(cur.fetchall(), [(2, D("-3.50"))], "the sign survived the bind")
        cur.execute(f"SELECT id, price FROM {TABLE} WHERE price = %s", [D("0.10")])
        self.assert_equal(cur.fetchall(), [(3, D("0.10"))], "the scale survived the bind")

    def test_binary_numeric_carries_38_digits(self):
        """A 38-digit Decimal binds whole into the described DECIMAL(38, 2)."""
        value = D("123456789012345678901234567890123456.78")
        cur = self.conn.cursor()
        cur.execute(f"INSERT INTO {TABLE} (id, price) VALUES (4, %s)", [value])
        cur.execute(f"SELECT price FROM {TABLE} WHERE id = 4")
        self.assert_equal(cur.fetchall(), [(value,)], "the 38-digit decimal round-tripped")
        self.conn.rollback()

    def test_binary_numeric_null_and_arithmetic(self):
        """A NULL parameter and a decimal arithmetic context bind as the engine types them."""
        cur = self.conn.cursor()
        cur.execute(f"SELECT id FROM {TABLE} WHERE price = %s", [None])
        self.assert_equal(cur.fetchall(), [], "price = NULL answers no row")
        cur.execute(f"SELECT id FROM {TABLE} WHERE price + %s > 0", [D("-0.01")])
        self.assert_equal(sorted(cur.fetchall()), [(1,), (3,)], "the arithmetic context bound the decimal")
        cur.execute(f"SELECT id FROM {TABLE} WHERE price * %s = 12.50", [D("10")])
        self.assert_equal(cur.fetchall(), [(1,)], "a decimal times a decimal kept the comparison exact")

    def test_binary_numeric_refused_past_the_declared_precision(self):
        """A value past a DECIMAL(10, 2) context is refused 22003, the connection alive."""
        cur = self.conn.cursor()
        cur.execute(f"CREATE TABLE {TABLE}_small (id bigint, price decimal(10, 2))")
        self.conn.commit()
        try:
            cur.execute(f"INSERT INTO {TABLE}_small (id, price) VALUES (1, %s)",
                        [D("12345678901.00")])
            raise AssertionError("a decimal past the described precision must be refused")
        except psycopg.errors.NumericValueOutOfRange as error:
            assert error.sqlstate == "22003", error
        finally:
            self.conn.rollback()
            cur.execute(f"DROP TABLE IF EXISTS {TABLE}_small")
            self.conn.commit()

    def run_all_tests(self):
        try:
            self.setup()
            self.test_binary_numeric_selects_its_exact_row()
            self.test_binary_numeric_carries_38_digits()
            self.test_binary_numeric_null_and_arithmetic()
            self.test_binary_numeric_refused_past_the_declared_precision()
        finally:
            self.cleanup()


def main_test():
    test = PreparedDecimalPsycopgTest()
    test.run_all_tests()
    print("ALL-PASSED: prepared decimal psycopg")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main_test())
