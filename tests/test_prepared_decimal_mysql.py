# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 OtterStax

"""Real mysql-connector-python coverage for the NEWDECIMAL Bind parameter.

A prepared (binary) execute sends a Decimal parameter as NEWDECIMAL (type
246), a length-encoded string the server reads with the engine's parse_decimal
at the prepare's described DECIMAL — rounding half away from zero, refusing a
value past the precision with 1366. Hosts and ports come from the environment
with the in-stack values as defaults.
"""

import decimal
import os

import mysql.connector

OTTERSTAX_HOST = os.environ.get("OTTERSTAX_HOST", "test-otterstax")
OTTERSTAX_MYSQL_PORT = int(os.environ.get("OTTERSTAX_MYSQL_PORT", "8816"))
TABLE = f"prepared_decimal_mysql_{os.getpid()}"

D = decimal.Decimal


class PreparedDecimalMysqlTest:
    def __init__(self):
        self.conn = mysql.connector.connect(host=OTTERSTAX_HOST, port=OTTERSTAX_MYSQL_PORT,
                                            user="testuser", password="testpass",
                                            database="local", autocommit=True)
        self.cur = self.conn.cursor(prepared=True)

    def assert_equal(self, actual, expected, what):
        assert actual == expected, f"{what}: {actual!r} != {expected!r}"

    def setup(self):
        self.cur.execute(f"CREATE TABLE {TABLE} (id bigint, price decimal(10, 2))")
        self.cur.execute(
            f"INSERT INTO {TABLE} (id, price) VALUES (1, 1.25), (2, -3.50), (3, 0.10)")

    def cleanup(self):
        self.cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        self.cur.close()
        self.conn.close()

    def test_newdecimal_selects_its_exact_row(self):
        """A Decimal parameter in a prepared execute matches the row, sign and scale kept."""
        self.cur.execute(f"SELECT id, price FROM {TABLE} WHERE price = %s", (D("1.25"),))
        self.assert_equal(self.cur.fetchall(), [(1, D("1.25"))], "the NEWDECIMAL matched")
        self.cur.execute(f"SELECT id, price FROM {TABLE} WHERE price = %s", (D("-3.50"),))
        self.assert_equal(self.cur.fetchall(), [(2, D("-3.50"))], "the sign survived the bind")
        self.cur.execute(f"SELECT id, price FROM {TABLE} WHERE price = %s", (D("0.10"),))
        self.assert_equal(self.cur.fetchall(), [(3, D("0.10"))], "the scale survived the bind")

    def test_newdecimal_rounds_half_away_from_zero(self):
        """A longer fraction rounds half away from zero at the described scale."""
        self.cur.execute(f"INSERT INTO {TABLE} (id, price) VALUES (4, %s)", (D("1.235"),))
        self.cur.execute("SELECT price FROM {0} WHERE id = 4".format(TABLE))
        self.assert_equal(self.cur.fetchall(), [(D("1.24"),)], "1.235 rounded half away from zero to 1.24")
        self.cur.execute(f"INSERT INTO {TABLE} (id, price) VALUES (5, %s)", (D("-1.235"),))
        self.cur.execute("SELECT price FROM {0} WHERE id = 5".format(TABLE))
        self.assert_equal(self.cur.fetchall(), [(D("-1.24"),)], "-1.235 rounded away from zero too")

    def test_newdecimal_null_binds_as_null(self):
        """A NULL parameter answers the NULL the comparison asks for."""
        self.cur.execute(f"INSERT INTO {TABLE} (id, price) VALUES (6, %s)", (None,))
        self.cur.execute("SELECT id FROM {0} WHERE price IS NULL".format(TABLE))
        self.assert_equal(self.cur.fetchall(), [(6,)], "the NULL parameter stored a NULL")

    def test_newdecimal_refused_past_the_declared_precision(self):
        """A value past the described DECIMAL(10, 2) is refused 1366, the statement alive."""
        try:
            self.cur.execute(f"INSERT INTO {TABLE} (id, price) VALUES (7, %s)", (D("12345678901.00"),))
            raise AssertionError("a decimal past the described precision must be refused")
        except mysql.connector.Error as error:
            assert error.errno == 1366, error
        # The statement is alive: a value within the precision binds after.
        self.cur.execute(f"SELECT id FROM {TABLE} WHERE price = %s", (D("1.25"),))
        self.assert_equal(self.cur.fetchall(), [(1,)], "the statement still answers after the refusal")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_newdecimal_selects_its_exact_row()
            self.test_newdecimal_rounds_half_away_from_zero()
            self.test_newdecimal_null_binds_as_null()
            self.test_newdecimal_refused_past_the_declared_precision()
        finally:
            self.cleanup()


def main_test():
    test = PreparedDecimalMysqlTest()
    test.run_all_tests()
    print("ALL-PASSED: prepared decimal mysql")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main_test())
