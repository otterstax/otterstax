# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

import mysql.connector
import pymysql
import sys
import json
import struct
import argparse
from contextlib import contextmanager

# from otterbrix/integration/python
def gen_id(num):
    res = str(num)
    while (len(res) < 24):
        res = '0' + res
    return res

class client:
    def __init__(self, local=False):
        # Select host based on local flag
        host = '0.0.0.0' if local else 'test-otterstax'

        self.proxy_config = {
            'host': host,
            'port': 8816,
            'user': 'testuser',
            'password': 'testpass',
            # Every statement here is meant to commit on its own: the driver's
            # SET autocommit reaches the engine and is the connection's own.
            'autocommit': True,
        }
        self.test_database = 'campaigns.db1.public'
        self.test_table = 'mysql_test_table'
        self.test_encoding_table = 'mysql_test_encoding'

        self.test_db_created = False
        self.encoding_db_created = False
        print(f"Connecting to host: {host}")

    def assert_equal(self, a, b, msg=""):
        if a != b:
            raise AssertionError(f"Assertion failed: {a!r} != {b!r}. {msg}")

    def assert_floating_equal(self, a, b, tol=1e-6, msg=""):
        if abs(a - b) > tol:
            raise AssertionError(f"Assertion failed: {a!r} != {b!r} ± {tol}. {msg}")

    @contextmanager
    def mysql_connector_connection(self):
        """Context manager for mysql-connector-python"""
        conn = None
        try:
            conn = mysql.connector.connect(**self.proxy_config)
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def pymysql_connection(self):
        """Context manager for PyMySQL"""
        conn = None
        try:
            conn = pymysql.connect(**self.proxy_config)
            yield conn
        finally:
            if conn:
                conn.close()

    def test_basic_connection(self):
        # PyMySQL
        try:
            with self.pymysql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"create table {self.test_database}.{self.test_table} (_id string, campaign_name string, campaign_length int, budget float);")
                self.test_db_created = True

        except Exception as e:
            raise ValueError("Failed to connect with PyMySQL")

    def test_crud_queries(self):
        # mysql-connector-python
        with self.mysql_connector_connection() as conn:
            cursor = conn.cursor()

            insert_sql = f"""
            insert into {self.test_database}.{self.test_table}
            (_id, campaign_name, campaign_length, budget) values
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s),
            (%s, %s, %s, %s);
            """

            cursor.execute(insert_sql, (
                gen_id(1), 'Campaign Agree Recently', 40, 64647.54,
                gen_id(2), 'Campaign Religious Else', 49, 87452.68,
                gen_id(3), 'Campaign Finally Bit', 77, 75317.39,
                gen_id(4), 'Campaign Every Company', 85, 80728.08,
                gen_id(5), 'Campaign Arm Election', 83, 68484.63,
                gen_id(6), 'Campaign Drive Paper', 84, 25363.38,
                gen_id(7), 'Campaign Actually Box', 38, 9764.74,
                gen_id(8), 'Campaign Run Morning', 39, 41699.68,
                gen_id(9), 'Campaign Get Great', 51, 18711.98,
                gen_id(10), 'Campaign Study Message', 36, 82243.5,
                gen_id(11), 'Campaign Positive Media', 66, 52093.63,
                gen_id(12), 'Campaign Surface Great', 82, 48407.42,
                gen_id(13), 'Campaign Guess Final', 46, 54048.33,
            ))

            cursor.execute(f"select campaign_length from {self.test_database}.{self.test_table}")
            initial_l = cursor.fetchall()

            # A FLOAT column is read over the binary protocol (a prepared
            # cursor), where it travels as its four bytes: the text protocol
            # writes it as MySQL does, at most six significant digits.
            binary = conn.cursor(prepared=True)
            binary.execute(f"select budget from {self.test_database}.{self.test_table}")
            initial_b = binary.fetchall()

            cursor.execute(f"update {self.test_database}.{self.test_table} set campaign_length = %s where _id = %s", (30, gen_id(1))) # subtract 10
            cursor.execute(f"update {self.test_database}.{self.test_table} set budget = %s where _id = %s", (64617.54, gen_id(1))) # subtract 40

            # The engine types the result, not MariaDB: SUM over an integer
            # column is BIGINT (MariaDB's own answer, DECIMAL text, is converted
            # to it), so the value arrives as an int.
            cursor.execute(f"select sum(campaign_length) as sum_ from {self.test_database}.{self.test_table}")
            upd = cursor.fetchall()[0][0]
            if not isinstance(upd, int):
                raise ValueError(f"sum(int) must arrive as BIGINT (int), got {type(upd).__name__}: {upd!r}")

            for row in initial_l:
                upd -= row[0]

            if (upd != -10):
                raise ValueError(f"Expected campaign_length diff: -10, got: {upd}")

            binary.execute(f"select sum(budget) as sum_ from {self.test_database}.{self.test_table}")
            upd = binary.fetchall()[0][0]

            for row in initial_b:
                upd -= row[0]

            # SUM over a FLOAT column is FLOAT (the engine's rule: a floating
            # sum keeps its input type), so the ~7.4e5 total carries float32
            # precision — a step of 1/16 there — and the difference is -30 to
            # within that, not to 1e-6.
            self.assert_floating_equal(upd, -30.0, tol=0.1)

    def test_float_column_reads_back_the_stored_value(self):
        """A FLOAT column comes back as the float32 the backend holds, not a rounding of it.

        MariaDB stores 64647.54 as the nearest float32, 64647.5390625. Its text
        protocol renders a FLOAT column with six significant digits ("64647.5"),
        so a connector reading rows over COM_QUERY hands back a value 0.04 off
        the stored one while SUM() over the same column is exact — the
        inconsistency test_crud_queries trips over. The connector must read
        the rows over the binary protocol, where the float travels as its four
        bytes. OtterStax's own text protocol writes a FLOAT as MySQL does (at
        most six significant digits, "64647.5"), so the client reads it here
        over the binary protocol too (a prepared cursor).
        """
        table = f"{self.test_database}.{self.test_table}"
        stored = struct.unpack('f', struct.pack('f', 64647.54))[0]

        with self.mysql_connector_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"insert into {table} (_id, campaign_name, campaign_length, budget) values (%s, %s, %s, %s)",
                           (gen_id(100), 'Campaign Float Exact', 1, 64647.54))
            binary = conn.cursor(prepared=True)
            binary.execute(f"select budget from {table} where _id = ?", (gen_id(100),))
            self.assert_floating_equal(binary.fetchall()[0][0], stored, msg="FLOAT column read back rounded")
            cursor.execute(f"delete from {table} where _id = %s", (gen_id(100),))

    def test_remote_dml_row_counts(self):
        """A REMOTE DML must report the rows the backend touched.

        boost.mysql carries the count in the statement's OK packet, not in the
        result set; nothing read it, so every remote INSERT/UPDATE/DELETE came
        back as 0 affected rows over the MySQL wire while really having changed
        the backend. Both drivers are checked: mysql-connector-python and
        PyMySQL read `rowcount` from the same OK packet but frame their packets
        differently.
        """
        table = f"{self.test_database}.{self.test_table}"

        with self.pymysql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"insert into {table} (_id, campaign_name, campaign_length, budget) values "
                f"('{gen_id(90)}', 'Counted Alpha', 7, 11.0), "
                f"('{gen_id(91)}', 'Counted Beta', 7, 12.0)")
            self.assert_equal(cursor.rowcount, 2, "remote INSERT affected rows (PyMySQL)")

            cursor.execute(f"update {table} set budget = 13.0 where campaign_length = 7")
            self.assert_equal(cursor.rowcount, 2, "remote UPDATE affected rows (PyMySQL)")

            cursor.execute(f"delete from {table} where _id = '{gen_id(91)}'")
            self.assert_equal(cursor.rowcount, 1, "remote DELETE affected rows (PyMySQL)")

            # Matching nothing must report 0 — the count is read, not invented.
            cursor.execute(f"delete from {table} where campaign_length = 4242")
            self.assert_equal(cursor.rowcount, 0, "remote DELETE matching nothing (PyMySQL)")

        with self.mysql_connector_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"insert into {table} (_id, campaign_name, campaign_length, budget) values "
                f"('{gen_id(92)}', 'Counted Gamma', 8, 14.0)")
            self.assert_equal(cursor.rowcount, 1, "remote INSERT affected rows (mysql-connector)")

            cursor.execute(f"delete from {table} where campaign_length in (7, 8)")
            self.assert_equal(cursor.rowcount, 2, "remote DELETE affected rows (mysql-connector)")

    def test_remote_dml_row_counts_span_chunks(self):
        """A remote UPDATE/DELETE over 2000 rows reports 2000, not a chunk's worth.

        The affected count crosses the actor graph in a payload whose size IS
        the count, and the engine caps a chunk at 1024 rows — a carrier clamped
        to one chunk reports 1024 (or breaks) for anything larger. COM_QUERY and
        COM_STMT_EXECUTE both read the count off the same OK packet.
        """
        table = f"{self.test_database}.{self.test_table}"
        bulk_rows = 2000
        batch = 1000
        first = 10000
        bulk_length = 4242          # no other row uses this campaign_length

        with self.pymysql_connection() as conn:                 # COM_QUERY
            cursor = conn.cursor()
            for start in range(0, bulk_rows, batch):
                values = ", ".join(
                    f"('{gen_id(first + i)}', 'Bulk {i}', {bulk_length}, {float(i)})"
                    for i in range(start, start + batch))
                cursor.execute(f"insert into {table} (_id, campaign_name, campaign_length, budget) values {values}")
                self.assert_equal(cursor.rowcount, batch, f"bulk remote INSERT batch at {start}")

            cursor.execute(f"select count(_id) from {table} where campaign_length = {bulk_length}")
            self.assert_equal(int(cursor.fetchall()[0][0]), bulk_rows, "bulk rows must be on the backend")

            # OtterStax reports the rows a remote UPDATE matched, read off the
            # OK packet's "Rows matched: N" (MariaDB's own affected-row count is
            # the rows it changed: CLIENT_FOUND_ROWS is not requested). The new
            # value is one no bulk row holds, so matched and changed agree here
            # either way.
            cursor.execute(f"update {table} set budget = -1.0 where campaign_length = {bulk_length}")
            self.assert_equal(cursor.rowcount, bulk_rows, "COM_QUERY remote UPDATE over 2000 rows")

        with self.mysql_connector_connection() as conn:         # COM_STMT_EXECUTE
            cursor = conn.cursor(prepared=True)
            cursor.execute(f"update {table} set budget = ? where campaign_length = ?", (-2.0, bulk_length))
            self.assert_equal(cursor.rowcount, bulk_rows, "COM_STMT_EXECUTE remote UPDATE over 2000 rows")

            cursor.execute(f"delete from {table} where campaign_length = ?", (bulk_length,))
            self.assert_equal(cursor.rowcount, bulk_rows, "COM_STMT_EXECUTE remote DELETE over 2000 rows")

            cursor.execute(f"select count(_id) from {table} where campaign_length = ?", (bulk_length,))
            self.assert_equal(int(cursor.fetchall()[0][0]), 0, "every bulk row must be gone")

    def test_character_encoding(self):
        with self.pymysql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"create table {self.test_database}.{self.test_encoding_table}(_id string, utf8_text string)")
            self.encoding_db_created = True

            insert_sql = f"""
            insert into {self.test_database}.{self.test_encoding_table}
            (_id, utf8_text) values
            (%s, %s),
            (%s, %s),
            (%s, %s),
            (%s, %s),
            (%s, %s);
            """

            cursor.execute(insert_sql, (
                gen_id(1), 'Hello World',
                gen_id(2), 'Привет мир',
                gen_id(3), '你好世界',
                gen_id(4), 'こんにちは世界',
                gen_id(5), '🌍🚀✨',
            ))

            cursor.execute(f"select utf8_text from {self.test_database}.{self.test_encoding_table}")
            results = cursor.fetchall()

            if (len(results) != 5):
                raise ValueError(f"Failed to get encoding_test strings, expected len: 5, got: {len(results)}")

            print("Encoding test:")
            for string in results:
                print(string[0])

    def test_protocol_capability_flags(self):
        with self.mysql_connector_connection() as conn:
            connection_info = {
                "server_version": conn.server_version,
                "connection_id": getattr(conn, 'connection_id', 'unknown'),
                "charset": getattr(conn, 'charset', 'unknown')
            }

            if (connection_info["server_version"] != (9, 5, 0) or connection_info["charset"] != "utf8mb4"
                or connection_info["connection_id"] == "unknown"):
                raise ValueError("Failed to get server capabilities")

    def test_prepared_queries(self):
        try:
            with self.mysql_connector_connection() as conn:
                cursor = conn.cursor(prepared=True)

                query = f"select budget from {self.test_database}.{self.test_table} where _id = ?"
                cursor.execute(query, (gen_id(1),))
                result = cursor.fetchall()
                self.assert_equal(len(result), 1, "Expected one row for prepared select")

                two_field = f"select budget, campaign_name from {self.test_database}.{self.test_table} where _id = ?"
                cursor.execute(two_field, (gen_id(10),))
                budget, name = cursor.fetchall()[0]
                self.assert_floating_equal(budget, 82243.5)
                self.assert_equal(name, "Campaign Study Message")

                update_query = f"update {self.test_database}.{self.test_table} set budget = ? where _id = ?"
                cursor.execute(update_query, (budget + 100.5, gen_id(10)))
                # COM_STMT_EXECUTE answers a DML with an OK packet; its
                # affected_rows must be the backend's count, not 0.
                self.assert_equal(cursor.rowcount, 1, "COM_STMT_EXECUTE remote UPDATE affected rows")
                cursor.execute(query, (gen_id(10),))
                self.assert_floating_equal(cursor.fetchall()[0][0], budget + 100.5, msg="budget should increase by 100.5")

                insert_query = f"insert into {self.test_database}.{self.test_table}(_id, campaign_name, campaign_length, budget) values (?, ?, ?, ?)"
                new_id = gen_id(99)
                cursor.execute(insert_query, (new_id, "Campaign Agree Recently", 40, budget + 100.5))
                self.assert_equal(cursor.rowcount, 1, "COM_STMT_EXECUTE remote INSERT affected rows")

                cursor.execute(f"select campaign_name from {self.test_database}.{self.test_table} where _id = ?", (new_id,))
                self.assert_equal(cursor.fetchall()[0][0], "Campaign Agree Recently", "Inserted name mismatch")

                delete_query = f"delete from {self.test_database}.{self.test_table} where _id = ?"
                cursor.execute(delete_query, (new_id,))
                self.assert_equal(cursor.rowcount, 1, "COM_STMT_EXECUTE remote DELETE affected rows")
                cursor.execute(delete_query, (new_id,))
                self.assert_equal(cursor.rowcount, 0, "COM_STMT_EXECUTE remote DELETE matching nothing")
                cursor.execute(f"select count(_id) as cnt from {self.test_database}.{self.test_table} where _id = ?", (new_id,))
                self.assert_equal(cursor.fetchall()[0][0], 0, "Row was not deleted")
                
                cursor.execute(query, (gen_id(2),))
                second_row = cursor.fetchall()
                self.assert_equal(len(second_row), 1, "Expected one row for reused prepared statement")

        except Exception as e:
            raise ValueError("Prepared queries threw something: " + str(e))

    def test_prepared_select_of_a_parameter(self):
        """A prepared `SELECT ? FROM t` answers the bound value per row.

        The engine types a select-list parameter (OTB-select-param closed; the
        C++ twin is "mysql protocol over the wire: a prepared SELECT ? FROM t
        answers the bound value per row", tests/system/test_frontend_mysql_protocol.cpp).
        """
        with self.mysql_connector_connection() as conn:
            cursor = conn.cursor(prepared=True)
            cursor.execute(f"select ? from {self.test_database}.{self.test_table}", (100,))
            self.assert_equal(cursor.fetchall()[0][0], 100, "Select ? constant failed")

    def test_missing_remote_table_is_an_error_not_a_transport_failure(self):
        """A DROP TABLE of a remote table neither the catalog nor MariaDB has is a "does not exist" error.

        The engine resolves the four-part name in its catalog; the miss sends
        the session to discover the table on MariaDB, which has none, so the
        statement fails with the catalog's verdict (never io_error / "connect
        failed"), and the connection keeps serving. COM_STMT_EXECUTE takes the
        same path twice and fails the same way both times.
        """
        missing = f"{self.test_database}.no_such_table_{gen_id(404)}"

        def refused(cursor):
            try:
                cursor.execute(f"drop table {missing}")
            except mysql.connector.Error as e:
                return str(e)
            raise AssertionError("DROP TABLE of a table the backend does not have must fail")

        def check(message, what):
            assert "no_such_table" in message or "does not exist" in message, \
                f"{what}: the refusal names neither the table nor its absence: {message}"
            assert "connect failed" not in message, f"{what}: a transport error was reported instead: {message}"

        with self.mysql_connector_connection() as conn:
            check(refused(conn.cursor()), "COM_QUERY")

            # The connection is still usable after the refusal.
            cursor = conn.cursor()
            cursor.execute(f"select count(_id) as cnt from {self.test_database}.{self.test_table}")
            cursor.fetchall()

            prepared = conn.cursor(prepared=True)
            for attempt in (1, 2):
                check(refused(prepared), f"COM_STMT_EXECUTE attempt {attempt}")

    def cleanup_test_data(self):
        try:
            with self.pymysql_connection() as conn:
                cursor = conn.cursor()
                if self.test_db_created:
                    cursor.execute(f"drop table {self.test_database}.{self.test_table}")
                
                if self.encoding_db_created:
                    cursor.execute(f"drop table {self.test_database}.{self.test_encoding_table}")
        except Exception as e:
            raise ValueError(f"Cleanup error {e}")

    def run_all_tests(self):
        try:
            self.test_basic_connection()
            self.test_crud_queries()
            self.test_float_column_reads_back_the_stored_value()
            self.test_remote_dml_row_counts()
            self.test_remote_dml_row_counts_span_chunks()
            self.test_character_encoding()
            self.test_protocol_capability_flags()
            self.test_prepared_queries()
            self.test_missing_remote_table_is_an_error_not_a_transport_failure()
            self.test_prepared_select_of_a_parameter()
            print("\033[92mTest success.\033[0m")
        except Exception as e:
            print(f"\033[91mAn error occurred: {e}\033[0m")
            print("\033[91mTest fails.\033[0m")
            # Without this the banner lies: main_test() still returns 0 and CI stays
            # green while every assertion in this file is effectively decorative.
            raise
        finally:
            self.cleanup_test_data()
            print("Test completed.")

def main_test():
    parser = argparse.ArgumentParser(description='Test MySQL protocol compatibility')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    tests = client(local=args.local)
    try:
        tests.run_all_tests()
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - MySQL Client/Backend\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - MySQL Client/Backend\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
        return 1
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")

if __name__ == "__main__":
    sys.exit(main_test())
