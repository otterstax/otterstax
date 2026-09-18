# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

import sys
import psycopg2
import psycopg
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
            'port': 8817,
            'user': 'testuser',
            'password': 'testpass',
            'dbname': 'campaigns',
        }
        self.test_database = 'campaigns.db1.schema'
        self.test_table = 'postgresql_test_table'
        self.test_encoding_table = 'postgresql_test_encoding'

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
    def psycopg2_connection(self):
        """Context manager for psycopg2"""
        conn = None
        try:
            conn = psycopg2.connect(**self.proxy_config)
            conn.autocommit = True
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def psycopg3_connection(self, prepare):
        """Context manager for psycopg v3"""
        conn = None
        try:
            if not prepare:
                conn = psycopg.connect(**self.proxy_config, autocommit=True)
                conn.prepare_threshold = None
            else:
                conn = psycopg.connect(**self.proxy_config, autocommit=True)
            yield conn
        finally:
            if conn:
                conn.close()

    async def asyncpg_connection(self):
        """Async connection for asyncpg"""
        return await asyncpg.connect(
            host=self.proxy_config['host'],
            port=self.proxy_config['port'],
            user=self.proxy_config['user'],
            password=self.proxy_config['password'],
            database=self.proxy_config['dbname'],
        )

    def test_basic_connection(self):
        """Test 1: Basic connection and table creation with psycopg2"""
        try:
            with self.psycopg2_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"CREATE TABLE {self.test_database}.{self.test_table} (_id string, campaign_name string, campaign_length int, budget float);")
                self.test_db_created = True

        except Exception as e:
            raise ValueError(f"Failed to connect with psycopg2: {e}")

    def test_crud_queries(self):
        """Test 2: CRUD operations with psycopg v3"""
        with self.psycopg2_connection() as conn:
            cursor = conn.cursor()

            insert_sql = f"""
            INSERT INTO {self.test_database}.{self.test_table}
            (_id, campaign_name, campaign_length, budget) VALUES
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

            cursor.execute(f"SELECT campaign_length FROM {self.test_database}.{self.test_table}")
            initial_l = cursor.fetchall()

            cursor.execute(f"SELECT budget FROM {self.test_database}.{self.test_table}")
            initial_b = cursor.fetchall()

            cursor.execute(f"UPDATE {self.test_database}.{self.test_table} SET campaign_length = %s WHERE _id = %s",
                           (30, gen_id(1)))  # subtract 10
            cursor.execute(f"UPDATE {self.test_database}.{self.test_table} SET budget = %s WHERE _id = %s",
                           (64617.54, gen_id(1)))  # subtract 30

            cursor.execute(f"SELECT SUM(campaign_length) AS sum_ FROM {self.test_database}.{self.test_table}")
            upd = int(cursor.fetchall()[0][0])

            for row in initial_l:
                upd -= row[0]

            if (upd != -10):
                raise ValueError(f"Expected campaign_length diff: -10, got: {upd}")

            cursor.execute(f"SELECT SUM(budget) AS sum_ FROM {self.test_database}.{self.test_table}")
            upd = cursor.fetchall()[0][0]

            for row in initial_b:
                upd -= row[0]

            self.assert_floating_equal(upd, -29.999999)

    def test_float_column_reads_back_the_stored_value(self):
        """A FLOAT column comes back as the float32 the backend holds, not a rounding of it.

        MariaDB stores 64647.54 as the nearest float32, 64647.5390625. Its text
        protocol renders a FLOAT column with six significant digits ("64647.5"),
        so a connector reading rows over COM_QUERY hands back a value 0.04 off
        the stored one while SUM() over the same column is exact — the
        inconsistency test_crud_queries trips over. The connector must read
        the rows over the binary protocol, where the float travels as its four
        bytes.
        """
        table = f"{self.test_database}.{self.test_table}"
        stored = struct.unpack('f', struct.pack('f', 64647.54))[0]

        with self.psycopg2_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO {table} (_id, campaign_name, campaign_length, budget) VALUES (%s, %s, %s, %s)",
                           (gen_id(100), 'Campaign Float Exact', 1, 64647.54))
            cursor.execute(f"SELECT budget FROM {table} WHERE _id = %s", (gen_id(100),))
            self.assert_floating_equal(cursor.fetchall()[0][0], stored, msg="FLOAT column read back rounded")
            cursor.execute(f"DELETE FROM {table} WHERE _id = %s", (gen_id(100),))

    def test_remote_dml_row_counts(self):
        """Test 3: a REMOTE DML on the MySQL backend must report the rows it touched.

        Over the PG wire the count ends up in the CommandComplete tag, but it
        originates in boost.mysql's OK packet — the MySQL manager must read it
        and the PG frontend must forward it unchanged. Asserted on three wire
        shapes: psycopg2 (simple query), psycopg3 with parameters (unnamed
        statement, Bind/Execute) and `prepare=True` (named statement, Parsed
        once and Executed repeatedly through re-prepare).
        """
        table = f"{self.test_database}.{self.test_table}"

        with self.psycopg2_connection() as conn:          # simple query protocol
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO {table} (_id, campaign_name, campaign_length, budget) VALUES "
                f"('{gen_id(90)}', 'Counted Alpha', 7, 11.0), "
                f"('{gen_id(91)}', 'Counted Beta', 7, 12.0)")
            self.assert_equal(cursor.rowcount, 2, "simple-protocol remote INSERT row count")
            self.assert_equal(cursor.statusmessage, "INSERT 0 2", "simple-protocol remote INSERT tag")

            cursor.execute(f"UPDATE {table} SET budget = 13.0 WHERE campaign_length = 7")
            self.assert_equal(cursor.rowcount, 2, "simple-protocol remote UPDATE row count")
            self.assert_equal(cursor.statusmessage, "UPDATE 2", "simple-protocol remote UPDATE tag")

            cursor.execute(f"DELETE FROM {table} WHERE _id = '{gen_id(91)}'")
            self.assert_equal(cursor.rowcount, 1, "simple-protocol remote DELETE row count")
            self.assert_equal(cursor.statusmessage, "DELETE 1", "simple-protocol remote DELETE tag")

            # A statement matching nothing must report 0 — the count is read, not invented.
            cursor.execute(f"DELETE FROM {table} WHERE campaign_length = 4242")
            self.assert_equal(cursor.rowcount, 0, "remote DELETE matching nothing")
            self.assert_equal(cursor.statusmessage, "DELETE 0", "remote DELETE matching nothing tag")

        with self.psycopg3_connection(prepare=False) as conn:   # extended protocol
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO {table} (_id, campaign_name, campaign_length, budget) VALUES (%s, %s, %s, %s)",
                (gen_id(92), 'Counted Gamma', 8, 14.0))
            self.assert_equal(cursor.rowcount, 1, "extended-protocol remote INSERT row count")
            self.assert_equal(cursor.statusmessage, "INSERT 0 1", "extended-protocol remote INSERT tag")

            cursor.execute(f"UPDATE {table} SET budget = %s WHERE _id = %s", (15.0, gen_id(92)))
            self.assert_equal(cursor.rowcount, 1, "extended-protocol remote UPDATE row count")
            self.assert_equal(cursor.statusmessage, "UPDATE 1", "extended-protocol remote UPDATE tag")

            cursor.execute(f"DELETE FROM {table} WHERE _id = %s", (gen_id(92),))
            self.assert_equal(cursor.rowcount, 1, "extended-protocol remote DELETE row count")
            self.assert_equal(cursor.statusmessage, "DELETE 1", "extended-protocol remote DELETE tag")

        with self.psycopg3_connection(prepare=True) as conn:    # named statements
            cursor = conn.cursor()
            insert = f"INSERT INTO {table} (_id, campaign_name, campaign_length, budget) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert, (gen_id(93), 'Counted Delta', 9, 16.0), prepare=True)
            self.assert_equal(cursor.rowcount, 1, "named-statement remote INSERT row count")
            self.assert_equal(cursor.statusmessage, "INSERT 0 1", "named-statement remote INSERT tag")
            cursor.execute(insert, (gen_id(94), 'Counted Epsilon', 9, 17.0), prepare=True)
            self.assert_equal(cursor.rowcount, 1, "named-statement remote INSERT row count (re-executed)")

            update = f"UPDATE {table} SET budget = %s WHERE campaign_length = %s"
            cursor.execute(update, (18.0, 9), prepare=True)
            self.assert_equal(cursor.rowcount, 2, "named-statement remote UPDATE row count")
            self.assert_equal(cursor.statusmessage, "UPDATE 2", "named-statement remote UPDATE tag")
            cursor.execute(update, (19.0, 9), prepare=True)
            self.assert_equal(cursor.rowcount, 2, "named-statement remote UPDATE row count (re-executed)")

            delete = f"DELETE FROM {table} WHERE _id = %s"
            cursor.execute(delete, (gen_id(93),), prepare=True)
            self.assert_equal(cursor.rowcount, 1, "named-statement remote DELETE row count")
            self.assert_equal(cursor.statusmessage, "DELETE 1", "named-statement remote DELETE tag")
            cursor.execute(delete, (gen_id(94),), prepare=True)
            self.assert_equal(cursor.rowcount, 1, "named-statement remote DELETE row count (re-executed)")
            cursor.execute(delete, (gen_id(94),), prepare=True)
            self.assert_equal(cursor.rowcount, 0, "named-statement remote DELETE matching nothing")

        # Leave the table as test_crud_queries left it.
        with self.psycopg2_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table} WHERE campaign_length = 7")
            self.assert_equal(cursor.rowcount, 1, "the one remaining Counted row")

    def test_character_encoding(self):
        """Test 4: Character encoding with psycopg2"""
        with self.psycopg2_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"CREATE TABLE {self.test_database}.{self.test_encoding_table}(_id string, utf8_text string)")
            self.encoding_db_created = True

            insert_sql = f"""
            INSERT INTO {self.test_database}.{self.test_encoding_table}
            (_id, utf8_text) VALUES
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

            cursor.execute(f"SELECT utf8_text FROM {self.test_database}.{self.test_encoding_table}")
            results = cursor.fetchall()

            if (len(results) != 5):
                raise ValueError(f"Failed to get encoding_test strings, expected len: 5, got: {len(results)}")

            print("Encoding test:")
            for string in results:
                print(string[0])

    def test_protocol_capability_flags(self):
        """Test 5: Protocol capability flags with psycopg v3"""
        with self.psycopg3_connection(False) as conn:
            connection_info = {
                "server_version": conn.info.server_version,
                "encoding": conn.info.encoding,
            }

            print(f"Server version: {connection_info['server_version']}")
            print(f"Encoding: {connection_info['encoding']}")

            if connection_info["server_version"] is None:
                raise ValueError("Failed to get server version")

            if connection_info["encoding"] is None:
                raise ValueError("Failed to get encoding")

    def test_prepared_queries_psycopg3(self):
        """Test 6: Prepared statements with psycopg v3 using %s placeholders"""
        try:
            with self.psycopg3_connection(True) as conn:
                query = f"SELECT budget FROM {self.test_database}.{self.test_table} WHERE _id = %s"
                result = conn.execute(query, (gen_id(2),)).fetchall()
                self.assert_equal(len(result), 1, "Expected one row for prepared select")

                insert_query = f"INSERT INTO {self.test_database}.{self.test_table} (_id, campaign_name, campaign_length, budget) VALUES (%s, %s, %s, %s)"
                new_id = gen_id(99)
                conn.execute(insert_query, (new_id, "Campaign Test Psycopg3", 50, 12345.67))

                select_query = f"SELECT campaign_name FROM {self.test_database}.{self.test_table} WHERE _id = %s"
                result = conn.execute(select_query, (new_id,)).fetchall()
                self.assert_equal(result[0][0], "Campaign Test Psycopg3", "Inserted name mismatch")

                delete_query = f"DELETE FROM {self.test_database}.{self.test_table} WHERE _id = %s"
                conn.execute(delete_query, (new_id,))

                count_query = f"SELECT COUNT(_id) AS cnt FROM {self.test_database}.{self.test_table} WHERE _id = %s"
                result = conn.execute(count_query, (new_id,)).fetchall()
                self.assert_equal(result[0][0], 0, "Row was not deleted")

        except Exception as e:
            raise ValueError("psycopg3 prepared queries threw: " + str(e))

    def cleanup_test_data(self):
        """Test 8: Cleanup - DROP TABLE"""
        try:
            with self.psycopg2_connection() as conn:
                cursor = conn.cursor()
                if self.test_db_created:
                    cursor.execute(f"DROP TABLE {self.test_database}.{self.test_table}")

                if self.encoding_db_created:
                    cursor.execute(f"DROP TABLE {self.test_database}.{self.test_encoding_table}")
        except Exception as e:
            raise ValueError(f"Cleanup error {e}")

    def test_backend_error_carries_server_message(self):
        """A statement MariaDB refuses surfaces MariaDB's verdict on the PG wire too.

        Same shape as the MySQL-wire case: DROP TABLE of a table the backend
        does not have is routed to the backend without a registered schema,
        MariaDB answers 1051 "Unknown table", and that text — not a transport
        failure — is what the ErrorResponse carries.
        """
        missing = f"{self.test_database}.no_such_table_{gen_id(404)}"
        with self.psycopg2_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"drop table {missing}")
            except psycopg2.Error as e:
                message = str(e)
            else:
                raise AssertionError("DROP TABLE of a table the backend does not have must fail")
            assert "Unknown table" in message, f"MariaDB's verdict is missing from: {message}"
            assert "no_such_table" in message, f"the refused table is not named in: {message}"
            assert "connect failed" not in message, f"a transport error was reported instead: {message}"

            # The connection is still usable after the backend refused a statement.
            cursor.execute(f"select count(_id) as cnt from {self.test_database}.{self.test_table}")
            cursor.fetchall()

    def run_all_tests(self):
        try:
            self.test_basic_connection()
            self.test_crud_queries()
            self.test_float_column_reads_back_the_stored_value()
            self.test_remote_dml_row_counts()
            self.test_character_encoding()
            self.test_protocol_capability_flags()
            self.test_prepared_queries_psycopg3()
            self.test_backend_error_carries_server_message()
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
    parser = argparse.ArgumentParser(description='Test PostgreSQL protocol compatibility')
    parser.add_argument('--local', action='store_true',
                        help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    tests = client(local=args.local)
    try:
        tests.run_all_tests()
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - PostgreSQL Client/MySQL Backend\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - PostgreSQL Client/MySQL Backend\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
        return 1
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")

if __name__ == "__main__":
    sys.exit(main_test())
