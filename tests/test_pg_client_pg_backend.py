# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

import sys
import psycopg2
import psycopg
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
            'dbname': 'products',  # PostgreSQL backend database
        }
        self.test_database = 'products.pgdb.public'
        self.test_table = 'postgresql_test_table'
        self.test_encoding_table = 'postgresql_test_encoding'

        self.test_db_created = False
        self.encoding_db_created = False
        print(f"Connecting to host: {host}")
        print(f"Testing PostgreSQL backend via pg_client protocol")

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
                    f"CREATE TABLE {self.test_database}.{self.test_table} (_id string, product_name string, price float, category string);")
                self.test_db_created = True

        except Exception as e:
            raise ValueError(f"Failed to connect with psycopg2: {e}")

    def test_crud_queries(self):
        """Test 2: CRUD operations with psycopg v3"""
        with self.psycopg2_connection() as conn:
            cursor = conn.cursor()

            insert_sql = f"""
            INSERT INTO {self.test_database}.{self.test_table}
            (_id, product_name, price, category) VALUES
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
                gen_id(1), 'Product Alpha', 149.99, 'Electronics',
                gen_id(2), 'Product Beta', 299.50, 'Electronics',
                gen_id(3), 'Product Gamma', 79.99, 'Books',
                gen_id(4), 'Product Delta', 199.00, 'Home',
                gen_id(5), 'Product Epsilon', 59.95, 'Books',
                gen_id(6), 'Product Zeta', 399.99, 'Electronics',
                gen_id(7), 'Product Eta', 29.99, 'Home',
                gen_id(8), 'Product Theta', 89.00, 'Books',
                gen_id(9), 'Product Iota', 159.50, 'Home',
                gen_id(10), 'Product Kappa', 249.99, 'Electronics',
                gen_id(11), 'Product Lambda', 39.95, 'Books',
                gen_id(12), 'Product Mu', 179.00, 'Home',
                gen_id(13), 'Product Nu', 129.99, 'Electronics',
            ))

            cursor.execute(f"SELECT price FROM {self.test_database}.{self.test_table}")
            initial_p = cursor.fetchall()

            cursor.execute(f"SELECT category FROM {self.test_database}.{self.test_table}")
            initial_c = cursor.fetchall()

            cursor.execute(f"UPDATE {self.test_database}.{self.test_table} SET price = %s WHERE _id = %s",
                           (139.99, gen_id(1)))  # subtract 10
            cursor.execute(f"UPDATE {self.test_database}.{self.test_table} SET category = %s WHERE _id = %s",
                           ('Mixed', gen_id(1)))

            cursor.execute(f"SELECT SUM(price) AS sum_ FROM {self.test_database}.{self.test_table}")
            upd = cursor.fetchall()[0][0]

            for row in initial_p:
                upd -= row[0]

            self.assert_floating_equal(upd, -10.0, tol=1e-3, msg="Price sum difference should be -10")

            cursor.execute(f"SELECT COUNT(category) AS cnt FROM {self.test_database}.{self.test_table} WHERE category = %s",
                           ('Mixed',))
            result = cursor.fetchall()[0][0]
            self.assert_equal(result, 1, msg="Should have 1 Mixed category")

    def test_remote_dml_row_counts(self):
        """Test 3: a REMOTE DML must report the rows the backend touched.

        The count for a backend statement is not in the result set — libpq puts
        it in the command tag and boost.mysql in the OK packet. Nothing read it,
        so every remote INSERT/UPDATE/DELETE came back as 0 rows while really
        having changed the backend. Asserted on three wire shapes: psycopg2 goes
        through simple query, psycopg3-with-parameters through Bind/Execute on
        the unnamed statement, and `prepare=True` through a NAMED statement that
        is Parsed once and Executed repeatedly (the worker drops a statement
        after it ran, so every later Execute is a re-prepare).
        """
        table = f"{self.test_database}.{self.test_table}"

        with self.psycopg2_connection() as conn:          # simple query protocol
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO {table} (_id, product_name, price, category) VALUES "
                f"('{gen_id(90)}', 'Counted Alpha', 11.0, 'Counted'), "
                f"('{gen_id(91)}', 'Counted Beta', 12.0, 'Counted')")
            self.assert_equal(cursor.rowcount, 2, "simple-protocol remote INSERT row count")
            self.assert_equal(cursor.statusmessage, "INSERT 0 2", "simple-protocol remote INSERT tag")

            cursor.execute(f"UPDATE {table} SET price = 13.0 WHERE category = 'Counted'")
            self.assert_equal(cursor.rowcount, 2, "simple-protocol remote UPDATE row count")
            self.assert_equal(cursor.statusmessage, "UPDATE 2", "simple-protocol remote UPDATE tag")

            cursor.execute(f"DELETE FROM {table} WHERE _id = '{gen_id(91)}'")
            self.assert_equal(cursor.rowcount, 1, "simple-protocol remote DELETE row count")
            self.assert_equal(cursor.statusmessage, "DELETE 1", "simple-protocol remote DELETE tag")

            # A statement matching nothing must report 0 — the count is read, not invented.
            cursor.execute(f"DELETE FROM {table} WHERE category = 'NoSuchCategory'")
            self.assert_equal(cursor.rowcount, 0, "remote DELETE matching nothing")
            self.assert_equal(cursor.statusmessage, "DELETE 0", "remote DELETE matching nothing tag")

        with self.psycopg3_connection(prepare=False) as conn:   # extended protocol
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO {table} (_id, product_name, price, category) VALUES (%s, %s, %s, %s)",
                (gen_id(92), 'Counted Gamma', 14.0, 'Counted3'))
            self.assert_equal(cursor.rowcount, 1, "extended-protocol remote INSERT row count")
            self.assert_equal(cursor.statusmessage, "INSERT 0 1", "extended-protocol remote INSERT tag")

            cursor.execute(f"UPDATE {table} SET price = %s WHERE _id = %s", (15.0, gen_id(92)))
            self.assert_equal(cursor.rowcount, 1, "extended-protocol remote UPDATE row count")
            self.assert_equal(cursor.statusmessage, "UPDATE 1", "extended-protocol remote UPDATE tag")

            cursor.execute(f"DELETE FROM {table} WHERE _id = %s", (gen_id(92),))
            self.assert_equal(cursor.rowcount, 1, "extended-protocol remote DELETE row count")
            self.assert_equal(cursor.statusmessage, "DELETE 1", "extended-protocol remote DELETE tag")

        with self.psycopg3_connection(prepare=True) as conn:    # named statements
            cursor = conn.cursor()
            insert = f"INSERT INTO {table} (_id, product_name, price, category) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert, (gen_id(93), 'Counted Delta', 16.0, 'Counted'), prepare=True)
            self.assert_equal(cursor.rowcount, 1, "named-statement remote INSERT row count")
            self.assert_equal(cursor.statusmessage, "INSERT 0 1", "named-statement remote INSERT tag")
            cursor.execute(insert, (gen_id(94), 'Counted Epsilon', 17.0, 'Counted'), prepare=True)
            self.assert_equal(cursor.rowcount, 1, "named-statement remote INSERT row count (re-executed)")

            update = f"UPDATE {table} SET price = %s WHERE _id = %s"
            cursor.execute(update, (18.0, gen_id(93)), prepare=True)
            self.assert_equal(cursor.rowcount, 1, "named-statement remote UPDATE row count")
            self.assert_equal(cursor.statusmessage, "UPDATE 1", "named-statement remote UPDATE tag")
            cursor.execute(update, (19.0, gen_id(94)), prepare=True)
            self.assert_equal(cursor.rowcount, 1, "named-statement remote UPDATE row count (re-executed)")

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
            cursor.execute(f"DELETE FROM {table} WHERE category = 'Counted'")
            self.assert_equal(cursor.rowcount, 1, "the one remaining Counted row")

    def test_remote_dml_row_counts_span_chunks(self):
        """Test 4: a remote UPDATE/DELETE over 2000 rows reports 2000, not a chunk's worth.

        The affected count crosses the actor graph in a payload whose size IS
        the count, and the engine caps a chunk at 1024 rows — a carrier clamped
        to one chunk reports 1024 (or breaks) for anything larger.
        """
        table = f"{self.test_database}.{self.test_table}"
        bulk_rows = 2000
        batch = 1000
        first = 10000

        with self.psycopg2_connection() as conn:          # simple query protocol
            cursor = conn.cursor()
            for start in range(0, bulk_rows, batch):
                values = ", ".join(
                    f"('{gen_id(first + i)}', 'Bulk {i}', {float(i)}, 'Bulk')"
                    for i in range(start, start + batch))
                cursor.execute(f"INSERT INTO {table} (_id, product_name, price, category) VALUES {values}")
                self.assert_equal(cursor.rowcount, batch, f"bulk remote INSERT batch at {start}")

            cursor.execute(f"SELECT COUNT(_id) FROM {table} WHERE category = 'Bulk'")
            self.assert_equal(cursor.fetchall()[0][0], bulk_rows, "bulk rows must be on the backend")

            cursor.execute(f"UPDATE {table} SET price = 1.0 WHERE category = 'Bulk'")
            self.assert_equal(cursor.rowcount, bulk_rows, "simple-protocol remote UPDATE over 2000 rows")
            self.assert_equal(cursor.statusmessage, f"UPDATE {bulk_rows}", "remote UPDATE tag over 2000 rows")

        with self.psycopg3_connection(prepare=False) as conn:   # extended protocol
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {table} SET price = %s WHERE category = %s", (2.0, 'Bulk'))
            self.assert_equal(cursor.rowcount, bulk_rows, "extended-protocol remote UPDATE over 2000 rows")
            self.assert_equal(cursor.statusmessage, f"UPDATE {bulk_rows}", "remote UPDATE tag over 2000 rows")

            cursor.execute(f"DELETE FROM {table} WHERE category = %s", ('Bulk',))
            self.assert_equal(cursor.rowcount, bulk_rows, "extended-protocol remote DELETE over 2000 rows")
            self.assert_equal(cursor.statusmessage, f"DELETE {bulk_rows}", "remote DELETE tag over 2000 rows")

            cursor.execute(f"SELECT COUNT(_id) FROM {table} WHERE category = %s", ('Bulk',))
            self.assert_equal(cursor.fetchall()[0][0], 0, "every bulk row must be gone")

    def test_character_encoding(self):
        """Test 5: Character encoding with psycopg2"""
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
        """Test 6: Protocol capability flags with psycopg v3"""
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
        """Test 7: Prepared statements with psycopg v3 using %s placeholders"""
        try:
            with self.psycopg3_connection(True) as conn:
                query = f"SELECT price FROM {self.test_database}.{self.test_table} WHERE _id = %s"
                result = conn.execute(query, (gen_id(2),)).fetchall()
                self.assert_equal(len(result), 1, "Expected one row for prepared select")

                insert_query = f"INSERT INTO {self.test_database}.{self.test_table} (_id, product_name, price, category) VALUES (%s, %s, %s, %s)"
                new_id = gen_id(99)
                conn.execute(insert_query, (new_id, "Product Test Psycopg3", 99.99, 'Test'))

                select_query = f"SELECT product_name FROM {self.test_database}.{self.test_table} WHERE _id = %s"
                result = conn.execute(select_query, (new_id,)).fetchall()
                self.assert_equal(result[0][0], "Product Test Psycopg3", "Inserted name mismatch")

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

    def run_all_tests(self):
        try:
            self.test_basic_connection()
            self.test_crud_queries()
            self.test_remote_dml_row_counts()
            self.test_remote_dml_row_counts_span_chunks()
            self.test_character_encoding()
            self.test_protocol_capability_flags()
            self.test_prepared_queries_psycopg3()
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
    parser = argparse.ArgumentParser(description='Test PostgreSQL protocol compatibility with PostgreSQL backend')
    parser.add_argument('--local', action='store_true',
                        help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    tests = client(local=args.local)
    try:
        tests.run_all_tests()
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - PostgreSQL Client/Backend\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - PostgreSQL Client/Backend\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
        return 1
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")

if __name__ == "__main__":
    sys.exit(main_test())
