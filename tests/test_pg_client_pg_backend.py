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

    def test_character_encoding(self):
        """Test 3: Character encoding with psycopg2"""
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
        """Test 4: Protocol capability flags with psycopg v3"""
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
            self.test_character_encoding()
            self.test_protocol_capability_flags()
            self.test_prepared_queries_psycopg3()
            print("\033[92mTest success.\033[0m")
        except Exception as e:
            print(f"\033[91mAn error occurred: {e}\033[0m")
            print("\033[91mTest fails.\033[0m")
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
