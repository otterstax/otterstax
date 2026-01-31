# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

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
                gen_id(2), 'Bonjour monde',
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


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Test PostgreSQL protocol compatibility')
    parser.add_argument('--local', action='store_true',
                        help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    tests = client(local=args.local)
    tests.run_all_tests()
