import mysql.connector
import pymysql
import sys
import json
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
        }
        self.test_database = 'campaigns.db1.schema'
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

            cursor.execute(f"select budget from {self.test_database}.{self.test_table}")
            initial_b = cursor.fetchall()

            cursor.execute(f"update {self.test_database}.{self.test_table} set campaign_length = %s where _id = %s", (30, gen_id(1))) # subtract 10
            cursor.execute(f"update {self.test_database}.{self.test_table} set budget = %s where _id = %s", (64617.54, gen_id(1))) # subtract 40

            # sum(int) here has type boost::mysql::decimal, which is translated to DocumentTypes::STRING by translator, hence the constructor
            # todo: support decimals
            cursor.execute(f"select sum(campaign_length) as sum_ from {self.test_database}.{self.test_table}")
            upd = int(cursor.fetchall()[0][0])

            for row in initial_l:
                upd -= row[0]

            if (upd != -10):
                raise ValueError(f"Expected campaign_length diff: -10, got: {upd}")

            cursor.execute(f"select sum(budget) as sum_ from {self.test_database}.{self.test_table}")
            upd = cursor.fetchall()[0][0]

            for row in initial_b:
                upd -= row[0]

            self.assert_floating_equal(upd, -29.999999)

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
                "server_version": getattr(conn, 'get_server_version', lambda: 'unknown')(),
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
                cursor.execute(query, (gen_id(10),))
                self.assert_floating_equal(cursor.fetchall()[0][0], budget + 100.5, msg="budget should increase by 100.5")

                insert_query = f"insert into {self.test_database}.{self.test_table}(_id, campaign_name, campaign_length, budget) values (?, ?, ?, ?)"
                new_id = gen_id(99)
                cursor.execute(insert_query, (new_id, "Campaign Agree Recently", 40, budget + 100.5))

                cursor.execute(f"select campaign_name from {self.test_database}.{self.test_table} where _id = ?", (new_id,))
                self.assert_equal(cursor.fetchall()[0][0], "Campaign Agree Recently", "Inserted name mismatch")
                
                delete_query = f"delete from {self.test_database}.{self.test_table} where _id = ?"
                cursor.execute(delete_query, (new_id,))
                cursor.execute(f"select count(_id) as cnt from {self.test_database}.{self.test_table} where _id = ?", (new_id,))
                self.assert_equal(cursor.fetchall()[0][0], 0, "Row was not deleted")
                
                cursor.execute(query, (gen_id(2),))
                second_row = cursor.fetchall()
                self.assert_equal(len(second_row), 1, "Expected one row for reused prepared statement")

                cursor.execute(f"select ? from {self.test_database}.{self.test_table}", (100,))
                self.assert_equal(cursor.fetchall()[0][0], 100, "Select ? constant failed")

        except Exception as e:
            raise ValueError("Prepared queries threw something: " + str(e))

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
            self.test_character_encoding()
            self.test_protocol_capability_flags()
            self.test_prepared_queries()
            print("\033[92mTest success.\033[0m")
        except Exception as e:
            print(f"\033[91mAn error occurred: {e}\033[0m")
            print("\033[91mTest fails.\033[0m")
        finally:
            self.cleanup_test_data()
            print("Test completed.")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Test MySQL protocol compatibility')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    tests = client(local=args.local)
    tests.run_all_tests()
