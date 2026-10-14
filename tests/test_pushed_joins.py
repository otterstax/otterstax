# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""JOINs of tables of ONE server, pushed to it as one statement (otterbrix/federation/CLAUDE.md).

Over the PostgreSQL wire (port 8817), against tables this file makes on the
backends directly (clickhouse_driver to clickhouse1, psycopg2 to postgres1,
pymysql to mariadb1, as create_test_data.py does), so it runs only inside the
test network; the server discovers each table on first use (one rediscovery
retry).

  * ClickHouse: an INNER and a LEFT JOIN whose second table has a Bool column
    (system.columns says Bool, the wire says UInt8): the JOIN's result columns
    are read with the declared types of every table of the statement, so the
    Bool arrives as a boolean (and NULL where the LEFT JOIN found no row), not
    as a stale-schema error.
  * ClickHouse and MariaDB: ORDER BY a name both tables have, with LIMIT — the
    output is named on the backend (`AS name`), so the backend sorts by the
    output, not by the left table's column (ClickHouse) and not refusing the
    name as ambiguous (MariaDB 1052).
  * PostgreSQL: DISTINCT ON over a JOIN — over plain columns and over a grouped
    projection, whose DISTINCT ON key names the group's output.
"""

import argparse
import sys

import clickhouse_driver
import psycopg2
import pymysql

import config

CLICKHOUSE = dict(host=config.CH_DOCKER_HOST, port=config.CH_PORT, user="chuser", password="chpassword",
                  database=config.CH_DATABASE)
POSTGRES = dict(host="postgres1", port=5432, user="pguser", password="pgpassword", dbname="pgdb")
MARIADB = dict(host="mariadb1", port=3306, user="user1", password="password1", database="db1")

CH_PRODUCTS = f"{config.CH_ALIAS}.{config.CH_DATABASE}.schema.pj_products"
CH_FLAGS = f"{config.CH_ALIAS}.{config.CH_DATABASE}.schema.pj_flags"
PG_CUSTOMERS = "products.pgdb.public.pj_customers"
PG_ORDERS = "products.pgdb.public.pj_orders"
MY_PRODUCTS = "campaigns.db1.schema.pj_products"
MY_FLAGS = "campaigns.db1.schema.pj_flags"

# products: 1 apple, 2 pear, 3 plum (no flags row). The flags' own `name` sorts the other way round
# from the products' (apple's flag is z-one, pear's a-two): sorting by the wrong table's `name`
# picks the other row.
PRODUCTS = [(1, "apple"), (2, "pear"), (3, "plum")]
FLAGS = [(1, "z-one", True), (2, "a-two", False)]
# customers 1 ann, 2 bob; ann's orders 10 and 30, bob's 20, 20 and 5.
CUSTOMERS = [(1, "ann"), (2, "bob")]
ORDERS = [(1, 1, 10), (2, 1, 30), (3, 2, 20), (4, 2, 20), (5, 2, 5)]


class PushedJoinsTest:
    def __init__(self, local=False):
        self.host = config.get_host(local)
        self.pg = None          # PostgreSQL wire to otterstax
        self.clickhouse = None
        self.postgres = None
        self.mariadb = None

    # ── helpers ─────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def rows(self, sql):
        """Run a SELECT over the PostgreSQL wire; its rows as tuples."""
        with self.pg.cursor() as cur:
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()]

    # ── setup / teardown ────────────────────────────────────────────────────
    def setup(self):
        self.clickhouse = clickhouse_driver.Client(**CLICKHOUSE)
        for table in ("pj_products", "pj_flags"):
            self.clickhouse.execute(f"DROP TABLE IF EXISTS {table}")
        self.clickhouse.execute(
            "CREATE TABLE pj_products (product_id Int32, name String) ENGINE = MergeTree ORDER BY product_id")
        self.clickhouse.execute(
            "CREATE TABLE pj_flags (product_id Int32, name String, ok Bool) ENGINE = MergeTree ORDER BY product_id")
        self.clickhouse.execute("INSERT INTO pj_products (product_id, name) VALUES", PRODUCTS)
        self.clickhouse.execute("INSERT INTO pj_flags (product_id, name, ok) VALUES", FLAGS)

        self.postgres = psycopg2.connect(**POSTGRES)
        self.postgres.autocommit = True
        with self.postgres.cursor() as cur:
            for table in ("pj_orders", "pj_customers"):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute("CREATE TABLE pj_customers (id integer PRIMARY KEY, name varchar(16) NOT NULL)")
            cur.execute("CREATE TABLE pj_orders (id integer PRIMARY KEY, cid integer NOT NULL, amount integer NOT NULL)")
            cur.executemany("INSERT INTO pj_customers (id, name) VALUES (%s, %s)", CUSTOMERS)
            cur.executemany("INSERT INTO pj_orders (id, cid, amount) VALUES (%s, %s, %s)", ORDERS)

        self.mariadb = pymysql.connect(**MARIADB, autocommit=True)
        with self.mariadb.cursor() as cur:
            for table in ("pj_products", "pj_flags"):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute("CREATE TABLE pj_products (product_id INT PRIMARY KEY, name VARCHAR(16) NOT NULL)")
            cur.execute("CREATE TABLE pj_flags (product_id INT PRIMARY KEY, name VARCHAR(16) NOT NULL)")
            cur.executemany("INSERT INTO pj_products (product_id, name) VALUES (%s, %s)", PRODUCTS)
            cur.executemany("INSERT INTO pj_flags (product_id, name) VALUES (%s, %s)",
                            [(pid, name) for pid, name, _ in FLAGS])

        self.pg = psycopg2.connect(host=self.host, port=config.PG_PORT, user="testuser", password="testpass",
                                   dbname=config.CH_ALIAS)
        self.pg.autocommit = True
        # The tables were made on the backends after the server started: the first statement naming
        # each one rediscovers it.
        for table in (CH_PRODUCTS, CH_FLAGS, PG_CUSTOMERS, PG_ORDERS, MY_PRODUCTS, MY_FLAGS):
            self.rows(f"SELECT count(*) FROM {table}")

    def cleanup(self):
        try:
            if self.pg is not None:
                self.pg.close()
        finally:
            if self.clickhouse is not None:
                for table in ("pj_products", "pj_flags"):
                    self.clickhouse.execute(f"DROP TABLE IF EXISTS {table}")
                self.clickhouse.disconnect()
            if self.postgres is not None:
                with self.postgres.cursor() as cur:
                    for table in ("pj_orders", "pj_customers"):
                        cur.execute(f"DROP TABLE IF EXISTS {table}")
                self.postgres.close()
            if self.mariadb is not None:
                with self.mariadb.cursor() as cur:
                    for table in ("pj_products", "pj_flags"):
                        cur.execute(f"DROP TABLE IF EXISTS {table}")
                self.mariadb.close()

    # ── ClickHouse ──────────────────────────────────────────────────────────
    def test_clickhouse_inner_join_reads_the_second_tables_bool(self):
        """ClickHouse INNER JOIN: the second table's Bool arrives as a boolean."""
        self.assert_equal(
            self.rows(f"SELECT p.name, f.ok FROM {CH_PRODUCTS} p JOIN {CH_FLAGS} f "
                      f"ON p.product_id = f.product_id ORDER BY p.product_id"),
            [("apple", True), ("pear", False)],
            "the Bool of the JOIN's second table")
        print("  ClickHouse INNER JOIN: the second table's Bool ✓")

    def test_clickhouse_left_join_pads_the_bool_with_null(self):
        """ClickHouse LEFT JOIN: a product without flags reads NULL, not the Bool's default false."""
        self.assert_equal(
            self.rows(f"SELECT p.name, f.ok FROM {CH_PRODUCTS} p LEFT JOIN {CH_FLAGS} f "
                      f"ON p.product_id = f.product_id ORDER BY p.product_id"),
            [("apple", True), ("pear", False), ("plum", None)],
            "plum has no flags row")
        print("  ClickHouse LEFT JOIN: the padded Bool is NULL ✓")

    def test_clickhouse_order_by_a_name_both_tables_have(self):
        """ClickHouse: ORDER BY the output `name` (the flags' name) with LIMIT, not the left table's name."""
        self.assert_equal(
            self.rows(f"SELECT f.name, count(p.product_id) AS n FROM {CH_PRODUCTS} p JOIN {CH_FLAGS} f "
                      f"ON p.product_id = f.product_id GROUP BY f.name ORDER BY name DESC LIMIT 1"),
            [("z-one", 1)],
            "sorted by the flags' name: z-one first (the products' name would put pear's a-two first)")
        print("  ClickHouse ORDER BY a shared column name + LIMIT ✓")

    # ── MariaDB ─────────────────────────────────────────────────────────────
    def test_mariadb_order_by_a_name_both_tables_have(self):
        """MariaDB: ORDER BY the output `name` with LIMIT is not refused as ambiguous (1052)."""
        self.assert_equal(
            self.rows(f"SELECT f.name, count(p.product_id) AS n FROM {MY_PRODUCTS} p JOIN {MY_FLAGS} f "
                      f"ON p.product_id = f.product_id GROUP BY f.name ORDER BY name DESC LIMIT 1"),
            [("z-one", 1)],
            "sorted by the flags' name")
        print("  MariaDB ORDER BY a shared column name + LIMIT ✓")

    # ── PostgreSQL ──────────────────────────────────────────────────────────
    def test_pg_distinct_on_over_a_join(self):
        """PostgreSQL: DISTINCT ON (c.name) over a JOIN keeps each customer's first row in ORDER BY order."""
        self.assert_equal(
            self.rows(f"SELECT DISTINCT ON (c.name) c.name, o.amount FROM {PG_ORDERS} o JOIN {PG_CUSTOMERS} c "
                      f"ON o.cid = c.id ORDER BY c.name, o.amount DESC"),
            [("ann", 30), ("bob", 20)],
            "each customer's largest amount")
        print("  PostgreSQL DISTINCT ON over a JOIN ✓")

    def test_pg_distinct_on_over_a_grouped_join(self):
        """PostgreSQL: DISTINCT ON over a grouped JOIN keys on the group's output c.name."""
        self.assert_equal(
            self.rows(f"SELECT DISTINCT ON (c.name) c.name, o.amount, count(o.id) AS n FROM {PG_ORDERS} o "
                      f"JOIN {PG_CUSTOMERS} c ON o.cid = c.id GROUP BY c.name, o.amount "
                      f"ORDER BY c.name, o.amount DESC"),
            [("ann", 30, 1), ("bob", 20, 2)],
            "each customer's largest amount and how many orders hold it")
        print("  PostgreSQL DISTINCT ON over a grouped JOIN ✓")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_clickhouse_inner_join_reads_the_second_tables_bool()
            self.test_clickhouse_left_join_pads_the_bool_with_null()
            self.test_clickhouse_order_by_a_name_both_tables_have()
            self.test_mariadb_order_by_a_name_both_tables_have()
            self.test_pg_distinct_on_over_a_join()
            self.test_pg_distinct_on_over_a_grouped_join()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="JOINs of one server pushed as one statement")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) for otterstax; the backends are reached by their "
                             "docker names, so the file needs the test network either way")
    args = parser.parse_args()

    tests = PushedJoinsTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - pushed JOINs: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - pushed JOINs\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
