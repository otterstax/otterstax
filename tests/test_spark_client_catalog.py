# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
E2E test: the Spark Connect catalog API (spark.catalog.*) of OtterStax.

The mapping under test, in catalog "otterstax" (frontend/spark_connect_server/
catalog_rows.hpp):
  * a database is a schema, named as SQL writes it: `d.s` for the schema s of
    the local database d, `c.s` for a schema of the connection c — its
    backend's database on MySQL / ClickHouse, its schema on PostgreSQL;
  * Table.namespace is [c, s] for a remote table and [d, s] for a local one,
    so ".".join(namespace + [name]) is the qualified name of a table — the one
    tableExists / getTable / listColumns take and spark.table() reads.

setup() creates a local schema and a table in it through spark.sql() under a
unique name (the matrix runs several clients against one server at once);
cleanup() drops them.

Cases:
  test_current_catalog_is_otterstax — currentCatalog() is "otterstax", listCatalogs() lists it
  test_list_databases               — both connections' schemas and the local schema, in catalog "otterstax"
  test_database_exists              — true for those, false for an unknown name
  test_list_tables_mysql_mirror     — listTables("campaigns.db1") -> namespace [campaigns, db1]
  test_list_tables_pg_mirror        — listTables("products.public") -> namespace [products, public]
  test_list_tables_local            — listTables("local.<schema>") -> namespace [local, <schema>]
  test_table_exists                 — by (name, database) and by qualified name; unknown -> false
  test_get_table                    — getTable(qualified name) -> name, catalog, namespace
  test_list_columns                 — listColumns(qualified name) -> column names in table order
  test_table_round_trip             — spark.table(qualified name of a listed table) reads its rows

Only API present in PySpark 3.5 is used, so the file runs unchanged on every
client of the matrix (3.5.0 ... 4.2.0, see Dockerfile.spark-test).

Usage:
    python test_spark_client_catalog.py            # docker (host=test-otterstax)
    python test_spark_client_catalog.py --local    # local  (host=127.0.0.1)
"""

import sys
import uuid
import argparse
import traceback
from collections import namedtuple

import pyspark
from pyspark.sql import SparkSession

import config

CATALOG = "otterstax"

# A table the catalog must describe: the database listing it, its name, its
# namespace and its column names in table order.
Expected = namedtuple("Expected", "database name namespace columns")

MYSQL_MIRROR = Expected("campaigns.db1", "campaigns", ["campaigns", "db1"],
                        ["campaign_id", "campaign_name", "campaign_length", "budget"])
PG_MIRROR = Expected("products.public", "products", ["products", "public"],
                     ["product_id", "campaign_id", "product_name", "price", "category"])

LOCAL_TABLE = "items"
LOCAL_ROWS = [(1, "one"), (2, "two"), (3, "three")]


class SparkCatalogTest:
    def __init__(self, local=False):
        self.url = f"sc://{config.get_host(local)}:{config.SPARK_CONNECT_PORT}"
        # A schema of `local`, the clients' own database.
        self.db = f"spark_catalog_{uuid.uuid4().hex[:8]}"
        # Spark sessions have no place: one- and two-part table names are
        # refused, so every table statement names its tables in full. The
        # catalog API takes the schema's database name, `local.<schema>`.
        self.local_db = f"local.{self.db}"
        self.local = Expected(self.local_db, LOCAL_TABLE, ["local", self.db], ["id", "name"])
        self.spark = None
        self.db_created = False
        self.table_created = False
        print(f"PySpark client {pyspark.__version__}, Spark Connect server {self.url}, local schema local.{self.db}")

    # --- helpers ---

    @staticmethod
    def assert_equal(a, b, msg=""):
        if a != b:
            raise AssertionError(f"{a!r} != {b!r}. {msg}")

    @staticmethod
    def qualified(namespace, name):
        return ".".join(namespace + [name])

    def assert_table(self, table, expected, what):
        """A catalog Table entry names `expected` in catalog "otterstax"."""
        self.assert_equal(table.name, expected.name, f"{what}: name")
        self.assert_equal(table.catalog, CATALOG, f"{what}: catalog")
        self.assert_equal(table.namespace, expected.namespace, f"{what}: namespace")
        print(f"  ✅ {what}: {table.catalog}.{self.qualified(table.namespace, table.name)}")

    def listed_table(self, expected):
        """The entry of `expected` in listTables(<its database>)."""
        tables = self.spark.catalog.listTables(expected.database)
        for table in tables:
            if table.name == expected.name:
                return table
        raise AssertionError(f"{expected.name!r} not in listTables({expected.database!r}): "
                             f"{[table.name for table in tables]}")

    # --- setup / teardown ---

    def setup(self):
        """The session, and a local schema + table with LOCAL_ROWS made through spark.sql()."""
        self.spark = SparkSession.builder.remote(self.url).getOrCreate()
        self.spark.sql(f"CREATE SCHEMA {self.db}")
        self.db_created = True
        self.spark.sql(f"CREATE TABLE {self.local_db}.{LOCAL_TABLE} (id bigint, name string)")
        self.table_created = True
        values = ", ".join(f"({i}, '{name}')" for i, name in LOCAL_ROWS)
        self.spark.sql(f"INSERT INTO {self.local_db}.{LOCAL_TABLE} (id, name) VALUES {values}")
        print(f"  created {self.local_db}.{LOCAL_TABLE} with {len(LOCAL_ROWS)} rows")

    def cleanup(self):
        if self.spark is None:
            return
        try:
            if self.table_created:
                self.spark.sql(f"DROP TABLE {self.local_db}.{LOCAL_TABLE}")
            if self.db_created:
                self.spark.sql(f"DROP SCHEMA {self.db}")
                print(f"  dropped {self.db}")
        finally:
            self.spark.stop()

    # --- cases ---

    def test_current_catalog_is_otterstax(self):
        """currentCatalog() is "otterstax", and listCatalogs() lists it."""
        self.assert_equal(self.spark.catalog.currentCatalog(), CATALOG, "currentCatalog()")
        names = [c.name for c in self.spark.catalog.listCatalogs()]
        if CATALOG not in names:
            raise AssertionError(f"{CATALOG!r} not in listCatalogs(): {names}")
        print(f"  ✅ currentCatalog() = {CATALOG!r}, listCatalogs() = {names}")

    def test_list_databases(self):
        """listDatabases() holds both connections' schemas and the local schema, in catalog "otterstax"."""
        databases = {d.name: d for d in self.spark.catalog.listDatabases()}
        for name in (MYSQL_MIRROR.database, PG_MIRROR.database, self.local.database):
            if name not in databases:
                raise AssertionError(f"{name!r} not in listDatabases(): {list(databases)}")
            self.assert_equal(databases[name].catalog, CATALOG, f"catalog of database {name!r}")
        print(f"  ✅ listDatabases() holds {MYSQL_MIRROR.database!r}, {PG_MIRROR.database!r}, {self.local.database!r}")

    def test_database_exists(self):
        """databaseExists() is true for a connection's schema and the local schema, false for an unknown name."""
        for name in (MYSQL_MIRROR.database, PG_MIRROR.database, self.local.database):
            self.assert_equal(self.spark.catalog.databaseExists(name), True, f"databaseExists({name!r})")
        # A bare alias is no database: the databases are schemas.
        for name in ("no_such_database", "campaigns"):
            self.assert_equal(self.spark.catalog.databaseExists(name), False, f"databaseExists({name!r})")
        print("  ✅ databaseExists(): true for both connections' schemas and the local schema, "
              "false for an unknown name and a bare alias")

    def test_list_tables_mysql_mirror(self):
        """listTables(<alias.db>) lists a MySQL table under namespace [alias, db]."""
        self.assert_table(self.listed_table(MYSQL_MIRROR), MYSQL_MIRROR, f"listTables({MYSQL_MIRROR.database!r})")

    def test_list_tables_pg_mirror(self):
        """listTables(<alias.schema>) lists a PostgreSQL table under namespace [alias, schema]."""
        self.assert_table(self.listed_table(PG_MIRROR), PG_MIRROR, f"listTables({PG_MIRROR.database!r})")

    def test_list_tables_local(self):
        """listTables(<main.schema>) lists a local table under namespace [main, schema]."""
        self.assert_table(self.listed_table(self.local), self.local, f"listTables({self.local.database!r})")

    def test_table_exists(self):
        """tableExists() finds each table by (name, database) and by qualified name, and no unknown table."""
        catalog = self.spark.catalog
        for expected in (MYSQL_MIRROR, PG_MIRROR, self.local):
            qualified = self.qualified(expected.namespace, expected.name)
            unknown = self.qualified(expected.namespace, "no_such_table")
            self.assert_equal(catalog.tableExists(expected.name, expected.database), True,
                              f"tableExists({expected.name!r}, {expected.database!r})")
            self.assert_equal(catalog.tableExists(qualified), True, f"tableExists({qualified!r})")
            self.assert_equal(catalog.tableExists("no_such_table", expected.database), False,
                              f"tableExists('no_such_table', {expected.database!r})")
            self.assert_equal(catalog.tableExists(unknown), False, f"tableExists({unknown!r})")
            print(f"  ✅ tableExists(): {qualified} found both ways, an unknown table in it not")

    def test_get_table(self):
        """getTable(<qualified name>) returns the table with its catalog and namespace."""
        for expected in (MYSQL_MIRROR, PG_MIRROR, self.local):
            qualified = self.qualified(expected.namespace, expected.name)
            self.assert_table(self.spark.catalog.getTable(qualified), expected, f"getTable({qualified!r})")

    def test_list_columns(self):
        """listColumns(<qualified name>) returns the column names in table order."""
        for expected in (MYSQL_MIRROR, PG_MIRROR, self.local):
            qualified = self.qualified(expected.namespace, expected.name)
            names = [column.name for column in self.spark.catalog.listColumns(qualified)]
            self.assert_equal(names, expected.columns, f"listColumns({qualified!r})")
            print(f"  ✅ listColumns({qualified!r}) = {names}")

    def test_table_round_trip(self):
        """spark.table() on the qualified name of a listed table reads its rows."""
        for expected in (MYSQL_MIRROR, PG_MIRROR, self.local):
            table = self.listed_table(expected)
            qualified = self.qualified(table.namespace, table.name)
            count = self.spark.table(qualified).count()
            if expected is self.local:
                self.assert_equal(count, len(LOCAL_ROWS), f"spark.table({qualified!r}).count()")
            elif not count > 0:
                raise AssertionError(f"spark.table({qualified!r}).count() = {count}, expected rows")
            print(f"  ✅ spark.table({qualified!r}).count() = {count}")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_current_catalog_is_otterstax()
            self.test_list_databases()
            self.test_database_exists()
            self.test_list_tables_mysql_mirror()
            self.test_list_tables_pg_mirror()
            self.test_list_tables_local()
            self.test_table_exists()
            self.test_get_table()
            self.test_list_columns()
            self.test_table_round_trip()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Spark Connect E2E tests - catalog API")
    parser.add_argument("--local", action="store_true",
                        help="Use the local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()
    try:
        SparkCatalogTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Spark Connect catalog API\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Spark Connect catalog API\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
