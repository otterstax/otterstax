# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
E2E test: spark.sql() and DataFrame.show() through the Spark Connect frontend.

spark.sql() runs its statement once, when it is called: collecting the DataFrame
it returns must not run a CREATE or an INSERT again. show() prints the text the
server lays out (Spark's Dataset.showString); the expected text is rebuilt here
by the same rules.

setup() creates a local database under a unique name (the matrix runs several
clients against one server at once) with a table for show() and one for the
run-once checks; cleanup() drops them.

Cases:
  test_sql_derived_table_mysql — spark.sql(<derived table over campaigns>).collect() = the rows filtered in Python
  test_sql_derived_table_pg    — spark.sql(<GROUP BY derived table over products>).collect() = the sums added in Python
  test_sql_count               — spark.sql("SELECT ...").count() = the statement's row count
  test_sql_ddl_runs_once       — collect() on the DataFrames of CREATE DATABASE / CREATE TABLE does not run them again
  test_sql_insert_runs_once    — collect(), twice, on the DataFrame of an INSERT leaves the one row it inserted
  test_show                    — df.show(): header, separators, NULL, a long string cut to 20 characters with "..."
  test_show_top_rows           — df.show(3): three rows, then "only showing top 3 rows"
  test_show_untruncated        — df.show(truncate=False): whole strings, left-aligned
  test_show_vertical           — df.show(vertical=True): one -RECORD block per row
  test_sql_show                — spark.sql("SELECT ...").show() prints the table df.show() prints

Only API present in PySpark 3.5 is used, so the file runs unchanged on every
client of the matrix (3.5.0 ... 4.2.0, see Dockerfile.spark-test).

Usage:
    python test_spark_client_sql_and_show.py            # docker (host=test-otterstax)
    python test_spark_client_sql_and_show.py --local    # local  (host=127.0.0.1)
"""

import io
import sys
import uuid
import argparse
import contextlib
import traceback

import pyspark
from pyspark.sql import SparkSession

import config

CAMPAIGNS = "campaigns.db1.schema.campaigns"  # MySQL: campaign_id, campaign_name, campaign_length, budget
PRODUCTS = "products.pgdb.public.products"    # PostgreSQL: product_id, campaign_id, product_name, price, category

# A derived table over each mirror. Sums of two-decimal prices never equal
# 800.005, so no campaign sits on the PostgreSQL threshold.
MYSQL_DERIVED = (f"SELECT d.campaign_id, d.budget FROM "
                 f"(SELECT campaign_id, budget FROM {CAMPAIGNS} WHERE budget > 50000) d "
                 f"WHERE d.campaign_id <= 25")
PG_DERIVED = (f"SELECT d.campaign_id, d.total FROM "
              f"(SELECT campaign_id, SUM(price) AS total FROM {PRODUCTS} GROUP BY campaign_id) d "
              f"WHERE d.total > 800.005")

SHOW_TABLE = "show_items"
SHOW_COLUMNS = ("id", "name")
SHOW_ROWS = [(1, "alpha"), (2, None), (3, "a string longer than twenty characters"), (4, "delta"), (5, "epsilon")]
ONCE_TABLE = "once_items"


class SparkSqlAndShowTest:
    def __init__(self, local=False):
        self.url = f"sc://{config.get_host(local)}:{config.SPARK_CONNECT_PORT}"
        self.db = f"spark_sql_{uuid.uuid4().hex[:8]}"
        self.spark = None
        self.created_db = None     # the DataFrame spark.sql() returned for CREATE DATABASE
        self.created_table = None  # ... and for CREATE TABLE <ONCE_TABLE>
        self.tables = []           # local tables to drop, in creation order
        print(f"PySpark client {pyspark.__version__}, Spark Connect server {self.url}, local database {self.db}")

    # --- helpers ---

    @staticmethod
    def assert_equal(a, b, msg=""):
        if a != b:
            raise AssertionError(f"{a!r} != {b!r}. {msg}")

    @staticmethod
    def assert_close(a, b, msg=""):
        """SQL and Python may add the same floats in a different order."""
        if abs(float(a) - float(b)) > 1e-6 * max(1.0, abs(float(b))):
            raise AssertionError(f"{a!r} != {b!r} (relative 1e-6). {msg}")

    def mysql_derived_expected(self):
        """MYSQL_DERIVED's rows, filtered in Python from the plain table."""
        rows = self.spark.sql(f"SELECT campaign_id, budget FROM {CAMPAIGNS}").collect()
        return sorted((r["campaign_id"], r["budget"]) for r in rows
                      if r["budget"] > 50000 and r["campaign_id"] <= 25), len(rows)

    def pg_derived_expected(self):
        """PG_DERIVED's rows, summed in Python from the plain table: {campaign_id: total}."""
        totals = {}
        for r in self.spark.sql(f"SELECT campaign_id, price FROM {PRODUCTS}").collect():
            totals[r["campaign_id"]] = totals.get(r["campaign_id"], 0.0) + r["price"]
        return {k: v for k, v in totals.items() if v > 800.005}, len(totals)

    @staticmethod
    def literal(text):
        """A string cell as a SQL literal, None as NULL."""
        return "NULL" if text is None else f"'{text}'"

    @staticmethod
    def printed(show, *args, **kwargs):
        """What a show() call prints."""
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            show(*args, **kwargs)
        return out.getvalue()

    @staticmethod
    def show_string(columns, rows, n=20, truncate=20, vertical=False, null="NULL"):
        """Spark's Dataset.showString (3.5 to 4.2) for ASCII cells: the text show() prints."""
        def cell(value):
            text = null if value is None else str(value)
            if 0 < truncate < len(text):
                text = text[:truncate] if truncate < 4 else text[:truncate - 3] + "..."
            return text

        table = [list(columns)] + [[cell(value) for value in row] for row in rows[:n]]
        if vertical:
            name_width = max([3] + [len(name) for name in columns])
            data_width = max([3] + [len(text) for row in table[1:] for text in row])
            out = ""
            for i, row in enumerate(table[1:]):
                out += f"-RECORD {i}".ljust(name_width + data_width + 5, "-") + "\n"
                out += "".join(f" {name.ljust(name_width)} | {text.ljust(data_width)} \n"
                               for name, text in zip(columns, row))
        else:
            widths = [max([3] + [len(row[i]) for row in table]) for i in range(len(columns))]
            pad = str.rjust if truncate > 0 else str.ljust
            sep = "+" + "+".join("-" * width for width in widths) + "+\n"
            lines = ["|" + "|".join(pad(text, width) for text, width in zip(row, widths)) + "|\n" for row in table]
            out = sep + lines[0] + sep + "".join(lines[1:]) + sep
        if vertical and len(table) == 1:
            out += "(0 rows)\n"
        elif len(rows) > n:
            out += f"only showing top {n} {'row' if n == 1 else 'rows'}\n"
        return out

    def assert_shown(self, printed, label, **show_args):
        """`printed` is the showString of SHOW_ROWS under these show() arguments.

        Spark 3.5+ prints a NULL cell as "NULL" and the frontend's spec as "null", so
        both are accepted; Spark 4 dropped the newline after the footer, so trailing
        newlines are not compared."""
        expected = [self.show_string(SHOW_COLUMNS, SHOW_ROWS, null=null, **show_args).rstrip("\n")
                    for null in ("NULL", "null")]
        if printed.rstrip("\n") not in expected:
            raise AssertionError(f"{label} printed:\n{printed}\nexpected:\n{expected[0]}")
        print(f"  ✅ {label} printed the expected table:\n{printed}")

    def show_frame(self):
        """The show() table, ordered so the printed rows are known."""
        return self.spark.table(f"{self.db}.{SHOW_TABLE}").orderBy("id")

    # --- setup / teardown ---

    def setup(self):
        """The session; a local database with the show() table and the run-once table, made through spark.sql()."""
        self.spark = SparkSession.builder.remote(self.url).getOrCreate()
        self.created_db = self.spark.sql(f"CREATE DATABASE {self.db}")
        self.spark.sql(f"CREATE TABLE {self.db}.{SHOW_TABLE} (id bigint, name string)")
        self.tables.append(SHOW_TABLE)
        values = ", ".join(f"({i}, {self.literal(name)})" for i, name in SHOW_ROWS)
        self.spark.sql(f"INSERT INTO {self.db}.{SHOW_TABLE} (id, name) VALUES {values}")
        self.created_table = self.spark.sql(f"CREATE TABLE {self.db}.{ONCE_TABLE} (id bigint, name string)")
        self.tables.append(ONCE_TABLE)
        print(f"  created {self.db}.{SHOW_TABLE} ({len(SHOW_ROWS)} rows) and {self.db}.{ONCE_TABLE} (empty)")

    def cleanup(self):
        if self.spark is None:
            return
        try:
            for table in reversed(self.tables):
                self.spark.sql(f"DROP TABLE {self.db}.{table}")
            if self.created_db is not None:
                self.spark.sql(f"DROP DATABASE {self.db}")
        finally:
            self.spark.stop()

    # --- cases ---

    def test_sql_derived_table_mysql(self):
        """spark.sql() over a derived table of campaigns returns the rows the plain table gives when filtered in Python."""
        expected, total = self.mysql_derived_expected()
        got = sorted((r["campaign_id"], r["budget"]) for r in self.spark.sql(MYSQL_DERIVED).collect())
        self.assert_equal(got, expected, f"rows of {MYSQL_DERIVED}")
        if not 0 < len(got) < total:
            raise AssertionError(f"{MYSQL_DERIVED} kept {len(got)} of {total} rows; it does not discriminate")
        print(f"  ✅ derived table over campaigns: {len(got)} of {total} rows, as filtered in Python")

    def test_sql_derived_table_pg(self):
        """spark.sql() over a GROUP BY derived table of products returns the sums the plain table adds up to."""
        expected, campaigns = self.pg_derived_expected()
        rows = self.spark.sql(PG_DERIVED).collect()
        got = {r["campaign_id"]: r["total"] for r in rows}
        self.assert_equal(len(got), len(rows), f"one row per campaign_id in {PG_DERIVED}")
        self.assert_equal(sorted(got), sorted(expected), f"campaigns of {PG_DERIVED}")
        for campaign_id, total in expected.items():
            self.assert_close(got[campaign_id], total, f"total of campaign {campaign_id}")
        if not 0 < len(got) < campaigns:
            raise AssertionError(f"{PG_DERIVED} kept {len(got)} of {campaigns} campaigns; it does not discriminate")
        print(f"  ✅ GROUP BY derived table over products: {len(got)} of {campaigns} campaigns, as summed in Python")

    def test_sql_count(self):
        """spark.sql("SELECT ...").count() is the number of rows the statement returns."""
        expected, _ = self.mysql_derived_expected()
        self.assert_equal(int(self.spark.sql(MYSQL_DERIVED).count()), len(expected), f"count() of {MYSQL_DERIVED}")
        products = len(self.spark.sql(f"SELECT product_id FROM {PRODUCTS}").collect())
        self.assert_equal(int(self.spark.sql(f"SELECT product_id FROM {PRODUCTS}").count()), products,
                          "count() of SELECT product_id FROM products")
        print(f"  ✅ spark.sql(...).count(): {len(expected)} (derived table), {products} (products)")

    def test_sql_ddl_runs_once(self):
        """Collecting the DataFrames of CREATE DATABASE and CREATE TABLE does not run them again."""
        # A second CREATE of the same database or table would fail: it exists.
        self.created_db.collect()
        self.created_table.collect()
        self.assert_equal(self.spark.sql(f"SELECT id FROM {self.db}.{ONCE_TABLE}").collect(), [],
                          f"{ONCE_TABLE} after collecting its CREATE TABLE")
        print("  ✅ collect() on CREATE DATABASE / CREATE TABLE ran neither again")

    def test_sql_insert_runs_once(self):
        """Collecting the DataFrame of an INSERT, twice, leaves the one row the INSERT wrote."""
        inserted = self.spark.sql(f"INSERT INTO {self.db}.{ONCE_TABLE} (id, name) VALUES (1, 'one')")
        inserted.collect()
        inserted.collect()
        rows = [tuple(r) for r in self.spark.sql(f"SELECT id, name FROM {self.db}.{ONCE_TABLE}").collect()]
        self.assert_equal(rows, [(1, "one")], "rows after one INSERT and two collect() calls on its DataFrame")
        print("  ✅ INSERT through spark.sql() ran once: one row after two collect() calls")

    def test_show(self):
        """df.show() prints the header, the separators, NULL and a long string cut to 20 characters with "..."."""
        self.assert_shown(self.printed(self.show_frame().show), "df.show()")

    def test_show_top_rows(self):
        """df.show(3) prints three rows, then "only showing top 3 rows"."""
        self.assert_shown(self.printed(self.show_frame().show, 3), "df.show(3)", n=3)

    def test_show_untruncated(self):
        """df.show(truncate=False) prints whole strings, left-aligned."""
        self.assert_shown(self.printed(self.show_frame().show, truncate=False), "df.show(truncate=False)",
                          truncate=0)

    def test_show_vertical(self):
        """df.show(vertical=True) prints one -RECORD block per row."""
        self.assert_shown(self.printed(self.show_frame().show, vertical=True), "df.show(vertical=True)",
                          vertical=True)

    def test_sql_show(self):
        """spark.sql("SELECT ...").show() prints the table df.show() prints."""
        df = self.spark.sql(f"SELECT id, name FROM {self.db}.{SHOW_TABLE} ORDER BY id")
        self.assert_shown(self.printed(df.show), "spark.sql('SELECT id, name ... ORDER BY id').show()")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_sql_derived_table_mysql()
            self.test_sql_derived_table_pg()
            self.test_sql_count()
            self.test_sql_ddl_runs_once()
            self.test_sql_insert_runs_once()
            self.test_show()
            self.test_show_top_rows()
            self.test_show_untruncated()
            self.test_show_vertical()
            self.test_sql_show()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Spark Connect E2E tests - spark.sql() and show()")
    parser.add_argument("--local", action="store_true",
                        help="Use the local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()
    try:
        SparkSqlAndShowTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Spark Connect spark.sql() and show()\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Spark Connect spark.sql() and show()\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
