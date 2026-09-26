# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
E2E test: Spark Connect DataFrame operations (Path B) over spark.table() of the
MySQL `campaigns` and PostgreSQL `products` mirrors and of a local table, each
checked against the same query written as SQL (Path A).

Cases:
  test_column_filter_matches_sql        — filter(df.col > x) and filter("col > x") keep the SQL WHERE rows
  test_literal_on_the_left              — filter(lit(x) < col) / filter(lit(x) >= col) keep the mirrored rows
  test_and_or_combinations              — &, | and a nested mix keep the rows of their SQL AND / OR
  test_chained_filters_are_conjunction  — filter(a).filter(b) keeps the rows of filter(a & b)
  test_group_by_aggregates_match_sql    — groupBy().agg(count, sum, avg) equals SQL GROUP BY
  test_avg_keeps_the_fraction           — avg over the local BIGINT column is the DOUBLE mean of the rows
  test_order_by_desc_limit              — orderBy(desc).limit(5) is the five largest prices, largest first
  test_select_then_count                — select(...).count() is SQL COUNT(*)
  test_distinct_then_count              — distinct().count() / select(c).distinct().count() count SELECT DISTINCT
  test_limit_then_first                 — orderBy(k).limit(5).first() is SQL ORDER BY k LIMIT 1
  test_group_by_then_select             — groupBy().agg(...).select(...) is the SQL GROUP BY's columns
  test_group_by_then_filter             — groupBy().agg(sum.alias("s")).filter(s > x) is SQL HAVING
  test_select_then_select               — select(...).select(...) is the SQL projection
  test_sql_limit_then_limit             — spark.sql("... LIMIT 10").limit(3) is the first three of those rows
  test_union_by_name_reordered_rejected — unionByName over reordered columns is refused, naming unionByName

setup() creates a local database and table through spark.sql() under a unique
name (the matrix runs several clients against one server at once); cleanup()
drops them. Only API present in PySpark 3.5 is used, so the file runs unchanged
on every client of the matrix (3.5.0 ... 4.2.0, see Dockerfile.spark-test).

Usage:
    python test_spark_client_dataframe_ops.py            # docker (host=test-otterstax)
    python test_spark_client_dataframe_ops.py --local    # local  (host=127.0.0.1)
"""

import sys
import uuid
import argparse
import traceback

import pyspark
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import config

CAMPAIGNS = "campaigns.db1.schema.campaigns"  # MySQL: campaign_id, campaign_name, campaign_length, budget
PRODUCTS = "products.pgdb.public.products"    # PostgreSQL: product_id, campaign_id, product_name, price, category

# The local table: (id, grp, amount), the last row twice so distinct() has a row to drop.
LOCAL_TABLE = "ops_items"
LOCAL_ROWS = [(1, "a", 10), (2, "a", 20), (3, "b", 30), (4, "b", 40), (5, "c", 50), (5, "c", 50)]


class SparkDataFrameOpsTest:
    def __init__(self, local=False):
        self.url = f"sc://{config.get_host(local)}:{config.SPARK_CONNECT_PORT}"
        self.db = f"spark_ops_{uuid.uuid4().hex[:8]}"
        self.local_table = f"{self.db}.{LOCAL_TABLE}"
        self.spark = None
        self.campaigns = None  # spark.table(CAMPAIGNS)
        self.products = None   # spark.table(PRODUCTS)
        self.local = None      # spark.table(self.local_table)
        self.totals = {}       # table -> row count
        self.db_created = False
        self.table_created = False
        print(f"PySpark client {pyspark.__version__}, Spark Connect server {self.url}, local database {self.db}")

    # --- helpers ---

    @staticmethod
    def assert_equal(a, b, msg=""):
        if a != b:
            raise AssertionError(f"{a!r} != {b!r}. {msg}")

    @staticmethod
    def close(a, b):
        """Equal values; a float on either side within 1e-6 relative (two plans may add in another order)."""
        if isinstance(a, float) or isinstance(b, float):
            return a is not None and b is not None and abs(float(a) - float(b)) <= 1e-6 * max(1.0, abs(float(b)))
        return a == b

    def assert_close(self, a, b, msg=""):
        if not self.close(a, b):
            raise AssertionError(f"{a!r} != {b!r} (relative 1e-6). {msg}")

    @staticmethod
    def keys(df, key):
        """The sorted `key` column of `df`: the rows two equivalent plans must share."""
        return sorted(row[key] for row in df.collect())

    def assert_filter(self, df, table, key, where, label):
        """`df` keeps exactly the rows of SQL `WHERE <where>` over `table`, and <where>
        keeps some rows and drops others (a vacuous predicate would prove nothing)."""
        got = self.keys(df, key)
        expected = self.keys(self.spark.sql(f"SELECT {key} FROM {table} WHERE {where}"), key)
        self.assert_equal(got, expected, f"{label}: rows differ from SQL WHERE {where}")
        total = self.totals[table]
        if not 0 < len(got) < total:
            raise AssertionError(f"{label}: kept {len(got)} of {total} rows; the predicate does not discriminate")
        print(f"  ✅ {label}: {len(got)} of {total} rows, as SQL WHERE {where}")

    def assert_rows(self, got, sql, label):
        """`got` (collected rows) holds the columns and rows of `sql`, in any order."""
        expected = self.spark.sql(sql).collect()
        if got and expected:
            self.assert_equal(list(got[0].asDict()), list(expected[0].asDict()), f"{label}: columns")
        self.assert_equal(sorted(map(tuple, got), key=repr), sorted(map(tuple, expected), key=repr),
                          f"{label}: rows differ from {sql}")
        print(f"  ✅ {label}: {len(got)} rows, as {sql}")

    def assert_rows_by_key(self, got, sql, key, label):
        """`got` holds the columns and rows of `sql`, rows matched on `key`, floats within 1e-6."""
        expected = self.spark.sql(sql).collect()
        got_by_key = {row[key]: row for row in got}
        expected_by_key = {row[key]: row for row in expected}
        self.assert_equal(len(got_by_key), len(got), f"{label}: one row per {key}")
        self.assert_equal(sorted(got_by_key), sorted(expected_by_key), f"{label}: the {key} values of {sql}")
        for value, want in expected_by_key.items():
            have = got_by_key[value]
            self.assert_equal(list(have.asDict()), list(want.asDict()), f"{label}: columns")
            for column in want.asDict():
                if not self.close(have[column], want[column]):
                    raise AssertionError(f"{label}: {column} of {key}={value!r} is {have[column]!r}, "
                                         f"{sql} answers {want[column]!r}")
        print(f"  ✅ {label}: {len(got)} rows, as {sql}")

    # --- setup / teardown ---

    def setup(self):
        """One session; both mirrors and a local table made through spark.sql(), with their row counts."""
        self.spark = SparkSession.builder.remote(self.url).getOrCreate()
        self.campaigns = self.spark.table(CAMPAIGNS)
        self.products = self.spark.table(PRODUCTS)
        self.spark.sql(f"CREATE DATABASE {self.db}")
        self.db_created = True
        self.spark.sql(f"CREATE TABLE {self.local_table} (id bigint, grp string, amount bigint)")
        self.table_created = True
        values = ", ".join(f"({i}, '{grp}', {amount})" for i, grp, amount in LOCAL_ROWS)
        self.spark.sql(f"INSERT INTO {self.local_table} (id, grp, amount) VALUES {values}")
        self.local = self.spark.table(self.local_table)
        self.totals = {CAMPAIGNS: len(self.campaigns.collect()), PRODUCTS: len(self.products.collect()),
                       self.local_table: len(self.local.collect())}
        print(f"  rows: {self.totals}")

    def cleanup(self):
        if self.spark is None:
            return
        try:
            if self.table_created:
                self.spark.sql(f"DROP TABLE {self.local_table}")
            if self.db_created:
                self.spark.sql(f"DROP DATABASE {self.db}")
        finally:
            self.spark.stop()

    # --- cases ---

    def test_column_filter_matches_sql(self):
        """A structured Column filter and a string filter keep the rows of the same SQL WHERE."""
        c, p = self.campaigns, self.products
        self.assert_filter(c.filter(c.budget > 50000), CAMPAIGNS, "campaign_id",
                           "budget > 50000", "MySQL filter(df.budget > 50000)")
        self.assert_filter(c.filter("budget > 50000"), CAMPAIGNS, "campaign_id",
                           "budget > 50000", "MySQL filter('budget > 50000')")
        self.assert_filter(p.filter(p.price > 250), PRODUCTS, "product_id",
                           "price > 250", "PostgreSQL filter(df.price > 250)")
        self.assert_filter(p.filter("price > 250"), PRODUCTS, "product_id",
                           "price > 250", "PostgreSQL filter('price > 250')")

    def test_literal_on_the_left(self):
        """A comparison with the literal on the left keeps the rows of its mirrored form."""
        self.assert_filter(self.campaigns.filter(F.lit(50000) < F.col("budget")), CAMPAIGNS, "campaign_id",
                           "budget > 50000", "MySQL filter(lit(50000) < col('budget'))")
        self.assert_filter(self.products.filter(F.lit(250) >= F.col("price")), PRODUCTS, "product_id",
                           "price <= 250", "PostgreSQL filter(lit(250) >= col('price'))")

    def test_and_or_combinations(self):
        """&, | and a nested mix of Column predicates keep the rows of their SQL AND / OR."""
        budget, price = F.col("budget"), F.col("price")
        self.assert_filter(self.campaigns.filter((budget > 20000) & (budget < 80000)), CAMPAIGNS, "campaign_id",
                           "budget > 20000 AND budget < 80000", "MySQL filter(a & b)")
        self.assert_filter(self.campaigns.filter((budget < 20000) | (budget > 80000)), CAMPAIGNS, "campaign_id",
                           "budget < 20000 OR budget > 80000", "MySQL filter(a | b)")
        self.assert_filter(self.products.filter(((price < 100) | (price > 400)) & (F.col("campaign_id") <= 25)),
                           PRODUCTS, "product_id", "(price < 100 OR price > 400) AND campaign_id <= 25",
                           "PostgreSQL filter((a | b) & c)")

    def test_chained_filters_are_conjunction(self):
        """filter(a).filter(b) keeps the rows of filter(a & b) and of SQL `a AND b`."""
        a, b = F.col("budget") > 20000, F.col("campaign_id") <= 25
        chained = self.campaigns.filter(a).filter(b)
        self.assert_filter(chained, CAMPAIGNS, "campaign_id",
                           "budget > 20000 AND campaign_id <= 25", "MySQL filter(a).filter(b)")
        self.assert_equal(self.keys(chained, "campaign_id"),
                          self.keys(self.campaigns.filter(a & b), "campaign_id"),
                          "filter(a).filter(b) vs filter(a & b)")
        print("  ✅ filter(a).filter(b) keeps the rows of filter(a & b)")

    def test_group_by_aggregates_match_sql(self):
        """groupBy().agg(count, sum, avg) returns the groups and values of SQL GROUP BY."""
        got = self.products.groupBy("campaign_id").agg(
            F.count("*").alias("n"), F.sum("price").alias("total"), F.avg("price").alias("avg_price")).collect()
        if len(got) < 2:
            raise AssertionError(f"groupBy('campaign_id') gave {len(got)} group(s); the case needs several")
        self.assert_rows_by_key(got, f"SELECT campaign_id, COUNT(*) AS n, SUM(price) AS total, "
                                     f"AVG(price) AS avg_price FROM {PRODUCTS} GROUP BY campaign_id",
                                "campaign_id", "PostgreSQL groupBy('campaign_id').agg(count, sum, avg)")

    def test_avg_keeps_the_fraction(self):
        """avg over the local table's BIGINT column answers the DOUBLE mean of the rows, fraction kept."""
        # The rows are the reference: the engine's own AVG over BIGINT, which the SQL
        # path runs, drops the fraction.
        amounts = [amount for _, _, amount in LOCAL_ROWS]
        got = self.local.agg(F.avg("amount").alias("mean")).collect()[0]["mean"]
        if not isinstance(got, float):
            raise AssertionError(f"local agg(avg('amount')) answered {got!r}, not a DOUBLE")
        self.assert_close(got, sum(amounts) / len(amounts), "local agg(avg('amount'))")
        by_group = {}
        for _, grp, amount in LOCAL_ROWS:
            by_group.setdefault(grp, []).append(amount)
        rows = self.local.groupBy("grp").agg(F.avg("amount").alias("mean")).collect()
        self.assert_equal(sorted(row["grp"] for row in rows), sorted(by_group), "local groupBy('grp') groups")
        for row in rows:
            values = by_group[row["grp"]]
            self.assert_close(row["mean"], sum(values) / len(values), f"avg(amount) of grp={row['grp']!r}")
        print(f"  ✅ local avg(amount) = {got!r}, and per grp the mean of its rows")

    def test_order_by_desc_limit(self):
        """orderBy(desc('price')).limit(5) returns the five largest prices, largest first."""
        got = [row["price"] for row in self.products.orderBy(F.desc("price")).limit(5).collect()]
        prices = sorted((row["price"] for row in self.spark.sql(f"SELECT price FROM {PRODUCTS}").collect()),
                        reverse=True)
        self.assert_equal(got, prices[:5], "orderBy(desc('price')).limit(5)")
        print(f"  ✅ orderBy(desc('price')).limit(5): {got}")

    def test_select_then_count(self):
        """select(...).count() is the row count SQL COUNT(*) reports for the table."""
        for df, table, columns in ((self.campaigns, CAMPAIGNS, ["campaign_id", "budget"]),
                                   (self.products, PRODUCTS, ["product_id", "price"]),
                                   (self.local, self.local_table, ["grp", "amount"])):
            expected = self.spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()[0]["n"]
            self.assert_equal(int(df.select(*columns).count()), int(expected), f"{table}: select{columns}.count()")
            print(f"  ✅ {table}: select{columns}.count() = {expected}, as SQL COUNT(*)")

    def test_distinct_then_count(self):
        """distinct().count() and select(c).distinct().count() count the rows of SELECT DISTINCT."""
        for df, sql, label in (
                (self.local.distinct(), f"SELECT DISTINCT id, grp, amount FROM {self.local_table}",
                 "local distinct()"),
                (self.campaigns.distinct(), f"SELECT DISTINCT * FROM {CAMPAIGNS}", "MySQL distinct()"),
                (self.products.select("campaign_id").distinct(), f"SELECT DISTINCT campaign_id FROM {PRODUCTS}",
                 "PostgreSQL select('campaign_id').distinct()")):
            expected = len(self.spark.sql(sql).collect())
            self.assert_equal(int(df.count()), expected, f"{label}.count() vs {sql}")
            print(f"  ✅ {label}.count() = {expected}, as {sql}")
        self.assert_equal(int(self.local.distinct().count()), len(set(LOCAL_ROWS)),
                          "local distinct().count() must drop the duplicated row")

    def test_limit_then_first(self):
        """orderBy(k).limit(5).first() is the row of SQL ORDER BY k LIMIT 1, and limit(5).first() a row of the table."""
        for df, table, key in ((self.campaigns, CAMPAIGNS, "campaign_id"), (self.products, PRODUCTS, "product_id"),
                               (self.local, self.local_table, "id")):
            first = df.orderBy(key).limit(5).first()
            expected = self.spark.sql(f"SELECT * FROM {table} ORDER BY {key} LIMIT 1").collect()[0]
            self.assert_equal(first.asDict(), expected.asDict(), f"{table}: orderBy({key}).limit(5).first()")
            some = df.limit(5).first()
            if some is None or tuple(some) not in {tuple(row) for row in df.collect()}:
                raise AssertionError(f"{table}: limit(5).first() = {some!r} is not a row of the table")
            print(f"  ✅ {table}: orderBy({key}).limit(5).first() = {first[key]!r}; limit(5).first() is a table row")

    def test_group_by_then_select(self):
        """groupBy().agg(...).select(...) returns the columns of the same SQL GROUP BY."""
        self.assert_rows_by_key(
            self.products.groupBy("campaign_id").agg(F.sum("price").alias("total"), F.count("*").alias("n"))
                .select("campaign_id", "total").collect(),
            f"SELECT campaign_id, SUM(price) AS total FROM {PRODUCTS} GROUP BY campaign_id",
            "campaign_id", "PostgreSQL groupBy('campaign_id').agg(sum, count).select(campaign_id, total)")
        self.assert_rows_by_key(
            self.local.groupBy("grp").agg(F.sum("amount").alias("total")).select("total", "grp").collect(),
            f"SELECT SUM(amount) AS total, grp FROM {self.local_table} GROUP BY grp",
            "grp", "local groupBy('grp').agg(sum).select(total, grp)")
        self.assert_rows(
            self.campaigns.groupBy().agg(F.count("*").alias("n"), F.max("budget").alias("top"))
                .select("top").collect(),
            f"SELECT MAX(budget) AS top FROM {CAMPAIGNS}",
            "MySQL groupBy().agg(count, max).select(top)")

    def test_group_by_then_filter(self):
        """groupBy().agg(sum(...).alias("s")).filter(s > x) keeps the groups SQL HAVING keeps."""
        # Sums of two-decimal prices never equal 800.005: no group sits on the threshold.
        got = self.products.groupBy("campaign_id").agg(F.sum("price").alias("s")).filter(F.col("s") > 800.005).collect()
        groups = len(self.spark.sql(f"SELECT DISTINCT campaign_id FROM {PRODUCTS}").collect())
        if not 0 < len(got) < groups:
            raise AssertionError(f"filter(s > 800.005) kept {len(got)} of {groups} groups; it does not discriminate")
        self.assert_rows_by_key(
            got, f"SELECT campaign_id, SUM(price) AS s FROM {PRODUCTS} GROUP BY campaign_id HAVING SUM(price) > 800.005",
            "campaign_id", "PostgreSQL groupBy('campaign_id').agg(sum.alias('s')).filter(s > 800.005)")
        self.assert_rows_by_key(
            self.local.groupBy("grp").agg(F.sum("amount").alias("s")).filter(F.col("s") > 50).collect(),
            f"SELECT grp, SUM(amount) AS s FROM {self.local_table} GROUP BY grp HAVING SUM(amount) > 50",
            "grp", "local groupBy('grp').agg(sum.alias('s')).filter(s > 50)")
        # A global aggregate: its one row passes s > 0 and fails s < 0.
        total = self.campaigns.groupBy().agg(F.sum("budget").alias("s"))
        kept = total.filter(F.col("s") > 0).collect()
        expected = self.spark.sql(f"SELECT SUM(budget) AS s FROM {CAMPAIGNS}").collect()
        self.assert_equal([list(row.asDict()) for row in kept], [["s"]],
                          "MySQL groupBy().agg(sum.alias('s')).filter(s > 0): one row, column s")
        self.assert_close(kept[0]["s"], expected[0]["s"], "MySQL groupBy().agg(sum.alias('s')).filter(s > 0)")
        self.assert_equal(total.filter(F.col("s") < 0).collect(), [],
                          "MySQL groupBy().agg(sum.alias('s')).filter(s < 0) must keep no row")
        print(f"  ✅ MySQL groupBy().agg(sum.alias('s')).filter(s > 0 / s < 0): one row / none, "
              f"s = SQL SUM(budget) = {expected[0]['s']}")

    def test_select_then_select(self):
        """select(...).select(...) returns the columns and rows of the same SQL projection."""
        self.assert_rows_by_key(
            self.campaigns.select("campaign_id", "campaign_name", (F.col("budget") * 2).alias("double_budget"))
                .select("double_budget", "campaign_id").collect(),
            f"SELECT budget * 2 AS double_budget, campaign_id FROM {CAMPAIGNS}",
            "campaign_id", "MySQL select(..., budget * 2).select(double_budget, campaign_id)")
        self.assert_rows_by_key(
            self.products.select("product_id", "product_name", "price").select("price", "product_id").collect(),
            f"SELECT price, product_id FROM {PRODUCTS}",
            "product_id", "PostgreSQL select(product_id, product_name, price).select(price, product_id)")
        self.assert_rows(
            self.local.select("id", "grp", "amount").select("grp", "amount").collect(),
            f"SELECT grp, amount FROM {self.local_table}",
            "local select(id, grp, amount).select(grp, amount)")

    def test_sql_limit_then_limit(self):
        """spark.sql("... LIMIT 10").limit(3) is the first three rows of the statement."""
        for table, key, value in ((CAMPAIGNS, "campaign_id", "budget"), (PRODUCTS, "product_id", "price"),
                                  (self.local_table, "id", "amount")):
            ordered = self.spark.sql(f"SELECT {key}, {value} FROM {table} ORDER BY {key} LIMIT 10").limit(3).collect()
            expected = self.spark.sql(f"SELECT {key}, {value} FROM {table} ORDER BY {key} LIMIT 3").collect()
            self.assert_equal([tuple(row) for row in ordered], [tuple(row) for row in expected],
                              f"{table}: spark.sql('... ORDER BY {key} LIMIT 10').limit(3)")
            unordered = self.spark.sql(f"SELECT {key}, {value} FROM {table} LIMIT 10").limit(3).collect()
            table_rows = {tuple(row) for row in self.spark.sql(f"SELECT {key}, {value} FROM {table}").collect()}
            self.assert_equal(len(unordered), 3, f"{table}: spark.sql('... LIMIT 10').limit(3) row count")
            if not all(tuple(row) in table_rows for row in unordered):
                raise AssertionError(f"{table}: spark.sql('... LIMIT 10').limit(3) = {unordered!r} holds a row "
                                     f"the table does not")
            print(f"  ✅ {table}: spark.sql('... LIMIT 10').limit(3) = the first three rows")

    def test_union_by_name_reordered_rejected(self):
        """unionByName over the same columns in another order is refused, naming unionByName, instead of returning rows."""
        a = self.campaigns.select("campaign_id", "budget")
        b = self.campaigns.select("budget", "campaign_id")
        try:
            rows = a.unionByName(b).collect()
        except PySparkException as e:
            message = str(e)
            if "unionbyname" not in message.lower():
                raise AssertionError(f"unionByName failed, but without naming unionByName: {message}")
            print(f"  ✅ unionByName over reordered columns refused: {message.strip().splitlines()[0][:200]}")
            return
        raise AssertionError(f"unionByName over reordered columns returned {len(rows)} rows instead of an error")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_column_filter_matches_sql()
            self.test_literal_on_the_left()
            self.test_and_or_combinations()
            self.test_chained_filters_are_conjunction()
            self.test_group_by_aggregates_match_sql()
            self.test_avg_keeps_the_fraction()
            self.test_order_by_desc_limit()
            self.test_select_then_count()
            self.test_distinct_then_count()
            self.test_limit_then_first()
            self.test_group_by_then_select()
            self.test_group_by_then_filter()
            self.test_select_then_select()
            self.test_sql_limit_then_limit()
            self.test_union_by_name_reordered_rejected()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Spark Connect E2E tests - DataFrame operations")
    parser.add_argument("--local", action="store_true",
                        help="Use the local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()
    try:
        SparkDataFrameOpsTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Spark Connect DataFrame operations\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Spark Connect DataFrame operations\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
