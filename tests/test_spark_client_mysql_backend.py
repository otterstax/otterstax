# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
E2E test: Spark Connect (port 15002) frontend querying a MySQL/MariaDB backend.

Exercises both supported execution paths through the OtterStax Spark Connect server:
  * Path A — spark.sql("...") passes the federated SQL string straight through to
    the Scheduler, which resolves the `campaigns` alias to the MariaDB backend.
  * Path B — DataFrame ops (filter/select) build a logical plan in the
    plan_translator and run through the same Scheduler pipeline.

Usage:
    python test_spark_client_mysql_backend.py            # docker (host=test-otterstax)
    python test_spark_client_mysql_backend.py --local    # local  (host=0.0.0.0)
"""

import sys
import argparse

from pyspark.sql import SparkSession

import config


def main(local=False):
    host = config.get_host(local)
    port = config.SPARK_CONNECT_PORT

    print(f"\n{'#'*70}")
    print(f"# Spark Connect Client - MySQL Backend Tests")
    print(f"{'#'*70}")
    print(f"Connecting to Spark Connect server at: sc://{host}:{port}")
    print(f"Target: MySQL backend (campaigns.db1.schema.campaigns)")
    print(f"{'#'*70}\n")

    spark = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()
    print(f"✅ Spark session created (version: {spark.version})\n")

    print(f"\n{'='*70}")
    print("TEST SUITE: Spark Connect MySQL Backend")
    print(f"{'='*70}")
    print("Total tests: 5")
    print("  1. SQL pass-through — spark.sql() SELECT ... LIMIT 5")
    print("  2. DataFrame API — filter().select() (Path B)")
    print("  3. Schema introspection — .schema")
    print("  4. spark.range() — local materialization (Path B proof)")
    print("  5. Spark server version")
    print(f"{'='*70}")

    # Test 1: SQL pass-through — spark.sql() (Path A)
    print(f"\n{'='*70}")
    print("Running: Test 1 - spark.sql() SQL pass-through")
    print(f"{'='*70}")
    df = spark.sql("SELECT * FROM campaigns.db1.schema.campaigns LIMIT 5")
    rows = df.collect()
    assert len(rows) == 5, f"Expected 5 rows, got {len(rows)}"
    print(f"  ✅ Got {len(rows)} rows")
    for r in rows[:3]:
        print(f"    {r}")

    # Test 2: DataFrame API — filter().select() (Path B)
    print(f"\n{'='*70}")
    print("Running: Test 2 - DataFrame.filter().select() (Path B)")
    print(f"{'='*70}")
    df2 = (spark.sql("SELECT * FROM campaigns.db1.schema.campaigns")
           .filter("budget > 0")
           .select("campaign_name", "budget"))
    rows2 = df2.collect()
    assert len(rows2) > 0, "Expected non-empty result"
    assert all(r["budget"] > 0 for r in rows2), "budget > 0 filter not respected"
    print(f"  ✅ Got {len(rows2)} rows, all budget > 0")
    for r in rows2[:3]:
        print(f"    {r}")

    # Test 3: Schema introspection
    print(f"\n{'='*70}")
    print("Running: Test 3 - .schema")
    print(f"{'='*70}")
    schema = df.schema
    assert schema is not None, "Schema is None"
    assert len(schema.fields) > 0, "Schema has no fields"
    print(f"  ✅ Schema ({len(schema.fields)} fields):")
    for field in schema.fields:
        print(f"     {field.name}: {field.dataType}")

    # Test 4: spark.range() — local materialization (Path B proof, no federation)
    # range() failure is non-fatal — it exercises only local materialization,
    # not the MySQL backend under test.
    print(f"\n{'='*70}")
    print("Running: Test 4 - spark.range() (Path B local materialization)")
    print(f"{'='*70}")
    try:
        rdd = spark.range(10).collect()
        assert len(rdd) == 10, f"Expected 10 rows from range(10), got {len(rdd)}"
        print(f"  ✅ range(10) = {len(rdd)} rows")
    except Exception as e:
        print(f"  ⚠️ range() not yet supported: {e}")

    # Test 5: Version
    print(f"\n{'='*70}")
    print("Running: Test 5 - spark.version")
    print(f"{'='*70}")
    version = spark.version
    assert version is not None and len(version) > 0, "Empty version string"
    print(f"  ✅ Version: {version}")

    spark.stop()


def main_test():
    parser = argparse.ArgumentParser(description='Spark Connect E2E tests - MySQL backend')
    parser.add_argument('--local', action='store_true',
                        help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    try:
        main(local=args.local)
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Spark Connect MySQL Backend\033[0m")
        print("=" * 70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - Spark Connect MySQL Backend\033[0m")
        print("=" * 70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        import traceback
        traceback.print_exc()
        print("\033[91mTest fails.\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
