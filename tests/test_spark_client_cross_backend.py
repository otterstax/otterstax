# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
E2E test: cross-backend JOINs driven through the Spark Connect frontend.

Builds on the cross-backend patterns in test_cross_backend_queries.py but issues
the queries via PySpark (Spark Connect, port 15002) instead of the MySQL wire
protocol. Exercises:

  * Path A — a federated cross-backend JOIN as a single spark.sql() string
    (MySQL `campaigns` JOIN PostgreSQL `products`).
  * Path B — two independent DataFrames (one per backend) joined client-side
    via the DataFrame .join() API, proving OtterStax can surface remote tables
    as Spark DataFrames and let PySpark compose them.

Usage:
    python test_spark_client_cross_backend.py            # docker (host=test-otterstax)
    python test_spark_client_cross_backend.py --local    # local  (host=0.0.0.0)
"""

import sys
import argparse

from pyspark.sql import SparkSession

import config


def main(local=False):
    host = config.get_host(local)
    port = config.SPARK_CONNECT_PORT

    print(f"\n{'#'*70}")
    print(f"# Spark Connect Client - Cross-Backend JOIN Tests")
    print(f"{'#'*70}")
    print(f"Connecting to Spark Connect server at: sc://{host}:{port}")
    print(f"Targets: MySQL (campaigns) + PostgreSQL (products)")
    print(f"{'#'*70}\n")

    spark = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()
    print(f"✅ Spark session created (version: {spark.version})\n")

    print(f"\n{'='*70}")
    print("TEST SUITE: Spark Connect Cross-Backend JOINs")
    print(f"{'='*70}")
    print("Total tests: 4")
    print("  1. Path A — federated JOIN via spark.sql()")
    print("  2. Path A — federated JOIN + WHERE filter")
    print("  3. Path B — two DataFrames joined via DataFrame API")
    print("  4. Path A — federated JOIN + aggregation (GROUP BY)")
    print(f"{'='*70}")

    # Test 1: Path A — federated JOIN as a single SQL string
    print(f"\n{'='*70}")
    print("Running: Test 1 - Path A: campaigns JOIN products via spark.sql()")
    print(f"{'='*70}")
    df = spark.sql("""
        SELECT p.product_id, p.product_name, p.price, c.campaign_name
        FROM products.pgdb.public.products p
        JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
        LIMIT 10
    """)
    rows = df.collect()
    assert len(rows) > 0, "Cross-backend JOIN returned no rows"
    print(f"  ✅ Got {len(rows)} rows")
    for r in rows[:3]:
        print(f"    product_id={r['product_id']}, product={r['product_name']}, "
              f"price={r['price']}, campaign={r['campaign_name']}")

    # Test 2: Path A — federated JOIN + WHERE filter
    print(f"\n{'='*70}")
    print("Running: Test 2 - Path A: federated JOIN + WHERE (price > 100)")
    print(f"{'='*70}")
    df2 = spark.sql("""
        SELECT p.product_name, p.price, c.campaign_name, c.budget
        FROM products.pgdb.public.products p
        JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
        WHERE p.price > 100
        LIMIT 10
    """)
    rows2 = df2.collect()
    assert len(rows2) > 0, "JOIN with WHERE returned no rows"
    assert all(r["price"] > 100 for r in rows2), "price > 100 not respected"
    print(f"  ✅ Got {len(rows2)} rows, all price > 100")
    for r in rows2[:3]:
        print(f"    product={r['product_name']}, price={r['price']}, "
              f"campaign={r['campaign_name']}, budget={r['budget']}")

    # Test 3: Path B — two DataFrames joined client-side via the DataFrame API
    print(f"\n{'='*70}")
    print("Running: Test 3 - Path B: DataFrame.join() across backends")
    print(f"{'='*70}")
    campaigns_df = spark.sql("SELECT campaign_id, campaign_name, budget "
                             "FROM campaigns.db1.schema.campaigns")
    products_df = spark.sql("SELECT product_id, campaign_id, product_name, price "
                            "FROM products.pgdb.public.products")
    joined = (products_df.join(campaigns_df, on="campaign_id", how="inner")
              .select("product_name", "price", "campaign_name")
              .limit(10))
    rows3 = joined.collect()
    assert len(rows3) > 0, "DataFrame.join() returned no rows"
    print(f"  ✅ Got {len(rows3)} rows via DataFrame.join()")
    for r in rows3[:3]:
        print(f"    product={r['product_name']}, price={r['price']}, "
              f"campaign={r['campaign_name']}")

    # Test 4: Path A — federated JOIN + GROUP BY aggregation
    print(f"\n{'='*70}")
    print("Running: Test 4 - Path A: JOIN + GROUP BY aggregation")
    print(f"{'='*70}")
    df4 = spark.sql("""
        SELECT c.campaign_name,
               COUNT(p.product_id) AS product_count,
               AVG(p.price) AS avg_product_price
        FROM campaigns.db1.schema.campaigns c
        JOIN products.pgdb.public.products p ON c.campaign_id = p.campaign_id
        GROUP BY c.campaign_name
        ORDER BY product_count DESC
        LIMIT 10
    """)
    rows4 = df4.collect()
    assert len(rows4) > 0, "GROUP BY aggregation returned no rows"
    print(f"  ✅ Got {len(rows4)} aggregated rows")
    for r in rows4[:3]:
        avg_p = f"{r['avg_product_price']:.2f}" if r['avg_product_price'] is not None else "NULL"
        print(f"    campaign={r['campaign_name']}, products={r['product_count']}, "
              f"avg_price={avg_p}")

    spark.stop()


def main_test():
    parser = argparse.ArgumentParser(description='Spark Connect E2E tests - cross-backend JOINs')
    parser.add_argument('--local', action='store_true',
                        help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    try:
        main(local=args.local)
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Spark Connect Cross-Backend JOINs\033[0m")
        print("=" * 70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - Spark Connect Cross-Backend JOINs\033[0m")
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
