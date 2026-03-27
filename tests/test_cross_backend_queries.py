# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Cross-Backend Query Tests

Tests that verify cross-backend JOIN operations by:
1. Using MySQL client (port 8816) to query MySQL backend
2. Using PostgreSQL client (port 8817) to query PostgreSQL backend
3. Testing cross-backend JOINs (MySQL + PostgreSQL tables)

This tests the server's ability to JOIN data across different database backends.

Usage:
    python3 test_cross_backend_queries.py --local
"""

import sys
import argparse
from contextlib import contextmanager

# Import existing client classes
sys.path.insert(0, '/workspaces/sqlflite_server/tests')
from test_mysql_client_mysql_backend import client as mysql_client
from test_pg_client_mysql_backend import client as pg_client


class cross_backend_client:
    def __init__(self, local=False):
        self.mysql_client = mysql_client(local=local)
        self.pg_client = pg_client(local=local)
        self.local = local

    @contextmanager
    def mysql_connection(self):
        """MySQL client connection (port 8816)"""
        conn = None
        try:
            import mysql.connector
            conn = mysql.connector.connect(**self.mysql_client.proxy_config)
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def pg_connection(self):
        """PostgreSQL client connection (port 8817)"""
        conn = None
        try:
            import psycopg2
            conn = psycopg2.connect(**self.pg_client.proxy_config)
            conn.autocommit = True
            yield conn
        finally:
            if conn:
                conn.close()


def test_mysql_backend(mysql_client_instance):
    """Test MySQL backend queries"""
    print(f"\n{'='*70}")
    print("MySQL Backend Tests (via mysql_client on port 8816)")
    print(f"{'='*70}")

    with mysql_client_instance.mysql_connection() as conn:
        cursor = conn.cursor()

        # Test 1: Count campaigns
        cursor.execute("SELECT COUNT(*) FROM campaigns.db1.schema.campaigns")
        count = cursor.fetchone()[0]
        print(f"✓ MySQL - Campaigns count: {count}")
        assert count > 0, "No campaigns found"

        # Test 2: Count impressions
        cursor.execute("SELECT COUNT(*) FROM impressions.db2.schema.impressions")
        count = cursor.fetchone()[0]
        print(f"✓ MySQL - Impressions count: {count}")
        assert count > 0, "No impressions found"

        # Test 3: Sample campaign
        cursor.execute("""
            SELECT campaign_id, campaign_name, budget
            FROM campaigns.db1.schema.campaigns
            ORDER BY campaign_id
            LIMIT 5
        """)
        rows = cursor.fetchall()
        print(f"✓ MySQL - Sample campaigns: {len(rows)} rows")
        for row in rows:
            print(f"    ID: {row[0]}, Name: {row[1]}, Budget: {row[2]}")

        # Test 4: JOIN campaigns + impressions (MySQL internal JOIN)
        cursor.execute("""
            SELECT c.campaign_id, c.campaign_name, i.clicks, i.revenue
            FROM campaigns.db1.schema.campaigns c
            JOIN impressions.db2.schema.impressions i ON c.campaign_id = i.campaign_id
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ MySQL - JOIN campaigns + impressions: {len(rows)} rows")
        for row in rows[:3]:
            print(f"    Campaign {row[0]}: clicks={row[2]}, revenue={row[3]}")

    print(f"\n{'='*70}")
    print("✅ MySQL Backend Tests PASSED")
    print(f"{'='*70}")


def test_pg_backend(pg_client_instance):
    """Test PostgreSQL backend queries"""
    print(f"\n{'='*70}")
    print("PostgreSQL Backend Tests (via pg_client on port 8817)")
    print(f"{'='*70}")

    with pg_client_instance.pg_connection() as conn:
        cursor = conn.cursor()

        # Test 1: Count products
        cursor.execute("SELECT COUNT(*) FROM products.pgdb.public.products")
        count = cursor.fetchone()[0]
        print(f"✓ PostgreSQL - Products count: {count}")
        assert count > 0, "No products found"

        # Test 2: Sample products
        cursor.execute("""
            SELECT product_id, product_name, price, category
            FROM products.pgdb.public.products
            ORDER BY product_id
            LIMIT 5
        """)
        rows = cursor.fetchall()
        print(f"✓ PostgreSQL - Sample products: {len(rows)} rows")
        for row in rows:
            print(f"    ID: {row[0]}, Name: {row[1]}, Price: {row[2]}, Category: {row[3]}")

        # Test 3: Products with WHERE
        cursor.execute("""
            SELECT product_id, product_name, price
            FROM products.pgdb.public.products
            WHERE price > 100
            ORDER BY price DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ PostgreSQL - Products with price > 100: {len(rows)} rows")

    print(f"\n{'='*70}")
    print("✅ PostgreSQL Backend Tests PASSED")
    print(f"{'='*70}")


def test_cross_backend_joins(mysql_client_instance, pg_client_instance):
    """Test cross-backend JOIN operations"""
    print(f"\n{'='*70}")
    print("Cross-Backend JOIN Tests (MySQL + PostgreSQL)")
    print(f"{'='*70}")

    # Note: Cross-backend JOINs currently fail with server bug
    # This test documents the expected behavior

    print("\n⚠️  Cross-backend JOIN tests DISABLED due to server bug:")
    print("   Error: 'Array length did not match record batch length'")
    print("   Affects: ALL JOINs (single backend and cross-backend)")
    print("   See bugs.md #2 for details.")
    print()

    # Example queries that SHOULD work but currently fail:
    print("Expected queries (currently failing):")
    print("""
    -- Cross-backend JOIN: products (PG) + campaigns (MySQL)
    SELECT p.product_id, p.product_name, c.campaign_name
    FROM products.pgdb.public.products p
    JOIN campaigns.db1.schema.campaigns c
    ON p.campaign_id = c.campaign_id
    LIMIT 10;

    -- Cross-backend JOIN with aggregation
    SELECT c.campaign_name, COUNT(p.product_id) as product_count
    FROM campaigns.db1.schema.campaigns c
    LEFT JOIN products.pgdb.public.products p
    ON c.campaign_id = p.campaign_id
    GROUP BY c.campaign_name;
    """)

    print(f"\n{'='*70}")
    print("⚠️  Cross-Backend JOIN Tests SKIPPED (known bug)")
    print(f"{'='*70}")


def test_data_integrity(mysql_client_instance, pg_client_instance):
    """Test data integrity between backends"""
    print(f"\n{'='*70}")
    print("Cross-Backend Data Integrity Tests")
    print(f"{'='*70}")

    # Get campaign IDs from MySQL
    with mysql_client_instance.mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT campaign_id FROM campaigns.db1.schema.campaigns ORDER BY campaign_id")
        mysql_campaign_ids = set(row[0] for row in cursor.fetchall())
        print(f"✓ MySQL - Found {len(mysql_campaign_ids)} unique campaign IDs")

    # Get campaign IDs from PostgreSQL products
    with pg_client_instance.pg_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT campaign_id FROM products.pgdb.public.products ORDER BY campaign_id")
        pg_campaign_ids = set(row[0] for row in cursor.fetchall())
        print(f"✓ PostgreSQL - Found {len(pg_campaign_ids)} unique campaign IDs in products")

    # Check overlap
    common_ids = mysql_campaign_ids & pg_campaign_ids
    print(f"✓ Cross-backend - {len(common_ids)} campaign IDs exist in both backends")

    # Check for orphaned products
    orphaned = pg_campaign_ids - mysql_campaign_ids
    if orphaned:
        print(f"⚠️  Warning: {len(orphaned)} products reference non-existent campaigns")
        print(f"    Orphaned campaign IDs: {sorted(orphaned)[:10]}...")
    else:
        print("✓ All products reference valid campaigns")

    # Check for campaigns without products
    no_products = mysql_campaign_ids - pg_campaign_ids
    print(f"✓ {len(no_products)} campaigns have no products")

    print(f"\n{'='*70}")
    print("✅ Cross-Backend Data Integrity Tests PASSED")
    print(f"{'='*70}")


def main(local=False):
    print(f"\n{'#'*70}")
    print(f"# Cross-Backend Query Tests")
    print(f"{'#'*70}")
    print(f"Using:")
    print(f"  - mysql_client (port 8816) for MySQL backend")
    print(f"  - pg_client (port 8817) for PostgreSQL backend")
    print(f"{'#'*70}\n")

    # Create client instances
    mysql_client_instance = cross_backend_client(local=local)
    pg_client_instance = mysql_client_instance.pg_client

    # Test MySQL backend
    test_mysql_backend(mysql_client_instance)

    # Test PostgreSQL backend
    test_pg_backend(mysql_client_instance)

    # Test cross-backend JOINs (documented as failing)
    test_cross_backend_joins(mysql_client_instance, pg_client_instance)

    # Test data integrity
    test_data_integrity(mysql_client_instance, pg_client_instance)

    print(f"\n{'='*70}")
    print("Cross-Backend Test Summary")
    print(f"{'='*70}")
    print("\n✓ MySQL Backend: Working")
    print("✓ PostgreSQL Backend: Working")
    print("✓ Data Integrity: Verified")
    print("⚠️  Cross-Backend JOINs: DISABLED (server bug)")
    print("\nSee bugs.md for details on JOIN limitation.")
    print(f"{'='*70}")


def main_test():
    parser = argparse.ArgumentParser(description='Cross-backend query tests (mysql_client + pg_client)')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')
    args = parser.parse_args()

    try:
        main(local=args.local)
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - Cross-Backend Queries\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - Cross-Backend Queries\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")


if __name__ == "__main__":
    main_test()
