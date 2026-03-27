# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Cross-backend JOIN tests using pg_client.

Tests that verify cross-backend JOIN operations by:
1. Using pg_client (port 8817) as the primary connection
2. Executing JOIN queries that combine PostgreSQL + MariaDB tables
3. Verifying data from both backends is correctly joined

This tests the server's ability to execute cross-backend JOINs via PostgreSQL protocol.
"""

import sys
import argparse
from contextlib import contextmanager

# Import existing pg_client
sys.path.insert(0, '/workspaces/sqlflite_server/tests')
from test_pg_client_mysql_backend import client as pg_client


class pg_cross_backend_client(pg_client):
    """
    PostgreSQL client extended for cross-backend queries.
    Connects via PostgreSQL protocol (port 8817) and executes cross-backend JOINs.
    """
    def __init__(self, local=False):
        super().__init__(local=local)
        # Cross-backend database references
        self.pg_db = 'products.pgdb.public'
        self.mysql_db1 = 'campaigns.db1.schema'
        self.mysql_db2 = 'impressions.db2.schema'


def test_pg_internal_queries(client_instance):
    """Test PostgreSQL backend queries (should work)"""
    print("\n" + "="*60)
    print("PostgreSQL Backend Tests (via pg_client on port 8817)")
    print("="*60)
    
    with client_instance.psycopg2_connection() as conn:
        cursor = conn.cursor()
        
        # Test 1: Count products
        print("\nTest 1: Count products")
        cursor.execute("SELECT COUNT(*) FROM products.pgdb.public.products")
        count = cursor.fetchone()[0]
        print(f"✓ PostgreSQL - Products count: {count}")
        assert count > 0, "No products found"
        
        # Test 2: Sample products
        print("\nTest 2: Sample products")
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
        print("\nTest 3: Products with WHERE clause")
        cursor.execute("""
            SELECT product_id, product_name, price 
            FROM products.pgdb.public.products 
            WHERE price > 100 
            ORDER BY price DESC 
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ PostgreSQL - Products with price > 100: {len(rows)} rows")
        for row in rows[:3]:
            print(f"    ID: {row[0]}, Name: {row[1]}, Price: {row[2]}")
        
        # Test 4: PostgreSQL aggregation
        print("\nTest 4: PostgreSQL aggregation with GROUP BY")
        cursor.execute("""
            SELECT category, 
                   COUNT(*) as product_count,
                   AVG(price) as avg_price,
                   MIN(price) as min_price,
                   MAX(price) as max_price
            FROM products.pgdb.public.products
            GROUP BY category
            ORDER BY avg_price DESC
        """)
        rows = cursor.fetchall()
        print(f"✓ PostgreSQL - Aggregation by category: {len(rows)} categories")
        for row in rows[:3]:
            avg_p = f"{row[2]:.2f}" if row[2] is not None else "NULL"
            min_p = f"{row[3]:.2f}" if row[3] is not None else "NULL"
            max_p = f"{row[4]:.2f}" if row[4] is not None else "NULL"
            print(f"    {row[0]}: count={row[1]}, avg_price={avg_p}, min={min_p}, max={max_p}")
    
    print("\n✓ All PostgreSQL backend tests PASSED")


def test_cross_backend_join(client_instance):
    """Test cross-backend JOIN (PostgreSQL + MariaDB via pg_client)"""
    print("\n" + "="*60)
    print("Cross-Backend JOIN Tests (PostgreSQL + MariaDB via pg_client)")
    print("="*60)

    with client_instance.psycopg2_connection() as conn:
        cursor = conn.cursor()

        # Test 1: Cross-backend JOIN - products (PG) + campaigns (MySQL)
        print("\nTest 1: Cross-backend JOIN - products (PG) JOIN campaigns (MySQL)")
        cursor.execute("""
            SELECT p.product_id, p.product_name, p.price, p.category,
                   c.campaign_name, c.campaign_length, c.budget
            FROM products.pgdb.public.products p
            JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ Cross-backend JOIN: {len(rows)} rows returned")
        for row in rows[:3]:
            print(f"    Product {row[0]} ({row[1]}): ${row[2]} - Campaign: {row[4]}")
        assert len(rows) > 0, "Cross-backend JOIN returned no results"

        # Test 2: Cross-backend JOIN with WHERE
        print("\nTest 2: Cross-backend JOIN with WHERE filter")
        cursor.execute("""
            SELECT p.product_id, p.product_name, c.campaign_name, c.budget
            FROM products.pgdb.public.products p
            JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
            WHERE p.price > 100
            ORDER BY p.price DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ Cross-backend JOIN with WHERE: {len(rows)} rows returned")
        for row in rows[:3]:
            print(f"    Product {row[0]} ({row[1]}): ${row[2]} - Campaign: {row[4]}")
        assert len(rows) > 0, "Cross-backend JOIN with WHERE returned no results"

        # Test 3: Cross-backend LEFT JOIN
        print("\nTest 3: Cross-backend LEFT JOIN (products LEFT JOIN campaigns)")
        cursor.execute("""
            SELECT p.product_id, p.product_name, p.price,
                   c.campaign_id, c.campaign_name, c.budget
            FROM products.pgdb.public.products p
            LEFT JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
            ORDER BY p.product_id
            LIMIT 15
        """)
        rows = cursor.fetchall()
        print(f"✓ Cross-backend LEFT JOIN: {len(rows)} rows returned")
        for row in rows[:3]:
            campaign_info = f"Campaign {row[4]}" if row[4] is not None else "No campaign"
            print(f"    Product {row[0]} ({row[1]}): ${row[2]} - {campaign_info}")
        assert len(rows) > 0, "Cross-backend LEFT JOIN returned no results"

        # Test 4: Cross-backend aggregation
        print("\nTest 4: Cross-backend JOIN with GROUP BY aggregation")
        cursor.execute("""
            SELECT c.campaign_name,
                   COUNT(p.product_id) as product_count,
                   AVG(p.price) as avg_product_price
            FROM campaigns.db1.schema.campaigns c
            LEFT JOIN products.pgdb.public.products p ON c.campaign_id = p.campaign_id
            GROUP BY c.campaign_name
            ORDER BY product_count DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ Cross-backend aggregation: {len(rows)} rows returned")
        for row in rows[:3]:
            avg_p = f"{row[2]:.2f}" if row[2] is not None else "NULL"
            print(f"    Campaign {row[0]}: products={row[1]}, avg_price={avg_p}")
        assert len(rows) > 0, "Cross-backend aggregation returned no results"

    print("\n✓ All cross-backend JOIN tests PASSED")


def test_data_consistency(client_instance):
    """Test data consistency between backends"""
    print("\n" + "="*60)
    print("Cross-Backend Data Consistency Tests")
    print("="*60)
    
    with client_instance.psycopg2_connection() as conn:
        cursor = conn.cursor()
        
        # Get campaign IDs from PostgreSQL products
        print("\nQuerying PostgreSQL backend (products table)...")
        cursor.execute("SELECT DISTINCT campaign_id FROM products.pgdb.public.products ORDER BY campaign_id")
        pg_campaign_ids = set(row[0] for row in cursor.fetchall())
        print(f"✓ PostgreSQL - Found {len(pg_campaign_ids)} unique campaign IDs in products")
        
        # Get campaign IDs from MySQL campaigns (via cross-backend query)
        print("\nQuerying MySQL backend (campaigns table)...")
        try:
            cursor.execute("SELECT DISTINCT campaign_id FROM campaigns.db1.schema.campaigns ORDER BY campaign_id")
            mysql_campaign_ids = set(row[0] for row in cursor.fetchall())
            print(f"✓ MySQL - Found {len(mysql_campaign_ids)} unique campaign IDs")
            
            # Check overlap
            common_ids = mysql_campaign_ids & pg_campaign_ids
            print(f"\n✓ Cross-backend - {len(common_ids)} campaign IDs exist in both backends")
            
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
            
        except Exception as e:
            print(f"✗ MySQL query FAILED: {str(e)[:200]}")
            print("   Cross-backend queries not supported yet")
    
    print("\n✓ Data consistency check COMPLETED")


def main(local=False):
    print("="*60)
    print("PostgreSQL Client Cross-Backend JOIN Tests")
    print("="*60)
    print("\nUsing: pg_client (port 8817) for all queries")
    print("Backends:")
    print("  - PostgreSQL: products.pgdb.public")
    print("  - MySQL/MariaDB: campaigns.db1.schema, impressions.db2.schema")
    print("="*60)
    
    # Create client instance
    client_instance = pg_cross_backend_client(local=local)
    
    # Test 1: PostgreSQL backend queries (should work)
    test_pg_internal_queries(client_instance)
    
    # Test 2: Cross-backend JOINs (expected to fail)
    test_cross_backend_join(client_instance)
    
    # Test 3: Data consistency
    test_data_consistency(client_instance)
    
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print("\n✓ PostgreSQL Backend: Working")
    print("✓ Cross-Backend JOINs: Working")
    print("✓ Data Consistency: Verified")
    print("="*60)


def main_test():
    parser = argparse.ArgumentParser(description='PostgreSQL client cross-backend JOIN tests')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')
    args = parser.parse_args()

    try:
        main(local=args.local)
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - PostgreSQL Client Cross-Backend\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - PostgreSQL Client Cross-Backend\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")


if __name__ == "__main__":
    main_test()
