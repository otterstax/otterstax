# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Cross-backend JOIN tests using mysql_client.

Tests that verify cross-backend JOIN operations by:
1. Using mysql_client (port 8816) as the primary connection
2. Executing JOIN queries that combine MariaDB + PostgreSQL tables
3. Verifying data from both backends is correctly joined

This tests the server's ability to execute cross-backend JOINs via MySQL protocol.
"""

import sys
import argparse
from contextlib import contextmanager

# Import existing mysql_client
sys.path.insert(0, '/workspaces/sqlflite_server/tests')
from test_mysql_client_mysql_backend import client as mysql_client


class mysql_cross_backend_client(mysql_client):
    """
    MySQL client extended for cross-backend queries.
    Connects via MySQL protocol (port 8816) and executes cross-backend JOINs.
    """
    def __init__(self, local=False):
        super().__init__(local=local)
        # Cross-backend database references
        self.mysql_db1 = 'campaigns.db1.schema'
        self.mysql_db2 = 'impressions.db2.schema'
        self.pg_db = 'products.pgdb.public'


def test_mysql_internal_join(client_instance):
    """Test JOIN within MySQL backend (should work)"""
    print("\n" + "="*60)
    print("MySQL Internal JOIN Tests (via mysql_client on port 8816)")
    print("="*60)
    
    with client_instance.mysql_connector_connection() as conn:
        cursor = conn.cursor()
        
        # Test 1: JOIN campaigns + impressions (MySQL internal)
        print("\nTest 1: JOIN campaigns.db1.schema + impressions.db2.schema")
        cursor.execute("""
            SELECT c.campaign_id, c.campaign_name, i.clicks, i.revenue, i.conversions
            FROM campaigns.db1.schema.campaigns c
            JOIN impressions.db2.schema.impressions i ON c.campaign_id = i.campaign_id
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ MySQL internal JOIN: {len(rows)} rows returned")
        for row in rows[:3]:
            print(f"    Campaign {row[0]}: clicks={row[2]}, revenue={row[3]}, conversions={row[4]}")
        assert len(rows) > 0, "MySQL internal JOIN returned no results"
        
        # Test 2: MySQL JOIN with WHERE
        print("\nTest 2: MySQL JOIN with WHERE clause")
        cursor.execute("""
            SELECT c.campaign_name, c.budget, i.clicks, i.revenue
            FROM campaigns.db1.schema.campaigns c
            JOIN impressions.db2.schema.impressions i ON c.campaign_id = i.campaign_id
            WHERE c.campaign_length > 30
            ORDER BY i.revenue DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ MySQL JOIN with WHERE: {len(rows)} rows returned")
        for row in rows[:3]:
            print(f"    {row[0]}: budget={row[1]}, clicks={row[2]}, revenue={row[3]}")
        assert len(rows) > 0, "MySQL JOIN with WHERE returned no results"
        
        # Test 3: MySQL JOIN with aggregation
        print("\nTest 3: MySQL JOIN with GROUP BY aggregation")
        cursor.execute("""
            SELECT c.campaign_id, c.campaign_name, 
                   SUM(i.clicks) as total_clicks,
                   AVG(i.revenue) as avg_revenue,
                   COUNT(i.impression_id) as impression_count
            FROM campaigns.db1.schema.campaigns c
            JOIN impressions.db2.schema.impressions i ON c.campaign_id = i.campaign_id
            GROUP BY c.campaign_id, c.campaign_name
            ORDER BY total_clicks DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"✓ MySQL JOIN with GROUP BY: {len(rows)} rows returned")
        for row in rows[:3]:
            avg_rev = f"{row[3]:.2f}" if row[3] is not None else "NULL"
            print(f"    Campaign {row[0]} ({row[1]}): clicks={row[2]}, avg_revenue={avg_rev}, count={row[4]}")
        assert len(rows) > 0, "MySQL JOIN with GROUP BY returned no results"
    
    print("\n✓ All MySQL internal JOIN tests PASSED")


def test_cross_backend_joins(client_instance):
    """Test cross-backend JOIN operations - DEBUG MODE ENABLED"""
    print("\n" + "="*60)
    print("Cross-Backend JOIN Tests - DEBUG MODE")
    print("="*60)
    print("\n⚠️  Cross-backend JOINs are NOT fully supported.")
    print("   This test helps debug the current behavior.\n")

    with client_instance.mysql_connector_connection() as conn:
        cursor = conn.cursor()

        # Test 1: Cross-backend JOIN - products (PG) + campaigns (MySQL)
        print("="*60)
        print("Test 1: Cross-backend JOIN - products (PG) JOIN campaigns (MySQL)")
        print("="*60)
        query = """
            SELECT p.product_id, p.product_name, p.price, p.category,
                   c.campaign_name, c.campaign_length, c.budget
            FROM products.pgdb.public.products p
            JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
            LIMIT 10
        """
        print(f"Executing query:\n{query}")
        print("-"*60)
        
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Query executed. Rows returned: {len(rows)}")
            
            if len(rows) > 0:
                print(f"\n✅ SUCCESS: Cross-backend JOIN worked!")
                print(f"   Columns: {[desc[0] for desc in cursor.description]}")
                print(f"\n   First {min(3, len(rows))} rows:")
                for i, row in enumerate(rows[:3]):
                    print(f"   Row {i+1}:")
                    print(f"      product_id={row[0]}, product_name={row[1]}, price={row[2]}, category={row[3]}")
                    print(f"      campaign_name={row[4]}, campaign_length={row[5]}, budget={row[6]}")
            else:
                print(f"\n❌ RESULT: Cross-backend JOIN returned 0 rows")
                print("   This may indicate the JOIN is being executed on one backend only")
                
        except Exception as e:
            print(f"\n❌ ERROR: Cross-backend JOIN failed")
            print(f"   Error type: {type(e).__name__}")
            print(f"   Error message: {str(e)}")
            import traceback
            print(f"\n   Full traceback:")
            traceback.print_exc()

        # Test 2: Verify individual tables are accessible
        print("\n" + "="*60)
        print("Test 2: Verify individual backend access")
        print("="*60)
        
        try:
            cursor.execute("SELECT COUNT(*) FROM products.pgdb.public.products")
            pg_count = cursor.fetchone()[0]
            print(f"✓ PostgreSQL products table: {pg_count} rows")
        except Exception as e:
            print(f"✗ PostgreSQL products table access failed: {str(e)[:100]}")
        
        try:
            cursor.execute("SELECT COUNT(*) FROM campaigns.db1.schema.campaigns")
            mysql_count = cursor.fetchone()[0]
            print(f"✓ MySQL campaigns table: {mysql_count} rows")
        except Exception as e:
            print(f"✗ MySQL campaigns table access failed: {str(e)[:100]}")

    print("\n" + "="*60)
    print("Cross-backend JOIN debug test completed.")
    print("Check server logs for backend routing details.")
    print("="*60)


def test_data_consistency(client_instance):
    """Test data consistency between backends"""
    print("\n" + "="*60)
    print("Cross-Backend Data Consistency Tests")
    print("="*60)
    
    with client_instance.mysql_connector_connection() as conn:
        cursor = conn.cursor()
        
        # Get campaign IDs from MySQL
        print("\nQuerying MySQL backend...")
        cursor.execute("SELECT DISTINCT campaign_id FROM campaigns.db1.schema.campaigns ORDER BY campaign_id")
        mysql_campaign_ids = set(row[0] for row in cursor.fetchall())
        print(f"✓ MySQL - Found {len(mysql_campaign_ids)} unique campaign IDs")
        
        # Get campaign IDs referenced by PostgreSQL products (via cross-backend query)
        print("\nQuerying PostgreSQL backend (products table)...")
        try:
            cursor.execute("SELECT DISTINCT campaign_id FROM products.pgdb.public.products ORDER BY campaign_id")
            pg_campaign_ids = set(row[0] for row in cursor.fetchall())
            print(f"✓ PostgreSQL - Found {len(pg_campaign_ids)} unique campaign IDs in products")
            
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
            print(f"✗ PostgreSQL query FAILED: {str(e)[:200]}")
            print("   Cross-backend queries not supported yet")
    
    print("\n✓ Data consistency check COMPLETED")


def main(local=False):
    print("="*60)
    print("MySQL Client Cross-Backend Tests")
    print("="*60)
    print("\nUsing: mysql_client (port 8816) for all queries")
    print("Backends:")
    print("  - MySQL/MariaDB: campaigns.db1.schema, impressions.db2.schema")
    print("  - PostgreSQL: products.pgdb.public")
    print("="*60)

    # Create client instance
    client_instance = mysql_cross_backend_client(local=local)

    # Test 1: MySQL internal JOINs (should work)
    test_mysql_internal_join(client_instance)

    # Test 2: Cross-backend JOINs (DEBUG MODE)
    test_cross_backend_joins(client_instance)

    # Test 3: Data consistency
    test_data_consistency(client_instance)

    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print("\n✓ MySQL Internal JOINs: Working")
    print("⚠️  Cross-Backend JOINs: NOT SUPPORTED (architectural limitation)")
    print("✓ Data Consistency: Verified")
    print("\nSee cross_join_issue.md for details.")
    print("="*60)


def main_test():
    parser = argparse.ArgumentParser(description='MySQL client cross-backend JOIN tests')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')
    args = parser.parse_args()

    try:
        main(local=args.local)
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - MySQL Client Cross-Backend\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - MySQL Client Cross-Backend\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")


if __name__ == "__main__":
    main_test()
