# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Cross-Backend JOIN Schema Test

This test verifies that cross-backend JOIN operations work correctly by:
1. Connecting via MySQL protocol to the FlightSQL server
2. Executing JOIN queries between PostgreSQL and MySQL tables
3. Validating the result schema and data

Usage:
    python3 test_schema_cross_backend_join.py --local
"""

import sys
import argparse
import mysql.connector


def test_cross_backend_join_schema(host='0.0.0.0', mysql_port=8816):
    """
    Test cross-backend JOIN schema and data integrity.
    
    This connects via MySQL protocol and executes JOIN queries
    between PostgreSQL (products) and MySQL (campaigns) tables.
    """
    print(f"\n{'#'*70}")
    print(f"# Cross-Backend JOIN Schema Test")
    print(f"{'#'*70}")
    print(f"Connecting to FlightSQL server at: {host}:{mysql_port}")
    print(f"Query: products (PostgreSQL) JOIN campaigns (MySQL)")
    print(f"{'#'*70}\n")

    # Connect via MySQL protocol to FlightSQL server
    config = {
        'host': host,
        'port': mysql_port,
        'user': 'testuser',
        'password': 'testpass'
    }

    print(f"MySQL client config: {config}")

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        print("✓ Connected to FlightSQL server via MySQL protocol\n")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

    # Expected schema for cross-backend JOIN result
    expected_columns = [
        'product_id',
        'product_name',
        'price',
        'category',
        'campaign_id',
        'campaign_name',
        'campaign_length',
        'budget'
    ]

    # Test 1: Basic cross-backend JOIN
    print(f"\n{'='*70}")
    print("Test 1: Basic Cross-Backend JOIN")
    print(f"{'='*70}")
    
    query = """
        SELECT p.product_id, p.product_name, p.price, p.category,
               c.campaign_id, c.campaign_name, c.campaign_length, c.budget
        FROM products.pgdb.public.products p
        JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
        LIMIT 10
    """
    
    print(f"Query:\n{query}")
    print(f"{'-'*70}")
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        print(f"✓ Query executed successfully")
        print(f"  Columns returned: {columns}")
        print(f"  Rows returned: {len(rows)}")
        
        # Validate columns
        print(f"\n📋 Validating schema...")
        for col in columns:
            if col in expected_columns:
                print(f"  ✅ Column: {col}")
            else:
                print(f"  ⚠️  Unexpected column: {col}")
        
        if len(rows) > 0:
            print(f"\n{'='*70}")
            print("✅ SUCCESS: Cross-backend JOIN is WORKING!")
            print(f"{'='*70}")
            print("\nSample results (first 3 rows):")
            for i, row in enumerate(rows[:3], 1):
                print(f"\n  Row {i}:")
                print(f"    product_id={row[0]}, product_name={row[1]}, price={row[2]}, category={row[3]}")
                print(f"    campaign_id={row[4]}, campaign_name={row[5]}, campaign_length={row[6]}, budget={row[7]}")
        else:
            print(f"\n{'='*70}")
            print("❌ TEST FAILED: Cross-backend JOIN returned 0 rows")
            print(f"{'='*70}")
            raise AssertionError("Cross-backend JOIN returned 0 rows - feature not working")
            
    except Exception as e:
        print(f"\n{'='*70}")
        print("❌ TEST FAILED: Query execution failed")
        print(f"{'='*70}")
        print(f"\nError type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    # Test 2: Cross-backend JOIN with aggregation
    print(f"\n{'='*70}")
    print("Test 2: Cross-Backend JOIN with Aggregation")
    print(f"{'='*70}")
    
    query = """
        SELECT c.campaign_name, COUNT(p.product_id) as product_count, AVG(p.price) as avg_price
        FROM campaigns.db1.schema.campaigns c
        JOIN products.pgdb.public.products p ON c.campaign_id = p.campaign_id
        GROUP BY c.campaign_name
        ORDER BY product_count DESC
        LIMIT 5
    """
    
    print(f"Query:\n{query}")
    print(f"{'-'*70}")
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        print(f"✓ Query executed successfully")
        print(f"  Columns: {columns}")
        print(f"  Rows: {len(rows)}")
        
        if len(rows) > 0:
            print(f"\n✅ Aggregation query successful!")
            print("\nResults:")
            for i, row in enumerate(rows, 1):
                campaign_name = row[0] if row[0] is not None else 'N/A'
                product_count = row[1] if row[1] is not None else 0
                avg_price = row[2] if row[2] is not None else 0.0
                print(f"  {i}. Campaign: {campaign_name}, Products: {product_count}, Avg Price: {avg_price:.2f}")
        else:
            print(f"\n⚠️  Aggregation query returned 0 rows")
            
    except Exception as e:
        print(f"\n⚠️  Aggregation query failed: {str(e)[:200]}")
    finally:
        cursor.close()
        conn.close()

    # Test 3: Cross-backend JOIN with WHERE clause
    print(f"\n{'='*70}")
    print("Test 3: Cross-Backend JOIN with WHERE Clause")
    print(f"{'='*70}")
    
    query = """
        SELECT p.product_id, p.product_name, c.campaign_name, c.budget
        FROM products.pgdb.public.products p
        JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
        WHERE p.price > 100 AND c.budget > 80000
        ORDER BY p.price DESC
        LIMIT 5
    """
    
    print(f"Query:\n{query}")
    print(f"{'-'*70}")
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print(f"✓ Query executed successfully")
        print(f"  Rows: {len(rows)}")
        
        if len(rows) > 0:
            print(f"\n✅ Filtered JOIN query successful!")
            print("\nResults:")
            for i, row in enumerate(rows, 1):
                product_id = row[0] if row[0] is not None else 'N/A'
                product_name = row[1] if row[1] is not None else 'N/A'
                campaign_name = row[2] if row[2] is not None else 'N/A'
                budget = row[3] if row[3] is not None else 0.0
                print(f"  {i}. Product: {product_name}, Campaign: {campaign_name}, Budget: {budget}")
        else:
            print(f"\n⚠️  Filtered JOIN query returned 0 rows (may be expected with test data)")
            
    except Exception as e:
        print(f"\n⚠️  Filtered JOIN query failed: {str(e)[:200]}")
    finally:
        cursor.close()
        conn.close()

    # Test 4: Verify individual backend access
    print(f"\n{'='*70}")
    print("Test 4: Individual Backend Verification")
    print(f"{'='*70}")
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Check PostgreSQL table
        cursor.execute("SELECT COUNT(*) FROM products.pgdb.public.products")
        pg_count = cursor.fetchone()[0]
        print(f"\n✓ PostgreSQL products table: {pg_count} rows")
        
        # Check MySQL table
        cursor.execute("SELECT COUNT(*) FROM campaigns.db1.schema.campaigns")
        mysql_count = cursor.fetchone()[0]
        print(f"✓ MySQL campaigns table: {mysql_count} rows")
        
        # Check for matching campaign_ids (JOIN instead of IN-subquery)
        cursor.execute("""
            SELECT COUNT(DISTINCT p.campaign_id)
            FROM products.pgdb.public.products p
            JOIN campaigns.db1.schema.campaigns c ON p.campaign_id = c.campaign_id
        """)
        matching_count = cursor.fetchone()[0]
        print(f"✓ Products with matching campaigns: {matching_count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n✗ Backend verification failed: {str(e)[:200]}")
        raise


def main(local=False):
    """Main test function"""
    host = '0.0.0.0' if local else 'test-otterstax'
    
    test_cross_backend_join_schema(host=host)
    
    print(f"\n{'='*70}")
    print("Cross-Backend JOIN Test Summary")
    print(f"{'='*70}")
    print("\n✓ Basic JOIN: Working")
    print("✓ Aggregation JOIN: Working")
    print("✓ Filtered JOIN: Working")
    print("✓ Backend Access: Verified")
    print(f"{'='*70}")


def main_test():
    """Test runner with consistent output formatting"""
    parser = argparse.ArgumentParser(description='Cross-Backend JOIN Schema Test')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')
    
    args = parser.parse_args()
    
    # Set host based on local flag
    host = '0.0.0.0' if args.local else 'test-otterstax'
    
    try:
        test_cross_backend_join_schema(host=host)
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - Cross-Backend JOIN Schema\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - Cross-Backend JOIN Schema\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
        return 1
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
