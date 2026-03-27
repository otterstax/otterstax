# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Simple Cross-Backend JOIN Test

This test executes a cross-backend JOIN query to verify the server's
ability to join data from multiple database backends.

Usage:
    python3 test_simple_cross_backend_join.py --local

Expected behavior:
    - If cross-backend JOINs work: Query returns joined data
    - If not supported: Query fails or returns empty results
"""

import sys
import argparse
import mysql.connector


def test_cross_backend_join(host='0.0.0.0', mysql_port=8816, pg_port=8817):
    """
    Test a simple cross-backend JOIN.

    This connects via MySQL protocol and attempts to JOIN
    a PostgreSQL table with a MySQL table.
    """
    print(f"\n{'#'*70}")
    print(f"# Simple Cross-Backend JOIN Test")
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

    # The cross-backend JOIN query
    query = """
        SELECT p.product_id, p.product_name, p.price,
               c.campaign_name, c.campaign_length
        FROM products.pgdb.public.products p
        JOIN campaigns.db1.schema.campaigns c
            ON p.campaign_id = c.campaign_id
        LIMIT 5
    """

    print(f"\n{'='*70}")
    print("Executing cross-backend JOIN query:")
    print(f"{'='*70}")
    print(query)
    print(f"{'='*70}\n")

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"✓ Query executed successfully")
        print(f"  Rows returned: {len(rows)}")
        print(f"  Columns: {[desc[0] for desc in cursor.description]}")

        if len(rows) > 0:
            print(f"\n{'='*70}")
            print("✅ SUCCESS: Cross-backend JOIN is WORKING!")
            print(f"{'='*70}")
            print("\nResults:")
            for i, row in enumerate(rows, 1):
                print(f"\n  Row {i}:")
                print(f"    product_id={row[0]}")
                print(f"    product_name={row[1]}")
                print(f"    price={row[2]}")
                print(f"    campaign_name={row[3]}")
                print(f"    campaign_length={row[4]}")
        else:
            print(f"\n{'='*70}")
            print("❌ TEST FAILED: Cross-backend JOIN returned 0 rows")
            print(f"{'='*70}")
            print("\nThis indicates the JOIN is not working properly.")
            print("The server likely executed the query on only one backend.")
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

    # Verify individual backends are accessible
    print(f"\n{'='*70}")
    print("Verifying individual backend access")
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

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n✗ Backend verification failed: {str(e)[:200]}")


def main():
    parser = argparse.ArgumentParser(
        description='Simple cross-backend JOIN debug test'
    )
    parser.add_argument(
        '--local',
        action='store_true',
        help='Use local host (0.0.0.0) instead of test-otterstax'
    )
    parser.add_argument(
        '--mysql-port',
        type=int,
        default=8816,
        help='MySQL protocol port (default: 8816)'
    )
    parser.add_argument(
        '--pg-port',
        type=int,
        default=8817,
        help='PostgreSQL protocol port (default: 8817)'
    )

    args = parser.parse_args()

    # Set host based on local flag
    host = '0.0.0.0' if args.local else 'test-otterstax'

    test_cross_backend_join(
        host=host,
        mysql_port=args.mysql_port,
        pg_port=args.pg_port
    )


def main_test():
    parser = argparse.ArgumentParser(
        description='Simple cross-backend JOIN debug test'
    )
    parser.add_argument(
        '--local',
        action='store_true',
        help='Use local host (0.0.0.0) instead of test-otterstax'
    )
    parser.add_argument(
        '--mysql-port',
        type=int,
        default=8816,
        help='MySQL protocol port (default: 8816)'
    )
    parser.add_argument(
        '--pg-port',
        type=int,
        default=8817,
        help='PostgreSQL protocol port (default: 8817)'
    )

    args = parser.parse_args()

    # Set host based on local flag
    host = '0.0.0.0' if args.local else 'test-otterstax'

    try:
        test_cross_backend_join(
            host=host,
            mysql_port=args.mysql_port,
            pg_port=args.pg_port
        )
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - Cross-Backend JOIN\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - Cross-Backend JOIN\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")


if __name__ == "__main__":
    main_test()
