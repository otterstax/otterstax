# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

import sys
import argparse
from flightsql import FlightSQLClient
import pyarrow as pa

def validate_by_request(client, test_name, test_query, expected_schema):
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"{'='*70}")
    print(f"Query: {test_query}")
    print(f"Expected schema: {expected_schema}")
    print(f"{'-'*70}")

    # Execute a query
    print("Executing query...")
    info = client.execute(test_query)

    # Retrieve data
    print("Retrieving data via FlightSQL do_get...")
    ticket = info.endpoints[0].ticket
    reader = client.do_get(ticket)

    # Read the table
    print("Reading Arrow table...")
    table = reader.read_all()

    # 1. Check if table exists and has data
    if table is None or table.num_rows == 0:
        print(f"❌ FAILED [{test_name}]: No data received")
        sys.exit(1)
    
    print(f"✅ Data received: {table.num_rows} rows, {len(table.schema)} columns")

    # 2. Validate schema and check sizes
    print(f"\n📋 Validating schema ({len(expected_schema)} expected fields)...")
    for field in table.schema:
        field_name = field.name
        field_type = field.type

        # Check if field exists in expected columns
        if field_name not in expected_schema:
            print(f"❌ FAILED [{test_name}]: Unexpected field '{field_name}' found")
            raise KeyError(f"Unexpected field '{field_name}' found")

        # Check type matching
        if field_type != expected_schema[field_name]:
            print(f"❌ FAILED [{test_name}]: Field '{field_name}' has type {field_type}, expected {expected_schema[field_name]}")
            raise TypeError(f"Field '{field_name}' has type {field_type}, expected {expected_schema[field_name]}")

        # Get the column data
        column_data = table[field_name]

        # Check size (rows)
        actual_size = len(column_data)

        if actual_size == 0:
            print(f"❌ FAILED [{test_name}]: Field '{field_name}' has {actual_size} rows, expected > 0")
            raise ValueError(f"Field '{field_name}' has {actual_size} rows, expected > 0")

        # Check null count
        null_count = column_data.null_count

        # Print detailed information
        print(f"  ✅ Field: {field_name}")
        print(f"     Type: {field_type}")
        print(f"     Size: {actual_size} rows")
        print(f"     Null count: {null_count}")
        print(f"     First 5 values: {column_data.to_pylist()[:5]}\n")

    # 3. Alternative: Direct column access with checks
    print(f"\n📋 Validating expected columns...")
    for col_name in expected_schema.keys():
        try:
            col = table[col_name]
            print(f"  ✅ {col_name}: {len(col)} values | Type: {col.type}")
        except KeyError:
            print(f"  ❌ Missing expected column: {col_name}")

    print(f"\n{'='*70}")
    print(f"✅ {test_name} PASSED")
    print(f"{'='*70}")

# This script is designed to test the functionality of the FlightSQLServer
def main(local=False):
    # Select host based on local flag
    host = '0.0.0.0' if local else 'test-otterstax'

    print(f"\n{'#'*70}")
    print(f"# FlightSQL Client - PostgreSQL Backend Tests")
    print(f"{'#'*70}")
    print(f"Connecting to FlightSQL server at: {host}:8815")
    print(f"Target: PostgreSQL backend (products.pgdb.public)")
    print(f"{'#'*70}\n")

    # Initialize the client
    print("Initializing FlightSQL client...")
    client = FlightSQLClient(
        host=host,
        port=8815,
        insecure=True
    )
    print("✅ FlightSQL client initialized\n")

    # Expected schema for products table (from fixtures/generate_data.py)
    # Note: PostgreSQL INT/SERIAL maps to int32, unlike MySQL which maps to int64
    expected_schema_products = {
        'product_id': pa.int32(),
        'campaign_id': pa.int32(),
        'product_name': pa.string(),
        'price': pa.float64(),
        'category': pa.string()
    }

    print(f"\n{'='*70}")
    print("TEST SUITE: FlightSQL PostgreSQL Backend")
    print(f"{'='*70}")
    print("Total tests: 5")
    print("  1. Simple SELECT")
    print("  2. SELECT with WHERE clause")
    print("  3. SELECT with ORDER BY")
    print("  4. SELECT with numeric filter")
    print("  5. SELECT with LIMIT")
    print(f"{'='*70}")

    # Test query 1: Simple SELECT from products table
    test_query_1 = "SELECT * FROM products.pgdb.public.products;"
    validate_by_request(client, "Test 1: Simple SELECT", test_query_1, expected_schema_products)

    # Test query 2: SELECT with WHERE clause
    test_query_2 = "SELECT * FROM products.pgdb.public.products WHERE price > 100;"
    validate_by_request(client, "Test 2: SELECT with WHERE", test_query_2, expected_schema_products)

    # Test query 3: SELECT with ORDER BY
    test_query_3 = "SELECT * FROM products.pgdb.public.products ORDER BY price DESC;"
    validate_by_request(client, "Test 3: SELECT with ORDER BY", test_query_3, expected_schema_products)

    # Test query 4: SELECT specific columns with numeric filter (string filter has server bug)
    test_query_4 = "SELECT product_id, product_name, price FROM products.pgdb.public.products WHERE price > 300;"
    expected_schema_subset = {
        'product_id': pa.int32(),
        'product_name': pa.string(),
        'price': pa.float64()
    }
    validate_by_request(client, "Test 4: SELECT with numeric filter", test_query_4, expected_schema_subset)

    # Test query 5: SELECT with LIMIT
    test_query_5 = "SELECT product_id, product_name, price FROM products.pgdb.public.products WHERE price > 200 LIMIT 10;"
    expected_schema_limit = {
        'product_id': pa.int32(),
        'product_name': pa.string(),
        'price': pa.float64()
    }
    validate_by_request(client, "Test 5: SELECT with LIMIT", test_query_5, expected_schema_limit)


def main_test():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Validate FlightSQL queries for PostgreSQL backend')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    try:
        main(local=args.local)
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - FlightSQL PostgreSQL Backend\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - FlightSQL PostgreSQL Backend\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
        return 1
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")

if __name__ == "__main__":
    sys.exit(main_test())
