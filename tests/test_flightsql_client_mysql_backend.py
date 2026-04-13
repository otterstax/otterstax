# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

import sys
import argparse
from flightsql import FlightSQLClient
import pyarrow as pa

def validate_by_request(client, test_name, test_query, expected_schema):
    print(f"\n{'='*60}")
    print(f"Running: {test_name}")
    print(f"{'='*60}")
    
    # Execute a query
    info = client.execute(test_query)

    # Retrieve data
    ticket = info.endpoints[0].ticket
    reader = client.do_get(ticket)

    # Read the table
    table = reader.read_all()

    # 1. Check if table exists and has data
    if table is None or table.num_rows == 0:
        print(f"Error [{test_name}]: No data received")
        sys.exit(1)

    # 2. Validate schema and check sizes
    for field in table.schema:
        field_name = field.name
        field_type = field.type

        # Check if field exists in expected columns
        if field_name not in expected_schema:
            print(f"Error [{test_name}]: Unexpected field '{field_name}' found")
            raise KeyError(f"Unexpected field '{field_name}' found")

        # Check type matching
        if field_type != expected_schema[field_name]:
            print(f"Error [{test_name}]: Field '{field_name}' has type {field_type}, expected {expected_schema[field_name]}")
            raise TypeError(f"Field '{field_name}' has type {field_type}, expected {expected_schema[field_name]}")

        # Get the column data
        column_data = table[field_name]

        # Check size (rows)
        actual_size = len(column_data)

        if actual_size == 0:
            print(f"Error [{test_name}]: Field '{field_name}' has {actual_size} rows, expected > 0")
            raise ValueError(f"Field '{field_name}' has {actual_size} rows, expected > 0")

        # Check null count
        null_count = column_data.null_count

        # Print detailed information
        print(f"Field: {field_name}")
        print(f"  Type: {field_type}")
        print(f"  Size: {actual_size} rows")
        print(f"  Null count: {null_count}")
        print(f"  First 5 values: {column_data.to_pylist()[:5]}\n")

    # 3. Alternative: Direct column access with checks
    for col_name in expected_schema.keys():
        try:
            col = table[col_name]
            print(f"{col_name}: {len(col)} values | Type: {col.type}")
        except KeyError:
            print(f"Missing expected column: {col_name}")
    
    print(f"✓ {test_name} PASSED")

# This script is designed to test the functionality of the FlightSQLServer
def main(local=False):
    # Select host based on local flag
    host = '0.0.0.0' if local else 'test-otterstax'
    
    print(f"Connecting to host: {host}")
    
    # Initialize the client
    client = FlightSQLClient(
        host=host,
        port=8815,
        insecure=True
    )

    # List of expected columns (modify as needed)
    expected_schema_campaigns = {
        'campaign_name': pa.string(),
        'campaign_id': pa.int32(),
        'campaign_length': pa.int32(),
        'budget': pa.float32()
    }

    expected_schema_impressions = {
        'impression_id': pa.int32(),
        'campaign_id': pa.int32(),
        'clicks': pa.int32(),
        'days_since_start': pa.int32(),
        'revenue': pa.float32(),
        'conversions': pa.int32()
    }

    expected_schema_join = {
        'campaign_length': pa.int32(),
        'campaign_id': pa.int32(),
        'budget': pa.float32(),
        'campaign_name': pa.string(),
        'impression_id': pa.int32(),
        'clicks': pa.int32(),
        'days_since_start': pa.int32(),
        'revenue': pa.float32(),
        'conversions': pa.int32()
    }

    # Test 1: Basic JOIN with WHERE and ORDER BY (from examples/example_2.txt)
    test_query_1 = """SELECT * FROM campaigns.db1.schema.campaigns JOIN
impressions.db2.schema.impressions ON
campaigns.campaign_id = impressions.campaign_id
WHERE campaigns.campaign_length > 30
ORDER BY impressions.clicks DESC;"""

    validate_by_request(client, "Test 1: Basic JOIN (example_2)", test_query_1, expected_schema_join)

    # Test 2: JOIN with complex WHERE (from examples/example_1.txt)
    test_query_2 = """SELECT * FROM campaigns.db1.schema.campaigns JOIN
impressions.db2.schema.impressions ON
campaigns.campaign_id = impressions.campaign_id AND campaign_length > 30
WHERE budget > 80000 OR budget < 90000
ORDER BY clicks;"""

    validate_by_request(client, "Test 2: JOIN with complex WHERE (example_1)", test_query_2, expected_schema_join)

    # Test 3: Simple SELECT from single table
    test_query_3 = "SELECT * FROM campaigns.db1.schema.campaigns WHERE campaign_length > 30;"

    validate_by_request(client, "Test 3: Simple SELECT from single table", test_query_3, expected_schema_campaigns)

    # Test 4: Simple JOIN (from examples/example_2.txt)
    test_query_4 = """SELECT * FROM campaigns.db1.schema.campaigns JOIN
impressions.db2.schema.impressions ON
campaigns.campaign_id = impressions.campaign_id
WHERE campaigns.campaign_length > 30
ORDER BY impressions.clicks DESC;"""

    validate_by_request(client, "Test 4: Simple JOIN (example_2)", test_query_4, expected_schema_join)

    # Test 5: JOIN with AND condition (from examples/example_1.txt)
    test_query_5 = """SELECT * FROM campaigns.db1.schema.campaigns JOIN
impressions.db2.schema.impressions ON
campaigns.campaign_id = impressions.campaign_id AND campaign_length > 30
WHERE budget > 80000 OR budget < 90000
ORDER BY clicks;"""

    validate_by_request(client, "Test 5: JOIN with AND condition (example_1)", test_query_5, expected_schema_join)

    # Test 6: JOIN with multiple conditions (from examples/example_5.txt)
    test_query_6 = """SELECT * FROM campaigns.db1.schema.campaigns JOIN
impressions.db2.schema.impressions ON
campaigns.campaign_id = impressions.campaign_id AND campaign_length > 80 AND clicks > 200
WHERE budget > 80000 OR budget < 90000;"""

    validate_by_request(client, "Test 6: JOIN with multiple conditions (example_5)", test_query_6, expected_schema_join)

    # Test 7: Simple SELECT from single table (from examples/example_3.txt)
    test_query_7 = "SELECT * FROM campaigns.db1.schema.campaigns WHERE campaign_length > 30;"

    validate_by_request(client, "Test 7: Simple SELECT (example_3)", test_query_7, expected_schema_campaigns)

    # Test 8: SELECT from impressions table (from examples/example_4.txt)
    test_query_8 = "SELECT * FROM impressions.db2.schema.impressions WHERE clicks > 200;"

    validate_by_request(client, "Test 8: SELECT from impressions (example_4)", test_query_8, expected_schema_impressions)


def main_test():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Validate FlightSQL queries')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')

    args = parser.parse_args()

    try:
        main(local=args.local)
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - FlightSQL MySQL Backend\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - FlightSQL MySQL Backend\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
        return 1
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")

if __name__ == "__main__":
    sys.exit(main_test())
