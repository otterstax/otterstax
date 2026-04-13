# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Cross-backend schema validation tests.

Tests that verify schema discovery works correctly across different backend combinations:
- MySQL client connecting to MySQL backend
- MySQL client connecting to PostgreSQL backend  
- PostgreSQL client connecting to MySQL backend
- PostgreSQL client connecting to PostgreSQL backend
"""

import sys
import argparse
import time
from flightsql import FlightSQLClient
import pyarrow as pa
import pyarrow.ipc as ipc


def validate_table_schemas(client, expected_schemas, test_name):
    """Validate table schemas using get_tables method"""
    print(f"\n{'='*60}")
    print(f"Test: {test_name}")
    print(f"{'='*60}")

    max_attempts = 15
    retry_delay_seconds = 2
    tables_table = None

    # Metadata discovery can lag briefly after connection registration.
    for attempt in range(1, max_attempts + 1):
        flight_info = client.get_tables(include_schema=True)

        if flight_info is not None and flight_info.endpoints:
            ticket = flight_info.endpoints[0].ticket
            reader = client.do_get(ticket)
            tables_table = reader.read_all()

        if tables_table is not None and tables_table.num_rows > 0:
            break

        if attempt < max_attempts:
            print(f"No tables data received yet [{test_name}], retrying ({attempt}/{max_attempts})...")
            time.sleep(retry_delay_seconds)

    if tables_table is None or tables_table.num_rows == 0:
        print(f"Error [{test_name}]: No tables data received")
        raise ValueError("No tables data received")

    print(f"Found {tables_table.num_rows} tables")

    if 'table_schema' not in tables_table.column_names:
        print(f"Error [{test_name}]: table_schema column not found")
        raise KeyError("table_schema column not found")

    found_tables = {}

    for i in range(tables_table.num_rows):
        catalog = tables_table['catalog_name'][i].as_py()
        schema_name = tables_table['db_schema_name'][i].as_py()
        table_name = tables_table['table_name'][i].as_py()

        full_table_name = f"{catalog}.{schema_name}.{table_name}"
        print(f"  Table: {full_table_name}")

        schema_bytes = tables_table['table_schema'][i].as_py()
        if schema_bytes is None:
            continue

        buffer = pa.py_buffer(schema_bytes)
        deserialized_schema = ipc.read_schema(buffer)

        actual_schema = {}
        for field in deserialized_schema:
            actual_schema[field.name] = field.type
        found_tables[table_name] = actual_schema

    # Validate expected tables
    for expected_table, expected_schema in expected_schemas.items():
        if expected_table not in found_tables:
            print(f"Error [{test_name}]: Expected table '{expected_table}' not found")
            raise ValueError(f"Expected table '{expected_table}' not found")

        actual = found_tables[expected_table]
        for field, expected_type in expected_schema.items():
            if field not in actual:
                print(f"Error [{test_name}]: Missing field '{field}' in {expected_table}")
                raise KeyError(f"Missing field '{field}'")
            if actual[field] != expected_type:
                print(f"Error [{test_name}]: Field '{field}' type mismatch")
                raise TypeError(f"Type mismatch for '{field}'")

    print(f"✓ {test_name} PASSED")
    return True


def main(local=False):
    host = '0.0.0.0' if local else 'test-otterstax'
    print(f"Connecting to host: {host}")

    client = FlightSQLClient(host=host, port=8815, insecure=True)

    # MySQL expected schemas (int32 for INT columns)
    mysql_schemas = {
        'campaigns': {
            'campaign_name': pa.string(),
            'campaign_id': pa.int32(),
            'campaign_length': pa.int32(),
            'budget': pa.float32()
        },
        'impressions': {
            'impression_id': pa.int32(),
            'campaign_id': pa.int32(),
            'clicks': pa.int32(),
            'days_since_start': pa.int32(),
            'revenue': pa.float32(),
            'conversions': pa.int32()
        }
    }

    # PostgreSQL expected schemas (int32 for INT/SERIAL)
    pg_schemas = {
        'products': {
            'product_id': pa.int32(),
            'campaign_id': pa.int32(),
            'product_name': pa.string(),
            'price': pa.float64(),
            'category': pa.string()
        }
    }

    # Test 1: MySQL tables via FlightSQL (MySQL backend)
    validate_table_schemas(client, mysql_schemas, "MySQL Backend - MySQL Tables")

    # Test 2: PostgreSQL tables via FlightSQL (PostgreSQL backend)
    validate_table_schemas(client, pg_schemas, "PostgreSQL Backend - PostgreSQL Tables")

    # Test 3: Cross-backend - all tables visible
    all_schemas = {**mysql_schemas, **pg_schemas}
    validate_table_schemas(client, all_schemas, "Cross-Backend - All Tables")

    print("\n" + "="*60)
    print("All cross-backend schema tests PASSED")
    print("="*60)


def main_test():
    parser = argparse.ArgumentParser(description='Cross-backend schema validation')
    parser.add_argument('--local', action='store_true',
                       help='Use local host (0.0.0.0) instead of test-otterstax')
    args = parser.parse_args()

    try:
        main(local=args.local)
        # Print Test Success message in Green
        print("\n" + "="*70)
        print("\033[92m✅ ALL TESTS PASSED - Cross-Backend Schema\033[0m")
        print("="*70)
        print("\033[92mTest success.\033[0m")
        return 0
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print("\n" + "="*70)
        print(f"\033[91m❌ TEST FAILED - Cross-Backend Schema\033[0m")
        print("="*70)
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
        return 1
    finally:
        # Print Test Completed message in default color
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
