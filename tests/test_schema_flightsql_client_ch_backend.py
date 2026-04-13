# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Schema validation for ClickHouse backend via Arrow Flight SQL."""

import sys
import argparse
import time
from flightsql import FlightSQLClient
import pyarrow as pa
import pyarrow.ipc as ipc

import config


def validate_table_schemas(client, expected_schemas):
    max_attempts = 15
    retry_delay_seconds = 2
    tables_table = None

    for attempt in range(1, max_attempts + 1):
        flight_info = client.get_tables(include_schema=True)
        if flight_info is not None and flight_info.endpoints:
            ticket = flight_info.endpoints[0].ticket
            reader = client.do_get(ticket)
            tables_table = reader.read_all()
        if tables_table is not None and tables_table.num_rows > 0:
            break
        if attempt < max_attempts:
            print(f"No tables data yet, retrying ({attempt}/{max_attempts})...")
            time.sleep(retry_delay_seconds)

    if tables_table is None or tables_table.num_rows == 0:
        raise ValueError("No tables data received")

    print(f"Found {tables_table.num_rows} tables")

    if 'table_schema' not in tables_table.column_names:
        raise KeyError("table_schema column not found in response")

    found_tables = {}
    for i in range(tables_table.num_rows):
        catalog    = tables_table['catalog_name'][i].as_py()
        schema_name = tables_table['db_schema_name'][i].as_py()
        table_name  = tables_table['table_name'][i].as_py()

        full_name = f"{catalog}.{schema_name}.{table_name}"
        print(f"\nTable: {full_name}")

        schema_bytes = tables_table['table_schema'][i].as_py()
        if schema_bytes is None:
            raise ValueError(f"No schema data for {full_name}")

        deserialized = ipc.read_schema(pa.py_buffer(schema_bytes))
        actual_schema = {field.name: field.type for field in deserialized}
        found_tables[table_name] = actual_schema

        if table_name in expected_schemas:
            expected = expected_schemas[table_name]
            print(f"  Validating schema for {table_name}:")
            for field_name, expected_type in expected.items():
                if field_name not in actual_schema:
                    raise KeyError(f"Missing field '{field_name}' in {table_name}")
                actual_type = actual_schema[field_name]
                if actual_type != expected_type:
                    raise TypeError(
                        f"Field '{field_name}': got {actual_type}, expected {expected_type}"
                    )
                print(f"    ✓ {field_name}: {actual_type}")
            for actual_field in actual_schema:
                if actual_field not in expected:
                    raise KeyError(f"Unexpected field '{actual_field}' in {table_name}")

    for expected_table in expected_schemas:
        if expected_table not in found_tables:
            raise ValueError(f"Expected table '{expected_table}' not found")

    print(f"\nSuccessfully validated {len(found_tables)} tables")


def main(local=False):
    host = config.get_host(local)
    print(f"Connecting to host: {host}:{config.FLIGHT_PORT}")

    flight_client = FlightSQLClient(host=host, port=config.FLIGHT_PORT, insecure=True)

    # CH type mappings (from ch_to_chunk.cpp / to_local_translator):
    # Int64  → BIGINT         → pa.int64()
    # Int32  → INTEGER        → pa.int32()
    # String → STRING_LITERAL → pa.string()
    # DateTime → STRING_LITERAL → pa.string()
    # Float64 → DOUBLE        → pa.float64()
    expected_schemas = {
        'orders': {
            'order_id':      pa.int64(),
            'campaign_id':   pa.int32(),
            'product_id':    pa.int32(),
            'customer_name': pa.string(),
            'order_date':    pa.string(),
            'quantity':      pa.int32(),
            'total_amount':  pa.float64(),
        }
    }

    validate_table_schemas(flight_client, expected_schemas)


def main_test():
    parser = argparse.ArgumentParser(description='Schema validation for ClickHouse backend')
    parser.add_argument('--local', action='store_true',
                        help='Use local host instead of test-otterstax')
    args = parser.parse_args()

    try:
        main(local=args.local)
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Schema Validation ClickHouse\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - Schema Validation ClickHouse\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
