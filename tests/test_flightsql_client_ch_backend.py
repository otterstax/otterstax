# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""ClickHouse backend tests via Arrow Flight SQL (port 8815)."""

import sys
import argparse
from flightsql import FlightSQLClient
import pyarrow as pa

import config

_ORDERS = f"{config.CH_ALIAS}.{config.CH_DATABASE}.schema.orders"


def validate_by_request(client, test_name, query, expected_schema):
    print(f"\n{'='*60}")
    print(f"Running: {test_name}")
    print(f"{'='*60}")

    info = client.execute(query)
    ticket = info.endpoints[0].ticket
    reader = client.do_get(ticket)
    table = reader.read_all()

    if table is None or table.num_rows == 0:
        raise ValueError(f"{test_name}: no data received")

    for field in table.schema:
        if field.name not in expected_schema:
            raise KeyError(f"{test_name}: unexpected field '{field.name}'")
        if field.type != expected_schema[field.name]:
            raise TypeError(
                f"{test_name}: field '{field.name}' has type {field.type},"
                f" expected {expected_schema[field.name]}"
            )
        col = table[field.name]
        if len(col) == 0:
            raise ValueError(f"{test_name}: field '{field.name}' has 0 rows")
        print(f"  ✓ {field.name}: {field.type} ({len(col)} rows,"
              f" first={col.to_pylist()[0]})")

    print(f"✓ {test_name} PASSED")


def main(local=False):
    host = config.get_host(local)
    print(f"Connecting to host: {host}:{config.FLIGHT_PORT}")

    flight_client = FlightSQLClient(host=host, port=config.FLIGHT_PORT, insecure=True)

    # CH type mappings (from ch_to_chunk.cpp):
    # Int64 → BIGINT → int64, Int32 → INTEGER → int32
    # String/DateTime → STRING_LITERAL → string, Float64 → DOUBLE → float64
    expected_orders = {
        'order_id':      pa.int64(),
        'campaign_id':   pa.int32(),
        'product_id':    pa.int32(),
        'customer_name': pa.string(),
        'order_date':    pa.string(),
        'quantity':      pa.int32(),
        'total_amount':  pa.float64(),
    }

    validate_by_request(
        flight_client,
        "Test 1: Simple SELECT *",
        f"SELECT * FROM {_ORDERS} LIMIT 10",
        expected_orders,
    )

    validate_by_request(
        flight_client,
        "Test 2: SELECT with WHERE",
        f"SELECT * FROM {_ORDERS} WHERE quantity > 5 LIMIT 20",
        expected_orders,
    )

    validate_by_request(
        flight_client,
        "Test 3: SELECT with ORDER BY",
        f"SELECT * FROM {_ORDERS} ORDER BY total_amount DESC LIMIT 10",
        expected_orders,
    )

    expected_subset = {
        'order_id':     pa.int64(),
        'total_amount': pa.float64(),
    }
    validate_by_request(
        flight_client,
        "Test 4: SELECT specific columns",
        f"SELECT order_id, total_amount FROM {_ORDERS} WHERE total_amount > 100 LIMIT 10",
        expected_subset,
    )


def main_test():
    parser = argparse.ArgumentParser(description='ClickHouse backend tests via FlightSQL')
    parser.add_argument('--local', action='store_true',
                        help='Use local host instead of test-otterstax')
    args = parser.parse_args()

    try:
        main(local=args.local)
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - FlightSQL Client / ClickHouse Backend\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - FlightSQL Client / ClickHouse Backend\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
