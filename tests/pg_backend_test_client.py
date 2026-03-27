# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

#!/usr/bin/env python

"""
PostgreSQL Backend Integration Tests

Tests for PostgreSQL backend integration including:
- Connection registration
- Basic SELECT queries
- Aggregation queries
- Mixed-backend JOIN (MySQL + PostgreSQL)
"""

from flightsql import FlightSQLClient
import requests
import time
import json
import sys

# Configuration
FLIGHT_SERVER_HOST = "test-otterstax"
FLIGHT_SERVER_PORT = 8815
HTTP_SERVER_HOST = "test-otterstax"
HTTP_SERVER_PORT = 8085

# PostgreSQL connection parameters
PG_CONNECTION = {
    "alias": "pgtest",
    "host": "postgres1",
    "port": "5432",
    "username": "pguser",
    "password": "pgpassword",
    "database": "pgdb",
    "table": "products"
}


def get_flight_client():
    """Create and return a FlightSQL client."""
    return FlightSQLClient(
        host=FLIGHT_SERVER_HOST,
        port=FLIGHT_SERVER_PORT,
        insecure=True
    )


def test_register_pg_connection():
    """Test registering a PostgreSQL connection via HTTP API."""
    print("\n=== Test: Register PostgreSQL Connection ===")

    url = f"http://{HTTP_SERVER_HOST}:{HTTP_SERVER_PORT}/add_pg_connection"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, json=PG_CONNECTION, headers=headers, timeout=30)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")

        if response.status_code == 200:
            print("✅ PostgreSQL connection registered successfully")
            return True
        else:
            print(f"❌ Failed to register PostgreSQL connection: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error registering connection: {e}")
        return False


def test_check_pg_connection():
    """Test checking if PostgreSQL connection exists."""
    print("\n=== Test: Check PostgreSQL Connection ===")

    url = f"http://{HTTP_SERVER_HOST}:{HTTP_SERVER_PORT}/check_pg_connection"
    headers = {"Content-Type": "application/json"}
    data = {"alias": PG_CONNECTION["alias"]}

    try:
        response = requests.get(url, json=data, headers=headers, timeout=30)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")

        if response.status_code == 200 and "exists" in response.text.lower():
            print("✅ PostgreSQL connection check passed")
            return True
        else:
            print(f"❌ PostgreSQL connection check failed")
            return False
    except Exception as e:
        print(f"❌ Error checking connection: {e}")
        return False


def execute_query(client, query):
    """Execute a query and return results."""
    print(f"\nExecuting: {query}")

    try:
        # Use FlightSQL execute method
        info = client.execute(query)

        # Retrieve data
        ticket = info.endpoints[0].ticket
        reader = client.do_get(ticket)
        table = reader.read_all()

        print(f"Rows returned: {table.num_rows}")
        print(f"Columns: {table.column_names}")

        if table.num_rows > 0:
            # Print first few values from each column
            for col_name in table.column_names[:3]:  # First 3 columns
                col = table.column(col_name)
                print(f"  {col_name}: {col.to_pylist()[:5]}")

        return table
    except Exception as e:
        print(f"❌ Query execution error: {e}")
        return None


def test_basic_select():
    """Test basic SELECT query on PostgreSQL table."""
    print("\n=== Test: Basic SELECT ===")

    client = get_flight_client()
    # Parser expects: alias.database.schema.collection
    query = f"SELECT product_id, product_name, price FROM {PG_CONNECTION['alias']}.{PG_CONNECTION['database']}.public.products LIMIT 10"

    result = execute_query(client, query)

    if result is not None and result.num_rows > 0:
        print("✅ Basic SELECT test passed")
        return True
    else:
        print("❌ Basic SELECT test failed")
        return False


def test_aggregation():
    """Test aggregation query on PostgreSQL table."""
    print("\n=== Test: Aggregation ===")

    client = get_flight_client()
    # Parser expects: alias.database.schema.collection
    # Note: Using COUNT(price) instead of COUNT(*) due to parser limitation
    query = f"""
        SELECT category, COUNT(price) as count, AVG(price) as avg_price
        FROM {PG_CONNECTION['alias']}.{PG_CONNECTION['database']}.public.products
        GROUP BY category
    """

    result = execute_query(client, query)

    if result is not None and result.num_rows > 0:
        print("✅ Aggregation test passed")
        return True
    else:
        print("❌ Aggregation test failed")
        return False


def test_mixed_backend_join():
    """Test JOIN between MySQL (campaigns) and PostgreSQL (products) tables."""
    print("\n=== Test: Mixed Backend JOIN (MySQL + PostgreSQL) ===")

    client = get_flight_client()

    # This query joins MySQL campaigns table with PostgreSQL products table
    # Parser expects: alias.database.schema.collection for both
    # MySQL: campaigns.db1.campaigns (3-part, schema implied)
    # PostgreSQL: pgtest.pgdb.public.products (4-part with explicit schema)
    query = """
        SELECT c.campaign_name, p.product_name, p.price
        FROM campaigns.db1.campaigns c
        JOIN pgtest.pgdb.public.products p ON c.campaign_id = p.campaign_id
        LIMIT 20
    """

    result = execute_query(client, query)

    if result is not None and result.num_rows > 0:
        print("✅ Mixed backend JOIN test passed")
        return True
    else:
        print("❌ Mixed backend JOIN test failed (may not be implemented yet)")
        return False


def wait_for_postgres():
    """Wait for PostgreSQL to be ready."""
    print("\n=== Waiting for PostgreSQL ===")
    max_retries = 30
    retry_interval = 2

    for i in range(max_retries):
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="postgres1",
                port=5432,
                user="pguser",
                password="pgpassword",
                database="pgdb"
            )
            conn.close()
            print("✅ PostgreSQL is ready")
            return True
        except Exception as e:
            print(f"Waiting for PostgreSQL... ({i+1}/{max_retries})")
            time.sleep(retry_interval)

    print("❌ PostgreSQL is not available")
    return False


def main():
    """Run all PostgreSQL backend tests."""
    print("=" * 60)
    print("PostgreSQL Backend Integration Tests")
    print("=" * 60)

    results = {}

    # Wait for services to be ready
    time.sleep(5)  # Initial delay

    # Test 1: Register PostgreSQL connection
    results['register_connection'] = test_register_pg_connection()
    time.sleep(2)

    # Test 2: Check connection exists
    results['check_connection'] = test_check_pg_connection()
    time.sleep(1)

    # Test 3: Basic SELECT
    results['basic_select'] = test_basic_select()
    time.sleep(1)

    # Test 4: Aggregation
    results['aggregation'] = test_aggregation()
    time.sleep(1)

    # Test 5: Mixed backend JOIN
    results['mixed_join'] = test_mixed_backend_join()

    # Summary
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)

    passed = 0
    failed = 0

    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {test_name}: {status}")
        if result:
            passed += 1
        else:
            failed += 1

    print(f"\nTotal: {passed} passed, {failed} failed")

    if failed > 0:
        print("\n⚠️ Some tests failed!")
        sys.exit(1)
    else:
        print("\n✅ All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
