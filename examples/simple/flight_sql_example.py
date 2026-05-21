# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

import sys
import ssl
from flightsql import FlightSQLClient
import pyarrow as pa

def main():
    if len(sys.argv) != 2:
        print("Usage: python client.py <query_file>")
        sys.exit(1)

    query_file = sys.argv[1]

    # Read query from file
    try:
        with open(query_file, 'r') as file:
            query = file.read().strip()
    except Exception as e:
        print(f"Error reading query file: {e}")
        sys.exit(1)

    # Initialize the client
    client = FlightSQLClient(
        host='0.0.0.0',
        port=8815,
        insecure=True
    )

    # For UPDATE/INSERT/DELETE operations
    if query.strip().upper().startswith(('UPDATE', 'INSERT', 'DELETE')):
        # Execute the update command
        result = client.execute_update(query)

        print(f"Operation successful. Affected rows: {result}")
    else:
        # Execute a query (original behavior)
        info = client.execute(query)
        # Retrieve data
        ticket = info.endpoints[0].ticket
        reader = client.do_get(ticket)
        # Read the table
        table = reader.read_all()

        # 1. Check if table exists and has data
        if table is None or table.num_rows == 0:
            print("No data received")
            sys.exit(1)

        # 2. Validate schema and check sizes
        for field in table.schema:
            field_name = field.name
            field_type = field.type
            # Get the column data
            column_data = table[field_name]
            
            print(f"Field: {field_name}")
            print(f"  Type: {field_type}")
            print(f"  Size: {len(column_data)} rows")
            print(f"  Null count: {column_data.null_count}")
            print(f"  First 5 values: {column_data.to_pylist()[:5]}\n")

if __name__ == "__main__":
    main()