# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

import sys
import argparse
from flightsql import FlightSQLClient
import pyarrow as pa
import pyarrow.ipc as ipc

def validate_table_schemas(client, expected_schemas):
    """Validate table schemas using get_tables method"""
    
    # Get tables information with schema included
    flight_info = client.get_tables(include_schema=True)
    
    if flight_info is None:
        print("Error: No tables information received")
        raise ValueError("No tables information received")
    
    ticket = flight_info.endpoints[0].ticket
    reader = client.do_get(ticket)
    tables_table = reader.read_all()
    
    # 1. Check if table exists and has data
    if tables_table is None or tables_table.num_rows == 0:
        print("Error: No tables data received")
        raise ValueError("No tables data received")
    
    print(f"Found {tables_table.num_rows} tables")
    print(f"Tables response columns: {tables_table.column_names}")
    
    # Check if schema column is present
    if 'table_schema' not in tables_table.column_names:
        print("Error: table_schema column not found in response")
        raise KeyError("table_schema column not found in response")
    
    found_tables = {}
    
    # 2. Process each table
    for i in range(tables_table.num_rows):
        catalog = tables_table['catalog_name'][i].as_py()
        schema_name = tables_table['db_schema_name'][i].as_py()
        table_name = tables_table['table_name'][i].as_py()
        table_type = tables_table['table_type'][i].as_py()
        
        full_table_name = f"{catalog}.{schema_name}.{table_name}"
        print(f"\nTable: {full_table_name} (type: {table_type})")
        
        schema_bytes = tables_table['table_schema'][i].as_py()
        if schema_bytes is None:
            print(f"Error: No schema data for {full_table_name}")
            raise ValueError(f"No schema data for {full_table_name}")
        
        # Deserialize the schema from binary format
        buffer = pa.py_buffer(schema_bytes)
        deserialized_schema = ipc.read_schema(buffer)
        
        actual_schema = {}
        for field in deserialized_schema:
            actual_schema[field.name] = field.type
        found_tables[table_name] = actual_schema
        
        # 3. Validate against expected schema if available
        if table_name in expected_schemas:
            expected = expected_schemas[table_name]
            print(f"Validating schema for {table_name}:")
            
            for expected_field, expected_type in expected.items():
                if expected_field not in actual_schema:
                    print(f"Error: Missing expected field '{expected_field}' in {table_name}")
                    raise KeyError(f"Missing expected field '{expected_field}' in {table_name}")
                actual_type = actual_schema[expected_field]
                if actual_type != expected_type:
                    print(f"Error: Field '{expected_field}' has type {actual_type}, expected {expected_type}")
                    raise TypeError(f"Field '{expected_field}' has type {actual_type}, expected {expected_type}")
                
                print(f"  ✓ {expected_field}: {actual_type} (matches expected)")
            for actual_field in actual_schema.keys():
                if actual_field not in expected:
                    print(f"Warning: Unexpected field '{actual_field}' found in {table_name}")
                    raise KeyError(f"Unexpected field '{actual_field}' found in {table_name}")
    
    # 4. Check if all expected tables were found
    for expected_table in expected_schemas.keys():
        if expected_table not in found_tables:
            print(f"Error: Expected table '{expected_table}' not found")
            raise ValueError(f"Expected table '{expected_table}' not found")
    
    print(f"Successfully validated {len(found_tables)} tables")

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

    # Expected schemas for tables
    expected_schemas = {
        'campaigns': {
            'campaign_name': pa.string(),
            'campaign_id': pa.int64(),
            'campaign_length': pa.int64(),
            'budget': pa.float32()
        },
        'impressions': {
            'impression_id': pa.int64(),
            'campaign_id': pa.int64(),
            'clicks': pa.int64(),
            'days_since_start': pa.int64(),
            'revenue': pa.float32(),
            'conversions': pa.int64()
        }
    }

    validate_table_schemas(client, expected_schemas)

def main_test():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Validate table schemas')
    parser.add_argument('--local', action='store_true', 
                       help='Use local host (0.0.0.0) instead of test-otterstax')
    
    args = parser.parse_args()
    
    try:
        main(local=args.local)
        print("\033[92mTest success.\033[0m")
    except Exception as e:
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
    finally:
        print("Test completed.")

if __name__ == "__main__":
    main_test()