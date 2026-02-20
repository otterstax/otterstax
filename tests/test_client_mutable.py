import sys
import argparse
from flightsql import FlightSQLClient
import pyarrow as pa

def validate_by_request(client, test_query, expected_schema):
    # Execute a query
    info = client.execute(test_query)

    # Retrieve data
    ticket = info.endpoints[0].ticket
    reader = client.do_get(ticket)

    # Read the table
    table = reader.read_all()

    # 1. Check if table exists
    if table is None:
        print("Error: No data received")
        sys.exit(1)

    # 2. Validate schema and check sizes
    for field in table.schema:
        field_name = field.name
        field_type = field.type

        # Check if field exists in expected columns
        if field_name not in expected_schema:
            print(f"Warning: Unexpected field '{field_name}' found")
            raise KeyError(f"Unexpected field '{field_name}' found")

        # Check type matching
        if field_type != expected_schema[field_name]:
            print(f"Warning: Field '{field_name}' has type {field_type}, expected {expected_schema[field_name]}")
            raise TypeError(f"Field '{field_name}' has type {field_type}, expected {expected_schema[field_name]}")

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
    expected_schema = {
        'temp_int': pa.int64()
    }

    test_query_1 = "CREATE TABLE campaigns.db1.schema.temp_table (temp_int int);"
    validate_by_request(client, test_query_1, expected_schema)

    test_query_2 = "INSERT INTO campaigns.db1.schema.temp_table (temp_int) VALUES (8888);"
    validate_by_request(client, test_query_2, expected_schema)

    test_query_3 = "INSERT INTO campaigns.db1.schema.temp_table (temp_int) \
VALUES (1), (2), (3), (4);"
    validate_by_request(client, test_query_3, expected_schema)

    test_query_4 = "UPDATE campaigns.db1.schema.temp_table SET temp_int = temp_int * 1000 WHERE temp_int < 1000;"
    validate_by_request(client, test_query_4, expected_schema)

    test_query_5 = "DELETE FROM campaigns.db1.schema.temp_table WHERE temp_int >= 8888;"
    validate_by_request(client, test_query_5, expected_schema)

    test_query_6 = "INSERT INTO campaigns.db1.schema.temp_table (temp_int) \
SELECT * FROM campaigns.db1.schema.temp_table \
WHERE temp_int = 1000;"
    validate_by_request(client, test_query_6, expected_schema)

    test_query_7 = "CREATE INDEX temp_index ON campaigns.db1.schema.temp_table (temp_int);"
    validate_by_request(client, test_query_7, expected_schema)

    test_query_8 = "DROP INDEX campaigns.db1.schema.temp_table.temp_index;"
    validate_by_request(client, test_query_8, expected_schema)

    test_query_9 = "DROP TABLE campaigns.db1.schema.temp_table;"
    validate_by_request(client, test_query_9, expected_schema)

def main_test():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Test FlightSQL DDL operations')
    parser.add_argument('--local', action='store_true', 
                       help='Use local host (0.0.0.0) instead of test-otterstax')
    
    args = parser.parse_args()
    
    try:
        main(local=args.local)
        # Print Test Success message in Green
        print("\033[92mTest success.\033[0m")
    except Exception as e:
        # Print Test Fail message in Red and the error details
        print(f"\033[91mAn error occurred: {e}\033[0m")
        print("\033[91mTest fails.\033[0m")
    finally:
        # Print Test Completed message in default color
        print("Test completed.")

if __name__ == "__main__":
    main_test()