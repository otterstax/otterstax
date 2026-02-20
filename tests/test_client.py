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

    # 1. Check if table exists and has data
    if table is None or table.num_rows == 0:
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

        # Get the column data
        column_data = table[field_name]

        # Check size (rows)
        actual_size = len(column_data)

        if actual_size == 0:
            print(f"Error: Field '{field_name}' has {actual_size} rows, expected > 0")
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
        'campaign_id': pa.int64(),
        'campaign_length': pa.int64(),
        'budget': pa.float32()
    }

    expected_schema_join = {
        'campaign_length': pa.int64(),
        'campaign_id': pa.int64(),
        'budget': pa.float32(),
        'campaign_name': pa.string(),
        'impression_id': pa.int64(),
        'clicks': pa.int64(),
        'days_since_start': pa.int64(),
        'revenue': pa.float32(),
        'conversions': pa.int64()
    }

    test_query_1 = "SELECT * FROM campaigns.db1.schema.campaigns JOIN \
impressions.db2.schema.impressions ON \
campaigns.campaign_id = impressions.campaign_id \
WHERE campaigns.campaign_length > 30 \
ORDER BY impressions.clicks DESC;"

    validate_by_request(client, test_query_1, expected_schema_join)


    test_query_2 = "SELECT * FROM campaigns.db1.schema.campaigns JOIN \
impressions.db2.schema.impressions ON \
campaigns.campaign_id = impressions.campaign_id AND campaign_length > 30 \
WHERE budget > 80000 OR budget < 90000 \
ORDER BY clicks;"


    validate_by_request(client, test_query_2, expected_schema_join)

    test_query_3= "SELECT * FROM campaigns.db1.schema.campaigns WHERE campaign_length > 30;"

    validate_by_request(client, test_query_3, expected_schema_campaigns)


def main_test():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Validate FlightSQL queries')
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