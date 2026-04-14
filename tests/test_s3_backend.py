"""
E2E tests for S3 backend.
Requires: OtterStax + LocalStack (docker-compose.test.yml).
"""
from config import get_host, FLIGHT_PORT, HTTP_PORT
import pyarrow.flight as fl
import requests


def register_s3_connection(host, bucket, prefix, fmt, alias,
                           endpoint="http://localstack:4566"):
    url = f"http://{host}:{HTTP_PORT}/add_s3_connection"
    payload = {
        "bucket": bucket,
        "region": "us-east-1",
        "access_key": "test",
        "secret_key": "test",
        "endpoint": endpoint,
        "prefix": prefix,
        "format": fmt,
        "alias": alias
    }
    resp = requests.post(url, json=payload)
    assert resp.status_code in (200, 201), f"Register S3 failed: {resp.text}"


def query(host, sql):
    client = fl.FlightClient(f"grpc://{host}:{FLIGHT_PORT}")
    info = client.get_flight_info(fl.FlightDescriptor.for_command(sql.encode()))
    reader = client.do_get(info.endpoints[0].ticket)
    return reader.read_all()


def test_s3_parquet_select_all(host):
    """Read Parquet from S3 bucket."""
    register_s3_connection(host, "test-bucket", "data/events.parquet", "parquet", "s3events")
    table = query(host, "SELECT * FROM s3events")
    assert table.num_rows > 0, "S3 Parquet returned no rows"
    assert "id" in table.schema.names
    assert "event_name" in table.schema.names
    print(f"  s3_parquet_select_all: {table.num_rows} rows")


def test_s3_parquet_with_filter(host):
    """SELECT with WHERE (filtering via Otterbrix)."""
    table = query(host, "SELECT id, event_name FROM s3events WHERE id <= 10")
    assert table.num_rows == 10
    print(f"  s3_parquet_with_filter: {table.num_rows} rows")


def test_s3_parquet_aggregation(host):
    """GROUP BY via Otterbrix after loading from S3."""
    table = query(host, "SELECT campaign_id, count(*) as cnt FROM s3events GROUP BY campaign_id")
    assert table.num_rows > 0
    print(f"  s3_parquet_aggregation: {table.num_rows} groups")


def test_s3_csv(host):
    """Read CSV from S3."""
    register_s3_connection(host, "test-bucket", "data/users.csv", "csv", "s3users")
    table = query(host, "SELECT * FROM s3users")
    assert table.num_rows > 0
    assert "name" in table.schema.names
    print(f"  s3_csv: {table.num_rows} rows")


def test_s3_json(host):
    """Read NDJSON from S3."""
    register_s3_connection(host, "test-bucket", "data/logs.ndjson", "json", "s3logs")
    table = query(host, "SELECT level, count(*) FROM s3logs GROUP BY level")
    assert table.num_rows > 0
    print(f"  s3_json: {table.num_rows} groups")


def test_s3_cross_backend_join(host):
    """S3 JOIN MySQL - cross-backend query."""
    table = query(host,
        "SELECT e.event_name, c.campaign_name "
        "FROM s3events e "
        "JOIN campaigns.db1.schema.campaigns c "
        "ON e.campaign_id = c.campaign_id "
        "LIMIT 10")
    assert table.num_rows > 0
    assert "event_name" in table.schema.names
    assert "campaign_name" in table.schema.names
    print(f"  s3_cross_backend_join: {table.num_rows} rows")


def test_s3_nonexistent_key(host):
    """Query to nonexistent key - expect error."""
    register_s3_connection(host, "test-bucket", "does/not/exist.parquet", "parquet", "s3missing")
    try:
        query(host, "SELECT * FROM s3missing")
        assert False, "Expected error for missing S3 key"
    except Exception as e:
        assert "NoSuchKey" in str(e) or "not found" in str(e).lower()
        print(f"  s3_nonexistent_key: correctly raised error")


def test_s3_wildcard_prefix(host):
    """Read multiple files by wildcard prefix."""
    register_s3_connection(host, "test-bucket", "data/*.parquet", "parquet", "s3wild")
    table = query(host, "SELECT * FROM s3wild")
    assert table.num_rows > 0
    print(f"  s3_wildcard_prefix: {table.num_rows} rows")


if __name__ == "__main__":
    import sys
    local = "--local" in sys.argv
    host = get_host(local)

    print("S3 Backend E2E Tests")
    print("=" * 50)
    test_s3_parquet_select_all(host)
    test_s3_parquet_with_filter(host)
    test_s3_parquet_aggregation(host)
    test_s3_csv(host)
    test_s3_json(host)
    test_s3_cross_backend_join(host)
    test_s3_nonexistent_key(host)
    test_s3_wildcard_prefix(host)
    print("=" * 50)
    print("All S3 backend tests passed")
