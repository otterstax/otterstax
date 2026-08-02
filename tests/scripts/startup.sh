#!/bin/bash

# Do not exit immediately on first failure; we aggregate failures and continue
# so the test harness can report diagnostics for all tests.
# set -e is intentionally NOT used here.

# Counter for test results
PASSED=0
FAILED=0

run_test() {
    local test_name=$1
    local test_file=$2
    local logfile
    logfile=$(mktemp /tmp/$(basename "$test_file").XXXXXX)

    echo ""
    echo "========================================="
    echo "========================================="
    echo " Running: $test_name "
    echo "========================================="
    echo "========================================="

    # Run the test and capture stdout/stderr to a temporary buffer
    python "$test_file" > "$logfile" 2>&1
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "✅ PASSED: $test_name"
        ((PASSED++))
        # Concurrency / stress tests report measurements that are worth keeping
        # in the main run log. Other tests are noisy on success — keep them quiet.
        case "$test_name" in
            Concurrency|"Stress (optional)")
                echo "--- $test_name output ---"
                cat "$logfile" || true
                echo "--- end output ---"
                ;;
        esac
        rm -f "$logfile" || true
    else
        echo "❌ FAILED: $test_name (exit code $rc)"
        ((FAILED++))
        echo "--- Last 200 lines of test output ---"
        tail -n 200 "$logfile" || true
        echo "--- end output ---"
        rm -f "$logfile" || true
    fi
}

# Wait for otterstax to be healthy
echo ""
echo "========================================="
echo "Waiting for OtterStax to be ready..."
echo "========================================="
./http_healthcheck.sh

if [ $? -ne 0 ]; then
    echo "❌ OtterStax health check failed!"
    exit 1
fi

# Run connection setup script with retries
echo ""
echo "========================================="
echo "Adding connections..."
echo "========================================="
./add_connections.sh

# Add PostgreSQL connection (extra attempt)
echo "Adding PostgreSQL connection (extra attempt)..."
curl -X POST "http://test-otterstax:8085/add_pg_connection" \
    -H "Content-Type: application/json" \
    -d @connection_postgres.json || echo "PostgreSQL connection may already exist"

# Add ClickHouse connection (extra attempt)
echo "Adding ClickHouse connection (extra attempt)..."
curl -X POST "http://test-otterstax:8085/add_ch_connection" \
    -H "Content-Type: application/json" \
    -d @connection_clickhouse.json || echo "ClickHouse connection may already exist"

# Give server time to register connections
echo "Waiting for connections to register..."
sleep 3

# Schema tests
run_test "FlightSQL Schema (MySQL backend)" "test_schema_flightsql_client_mysql_backend.py"
run_test "FlightSQL Schema (PostgreSQL backend)" "test_schema_flightsql_client_pg_backend.py"
run_test "FlightSQL Schema (ClickHouse backend)" "test_schema_flightsql_client_ch_backend.py"
run_test "Cross-backend Schema" "test_schema_cross_backend.py"
run_test "Cross-backend JOIN Schema" "test_schema_cross_backend_join.py"

# Functional tests
run_test "FlightSQL Client (MySQL backend)" "test_flightsql_client_mysql_backend.py"
run_test "FlightSQL Client (PostgreSQL backend)" "test_flightsql_client_pg_backend.py"
run_test "FlightSQL Client (ClickHouse backend)" "test_flightsql_client_ch_backend.py"
run_test "MySQL Client (MySQL backend)" "test_mysql_client_mysql_backend.py"
run_test "PostgreSQL Client (MySQL backend)" "test_pg_client_mysql_backend.py"
run_test "MySQL Client (PostgreSQL backend)" "test_mysql_client_pg_backend.py"
run_test "PostgreSQL Client (PostgreSQL backend)" "test_pg_client_pg_backend.py"
run_test "MySQL Client (ClickHouse backend)" "test_mysql_client_ch_backend.py"
run_test "PostgreSQL Client (ClickHouse backend)" "test_pg_client_ch_backend.py"
run_test "FlightSQL Client (MySQL backend, mutable)" "test_flightsql_client_mysql_backend_mutable.py"

# Cross-backend JOIN tests
run_test "Cross-backend Queries (MySQL wire)" "test_cross_backend_queries_mysql.py"
run_test "Cross-backend Queries (PostgreSQL wire)" "test_cross_backend_queries_pg.py"

# Spark Connect E2E tests — only runnable in an image that ships pyspark + a JVM
# (Dockerfile.spark-test). The plain integration-test client skips these.
echo ""
echo "========================================="
echo "=== Spark Connect tests ==="
echo "========================================="
if python -c "import pyspark" 2>/dev/null; then
    run_test "Spark Client (MySQL backend)" "test_spark_client_mysql_backend.py"
    run_test "Spark Client (PostgreSQL backend)" "test_spark_client_pg_backend.py"
    run_test "Spark Client (Cross-backend)" "test_spark_client_cross_backend.py"
else
    echo "⚠️  Skipping Spark Connect tests (pyspark not installed in this image)"
fi

# Concurrency tests (sync-code regression net)
run_test "Concurrency" "test_concurrency.py"

# Stress tests are gated behind OTTERSTAX_RUN_STRESS=1; the script no-ops fast
# when unset, so it's cheap to always invoke it here.
run_test "Stress (optional)" "test_stress.py"

# External-table (s3/file grammar extension) tests over the MySQL wire
run_test "Schema MySQL Client (file external, all formats)" "test_schema_mysql_file.py"
run_test "Schema MySQL Client (s3 external, all formats)" "test_schema_mysql_s3.py"
run_test "MySQL Client (file external, all formats)" "test_mysql_file.py"
run_test "MySQL Client (file external, ndjson)" "test_mysql_file_ndjson.py"
run_test "MySQL Client (s3 external, all formats)" "test_mysql_s3.py"
run_test "MySQL Client (JOIN sql-backend ⋈ s3 parquet → s3 csv)" "test_mysql_join_sql_s3_to_s3.py"
run_test "MySQL Client (JOIN otterbrix-local ⋈ s3 parquet)" "test_mysql_join_otb_local_s3.py"
run_test "MySQL Client (JOIN sql backend ⋈ otterbrix-local, string key)" "test_mysql_join_otb_local_backend.py"
run_test "MySQL Client (JOIN s3 parquet ⋈ file csv ⋈ otterbrix-local)" "test_mysql_join_otb_local_s3_file.py"
# Kafka integration tests
run_test "Kafka SOURCE Ingestion (PostgreSQL wire)" "test_kafka_source_ingestion.py"
run_test "Kafka SOURCE Exactly-Once (PostgreSQL wire)" "test_kafka_exactly_once_source.py"
run_test "Kafka INSERT-Produce (PostgreSQL wire)" "test_kafka_insert_produce.py"
run_test "Kafka Continuous STREAM (PostgreSQL wire)" "test_kafka_stream.py"
run_test "Kafka STREAM Exactly-Once (PostgreSQL wire)" "test_kafka_exactly_once.py"
run_test "Kafka STREAM Write / INSERT VALUES (PostgreSQL wire)" "test_kafka_stream_write.py"
# NB: test_kafka_crash_recovery.py is deliberately NOT here — it is native-only
# (owns the server via Popen kill/restart). Its docker/CI variant is a separate task.

echo ""
echo "========================================="
echo "All tests completed!"
echo "========================================="

echo ""
echo "========================================="
echo "Test Summary: $PASSED passed, $FAILED failed"
echo "========================================="

if [ $FAILED -gt 0 ]; then
    exit 1
fi
