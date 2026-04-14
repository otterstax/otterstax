#!/bin/bash
# Initialize LocalStack S3 with test data for S3 backend tests

AWS_ENDPOINT="http://localstack:4566"

# Create bucket
aws --endpoint-url=$AWS_ENDPOINT s3 mb s3://test-bucket

# Upload test data files
aws --endpoint-url=$AWS_ENDPOINT s3 cp /data/test_data/sample.parquet s3://test-bucket/data/events.parquet
aws --endpoint-url=$AWS_ENDPOINT s3 cp /data/test_data/sample.csv s3://test-bucket/data/users.csv
aws --endpoint-url=$AWS_ENDPOINT s3 cp /data/test_data/sample.ndjson s3://test-bucket/data/logs.ndjson

echo "S3 test data uploaded to LocalStack"
