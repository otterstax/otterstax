-- Step 7: Load a CSV from S3 into the otterbrix engine.
--   Source: s3://demo-bucket/regions.csv (seeded into MinIO by `up.sh`).
--   The s3_alias 'demo_s3' must be registered first via
--   examples/demo/connections/add_s3_credentials.sh (run automatically by up.sh in
--   full mode; in --local bench mode add `--local` so it points at host:3206).
--
-- After this DDL, otter.regions is a normal engine-internal table. Inspect with:
--   SELECT * FROM otter.regions ORDER BY region_id LIMIT 10;
CREATE EXTERNAL TABLE otter.regions
    WITH (s3_alias = 'demo_s3',
          location = 's3://demo-bucket/regions.csv',
          format   = 'csv');
