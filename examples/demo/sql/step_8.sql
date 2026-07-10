-- Step 8: Load a Parquet file from S3 into the otterbrix engine.
--   Source: s3://demo-bucket/promos.parquet (seeded by `up.sh`).
--   region_id is an INT64 column in parquet; the engine surfaces it as BIGINT.
--
-- After this DDL, otter.promos joins to otter.regions on region_id (see step 9).
CREATE EXTERNAL TABLE otter.promos
    WITH (s3_alias = 'demo_s3',
          location = 's3://demo-bucket/promos.parquet',
          format   = 'parquet');
