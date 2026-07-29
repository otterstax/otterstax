-- Step 7: Load a CSV from S3 into the otterbrix engine.
--   Source: s3://demo-bucket/regions.csv (seeded into MinIO by `up.sh`).
--   The s3_alias 'demo_s3' is registered at server startup from the `s3:` section
--   of the config file (config.yaml in docker mode / config_local.yaml with
--   --local, where the endpoint points at host:3206).
--
-- After this DDL, otter.regions is a normal engine-internal table. Inspect with:
--   SELECT * FROM otter.regions ORDER BY region_id LIMIT 10;
CREATE EXTERNAL TABLE otter.regions
    WITH (s3_alias = 'demo_s3',
          location = 's3://demo-bucket/regions.csv',
          format   = 'csv');
