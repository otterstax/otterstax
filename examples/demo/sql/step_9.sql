-- Step 9: Pure otterbrix JOIN across two S3-sourced tables, then export the
-- result back to S3 as CSV.
--   Inputs:  local.otter.regions (csv, from step 7) ⋈ local.otter.promos (parquet, step 8)
--   Output:  s3://demo-bucket/exports/promos_by_region.csv
--
-- Re-read the dump with:
--   CREATE EXTERNAL TABLE local.otter.promos_by_region_rt
--       WITH (s3_alias='demo_s3',
--             location='s3://demo-bucket/exports/promos_by_region.csv',
--             format='csv');
--   SELECT * FROM local.otter.promos_by_region_rt;
COPY (
    SELECT r.country,
           r.region_name,
           p.promo_code,
           p.discount_pct
    FROM   local.otter.regions r
    JOIN   local.otter.promos  p ON p.region_id = r.region_id
    ORDER  BY r.country, p.discount_pct DESC
) TO 's3://demo-bucket/exports/promos_by_region.csv'
    WITH (s3_alias = 'demo_s3', format = 'csv');
