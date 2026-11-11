-- Example 6: Load three S3 files (csv + parquet + ndjson) into the OtterStax
-- engine as internal tables under the schema `qs` of the local database
-- (`local.qs.regions`; `CREATE SCHEMA qs` always lands in `local`). CREATE EXTERNAL
-- TABLE names its table as CREATE TABLE does and never creates the schema, so it
-- is created first. The format is auto-detected from the file extension (no `format`
-- option needed). After this they behave like any CREATE TABLE table
-- (SELECT / JOIN / UPDATE / COPY work).
CREATE SCHEMA IF NOT EXISTS qs;
CREATE EXTERNAL TABLE local.qs.regions
    WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/regions.csv');

CREATE EXTERNAL TABLE local.qs.product_costs
    WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/product_costs.parquet');

CREATE EXTERNAL TABLE local.qs.promotions
    WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/promotions.ndjson');

-- Prove the load worked, and JOIN an s3-in-otterbrix dimension (local.qs.regions)
-- to a live backend (customers) — customers per region.
SELECT r.region_name, COUNT(*) AS customers
FROM   pgshop.shop.customers c
JOIN   local.qs.regions r ON r.country = c.country
GROUP BY r.region_name
ORDER BY customers DESC;
