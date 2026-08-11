-- Example 6: Load three S3 files (csv + parquet + ndjson) into the OtterStax
-- engine as internal tables under the `qs` database. The `qs` database is
-- auto-created by the first CREATE EXTERNAL TABLE, and the format is
-- auto-detected from the file extension (no `format` option needed). After this
-- they behave like any CREATE TABLE table (SELECT / JOIN / UPDATE / COPY work).
CREATE EXTERNAL TABLE qs.regions
    WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/regions.csv');

CREATE EXTERNAL TABLE qs.product_costs
    WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/product_costs.parquet');

CREATE EXTERNAL TABLE qs.promotions
    WITH (s3_alias = 'qs_s3', location = 's3://quickstart-bucket/promotions.ndjson');

-- Prove the load worked, and JOIN an s3-in-otterbrix dimension (qs.regions)
-- to a live backend (customers) — customers per region.
SELECT r.region_name, COUNT(*) AS customers
FROM   pgshop.shop.customers c
JOIN   qs.regions r ON r.country = c.country
GROUP BY r.region_name
ORDER BY customers DESC;
