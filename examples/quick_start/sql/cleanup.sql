-- Reset engine state from a previous run so examples 7-10 can be replayed.
-- Drops the S3-loaded tables and the local `qs` database. Backend data
-- (customers/products/orders/pageviews) and the S3 files are untouched.
DROP TABLE IF EXISTS qs.regions;
DROP TABLE IF EXISTS qs.product_costs;
DROP TABLE IF EXISTS qs.promotions;
DROP DATABASE IF EXISTS qs;
