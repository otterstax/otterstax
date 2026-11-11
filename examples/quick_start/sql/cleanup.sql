-- Reset engine state from a previous run so examples 7-10 can be replayed.
-- Drops the S3-loaded tables and the local schema `qs` (`local.qs`). Backend data
-- (customers/products/orders/pageviews) and the S3 files are untouched.
DROP TABLE IF EXISTS local.qs.regions;
DROP TABLE IF EXISTS local.qs.product_costs;
DROP TABLE IF EXISTS local.qs.promotions;
DROP SCHEMA IF EXISTS qs CASCADE;
