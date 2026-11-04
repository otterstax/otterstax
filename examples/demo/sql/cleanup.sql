-- Reset state from a previous demo run. Drops the local schema `otter` of the
-- session's database `main` (the warehouses of step 3a and the S3-loaded
-- external tables created in steps 7-8) and the types of step 3a.

DROP TABLE IF EXISTS otter.promos_by_region_rt;
DROP TABLE IF EXISTS otter.regions;
DROP TABLE IF EXISTS otter.promos;
DROP TABLE IF EXISTS otter.warehouses;
DROP SCHEMA IF EXISTS otter CASCADE;
DROP TYPE IF EXISTS spec_t;
DROP TYPE IF EXISTS address_t;
DROP TYPE IF EXISTS tier_t;
