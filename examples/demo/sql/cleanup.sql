-- Reset state from a previous demo run. Drops the local `otter` database
-- (including the S3-loaded external tables created in steps 7-8).

DROP TABLE IF EXISTS otter.promos_by_region_rt;
DROP TABLE IF EXISTS otter.regions;
DROP TABLE IF EXISTS otter.promos;
DROP DATABASE IF EXISTS otter;
