-- Full reset of local.qs before replaying examples 6–9: the one CASCADE
-- statement, as documented in examples/quick_start/README.md. It removes every
-- object in the schema, including user-created objects, and dependent objects
-- in other schemas (a view reading a local.qs table is dropped with it).
-- To remove only the three demo tables, use the three DROP TABLE commands
-- from the README instead. Backend data and S3 objects are unaffected.
DROP SCHEMA IF EXISTS qs CASCADE;
