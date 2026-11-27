-- The app's own table, created on the PostgreSQL backend itself (not through OtterStax): the backend
-- owns its schema, and OtterStax refuses keys, defaults and serial in a CREATE TABLE of a backend
-- (docs/integration.md, section 7). Run once against the quick start's pgshop database:
--   docker exec -i qs-postgres-shop psql -U demo shop < setup_backend.sql
CREATE TABLE IF NOT EXISTS shop.app_notes (
    id          serial PRIMARY KEY,
    customer_id text   NOT NULL,
    body        text   NOT NULL
);
