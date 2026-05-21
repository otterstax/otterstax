-- Step 3a: ENUM + composite types + a local table — fully on the engine,
-- no external backends needed.
CREATE DATABASE otter;

CREATE TYPE tier_t AS ENUM('bronze','silver','gold');

CREATE TYPE address_t AS (
  city    STRING,
  country STRING,
  zip     STRING
);

CREATE TYPE spec_t AS (
  weight_1          INT,
  weight_2          INT,
  weight_3          INT,
  weight_4          INT,
  weight_5          INT,
  weight_6          INT,
  primary_barcode   STRING,
  secondary_barcode STRING,
  photo             STRING
);

CREATE TABLE otter.warehouses (
  warehouse_id  STRING,
  code          STRING,        -- e.g. "TLV-1" / "BER-1" / "NYC-1"
  tier          tier_t,
  location      address_t,
  spec          spec_t,
  priority_high BIGINT,
  priority_med  BIGINT,
  priority_low  BIGINT
);
