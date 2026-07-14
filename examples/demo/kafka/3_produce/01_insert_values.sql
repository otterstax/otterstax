-- Sub-step 3a — INSERT INTO a kafka object produces to its topic. There is no
-- engine staging table for writes: the rows go to the Kafka log (durability =
-- the log itself), in one Kafka transaction. channel='api' tags them so the
-- round-trip in 3b is easy to spot.
\echo '>>> INSERT INTO kafka.orders_live VALUES — produce events straight from SQL'

INSERT INTO kafka.orders_live (event_id, customer_id, amount, qty, status, channel)
VALUES
  ('demo-sql-0001', 'sql-writer', 199.99, 2, 'paid',    'api'),
  ('demo-sql-0002', 'sql-writer',  49.50, 1, 'pending', 'api');
