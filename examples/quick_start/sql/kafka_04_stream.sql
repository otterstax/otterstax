-- Kafka K4: a continuous stream. CREATE STREAM spawns a persistent worker that
-- consumes qs_orders, applies the SELECT (filter to paid) to every batch, and
-- produces the results to the qs_orders_paid topic — forever, exactly-once
-- (TRANSACTIONAL=true). Requires K1 (the qs_orders source).
DROP STREAM IF EXISTS qs_orders_paid;

CREATE STREAM qs_orders_paid
    WITH (KAFKA_TOPIC       = 'qs_orders_paid',
          VALUE_FORMAT      = 'JSON',
          BOOTSTRAP_SERVERS = 'qs-kafka:9092',
          OFFSET_RESET      = 'earliest',
          TRANSACTIONAL     = true)
    AS SELECT event_id, customer_id, amount, channel
       FROM   kafka.qs_orders
       WHERE  status = 'paid';
