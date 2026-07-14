-- Sub-step 4b — CREATE STREAM spawns a persistent worker: it consumes the
-- source topic, applies the SELECT (filter + projection) to every batch, and
-- produces the results to demo_orders_paid. It runs forever, transforming new
-- records as they arrive.
--
-- TRANSACTIONAL=true = exactly-once: each batch's produce + source-offset
-- commit is one Kafka read-process-write transaction, so a read_committed
-- consumer of the output never sees duplicates or partial batches.
\echo '>>> CREATE STREAM orders_paid  (continuous, exactly-once transform)'

DROP STREAM IF EXISTS orders_paid;

CREATE STREAM orders_paid
    WITH (KAFKA_TOPIC       = 'demo_orders_paid',
          VALUE_FORMAT      = 'JSON',
          BOOTSTRAP_SERVERS = :'broker',
          OFFSET_RESET      = 'earliest',
          TRANSACTIONAL     = true)
    AS SELECT event_id, customer_id, amount, channel
       FROM   kafka.orders_live
       WHERE  status = 'paid';
