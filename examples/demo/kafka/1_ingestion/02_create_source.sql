-- Sub-step 1b — CREATE SOURCE: turn the Kafka topic into a queryable table.
--
-- A background poller consumes demo_orders_live and INSERTs each record into
-- kafka.orders_live. TRANSACTIONAL=true makes ingestion exactly-once: the row
-- INSERT and the offset advance commit atomically in one engine transaction,
-- with the offsets table (not the broker group) as the source of truth, so a
-- crash mid-batch never duplicates or drops a record.
--
-- :broker is supplied by the step runner (demo-kafka:9092 in docker mode,
-- 127.0.0.1:19093 with --local). DROP IF EXISTS keeps the step re-runnable.
\echo '>>> CREATE SOURCE orders_live  (Kafka topic → SQL table, exactly-once)'

DROP SOURCE IF EXISTS orders_live;

CREATE SOURCE orders_live (
    event_id    VARCHAR,
    customer_id VARCHAR,
    amount      DOUBLE,
    qty         INT,
    status      VARCHAR,
    channel     VARCHAR
) WITH (KAFKA_TOPIC       = 'demo_orders_live',
        VALUE_FORMAT      = 'JSON',
        BOOTSTRAP_SERVERS = :'broker',
        OFFSET_RESET      = 'earliest',
        TRANSACTIONAL     = true);
