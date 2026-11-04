-- Kafka K1: turn the Kafka topic `qs_orders` into a queryable SQL table.
-- A background poller consumes the topic and materialises each record into
-- kafka.qs_orders. TRANSACTIONAL=true makes ingestion exactly-once. The broker
-- is the in-network redpanda listener (qs-kafka:9092). DROP IF EXISTS keeps it
-- re-runnable; CASCADE also drops the K4 stream reading it, which a plain
-- (RESTRICT) DROP SOURCE would refuse over (re-run K4 afterwards).
--
-- Ingestion is asynchronous: after this runs, wait a few seconds, then query
-- kafka.qs_orders (see the Kafka section in README.md).
DROP SOURCE IF EXISTS qs_orders CASCADE;

CREATE SOURCE qs_orders (
    event_id    VARCHAR,
    customer_id VARCHAR,
    product_id  VARCHAR,
    amount      DOUBLE,
    status      VARCHAR,
    channel     VARCHAR
) WITH (KAFKA_TOPIC       = 'qs_orders',
        VALUE_FORMAT      = 'JSON',
        BOOTSTRAP_SERVERS = 'qs-kafka:9092',
        OFFSET_RESET      = 'earliest',
        TRANSACTIONAL     = true);
