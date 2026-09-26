-- Kafka K1: turn the Kafka topic `qs_orders` into a queryable SQL table.
-- A background poller consumes the topic and materialises each record into
-- kafka.qs_orders. TRANSACTIONAL=true makes ingestion exactly-once. The broker
-- is the in-network redpanda listener (qs-kafka:9092). DROP IF EXISTS keeps it
-- re-runnable.
--
-- Ingestion is asynchronous: after this runs, wait a few seconds, then query
-- kafka.qs_orders (see the Kafka section in README.md).
DROP SOURCE IF EXISTS qs_orders;

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
