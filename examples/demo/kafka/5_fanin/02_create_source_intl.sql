-- Sub-step 5b — a second SOURCE over the international feed. Same schema as
-- orders_live; the fan-in query in 5c reads THIS source's topic.
\echo '>>> CREATE SOURCE orders_intl  (the 2nd feed)'

DROP SOURCE IF EXISTS orders_intl;

CREATE SOURCE orders_intl (
    event_id    VARCHAR,
    customer_id VARCHAR,
    amount      DOUBLE,
    qty         INT,
    status      VARCHAR,
    channel     VARCHAR
) WITH (KAFKA_TOPIC       = 'demo_orders_intl',
        VALUE_FORMAT      = 'JSON',
        BOOTSTRAP_SERVERS = :'broker',
        OFFSET_RESET      = 'earliest',
        TRANSACTIONAL     = true);
