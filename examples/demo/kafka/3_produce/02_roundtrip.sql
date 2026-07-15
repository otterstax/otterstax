-- Sub-step 3b — the rows we INSERTed were produced to the topic and the poller
-- ingested them straight back: the write path round-trips through Kafka.
\echo '>>> round-trip: the SQL-produced events came back through the topic'

SELECT event_id, customer_id, amount, status, channel
FROM   kafka.orders_live
WHERE  channel = 'api'
ORDER  BY event_id;
