-- Sub-step 5c — the fan-in. ksqlDB's second INSERT form: instead of one-shot
-- VALUES, INSERT INTO <stream> SELECT registers ANOTHER continuous query that
-- feeds an EXISTING stream. orders_paid now receives paid orders from BOTH the
-- main feed (via CREATE STREAM in step 4) and this international one — merged
-- onto one output topic.
--
--   CREATE STREAM orders_paid AS SELECT … FROM kafka.orders_live   (step 4)
--   INSERT INTO   orders_paid    SELECT … FROM kafka.orders_intl   (this)
\echo '>>> INSERT INTO kafka.orders_paid SELECT — fan-in a 2nd feed into the stream'

INSERT INTO kafka.orders_paid
    SELECT event_id, customer_id, amount, channel
    FROM   kafka.orders_intl
    WHERE  status = 'paid';
