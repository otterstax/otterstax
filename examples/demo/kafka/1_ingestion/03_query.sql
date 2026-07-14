-- Sub-step 1c — the topic is now a normal SQL table: COUNT, GROUP BY, aggregates
-- all work over the poller-materialised kafka.orders_live.
\echo '>>> kafka.orders_live — a Kafka topic queried as a SQL table'

SELECT count(*) AS events_ingested FROM kafka.orders_live;

SELECT status,
       count(*)     AS n,
       sum(amount)  AS total_amount,
       avg(amount)  AS avg_amount
FROM   kafka.orders_live
GROUP  BY status
ORDER  BY n DESC;
