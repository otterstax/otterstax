-- Kafka K3: produce to the topic straight from SQL. INSERT INTO a kafka object
-- writes the rows to the Kafka log (in one Kafka transaction); the poller then
-- ingests them straight back, so the write path round-trips through Kafka.
-- channel='api' tags them so the round-trip is easy to spot.
-- Requires K1 (the qs_orders source).
INSERT INTO kafka.qs_orders (event_id, customer_id, product_id, amount, status, channel)
VALUES ('E9001', 'C0001', 'P0001', 199.99, 'paid',    'api'),
       ('E9002', 'C0002', 'P0002',  49.50, 'pending', 'api');
