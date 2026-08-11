-- Kafka K2: the federated JOIN — live Kafka events joined to reference data in
-- BOTH PostgreSQL backends in one statement:
--   kafka.qs_orders ⋈ pgshop.shop.customers ⋈ pgcat.catalog.products
-- Shows each live order with its customer's country/tier (PG #1) and the
-- product's category (PG #2). Requires K1 (the qs_orders source) to have ingested.
-- (Note: `customers` and `products` both have a `name` column; referencing `name`
--  in this mixed 3-way join trips column resolution, so we project other columns.)
SELECT o.event_id, c.country, c.tier, p.category, o.amount
FROM   kafka.qs_orders o
JOIN   pgshop.shop.customers  c ON c.customer_id = o.customer_id
JOIN   pgcat.catalog.products p ON p.product_id  = o.product_id
WHERE  o.status = 'paid'
ORDER  BY o.amount DESC
LIMIT  10;
