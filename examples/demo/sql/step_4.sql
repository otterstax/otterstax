-- Step 4: gold customers paired with the warehouse in their city.
-- pg.shop.customers (PG ENUM `tier`) × local otter.warehouses (composite
-- `location`). ENUM cast in WHERE, struct field access on the JOIN key
-- and projection.
SELECT c.name,
       c.tier,
       c.addr_city            AS customer_city,
       c.addr_country         AS customer_country,
       w.code                 AS warehouse,
       (w.location).city      AS warehouse_city,
       (w.location).country   AS warehouse_country,
       (w.location).zip       AS warehouse_zip
FROM   pg.shop.customers c
INNER JOIN otter.warehouses w
        ON c.addr_city = (w.location).city
WHERE  c.tier = 'gold'::tier_t
  AND  (w.location).country IN ('DE','IL','US')
ORDER BY c.addr_city;
