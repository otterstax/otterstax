-- Step 2: Derived aggregates + HAVING via alias + outer NOT LIKE.
-- Backends: mysql.bill (orders), pg.shop (customers).
SELECT c.name,
       c.email,
       agg.orders_cnt,
       agg.avg_check,
       agg.min_check,
       agg.max_check
FROM (
    SELECT o.customer_id,
           COUNT(DISTINCT o.order_id) AS orders_cnt,
           AVG(o.amount)        AS avg_check,
           MIN(o.amount)        AS min_check,
           MAX(o.amount)        AS max_check
    FROM   mysql.bill.orders o
    WHERE  o.ts >= '2026-03-20'
      AND  o.status = 'paid'
    GROUP BY o.customer_id
    HAVING orders_cnt >= 3
) agg
INNER JOIN pg.shop.customers c ON c.customer_id = agg.customer_id
WHERE c.email NOT LIKE '%@test.%'
ORDER BY agg.avg_check DESC
LIMIT 20;
