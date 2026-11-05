-- Step 1: Sanity + federation. Cross-source JOIN, time-bound derived table.
-- Backends: mysql.bill (orders), pg.shop (products).
SELECT p.category,
       COUNT(*)      AS n,
       SUM(o.amount) AS revenue
FROM (
    SELECT product_id, amount
    FROM   mysql.bill.orders
    WHERE  ts >= '2026-04-18'
      AND  ts <  '2026-04-19'
      AND  status IN ('paid','shipped')
) o
INNER JOIN pg.shop.products p ON p.product_id = o.product_id
GROUP BY p.category
ORDER BY revenue DESC
LIMIT 10;
