-- Example 3: 2-backend JOIN — orders (MariaDB) ⋈ customers (PostgreSQL #1).
-- Top-spending customers on paid orders.
SELECT c.name, c.country, COUNT(*) AS orders_cnt, SUM(o.amount) AS spend
FROM   sales.ops.orders o
JOIN   pgshop.shop.customers c ON c.customer_id = o.customer_id
WHERE  o.status = 'paid'
GROUP BY c.name, c.country
ORDER BY spend DESC
LIMIT 10;
