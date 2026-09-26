-- Example 5: Complex 4-backend JOIN — adds ClickHouse.
-- pageviews (ClickHouse) ⋈ orders (MariaDB) ⋈ customers (PG #1) ⋈ products (PG #2).
-- Clickstream joined to actual purchases (on customer_id AND product_id),
-- aggregated by channel and category.
SELECT v.channel, p.category,
       COUNT(DISTINCT o.order_id) AS orders_cnt,
       SUM(o.amount)              AS revenue
FROM   web.analytics.pageviews v
JOIN   sales.ops.orders     o  ON o.customer_id = v.customer_id
                              AND o.product_id  = v.product_id
JOIN   pgshop.shop.customers c ON c.customer_id = o.customer_id
JOIN   pgcat.catalog.products p ON p.product_id = o.product_id
WHERE  o.status = 'paid'
GROUP BY v.channel, p.category
ORDER BY revenue DESC
LIMIT 10;
