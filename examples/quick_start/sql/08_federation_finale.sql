-- Example 9: Federation finale — all four live backends + an S3-loaded engine
-- table in a single statement:
--   orders (MariaDB) ⋈ customers (PostgreSQL #1) ⋈ products (PostgreSQL #2)
--   ⋈ pageviews (ClickHouse) ⋈ product_costs (S3 parquet, loaded in example 6).
-- Gross margin (revenue - unit cost) per country, category and channel.
-- Requires example 6 to have run first.
SELECT c.country, p.category, v.channel,
       COUNT(DISTINCT o.order_id)     AS orders_cnt,
       SUM(o.amount)                  AS revenue,
       SUM(o.amount - pcst.unit_cost) AS gross_margin
FROM   sales.ops.orders       o
JOIN   pgshop.shop.customers  c    ON c.customer_id   = o.customer_id
JOIN   pgcat.catalog.products p    ON p.product_id    = o.product_id
JOIN   web.analytics.pageviews v   ON v.customer_id   = o.customer_id
                                  AND v.product_id    = o.product_id
JOIN   qs.product_costs       pcst ON pcst.product_id = o.product_id
WHERE  o.status = 'paid'
GROUP BY c.country, p.category, v.channel
ORDER BY gross_margin DESC
LIMIT 10;
