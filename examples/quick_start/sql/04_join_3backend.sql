-- Example 4: 3-backend JOIN across BOTH PostgreSQL backends + MariaDB.
-- orders (MariaDB) ⋈ customers (PG #1) ⋈ products (PG #2).
-- Revenue by product category and customer country.
SELECT p.category, c.country, SUM(o.amount) AS revenue
FROM   sales.ops.public.orders o
JOIN   pgshop.shop.shop.customers c  ON c.customer_id = o.customer_id
JOIN   pgcat.catalog.catalog.products p ON p.product_id  = o.product_id
WHERE  o.status IN ('paid', 'shipped')
GROUP BY p.category, c.country
ORDER BY revenue DESC
LIMIT 10;
