-- Example 1: Simple SELECT with filters against a single backend (PostgreSQL #2).
-- Premium products priced over 485, excluding the Clothing category.
SELECT product_id, name, category, price
FROM   pgcat.catalog.products
WHERE  price > 485
  AND  category <> 'Clothing'
ORDER BY price DESC;
