-- Step 5: browsed-vs-bought correlation across four sources.
-- Find customers whose campaign-session category matches what they
-- actually purchased. DISTINCT + nested struct projection (`((s.props).geo).ip`).
-- pg.shop.shop (customers, products) × ch.ev.public (sessions) × mysql.bill.public (orders).
SELECT DISTINCT
       c.name,
       s.browsed_category AS browsed_cat,
       p.category         AS bought_cat,
       ((s.props).geo).ip AS src_ip
FROM   pg.shop.shop.customers c
INNER JOIN (
    SELECT user_id, browsed_category, traffic_source, props
    FROM   ch.ev.public.sessions
    WHERE  ts >= '2026-04-12'
) s ON s.user_id = c.customer_id
INNER JOIN (
    SELECT customer_id, product_id
    FROM   mysql.bill.public.orders
    WHERE  ts >= '2026-04-12'
) o ON o.customer_id = c.customer_id
INNER JOIN pg.shop.shop.products p ON p.product_id = o.product_id
WHERE  s.browsed_category = p.category
  AND  s.traffic_source   = 'campaign'
ORDER BY s.browsed_category;
