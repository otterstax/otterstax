-- Step 3d: campaign attribution funnel across three backends.
-- mysql.bill (orders) → ch.ev (sessions) → pg.shop (products).
-- Filters on EU shipping country, campaign traffic and a populated channel.
SELECT p.category, COUNT(*) AS n
FROM (
    SELECT order_id, product_id
    FROM   mysql.bill.orders
    WHERE  ts >= '2026-04-12'
      AND  ts <  '2026-04-19'
) o
INNER JOIN pg.shop.products p ON p.product_id = o.product_id
INNER JOIN ch.ev.sessions  s ON s.order_id   = o.order_id
WHERE  (s.ship_addr).country IN ('DE','FR','IT','ES','NL')
  AND  s.traffic_source = 'campaign'
  AND  (s.props).channel IS NOT NULL
GROUP BY p.category;
