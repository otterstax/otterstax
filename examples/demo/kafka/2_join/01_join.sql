-- Sub-step 2 — the federated join. One statement, three systems:
--
--   kafka.orders_live   (engine-internal, fed from Kafka in real time)
--     ⋈ pg.shop.customers   (PostgreSQL — customer reference, ENUM tier)
--     ⋈ ch.ev.sessions      (ClickHouse — session analytics)
--
-- The "backend ⋈ otterbrix-internal" shape step_4 proves, extended to two
-- backends. The join key is the customer UUID as a string on every side
-- (string keys avoid the int32/int64 width footgun in the top-level CLAUDE.md).
\echo '>>> JOIN: live Kafka orders  ⋈  ClickHouse sessions  ⋈  Postgres customers'

SELECT c.name,
       c.tier,
       c.addr_country                 AS country,
       k.amount,
       k.status,
       k.channel                      AS order_channel,
       s.browsed_category,
       s.traffic_source
FROM   kafka.orders_live k
INNER JOIN pg.shop.customers c
        ON c.customer_id = k.customer_id
INNER JOIN (
    SELECT user_id, browsed_category, traffic_source
    FROM   ch.ev.sessions
) s ON s.user_id = k.customer_id
WHERE  k.status = 'paid'
ORDER  BY k.amount DESC
LIMIT  15;
