-- Step 6: Federated MRR. Three backends, LEFT JOIN, CASE WHEN inside SUM,
-- HAVING via alias.
-- Backends: mysql.bill (invoices), pg.shop (customers), ch.ev (sessions).
SELECT c.tier,
       SUM(CASE WHEN i.status = 'paid' THEN i.amount ELSE 0 END) AS mrr,
       COUNT(DISTINCT s.user_id)                                 AS active_users,
       MIN(i.amount)                                             AS min_invoice,
       MAX(i.amount)                                             AS max_invoice
FROM (
    SELECT customer_id, amount, status
    FROM   mysql.bill.invoices
    WHERE  ts >= '2026-03-20'
) i
INNER JOIN pg.shop.customers c ON c.customer_id = i.customer_id
LEFT  JOIN (
    SELECT user_id
    FROM   ch.ev.sessions
    WHERE  ts >= '2026-03-20'
) s ON s.user_id = c.customer_id
GROUP BY c.tier
HAVING mrr > 1000
ORDER BY mrr DESC;
