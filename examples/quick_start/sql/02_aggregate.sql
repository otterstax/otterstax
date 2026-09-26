-- Example 2: Aggregate against a single backend (MariaDB). GROUP BY + SUM.
SELECT status, COUNT(*) AS n, SUM(amount) AS total
FROM   sales.ops.orders
GROUP BY status
ORDER BY total DESC;
