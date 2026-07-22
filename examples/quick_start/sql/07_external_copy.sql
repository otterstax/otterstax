-- Example 8: Export a JOIN result back to S3 (COPY ... TO).
-- Joins the two S3-sourced engine tables and writes the result as CSV to
-- s3://quickstart-bucket/exports/promo_costs.csv (browse it in the MinIO
-- console afterwards). Requires example 6 to have run first.
COPY (
    SELECT pc.product_id, pc.unit_cost, pr.promo_code, pr.discount_pct
    FROM   qs.product_costs pc
    JOIN   qs.promotions   pr ON pr.product_id = pc.product_id
    ORDER  BY pc.product_id
) TO 's3://quickstart-bucket/exports/promo_costs.csv'
    WITH (s3_alias = 'qs_s3', format = 'csv');
