-- Step 3c: SELECT with (struct).field + IS NULL on local table.
-- Demonstrates struct field access and nested NULL check.
SELECT warehouse_id,
       (location).city,
       (spec).weight_2,
       (spec).primary_barcode,
       priority_med
FROM   otter.warehouses
WHERE  (location).country = 'IL'
  AND  (spec).photo IS NULL;
