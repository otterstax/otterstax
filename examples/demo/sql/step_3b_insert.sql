-- Step 3b: INSERT three warehouses with composite payload via ROW(...).
-- Cities chosen to match seeded customer addresses for step 4.
INSERT INTO otter.warehouses
       (warehouse_id, code, tier, location, spec, priority_high, priority_med, priority_low) VALUES
  ('11111111-1111-1111-1111-111111111111',
   'TLV-1',
   'gold',
   ROW('Tel Aviv', 'IL', '6701101'),
   ROW(10, 20, 30, 15, 25, 35, 'BC-001', 'BC-002', NULL),
   1, 2, 3),
  ('22222222-2222-2222-2222-222222222222',
   'BER-1',
   'silver',
   ROW('Berlin', 'DE', '10115'),
   ROW(5, 10, 15, 20, 25, 30, 'BC-003', 'BC-004', NULL),
   2, 3, 4),
  ('33333333-3333-3333-3333-333333333333',
   'NYC-1',
   'gold',
   ROW('New York', 'US', '10001'),
   ROW(1, 2, 3, 4, 5, 6, 'BC-005', 'BC-006', NULL),
   1, 1, 1);
