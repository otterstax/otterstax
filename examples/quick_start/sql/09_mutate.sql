-- Example 9: Mutations on an engine table — INSERT / UPDATE / DROP.
-- Requires example 6 (for the `qs` database). A small scratch table shows the
-- full lifecycle; UPDATE echoes the changed rows with their new values.
-- (DELETE works the same way and reports "DELETE N"; the same statements also
--  work on the S3-loaded qs.* tables.)
CREATE TABLE qs.scratch (id bigint, label string, qty bigint);

INSERT INTO qs.scratch (id, label, qty) VALUES
  (1, 'alpha', 1), (2, 'beta', 2), (3, 'gamma', 3);

UPDATE qs.scratch SET qty = qty * 10 WHERE id <= 2;   -- echoes rows 1 & 2

SELECT * FROM qs.scratch ORDER BY id;

DROP TABLE qs.scratch;
