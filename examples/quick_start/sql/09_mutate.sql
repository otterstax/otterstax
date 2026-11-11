-- Example 9: Mutations on an engine table — INSERT / UPDATE / DROP.
-- Requires example 6 (for the `qs` schema). A small scratch table shows the
-- full lifecycle; UPDATE reports the number of rows it changed ("UPDATE 2").
-- (DELETE works the same way and reports "DELETE N"; the same statements also
--  work on the S3-loaded local.qs.* tables.)
CREATE TABLE local.qs.scratch (id bigint, label string, qty bigint);

INSERT INTO local.qs.scratch (id, label, qty) VALUES
  (1, 'alpha', 1), (2, 'beta', 2), (3, 'gamma', 3);

UPDATE local.qs.scratch SET qty = qty * 10 WHERE id <= 2;   -- rows 1 & 2

SELECT * FROM local.qs.scratch ORDER BY id;

DROP TABLE local.qs.scratch;
