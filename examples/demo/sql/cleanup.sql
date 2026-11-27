-- Full reset of local.otter. CASCADE can also remove dependents in other schemas.
-- Keep the step-3a types for reuse. See Re-running in examples/demo/CLAUDE.md.

DROP SCHEMA IF EXISTS otter CASCADE;
