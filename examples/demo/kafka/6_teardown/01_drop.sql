-- Sub-step 6b — teardown. DROP STREAM joins its worker (and every fan-in
-- feeder); DROP SOURCE stops its poller. Both delete their kafka.__sources
-- row so a server restart won't relaunch them. IF EXISTS keeps it idempotent.
\echo '>>> DROP — stop the stream + sources'

DROP STREAM IF EXISTS orders_paid;
DROP SOURCE IF EXISTS orders_live;
DROP SOURCE IF EXISTS orders_intl;
