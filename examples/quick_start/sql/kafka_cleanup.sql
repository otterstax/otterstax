-- Remove the SQL stream and source. Broker topics and group offsets stay in
-- place. For a clean K4 replay see the Quick Start README. DROP STREAM joins its
-- worker; DROP SOURCE stops its poller. Both remove their federation.kafka.__sources row so
-- a server restart won't relaunch them. IF EXISTS keeps it idempotent.
DROP STREAM IF EXISTS qs_orders_paid;
DROP SOURCE IF EXISTS qs_orders;
