-- Stop the Kafka stream + source so K1–K4 can be replayed. DROP STREAM joins its
-- worker; DROP SOURCE stops its poller. Both remove their kafka.__sources row so
-- a server restart won't relaunch them. IF EXISTS keeps it idempotent.
DROP STREAM IF EXISTS qs_orders_paid;
DROP SOURCE IF EXISTS qs_orders;
