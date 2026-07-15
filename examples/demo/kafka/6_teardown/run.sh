#!/usr/bin/env bash
# STEP 6 — input validation + teardown. DROP joins every poller/stream worker
# and cleans the kafka.__sources persistence rows so a restart won't relaunch
# them; the backing kafka.<name> tables are dropped with the object.
cd "$(dirname "$0")"; source ../lib/_common.sh

title "STEP 6  ·  teardown"

title "6  ·  DROP — stop pollers/workers, clean kafka.__sources"
pause; psql_run 01_drop.sql
