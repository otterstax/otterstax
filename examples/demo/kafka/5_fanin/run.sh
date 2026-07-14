#!/usr/bin/env bash
# STEP 5 — fan-in / union: INSERT INTO <stream> SELECT registers ANOTHER
# continuous query feeding an EXISTING stream. orders_paid ends up carrying paid
# orders from BOTH feeds. Needs STEP 4's orders_paid stream.
cd "$(dirname "$0")"; source ../lib/_common.sh

title "STEP 5  ·  fan-in / union — INSERT INTO <stream> SELECT"

title "5a  ·  bring up the 2nd feed topic — produce 25 international events"
pause; bash 01_seed_intl.sh

title "5b  ·  CREATE SOURCE orders_intl over the 2nd feed"
pause; psql_run 02_create_source_intl.sql
# The fan-in query below reads orders_intl's TOPIC directly (a continuous
# query), so it does not depend on the source table filling first — just let
# the source register.
sleep 2

title "5c  ·  INSERT INTO kafka.orders_paid SELECT … FROM kafka.orders_intl"
pause; psql_run 03_fanin_insert.sql
echo -e "${DIM}    letting the fan-in query drain the 2nd feed…${RESET}"
sleep 6

title "5d  ·  consume — the output topic now carries BOTH feeds"
pause; bash 04_consume_union.sh
