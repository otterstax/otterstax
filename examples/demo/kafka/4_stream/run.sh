#!/usr/bin/env bash
# STEP 4 — a continuous, ksqlDB-style STREAM: consume the source topic, apply a
# SELECT (filter + projection) to every batch, produce to an output topic —
# exactly-once. Needs STEP 1's orders_live source (it reads that topic).
cd "$(dirname "$0")"; source ../lib/_common.sh

title "STEP 4  ·  continuous STREAM (exactly-once transform to a topic)"

title "4a  ·  (re)create the stream's output topic demo_orders_paid"
pause; bash 01_seed_output.sh

title "4b  ·  CREATE STREAM orders_paid AS SELECT … WHERE status='paid'"
pause; psql_run 02_create_stream.sql
echo -e "${DIM}    letting the stream worker process the backlog…${RESET}"
sleep 6

title "4c  ·  consume the stream's output topic (only 'paid' events)"
pause; bash 03_consume_output.sh
