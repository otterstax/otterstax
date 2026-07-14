#!/usr/bin/env bash
# STEP 3 — write path: INSERT INTO a kafka object PRODUCES to its topic; the
# source's poller re-ingests, so a follow-up SELECT sees the rows round-trip.
# Needs STEP 1's orders_live source.
cd "$(dirname "$0")"; source ../lib/_common.sh

title "STEP 3  ·  write path — INSERT INTO produces to the topic"

title "3a  ·  INSERT INTO kafka.orders_live VALUES (produce from SQL)"
pause; psql_run 01_insert_values.sql
echo -e "${DIM}    waiting for the round-trip through the topic…${RESET}"
wait_rows "kafka.orders_live WHERE channel = 'api'" 2 20

title "3b  ·  round-trip — the produced rows came back through the topic"
pause; psql_run 02_roundtrip.sql
