#!/usr/bin/env bash
# STEP 2 — the selling point: one SQL query joins the LIVE Kafka feed against
# ClickHouse OLAP history and a Postgres reference table. Needs STEP 1's
# orders_live source and the pg/ch connections (add_connections.sh).
cd "$(dirname "$0")"; source ../lib/_common.sh

title "STEP 2  ·  federated JOIN — Kafka ⋈ ClickHouse ⋈ Postgres (one query)"
pause; psql_run 01_join.sql
