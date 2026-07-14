#!/usr/bin/env bash
# STEP 1 — a Kafka topic becomes a queryable SQL table (exactly-once ingestion).
# Sub-steps: 01 bring up + seed the topic, 02 CREATE SOURCE, 03 query the table.
cd "$(dirname "$0")"; source ../lib/_common.sh

title "STEP 1  ·  Kafka topic  →  SQL table  (exactly-once ingestion)"

title "1a  ·  bring up the topic — produce 40 live order events"
pause; bash 01_seed.sh

title "1b  ·  CREATE SOURCE — the topic is now a SQL table (TRANSACTIONAL=true)"
pause; psql_run 02_create_source.sql
echo -e "${DIM}    waiting for the poller to ingest…${RESET}"
wait_rows kafka.orders_live 40

title "1c  ·  query the Kafka-fed table (COUNT + GROUP BY)"
pause; psql_run 03_query.sql
