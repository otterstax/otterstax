#!/usr/bin/env bash
# Sub-step 5a — bring up the 2nd feed: (re)create demo_orders_intl and produce
# the 25 international order events from init/kafka/orders_intl.ndjson.
cd "$(dirname "$0")"; source ../lib/_common.sh
seed --topic demo_orders_intl --fixture orders_intl.ndjson --reset
