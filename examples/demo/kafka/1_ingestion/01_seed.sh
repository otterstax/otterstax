#!/usr/bin/env bash
# Sub-step 1a — bring up the topic: (re)create demo_orders_live and produce the
# 40 live order events from examples/demo/init/kafka/orders_live.ndjson.
cd "$(dirname "$0")"; source ../lib/_common.sh
seed --topic demo_orders_live --fixture orders_live.ndjson --reset
