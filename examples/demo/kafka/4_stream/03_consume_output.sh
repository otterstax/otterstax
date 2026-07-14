#!/usr/bin/env bash
# Sub-step 4c — the stream's results live on Kafka, so read them back with a
# plain read_committed consumer, exactly as any downstream Kafka client would.
cd "$(dirname "$0")"; source ../lib/_common.sh
consume --topic demo_orders_paid --timeout 12
