#!/usr/bin/env bash
# Sub-step 5d — the output topic now carries BOTH feeds (main stream + fan-in).
# Read it back: the count is the union of both feeds' paid events.
cd "$(dirname "$0")"; source ../lib/_common.sh
consume --topic demo_orders_paid --timeout 12
