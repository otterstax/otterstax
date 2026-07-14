#!/usr/bin/env bash
# Sub-step 4a — (re)create the stream's output topic so a re-run starts clean.
# No fixture: the stream worker populates it.
cd "$(dirname "$0")"; source ../lib/_common.sh
seed --topic demo_orders_paid --reset
