#!/usr/bin/env bash
# Run the whole Kafka act end-to-end, non-interactively (no [Enter] pauses).
# For a live demo, prefer walking the steps one at a time:
#   examples/demo/kafka/1_ingestion/run.sh [--local]
#   examples/demo/kafka/2_join/run.sh      [--local]
#   … etc (steps are ordered and build on each other).
#
# Checks the otterstax PG wire first, stops at the first failing step and exits
# with that step's code; the ✅ banner is printed only when every step succeeded.
#
# Usage:
#   examples/demo/kafka/run_all.sh [--local]
set -uo pipefail
cd "$(dirname "$0")"
export NONINTERACTIVE=1
source lib/_common.sh

require_server || exit 1

for step in [0-9]*_*/; do
    bash "${step}run.sh" "$@" || {
        rc=$?
        echo
        echo "${BOLD}${RED}❌ Kafka act failed at step ${step%/} (exit ${rc}).${RESET}" >&2
        exit "${rc}"
    }
done

echo
echo "${BOLD}${GREEN}✅ Kafka act complete (all steps).${RESET}"
