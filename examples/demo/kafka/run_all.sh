#!/usr/bin/env bash
# Run the whole Kafka act end-to-end, non-interactively (no [Enter] pauses).
# For a live demo, prefer walking the steps one at a time:
#   examples/demo/kafka/1_ingestion/run.sh [--local]
#   examples/demo/kafka/2_join/run.sh      [--local]
#   … etc (steps are ordered and build on each other).
#
# Usage:
#   examples/demo/kafka/run_all.sh [--local]
set -uo pipefail
cd "$(dirname "$0")"
export NONINTERACTIVE=1

for step in [0-9]*_*/; do
    bash "${step}run.sh" "$@"
done

echo
echo -e "\033[92m\033[1m✅ Kafka act complete (all steps).\033[0m"
