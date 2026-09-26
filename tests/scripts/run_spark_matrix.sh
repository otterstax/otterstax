#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# PySpark client compatibility matrix (image: Dockerfile.spark-test). Runs every
# test_spark_client_*.py under every client in /opt/pyspark/manifest against the
# Spark Connect frontend on test-otterstax:15002. SPARK_MATRIX_JOBS client
# versions run at once (default 4); a version runs its test files one after
# another. Writes /app/logs/pyspark-<version>.log per version, prints the tail of
# every failed run and a version x file PASS/FAIL table, exits 1 on any failure.

set -uo pipefail

JOBS=${SPARK_MATRIX_JOBS:-4}
MANIFEST=/opt/pyspark/manifest
LOG_DIR=/app/logs
SERVER=test-otterstax
PORT=15002

cd /app
TESTS=(test_spark_client_*.py)
RESULTS=$(mktemp -d)

if [ ! -s "$MANIFEST" ]; then
    echo "❌ No PySpark clients listed in $MANIFEST"
    exit 1
fi

./wait_for_otterstax.sh || exit 1
mkdir -p "$LOG_DIR"

# Runs every test file under one client and records, per file, its status
# (PASS/FAIL) and output in $RESULTS/<version>.<file>.{status,out}.
run_version() {
    local version=$1 python=$2 test out status passed=0
    local log="$LOG_DIR/pyspark-$version.log"
    : > "$log"
    for test in "${TESTS[@]}"; do
        out="$RESULTS/$version.$test.out"
        # A client retries an unreachable server for ~10 minutes per call: once
        # the server is gone, the remaining runs fail at once instead.
        if ! timeout 2 bash -c "</dev/tcp/$SERVER/$PORT" 2>/dev/null; then
            echo "Spark Connect server $SERVER:$PORT is unreachable; not run" > "$out"
            status=FAIL
        elif PYTHONPATH="/opt/pyspark/$version" "$python" "$test" > "$out" 2>&1; then
            status=PASS
            passed=$((passed + 1))
        else
            status=FAIL
        fi
        echo "$status" > "$RESULTS/$version.$test.status"
        { echo "===== $test | pyspark $version | $python | $status"; cat "$out"; echo; } >> "$log"
    done
    echo "pyspark $version: $passed/${#TESTS[@]} passed"
}

echo "Spark Connect client matrix: $(wc -l < "$MANIFEST") PySpark versions x ${#TESTS[@]} test files, $JOBS versions at a time"
running=0
while read -r version python; do
    run_version "$version" "$python" < /dev/null &
    running=$((running + 1))
    if [ "$running" -ge "$JOBS" ]; then
        wait -n
        running=$((running - 1))
    fi
done < "$MANIFEST"
wait

failed=0
while read -r version python; do
    for test in "${TESTS[@]}"; do
        if [ "$(cat "$RESULTS/$version.$test.status")" != PASS ]; then
            failed=$((failed + 1))
            echo ""
            echo "--- FAILED: $test with pyspark $version (last 40 lines; full log: $LOG_DIR/pyspark-$version.log) ---"
            tail -n 40 "$RESULTS/$version.$test.out"
        fi
    done
done < "$MANIFEST"

echo ""
printf '%-8s' pyspark
for test in "${TESTS[@]}"; do
    name=${test#test_spark_client_}
    printf '  %-13s' "${name%.py}"
done
echo ""
while read -r version python; do
    printf '%-8s' "$version"
    for test in "${TESTS[@]}"; do
        printf '  %-13s' "$(cat "$RESULTS/$version.$test.status")"
    done
    echo ""
done < "$MANIFEST"

echo ""
if [ "$failed" -gt 0 ]; then
    echo "❌ Spark Connect client matrix: $failed failed run(s)"
    exit 1
fi
echo "✅ Spark Connect client matrix: all runs passed"
