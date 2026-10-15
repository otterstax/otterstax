# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# Light stress / load tests for OtterStax. Gated behind OTTERSTAX_RUN_STRESS=1
# so the main CI integration job stays fast; this is intended for nightly /
# on-demand runs.
#
# Reports P50/P95/P99 latency and error rate to stdout. Does not assert
# performance numbers (those would be flaky in CI). The only failure mode is:
# query errors, hangs, or any thread dying with an exception.

import argparse
import os
import statistics
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector


def make_config(local: bool):
    host = '0.0.0.0' if local else 'test-otterstax'
    return {
        'host': host,
        'mysql_port': 8816,
        'user': 'testuser',
        'password': 'testpass',
        'mysql_db': 'campaigns.db1',
    }


def _connect(cfg):
    return mysql.connector.connect(
        autocommit=True, connection_timeout=10, read_timeout=120, write_timeout=120,
        host=cfg['host'], port=cfg['mysql_port'],
        user=cfg['user'], password=cfg['password'],
    )


def _do_query(cfg, sql):
    t0 = time.monotonic()
    conn = _connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()
    finally:
        conn.close()
    return time.monotonic() - t0


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p / 100.0 * (len(s) - 1)))))
    return s[k]


def test_sustained_load(cfg, threads=32, total_queries=1000):
    print(f"\n[sustained_load] threads={threads} total={total_queries}")
    sql_mix = [
        f"SELECT campaign_id FROM {cfg['mysql_db']}.campaigns LIMIT 1",
        f"SELECT campaign_id, budget FROM {cfg['mysql_db']}.campaigns WHERE budget > 50000 LIMIT 10",
        f"SELECT count(*) FROM {cfg['mysql_db']}.campaigns",
    ]

    latencies = []
    errors = []
    lock = threading.Lock()

    def worker(i):
        sql = sql_mix[i % len(sql_mix)]
        try:
            lat = _do_query(cfg, sql)
            with lock:
                latencies.append(lat)
        except Exception as e:
            with lock:
                errors.append(str(e))

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker, i) for i in range(total_queries)]
        for f in as_completed(futures):
            f.result()
    wall = time.monotonic() - t0

    qps = total_queries / wall if wall > 0 else 0.0
    err_rate = len(errors) / total_queries if total_queries > 0 else 0.0
    print(f"  wall: {wall:.2f}s")
    print(f"  throughput: {qps:.1f} q/s")
    print(f"  errors: {len(errors)} ({err_rate*100:.2f}%)")
    if latencies:
        print(f"  latency: avg={statistics.mean(latencies)*1000:.1f}ms "
              f"p50={percentile(latencies, 50)*1000:.1f}ms "
              f"p95={percentile(latencies, 95)*1000:.1f}ms "
              f"p99={percentile(latencies, 99)*1000:.1f}ms "
              f"max={max(latencies)*1000:.1f}ms")
    if errors:
        raise AssertionError(f"{len(errors)} of {total_queries} queries failed: {errors[:5]}")


def test_concurrent_writes(cfg, writers=8, rows_per_writer=50):
    """Concurrent INSERTs from multiple connections into the same table.
    Catches deadlocks and ordering bugs on the write path.
    """
    print(f"\n[concurrent_writes] writers={writers} rows_each={rows_per_writer}")
    table = f"{cfg['mysql_db']}.stress_writes_{uuid.uuid4().hex}"

    # Setup
    setup = _connect(cfg)
    try:
        cur = setup.cursor()
        cur.execute(f"CREATE TABLE {table} (_id string, payload string, n int)")
    finally:
        setup.close()

    errors = []
    lock = threading.Lock()

    def writer(wid):
        try:
            conn = _connect(cfg)
            try:
                cur = conn.cursor()
                for r in range(rows_per_writer):
                    _id = f"{wid:03d}{r:09d}{'0' * 12}"[:24]
                    cur.execute(
                        f"INSERT INTO {table} (_id, payload, n) VALUES (%s, %s, %s)",
                        (_id, f"payload-from-writer-{wid}", wid * 1000 + r))
            finally:
                conn.close()
        except Exception as e:
            with lock:
                errors.append((wid, str(e)))

    try:
        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=writers) as ex:
            futures = [ex.submit(writer, w) for w in range(writers)]
            for f in as_completed(futures):
                f.result()
        wall = time.monotonic() - t0

        total = writers * rows_per_writer
        print(f"  completed writers in {wall:.2f}s")
        if errors:
            raise AssertionError(f"{len(errors)} writer(s) hit errors: {errors[:5]}")
        verify = _connect(cfg)
        try:
            cur = verify.cursor()
            cur.execute(f"SELECT count(*) FROM {table}")
            actual = cur.fetchone()[0]
            assert actual == total, f"row count mismatch: expected {total}, got {actual}"
            cur.execute(f"SELECT _id, payload, n FROM {table} ORDER BY _id")
            expected = [(f"{wid:03d}{r:09d}{'0' * 12}"[:24],
                         f"payload-from-writer-{wid}", wid * 1000 + r)
                        for wid in range(writers) for r in range(rows_per_writer)]
            assert cur.fetchall() == sorted(expected), "Committed rows differ from the submitted writes"
            print(f"  verified all {total} committed rows and their values")
        finally:
            verify.close()

    finally:
        cleanup = _connect(cfg)
        try:
            cleanup.cursor().execute(f"DROP TABLE {table}")
        finally:
            cleanup.close()


ALL_TESTS = [
    test_sustained_load,
    test_concurrent_writes,
]


def main_test():
    parser = argparse.ArgumentParser(description='OtterStax stress tests')
    parser.add_argument('--local', action='store_true',
                        help='Use 0.0.0.0 instead of test-otterstax')
    args = parser.parse_args()

    if os.environ.get('OTTERSTAX_RUN_STRESS') != '1':
        print("Skipped: set OTTERSTAX_RUN_STRESS=1 to run stress tests.")
        return 0

    cfg = make_config(local=args.local)
    print(f"Connecting to host: {cfg['host']}")

    failures = []
    for fn in ALL_TESTS:
        try:
            fn(cfg)
        except Exception as e:
            print(f"\033[91m  FAIL: {fn.__name__}: {e}\033[0m")
            traceback.print_exc()
            failures.append(fn.__name__)

    print("\n" + "=" * 70)
    if failures:
        print(f"\033[91m❌ Stress: {len(failures)} test(s) failed: {failures}\033[0m")
        return 1
    print("\033[92m✅ Stress: all tests passed\033[0m")
    return 0


if __name__ == '__main__':
    sys.exit(main_test())
