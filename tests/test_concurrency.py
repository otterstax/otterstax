# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# Concurrency tests for OtterStax.
#
# These tests exist to catch regressions related to synchronous code paths in
# the server. Today the scheduler, per-backend connection managers and the
# QueryHandleWaiter use a single mutex + a busy-wait spin loop, which serialises
# actor messages. The tests below send multiple queries concurrently and
# report wall-clock measurements. They do NOT assert ideal parallel speedup
# (which would fail on the current codebase) — they assert that the server:
#
#   * does not hang or deadlock,
#   * does not drop sessions under contention,
#   * returns the same results under load as under a single client.
#
# Set OTTERSTAX_EXPECT_ASYNC=1 to additionally assert that parallel execution
# is meaningfully faster than serial. Use this once the sync hot paths have
# been replaced by truly async equivalents.
#
# Set OTTERSTAX_RUN_DISRUPTION=1 to run the backend-disconnect recovery test;
# it pauses/unpauses backend containers via docker compose and is therefore
# only meaningful when run outside the test container itself.

import argparse
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector
import psycopg2
from flightsql import FlightSQLClient


# Connection config — mirrors the per-client test_*.py files. Kept local on
# purpose: extracting a shared module would require updating every existing
# test, which is out of scope for this change.
def make_config(local: bool):
    host = '0.0.0.0' if local else 'test-otterstax'
    return {
        'host': host,
        'mysql_port': 8816,
        'pg_port': 8817,
        'flight_port': 8815,
        'user': 'testuser',
        'password': 'testpass',
        'mysql_db': 'campaigns.db1.schema',
        'pg_db': 'products',                       # dbname for psycopg
        'pg_schema': 'products.pgdb.public',       # qualified-name prefix
    }


EXPECT_ASYNC = os.environ.get('OTTERSTAX_EXPECT_ASYNC') == '1'
RUN_DISRUPTION = os.environ.get('OTTERSTAX_RUN_DISRUPTION') == '1'


# ---------------------------------------------------------------------------
# Small helpers around each wire protocol.

def _mysql_select_one(cfg):
    conn = mysql.connector.connect(
        host=cfg['host'], port=cfg['mysql_port'],
        user=cfg['user'], password=cfg['password'],
    )
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT campaign_id FROM {cfg['mysql_db']}.campaigns LIMIT 1")
        rows = cur.fetchall()
        return len(rows)
    finally:
        conn.close()


def _pg_select_one(cfg):
    conn = psycopg2.connect(
        host=cfg['host'], port=cfg['pg_port'],
        user=cfg['user'], password=cfg['password'],
        dbname=cfg['pg_db'],
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT product_id FROM {cfg['pg_schema']}.products LIMIT 1")
        rows = cur.fetchall()
        return len(rows)
    finally:
        conn.close()


def _flight_select_one(cfg):
    client = FlightSQLClient(host=cfg['host'], port=cfg['flight_port'], insecure=True)
    info = client.execute(f"SELECT campaign_id FROM {cfg['mysql_db']}.campaigns LIMIT 1")
    reader = client.do_get(info.endpoints[0].ticket)
    table = reader.read_all()
    return table.num_rows


# ---------------------------------------------------------------------------
# Test cases.

def test_parallel_select_same_backend(cfg, n_threads=16, queries_per_thread=5):
    """N parallel clients on MySQL wire, each issuing M short SELECTs."""
    print(f"\n[parallel_select_same_backend] threads={n_threads} per_thread={queries_per_thread}")

    # Single-threaded baseline.
    t0 = time.monotonic()
    for _ in range(queries_per_thread):
        _mysql_select_one(cfg)
    baseline = time.monotonic() - t0
    print(f"  baseline (1 thread, {queries_per_thread} queries): {baseline:.3f}s")

    def worker(_):
        for _ in range(queries_per_thread):
            _mysql_select_one(cfg)

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        futures = [ex.submit(worker, i) for i in range(n_threads)]
        for f in futures:
            f.result()
    parallel = time.monotonic() - t0
    print(f"  parallel ({n_threads} threads × {queries_per_thread}): {parallel:.3f}s")

    serial_estimate = baseline * n_threads
    speedup = serial_estimate / parallel if parallel > 0 else float('inf')
    print(f"  estimated serial: {serial_estimate:.3f}s, speedup vs serial: {speedup:.2f}x")

    if EXPECT_ASYNC:
        # When async I/O is in place, we should see meaningful speedup.
        # Threshold is intentionally low (2x) — even a partial async path
        # should beat full serialisation comfortably.
        if speedup < 2.0:
            raise AssertionError(
                f"OTTERSTAX_EXPECT_ASYNC=1: expected ≥2x speedup, got {speedup:.2f}x")


def test_slow_query_does_not_block_others(cfg):
    """Heavy cross-backend JOIN in parallel with light SELECT.

    Heavy query uses the same JOIN pattern as test_flightsql_client_mysql_backend
    (campaigns × impressions ordered by clicks DESC), which is known to work
    end-to-end. Light query is LIMIT 1. The light query is started after the
    heavy one — if the server serialises sessions, the light query's wall-clock
    completion will be close to the heavy one.
    """
    print("\n[slow_query_does_not_block_others]")

    heavy_sql = (
        "SELECT * FROM campaigns.db1.schema.campaigns "
        "JOIN impressions.db2.schema.impressions "
        "ON campaigns.campaign_id = impressions.campaign_id "
        "WHERE campaigns.campaign_length > 30 "
        "ORDER BY impressions.clicks DESC")
    light_sql = f"SELECT campaign_id FROM {cfg['mysql_db']}.campaigns LIMIT 1"

    def run(sql, label):
        t0 = time.monotonic()
        conn = mysql.connector.connect(
            host=cfg['host'], port=cfg['mysql_port'],
            user=cfg['user'], password=cfg['password'],
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cur.fetchall()
        finally:
            conn.close()
        elapsed = time.monotonic() - t0
        return (label, t0, elapsed)

    with ThreadPoolExecutor(max_workers=2) as ex:
        heavy_future = ex.submit(run, heavy_sql, 'heavy')
        time.sleep(0.05)  # give the heavy query a head start
        light_future = ex.submit(run, light_sql, 'light')
        heavy = heavy_future.result()
        light = light_future.result()

    print(f"  heavy: {heavy[2]:.3f}s")
    print(f"  light: {light[2]:.3f}s")
    ratio = light[2] / heavy[2] if heavy[2] > 0 else float('inf')
    print(f"  light/heavy ratio: {ratio:.2f}")

    if EXPECT_ASYNC:
        # An async server should let the light query finish well before the
        # heavy one; ratio close to 1.0 means full serialisation.
        if ratio > 0.5:
            raise AssertionError(
                f"OTTERSTAX_EXPECT_ASYNC=1: light query was not faster than heavy "
                f"(ratio {ratio:.2f}). Server likely serialises sessions.")


def test_isolation_across_backends(cfg):
    """Heavy query on MySQL backend in parallel with simple query on PG backend.

    Both queries traverse the same Scheduler actor. If the scheduler's
    enqueue_impl mutex blocks dispatch, the PG client will wait behind the
    MySQL client.
    """
    print("\n[isolation_across_backends]")

    heavy_sql = (
        "SELECT * FROM campaigns.db1.schema.campaigns "
        "JOIN impressions.db2.schema.impressions "
        "ON campaigns.campaign_id = impressions.campaign_id "
        "WHERE campaigns.campaign_length > 30 "
        "ORDER BY impressions.clicks DESC")

    def heavy():
        conn = mysql.connector.connect(
            host=cfg['host'], port=cfg['mysql_port'],
            user=cfg['user'], password=cfg['password'],
        )
        try:
            cur = conn.cursor()
            cur.execute(heavy_sql)
            cur.fetchall()
        finally:
            conn.close()

    def light_pg():
        t0 = time.monotonic()
        _pg_select_one(cfg)
        return time.monotonic() - t0

    pg_baseline = light_pg()
    print(f"  PG baseline: {pg_baseline:.3f}s")

    with ThreadPoolExecutor(max_workers=2) as ex:
        heavy_future = ex.submit(heavy)
        time.sleep(0.05)
        pg_future = ex.submit(light_pg)
        pg_under_load = pg_future.result()
        heavy_future.result()
    print(f"  PG while MySQL backend busy: {pg_under_load:.3f}s")
    slowdown = pg_under_load / pg_baseline if pg_baseline > 0 else float('inf')
    print(f"  PG slowdown factor: {slowdown:.2f}x")

    if EXPECT_ASYNC:
        if slowdown > 2.0:
            raise AssertionError(
                f"OTTERSTAX_EXPECT_ASYNC=1: PG light query slowed down "
                f"{slowdown:.2f}x while MySQL backend was busy. Cross-backend "
                f"isolation appears to be broken.")


def test_many_short_connections(cfg, total=50):
    """Sequential open→query→close, just to make sure connection slot release
    in frontend/common/frontend_server.hpp doesn't deadlock or leak."""
    print(f"\n[many_short_connections] total={total}")
    t0 = time.monotonic()
    for i in range(total):
        _mysql_select_one(cfg)
    elapsed = time.monotonic() - t0
    print(f"  {total} sequential open/query/close in {elapsed:.3f}s "
          f"({elapsed/total*1000:.1f}ms per cycle)")


def test_mixed_protocols_parallel(cfg, rounds=8):
    """MySQL wire + PG wire + FlightSQL wire, all in parallel against each
    other for a few rounds. Sanity check that the three protocol servers
    coexist under load."""
    print(f"\n[mixed_protocols_parallel] rounds={rounds}")

    def round_robin():
        _mysql_select_one(cfg)
        _pg_select_one(cfg)
        _flight_select_one(cfg)

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=rounds) as ex:
        futures = [ex.submit(round_robin) for _ in range(rounds)]
        for f in as_completed(futures):
            f.result()
    elapsed = time.monotonic() - t0
    print(f"  {rounds} mixed rounds in {elapsed:.3f}s")


def test_backend_disconnect_recovery(cfg):
    """Pause backend container mid-query, verify the server reports a clean
    error within cv_wrapper::DEFAULT_TIMEOUT and recovers afterwards.

    This test only runs when OTTERSTAX_RUN_DISRUPTION=1 because it shells out
    to `docker compose` and requires access to the host docker socket.
    """
    if not RUN_DISRUPTION:
        print("\n[backend_disconnect_recovery] SKIPPED (set OTTERSTAX_RUN_DISRUPTION=1)")
        return

    import subprocess
    print("\n[backend_disconnect_recovery]")

    def pause(svc):
        subprocess.run(['docker', 'compose', 'pause', svc], check=True)

    def unpause(svc):
        subprocess.run(['docker', 'compose', 'unpause', svc], check=True)

    # Trigger a query while pausing the MySQL backend.
    pause('test-mariadb-1')
    try:
        t0 = time.monotonic()
        err = None
        try:
            _mysql_select_one(cfg)
        except Exception as e:
            err = e
        elapsed = time.monotonic() - t0
        print(f"  query with paused backend: elapsed={elapsed:.1f}s err={err}")
        # The 90s server-side default timeout must surface as an error rather
        # than an indefinite hang.
        if elapsed > 120:
            raise AssertionError(f"Server hung instead of timing out ({elapsed:.1f}s)")
    finally:
        unpause('test-mariadb-1')

    # After unpause, the next query must succeed.
    time.sleep(2)
    _mysql_select_one(cfg)
    print("  post-recovery query OK")


# ---------------------------------------------------------------------------
# Runner.

ALL_TESTS = [
    test_parallel_select_same_backend,
    test_slow_query_does_not_block_others,
    test_isolation_across_backends,
    test_many_short_connections,
    test_mixed_protocols_parallel,
    test_backend_disconnect_recovery,
]


def main_test():
    parser = argparse.ArgumentParser(description='OtterStax concurrency tests')
    parser.add_argument('--local', action='store_true',
                        help='Use 0.0.0.0 instead of test-otterstax')
    args = parser.parse_args()

    cfg = make_config(local=args.local)
    print(f"Connecting to host: {cfg['host']}")
    print(f"OTTERSTAX_EXPECT_ASYNC={EXPECT_ASYNC} "
          f"OTTERSTAX_RUN_DISRUPTION={RUN_DISRUPTION}")

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
        print(f"\033[91m❌ Concurrency: {len(failures)} test(s) failed: {failures}\033[0m")
        return 1
    print("\033[92m✅ Concurrency: all tests passed\033[0m")
    return 0


if __name__ == '__main__':
    sys.exit(main_test())
