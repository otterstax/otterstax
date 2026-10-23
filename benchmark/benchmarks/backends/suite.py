#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""The per-backend suite: the same four tests for every backend, plus join_concurrent.

    suite.py <test> --frontend mysql|postgres|spark [--host H --port P --repetitions N --out-dir D]

<test> is <backend>_<shape> with <backend> one of BACKEND_SUITES (ch, mysql, pg):

  <backend>_select_wide  ~60k-row results: decoding and the frontend's row streaming
                         dominate, not the backend;
  <backend>_insert       INSERT … VALUES of 100 and 1000 rows into the backend's
                         bench_inserts; the run's row count is the affected-row count
                         the frontend reports (Spark Connect reports none: -1);
  <backend>_prepare      each statement plain and prepared, a fresh connection per rep
                         (untimed): the difference is the prepare path — the describe
                         probe OtterStax sends the backend. Prepared means
                         COM_STMT_PREPARE (mysql), a named Parse (postgres, psycopg 3),
                         df.schema before the collect (spark: AnalyzePlan);
  <backend>_concurrent   N sessions (N = 1, 4, 8) send the same statement at once:
                         <name>_x<N> is the wall time of the batch, <name>_x<N>_each each
                         session's own latency;

and join_concurrent, the same N-session shape over cross-backend JOINs; join_wide (one
session) and join_wide_concurrent (N = 1, 4, 8) over JOINs whose cost is fetching each
side — a whole 60k-row table from MySQL and from PostgreSQL — where running the backends
side by side should show."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import BenchmarkResult, RunResult, concurrent_batch, run_benchmark, suite_main, write_txt_result
from queries import BACKEND_SUITES, JOIN_CONCURRENT, JOIN_WIDE

CONCURRENCY = (1, 4, 8)
INSERT_SIZES = (100, 1000)


def insert_sql(table, rows, rep):
    base = rep * 10_000
    values = ", ".join(f"({base + i}, {i % 1000 + 1}, 'row {i}', {i * 0.5})" for i in range(rows))
    return f"INSERT INTO {table} (id, campaign_id, note, amount) VALUES {values}"


def run_select_wide(test, suite, fe, host, port, reps, out_dir):
    return run_benchmark(test, fe.FRONTEND, suite["wide"], fe.make_fetch_factory(host, port), reps, out_dir)


def run_insert(test, suite, fe, host, port, reps, out_dir):
    result = BenchmarkResult(test, fe.FRONTEND, reps)
    table = suite["insert"]
    conn = fe.connect(host, port)
    try:
        for size in INSERT_SIZES:
            sub = f"insert_{size}_rows"
            result.queries[sub] = insert_sql(table, min(size, 3), 0) + f" …  ({size} rows)"
            for rep in range(1, reps + 1):
                sql = insert_sql(table, size, rep)
                cur = conn.cursor()
                try:
                    t0 = time.perf_counter()
                    cur.execute(sql)
                    elapsed = (time.perf_counter() - t0) * 1000
                    affected = cur.rowcount  # the count the frontend reports for the INSERT
                    result.runs.append(RunResult(sub, rep, elapsed, affected))
                    print(f"  [{sub}] rep {rep}/{reps}: {elapsed:.1f} ms ({affected} rows reported)")
                except Exception as exc:  # noqa: BLE001
                    result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                    print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
                finally:
                    cur.close()
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


def run_prepare(test, suite, fe, host, port, reps, out_dir):
    result = BenchmarkResult(test, fe.FRONTEND, reps)
    for name, sql in suite["statements"]:
        for mode in ("plain", "prepared"):
            sub = f"{name}_{mode}"
            result.queries[sub] = sql
            for rep in range(1, reps + 1):
                # A fresh connection every rep: OtterStax answers a second execute of
                # a prepared statement with "must be re-prepared", and a client that
                # caches prepared statements by text (psycopg 3) would reuse one.
                conn = fe.prepare_connect(host, port)
                cur = fe.statement_cursor(conn, prepared=(mode == "prepared"))
                try:
                    t0 = time.perf_counter()
                    cur.execute(sql)
                    rows = cur.fetchall()
                    elapsed = (time.perf_counter() - t0) * 1000
                    result.runs.append(RunResult(sub, rep, elapsed, len(rows)))
                    print(f"  [{sub}] rep {rep}/{reps}: {elapsed:.1f} ms ({len(rows)} rows)")
                except Exception as exc:  # noqa: BLE001
                    result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                    print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
                finally:
                    cur.close()  # outside the timing (COM_STMT_CLOSE for mysql)
                    conn.close()
    write_txt_result(result, out_dir)
    return result


def run_concurrent(test, statements, fe, host, port, reps, out_dir):
    result = BenchmarkResult(test, fe.FRONTEND, reps)
    for name, sql in statements:
        for n in CONCURRENCY:
            sub = f"{name}_x{n}"
            result.queries[sub] = f"{sql}   -- {n} session(s) at once; elapsed = wall time of the batch"
            for rep in range(1, reps + 1):
                wall, sessions = concurrent_batch(fe.connect, host, port, [sql] * n)
                failed = next((err for _, _, err in sessions if err), None)
                if failed:
                    result.runs.append(RunResult(sub, rep, 0, 0, failed))
                    print(f"  [{sub}] rep {rep}/{reps}: ERROR {failed}")
                    continue
                result.runs.append(RunResult(sub, rep, wall, sessions[0][1]))
                if n > 1:
                    for ms, rows, _ in sessions:
                        result.runs.append(RunResult(f"{sub}_each", rep, ms, rows))
                slowest = max(ms for ms, _, _ in sessions)
                print(f"  [{sub}] rep {rep}/{reps}: wall {wall:.1f} ms, slowest session {slowest:.1f} ms")
    write_txt_result(result, out_dir)
    return result


SHAPES = {
    "select_wide": run_select_wide,
    "insert": run_insert,
    "prepare": run_prepare,
    "concurrent": lambda test, suite, *args: run_concurrent(test, suite["statements"], *args),
}


def tests():
    """Every test this script runs."""
    names = [f"{backend}_{shape}" for backend in BACKEND_SUITES for shape in SHAPES]
    return names + ["join_concurrent", "join_wide", "join_wide_concurrent"]


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in tests():
        raise SystemExit(f"usage: suite.py <test> --frontend F ...; <test> is one of {' '.join(tests())}")
    test = sys.argv.pop(1)
    if test == "join_concurrent":
        suite_main(test, lambda fe, *args: run_concurrent(test, JOIN_CONCURRENT, fe, *args))
        return
    if test == "join_wide":
        suite_main(test, lambda fe, host, port, reps, out_dir: run_benchmark(
            test, fe.FRONTEND, JOIN_WIDE, fe.make_fetch_factory(host, port), reps, out_dir))
        return
    if test == "join_wide_concurrent":
        suite_main(test, lambda fe, *args: run_concurrent(test, JOIN_WIDE, fe, *args))
        return
    backend, shape = test.split("_", 1)
    suite_main(test, lambda fe, *args: SHAPES[shape](test, BACKEND_SUITES[backend], fe, *args))


if __name__ == "__main__":
    main()
