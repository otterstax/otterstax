# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""
Concurrent worker engine for the stress test.

Each worker thread opens one persistent connection, cycles through its
shuffled copy of the query pool, and records per-query latency until
the shared stop_event fires.  On error the worker attempts to reconnect
up to 3 times before exiting early.
"""

import random
import threading
import time
from dataclasses import dataclass, field

import mysql.connector as _mysql
import psycopg2 as _psycopg2


@dataclass
class WorkerStats:
    worker_id: int
    frontend: str
    query_count: int = 0
    error_count: int = 0
    connect_error: bool = False      # True when the initial connection failed
    latencies_ms: list = field(default_factory=list)
    latencies_by_category: dict = field(default_factory=dict)  # category → [ms]
    wall_start: float = 0.0         # time.perf_counter() at first query attempt
    wall_end: float = 0.0           # time.perf_counter() after stop_event


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _connect(frontend: str, host: str, port: int):
    if frontend == "mysql":
        return _mysql.connect(
            host=host,
            port=port,
            user="testuser",
            password="testpass",
            connection_timeout=10,
        )
    # postgres
    conn = _psycopg2.connect(
        host=host,
        port=port,
        user="testuser",
        password="testpass",
        dbname="default",
        connect_timeout=10,
    )
    conn.autocommit = True   # avoid implicit transaction overhead on read-only queries
    return conn


def _execute(conn, frontend: str, sql: str) -> int:
    """Execute sql, return row count."""
    if frontend == "mysql":
        cur = conn.cursor(buffered=True)
    else:
        cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return len(rows)


# ---------------------------------------------------------------------------
# Worker thread
# ---------------------------------------------------------------------------

def _worker(
    frontend: str,
    host: str,
    port: int,
    pool: list,
    ramp_delay_s: float,
    stop_event: threading.Event,
    stats: WorkerStats,
) -> None:
    time.sleep(ramp_delay_s)

    try:
        conn = _connect(frontend, host, port)
    except Exception:
        stats.connect_error = True
        stats.error_count += 1
        return

    # Each worker shuffles its own copy so concurrent workers hit different queries.
    worker_pool = list(pool)
    random.shuffle(worker_pool)
    idx = 0
    stats.wall_start = time.perf_counter()

    while not stop_event.is_set():
        _category, _name, sql = worker_pool[idx % len(worker_pool)]
        idx += 1

        t0 = time.perf_counter()
        try:
            _execute(conn, frontend, sql)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            stats.latencies_ms.append(elapsed_ms)
            stats.latencies_by_category.setdefault(_category, []).append(elapsed_ms)
            stats.query_count += 1
        except Exception:
            stats.error_count += 1
            # Attempt reconnect up to 3 times with a short back-off.
            reconnected = False
            for _ in range(3):
                try:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = _connect(frontend, host, port)
                    reconnected = True
                    break
                except Exception:
                    time.sleep(0.5)
            if not reconnected:
                break

    stats.wall_end = time.perf_counter()
    try:
        conn.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run_stage(stage_config, host: str) -> dict:
    """
    Run one stage. Blocks until all workers finish.
    Returns {frontend: [WorkerStats]}.
    """
    from profiles import FRONTEND_PORTS

    frontends = stage_config.frontends
    total_workers = stage_config.workers_per_frontend * len(frontends)
    ramp_interval = stage_config.ramp_secs / max(total_workers - 1, 1)

    stop_event = threading.Event()
    per_frontend: dict = {f: [] for f in frontends}
    threads: list = []

    for i, frontend in enumerate(frontends):
        port = FRONTEND_PORTS[frontend]
        for w in range(stage_config.workers_per_frontend):
            global_idx = i * stage_config.workers_per_frontend + w
            ramp_delay = global_idx * ramp_interval
            stats = WorkerStats(worker_id=global_idx, frontend=frontend)
            per_frontend[frontend].append(stats)
            t = threading.Thread(
                target=_worker,
                args=(frontend, host, port, stage_config.query_pool,
                      ramp_delay, stop_event, stats),
                daemon=True,
                name=f"stress-{frontend}-{w}",
            )
            threads.append(t)

    print(f"  Starting {total_workers} workers "
          f"({stage_config.workers_per_frontend} × {len(frontends)} frontends)...")
    for t in threads:
        t.start()

    _progress_sleep(
        stage_config.duration_s + stage_config.ramp_secs,
        stage_config.duration_s,
        stage_config.ramp_secs,
    )

    print("  Stopping workers...")
    stop_event.set()
    for t in threads:
        t.join(timeout=60.0)

    return per_frontend


def _progress_sleep(total_s: float, duration_s: float, ramp_s: float) -> None:
    """Sleep for total_s, printing a brief progress line every 10 s."""
    elapsed = 0.0
    tick = 10.0
    while elapsed < total_s:
        chunk = min(tick, total_s - elapsed)
        time.sleep(chunk)
        elapsed += chunk
        if elapsed <= ramp_s:
            phase = f"ramping  {elapsed:.0f}/{ramp_s:.0f}s"
        else:
            active = elapsed - ramp_s
            phase = f"running  {active:.0f}/{duration_s:.0f}s"
        print(f"    [{phase}]")
