#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Timing, result serialisation, stats, and output formatting."""

import json
import statistics
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class RunResult:
    sub_test:   str
    repetition: int
    elapsed_ms: float
    row_count:  int
    error:      Optional[str] = None


@dataclass
class BenchmarkResult:
    test_name:   str
    frontend:    str
    repetitions: int
    runs:        list = field(default_factory=list)
    queries:     dict = field(default_factory=dict)  # sub_test → SQL string

    min_ms: float = 0.0
    max_ms: float = 0.0
    avg_ms: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0


def timed_query(fn):
    t0 = time.perf_counter()
    rows = list(fn())
    return (time.perf_counter() - t0) * 1000, len(rows)


def compute_stats(result: BenchmarkResult) -> None:
    times = sorted(r.elapsed_ms for r in result.runs if r.error is None)
    if not times:
        return
    n = len(times)
    result.min_ms = times[0]
    result.max_ms = times[-1]
    result.avg_ms = statistics.mean(times)
    result.p50_ms = statistics.median(times)
    result.p95_ms = times[min(int(n * 0.95), n - 1)]
    result.p99_ms = times[min(int(n * 0.99), n - 1)]


def _detect_backend(sub_test: str) -> str:
    name = sub_test.lower()
    has_mysql = "mysql" in name
    has_pg    = "pg"    in name
    has_ch    = "ch"    in name
    if sum([has_mysql, has_pg, has_ch]) != 1:
        return "cross"
    if has_mysql: return "mysql"
    if has_pg:    return "postgres"
    return "clickhouse"


def _sub_test_stats(runs) -> Optional[dict]:
    times = sorted(r.elapsed_ms for r in runs if r.error is None)
    if not times:
        return None
    n = len(times)
    return dict(
        min_ms=times[0], max_ms=times[-1],
        avg_ms=statistics.mean(times),
        p50_ms=statistics.median(times),
        p95_ms=times[min(int(n * 0.95), n - 1)],
        p99_ms=times[min(int(n * 0.99), n - 1)],
    )


def write_txt_result(result: BenchmarkResult, out_dir: Path) -> None:
    compute_stats(result)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{result.test_name}.txt"
    now = datetime.now(timezone.utc).isoformat()

    # group runs by sub_test preserving insertion order
    grouped: dict = {}
    for r in result.runs:
        grouped.setdefault(r.sub_test, []).append(r)

    with path.open("w") as f:
        f.write(f"# {result.test_name} — {result.frontend} frontend\n")
        f.write(f"# Generated : {now}\n")
        f.write(f"# Repetitions: {result.repetitions}\n\n")
        for sub_test, runs in grouped.items():
            f.write(f"## {sub_test}\n")
            if sub_test in result.queries:
                sql = result.queries[sub_test]
                f.write(f"  SQL: {sql}\n\n")
            f.write(f"{'rep':>4} {'elapsed_ms':>12} {'rows':>8}  error\n")
            f.write("-" * 48 + "\n")
            for r in runs:
                f.write(f"{r.repetition:>4} {r.elapsed_ms:>12.2f} "
                        f"{r.row_count:>8}  {r.error or ''}\n")
            st = _sub_test_stats(runs)
            if st:
                f.write(f"  stats: min={st['min_ms']:.2f} avg={st['avg_ms']:.2f} "
                        f"p50={st['p50_ms']:.2f} p95={st['p95_ms']:.2f} "
                        f"p99={st['p99_ms']:.2f} max={st['max_ms']:.2f} ms\n")
            else:
                f.write("  stats: all runs failed\n")
            f.write("\n")
        # per-backend-type aggregate
        by_backend: dict = {}
        for r in result.runs:
            by_backend.setdefault(_detect_backend(r.sub_test), []).append(r)
        backend_order = [b for b in ("mysql", "postgres", "clickhouse", "cross")
                         if b in by_backend]
        if len(backend_order) > 1 or (backend_order and backend_order[0] != "cross"):
            f.write("## By backend type\n")
            for backend in backend_order:
                runs_b = by_backend[backend]
                st = _sub_test_stats(runs_b)
                ok  = sum(1 for r in runs_b if not r.error)
                total = len(runs_b)
                if st:
                    f.write(f"  {backend:<12} ({ok}/{total} ok) "
                            f"min={st['min_ms']:.2f} avg={st['avg_ms']:.2f} "
                            f"p50={st['p50_ms']:.2f} p95={st['p95_ms']:.2f} "
                            f"p99={st['p99_ms']:.2f} max={st['max_ms']:.2f} ms\n")
                else:
                    f.write(f"  {backend:<12} ({ok}/{total} ok) all runs failed\n")
            f.write("\n")
        f.write("## Overall (successful runs only)\n")
        for label, val in [("min", result.min_ms), ("avg", result.avg_ms),
                            ("p50", result.p50_ms), ("p95", result.p95_ms),
                            ("p99", result.p99_ms), ("max", result.max_ms)]:
            f.write(f"  {label:<4}: {val:>10.2f} ms\n")
    (out_dir / f"{result.test_name}.json").write_text(
        json.dumps(asdict(result), indent=2, default=str))


def _git_commit() -> str:
    import os
    import subprocess
    commit = os.getenv("GIT_COMMIT")
    if commit:
        return commit
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def write_frontend_summary(frontend: str, results: list,
                           out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "summary.md").open("w") as f:
        f.write(f"# Benchmark Summary — `{frontend}` frontend\n\n")
        f.write(f"Generated : {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Commit    : {_git_commit()}\n\n")
        f.write("| test | reps | min ms | avg ms | p50 ms | p95 ms | p99 ms | max ms |\n")
        f.write("|------|-----:|-------:|-------:|-------:|-------:|-------:|-------:|\n")
        for r in results:
            compute_stats(r)
            f.write(f"| {r.test_name} | {r.repetitions} | "
                    f"{r.min_ms:.1f} | {r.avg_ms:.1f} | {r.p50_ms:.1f} | "
                    f"{r.p95_ms:.1f} | {r.p99_ms:.1f} | {r.max_ms:.1f} |\n")


def run_benchmark(test_name, frontend, queries, make_fetch, repetitions, out_dir):
    result = BenchmarkResult(test_name, frontend, repetitions)
    result.queries = {sub_test: sql for sub_test, sql in queries}
    for sub_test, sql in queries:
        fetch = make_fetch(sql)
        for rep in range(1, repetitions + 1):
            try:
                elapsed, rows = timed_query(fetch)
                result.runs.append(RunResult(sub_test, rep, elapsed, rows))
                print(f"  [{sub_test}] rep {rep}/{repetitions}: {elapsed:.1f} ms ({rows} rows)")
            except Exception as exc:
                result.runs.append(RunResult(sub_test, rep, 0, 0, str(exc)))
                print(f"  [{sub_test}] rep {rep}/{repetitions}: ERROR {exc}")
    write_txt_result(result, out_dir)
    return result


def benchmark_main(test_name, frontend, default_port, queries, make_fetch_factory):
    import argparse, sys
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="bench_otterstax")
    p.add_argument("--port", type=int, default=default_port)
    p.add_argument("--repetitions", type=int, default=10)
    p.add_argument("--out-dir", type=Path, default=Path(f"benchmark_results/{frontend}"))
    args = p.parse_args()
    print(f"=== {test_name} ({frontend}) reps={args.repetitions} ===")
    make_fetch = make_fetch_factory(args.host, args.port)
    result = run_benchmark(test_name, frontend, queries, make_fetch, args.repetitions, Path(args.out_dir))
    if any(r.error for r in result.runs):
        sys.exit(1)


def write_db_info(out_root: Path) -> None:
    import os
    import yaml as _yaml

    bench_yaml = Path(os.getenv("BENCH_YAML", "/app/bench.yaml"))
    cfg: dict = {}
    if bench_yaml.exists():
        with bench_yaml.open() as _f:
            cfg = _yaml.safe_load(_f) or {}

    ga = cfg.get("group_a", cfg.get("tables", {}))
    gb = cfg.get("group_b", cfg.get("tables", {}))

    num_campaigns        = ga.get("num_campaigns",            1_000)
    impressions_per_cam  = ga.get("impressions_per_campaign",    60)
    stats_per_cam        = ga.get("stats_per_campaign",          60)
    products_per_cam     = gb.get("products_per_campaign",        5)
    orders_per_product   = gb.get("orders_per_product",           1)
    events_per_cam       = gb.get("events_per_campaign",          4)

    num_impressions = num_campaigns * impressions_per_cam
    num_stats       = num_campaigns * stats_per_cam
    num_products    = num_campaigns * products_per_cam
    num_orders      = num_products  * orders_per_product
    num_events      = num_campaigns * events_per_cam

    now = datetime.now(timezone.utc).isoformat()

    with (out_root / "db_info.md").open("w") as f:
        f.write("# Benchmark Database Information\n\n")
        f.write(f"Generated : {now}\n")
        f.write(f"Commit    : {_git_commit()}\n\n")

        f.write("## Configuration (bench.yaml)\n\n")
        f.write("| parameter | value |\n|-----------|-------|\n")
        for key, val in [
            ("num_campaigns",           num_campaigns),
            ("impressions_per_campaign", impressions_per_cam),
            ("stats_per_campaign",       stats_per_cam),
            ("products_per_campaign",    products_per_cam),
            ("orders_per_product",       orders_per_product),
            ("events_per_campaign",      events_per_cam),
        ]:
            f.write(f"| {key} | {val:,} |\n")
        f.write("\n")

        f.write("## Connection Aliases\n\n")
        f.write("| alias | engine | host | port | database |\n")
        f.write("|-------|--------|------|------|----------|\n")
        for alias, engine, host, port, db in [
            ("mysql1", "MariaDB",    "bench_mariadb1",    3306, "benchdb1"),
            ("mysql2", "MariaDB",    "bench_mariadb2",    3306, "benchdb2"),
            ("pg1",    "PostgreSQL", "bench_postgres1",   5432, "benchpg1"),
            ("pg2",    "PostgreSQL", "bench_postgres2",   5432, "benchpg2"),
            ("ch1",    "ClickHouse", "bench_clickhouse1", 9000, "benchch1"),
            ("ch2",    "ClickHouse", "bench_clickhouse2", 9000, "benchch2"),
        ]:
            f.write(f"| `{alias}` | {engine} | {host} | {port} | {db} |\n")
        f.write("\n")

        f.write("## Group A — Large Databases\n\n")
        f.write("Aliases: `mysql1` (MariaDB · benchdb1), `pg1` (PostgreSQL · benchpg1), "
                "`ch1` (ClickHouse · benchch1)\n\n")

        f.write("### campaigns\n\n")
        f.write("| column | MariaDB type | PostgreSQL type | ClickHouse type | notes |\n")
        f.write("|--------|-------------|-----------------|-----------------|-------|\n")
        f.write("| campaign_id | INT PK | INT PK | UInt32 | 1–{:,} |\n".format(num_campaigns))
        f.write("| campaign_name | VARCHAR(255) | VARCHAR(255) | String | Faker catch_phrase |\n")
        f.write("| budget | DECIMAL(12,2) | NUMERIC(12,2) | Float64 | 5 000–500 000 |\n")
        f.write("| status | VARCHAR(16) | VARCHAR(16) | LowCardinality(String) | active/paused/completed |\n")
        f.write(f"\n**Rows per engine: {num_campaigns:,}**\n\n")

        f.write("### impressions\n\n")
        f.write("| column | MariaDB type | PostgreSQL type | ClickHouse type | notes |\n")
        f.write("|--------|-------------|-----------------|-----------------|-------|\n")
        f.write("| impression_id | BIGINT AUTO_INCREMENT PK | BIGSERIAL PK | UInt64 | sequential |\n")
        f.write("| campaign_id | INT | INT | UInt32 | FK → campaigns |\n")
        f.write("| views | INT | INT | UInt32 | 1 000–50 000 |\n")
        f.write("| clicks | INT | INT | UInt32 | 10–min(5 000, views) |\n")
        f.write("| cost | DECIMAL(10,4) | NUMERIC(10,4) | Float64 | views × 0.001–0.01 |\n")
        f.write(f"\n**Rows per engine: {num_impressions:,}** "
                f"({num_campaigns:,} campaigns × {impressions_per_cam} impressions)\n\n")

        f.write("### daily_stats\n\n")
        f.write("| column | MariaDB type | PostgreSQL type | ClickHouse type | notes |\n")
        f.write("|--------|-------------|-----------------|-----------------|-------|\n")
        f.write("| stat_id | BIGINT AUTO_INCREMENT PK | BIGSERIAL PK | UInt64 | sequential |\n")
        f.write("| campaign_id | INT | INT | UInt32 | FK → campaigns |\n")
        f.write("| total_spend | DECIMAL(12,2) | NUMERIC(12,2) | Float64 | 10–5 000 |\n")
        f.write("| total_revenue | DECIMAL(12,2) | NUMERIC(12,2) | Float64 | spend × 0.5–3.0 |\n")
        f.write("| conversion_count | INT | INT | UInt32 | 0–500 |\n")
        f.write("| ctr | FLOAT | FLOAT | Float64 | clicks/views |\n")
        f.write("| roas | FLOAT | FLOAT | Float64 | revenue/spend |\n")
        f.write(f"\n**Rows per engine: {num_stats:,}** "
                f"({num_campaigns:,} campaigns × {stats_per_cam} stats)\n\n")

        f.write("## Group B — Small Databases\n\n")
        f.write("Aliases: `mysql2` (MariaDB · benchdb2), `pg2` (PostgreSQL · benchpg2), "
                "`ch2` (ClickHouse · benchch2)\n\n")

        f.write("### products\n\n")
        f.write("| column | MariaDB type | PostgreSQL type | ClickHouse type | notes |\n")
        f.write("|--------|-------------|-----------------|-----------------|-------|\n")
        f.write("| product_id | INT AUTO_INCREMENT PK | SERIAL PK | UInt32 | sequential |\n")
        f.write("| campaign_id | INT | INT | UInt32 | 1–{:,} |\n".format(num_campaigns))
        f.write("| product_name | VARCHAR(255) | VARCHAR(255) | String | Faker catch_phrase |\n")
        f.write("| category | VARCHAR(100) | VARCHAR(100) | LowCardinality(String) | 8 categories |\n")
        f.write("| price | DECIMAL(10,2) | NUMERIC(10,2) | Float64 | 9.99–999.99 |\n")
        f.write("| stock_qty | INT | INT | UInt32 | 0–10 000 |\n")
        f.write(f"\n**Rows per engine: {num_products:,}** "
                f"({num_campaigns:,} campaigns × {products_per_cam} products)\n\n")

        f.write("### orders\n\n")
        f.write("| column | MariaDB type | PostgreSQL type | ClickHouse type | notes |\n")
        f.write("|--------|-------------|-----------------|-----------------|-------|\n")
        f.write("| order_id | INT AUTO_INCREMENT PK | SERIAL PK | UInt32 | sequential |\n")
        f.write("| product_id | INT | INT | UInt32 | FK → products |\n")
        f.write("| campaign_id | INT | INT | UInt32 | FK → campaigns |\n")
        f.write("| customer_email | VARCHAR(255) | VARCHAR(255) | String | user{N}@example.com |\n")
        f.write("| quantity | INT | INT | UInt32 | 1–20 |\n")
        f.write("| unit_price | DECIMAL(10,2) | NUMERIC(10,2) | Float64 | = product.price |\n")
        f.write("| total_price | DECIMAL(12,2) | NUMERIC(12,2) | Float64 | quantity × unit_price |\n")
        f.write(f"\n**Rows per engine: {num_orders:,}** "
                f"({num_products:,} products × {orders_per_product} orders)\n\n")

        f.write("### events\n\n")
        f.write("| column | MariaDB type | PostgreSQL type | ClickHouse type | notes |\n")
        f.write("|--------|-------------|-----------------|-----------------|-------|\n")
        f.write("| event_id | BIGINT AUTO_INCREMENT PK | BIGSERIAL PK | UInt64 | sequential |\n")
        f.write("| campaign_id | INT | INT | UInt32 | 1–{:,} |\n".format(num_campaigns))
        f.write("| product_id | INT | INT | UInt32 | from campaign's products |\n")
        f.write("| event_type | VARCHAR(32) | VARCHAR(32) | LowCardinality(String) | click/view/add_to_cart/purchase |\n")
        f.write("| user_id | BIGINT | BIGINT | UInt64 | 1–1 000 000 |\n")
        f.write("| device | VARCHAR(16) | VARCHAR(16) | LowCardinality(String) | desktop/mobile/tablet |\n")
        f.write(f"\n**Rows per engine: {num_events:,}** "
                f"({num_campaigns:,} campaigns × {events_per_cam} events)\n\n")

        f.write("## Row Count Summary\n\n")
        f.write("| table | group | rows per engine |\n")
        f.write("|-------|-------|----------------:|\n")
        for name, group, rows in [
            ("campaigns",   "A", num_campaigns),
            ("impressions", "A", num_impressions),
            ("daily_stats", "A", num_stats),
            ("products",    "B", num_products),
            ("orders",      "B", num_orders),
            ("events",      "B", num_events),
        ]:
            f.write(f"| {name} | {group} | {rows:,} |\n")
        f.write(f"\nTotal rows across all 6 engines: "
                f"**{3*(num_campaigns+num_impressions+num_stats+num_products+num_orders+num_events):,}**\n")


def write_global_summary(all_results: dict, out_root: Path) -> None:
    # Collect all individual run records for cross-result stats
    all_runs: list[RunResult] = []
    for results in all_results.values():
        for r in results:
            all_runs.extend(r.runs)
    successful = [r for r in all_runs if not r.error]
    failed     = [r for r in all_runs if r.error]
    total_ms   = sum(r.elapsed_ms for r in successful)
    slowest    = max(successful, key=lambda r: r.elapsed_ms) if successful else None
    fastest    = min((r for r in successful if r.elapsed_ms > 0),
                     key=lambda r: r.elapsed_ms, default=None)

    with (out_root / "summary.md").open("w") as f:
        f.write("# Global Benchmark Summary\n\n")
        f.write(f"Generated : {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Commit    : {_git_commit()}\n\n")

        # ── Overall stats ────────────────────────────────────────────────────
        f.write("## Overall\n\n")
        f.write(f"| metric | value |\n|--------|-------|\n")
        f.write(f"| total runs | {len(all_runs)} "
                f"({len(successful)} ok / {len(failed)} failed) |\n")
        h, m, s = int(total_ms // 3_600_000), int((total_ms % 3_600_000) // 60_000), total_ms % 60_000 / 1000
        f.write(f"| total elapsed | {h:02d}h {m:02d}m {s:05.2f}s "
                f"({total_ms / 1000:.1f} s) |\n")
        if slowest:
            f.write(f"| slowest run | {slowest.elapsed_ms:.1f} ms — "
                    f"`{slowest.sub_test}` rep {slowest.repetition} |\n")
        if fastest:
            f.write(f"| fastest run | {fastest.elapsed_ms:.1f} ms — "
                    f"`{fastest.sub_test}` rep {fastest.repetition} |\n")
        f.write("\n")

        # ── Per-frontend tables ──────────────────────────────────────────────
        for frontend, results in sorted(all_results.items()):
            f.write(f"\n## Frontend: `{frontend}`\n\n")
            f.write("| test | reps | min ms | avg ms | p50 ms | p95 ms | p99 ms | max ms |\n")
            f.write("|------|-----:|-------:|-------:|-------:|-------:|-------:|-------:|\n")
            for r in results:
                compute_stats(r)
                f.write(f"| {r.test_name} | {r.repetitions} | "
                        f"{r.min_ms:.1f} | {r.avg_ms:.1f} | {r.p50_ms:.1f} | "
                        f"{r.p95_ms:.1f} | {r.p99_ms:.1f} | {r.max_ms:.1f} |\n")
