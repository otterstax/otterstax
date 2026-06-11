# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""
Result aggregation and degradation report for the stress test.

build_stage_result()      — aggregate raw WorkerStats → StageResult
write_stage_json()        — write per-frontend JSON for one stage
print_stage_summary()     — console banner after each stage
write_degradation_report()— cross-stage markdown comparison table
"""

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FrontendStats:
    frontend: str
    workers: int
    total_queries: int
    total_errors: int
    connect_errors: int
    qps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    error_rate_pct: float
    per_query_type: dict = field(default_factory=dict)  # category → {count, qps, p50/p95/p99}


@dataclass
class StageResult:
    stage_name: str
    stage_label: str
    workers_per_frontend: int
    duration_s: float
    query_mix: str
    per_frontend: dict = field(default_factory=dict)   # frontend → FrontendStats
    total_queries: int = 0
    total_errors: int = 0
    qps_total: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    max_ms: float = 0.0
    error_rate_pct: float = 0.0
    per_query_type: dict = field(default_factory=dict)  # combined across frontends


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _pct(sorted_vals: list, p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(int(len(sorted_vals) * p / 100), len(sorted_vals) - 1)
    return sorted_vals[idx]


def _cat_stats(lats: list, duration_s: float) -> dict:
    """Compute per-category stat dict from a list of latencies."""
    s = sorted(lats)
    return {
        "count":   len(lats),
        "qps":     round(len(lats) / duration_s, 3) if duration_s > 0 else 0.0,
        "p50_ms":  round(_pct(s, 50), 2),
        "p95_ms":  round(_pct(s, 95), 2),
        "p99_ms":  round(_pct(s, 99), 2),
    }


def build_stage_result(stage_config, raw_stats: dict) -> "StageResult":
    """
    Aggregate raw {frontend: [WorkerStats]} into a StageResult.
    QPS is calculated as total_queries / duration_s so it stays comparable
    across stages regardless of ramp-up timing differences.
    """
    from profiles import query_mix_label

    per_frontend: dict = {}
    all_latencies: list = []
    all_cat_lats: dict = {}   # category → [ms] combined across all frontends

    for frontend, workers in raw_stats.items():
        total_q = sum(w.query_count for w in workers)
        total_e = sum(w.error_count for w in workers)
        connect_e = sum(1 for w in workers if w.connect_error)
        latencies = []
        fe_cat_lats: dict = {}   # per-frontend per-category latencies

        for w in workers:
            latencies.extend(w.latencies_ms)
            for cat, lats in w.latencies_by_category.items():
                fe_cat_lats.setdefault(cat, []).extend(lats)
                all_cat_lats.setdefault(cat, []).extend(lats)

        all_latencies.extend(latencies)
        lat_sorted = sorted(latencies)
        qps = total_q / stage_config.duration_s if stage_config.duration_s > 0 else 0.0

        per_frontend[frontend] = FrontendStats(
            frontend=frontend,
            workers=len(workers),
            total_queries=total_q,
            total_errors=total_e,
            connect_errors=connect_e,
            qps=round(qps, 2),
            p50_ms=round(_pct(lat_sorted, 50), 2),
            p95_ms=round(_pct(lat_sorted, 95), 2),
            p99_ms=round(_pct(lat_sorted, 99), 2),
            max_ms=round(lat_sorted[-1], 2) if lat_sorted else 0.0,
            error_rate_pct=round(
                100 * total_e / (total_q + total_e), 2
            ) if (total_q + total_e) > 0 else 0.0,
            per_query_type={
                cat: _cat_stats(lats, stage_config.duration_s)
                for cat, lats in fe_cat_lats.items()
            },
        )

    # Combined per-category stats (all frontends together)
    combined_per_query_type = {
        cat: _cat_stats(lats, stage_config.duration_s)
        for cat, lats in all_cat_lats.items()
    }

    all_sorted = sorted(all_latencies)
    total_q = sum(s.total_queries for s in per_frontend.values())
    total_e = sum(s.total_errors for s in per_frontend.values())

    return StageResult(
        stage_name=stage_config.name,
        stage_label=stage_config.label,
        workers_per_frontend=stage_config.workers_per_frontend,
        duration_s=stage_config.duration_s,
        query_mix=query_mix_label(stage_config.query_weights),
        per_frontend=per_frontend,
        total_queries=total_q,
        total_errors=total_e,
        qps_total=round(sum(s.qps for s in per_frontend.values()), 2),
        p50_ms=round(_pct(all_sorted, 50), 2),
        p95_ms=round(_pct(all_sorted, 95), 2),
        p99_ms=round(_pct(all_sorted, 99), 2),
        max_ms=round(all_sorted[-1], 2) if all_sorted else 0.0,
        error_rate_pct=round(
            100 * total_e / (total_q + total_e), 2
        ) if (total_q + total_e) > 0 else 0.0,
        per_query_type=combined_per_query_type,
    )


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def write_stage_json(result: StageResult, raw_stats: dict, out_dir: Path) -> None:
    """Write per-frontend JSON file for one stage."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for frontend, fstats in result.per_frontend.items():
        workers = raw_stats.get(frontend, [])
        payload = {
            "stage":          result.stage_name,
            "frontend":       frontend,
            "workers":        fstats.workers,
            "duration_s":     result.duration_s,
            "query_mix":      result.query_mix,
            "total_queries":  fstats.total_queries,
            "total_errors":   fstats.total_errors,
            "connect_errors": fstats.connect_errors,
            "qps":            fstats.qps,
            "p50_ms":         fstats.p50_ms,
            "p95_ms":         fstats.p95_ms,
            "p99_ms":         fstats.p99_ms,
            "max_ms":         fstats.max_ms,
            "error_rate_pct": fstats.error_rate_pct,
            "by_query_type":  fstats.per_query_type,
            # Per-worker summary (no raw latency arrays — these can be millions of floats)
            "workers_detail": [
                {
                    "worker_id":     w.worker_id,
                    "query_count":   w.query_count,
                    "error_count":   w.error_count,
                    "connect_error": w.connect_error,
                }
                for w in workers
            ],
        }
        path = out_dir / f"{frontend}_results.json"
        path.write_text(json.dumps(payload, indent=2))
        print(f"  Wrote {path}")


# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------

def print_stage_summary(result: StageResult) -> None:
    total_w = result.workers_per_frontend * len(result.per_frontend)
    bar = "─" * 56
    print(f"\n  {bar}")
    print(f"  {result.stage_label}")
    print(f"  {bar}")
    print(f"  Workers : {total_w} ({result.workers_per_frontend} per frontend)")
    print(f"  Duration: {result.duration_s:.0f}s  |  Mix: {result.query_mix}")
    print(f"  Queries : {result.total_queries}  "
          f"Errors: {result.total_errors} ({result.error_rate_pct:.1f}%)")
    print(f"  QPS total: {result.qps_total:.1f}")
    for fe, fs in result.per_frontend.items():
        print(f"    QPS {fe:<9}: {fs.qps:.1f}")
    print(f"  p50: {result.p50_ms:.1f} ms  "
          f"p95: {result.p95_ms:.1f} ms  "
          f"p99: {result.p99_ms:.1f} ms  "
          f"max: {result.max_ms:.1f} ms")
    if result.per_query_type:
        print("  By query type:")
        for cat, cs in result.per_query_type.items():
            print(f"    {cat:<22} n={cs['count']:<4}  "
                  f"p50={cs['p50_ms']:.0f} ms  p99={cs['p99_ms']:.0f} ms")


# ---------------------------------------------------------------------------
# Degradation report
# ---------------------------------------------------------------------------

def _git_commit() -> str:
    commit = os.getenv("GIT_COMMIT")
    if commit:
        return commit
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def _ratio_str(new_val: float, base_val: float, warn_at: float = 3.0) -> str:
    """Return '+XX%' or '-XX%', appending ⚠ when the ratio exceeds warn_at."""
    if base_val == 0:
        return "N/A"
    ratio = new_val / base_val
    pct = (ratio - 1) * 100
    sign = "+" if pct >= 0 else ""
    mark = " ⚠" if ratio >= warn_at else ""
    return f"{sign}{pct:.0f}%{mark}"


def write_degradation_report(stages: list, out_dir: Path) -> Path:
    """
    Write degradation_report.md comparing all stages side by side.
    Stage 1 is always the baseline for ratio columns.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "degradation_report.md"
    base = stages[0]
    n = len(stages)
    frontends = list(base.per_frontend.keys())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Collect all query categories that appear across any stage (preserving insertion order)
    all_categories: list = []
    seen: set = set()
    for s in stages:
        for cat in s.per_query_type:
            if cat not in seen:
                all_categories.append(cat)
                seen.add(cat)

    with report_path.open("w") as f:

        f.write("# OtterStax Stress Test — Degradation Report\n\n")
        f.write(f"Generated : {now}   Commit: {_git_commit()}\n\n")

        # ── Load Stages ───────────────────────────────────────────────────────
        f.write("## Load Stages\n\n")
        f.write("| Stage | Workers (total) | Duration | Query Mix |\n")
        f.write("| ----- | --------------- | -------- | --------- |\n")
        for s in stages:
            total_w = s.workers_per_frontend * len(s.per_frontend)
            per_fe = " + ".join(str(s.workers_per_frontend) for _ in s.per_frontend)
            f.write(
                f"| {s.stage_label} | {total_w} ({per_fe})"
                f" | {s.duration_s:.0f} s | {s.query_mix} |\n"
            )
        f.write("\n")

        # ── Build shared header strings ───────────────────────────────────────
        stage_cols = " | ".join(s.stage_label for s in stages)
        ratio_header = (
            " | ".join(f"S{i + 2} / S1" for i in range(n - 1))
            if n > 1 else ""
        )
        sep_count = 1 + n + (n - 1)
        header_line = (
            f"| Metric | {stage_cols}"
            + (f" | {ratio_header}" if ratio_header else "")
            + " |"
        )
        sep_line = "| " + " | ".join("---" for _ in range(sep_count)) + " |"

        # ── Throughput ────────────────────────────────────────────────────────
        f.write("## Throughput\n\n")
        f.write(header_line + "\n")
        f.write(sep_line + "\n")

        for fe in frontends:
            row = f"| QPS — {fe} |"
            for s in stages:
                fs = s.per_frontend.get(fe)
                row += f" {fs.qps:.1f} |" if fs else " — |"
            for s in stages[1:]:
                fs = s.per_frontend.get(fe)
                bf = base.per_frontend.get(fe)
                row += f" {_ratio_str(fs.qps, bf.qps) if fs and bf else 'N/A'} |"
            f.write(row + "\n")

        row = "| QPS — total |"
        for s in stages:
            row += f" {s.qps_total:.1f} |"
        for s in stages[1:]:
            row += f" {_ratio_str(s.qps_total, base.qps_total)} |"
        f.write(row + "\n\n")

        # ── Latency (combined) ────────────────────────────────────────────────
        f.write("## Latency (all frontends combined)\n\n")
        f.write(header_line + "\n")
        f.write(sep_line + "\n")

        latency_metrics = [
            ("p50 (ms)", "p50_ms"),
            ("p95 (ms)", "p95_ms"),
            ("p99 (ms)", "p99_ms"),
            ("max (ms)", "max_ms"),
        ]
        for label, attr in latency_metrics:
            row = f"| {label} |"
            for s in stages:
                row += f" {getattr(s, attr):.1f} |"
            if attr != "max_ms" and n > 1:
                for s in stages[1:]:
                    row += f" {_ratio_str(getattr(s, attr), getattr(base, attr))} |"
            f.write(row + "\n")
        f.write("\n")

        # ── Errors ────────────────────────────────────────────────────────────
        f.write("## Errors\n\n")
        err_sep_count = 1 + n
        f.write("| Metric | " + " | ".join(s.stage_label for s in stages) + " |\n")
        f.write("| " + " | ".join("---" for _ in range(err_sep_count)) + " |\n")

        for label, getter in [
            ("Total queries",    lambda s: str(s.total_queries)),
            ("Errors",           lambda s: str(s.total_errors)),
            ("Error rate",       lambda s: f"{s.error_rate_pct:.1f}%"),
            ("Connection errors", lambda s: str(sum(
                fs.connect_errors for fs in s.per_frontend.values()
            ))),
        ]:
            row = f"| {label} |"
            for s in stages:
                row += f" {getter(s)} |"
            f.write(row + "\n")
        f.write("\n")

        # ── Per-query-type breakdown ───────────────────────────────────────────
        if all_categories:
            f.write("## Per-query-type Breakdown (all frontends combined)\n\n")

            for cat in all_categories:
                f.write(f"### {cat}\n\n")
                f.write(header_line + "\n")
                f.write(sep_line + "\n")

                base_cs = base.per_query_type.get(cat, {})

                # Count row (no ratio)
                row = "| Count |"
                for s in stages:
                    cs = s.per_query_type.get(cat, {})
                    row += f" {cs.get('count', 0)} |"
                row += " |" * (n - 1)   # empty ratio cells
                f.write(row + "\n")

                # QPS row (no ratio)
                row = "| QPS |"
                for s in stages:
                    cs = s.per_query_type.get(cat, {})
                    row += f" {cs.get('qps', 0.0):.3f} |"
                row += " |" * (n - 1)
                f.write(row + "\n")

                # Latency rows with ratios
                for label, key in [("p50 (ms)", "p50_ms"),
                                    ("p95 (ms)", "p95_ms"),
                                    ("p99 (ms)", "p99_ms")]:
                    row = f"| {label} |"
                    for s in stages:
                        cs = s.per_query_type.get(cat, {})
                        row += f" {cs.get(key, 0.0):.1f} |"
                    for s in stages[1:]:
                        cs = s.per_query_type.get(cat, {})
                        row += f" {_ratio_str(cs.get(key, 0.0), base_cs.get(key, 0.0))} |"
                    f.write(row + "\n")

                f.write("\n")

        if n > 1:
            f.write(
                "> `⚠` marks latency values that exceed 3× "
                f"the {base.stage_label} baseline.\n"
            )

    print(f"\nDegradation report written: {report_path}")
    return report_path
