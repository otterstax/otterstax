#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""
Entry point for the OtterStax stress test.

Runs all stages defined in a YAML profile (or the built-in 3-stage default),
keeps the service alive between stages, prints a live console summary after
each stage, and writes a final degradation report comparing all stages.

Typical invocations (inside the benchmark-client Docker container):
  python /app/stress/stress_main.py                           # default 3 stages
  python /app/stress/stress_main.py -p /app/stress_profile.yaml
  python /app/stress/stress_main.py --workers-small 2 --duration-small 15
"""

import argparse
import sys
import time
from pathlib import Path

# Allow running directly from the repo (not inside Docker) by adding the
# stress/ directory to the import path so sibling modules resolve correctly.
sys.path.insert(0, str(Path(__file__).parent))

from profiles import build_stages, load_profile, FRONTEND_PORTS  # noqa: E402
from stress_runner import run_stage                               # noqa: E402
from report import (                                             # noqa: E402
    build_stage_result,
    print_stage_summary,
    write_degradation_report,
    write_stage_json,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="OtterStax stress test — N escalating load stages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "-p", "--profile", metavar="FILE",
        help="YAML profile defining stages. When given, --workers-* and --duration-* are ignored.",
    )
    p.add_argument("--host", default="bench_otterstax",
                   help="OtterStax hostname (default: bench_otterstax)")
    p.add_argument("--port-mysql",    type=int, default=8816)
    p.add_argument("--port-postgres", type=int, default=8817)
    p.add_argument("--out-dir", type=Path, default=Path("/results"),
                   help="Output root directory (default: /results)")
    # Built-in default stage knobs (ignored when --profile is given)
    p.add_argument("--workers-small",   type=int,   default=1)
    p.add_argument("--workers-medium",  type=int,   default=5)
    p.add_argument("--workers-heavy",   type=int,   default=15)
    p.add_argument("--duration-small",  type=float, default=60.0)
    p.add_argument("--duration-medium", type=float, default=60.0)
    p.add_argument("--duration-heavy",  type=float, default=90.0)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # ── Load stages ──────────────────────────────────────────────────────────
    if args.profile:
        try:
            stages, cooling_s = load_profile(args.profile)
        except (ValueError, KeyError, FileNotFoundError) as exc:
            print(f"ERROR loading profile '{args.profile}':\n{exc}", file=sys.stderr)
            sys.exit(1)
        print(f"Profile: {args.profile}  ({len(stages)} stage(s))")
    else:
        stages, cooling_s = build_stages(
            workers_small=args.workers_small,
            workers_medium=args.workers_medium,
            workers_heavy=args.workers_heavy,
            duration_small=args.duration_small,
            duration_medium=args.duration_medium,
            duration_heavy=args.duration_heavy,
        )
        print(f"Using built-in default profile ({len(stages)} stages)")

    # ── Apply port overrides ─────────────────────────────────────────────────
    FRONTEND_PORTS["mysql"]    = args.port_mysql
    FRONTEND_PORTS["postgres"] = args.port_postgres

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output  : {out_dir}")
    print(f"Host    : {args.host}  "
          f"mysql:{args.port_mysql}  postgres:{args.port_postgres}")
    print(f"Stages  : {len(stages)}  "
          f"Cooling between stages: {cooling_s:.0f}s\n")

    # ── Run stages ───────────────────────────────────────────────────────────
    all_results = []

    for i, stage in enumerate(stages):
        total_w = stage.workers_per_frontend * len(stage.frontends)

        print(f"\n{'=' * 60}")
        print(f"  {stage.label}")
        print(f"  {total_w} workers  "
              f"({stage.workers_per_frontend} × {len(stage.frontends)} frontends)")
        print(f"  {stage.duration_s:.0f}s active + {stage.ramp_secs:.0f}s ramp")
        print(f"{'=' * 60}\n")

        raw = run_stage(stage, args.host)
        result = build_stage_result(stage, raw)
        write_stage_json(result, raw, out_dir / stage.name)
        print_stage_summary(result)
        all_results.append(result)

        if i < len(stages) - 1:
            print(f"\n  [cooling period: {cooling_s:.0f}s — "
                  "letting in-flight work drain before next stage]\n")
            time.sleep(cooling_s)

    # ── Final degradation report ─────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("  Writing degradation report...")
    print(f"{'=' * 60}")
    write_degradation_report(all_results, out_dir)


if __name__ == "__main__":
    main()
