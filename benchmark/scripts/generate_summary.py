#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Generate per-frontend and global summary markdown from benchmark JSON results."""
import sys
import json
from pathlib import Path

sys.path.insert(0, "/app/benchmarks")
from common import (BenchmarkResult, RunResult, write_global_summary,
                    write_frontend_summary, compute_stats, write_db_info)

root = Path(sys.argv[1] if len(sys.argv) > 1 else "/results")

all_results: dict = {}
for jf in sorted(root.rglob("*.json")):
    try:
        d = json.loads(jf.read_text())
        r = BenchmarkResult(**{k: v for k, v in d.items()
                               if k not in ("runs", "queries")})
        r.runs = [RunResult(**ru) for ru in d.get("runs", [])]
        r.queries = d.get("queries", {})
        compute_stats(r)
        all_results.setdefault(r.frontend, []).append(r)
    except Exception as e:
        print(f"  skipping {jf}: {e}", file=sys.stderr)

if not all_results:
    print("WARNING: no benchmark JSON files found under", root, file=sys.stderr)

for frontend, results in all_results.items():
    write_frontend_summary(frontend, results, root / frontend)
    print(f"Frontend summary: {root}/{frontend}/summary.md")

write_global_summary(all_results, root)
print(f"Global summary:   {root}/summary.md")

write_db_info(root)
print(f"DB info:          {root}/db_info.md")
