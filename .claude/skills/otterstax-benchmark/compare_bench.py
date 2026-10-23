#!/usr/bin/env python3
"""Compare end-to-end benchmark runs: median per frontend/test/sub-test, all runs pooled.

usage: compare_bench.py NAME=DIR [NAME=DIR ...]   (first one is the reference)
Each DIR holds run*/<frontend>/<test>.json as written by run_benchmark.sh.
"""
import glob
import json
import os
import statistics
import sys
from collections import defaultdict


def load(root):
    times = defaultdict(list)
    errors = defaultdict(int)
    for path in glob.glob(os.path.join(root, "run*", "*", "*.json")):
        with open(path) as f:
            data = json.load(f)
        if not isinstance(data, dict) or "runs" not in data:
            continue
        for r in data["runs"]:
            key = (data["frontend"], data["test_name"], r["sub_test"])
            if r.get("error"):
                errors[key] += 1
            elif r.get("elapsed_ms") is not None:
                times[key].append(r["elapsed_ms"])
    return times, errors


def fmt(ms):
    return f"{ms / 1000:.2f} s" if ms >= 1000 else f"{ms:.1f}"


def main():
    builds = [a.split("=", 1) for a in sys.argv[1:]]
    data = {name: load(d) for name, d in builds}
    ref = builds[0][0]
    keys = sorted(set().union(*(set(t) | set(e) for t, e in data.values())))
    names = [n for n, _ in builds]
    header = "| frontend | test | sub-test | " + " | ".join(names) + " | " + \
             " | ".join(f"{n} vs {ref}" for n in names[1:]) + " |"
    print(header)
    print("|" + "---|" * (3 + 2 * len(names) - 1))
    per_test = defaultdict(lambda: defaultdict(list))
    total_err = defaultdict(int)
    for key in keys:
        meds = {}
        cells = []
        for n in names:
            t, e = data[n]
            total_err[n] += e.get(key, 0)
            if t.get(key):
                meds[n] = statistics.median(t[key])
                cell = fmt(meds[n])
                if e.get(key):
                    cell += f" ({e[key]} err)"
            else:
                cell = f"— ({e.get(key, 0)} err)" if e.get(key) else "—"
            cells.append(cell)
        deltas = []
        for n in names[1:]:
            if ref in meds and n in meds:
                d = (meds[n] / meds[ref] - 1) * 100
                deltas.append(f"{d:+.0f}%")
                per_test[n][(key[0], key[1])].append(meds[n] / meds[ref])
            else:
                deltas.append("—")
        print(f"| {key[0]} | {key[1]} | {key[2]} | " + " | ".join(cells) + " | " + " | ".join(deltas) + " |")
    print()
    print("errors per build: " + ", ".join(f"{n}={total_err[n]}" for n in names))
    for n in names[1:]:
        print(f"\nper-test geometric mean of median ratios, {n} vs {ref}:")
        for (fe, test), ratios in sorted(per_test[n].items()):
            g = statistics.geometric_mean(ratios)
            print(f"  {fe:8s} {test:26s} {(g - 1) * 100:+5.0f}%  ({len(ratios)} sub-tests)")


if __name__ == "__main__":
    main()
