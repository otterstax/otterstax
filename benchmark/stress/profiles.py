# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""
Stage definitions and YAML profile loading for the stress test.

Three built-in stages are used when no profile is provided.
All stages run the same query pool; only concurrency (workers_per_frontend) scales up:
  Stage 1 — Baseline:  1 worker/frontend  — sequential reference point
  Stage 2 — Medium:    5 workers/frontend  — moderate concurrency
  Stage 3 — Heavy:    15 workers/frontend  — high concurrency

A custom YAML profile can override everything; see benchmark/stress/profiles/ for
examples and STRESS_BENCHMARK_PLAN.md for the full schema reference.
"""

import random
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Allow importing queries.py from the sibling benchmarks/ directory (works both
# on-host and inside the Docker image where /app/benchmarks/ is on the path).
sys.path.insert(0, str(Path(__file__).parent.parent / "benchmarks"))
from queries import (  # noqa: E402
    COMPLEX_SELECT,
    JOIN_CROSS_ENGINE,
    JOIN_SAME_INSTANCE,
    SIMPLE_SELECT,
)

FRONTENDS = ["mysql", "postgres"]
FRONTEND_PORTS: dict = {"mysql": 8816, "postgres": 8817}

# Group B join_same_instance sub-tests only (~5 k rows; complete in 1–3 s).
# Group A camp×imp joins take 3–9 s and are too slow for baseline/medium stages.
_JOIN_SAME_B = [(n, s) for n, s in JOIN_SAME_INSTANCE if n.startswith("prod_ord_")]

VALID_POOL_KEYS: dict = {
    "simple_select":      SIMPLE_SELECT,
    "complex_select":     COMPLEX_SELECT,
    "join_same_b":        _JOIN_SAME_B,
    "join_same_instance": JOIN_SAME_INSTANCE,
    "join_cross_engine":  JOIN_CROSS_ENGINE,
}


@dataclass
class StageConfig:
    name: str                           # used as output subdirectory name
    label: str                          # human-readable banner
    workers_per_frontend: int
    duration_s: float
    ramp_secs: float
    query_weights: dict                 # category → repeat count (relative weight)
    query_pool: list = field(default_factory=list)   # built by _build_pool()
    frontends: list = field(default_factory=lambda: list(FRONTENDS))


def _build_pool(weights: dict) -> list:
    """Weighted, shuffled list of (category, name, sql) tuples for one stage."""
    pool = []
    for key, repeat in weights.items():
        pool.extend([(key, name, sql) for name, sql in VALID_POOL_KEYS[key]] * repeat)
    random.shuffle(pool)
    return pool


def query_mix_label(weights: dict) -> str:
    """Human-readable one-liner, e.g. 'simple 60% · complex 30% · join_b 10%'."""
    labels = {
        "simple_select":      "simple",
        "complex_select":     "complex",
        "join_same_b":        "join_b",
        "join_same_instance": "join",
        "join_cross_engine":  "cross",
    }
    total = sum(weights.values())
    return " · ".join(
        f"{labels.get(k, k)} {100 * v // total}%"
        for k, v in weights.items()
    )


# ---------------------------------------------------------------------------
# YAML profile loading
# ---------------------------------------------------------------------------

def _validate_stage(raw: dict, idx: int) -> None:
    name = raw.get("name", f"stage{idx + 1}")
    errors = []

    wpf = raw.get("workers_per_frontend")
    if not isinstance(wpf, int) or wpf <= 0:
        errors.append(f"stage '{name}': workers_per_frontend must be a positive integer")

    dur = raw.get("duration_s")
    if not isinstance(dur, (int, float)) or dur <= 0:
        errors.append(f"stage '{name}': duration_s must be positive")

    pool = raw.get("query_pool")
    if not pool:
        errors.append(f"stage '{name}': query_pool must contain at least one category")
    else:
        for key, weight in pool.items():
            if key not in VALID_POOL_KEYS:
                errors.append(
                    f"stage '{name}': unknown query_pool key '{key}'. "
                    f"Valid keys: {', '.join(sorted(VALID_POOL_KEYS))}"
                )
            elif not isinstance(weight, int) or weight <= 0:
                errors.append(
                    f"stage '{name}': weight for '{key}' must be a positive integer"
                )

    if errors:
        raise ValueError("\n".join(errors))


def load_profile(path: str) -> tuple:
    """
    Parse a YAML profile file.

    Returns (list[StageConfig], cooling_s).
    Raises ValueError with a descriptive message on any validation failure.
    """
    import yaml  # pyyaml — already in Dockerfile.benchmark

    with open(path) as fh:
        doc = yaml.safe_load(fh)

    cooling_s = float(doc.get("cooling_s", 10.0))
    raw_stages = doc.get("stages", [])

    if not raw_stages:
        raise ValueError("profile must define at least one stage")

    stages = []
    for i, raw in enumerate(raw_stages):
        _validate_stage(raw, i)
        dur = float(raw["duration_s"])
        ramp = float(raw.get("ramp_secs", max(2.0, dur / 6.0)))
        weights = {k: int(v) for k, v in raw["query_pool"].items()}
        stage = StageConfig(
            name=raw["name"],
            label=raw.get("label", raw["name"]),
            workers_per_frontend=int(raw["workers_per_frontend"]),
            duration_s=dur,
            ramp_secs=ramp,
            query_weights=weights,
            query_pool=_build_pool(weights),
        )
        stages.append(stage)

    return stages, cooling_s


# ---------------------------------------------------------------------------
# Built-in default stages (used when no --profile flag is given)
# ---------------------------------------------------------------------------

_DEFAULT_POOL = {
    "simple_select":    1,
    "complex_select":   1,
    "join_same_b":      1,
    "join_cross_engine": 1,
}


def build_stages(
    workers_small: int = 1,
    workers_medium: int = 5,
    workers_heavy: int = 15,
    duration_small: float = 60.0,
    duration_medium: float = 60.0,
    duration_heavy: float = 90.0,
) -> tuple:
    """Return (list[StageConfig], cooling_s) for the default 3-stage run.

    All stages use the same query pool; concurrency is the only variable.
    """
    defs = [
        StageConfig(
            name="baseline",
            label="Stage 1 — Baseline (1 worker/frontend)",
            workers_per_frontend=workers_small,
            duration_s=duration_small,
            ramp_secs=2.0,
            query_weights=dict(_DEFAULT_POOL),
        ),
        StageConfig(
            name="medium",
            label="Stage 2 — Medium Load (5 workers/frontend)",
            workers_per_frontend=workers_medium,
            duration_s=duration_medium,
            ramp_secs=5.0,
            query_weights=dict(_DEFAULT_POOL),
        ),
        StageConfig(
            name="heavy",
            label="Stage 3 — Heavy Load (15 workers/frontend)",
            workers_per_frontend=workers_heavy,
            duration_s=duration_heavy,
            ramp_secs=15.0,
            query_weights=dict(_DEFAULT_POOL),
        ),
    ]
    for stage in defs:
        stage.query_pool = _build_pool(stage.query_weights)
    return defs, 10.0
