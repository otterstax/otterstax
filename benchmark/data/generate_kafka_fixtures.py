#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Generate the JSON dataset seeded into Kafka for the kafka_ingest benchmark.

Writes ``kafka_events.ndjson`` (one JSON object per line), sized from the
``kafka:`` block in bench.yaml.  Primitive columns only — ``json_to_chunk``
supports INTEGER / BIGINT / DOUBLE / STRING / BOOLEAN:

  id BIGINT (0..N-1, distinct), campaign_id BIGINT, event_type STRING,
  amount DOUBLE (named `amount`, not `value`, to dodge the reserved word in the
  SELECT sub-tests), ts BIGINT.

Must match the CREATE SOURCE column list in benchmarks/kafka_common.py.

    python benchmark/data/generate_kafka_fixtures.py --out benchmark/data/fixtures
"""

import argparse
import json
import os
import random
from pathlib import Path

import yaml

SEED = 20260703
EVENT_TYPES = ["view", "click", "purchase"]


def _load_config():
    cfg_path = Path(os.getenv("BENCH_YAML", Path(__file__).parent.parent / "bench.yaml"))
    cfg = {}
    if cfg_path.exists():
        with cfg_path.open() as f:
            cfg = yaml.safe_load(f) or {}
    kafka = cfg.get("kafka", {})
    return {
        "num_records": kafka.get("num_records", 50000),
        # campaign_id space aligns with group_a (the other benchmark data).
        "num_campaigns": cfg.get("group_a", {}).get("num_campaigns", 1000),
    }


def generate(out_dir: Path):
    cfg = _load_config()
    num_records = cfg["num_records"]
    num_campaigns = cfg["num_campaigns"]

    out_dir.mkdir(parents=True, exist_ok=True)
    random.seed(SEED)

    path = out_dir / "kafka_events.ndjson"
    base_ts = 1_700_000_000_000  # arbitrary fixed epoch-ms origin
    with path.open("w") as fh:
        for i in range(num_records):
            json.dump({
                "id": i,
                "campaign_id": random.randint(1, num_campaigns),
                "event_type": random.choice(EVENT_TYPES),
                "amount": round(random.uniform(0.0, 999.99), 2),
                "ts": base_ts + i,
            }, fh)
            fh.write("\n")
    print(f"wrote {path}: {num_records} rows, columns "
          "[id, campaign_id, event_type, amount, ts]")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Generate the kafka_ingest benchmark JSON dataset")
    p.add_argument("--out", type=Path, default=Path(__file__).parent / "fixtures",
                   help="Output directory (default: benchmark/data/fixtures)")
    args = p.parse_args()
    generate(args.out)
