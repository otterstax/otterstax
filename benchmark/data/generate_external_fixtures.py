#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Generate the joinable external-table fixtures used by the s3/file benchmarks
(external_load / external_join / external_dump).

This is the benchmark-scale sibling of
tests/minio/fixtures/generate_external_fixtures.py.  It writes the same three
files but sized from bench.yaml so the fixtures share the campaign_id space with
the benchmark backend data (Group A campaigns 1..num_campaigns):

  regions.parquet   dimension table   num_campaigns × regions_per_campaign rows
  web_events.csv    fact table        num_campaigns × events_per_campaign  rows
  campaigns.ndjson  dimension table   num_campaigns                        rows

regions ⋈ web_events correlate on campaign_id, so external_join performs an
*internal* (otterbrix-on-otterbrix) join of two CREATE EXTERNAL TABLE'd sources
— no remote backend is involved.  All integer columns are int64 so the
parquet/csv tables join without type coercion.  Seeded → reproducible.

Scale comes from bench.yaml `external:` (falling back to `group_a` for
num_campaigns so the fixtures stay aligned with the backend campaigns table):

    python benchmark/data/generate_external_fixtures.py --out benchmark/data/fixtures
"""

import argparse
import csv
import json
import os
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
import yaml

SEED = 20260626
COUNTRIES = ["US", "UK", "DE", "FR", "JP", "BR", "IN", "CA"]
EVENT_TYPES = ["view", "click", "purchase"]
STATUSES = ["active", "paused", "completed"]


def _load_config():
    cfg_path = Path(os.getenv("BENCH_YAML", Path(__file__).parent.parent / "bench.yaml"))
    cfg = {}
    if cfg_path.exists():
        with cfg_path.open() as f:
            cfg = yaml.safe_load(f) or {}
    ext = cfg.get("external", {})
    ga = cfg.get("group_a", cfg.get("tables", {}))
    gb = cfg.get("group_b", cfg.get("tables", {}))
    # num_campaigns defaults to Group A so external rows share its campaign_id space.
    return {
        "num_campaigns": ext.get("num_campaigns", ga.get("num_campaigns", 1000)),
        "regions_per_campaign": ext.get("regions_per_campaign", 4),
        "events_per_campaign": ext.get("events_per_campaign", 20),
        # products.product_id space (Group B: num_campaigns × products_per_campaign)
        "max_product_id": ext.get(
            "max_product_id",
            gb.get("num_campaigns", 1000) * gb.get("products_per_campaign", 5),
        ),
    }


def generate(out_dir: Path):
    cfg = _load_config()
    num_campaigns = cfg["num_campaigns"]
    regions_per = cfg["regions_per_campaign"]
    events_per = cfg["events_per_campaign"]
    max_product = cfg["max_product_id"]

    out_dir.mkdir(parents=True, exist_ok=True)
    fake = Faker("en_US")
    Faker.seed(SEED)
    random.seed(SEED)

    # ── regions.parquet — geo targeting dimension (joins campaign_id) ──────────
    region_id, campaign_col, region_name, country, population, ad_spend = [], [], [], [], [], []
    rid = 1
    for cid in range(1, num_campaigns + 1):
        for _ in range(regions_per):
            region_id.append(rid)
            campaign_col.append(cid)
            region_name.append(f"{fake.city()} Region")
            country.append(random.choice(COUNTRIES))
            population.append(random.randint(50_000, 5_000_000))
            ad_spend.append(round(random.uniform(1000.0, 50_000.0), 2))
            rid += 1
    regions = pa.table({
        "region_id": pa.array(region_id, pa.int64()),
        "campaign_id": pa.array(campaign_col, pa.int64()),
        "region_name": pa.array(region_name, pa.string()),
        "country": pa.array(country, pa.string()),
        "population": pa.array(population, pa.int64()),
        "ad_spend": pa.array(ad_spend, pa.float64()),
    })
    regions_path = out_dir / "regions.parquet"
    pq.write_table(regions, regions_path)
    print(f"wrote {regions_path}: {regions.num_rows} rows, columns {regions.column_names}")

    # ── web_events.csv — web analytics fact (joins campaign_id) ────────────────
    events_path = out_dir / "web_events.csv"
    total = 0
    with events_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["event_id", "campaign_id", "product_id", "event_type", "session_seconds", "value"])
        eid = 1
        for cid in range(1, num_campaigns + 1):
            for _ in range(events_per):
                writer.writerow([
                    eid,
                    cid,
                    random.randint(1, max_product),
                    random.choice(EVENT_TYPES),
                    random.randint(5, 1800),
                    round(random.uniform(0.0, 999.99), 2),
                ])
                eid += 1
                total += 1
    print(f"wrote {events_path}: {total} rows, columns "
          "[event_id, campaign_id, product_id, event_type, session_seconds, value]")

    # ── campaigns.ndjson — one row per campaign (joins campaign_id) ────────────
    campaigns_path = out_dir / "campaigns.ndjson"
    with campaigns_path.open("w") as fh:
        for cid in range(1, num_campaigns + 1):
            json.dump({
                "campaign_id": cid,
                "campaign_name": f"{fake.company()} Campaign",
                "budget": round(random.uniform(10_000.0, 250_000.0), 2),
                "status": random.choice(STATUSES),
            }, fh)
            fh.write("\n")
    print(f"wrote {campaigns_path}: {num_campaigns} rows, columns "
          "[campaign_id, campaign_name, budget, status]")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Generate s3/file external-table benchmark fixtures")
    p.add_argument("--out", type=Path, default=Path(__file__).parent / "fixtures",
                   help="Output directory (default: benchmark/data/fixtures)")
    args = p.parse_args()
    generate(args.out)
