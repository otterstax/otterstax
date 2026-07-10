#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Generate the richer, joinable external-table fixtures used by the s3/file
integration tests (tests/test_{schema_}mysql_{file,s3}.py via external_helpers).

Modeled on tests/create_test_data.py but at a smaller scale (<= 10k rows) and
written to files instead of databases:

  regions.parquet   dimension table   200 rows   (4 per campaign)
  web_events.csv    fact table       5000 rows   (100 per campaign)
  campaigns.ndjson  dimension table    50 rows   (1 per campaign)

All three correlate with the backend data on campaign_id (1..NUM_CAMPAIGNS), so
the external tables can be JOINed with each other and with the MariaDB/PG/CH
backends; web_events.product_id references the PostgreSQL products id range.
Per-campaign counts are fixed so the tests can assert exact join cardinalities.

All integer columns are int64 in both files so the parquet/csv tables join
without type coercion. Seeded → reproducible; re-run after changing the shape:

    python tests/minio/fixtures/generate_external_fixtures.py
"""

import csv
import json
import os
import random

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

SEED = 20260625
NUM_CAMPAIGNS = 50
REGIONS_PER_CAMPAIGN = 4    # -> 200 rows
EVENTS_PER_CAMPAIGN = 100   # -> 5000 rows
MAX_PRODUCT_ID = 150        # PostgreSQL products.product_id range (~2-5 per campaign)

COUNTRIES = ["US", "UK", "DE", "FR", "JP", "BR", "IN", "CA"]
EVENT_TYPES = ["view", "click", "purchase"]

HERE = os.path.dirname(os.path.abspath(__file__))


def generate():
    fake = Faker("en_US")
    Faker.seed(SEED)
    random.seed(SEED)

    # ── regions.parquet — campaign geo targeting (joins campaigns.campaign_id) ──
    region_id, campaign_col, region_name, country, population, ad_spend = [], [], [], [], [], []
    rid = 1
    for cid in range(1, NUM_CAMPAIGNS + 1):
        for _ in range(REGIONS_PER_CAMPAIGN):
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
    regions_path = os.path.join(HERE, "regions.parquet")
    pq.write_table(regions, regions_path)
    print(f"wrote {regions_path}: {regions.num_rows} rows, columns {regions.column_names}")

    # ── web_events.csv — web analytics fact (joins campaign_id + products.product_id) ──
    events_path = os.path.join(HERE, "web_events.csv")
    total = 0
    with open(events_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["event_id", "campaign_id", "product_id", "event_type", "session_seconds", "value"])
        eid = 1
        for cid in range(1, NUM_CAMPAIGNS + 1):
            for _ in range(EVENTS_PER_CAMPAIGN):
                writer.writerow([
                    eid,
                    cid,
                    random.randint(1, MAX_PRODUCT_ID),
                    random.choice(EVENT_TYPES),
                    random.randint(5, 1800),
                    round(random.uniform(0.0, 999.99), 2),
                ])
                eid += 1
                total += 1
    print(f"wrote {events_path}: {total} rows, columns "
          "[event_id, campaign_id, product_id, event_type, session_seconds, value]")

    # ── campaigns.ndjson — dimension table, one row per campaign (joins campaign_id) ──
    campaigns_path = os.path.join(HERE, "campaigns.ndjson")
    statuses = ["active", "paused", "completed"]
    with open(campaigns_path, "w") as fh:
        for cid in range(1, NUM_CAMPAIGNS + 1):
            json.dump({
                "campaign_id": cid,
                "campaign_name": f"{fake.company()} Campaign",
                "budget": round(random.uniform(10_000.0, 250_000.0), 2),
                "status": random.choice(statuses),
            }, fh)
            fh.write("\n")
    print(f"wrote {campaigns_path}: {NUM_CAMPAIGNS} rows, columns "
          "[campaign_id, campaign_name, budget, status]")


if __name__ == "__main__":
    generate()
