# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# Focused MySQL-wire integration test for the ndjson format via the file source.
# Loads campaigns.ndjson (50 rows, one per campaign_id) as an external table,
# verifies row count, schema, a per-campaign aggregate, and JOINs against the
# parquet fixture (regions.parquet) on the shared campaign_id key.
#
# Complements test_mysql_file.py (parquet + csv only) — see external_helpers.py.

import sys

import mysql.connector

import config
from external_helpers import (
    NUM_CAMPAIGNS,
    REGIONS_PER_CAMPAIGN,
    ExternalTableTester,
)

NDJSON_FILE = "campaigns.ndjson"
NDJSON_COLUMNS = ["campaign_id", "campaign_name", "budget", "status"]
PARQUET_FILE = "regions.parquet"

LABEL = "DATA — MySQL wire / file external table (ndjson)"


def _scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchall()[0][0]


def run(local=False):
    tester = ExternalTableTester("file", local=local)
    conn = tester.connect()
    db = "extndjson_file"
    try:
        cur = conn.cursor()

        # CREATE EXTERNAL TABLE … WITH (location='/fixtures/campaigns.ndjson', format='ndjson')
        tester.create_external(cur, db, "campaigns", NDJSON_FILE, "ndjson")

        # Schema: columns exposed exactly as declared in the ndjson rows.
        cur.execute(f"select * from {db}.campaigns")
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]
        if columns != NDJSON_COLUMNS:
            raise AssertionError(f"columns {columns} != {NDJSON_COLUMNS}")
        if len(rows) != NUM_CAMPAIGNS:
            raise AssertionError(f"row count {len(rows)} != {NUM_CAMPAIGNS}")

        # Aggregate: campaign_id range is dense [1..NUM_CAMPAIGNS].
        cur.execute(f"select min(campaign_id), max(campaign_id) from {db}.campaigns")
        lo, hi = cur.fetchall()[0]
        if int(lo) != 1 or int(hi) != NUM_CAMPAIGNS:
            raise AssertionError(f"campaign_id range [{lo},{hi}] != [1,{NUM_CAMPAIGNS}]")

        # COPY round-trip: write the ndjson out, re-read it, count must match.
        target = tester.copy_to(cur, db, "campaigns", "campaigns", "ndjson")
        tester.create_external_from(cur, db, "campaigns_rt", target, "ndjson")
        rt_count = int(_scalar(cur, f"select count(*) from {db}.campaigns_rt"))
        if rt_count != NUM_CAMPAIGNS:
            raise AssertionError(f"COPY round-trip count {rt_count} != {NUM_CAMPAIGNS}")

        # JOIN against the parquet fixture on the shared campaign_id key.
        tester.create_external(cur, db, "regions", PARQUET_FILE, "parquet")
        join_count = int(_scalar(
            cur,
            f"select count(*) from {db}.campaigns c "
            f"join {db}.regions r on c.campaign_id = r.campaign_id"))
        expected = NUM_CAMPAIGNS * REGIONS_PER_CAMPAIGN
        if join_count != expected:
            raise AssertionError(f"campaigns⋈regions count {join_count} != {expected}")

        print(f"  ✓ file/campaigns (ndjson): {len(rows)} rows, "
              f"COPY round-trip OK, campaigns⋈regions = {join_count}")
    finally:
        conn.close()
        tester.cleanup()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="External-table data test (file/ndjson) over the MySQL wire")
    parser.add_argument("--local", action="store_true", help="Use 0.0.0.0 instead of test-otterstax")
    args = parser.parse_args()
    try:
        run(local=args.local)
        print("\n" + "=" * 70)
        print(f"\033[92m✅ ALL TESTS PASSED - {LABEL}\033[0m")
        print("=" * 70)
        return 0
    except Exception as exc:  # noqa: BLE001
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - {LABEL}\033[0m")
        print(f"\033[91m{exc}\033[0m")
        print("=" * 70)
        return 1
    finally:
        print("Test completed.")


if __name__ == "__main__":
    sys.exit(main())
