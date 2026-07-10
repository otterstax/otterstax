# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# MySQL-wire integration test: three-origin JOIN — s3 parquet `regions` ⋈ local
# file csv `web_events` ⋈ otterbrix-internal `weights` (CREATE TABLE + INSERT
# VALUES), all keyed on campaign_id. Mirrors the `external_join_all` benchmark
# shape so a regression on either side is caught by a fast python check before
# the benchmark suite runs.
#
# All three keys are int64 (parquet/csv loaders emit int64; `weights.campaign_id`
# is declared bigint) to sidestep the silent zero-row JOIN trap documented in
# FIX_JOIN.md.

import sys

import mysql.connector

import config
from external_helpers import (
    EVENTS_PER_CAMPAIGN,
    REGIONS_PER_CAMPAIGN,
    ExternalTableTester,
)

LABEL = "DATA — MySQL wire / JOIN s3 parquet ⋈ file csv ⋈ otterbrix-local"

# Inserted by hand; campaign_ids overlap the fixture range so every weights row
# joins to REGIONS_PER_CAMPAIGN regions × EVENTS_PER_CAMPAIGN events.
LOCAL_WEIGHTS = [
    (1, "alpha", 1.5),
    (2, "beta", 2.0),
    (3, "gamma", 2.5),
    (4, "delta", 3.0),
    (5, "epsilon", 3.5),
]
EXPECTED_JOIN_ROWS = (
    len(LOCAL_WEIGHTS) * REGIONS_PER_CAMPAIGN * EVENTS_PER_CAMPAIGN
)  # 5 × 4 × 100 = 2000

INTERNAL_DB = "jotb_local_s3_file"


def _scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchall()[0][0]


def run(local=False):
    # Use the s3 tester to register MinIO credentials; we'll borrow the same
    # connection to also create a `file`-source external table by hand.
    s3_tester = ExternalTableTester("s3", local=local)
    s3_tester.ensure_credentials()
    file_tester = ExternalTableTester("file", local=local)  # for SQL builders only

    conn = s3_tester.connect()
    try:
        cur = conn.cursor()

        # ── 1. regions.parquet from s3 → otterbrix-internal ─────────────────
        s3_tester.create_external(cur, INTERNAL_DB, "regions", "regions.parquet", "parquet")
        regions_total = int(_scalar(cur, f"select count(*) from {INTERNAL_DB}.regions"))

        # ── 2. web_events.csv from /fixtures → otterbrix-internal ───────────
        file_tester._created = s3_tester._created  # share cleanup list
        file_tester.create_external(cur, INTERNAL_DB, "web_events", "web_events.csv", "csv")
        events_total = int(_scalar(cur, f"select count(*) from {INTERNAL_DB}.web_events"))

        # ── 3. weights — plain CREATE TABLE + INSERT VALUES ─────────────────
        # bigint matches the int64 the parquet/csv loaders expose, so the
        # equi-JOIN doesn't silently drop rows (FIX_JOIN.md).
        cur.execute(
            f"CREATE TABLE {INTERNAL_DB}.weights ("
            f"  campaign_id bigint,"
            f"  label string,"
            f"  weight double"
            f")"
        )
        s3_tester._created.append((INTERNAL_DB, "weights"))
        values_sql = ", ".join(
            f"({cid}, '{label}', {weight})" for cid, label, weight in LOCAL_WEIGHTS
        )
        cur.execute(
            f"INSERT INTO {INTERNAL_DB}.weights (campaign_id, label, weight) "
            f"VALUES {values_sql}"
        )
        weights_total = int(_scalar(cur, f"select count(*) from {INTERNAL_DB}.weights"))
        assert weights_total == len(LOCAL_WEIGHTS), \
            f"weights row count {weights_total} != {len(LOCAL_WEIGHTS)}"

        # ── 4. 3-way JOIN on campaign_id ─────────────────────────────────────
        # Both joins anchored to weights — same shape as benchmark
        # external_join_all (otterbrix/benchmark/benchmarks/external_common.py).
        cur.execute(
            f"SELECT w.campaign_id, w.label,"
            f"       r.region_id, r.country,"
            f"       e.event_id, e.value"
            f"  FROM {INTERNAL_DB}.weights w"
            f"  JOIN {INTERNAL_DB}.regions r    ON w.campaign_id = r.campaign_id"
            f"  JOIN {INTERNAL_DB}.web_events e ON w.campaign_id = e.campaign_id"
        )
        rows = cur.fetchall()
        assert len(rows) == EXPECTED_JOIN_ROWS, \
            f"3-way join row count {len(rows)} != {EXPECTED_JOIN_ROWS}"

        got_campaign_ids = sorted({int(r[0]) for r in rows})
        expected_campaign_ids = sorted({cid for cid, _, _ in LOCAL_WEIGHTS})
        assert got_campaign_ids == expected_campaign_ids, \
            f"3-way join campaign_id set {got_campaign_ids} != {expected_campaign_ids}"

        print(
            f"  ✓ otterbrix-local weights({weights_total}) ⋈ "
            f"s3 parquet regions({regions_total}) ⋈ "
            f"file csv web_events({events_total}) "
            f"on campaign_id → {len(rows)} rows"
        )
    finally:
        conn.close()
        s3_tester.cleanup()


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="JOIN s3 parquet ⋈ file csv ⋈ otterbrix-local (MySQL wire)")
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
