# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# MySQL-wire integration test: JOIN data that was inserted directly into
# otterbrix-internal storage (CREATE TABLE + INSERT VALUES) against a fresh
# s3-loaded external table (regions.parquet). Both sides live in otterbrix
# after the load, so the engine performs a pure local-local JOIN — this is
# the supported shape and is intended to remain green.

import sys

import mysql.connector

import config
from external_helpers import (
    S3_BUCKET,
    ExternalTableTester,
)

LABEL = "DATA — MySQL wire / JOIN otterbrix-local (manual INSERT) ⋈ s3 parquet"

# Inserted by hand into the local table; campaign_id values intentionally fall
# within regions.parquet's 1..50 range so the JOIN produces a known cardinality.
LOCAL_WEIGHTS = [
    (1, "alpha", 1.5),
    (2, "beta", 2.0),
    (3, "gamma", 2.5),
    (4, "delta", 3.0),
    (5, "epsilon", 3.5),
]
REGIONS_PER_CAMPAIGN = 4
EXPECTED_JOIN_ROWS = len(LOCAL_WEIGHTS) * REGIONS_PER_CAMPAIGN  # 5 × 4 = 20

INTERNAL_DB = "jotb_local_s3"


def _scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchall()[0][0]


def run(local=False):
    tester = ExternalTableTester("s3", local=local)
    tester.ensure_credentials()

    conn = tester.connect()
    cur = conn.cursor()

    # ── 1. Load regions.parquet from s3 → otterbrix internal storage ────────
    tester.create_external(cur, INTERNAL_DB, "regions", "regions.parquet", "parquet")
    regions_total = int(_scalar(cur, f"select count(*) from {INTERNAL_DB}.regions"))
    assert regions_total == 200, f"regions load size {regions_total} != 200"

    # ── 2. Hand-populate a second otterbrix-internal table via plain
    #       CREATE TABLE + INSERT VALUES. bigint matches the int64 the parquet
    #       loader exposes on regions.campaign_id so the equi-JOIN matches.
    cur.execute(
        f"CREATE TABLE {INTERNAL_DB}.weights ("
        f"  campaign_id bigint,"
        f"  label string,"
        f"  weight double"
        f")"
    )
    tester._created.append((INTERNAL_DB, "weights"))
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

    # ── 3. JOIN — both sides are now otterbrix-internal tables, so the engine
    #       executes the equi-JOIN entirely on its own. Each weights row joins
    #       to REGIONS_PER_CAMPAIGN regions rows for the same campaign_id.
    cur.execute(
        f"SELECT w.campaign_id, w.label, w.weight, r.region_id, r.region_name, r.country "
        f"FROM {INTERNAL_DB}.weights w "
        f"JOIN {INTERNAL_DB}.regions r ON w.campaign_id = r.campaign_id"
    )
    rows = cur.fetchall()
    assert len(rows) == EXPECTED_JOIN_ROWS, \
        f"join row count {len(rows)} != {EXPECTED_JOIN_ROWS}"

    got_campaign_ids = sorted({int(r[0]) for r in rows})
    expected_campaign_ids = sorted({cid for cid, _, _ in LOCAL_WEIGHTS})
    assert got_campaign_ids == expected_campaign_ids, \
        f"join campaign_id set {got_campaign_ids} != {expected_campaign_ids}"

    # ── 4. COPY the JOIN result out to s3 as csv, then read it back as a fresh
    #       external table to exercise the full write/read round-trip across
    #       the engine boundary.
    out_csv = f"s3://{S3_BUCKET}/exported/joined_otb_local_s3.csv"
    cur.execute(
        f"COPY ("
        f"  SELECT w.campaign_id, w.label, w.weight, r.region_id, r.region_name, r.country "
        f"  FROM {INTERNAL_DB}.weights w "
        f"  JOIN {INTERNAL_DB}.regions r ON w.campaign_id = r.campaign_id"
        f") TO '{out_csv}' WITH (s3_alias = 'miniotest', format = 'csv')"
    )
    tester.create_external_from(cur, "jotb_local_s3_rt", "joined_csv", out_csv, "csv")
    rt_count = int(_scalar(cur, "select count(*) from jotb_local_s3_rt.joined_csv"))
    assert rt_count == EXPECTED_JOIN_ROWS, \
        f"s3 csv round-trip count {rt_count} != {EXPECTED_JOIN_ROWS}"

    print(f"  ✓ otterbrix-local({weights_total}) ⋈ s3 parquet regions({regions_total}) "
          f"on campaign_id → {len(rows)} rows, s3 csv round-trip OK")

    conn.close()
    tester.cleanup()


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="JOIN otterbrix-local (manual INSERT) ⋈ s3 parquet external (MySQL wire)")
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
