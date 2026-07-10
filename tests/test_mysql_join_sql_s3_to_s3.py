# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# End-to-end MySQL-wire integration test for the full federated → external cycle:
#
#   1. CREATE EXTERNAL TABLE for an s3 parquet fixture (regions.parquet)
#         → otterbrix loads it into its own internal storage
#   2. Stage selected MariaDB `campaigns` rows into an otterbrix-internal
#      table (workaround for a JOIN-key width mismatch — see below).
#   3. CREATE TABLE + INSERT INTO … SELECT to persist the JOIN result.
#   4. COPY (SELECT * FROM internal_table) TO 's3://...' WITH format='csv' to
#      dump the persisted result back out to s3.
#   5. CREATE EXTERNAL TABLE over the dumped csv to read it back and verify
#      the row count + campaign_id set.
#
# Why the staging step is still here (FIX_JOIN.md has the full story):
# A direct `JOIN MariaDB.campaigns ⋈ engine.regions ON campaign_id` is a plan
# shape the engine DOES support (see examples/demo/sql/step_4.sql, which runs
# exactly this shape against PG every demo run and returns 14 rows). The
# blocker on this particular path is the JOIN-key type width:
#   - regions.parquet's campaign_id is int64 (parquet loader output).
#   - MariaDB's campaigns.campaign_id is INT (int32), which `mysql_to_chunk`
#     surfaces unchanged in the raw_data chunk it inlines for the backend
#     slice.
# Equi-JOIN across int32 and int64 silently drops every row in otterbrix,
# the same failure mode we already burnt-in for `int` vs `bigint` local
# declarations against parquet. Staging the backend rows into a `bigint`
# engine table first normalises the width and the in-engine JOIN goes
# through. Replace this with a direct backend-on-the-left JOIN once the
# backend-translator width can be coerced (CAST in the JOIN predicate, or a
# wider mysql_to_chunk mapping).

import sys

import mysql.connector

import config
from external_helpers import (
    S3_ALIAS,
    S3_BUCKET,
    ExternalTableTester,
)

LABEL = "DATA — MySQL wire / JOIN sql-backend ⋈ s3 parquet, persist, dump to s3 csv"

# Three campaigns × four regions per campaign = 12 rows. Keeping the JOIN
# small + deterministic so the count assertion is exact.
JOIN_CAMPAIGN_IDS = (1, 2, 3)
EXPECTED_JOIN_ROWS = len(JOIN_CAMPAIGN_IDS) * 4

# Source: campaigns lives in MariaDB1 (alias 'campaigns', db 'db1', schema 'schema').
SQL_BACKEND_TABLE = "campaigns.db1.schema.campaigns"

# Otterbrix-internal database. Used for BOTH the parquet-loaded regions and the
# persisted JOIN result — CREATE EXTERNAL TABLE auto-creates the database, then
# plain CREATE TABLE can reuse it. The name must not collide with any registered
# backend alias (campaigns/products/orders) or other external test db.
INTERNAL_DB = "joinflow_otb"

# Round-trip verification reads the dumped csv back from s3 as a fresh external
# table; this database is auto-created by that CREATE EXTERNAL TABLE call.
VERIFY_DB = "joinverify_s3"


def _scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchall()[0][0]


def run(local=False):
    tester = ExternalTableTester("s3", local=local)
    tester.ensure_credentials()

    conn = tester.connect()
    cur = conn.cursor()

    s3_result_csv = f"s3://{S3_BUCKET}/exported/joined_sql_s3.csv"

    in_list = ",".join(str(cid) for cid in JOIN_CAMPAIGN_IDS)

    # ── 1. Load regions.parquet from s3 → otterbrix internal storage ────────
    tester.create_external(cur, INTERNAL_DB, "regions", "regions.parquet", "parquet")
    regions_total = int(_scalar(cur, f"select count(*) from {INTERNAL_DB}.regions"))
    assert regions_total == 200, f"regions load size {regions_total} != 200"

    # ── 2. Stage selected backend rows into otterbrix-internal storage so the
    #       JOIN can run entirely within the engine. campaign_id is declared
    #       bigint so its width matches the int64 column type the parquet
    #       loader exposes on regions — a width mismatch on the JOIN key
    #       causes the equi-JOIN to drop every row silently.
    cur.execute(
        f"CREATE TABLE {INTERNAL_DB}.campaigns_stg ("
        f"  campaign_id bigint,"
        f"  campaign_name string"
        f")"
    )
    tester._created.append((INTERNAL_DB, "campaigns_stg"))
    cur.execute(
        f"INSERT INTO {INTERNAL_DB}.campaigns_stg (campaign_id, campaign_name) "
        f"SELECT campaign_id, campaign_name FROM {SQL_BACKEND_TABLE} "
        f"WHERE campaign_id IN ({in_list})"
    )
    staged = int(_scalar(cur, f"select count(*) from {INTERNAL_DB}.campaigns_stg"))
    assert staged == len(JOIN_CAMPAIGN_IDS), \
        f"backend → internal staging count {staged} != {len(JOIN_CAMPAIGN_IDS)}"

    # ── 3. Create the destination table for the JOIN result ─────────────────
    cur.execute(
        f"CREATE TABLE {INTERNAL_DB}.campaign_regions ("
        f"  campaign_id bigint,"
        f"  campaign_name string,"
        f"  region_id bigint,"
        f"  region_name string,"
        f"  country string"
        f")"
    )
    tester._created.append((INTERNAL_DB, "campaign_regions"))

    # ── 4. JOIN the staged backend rows against the s3-sourced regions table
    #       (both now live in otterbrix-internal storage) and persist the
    #       result.
    cur.execute(
        f"INSERT INTO {INTERNAL_DB}.campaign_regions "
        f"  (campaign_id, campaign_name, region_id, region_name, country) "
        f"SELECT c.campaign_id, c.campaign_name, r.region_id, r.region_name, r.country "
        f"FROM {INTERNAL_DB}.campaigns_stg c "
        f"JOIN {INTERNAL_DB}.regions r ON c.campaign_id = r.campaign_id"
    )

    persisted = int(_scalar(cur, f"select count(*) from {INTERNAL_DB}.campaign_regions"))
    assert persisted == EXPECTED_JOIN_ROWS, \
        f"persisted JOIN count {persisted} != {EXPECTED_JOIN_ROWS}"

    # Spot-check: every row's campaign_id is in the requested set.
    cur.execute(f"select distinct campaign_id from {INTERNAL_DB}.campaign_regions")
    got_ids = sorted(int(r[0]) for r in cur.fetchall())
    assert got_ids == sorted(JOIN_CAMPAIGN_IDS), \
        f"persisted campaign_ids {got_ids} != {sorted(JOIN_CAMPAIGN_IDS)}"

    # ── 4. Dump otterbrix-internal table to s3 as csv ────────────────────────
    cur.execute(
        f"COPY (SELECT * FROM {INTERNAL_DB}.campaign_regions) "
        f"TO '{s3_result_csv}' "
        f"WITH (s3_alias = '{S3_ALIAS}', format = 'csv')"
    )

    # ── 5. Read the dumped csv back from s3 to verify the full cycle ─────────
    tester.create_external_from(cur, VERIFY_DB, "joined_csv", s3_result_csv, "csv")

    round_tripped = int(_scalar(cur, f"select count(*) from {VERIFY_DB}.joined_csv"))
    assert round_tripped == EXPECTED_JOIN_ROWS, \
        f"s3 csv round-trip count {round_tripped} != {EXPECTED_JOIN_ROWS}"

    cur.execute(f"select distinct campaign_id from {VERIFY_DB}.joined_csv")
    rt_ids = sorted(int(r[0]) for r in cur.fetchall())
    assert rt_ids == sorted(JOIN_CAMPAIGN_IDS), \
        f"s3 csv round-trip campaign_ids {rt_ids} != {sorted(JOIN_CAMPAIGN_IDS)}"

    print(
        f"  ✓ regions(s3 parquet, {regions_total}) ⋈ {SQL_BACKEND_TABLE} "
        f"on campaign_id IN {JOIN_CAMPAIGN_IDS} "
        f"→ otterbrix.{INTERNAL_DB}.campaign_regions ({persisted} rows) "
        f"→ {s3_result_csv} → external csv round-trip OK ({round_tripped} rows)"
    )

    conn.close()
    tester.cleanup()


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="JOIN sql-backend ⋈ s3 parquet, persist, dump to s3 csv (MySQL wire)")
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
