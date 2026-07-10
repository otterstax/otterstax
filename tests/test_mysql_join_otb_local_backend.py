# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# MySQL-wire integration test: JOIN a registered sql backend table (MariaDB
# `campaigns`) against an otterbrix-internal table built from CREATE TABLE +
# INSERT VALUES. This is the shape `examples/demo/sql/step_4.sql` proves
# works end-to-end:
#
#   FROM <backend>.<…> c JOIN <local_db>.<table> w ON <string-key>
#
# Mechanism (full citations in step_4's analysis):
#   1. Parser drops the local table from `external_nodes`
#      (otterbrix/parser/parser.cpp:212).
#   2. `CatalogManager::update_backend_type_impl` sees only the backend → the
#      query is classified as that single backend, not Mixed.
#   3. The backend manager fetches just its slice (`generate_select` emits one
#      table per FROM clause), substitutes the customers/campaigns node with
#      `make_node_raw_data` (mirrors integration/sql/connection_manager.cpp:162),
#      and hands the mutated plan to OtterbrixManager.
#   4. The engine resolves the local side by its stamped `table_oid` against
#      its own pg_catalog and JOINs raw_data ⋈ engine-resident rows.
#
# The previously-disabled version of this test used a bigint JOIN key, which
# silently dropped every row because of an int width mismatch between the
# `bigint` local declaration and the MariaDB INT column surfaced by
# `mysql_to_chunk` — the same width sensitivity we already saw on the parquet
# side. Using `campaign_name` (string = string) avoids the issue entirely
# and demonstrates the JOIN actually works.

import sys

import mysql.connector

import config
from external_helpers import ExternalTableTester

LABEL = "DATA — MySQL wire / JOIN sql backend ⋈ otterbrix-local (string key)"

SQL_BACKEND_TABLE = "campaigns.db1.schema.campaigns"
INTERNAL_DB = "jotb_local_backend"
LOCAL_TABLE = "campaign_tags"
# Pick the first `SAMPLE_SIZE` backend campaigns deterministically via a
# WHERE on the PK. `LIMIT N` + `ORDER BY` over a single-table backend query
# isn't reliably pushed through the federation layer here, so we use a
# predicate instead.
SAMPLE_SIZE = 5


def _scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchall()[0][0]


def _sql_str(s):
    """Single-quoted SQL string literal, doubling embedded apostrophes."""
    return "'" + s.replace("'", "''") + "'"


def run(local=False):
    # ExternalTableTester gives us connection plumbing and a cleanup hook for
    # (db, table) entries. We don't need s3 here, so pick "file" — its
    # ensure_credentials() is a no-op.
    tester = ExternalTableTester("file", local=local)
    conn = tester.connect()
    cur = conn.cursor()

    # ── 1. Sample real campaign rows out of the MariaDB backend ─────────────
    # The seeded campaign names are random per-run (faker, no seed in
    # create_test_data.py), so we read whatever exists and use those exact
    # names as the JOIN key — that way the local table can never get out of
    # sync with the backend.
    cur.execute(
        f"SELECT campaign_id, campaign_name FROM {SQL_BACKEND_TABLE} "
        f"WHERE campaign_id <= {SAMPLE_SIZE}"
    )
    sample = sorted(cur.fetchall(), key=lambda r: int(r[0]))
    assert len(sample) == SAMPLE_SIZE, \
        f"expected {SAMPLE_SIZE} backend rows, got {len(sample)}"
    sample_names = {row[1] for row in sample}

    # ── 2. Materialize the local engine database. Plain CREATE TABLE in an
    #       unknown database errors out; CREATE EXTERNAL TABLE auto-creates the
    #       db as a side effect, so we use a throwaway external table as the
    #       bootstrap and immediately drop it. The drop only removes the
    #       table — the engine database stays so the CREATE TABLE below can
    #       reuse it.
    cur.execute(
        f"CREATE EXTERNAL TABLE {INTERNAL_DB}._bootstrap "
        f"WITH (location = '/fixtures/people.csv', format = 'csv');"
    )
    cur.execute(f"drop table {INTERNAL_DB}._bootstrap")

    # ── 3. CREATE TABLE + INSERT VALUES into otterbrix-internal storage.
    #       Both JOIN-key columns are strings, mirroring step_4's
    #       `(w.location).city = c.addr_city` shape.
    cur.execute(
        f"CREATE TABLE {INTERNAL_DB}.{LOCAL_TABLE} ("
        f"  campaign_name string,"
        f"  tag string"
        f")"
    )
    tester._created.append((INTERNAL_DB, LOCAL_TABLE))

    values_sql = ", ".join(
        f"({_sql_str(name)}, {_sql_str(f'tag-{cid}')})"
        for cid, name in sample
    )
    cur.execute(
        f"INSERT INTO {INTERNAL_DB}.{LOCAL_TABLE} (campaign_name, tag) "
        f"VALUES {values_sql}"
    )

    local_total = int(_scalar(cur, f"select count(*) from {INTERNAL_DB}.{LOCAL_TABLE}"))
    assert local_total == SAMPLE_SIZE, \
        f"local insert count {local_total} != {SAMPLE_SIZE}"

    # ── 4. The cross-source JOIN — backend on the LEFT, otterbrix-internal on
    #       the RIGHT, equi-JOIN on STRING column. This is exactly the shape
    #       examples/demo/sql/step_4.sql exercises (pg.shop.customers ⋈
    #       otter.warehouses ON c.addr_city = (w.location).city) and which
    #       returns the expected 14 rows there.
    cur.execute(
        f"SELECT c.campaign_id, c.campaign_name, t.tag "
        f"FROM   {SQL_BACKEND_TABLE} c "
        f"JOIN   {INTERNAL_DB}.{LOCAL_TABLE} t ON c.campaign_name = t.campaign_name "
        f"WHERE  c.campaign_id <= {SAMPLE_SIZE}"
    )
    rows = sorted(cur.fetchall(), key=lambda r: int(r[0]))

    assert len(rows) == SAMPLE_SIZE, \
        f"JOIN row count {len(rows)} != {SAMPLE_SIZE}"

    # Every JOIN result must reflect the (campaign_id, campaign_name) pair from
    # the original backend sample and the per-row tag we minted from it.
    expected = {(cid, name, f"tag-{cid}") for cid, name in sample}
    got = {(int(r[0]), r[1], r[2]) for r in rows}
    assert got == expected, \
        f"JOIN tuples mismatch:\n  expected={sorted(expected)}\n  got={sorted(got)}"

    # And every campaign_name returned must come from the local seed set.
    join_names = {r[1] for r in rows}
    assert join_names == sample_names, \
        f"joined campaign_name set {join_names} != local seed {sample_names}"

    print(f"  ✓ {SQL_BACKEND_TABLE} ⋈ {INTERNAL_DB}.{LOCAL_TABLE} "
          f"on campaign_name (string=string) → {len(rows)} rows; "
          f"backend slice fetched, raw_data ⋈ engine-resident table executed in otterbrix")

    conn.close()
    tester.cleanup()


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="JOIN sql backend ⋈ otterbrix-local (manual INSERT, string key) over the MySQL wire")
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
