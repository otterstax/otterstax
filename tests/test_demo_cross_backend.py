# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
End-to-end integration test for the six demo queries with all three backends
(mysql.bill, pg.shop, ch.ev) live.

Reads SQL from `demo/*.sql` — single source of truth shared with the
interactive psql session. If you change a query in demo/, the test changes
with it.

Pre-conditions:
  * Demo backends are up and seeded (./demo-up.sh OR ./demo-bench-up.sh +
    local server).
  * Connections registered (mysql / pg / ch aliases).
  * For step 3 to be re-runnable, run cleanup.sql or restart the server
    between runs (otterbrix DROP DATABASE has no IF EXISTS).

Run:
    python tests/test_demo_cross_backend.py [--local]
"""

import argparse
import shutil
import sys
from pathlib import Path

import psycopg

import config

DEMO_DIR = Path(__file__).resolve().parent.parent / "demo"


def make_conn(local: bool) -> psycopg.Connection:
    return psycopg.connect(
        host=config.get_host(local),
        port=config.PG_PORT,
        user="demo",
        password="demo",
        autocommit=True,
    )


def load_sql(name: str) -> str:
    return (DEMO_DIR / name).read_text()


def run(conn, sql: str):
    """Execute a query, return list of rows. Empty list for DDL/DML."""
    with conn.cursor() as cur:
        cur.execute(sql)
        if cur.description is None:
            return []
        return cur.fetchall()


# --------------------------------------------------------------------------
# Step functions — each loads its SQL from demo/ and asserts shape.
# --------------------------------------------------------------------------


def step_1(conn):
    print("\n-- step 1: cross-source JOIN with date-bound derived table")
    rows = run(conn, load_sql("step_1.sql"))
    assert len(rows) > 0, "step 1: no rows — check spike-day order seeding"
    # ORDER BY revenue DESC — verify monotonically non-increasing.
    revenues = [r[2] for r in rows]
    assert revenues == sorted(revenues, reverse=True), "step 1: revenue not sorted DESC"
    print(f"  ✓ step 1: {len(rows)} categories, top revenue={rows[0][2]}")


def step_2(conn):
    print("\n-- step 2: derived aggregates + HAVING alias + outer NOT LIKE")
    rows = run(conn, load_sql("step_2.sql"))
    assert len(rows) > 0, "step 2: no rows — check heavy_customers top-up"
    assert all(r[2] >= 3 for r in rows), "step 2: HAVING orders_cnt >= 3 not enforced"
    assert all("@test." not in (r[1] or "") for r in rows), "step 2: NOT LIKE leaked"
    # ORDER BY agg.avg_check DESC
    avg_checks = [r[3] for r in rows]
    assert avg_checks == sorted(avg_checks, reverse=True), "step 2: avg_check not sorted DESC"
    print(f"  ✓ step 2: {len(rows)} customers (top avg_check={rows[0][3]})")


def step_3(conn):
    """3a (DDL) + 3b (insert) + 3c (local SELECT) + 3d (3-source JOIN)."""
    print("\n-- step 3a: DDL — CREATE TYPE + CREATE TABLE")
    run(conn, load_sql("step_3a_ddl.sql"))
    print("  ✓ step 3a")

    print("\n-- step 3b: INSERT 3 warehouse rows")
    run(conn, load_sql("step_3b_insert.sql"))
    print("  ✓ step 3b")

    print("\n-- step 3c: local SELECT (struct).field / arr[n]")
    rows = run(conn, load_sql("step_3c_select.sql"))
    # Only TLV-1 has country='IL' AND photo IS NULL.
    assert len(rows) == 1, f"step 3c: expected 1 row (TLV-1 only), got {len(rows)}"
    (id_, city, w_2, bc_1, prio_2) = rows[0]
    assert str(id_) == "11111111-1111-1111-1111-111111111111", f"step 3c: id mismatch: {id_}"
    assert city == "Tel Aviv", f"step 3c: city mismatch: {city!r}"
    assert w_2 == 20, f"step 3c: weights[2] expected 20, got {w_2}"
    assert bc_1 == "BC-001", f"step 3c: barcodes[1] expected 'BC-001', got {bc_1!r}"
    assert prio_2 == 2, f"step 3c: priority[2] expected 2, got {prio_2}"
    print(f"  ✓ step 3c: row={rows[0]}")

    print("\n-- step 3d: 3-source JOIN with struct/array predicates")
    rows = run(conn, load_sql("step_3d_main.sql"))
    assert len(rows) > 0, "step 3d: no rows — check sessions tags[1]='campaign' seeding"
    # Categories from PRODUCTS — should be a subset of the 8 known.
    known = {"Electronics", "Clothing", "Food", "Home", "Sports", "Books", "Toys", "Beauty"}
    for r in rows:
        assert r[0] in known, f"step 3d: unknown category {r[0]!r}"
    print(f"  ✓ step 3d: {len(rows)} categories")


def step_4(conn):
    print("\n-- step 4: struct.* + JOIN ON struct field + ENUM cast")
    rows = run(conn, load_sql("step_4.sql"))
    assert len(rows) > 0, "step 4: no rows — check gold-customer-in-warehouse-city seeding"
    # Columns: name + addr.* (city, country, zip) + warehouse + location.* = 8.
    assert len(rows[0]) == 8, f"step 4: expected 8 columns, got {len(rows[0])}"
    # City of customer must equal city of joined warehouse.
    for r in rows:
        c_city, w_city = r[1], r[5]
        assert c_city == w_city, f"step 4: JOIN-by-city violated: {c_city!r} != {w_city!r}"
    # ORDER BY (c.addr).city — verify non-decreasing.
    cities = [r[1] for r in rows]
    assert cities == sorted(cities), "step 4: cities not sorted ASC"
    print(f"  ✓ step 4: {len(rows)} gold-customer × warehouse rows")


def step_5(conn):
    print("\n-- step 5: 4-JOIN + array predicate + nested struct + DISTINCT")
    rows = run(conn, load_sql("step_5.sql"))
    assert len(rows) > 0, "step 5: no rows — check session category-correlation seeding"
    # Predicate: s.tags[1] = p.category — every result row must satisfy.
    for r in rows:
        browsed_cat, bought_cat = r[1], r[2]
        assert browsed_cat == bought_cat, \
            f"step 5: tags[1] != category: {browsed_cat!r} != {bought_cat!r}"
    # ORDER BY s.tags[1] — verify non-decreasing.
    tags = [r[1] for r in rows]
    assert tags == sorted(tags), "step 5: tags[1] not sorted ASC"
    print(f"  ✓ step 5: {len(rows)} distinct correlated rows")


def step_6(conn):
    print("\n-- step 6: federated MRR — CASE WHEN inside SUM, LEFT JOIN, HAVING alias")
    rows = run(conn, load_sql("step_6.sql"))
    assert len(rows) > 0, "step 6: no rows — check tier-amount distribution"
    assert all(r[1] > 1000 for r in rows), "step 6: HAVING mrr > 1000 not enforced"
    # ORDER BY mrr DESC
    mrrs = [r[1] for r in rows]
    assert mrrs == sorted(mrrs, reverse=True), "step 6: mrr not sorted DESC"
    # tier ENUM ∈ {bronze, silver, gold} — but gold-only is plausible if HAVING
    # filters bronze out. Just sanity-check the values.
    for r in rows:
        assert r[0] in ("bronze", "silver", "gold"), f"step 6: unknown tier {r[0]!r}"
    print(f"  ✓ step 6: {len(rows)} tiers passing HAVING (top mrr={rows[0][1]})")


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


def main_test():
    parser = argparse.ArgumentParser(description="otterstax demo cross-backend test")
    parser.add_argument("--local", action="store_true",
                        help="Use local host instead of test-otterstax")
    args = parser.parse_args()

    host = config.get_host(args.local)
    print(f"connecting to postgres wire @ {host}:{config.PG_PORT}")
    conn = make_conn(local=args.local)
    try:
        step_1(conn)
        step_2(conn)
        step_3(conn)
        step_4(conn)
        step_5(conn)
        step_6(conn)

        print("\n" + "=" * 70)
        print("\033[92m✅ demo cross-backend: ALL 6 STEPS PASSED\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ demo cross-backend: FAILED — {type(e).__name__}: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Drop local otter database so the next run can re-create it.
        # Types are in-memory so they vanish with the server already.
        try:
            run(conn, load_sql("cleanup.sql"))
        except Exception as e:
            print(f"   (cleanup skip: {e})")
        conn.close()
        # Wipe on-disk state — otterbrix WAL recovery currently asserts on
        # composite types from the previous run.
        shutil.rmtree("/tmp/test_collection_sql", ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main_test())
