# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Row counts reported for LOCAL (otterbrix-internal) DML over the PG wire.

Repro for the extended-protocol half of the row-count bug. The PG frontend has
two paths: the simple-query one counts rows from the payload, the extended one
counted `emitted` — a variable that only grows inside `if (column_count() > 0)`.
A DML statement has no result columns, so nothing is ever emitted and every
INSERT/UPDATE/DELETE issued through Bind/Execute reported "INSERT 0 0" while the
rows really were written.

Forcing the extended protocol is the whole point of this file: psycopg3 switches
to Bind/Execute as soon as `execute()` is given parameters, so every DML here
passes at least one. The parameterless variants stay on the simple path and are
kept as the control — they were already correct and must remain so.

Three wire shapes are covered:

  * simple query            — no parameters
  * unnamed statement       — parameters, `prepare_threshold = None`
  * named statement         — `execute(..., prepare=True)`: Parse once under a
                              server-side name, then Bind/Execute it again and
                              again. The worker drops a statement after it ran,
                              so the second Execute goes through re-prepare and
                              must still report its own count.

The last case pushes a DML over the engine's 1024-row chunk cap: the affected
count travels in a payload whose size IS the count, so a 2000-row UPDATE/DELETE
proves the carrier is not clamped to one chunk.

The counterpart for REMOTE DML (where the count has to come off the backend's
OK packet / command tag) lives in test_pg_client_pg_backend.py,
test_pg_client_mysql_backend.py and test_mysql_client_mysql_backend.py.
"""

import argparse
import sys

import psycopg

from config import get_host, PG_PORT

INTERNAL_DB = "otb_local_dml"
TABLE = "counters"

# Rows for the >1024 case; ids start high so they never collide with the
# handful of rows the small cases work on.
BULK_ROWS = 2000
BULK_FIRST_ID = 100000
BULK_BATCH = 1000                    # one INSERT statement per batch


class LocalDmlRowCountTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.conn = None
        self.database_created = False
        self.table_created = False

    # ── helpers ─────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def _connect(self):
        return psycopg.connect(host=self.host, port=PG_PORT, user="testuser",
                               password="testpass", dbname=INTERNAL_DB, autocommit=True)

    def setup(self):
        self.conn = self._connect()
        # The shared connection stays on the two unnamed shapes; the named
        # statement case opens its own connection with server-side prepare on.
        self.conn.prepare_threshold = None
        with self.conn.cursor() as cur:
            # The database is dropped on cleanup, so a failure here is a real
            # leak from an earlier run and must surface, not be skipped.
            cur.execute(f"CREATE DATABASE {INTERNAL_DB}")
            self.database_created = True
            cur.execute(f"CREATE TABLE {INTERNAL_DB}.{TABLE} (id bigint, name string)")
            self.table_created = True

    def cleanup(self):
        if self.conn is None:
            return
        try:
            with self.conn.cursor() as cur:
                if self.table_created:
                    cur.execute(f"DROP TABLE {INTERNAL_DB}.{TABLE}")
                if self.database_created:
                    cur.execute(f"DROP DATABASE {INTERNAL_DB}")
        finally:
            self.conn.close()

    def _row_total(self, cur):
        cur.execute(f"SELECT COUNT(*) FROM {INTERNAL_DB}.{TABLE}")
        return int(cur.fetchall()[0][0])

    def _insert_bulk(self, cur, name):
        """Insert BULK_ROWS rows in BULK_BATCH-sized statements; returns the ids."""
        ids = list(range(BULK_FIRST_ID, BULK_FIRST_ID + BULK_ROWS))
        for start in range(0, BULK_ROWS, BULK_BATCH):
            batch = ids[start:start + BULK_BATCH]
            values = ", ".join(f"({i}, '{name}')" for i in batch)
            cur.execute(f"INSERT INTO {INTERNAL_DB}.{TABLE} (id, name) VALUES {values}")
            self.assert_equal(cur.rowcount, len(batch), f"bulk INSERT batch at {start} row count")
        return ids

    # ── cases ───────────────────────────────────────────────────────────────
    def test_insert_extended_reports_row_count(self):
        """INSERT through Bind/Execute reports the rows it wrote, not 0."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {INTERNAL_DB}.{TABLE} (id, name) VALUES (%s, %s), (%s, %s), (%s, %s)",
                (1, "one", 2, "two", 3, "three"),
            )
            self.assert_equal(cur.rowcount, 3, "extended-protocol INSERT row count")
            self.assert_equal(cur.statusmessage, "INSERT 0 3", "extended-protocol INSERT tag")
            self.assert_equal(self._row_total(cur), 3, "rows must actually be in the table")

    def test_update_extended_reports_row_count(self):
        """UPDATE through Bind/Execute reports the rows it changed."""
        with self.conn.cursor() as cur:
            cur.execute(f"UPDATE {INTERNAL_DB}.{TABLE} SET name = %s WHERE id = %s", ("renamed", 2))
            self.assert_equal(cur.rowcount, 1, "extended-protocol UPDATE row count")
            self.assert_equal(cur.statusmessage, "UPDATE 1", "extended-protocol UPDATE tag")

    def test_delete_extended_reports_row_count(self):
        """DELETE through Bind/Execute reports the rows it removed."""
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {INTERNAL_DB}.{TABLE} WHERE id = %s", (3,))
            self.assert_equal(cur.rowcount, 1, "extended-protocol DELETE row count")
            self.assert_equal(cur.statusmessage, "DELETE 1", "extended-protocol DELETE tag")
            self.assert_equal(self._row_total(cur), 2, "one row must be gone")

    def test_dml_matching_nothing_reports_zero(self):
        """A DML that matches no row reports 0 — the fix must not invent rows."""
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {INTERNAL_DB}.{TABLE} WHERE id = %s", (9999,))
            self.assert_equal(cur.rowcount, 0, "DELETE matching nothing")
            self.assert_equal(self._row_total(cur), 2, "nothing may be removed")

    def test_simple_protocol_still_reports_row_count(self):
        """Control: the parameterless (simple-query) path was always correct."""
        with self.conn.cursor() as cur:
            cur.execute(f"INSERT INTO {INTERNAL_DB}.{TABLE} (id, name) VALUES (10, 'ten'), (11, 'eleven')")
            self.assert_equal(cur.rowcount, 2, "simple-protocol INSERT row count")
            self.assert_equal(self._row_total(cur), 4, "both rows must be in the table")

    def test_select_still_reports_emitted_rows(self):
        """Control: SELECT must keep reporting rows WRITTEN TO THE WIRE.

        The fix reads the payload size only when there are no columns, so a
        SELECT keeps counting what it actually emitted.
        """
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT id, name FROM {INTERNAL_DB}.{TABLE} WHERE id < %s", (100,))
            rows = cur.fetchall()
            self.assert_equal(len(rows), 4, "SELECT must return every row")
            self.assert_equal(cur.rowcount, 4, "SELECT row count")

    def test_named_statement_reports_row_count(self):
        """A named (server-side prepared) statement reports its count on every Execute."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                insert = f"INSERT INTO {INTERNAL_DB}.{TABLE} (id, name) VALUES (%s, %s)"
                # First Execute: Parse under a name, Bind, Execute.
                cur.execute(insert, (20, "twenty"), prepare=True)
                self.assert_equal(cur.rowcount, 1, "named-statement INSERT row count (first Execute)")
                self.assert_equal(cur.statusmessage, "INSERT 0 1", "named-statement INSERT tag")
                # Second Execute of the SAME statement name: the worker dropped
                # it after the first run, so this goes through re-prepare.
                cur.execute(insert, (21, "twenty-one"), prepare=True)
                self.assert_equal(cur.rowcount, 1, "named-statement INSERT row count (re-executed)")
                self.assert_equal(cur.statusmessage, "INSERT 0 1", "named-statement INSERT tag (re-executed)")

                update = f"UPDATE {INTERNAL_DB}.{TABLE} SET name = %s WHERE id >= %s"
                cur.execute(update, ("named", 20), prepare=True)
                self.assert_equal(cur.rowcount, 2, "named-statement UPDATE row count")
                self.assert_equal(cur.statusmessage, "UPDATE 2", "named-statement UPDATE tag")
                cur.execute(update, ("named-again", 21), prepare=True)
                self.assert_equal(cur.rowcount, 1, "named-statement UPDATE row count (re-executed)")

                delete = f"DELETE FROM {INTERNAL_DB}.{TABLE} WHERE id = %s"
                cur.execute(delete, (20,), prepare=True)
                self.assert_equal(cur.rowcount, 1, "named-statement DELETE row count")
                self.assert_equal(cur.statusmessage, "DELETE 1", "named-statement DELETE tag")
                cur.execute(delete, (21,), prepare=True)
                self.assert_equal(cur.rowcount, 1, "named-statement DELETE row count (re-executed)")
                cur.execute(delete, (21,), prepare=True)
                self.assert_equal(cur.rowcount, 0, "named-statement DELETE matching nothing (re-executed)")
                self.assert_equal(self._row_total(cur), 4, "the named-statement rows must be gone")

    def test_dml_spanning_chunks_reports_row_count(self):
        """UPDATE/DELETE touching 2000 rows report 2000, not the 1024-row chunk cap."""
        with self.conn.cursor() as cur:
            before = self._row_total(cur)
            self._insert_bulk(cur, "bulk")
            self.assert_equal(self._row_total(cur), before + BULK_ROWS, "bulk rows must be in the table")

            cur.execute(f"UPDATE {INTERNAL_DB}.{TABLE} SET name = %s WHERE id >= %s",
                        ("bulk-updated", BULK_FIRST_ID))
            self.assert_equal(cur.rowcount, BULK_ROWS, "extended-protocol UPDATE over 2000 rows")
            self.assert_equal(cur.statusmessage, f"UPDATE {BULK_ROWS}", "UPDATE tag over 2000 rows")

            cur.execute(f"UPDATE {INTERNAL_DB}.{TABLE} SET name = 'bulk-simple' WHERE id >= {BULK_FIRST_ID}")
            self.assert_equal(cur.rowcount, BULK_ROWS, "simple-protocol UPDATE over 2000 rows")

            cur.execute(f"DELETE FROM {INTERNAL_DB}.{TABLE} WHERE id >= %s", (BULK_FIRST_ID,))
            self.assert_equal(cur.rowcount, BULK_ROWS, "extended-protocol DELETE over 2000 rows")
            self.assert_equal(cur.statusmessage, f"DELETE {BULK_ROWS}", "DELETE tag over 2000 rows")
            self.assert_equal(self._row_total(cur), before, "every bulk row must be gone")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_insert_extended_reports_row_count()
            self.test_update_extended_reports_row_count()
            self.test_delete_extended_reports_row_count()
            self.test_dml_matching_nothing_reports_zero()
            self.test_simple_protocol_still_reports_row_count()
            self.test_select_still_reports_emitted_rows()
            self.test_named_statement_reports_row_count()
            self.test_dml_spanning_chunks_reports_row_count()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(
        description="Row counts for local otterbrix DML over the PostgreSQL wire")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()

    tests = LocalDmlRowCountTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - local otterbrix DML row counts: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - local otterbrix DML row counts\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
