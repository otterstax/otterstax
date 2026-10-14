# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Local transactions over the PostgreSQL wire, against the live server.

Every connection owns one query session and through it one engine session, so
BEGIN / COMMIT / ROLLBACK over otterbrix-internal tables behave as in
PostgreSQL (session/CLAUDE.md, "The engine session and transactions"):

  * a ROLLBACK discards the block's writes, a COMMIT publishes them, and a
    second connection sees only what was committed;
  * an error inside a block fails it: every statement but ROLLBACK is refused
    with 25P02 and ReadyForQuery reports 'E' until the block ends; a COMMIT of
    the failed block is a rollback (CommandComplete "ROLLBACK");
  * the extended protocol follows the same block — a BEGIN sent through
    Parse/Bind/Execute leaves the connection INTRANS, and a driver that runs
    with autocommit off (psycopg opens the block itself) commits and rolls back
    for real;
  * SAVEPOINT / RELEASE / ROLLBACK TO are refused with 0A000 (otterbrix has no
    savepoints); inside a block the refusal fails it, outside it fails nothing;
  * a SET is answered "SET" without effect: inside a block it keeps the block
    open, and `SET autocommit TO off` changes no connection, its own included.

The C++ counterparts are tests/system/test_frontend_transactions.cpp and
tests/session/test_session_lifetime.cpp; this file pins the same rules through
a real driver (psycopg 3).
"""

import argparse
import sys

import psycopg
from psycopg import pq

from config import get_host, PG_PORT

DB = "txn_pg_wire"
TABLE = "items"
QUALIFIED = f"{DB}.{TABLE}"

SQLSTATE_IN_FAILED_SQL_TRANSACTION = "25P02"
SQLSTATE_FEATURE_NOT_SUPPORTED = "0A000"


class PgWireTransactionTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.admin = None
        self.database_created = False
        self.table_created = False

    # ── helpers ─────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def connect(self, autocommit=True):
        return psycopg.connect(host=self.host, port=PG_PORT, user="testuser",
                               password="testpass", dbname=DB, autocommit=autocommit)

    def ids(self, conn, low, high):
        """The ids in [low, high] the connection sees, sorted."""
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {QUALIFIED} WHERE id >= {low} AND id <= {high}")
            return sorted(int(r[0]) for r in cur.fetchall())

    def expect_error(self, cur, sql, sqlstate=None, what=""):
        """`sql` must fail; returns the error. With `sqlstate`, the error carries it."""
        try:
            cur.execute(sql)
        except psycopg.Error as e:
            if sqlstate is not None:
                self.assert_equal(e.sqlstate, sqlstate, f"{what}: sqlstate of {sql!r} ({e})")
            return e
        raise AssertionError(f"{what}: {sql!r} must fail")

    def status(self, conn):
        return conn.info.transaction_status

    # ── setup / teardown ────────────────────────────────────────────────────
    def setup(self):
        self.admin = self.connect()
        with self.admin.cursor() as cur:
            cur.execute(f"CREATE DATABASE {DB}")
            self.database_created = True
            cur.execute(f"CREATE TABLE {QUALIFIED} (id bigint, name string)")
            self.table_created = True

    def cleanup(self):
        if self.admin is None:
            return
        try:
            with self.admin.cursor() as cur:
                if self.table_created:
                    cur.execute(f"DROP TABLE {QUALIFIED}")
                if self.database_created:
                    cur.execute(f"DROP DATABASE {DB}")
        finally:
            self.admin.close()

    # ── cases ───────────────────────────────────────────────────────────────
    def test_rollback_discards_a_local_insert(self):
        """BEGIN / INSERT / ROLLBACK: the row is seen inside the block only, and is gone after it."""
        with self.connect() as a, self.connect() as b:
            with a.cursor() as cur:
                cur.execute("BEGIN")
                self.assert_equal(self.status(a), pq.TransactionStatus.INTRANS, "BEGIN opens a block")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (100, 'rolled'), (101, 'rolled')")
                self.assert_equal(self.ids(a, 100, 109), [100, 101], "the block sees its own rows")
                self.assert_equal(self.ids(b, 100, 109), [], "another connection does not see uncommitted rows")
                cur.execute("ROLLBACK")
                self.assert_equal(cur.statusmessage, "ROLLBACK", "ROLLBACK tag")
                self.assert_equal(self.status(a), pq.TransactionStatus.IDLE, "ROLLBACK ends the block")
            self.assert_equal(self.ids(a, 100, 109), [], "ROLLBACK discards the rows")
            self.assert_equal(self.ids(b, 100, 109), [], "and nobody ever sees them")
        print("  BEGIN / INSERT / ROLLBACK: nothing kept ✓")

    def test_commit_publishes_to_another_connection(self):
        """A committed block is seen by another connection; until COMMIT it is not."""
        with self.connect() as a, self.connect() as b:
            with a.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (110, 'kept')")
                self.assert_equal(self.ids(b, 110, 119), [], "before COMMIT")
                cur.execute("COMMIT")
                self.assert_equal(cur.statusmessage, "COMMIT", "COMMIT tag")
            self.assert_equal(self.ids(b, 110, 119), [110], "after COMMIT")
        print("  BEGIN / INSERT / COMMIT: visible to another connection after COMMIT ✓")

    def test_two_open_blocks_see_only_committed_rows_of_each_other(self):
        """Two connections inside blocks at once: each sees its own rows, never the other's uncommitted ones."""
        with self.connect() as a, self.connect() as b:
            with a.cursor() as ca, b.cursor() as cb:
                ca.execute("BEGIN")
                cb.execute("BEGIN")
                ca.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (120, 'a')")
                cb.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (121, 'b')")
                self.assert_equal(self.ids(a, 120, 129), [120], "a sees its own row and not b's")
                self.assert_equal(self.ids(b, 120, 129), [121], "b sees its own row and not a's")
                ca.execute("COMMIT")
                cb.execute("ROLLBACK")
            self.assert_equal(self.ids(a, 120, 129), [120], "a's committed row stays, b's rolled back one never shows")
            self.assert_equal(self.ids(b, 120, 129), [120], "b sees a's committed row once its own block ended")
        print("  two concurrent blocks: only committed rows cross ✓")

    def test_error_fails_the_block_until_rollback(self):
        """An error inside a block: 25P02 for every later statement, status INERROR, ROLLBACK ends it."""
        with self.connect() as a:
            with a.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (130, 'lost')")
                self.expect_error(cur, f"SELECT * FROM {DB}.no_such_table", what="a missing table")
                self.assert_equal(self.status(a), pq.TransactionStatus.INERROR, "the error failed the block")
                for sql in (f"SELECT count(*) FROM {QUALIFIED}",
                            f"INSERT INTO {QUALIFIED} (id, name) VALUES (131, 'refused')"):
                    self.expect_error(cur, sql, SQLSTATE_IN_FAILED_SQL_TRANSACTION, "inside the failed block")
                    self.assert_equal(self.status(a), pq.TransactionStatus.INERROR, "the block stays failed")
                cur.execute("ROLLBACK")
                self.assert_equal(self.status(a), pq.TransactionStatus.IDLE, "ROLLBACK ends the failed block")
            self.assert_equal(self.ids(a, 130, 139), [], "nothing of the failed block was kept")
        print("  error in a block: 25P02 until ROLLBACK ✓")

    def test_commit_of_a_failed_block_is_a_rollback(self):
        """COMMIT of a failed block answers the ROLLBACK tag, ends the block and keeps nothing."""
        with self.connect() as a:
            with a.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (140, 'lost')")
                self.expect_error(cur, "SELEC broken", what="a statement that does not parse")
                self.assert_equal(self.status(a), pq.TransactionStatus.INERROR, "a parse error fails the block too")
                cur.execute("COMMIT")
                self.assert_equal(cur.statusmessage, "ROLLBACK", "COMMIT of a failed block is answered ROLLBACK")
                self.assert_equal(self.status(a), pq.TransactionStatus.IDLE, "and ends it")
            self.assert_equal(self.ids(a, 140, 149), [], "nothing was committed")
        print("  COMMIT of a failed block -> ROLLBACK ✓")

    def test_extended_protocol_begin_leaves_the_connection_intrans(self):
        """BEGIN through Parse/Bind/Execute (a prepared statement) opens the block the Sync reports."""
        with self.connect() as a, self.connect() as b:
            with a.cursor() as cur:
                cur.execute("BEGIN", prepare=True)
                self.assert_equal(self.status(a), pq.TransactionStatus.INTRANS,
                                  "ReadyForQuery after the extended BEGIN says T")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (%s, %s)", (150, "extended"))
                self.assert_equal(self.status(a), pq.TransactionStatus.INTRANS, "a Bind/Execute DML keeps the block")
                self.assert_equal(self.ids(b, 150, 159), [], "the extended-route INSERT is not committed yet")
                cur.execute("ROLLBACK", prepare=True)
                self.assert_equal(self.status(a), pq.TransactionStatus.IDLE, "the extended ROLLBACK ends it")
            self.assert_equal(self.ids(b, 150, 159), [], "rolled back")
        print("  extended-protocol BEGIN: INTRANS, ROLLBACK undoes ✓")

    def test_driver_managed_transaction_commits_and_rolls_back(self):
        """psycopg with autocommit off opens the block itself: commit() publishes, rollback() discards."""
        with self.connect(autocommit=False) as a, self.connect() as b:
            with a.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (%s, %s)", (160, "driver"))
                self.assert_equal(self.status(a), pq.TransactionStatus.INTRANS, "the driver opened a block")
                self.assert_equal(self.ids(b, 160, 169), [], "not visible before commit()")
            a.commit()
            self.assert_equal(self.ids(b, 160, 169), [160], "visible after commit()")
            with a.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (%s, %s)", (161, "driver"))
            a.rollback()
            self.assert_equal(self.ids(b, 160, 169), [160], "rollback() discards the second row")
        print("  psycopg autocommit=False: commit() / rollback() are real ✓")

    def test_savepoint_is_refused_with_0a000(self):
        """SAVEPOINT / RELEASE / ROLLBACK TO: 0A000; inside a block the refusal fails it, outside it fails nothing."""
        with self.connect() as a:
            with a.cursor() as cur:
                for sql in ("SAVEPOINT s1", "RELEASE SAVEPOINT s1", "ROLLBACK TO SAVEPOINT s1"):
                    self.expect_error(cur, sql, SQLSTATE_FEATURE_NOT_SUPPORTED, "outside a block")
                    self.assert_equal(self.status(a), pq.TransactionStatus.IDLE, f"{sql} outside a block fails nothing")
                cur.execute(f"SELECT count(*) FROM {QUALIFIED}")
                cur.fetchall()

                cur.execute("BEGIN")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (170, 'before savepoint')")
                self.expect_error(cur, "SAVEPOINT s1", SQLSTATE_FEATURE_NOT_SUPPORTED, "inside a block")
                self.assert_equal(self.status(a), pq.TransactionStatus.INERROR, "the refusal fails the block")
                self.expect_error(cur, f"SELECT count(*) FROM {QUALIFIED}", SQLSTATE_IN_FAILED_SQL_TRANSACTION,
                                  "after the refused SAVEPOINT")
                cur.execute("ROLLBACK")
            self.assert_equal(self.ids(a, 170, 179), [], "the block was rolled back")
        print("  SAVEPOINT / RELEASE / ROLLBACK TO -> 0A000 ✓")

    def test_set_inside_a_block_keeps_it_open(self):
        """A SET inside a block is answered "SET" without effect and the block commits after it."""
        with self.connect() as a, self.connect() as b:
            with a.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (180, 'around a set')")
                cur.execute("SET timezone TO 'UTC'")
                self.assert_equal(cur.statusmessage, "SET", "SET tag")
                self.assert_equal(self.status(a), pq.TransactionStatus.INTRANS, "the block is still open")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (181, 'around a set')")
                cur.execute("COMMIT")
            self.assert_equal(self.ids(b, 180, 189), [180, 181], "both rows committed")
        print("  SET inside a block: block kept ✓")

    def test_set_autocommit_off_changes_no_connection(self):
        """SET autocommit TO off is answered without effect: this and every other connection stay in autocommit."""
        with self.connect() as a, self.connect() as b, self.connect() as c:
            with a.cursor() as cur:
                cur.execute("SET autocommit TO off")
                self.assert_equal(cur.statusmessage, "SET", "SET tag")
                self.assert_equal(self.status(a), pq.TransactionStatus.IDLE, "no block was opened")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (190, 'a after set')")
            with b.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (191, 'b')")
            self.assert_equal(self.ids(c, 190, 199), [190, 191],
                              "both INSERTs committed on their own, the SET connection's included")
            with a.cursor() as cur:
                cur.execute("SET autocommit TO on")
        print("  SET autocommit TO off: no effect on any connection ✓")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_rollback_discards_a_local_insert()
            self.test_commit_publishes_to_another_connection()
            self.test_two_open_blocks_see_only_committed_rows_of_each_other()
            self.test_error_fails_the_block_until_rollback()
            self.test_commit_of_a_failed_block_is_a_rollback()
            self.test_extended_protocol_begin_leaves_the_connection_intrans()
            self.test_driver_managed_transaction_commits_and_rolls_back()
            self.test_savepoint_is_refused_with_0a000()
            self.test_set_inside_a_block_keeps_it_open()
            self.test_set_autocommit_off_changes_no_connection()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Local transactions over the PostgreSQL wire")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()

    tests = PgWireTransactionTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - transactions over the PostgreSQL wire: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - transactions over the PostgreSQL wire\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
