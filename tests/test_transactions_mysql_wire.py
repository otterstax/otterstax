# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Local transactions over the MySQL wire, against the live server.

The block rules are the engine's, the same as on the PostgreSQL wire
(session/CLAUDE.md, "The engine session and transactions"; frontend/CLAUDE.md,
"Transactions"):

  * BEGIN ... COMMIT publishes the block's rows to other connections, and a
    block the client never commits is rolled back when its connection closes;
  * `SET autocommit = 0` is answered OK WITHOUT EFFECT (otterbrix would apply a
    SET to every session; OTB-session-settings): a driver running with
    autocommit off still commits every statement it sends outside an explicit
    BEGIN, and the SET changes no other connection either;
  * an error inside a block fails it: later statements are refused and the
    COMMIT of the failed block is answered with an OK packet that commits
    nothing;
  * SAVEPOINT is refused with ER_NOT_SUPPORTED_YET (1235);
  * SET NAMES inside a block is answered by the frontend and keeps the block;
  * every OK packet carries SERVER_STATUS_IN_TRANS while a block is open.

tests/system/test_frontend_transactions.cpp pins the same over raw sockets.
"""

import argparse
import sys

import pymysql

import config

DB = "txn_mysql_wire"
TABLE = "items"
QUALIFIED = f"{DB}.{TABLE}"

ER_NOT_SUPPORTED_YET = 1235
SERVER_STATUS_IN_TRANS = 0x0001


class MySqlWireTransactionTest:
    def __init__(self, local=False):
        self.host = config.get_host(local)
        self.admin = None
        self.database_created = False
        self.table_created = False

    # ── helpers ─────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def connect(self, autocommit=True):
        return pymysql.connect(host=self.host, port=config.MYSQL_PORT, user="testuser",
                               password="testpass", autocommit=autocommit)

    def ids(self, conn, low, high):
        """The ids in [low, high] the connection sees, sorted."""
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {QUALIFIED} WHERE id >= {low} AND id <= {high}")
            return sorted(int(r[0]) for r in cur.fetchall())

    def expect_error(self, cur, sql, code=None, what=""):
        """`sql` must fail; returns the error. With `code`, the ERR packet carries it."""
        try:
            cur.execute(sql)
        except pymysql.MySQLError as e:
            if code is not None:
                self.assert_equal(e.args[0], code, f"{what}: error code of {sql!r} ({e})")
            return e
        raise AssertionError(f"{what}: {sql!r} must fail")

    @staticmethod
    def in_transaction(conn):
        """SERVER_STATUS_IN_TRANS of the last OK packet the connection read."""
        return bool(conn.server_status & SERVER_STATUS_IN_TRANS)

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
    def test_begin_commit_is_visible_to_another_connection(self):
        """BEGIN / INSERT / COMMIT: invisible to another connection until COMMIT, IN_TRANS while open."""
        a, b = self.connect(), self.connect()
        try:
            a.begin()
            with a.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (100, 'committed')")
            self.assert_equal(self.in_transaction(a), True, "the OK packet of the INSERT carries IN_TRANS")
            self.assert_equal(self.ids(b, 100, 109), [], "uncommitted rows are not another connection's")
            a.commit()
            self.assert_equal(self.in_transaction(a), False, "COMMIT clears IN_TRANS")
            self.assert_equal(self.ids(b, 100, 109), [100], "visible after commit()")
        finally:
            a.close()
            b.close()
        print("  BEGIN / INSERT / commit(): visible after COMMIT ✓")

    def test_block_left_open_is_rolled_back_when_the_connection_closes(self):
        """BEGIN / INSERT, then close without commit(): the rows are never committed."""
        a, b = self.connect(), self.connect()
        try:
            a.begin()
            with a.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (110, 'abandoned'), (111, 'abandoned')")
            self.assert_equal(self.ids(a, 110, 119), [110, 111], "the block sees its own rows")
        finally:
            a.close()  # COM_QUIT with the block open: the session rolls it back as it closes
        try:
            self.assert_equal(self.ids(b, 110, 119), [], "a block closed without COMMIT keeps nothing")
            with b.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (112, 'after')")
            self.assert_equal(self.ids(b, 110, 119), [112], "the table takes new writes as before")
        finally:
            b.close()
        print("  block left open at close: rolled back ✓")

    def test_autocommit_off_without_begin_commits_each_statement(self):
        """A driver with autocommit off sends SET autocommit = 0, answered without effect: its writes commit."""
        a, b = self.connect(autocommit=False), self.connect()
        try:
            with a.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (120, 'no commit')")
            self.assert_equal(self.in_transaction(a), False, "no block was opened by the driver's SET")
            self.assert_equal(self.ids(b, 120, 129), [120], "the statement committed on its own")
            a.commit()  # COMMIT outside a block is accepted and ends nothing
        finally:
            a.close()
            b.close()
        print("  autocommit=False without BEGIN: each statement commits (SET autocommit has no effect) ✓")

    def test_set_autocommit_on_one_connection_changes_no_other(self):
        """SET autocommit = 0 on one connection: another connection's INSERT is still seen at once by a third."""
        a, b, c = self.connect(), self.connect(), self.connect()
        try:
            with a.cursor() as cur:
                cur.execute("SET autocommit = 0")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (130, 'a after set')")
            with b.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (131, 'b')")
            self.assert_equal(self.ids(c, 130, 139), [130, 131],
                              "both INSERTs are committed: the SET changed neither connection")
            with a.cursor() as cur:
                cur.execute("SET autocommit = 1")
        finally:
            a.close()
            b.close()
            c.close()
        print("  SET autocommit = 0: no other connection affected ✓")

    def test_error_fails_the_block_and_commit_commits_nothing(self):
        """An error inside a block refuses the next statements; COMMIT answers OK and keeps nothing."""
        a, b = self.connect(), self.connect()
        try:
            a.begin()
            with a.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (140, 'lost')")
                self.expect_error(cur, f"SELECT * FROM {DB}.no_such_table", what="a missing table")
                error = self.expect_error(cur, f"INSERT INTO {QUALIFIED} (id, name) VALUES (141, 'refused')",
                                          what="inside the failed block")
                assert "only ROLLBACK is accepted" in str(error), f"the refusal must say why: {error}"
            self.assert_equal(self.in_transaction(a), True, "a failed block is still a block")
            a.commit()  # answered with an OK packet: the engine rolled the failed block back
            self.assert_equal(self.in_transaction(a), False, "the COMMIT ended the failed block")
            self.assert_equal(self.ids(b, 140, 149), [], "nothing of the failed block was committed")
            self.assert_equal(self.ids(a, 140, 149), [], "not even for the connection that ran it")
        finally:
            a.close()
            b.close()
        print("  error in a block: refused until it ends, COMMIT keeps nothing ✓")

    def test_savepoint_is_refused_with_1235(self):
        """SAVEPOINT: ER_NOT_SUPPORTED_YET outside a block (nothing failed) and inside one (the block fails)."""
        a, b = self.connect(), self.connect()
        try:
            with a.cursor() as cur:
                self.expect_error(cur, "SAVEPOINT s1", ER_NOT_SUPPORTED_YET, "outside a block")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (150, 'outside')")
            self.assert_equal(self.ids(b, 150, 159), [150], "a refusal outside a block fails nothing")

            a.begin()
            with a.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (151, 'inside')")
                self.expect_error(cur, "SAVEPOINT s1", ER_NOT_SUPPORTED_YET, "inside a block")
                self.expect_error(cur, f"INSERT INTO {QUALIFIED} (id, name) VALUES (152, 'refused')",
                                  what="after the refused SAVEPOINT")
            a.rollback()
            self.assert_equal(self.ids(b, 150, 159), [150], "the failed block kept nothing")
        finally:
            a.close()
            b.close()
        print("  SAVEPOINT -> 1235 ✓")

    def test_set_names_inside_a_block_keeps_it(self):
        """SET NAMES inside a block is the frontend's OK: the block stays open and commits both rows."""
        a, b = self.connect(), self.connect()
        try:
            a.begin()
            with a.cursor() as cur:
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (160, 'before')")
                cur.execute("SET NAMES utf8mb4")
                self.assert_equal(self.in_transaction(a), True, "SET NAMES keeps the block")
                cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (161, 'after')")
            a.commit()
            self.assert_equal(self.ids(b, 160, 169), [160, 161], "the block committed both rows")
        finally:
            a.close()
            b.close()
        print("  SET NAMES inside a block: block kept ✓")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_begin_commit_is_visible_to_another_connection()
            self.test_block_left_open_is_rolled_back_when_the_connection_closes()
            self.test_autocommit_off_without_begin_commits_each_statement()
            self.test_set_autocommit_on_one_connection_changes_no_other()
            self.test_error_fails_the_block_and_commit_commits_nothing()
            self.test_savepoint_is_refused_with_1235()
            self.test_set_names_inside_a_block_keeps_it()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Local transactions over the MySQL wire")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()

    tests = MySqlWireTransactionTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - transactions over the MySQL wire: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - transactions over the MySQL wire\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
