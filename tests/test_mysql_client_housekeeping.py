# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""What MySQL drivers send on their own, against the live server.

The MySQL frontend answers the protocol's housekeeping itself — the engine's
grammar has no `@@variable`, no SHOW VARIABLES / SHOW WARNINGS and no MySQL SET
forms (frontend/CLAUDE.md, "MySQL protocol statements"):

  * `SELECT @@version_comment LIMIT 1` (the mysql CLI at connect), multi-column
    variable selects with aliases (Connector/J), `SELECT @@session.autocommit` /
    `@@session.sql_mode` (mysql-connector-python's properties), SHOW VARIABLES
    LIKE, SHOW WARNINGS, SELECT DATABASE();
  * `SET @@session.autocommit = OFF` (mysql-connector-python, pure mode) is
    answered OK, without effect, as every SET is;
  * the text is read by MySQL's default lexical rules and the server says so:
    neither the status flags nor @@sql_mode carry NO_BACKSLASH_ESCAPES or
    ANSI_QUOTES, so the drivers escape with a backslash, and the frontend reads
    exactly that — a value with quotes, backslashes and control characters
    round-trips, and a PyMySQL tuple parameter holding `') OR 1=1 --` is found
    as a value, never run (PyMySQL escapes nested values with a backslash
    whatever the flag says); a bytes parameter (`_binary'...'`) is a string when
    it is utf-8 text and refused (ER_NOT_SUPPORTED_YET) otherwise;
  * a server-side prepared statement's `?` reaches the engine as `$n`
    (mysql-connector-python's prepared cursor sends COM_STMT_PREPARE with `?`).

tests/system/test_frontend_mysql_protocol.cpp pins the same over raw sockets.
"""

import argparse
import sys

import mysql.connector
import pymysql

import config

DB = "mysql_housekeeping"
TABLE = "items"
QUALIFIED = f"{DB}.{TABLE}"

ER_UNKNOWN_SYSTEM_VARIABLE = 1193
ER_PARSE_ERROR = 1064
ER_NOT_SUPPORTED_YET = 1235
SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200
# How the server reads the text: MySQL's defaults (no ANSI_QUOTES, no
# NO_BACKSLASH_ESCAPES), `||` concatenating as the engine does.
SQL_MODE = "PIPES_AS_CONCAT,STRICT_TRANS_TABLES"
INJECTION = "x') OR 1=1 --"


class MySqlClientHousekeepingTest:
    def __init__(self, local=False):
        self.host = config.get_host(local)
        self.admin = None
        self.database_created = False
        self.table_created = False

    # ── helpers ─────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def connect(self):
        return pymysql.connect(host=self.host, port=config.MYSQL_PORT, user="testuser",
                               password="testpass", autocommit=True)

    def connector(self, **kwargs):
        return mysql.connector.connect(host=self.host, port=config.MYSQL_PORT, user="testuser",
                                       password="testpass", **kwargs)

    # ── setup / teardown ────────────────────────────────────────────────────
    def setup(self):
        self.admin = self.connect()
        with self.admin.cursor() as cur:
            cur.execute(f"CREATE DATABASE {DB}")
            self.database_created = True
            cur.execute(f"CREATE TABLE {QUALIFIED} (id bigint, name string)")
            self.table_created = True
            cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (1, 'a'), (2, '?'), (3, 'c')")

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
    def test_cli_connect_query_is_answered(self):
        """The mysql CLI's `select @@version_comment limit 1` answers a row."""
        with self.admin.cursor() as cur:
            cur.execute("select @@version_comment limit 1")
            self.assert_equal([d[0] for d in cur.description], ["@@version_comment"])
            self.assert_equal(cur.fetchall(), (("OtterStax",),))
            cur.execute("SELECT @@version")
            self.assert_equal(cur.fetchone()[0], self.admin.get_server_info(),
                              "@@version is the version the handshake announced")

    def test_variable_select_with_aliases(self):
        """A Connector/J-style variable select answers every column under its alias."""
        with self.admin.cursor() as cur:
            cur.execute("/* mysql-connector-j-9.4.0 */SELECT  @@session.auto_increment_increment AS "
                        "auto_increment_increment, @@character_set_client AS character_set_client, "
                        "@@max_allowed_packet AS max_allowed_packet, @@transaction_isolation AS "
                        "transaction_isolation, @@lower_case_table_names AS lower_case_table_names")
            self.assert_equal([d[0] for d in cur.description],
                              ["auto_increment_increment", "character_set_client", "max_allowed_packet",
                               "transaction_isolation", "lower_case_table_names"])
            row = cur.fetchone()
            self.assert_equal(int(row[0]), 1)
            self.assert_equal(row[1], "utf8mb4")
            self.assert_equal(row[3], "REPEATABLE-READ")

    def test_unknown_variable_is_1193(self):
        """A variable the server does not have is ER_UNKNOWN_SYSTEM_VARIABLE."""
        with self.admin.cursor() as cur:
            try:
                cur.execute("SELECT @@query_cache_size")
            except pymysql.MySQLError as e:
                self.assert_equal(e.args[0], ER_UNKNOWN_SYSTEM_VARIABLE, str(e))
            else:
                raise AssertionError("SELECT @@query_cache_size must fail")

    def test_show_variables_and_warnings(self):
        """SHOW VARIABLES LIKE filters; SHOW WARNINGS is empty; DATABASE() is NULL."""
        with self.admin.cursor() as cur:
            cur.execute("SHOW VARIABLES LIKE 'sql_mode'")
            rows = cur.fetchall()
            self.assert_equal(len(rows), 1)
            self.assert_equal(rows[0][0], "sql_mode")
            self.assert_equal(rows[0][1], SQL_MODE, "MySQL's default lexical rules")
            cur.execute("SHOW WARNINGS")
            self.assert_equal(cur.fetchall(), ())
            cur.execute("SELECT DATABASE()")
            self.assert_equal(cur.fetchone()[0], None)

    def test_quote_and_backslash_round_trip(self):
        """PyMySQL escapes with a backslash (no NO_BACKSLASH_ESCAPES flag): the value comes back intact."""
        if self.admin.server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES:
            raise AssertionError("the status flags must not say NO_BACKSLASH_ESCAPES")
        value = "it's a \\ back'slash \"quoted\"\nline\ttab %_"
        with self.admin.cursor() as cur:
            cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (%s, %s)", (10, value))
            cur.execute(f"SELECT name FROM {QUALIFIED} WHERE id = %s", (10,))
            self.assert_equal(cur.fetchone()[0], value)
            cur.execute(f"SELECT id FROM {QUALIFIED} WHERE name = %s", (value,))
            self.assert_equal(cur.fetchall(), ((10,),))
            cur.execute(f"DELETE FROM {QUALIFIED} WHERE id = 10")

    def test_tuple_parameter_is_no_injection(self):
        """PyMySQL escapes the values of a tuple parameter with a backslash: the string is a value, never code."""
        with self.admin.cursor() as cur:
            cur.execute(f"SELECT id FROM {QUALIFIED} WHERE name IN %s", ([INJECTION],))
            self.assert_equal(cur.fetchall(), (), "nothing has that name yet; OR 1=1 must not run")
            cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (%s, %s)", (11, INJECTION))
            cur.execute(f"SELECT id FROM {QUALIFIED} WHERE name IN %s", ([INJECTION, "nope"],))
            self.assert_equal(cur.fetchall(), ((11,),))
            cur.execute(f"SELECT name FROM {QUALIFIED} WHERE id IN %s", ((11,),))
            self.assert_equal(cur.fetchone()[0], INJECTION)
            cur.execute(f"DELETE FROM {QUALIFIED} WHERE id = 11")

    def test_bytes_parameter(self):
        """PyMySQL writes a bytes parameter as _binary'...': utf-8 bytes are a string; other bytes are refused."""
        with self.admin.cursor() as cur:
            cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (%s, %s)", (13, "caf\u00e9 'q'".encode()))
            cur.execute(f"SELECT name FROM {QUALIFIED} WHERE id = %s", (13,))
            self.assert_equal(cur.fetchone()[0], "caf\u00e9 'q'")
            cur.execute(f"SELECT id FROM {QUALIFIED} WHERE name = %s", (b"caf\xc3\xa9 'q'",))
            self.assert_equal(cur.fetchall(), ((13,),))
            cur.execute(f"DELETE FROM {QUALIFIED} WHERE id = 13")
            try:
                cur.execute(f"SELECT id FROM {QUALIFIED} WHERE name = %s", (b"\xff\xfe",))
            except pymysql.MySQLError as e:
                self.assert_equal(e.args[0], ER_NOT_SUPPORTED_YET, str(e))
            else:
                raise AssertionError("bytes that are not utf-8 text must be refused")
            cur.execute(f"SELECT count(*) FROM {QUALIFIED}")
            self.assert_equal(int(cur.fetchone()[0]), 3)

    def test_mysql_literals_and_comments(self):
        """MySQL's literal and comment rules: \\', "..." strings, joined literals, `#` comments."""
        with self.admin.cursor() as cur:
            cur.execute("SELECT 'it\\'s', \"dq\", 'a' 'b'")
            self.assert_equal(cur.fetchone(), ("it's", "dq", "ab"))
            cur.execute(f"SELECT id # id\nFROM {QUALIFIED} ORDER BY id")
            self.assert_equal([r[0] for r in cur.fetchall()], [1, 2, 3])

    def test_placeholder_in_a_plain_query_is_a_syntax_error(self):
        """A `?` in a COM_QUERY text is ER_PARSE_ERROR, as MySQL answers it; the connection keeps serving."""
        with self.admin.cursor() as cur:
            try:
                cur.execute(f"SELECT ? + 1 FROM {QUALIFIED}")
            except pymysql.MySQLError as e:
                self.assert_equal(e.args[0], ER_PARSE_ERROR, str(e))
            else:
                raise AssertionError("SELECT ? + 1 through COM_QUERY must fail")
            cur.execute(f"SELECT count(*) FROM {QUALIFIED}")
            self.assert_equal(int(cur.fetchone()[0]), 3)

    def test_connector_python_pure_connects(self):
        """mysql-connector-python in pure mode: SET @@session.autocommit and its property selects."""
        conn = self.connector(use_pure=True, autocommit=True)
        try:
            self.assert_equal(conn.autocommit, True, "SELECT @@session.autocommit")
            self.assert_equal(conn.sql_mode, SQL_MODE, "SELECT @@session.sql_mode")
            cur = conn.cursor()
            cur.execute(f"SELECT count(*) FROM {QUALIFIED}")
            self.assert_equal(int(cur.fetchone()[0]), 3)
            # Its client-side escaping (a backslash) reads back intact.
            value = "o'neil \\ \"x\""
            cur.execute(f"INSERT INTO {QUALIFIED} (id, name) VALUES (%s, %s)", (12, value))
            cur.execute(f"SELECT name FROM {QUALIFIED} WHERE id = %s", (12,))
            self.assert_equal(cur.fetchone()[0], value)
            cur.execute(f"DELETE FROM {QUALIFIED} WHERE id = 12")
            cur.close()
        finally:
            conn.close()

    def test_prepared_placeholder_reaches_the_engine_as_dollar_n(self):
        """A server-side prepared `SELECT ? + 1` runs; a `?` inside a literal stays text."""
        conn = self.connector(autocommit=True)
        try:
            cur = conn.cursor(prepared=True)
            cur.execute(f"SELECT ? + 1 FROM {QUALIFIED} WHERE id = 1", (41,))
            self.assert_equal([int(r[0]) for r in cur.fetchall()], [42])
            cur.execute(f"SELECT id FROM {QUALIFIED} WHERE name = '?' OR id = ? ORDER BY id", (3,))
            self.assert_equal([int(r[0]) for r in cur.fetchall()], [2, 3])
            # A bound value travels in the binary protocol, never through the text.
            cur.execute(f"SELECT id FROM {QUALIFIED} WHERE name = ?", (INJECTION,))
            self.assert_equal(cur.fetchall(), [])
            # What a driver prepares on its own is answered by the frontend.
            cur.execute("SELECT @@max_allowed_packet")
            self.assert_equal(len(cur.fetchall()), 1)
            cur.close()
        finally:
            conn.close()

    def run_all_tests(self):
        try:
            self.setup()
            self.test_cli_connect_query_is_answered()
            self.test_variable_select_with_aliases()
            self.test_unknown_variable_is_1193()
            self.test_show_variables_and_warnings()
            self.test_quote_and_backslash_round_trip()
            self.test_tuple_parameter_is_no_injection()
            self.test_bytes_parameter()
            self.test_mysql_literals_and_comments()
            self.test_placeholder_in_a_plain_query_is_a_syntax_error()
            self.test_connector_python_pure_connects()
            self.test_prepared_placeholder_reaches_the_engine_as_dollar_n()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="What MySQL drivers send on their own")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()

    tests = MySqlClientHousekeepingTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - MySQL driver housekeeping: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - MySQL driver housekeeping\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
