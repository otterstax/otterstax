# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Writes into remote tables through the engine and directly on the backend.

A remote INSERT / UPDATE / DELETE written in THREE parts (`uid.db.table`, or
`uid.schema.table` for PostgreSQL) runs on the engine through the federation
(otterbrix/federation/CLAUDE.md):

  * what one backend can say is pushed as one statement — its count is the
    backend's (MySQL: the rows an UPDATE MATCHED, not the rows it changed);
  * what it cannot (here: `WHERE id IN (SELECT id FROM <local table>)`) runs in
    the engine over the table's scan, and every row the engine picked is
    changed by one statement naming the row by its PRIMARY KEY — exactly the
    picked rows change, whatever their other columns hold (duplicates
    included), and the count is the rows picked;
  * such a row change is refused (0A000 / ER_NOT_SUPPORTED_YET) on a table
    without a primary key, with a key over a binary column (never recorded at
    discovery) or a floating-point one, and on every ClickHouse table — before
    anything is written.

A target written in FOUR parts (`uid.db.schema.table`) runs on its backend
directly (session/CLAUDE.md, run_remote_dml): RETURNING is refused, and so is
an INSERT ... SELECT from another source; the three-part form of the same
INSERT ... SELECT runs on the engine.

Writes into a remote table run in the backend's autocommit, inside an explicit
transaction too: a ROLLBACK does not undo them (the engine has no storage
transaction hooks yet).

The remote tables are made here on the backends directly (pymysql to mariadb1,
psycopg2 to postgres1, as create_test_data.py does), so the file runs only
inside the test network; the server discovers each one on first use (one
rediscovery retry).
"""

import argparse
import sys

import psycopg
import psycopg2
import pymysql

import config

# Backends as the test network names them (tests/scripts/config.yaml).
MARIADB = dict(host="mariadb1", port=3306, user="user1", password="password1", database="db1")
POSTGRES = dict(host="postgres1", port=5432, user="pguser", password="pgpassword", dbname="pgdb")

MYSQL_KEYED = "campaigns.db1.rowdml_keyed"          # three parts: uid.db.table
MYSQL_NOKEY = "campaigns.db1.rowdml_nokey"
MYSQL_FLOATKEY = "campaigns.db1.rowdml_floatkey"
MYSQL_BINKEY = "campaigns.db1.rowdml_binkey"
PG_KEYED = "products.public.rowdml_keyed"           # three parts: uid.schema.table
PG_KEYED_4 = "products.pgdb.public.rowdml_keyed"    # four parts: uid.db.schema.table
PG_NOKEY = "products.public.rowdml_nokey"
PG_BYTEAKEY = "products.public.rowdml_byteakey"
CH_ORDERS = f"{config.CH_ALIAS}.{config.CH_DATABASE}.orders"

LOCAL_DB = "rowdml_local"
PICK = f"{LOCAL_DB}.pick"            # (id int): the ids the engine picks, 1 and 2
BIG_PICK = f"{LOCAL_DB}.big_pick"    # (id bigint): an order_id ClickHouse does not hold
NEW_ROWS = f"{LOCAL_DB}.new_rows"    # (id int, grp string, qty int): rows to copy into a remote table

# Rows 1, 2 and 4 share grp 'a'; rows 1, 2 and 3 share qty 5: only the key tells them apart.
KEYED_ROWS = [(1, "a", 5), (2, "a", 5), (3, "b", 5), (4, "a", 7)]

SQLSTATE_FEATURE_NOT_SUPPORTED = "0A000"
ER_NOT_SUPPORTED_YET = 1235


class RemoteRowDmlTest:
    def __init__(self, local=False):
        self.host = config.get_host(local)
        self.pg = None      # PostgreSQL wire to otterstax
        self.my = None      # MySQL wire to otterstax
        self.mariadb = None
        self.postgres = None
        self.local_created = False

    # ── helpers ─────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def mariadb_rows(self, table):
        with self.mariadb.cursor() as cur:
            cur.execute(f"SELECT id, grp, qty FROM {table} ORDER BY id")
            return [tuple(r) for r in cur.fetchall()]

    def postgres_rows(self, table):
        with self.postgres.cursor() as cur:
            cur.execute(f"SELECT id, grp, qty FROM {table} ORDER BY id")
            return [tuple(r) for r in cur.fetchall()]

    def reset_mariadb(self, table, rows):
        with self.mariadb.cursor() as cur:
            cur.execute(f"DELETE FROM {table}")
            cur.executemany(f"INSERT INTO {table} (id, grp, qty) VALUES (%s, %s, %s)", rows)

    def reset_postgres(self, table, rows):
        with self.postgres.cursor() as cur:
            cur.execute(f"DELETE FROM {table}")
            cur.executemany(f"INSERT INTO {table} (id, grp, qty) VALUES (%s, %s, %s)", rows)

    def pg_count(self, sql):
        """Run a DML over the PostgreSQL wire; its affected count."""
        with self.pg.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount

    def my_count(self, sql):
        """Run a DML over the MySQL wire; its affected count."""
        with self.my.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount

    def pg_refused(self, sql, needle, what):
        """`sql` over the PostgreSQL wire must fail with 0A000 and a message holding `needle`."""
        try:
            with self.pg.cursor() as cur:
                cur.execute(sql)
        except psycopg.Error as e:
            self.assert_equal(e.sqlstate, SQLSTATE_FEATURE_NOT_SUPPORTED, f"{what}: sqlstate ({e})")
            assert needle in str(e), f"{what}: the refusal must say {needle!r}: {e}"
            return
        raise AssertionError(f"{what}: {sql!r} must be refused")

    def my_refused(self, sql, needle, what):
        """`sql` over the MySQL wire must fail with ER_NOT_SUPPORTED_YET and a message holding `needle`."""
        try:
            with self.my.cursor() as cur:
                cur.execute(sql)
        except pymysql.MySQLError as e:
            self.assert_equal(e.args[0], ER_NOT_SUPPORTED_YET, f"{what}: error code ({e})")
            assert needle in str(e), f"{what}: the refusal must say {needle!r}: {e}"
            return
        raise AssertionError(f"{what}: {sql!r} must be refused")

    # ── setup / teardown ────────────────────────────────────────────────────
    def setup(self):
        self.mariadb = pymysql.connect(**MARIADB, autocommit=True)
        with self.mariadb.cursor() as cur:
            for table in ("rowdml_keyed", "rowdml_nokey", "rowdml_floatkey", "rowdml_binkey"):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute("CREATE TABLE rowdml_keyed (id INT PRIMARY KEY, grp VARCHAR(16) NOT NULL, qty INT NOT NULL)")
            cur.execute("CREATE TABLE rowdml_nokey (id INT NOT NULL, grp VARCHAR(16) NOT NULL, qty INT NOT NULL)")
            cur.execute("CREATE TABLE rowdml_floatkey (k FLOAT PRIMARY KEY, v INT NOT NULL)")
            cur.execute("CREATE TABLE rowdml_binkey (k VARBINARY(16) PRIMARY KEY, v INT NOT NULL)")
            cur.execute("INSERT INTO rowdml_floatkey (k, v) VALUES (0.5, 1)")
            cur.execute("INSERT INTO rowdml_binkey (k, v) VALUES (X'01', 1)")
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        # Two rows identical in every column: nothing but a key could tell them apart.
        self.reset_mariadb("rowdml_nokey", [(1, "a", 5), (1, "a", 5), (2, "b", 5)])

        self.postgres = psycopg2.connect(**POSTGRES)
        self.postgres.autocommit = True
        with self.postgres.cursor() as cur:
            for table in ("rowdml_keyed", "rowdml_nokey", "rowdml_byteakey"):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute("CREATE TABLE rowdml_keyed (id integer PRIMARY KEY, grp varchar(16) NOT NULL, qty integer NOT NULL)")
            cur.execute("CREATE TABLE rowdml_nokey (id integer NOT NULL, grp varchar(16) NOT NULL, qty integer NOT NULL)")
            cur.execute("CREATE TABLE rowdml_byteakey (k bytea PRIMARY KEY, v integer NOT NULL)")
            cur.execute("INSERT INTO rowdml_byteakey (k, v) VALUES ('\\x01'::bytea, 1)")
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        self.reset_postgres("rowdml_nokey", [(1, "a", 5), (1, "a", 5), (2, "b", 5)])

        self.pg = psycopg.connect(host=self.host, port=config.PG_PORT, user="testuser",
                                  password="testpass", dbname=LOCAL_DB, autocommit=True)
        self.my = pymysql.connect(host=self.host, port=config.MYSQL_PORT, user="testuser",
                                  password="testpass", autocommit=True)
        with self.pg.cursor() as cur:
            cur.execute(f"CREATE DATABASE {LOCAL_DB}")
            self.local_created = True
            cur.execute(f"CREATE TABLE {PICK} (id int)")
            cur.execute(f"INSERT INTO {PICK} (id) VALUES (1), (2)")
            cur.execute(f"CREATE TABLE {BIG_PICK} (id bigint)")
            cur.execute(f"INSERT INTO {BIG_PICK} (id) VALUES (990999)")
            cur.execute(f"CREATE TABLE {NEW_ROWS} (id int, grp string, qty int)")
            cur.execute(f"INSERT INTO {NEW_ROWS} (id, grp, qty) VALUES (60, 'n', 1), (61, 'n', 1)")
            # The tables were made on the backends after the server started: the
            # first statement naming each one rediscovers it.
            for table in (MYSQL_KEYED, MYSQL_NOKEY, MYSQL_FLOATKEY, MYSQL_BINKEY, PG_KEYED, PG_NOKEY, PG_BYTEAKEY):
                cur.execute(f"SELECT count(*) FROM {table}")
                cur.fetchall()

    def cleanup(self):
        try:
            if self.pg is not None and self.local_created:
                with self.pg.cursor() as cur:
                    cur.execute(f"DROP DATABASE {LOCAL_DB}")
        finally:
            for conn in (self.pg, self.my):
                if conn is not None:
                    conn.close()
            if self.mariadb is not None:
                with self.mariadb.cursor() as cur:
                    for table in ("rowdml_keyed", "rowdml_nokey", "rowdml_floatkey", "rowdml_binkey"):
                        cur.execute(f"DROP TABLE IF EXISTS {table}")
                self.mariadb.close()
            if self.postgres is not None:
                with self.postgres.cursor() as cur:
                    for table in ("rowdml_keyed", "rowdml_nokey", "rowdml_byteakey"):
                        cur.execute(f"DROP TABLE IF EXISTS {table}")
                self.postgres.close()

    # ── MariaDB, over the MySQL wire ────────────────────────────────────────
    def test_mysql_row_update_changes_exactly_the_picked_rows(self):
        """MariaDB: an UPDATE the engine runs row by row changes the picked keys only and counts them."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        update = f"UPDATE {MYSQL_KEYED} SET qty = 9 WHERE id IN (SELECT id FROM {PICK})"
        self.assert_equal(self.my_count(update), 2, "two rows picked, two changed")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), [(1, "a", 9), (2, "a", 9), (3, "b", 5), (4, "a", 7)],
                          "rows 3 and 4 share grp / qty with the picked rows and stay untouched")
        # The same UPDATE again changes no value: each row statement still matches
        # its row, and MySQL's count is the rows matched.
        self.assert_equal(self.my_count(update), 2, "an UPDATE that changes nothing still counts the matched rows")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), [(1, "a", 9), (2, "a", 9), (3, "b", 5), (4, "a", 7)],
                          "nothing changed")
        print("  MariaDB row-by-row UPDATE by primary key: rows 1, 2 changed, count 2 (twice) ✓")

    def test_mysql_pushed_update_counts_matched_rows(self):
        """MariaDB: an UPDATE pushed as one statement answers the rows it matched, not the rows it changed."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        self.assert_equal(self.my_count(f"UPDATE {MYSQL_KEYED} SET qty = 7 WHERE grp = 'a'"), 3,
                          "rows 1, 2 change, row 4 already holds 7: matched 3")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), [(1, "a", 7), (2, "a", 7), (3, "b", 5), (4, "a", 7)],
                          "the pushed UPDATE's effect")
        print("  MariaDB pushed UPDATE: count = rows matched ✓")

    def test_mysql_row_delete_removes_exactly_the_picked_rows(self):
        """MariaDB: a DELETE the engine runs row by row removes the picked keys only."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        self.assert_equal(self.my_count(f"DELETE FROM {MYSQL_KEYED} WHERE id IN (SELECT id FROM {PICK})"), 2,
                          "two rows picked")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), [(3, "b", 5), (4, "a", 7)], "rows 1 and 2 are gone")
        print("  MariaDB row-by-row DELETE by primary key ✓")

    def test_mysql_row_change_without_a_primary_key_is_refused(self):
        """MariaDB, no primary key: a row change the engine picks is refused; a pushed one still runs."""
        rows = [(1, "a", 5), (1, "a", 5), (2, "b", 5)]
        self.reset_mariadb("rowdml_nokey", rows)
        for sql in (f"UPDATE {MYSQL_NOKEY} SET qty = 9 WHERE id IN (SELECT id FROM {PICK})",
                    f"DELETE FROM {MYSQL_NOKEY} WHERE id IN (SELECT id FROM {PICK})"):
            self.my_refused(sql, "has no primary key", "no primary key")
        self.assert_equal(self.mariadb_rows("rowdml_nokey"), rows, "nothing was written")
        self.assert_equal(self.my_count(f"UPDATE {MYSQL_NOKEY} SET qty = 6 WHERE id = 2"), 1,
                          "an UPDATE the backend runs whole needs no key")
        print("  MariaDB table without a primary key: row change refused (1235), pushed UPDATE runs ✓")

    def test_mysql_row_change_over_a_float_or_binary_key_is_refused(self):
        """MariaDB: a FLOAT key names no row exactly; a VARBINARY key is never recorded — both refused."""
        self.my_refused(f"DELETE FROM {MYSQL_FLOATKEY} WHERE v IN (SELECT id FROM {PICK})",
                        "does not name a row exactly", "FLOAT primary key")
        self.my_refused(f"DELETE FROM {MYSQL_BINKEY} WHERE v IN (SELECT id FROM {PICK})",
                        "has no primary key", "VARBINARY primary key")
        with self.mariadb.cursor() as cur:
            cur.execute("SELECT count(*) FROM rowdml_floatkey")
            self.assert_equal(cur.fetchall()[0][0], 1, "the FLOAT-keyed row is still there")
            cur.execute("SELECT count(*) FROM rowdml_binkey")
            self.assert_equal(cur.fetchall()[0][0], 1, "the VARBINARY-keyed row is still there")
        print("  MariaDB FLOAT / VARBINARY primary key: row change refused ✓")

    # ── PostgreSQL, over the PostgreSQL wire ────────────────────────────────
    def test_pg_row_update_and_delete_change_exactly_the_picked_rows(self):
        """PostgreSQL: row-by-row UPDATE / DELETE by primary key change the picked rows only and count them."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        update = f"UPDATE {PG_KEYED} SET qty = 9 WHERE id IN (SELECT id FROM {PICK})"
        self.assert_equal(self.pg_count(update), 2, "two rows picked, two changed")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), [(1, "a", 9), (2, "a", 9), (3, "b", 5), (4, "a", 7)],
                          "rows 3 and 4 share grp / qty with the picked rows and stay untouched")
        self.assert_equal(self.pg_count(update), 2, "the same UPDATE again: still the two picked rows")
        self.assert_equal(self.pg_count(f"DELETE FROM {PG_KEYED} WHERE id IN (SELECT id FROM {PICK})"), 2,
                          "two rows picked for DELETE")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), [(3, "b", 5), (4, "a", 7)], "rows 1 and 2 are gone")
        print("  PostgreSQL row-by-row UPDATE / DELETE by primary key ✓")

    def test_pg_row_change_without_a_usable_key_is_refused(self):
        """PostgreSQL: no primary key, or a bytea one (never recorded): the row change is refused with 0A000."""
        rows = [(1, "a", 5), (1, "a", 5), (2, "b", 5)]
        self.reset_postgres("rowdml_nokey", rows)
        self.pg_refused(f"UPDATE {PG_NOKEY} SET qty = 9 WHERE id IN (SELECT id FROM {PICK})",
                        "has no primary key", "no primary key")
        self.pg_refused(f"DELETE FROM {PG_BYTEAKEY} WHERE v IN (SELECT id FROM {PICK})",
                        "has no primary key", "bytea primary key")
        self.assert_equal(self.postgres_rows("rowdml_nokey"), rows, "nothing was written")
        print("  PostgreSQL without a usable primary key: row change refused (0A000) ✓")

    def test_clickhouse_row_change_is_refused(self):
        """ClickHouse: a row change the engine picks is refused before anything runs."""
        with self.pg.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {CH_ORDERS}")
            before = int(cur.fetchall()[0][0])
        self.pg_refused(f"DELETE FROM {CH_ORDERS} WHERE order_id IN (SELECT id FROM {BIG_PICK})",
                        "cannot change a row by its number", "ClickHouse")
        with self.pg.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {CH_ORDERS}")
            self.assert_equal(int(cur.fetchall()[0][0]), before, "no order was touched")
        print("  ClickHouse row change refused (0A000) ✓")

    # ── four-part targets and transactions ──────────────────────────────────
    def test_four_part_returning_is_refused(self):
        """A four-part target runs on its backend directly, which answers a count only: RETURNING is refused."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        self.pg_refused(f"INSERT INTO {PG_KEYED_4} (id, grp, qty) VALUES (50, 'r', 1) RETURNING id",
                        "RETURNING is not supported", "four-part INSERT ... RETURNING")
        self.pg_refused(f"UPDATE {PG_KEYED_4} SET qty = 1 WHERE id = 3 RETURNING id",
                        "RETURNING is not supported", "four-part UPDATE ... RETURNING")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), KEYED_ROWS, "nothing was written")
        print("  four-part RETURNING refused ✓")

    def test_insert_select_from_another_source_needs_the_three_part_form(self):
        """INSERT ... SELECT from a local table: refused in the four-part form, run by the engine in the three-part one."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        try:
            with self.pg.cursor() as cur:
                cur.execute(f"INSERT INTO {PG_KEYED_4} (id, grp, qty) SELECT id, grp, qty FROM {NEW_ROWS}")
        except psycopg.Error as e:
            print(f"  four-part INSERT ... SELECT from a local table refused: {e}")
        else:
            raise AssertionError("a four-part INSERT ... SELECT from another source must be refused")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), KEYED_ROWS, "the refused statement wrote nothing")

        self.assert_equal(self.pg_count(f"INSERT INTO {PG_KEYED} (id, grp, qty) SELECT id, grp, qty FROM {NEW_ROWS}"), 2,
                          "the three-part form runs on the engine and writes both rows")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), KEYED_ROWS + [(60, "n", 1), (61, "n", 1)],
                          "the copied rows are on the backend")
        print("  INSERT ... SELECT from a local table: four-part refused, three-part runs ✓")

    def test_remote_write_inside_a_block_is_not_rolled_back(self):
        """A remote write inside BEGIN commits on the backend at once; ROLLBACK does not undo it."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        with self.pg.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(f"UPDATE {PG_KEYED} SET qty = 42 WHERE id = 3")              # through the engine
            self.assert_equal(cur.rowcount, 1, "three-part UPDATE inside the block")
            cur.execute(f"INSERT INTO {PG_KEYED_4} (id, grp, qty) VALUES (70, 't', 1)")  # on the backend directly
            self.assert_equal(cur.rowcount, 1, "four-part INSERT inside the block")
            self.assert_equal(self.postgres_rows("rowdml_keyed"),
                              [(1, "a", 5), (2, "a", 5), (3, "b", 42), (4, "a", 7), (70, "t", 1)],
                              "both writes are on the backend before the block ends")
            cur.execute("ROLLBACK")
        self.assert_equal(self.postgres_rows("rowdml_keyed"),
                          [(1, "a", 5), (2, "a", 5), (3, "b", 42), (4, "a", 7), (70, "t", 1)],
                          "ROLLBACK does not undo a remote write")
        print("  remote writes inside BEGIN survive ROLLBACK ✓")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_mysql_row_update_changes_exactly_the_picked_rows()
            self.test_mysql_pushed_update_counts_matched_rows()
            self.test_mysql_row_delete_removes_exactly_the_picked_rows()
            self.test_mysql_row_change_without_a_primary_key_is_refused()
            self.test_mysql_row_change_over_a_float_or_binary_key_is_refused()
            self.test_pg_row_update_and_delete_change_exactly_the_picked_rows()
            self.test_pg_row_change_without_a_usable_key_is_refused()
            self.test_clickhouse_row_change_is_refused()
            self.test_four_part_returning_is_refused()
            self.test_insert_select_from_another_source_needs_the_three_part_form()
            self.test_remote_write_inside_a_block_is_not_rolled_back()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Remote DML: row-by-row by primary key, four-part targets")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) for otterstax; the backends are reached by their "
                             "docker names, so the file needs the test network either way")
    args = parser.parse_args()

    tests = RemoteRowDmlTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - remote row DML: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - remote row DML\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
