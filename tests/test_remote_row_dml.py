# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Writes into remote tables through the engine.

A remote table is named in four parts, `alias.db.schema.table` (MySQL /
ClickHouse: the one schema `public`). Every INSERT / UPDATE / DELETE of one
runs on the engine, which offers it to the connection's backend
(otterbrix/federation/CLAUDE.md, "Offer"):

  * an UPDATE / DELETE the backend can say as ONE statement of its dialect is
    pushed whole — its count is the backend's (MySQL: the rows an UPDATE
    MATCHED, not the rows it changed); a table without a primary key is no
    obstacle;
  * an UPDATE / DELETE whose WHERE reads a local table through an
    uncorrelated sub-query (`WHERE id IN (SELECT id FROM <local table>)`) is one
    statement too: the engine computes the sub-query first and the backend gets
    its values as a list (`... IN (1, 2)`), so exactly the rows the sub-query
    names change;
  * an UPDATE / DELETE the backend cannot say whole is refused (0A000 /
    ER_NOT_SUPPORTED_YET) before anything is written: OtterStax numbers no
    remote row, so there is no row-by-row path;
  * RETURNING is sent to PostgreSQL and MariaDB, whose server answers what it
    has (MariaDB: INSERT / DELETE ... RETURNING, an UPDATE ... RETURNING its own
    syntax error); an INSERT ... SELECT from a local table runs (the engine hands
    the rows to the backend).

Inside an explicit block a PostgreSQL / MariaDB write runs in ONE backend
transaction: ROLLBACK undoes it on the backend, COMMIT publishes it. A
ClickHouse write inside a block is refused (ClickHouse has no transactions).

The remote tables are made here on the backends directly (pymysql to mariadb1,
psycopg2 to postgres1, as create_test_data.py does), so the file runs only
inside the test network; the server registers each one the first time a
statement names it.
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

MYSQL_KEYED = "campaigns.db1.rowdml_keyed"
MYSQL_NOKEY = "campaigns.db1.rowdml_nokey"
PG_KEYED = "products.public.rowdml_keyed"
PG_NOKEY = "products.public.rowdml_nokey"
CH_ORDERS = f"{config.CH_ALIAS}.{config.CH_DATABASE}.orders"

LOCAL_SCHEMA = "rowdml_local"            # a schema of the session's database, main
PICK = f"{LOCAL_SCHEMA}.pick"            # (id int): the ids a sub-query picks, 1 and 2
BIG_PICK = f"{LOCAL_SCHEMA}.big_pick"    # (id bigint): the order_ids a ClickHouse DELETE picks
NEW_ROWS = f"{LOCAL_SCHEMA}.new_rows"    # (id int, grp string, qty int): rows to copy into a remote table

# Rows 1, 2 and 4 share grp 'a'; rows 1, 2 and 3 share qty 5.
KEYED_ROWS = [(1, "a", 5), (2, "a", 5), (3, "b", 5), (4, "a", 7)]
NOKEY_ROWS = [(1, "a", 5), (1, "a", 5), (2, "b", 5)]

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

    def pg_refused(self, sql, needle, what, cur=None):
        """`sql` over the PostgreSQL wire must fail with 0A000 and a message holding `needle`."""
        try:
            if cur is not None:
                cur.execute(sql)
            else:
                with self.pg.cursor() as c:
                    c.execute(sql)
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
            for table in ("rowdml_keyed", "rowdml_nokey"):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute("CREATE TABLE rowdml_keyed (id INT PRIMARY KEY, grp VARCHAR(16) NOT NULL, qty INT NOT NULL)")
            cur.execute("CREATE TABLE rowdml_nokey (id INT NOT NULL, grp VARCHAR(16) NOT NULL, qty INT NOT NULL)")
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        self.reset_mariadb("rowdml_nokey", NOKEY_ROWS)

        self.postgres = psycopg2.connect(**POSTGRES)
        self.postgres.autocommit = True
        with self.postgres.cursor() as cur:
            for table in ("rowdml_keyed", "rowdml_nokey"):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute("CREATE TABLE rowdml_keyed (id integer PRIMARY KEY, grp varchar(16) NOT NULL, qty integer NOT NULL)")
            cur.execute("CREATE TABLE rowdml_nokey (id integer NOT NULL, grp varchar(16) NOT NULL, qty integer NOT NULL)")
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        self.reset_postgres("rowdml_nokey", NOKEY_ROWS)

        # The session's place is local: the place a client chose, one the engine
        # has (an unknown one is FATAL 3D000 / ERR 1049); the two-part local
        # names below complete by it.
        self.pg = psycopg.connect(host=self.host, port=config.PG_PORT, user="testuser",
                                  password="testpass", dbname="local", autocommit=True)
        self.my = pymysql.connect(host=self.host, port=config.MYSQL_PORT, user="testuser",
                                  password="testpass", database="local", autocommit=True)
        with self.pg.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {LOCAL_SCHEMA} CASCADE")
            cur.execute(f"CREATE SCHEMA {LOCAL_SCHEMA}")
            self.local_created = True
            cur.execute(f"CREATE TABLE {PICK} (id int)")
            cur.execute(f"INSERT INTO {PICK} (id) VALUES (1), (2)")
            cur.execute(f"CREATE TABLE {BIG_PICK} (id bigint)")
            cur.execute(f"INSERT INTO {BIG_PICK} (id) VALUES (990901), (990902)")
            cur.execute(f"CREATE TABLE {NEW_ROWS} (id int, grp string, qty int)")
            cur.execute(f"INSERT INTO {NEW_ROWS} (id, grp, qty) VALUES (60, 'n', 1), (61, 'n', 1)")
            # The tables were made on the backends after the server started: the
            # first statement naming each one registers it.
            for table in (MYSQL_KEYED, MYSQL_NOKEY, PG_KEYED, PG_NOKEY):
                cur.execute(f"SELECT count(*) FROM {table}")
                cur.fetchall()

    def cleanup(self):
        try:
            if self.pg is not None and self.local_created:
                with self.pg.cursor() as cur:
                    cur.execute(f"DROP SCHEMA {LOCAL_SCHEMA} CASCADE")
        finally:
            for conn in (self.pg, self.my):
                if conn is not None:
                    conn.close()
            if self.mariadb is not None:
                with self.mariadb.cursor() as cur:
                    for table in ("rowdml_keyed", "rowdml_nokey"):
                        cur.execute(f"DROP TABLE IF EXISTS {table}")
                self.mariadb.close()
            if self.postgres is not None:
                with self.postgres.cursor() as cur:
                    for table in ("rowdml_keyed", "rowdml_nokey"):
                        cur.execute(f"DROP TABLE IF EXISTS {table}")
                self.postgres.close()

    # ── MariaDB, over the MySQL wire ────────────────────────────────────────
    def test_mysql_pushed_update_counts_matched_rows(self):
        """MariaDB: an UPDATE pushed as one statement answers the rows it matched, not the rows it changed."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        self.assert_equal(self.my_count(f"UPDATE {MYSQL_KEYED} SET qty = 7 WHERE grp = 'a'"), 3,
                          "rows 1, 2 change, row 4 already holds 7: matched 3")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), [(1, "a", 7), (2, "a", 7), (3, "b", 5), (4, "a", 7)],
                          "the pushed UPDATE's effect")
        self.assert_equal(self.my_count(f"UPDATE {MYSQL_KEYED} SET qty = 7 WHERE grp = 'a'"), 3,
                          "the same UPDATE again changes nothing and still counts the matched rows")
        self.assert_equal(self.my_count(f"DELETE FROM {MYSQL_KEYED} WHERE id > 3"), 1, "pushed DELETE")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), [(1, "a", 7), (2, "a", 7), (3, "b", 5)],
                          "row 4 is gone")
        print("  MariaDB pushed UPDATE / DELETE: count = rows matched ✓")

    def test_mysql_pushed_write_needs_no_primary_key(self):
        """MariaDB, no primary key: an UPDATE / DELETE the backend runs whole needs none."""
        self.reset_mariadb("rowdml_nokey", NOKEY_ROWS)
        self.assert_equal(self.my_count(f"UPDATE {MYSQL_NOKEY} SET qty = 6 WHERE id = 1"), 2,
                          "both identical rows match")
        self.assert_equal(self.my_count(f"DELETE FROM {MYSQL_NOKEY} WHERE id = 2"), 1, "one row deleted")
        self.assert_equal(self.mariadb_rows("rowdml_nokey"), [(1, "a", 6), (1, "a", 6)], "the pushed writes' effect")
        print("  MariaDB table without a primary key: pushed UPDATE / DELETE run ✓")

    def test_mysql_write_with_a_local_subquery_changes_the_rows_it_picks(self):
        """MariaDB: an UPDATE / DELETE whose WHERE reads a local table changes exactly the rows the sub-query picks."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        self.assert_equal(self.my_count(f"UPDATE {MYSQL_KEYED} SET qty = 9 WHERE id IN (SELECT id FROM {PICK})"), 2,
                          "UPDATE with a local sub-query: the rows 1 and 2 it picks")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), [(1, "a", 9), (2, "a", 9), (3, "b", 5), (4, "a", 7)],
                          "only the picked rows changed")
        self.assert_equal(self.my_count(f"DELETE FROM {MYSQL_KEYED} WHERE id IN (SELECT id FROM {PICK})"), 2,
                          "DELETE with a local sub-query")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), [(3, "b", 5), (4, "a", 7)], "only the picked rows are gone")
        print("  MariaDB UPDATE / DELETE over a local sub-query: exactly the picked rows ✓")

    def mariadb_answers_update_returning(self):
        """Whether this MariaDB server takes UPDATE ... RETURNING (13.0 does, earlier ones refuse it): asked of the
        server itself, on a statement that changes no row."""
        try:
            with self.mariadb.cursor() as cur:
                cur.execute("UPDATE rowdml_keyed SET qty = qty WHERE id = -1 RETURNING id")
                cur.fetchall()
        except pymysql.MySQLError:
            return False
        return True

    def test_mysql_returning_is_the_servers(self):
        """MariaDB answers INSERT / DELETE ... RETURNING; its UPDATE ... RETURNING is whatever MariaDB answers:
        the rows on a server that has it, the server's own refusal on one that does not."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        with self.my.cursor() as cur:
            cur.execute(f"INSERT INTO {MYSQL_KEYED} (id, grp, qty) VALUES (5, 'c', 1) RETURNING id, grp")
            self.assert_equal([tuple(r) for r in cur.fetchall()], [(5, "c")], "INSERT ... RETURNING rows")
            cur.execute(f"DELETE FROM {MYSQL_KEYED} WHERE id = 5 RETURNING id, qty")
            self.assert_equal([tuple(r) for r in cur.fetchall()], [(5, 1)], "DELETE ... RETURNING rows")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), KEYED_ROWS, "inserted, then deleted")
        if self.mariadb_answers_update_returning():
            with self.my.cursor() as cur:
                cur.execute(f"UPDATE {MYSQL_KEYED} SET qty = 1 WHERE id = 3 RETURNING id, qty")
                self.assert_equal([tuple(r) for r in cur.fetchall()], [(3, 1)], "UPDATE ... RETURNING rows")
            expected = [r if r[0] != 3 else (3, r[1], 1) for r in KEYED_ROWS]
            self.assert_equal(self.mariadb_rows("rowdml_keyed"), expected, "the UPDATE wrote its row")
            self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
            print("  MariaDB INSERT / UPDATE / DELETE ... RETURNING answered by the server ✓")
            return
        try:
            with self.my.cursor() as cur:
                cur.execute(f"UPDATE {MYSQL_KEYED} SET qty = 1 WHERE id = 3 RETURNING id")
        except pymysql.MySQLError as e:
            assert "RETURNING" in str(e) or "syntax" in str(e).lower(), f"MariaDB's own refusal: {e}"
        else:
            raise AssertionError("MariaDB UPDATE ... RETURNING must be refused by a MariaDB that has none")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), KEYED_ROWS, "the refused UPDATE wrote nothing")
        print("  MariaDB INSERT / DELETE ... RETURNING answered, UPDATE ... RETURNING MariaDB's refusal ✓")

    # ── PostgreSQL, over the PostgreSQL wire ────────────────────────────────
    def test_pg_pushed_update_and_delete(self):
        """PostgreSQL: UPDATE / DELETE pushed as one statement change the matched rows and count them."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        self.assert_equal(self.pg_count(f"UPDATE {PG_KEYED} SET qty = 9 WHERE id <= 2"), 2, "two rows matched")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), [(1, "a", 9), (2, "a", 9), (3, "b", 5), (4, "a", 7)],
                          "rows 3 and 4 stay untouched")
        self.assert_equal(self.pg_count(f"DELETE FROM {PG_KEYED} WHERE qty = 9"), 2, "two rows deleted")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), [(3, "b", 5), (4, "a", 7)], "rows 1 and 2 are gone")
        print("  PostgreSQL pushed UPDATE / DELETE ✓")

    def test_pg_write_with_a_local_subquery_changes_the_rows_it_picks(self):
        """PostgreSQL: an UPDATE / DELETE whose WHERE reads a local table changes exactly the rows the sub-query picks."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        self.reset_postgres("rowdml_nokey", NOKEY_ROWS + [(3, "c", 5)])
        self.assert_equal(self.pg_count(f"UPDATE {PG_KEYED} SET qty = 9 WHERE id IN (SELECT id FROM {PICK})"), 2,
                          "UPDATE with a local sub-query: the rows 1 and 2 it picks")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), [(1, "a", 9), (2, "a", 9), (3, "b", 5), (4, "a", 7)],
                          "only the picked rows changed")
        self.assert_equal(self.pg_count(f"DELETE FROM {PG_NOKEY} WHERE id IN (SELECT id FROM {PICK})"), 3,
                          "DELETE with a local sub-query: both rows of id 1 and the row of id 2, no key needed")
        self.assert_equal(self.postgres_rows("rowdml_nokey"), [(3, "c", 5)], "only the picked rows are gone")
        print("  PostgreSQL UPDATE / DELETE over a local sub-query: exactly the picked rows ✓")

    def test_pg_update_returning_answers_the_written_rows(self):
        """PostgreSQL: UPDATE ... RETURNING answers the rows the backend wrote."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        with self.pg.cursor() as cur:
            cur.execute(f"UPDATE {PG_KEYED} SET qty = 1 WHERE id = 3 RETURNING id, qty")
            self.assert_equal([tuple(r) for r in cur.fetchall()], [(3, 1)], "the RETURNING rows")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), [(1, "a", 5), (2, "a", 5), (3, "b", 1), (4, "a", 7)],
                          "the UPDATE's effect")
        print("  PostgreSQL UPDATE ... RETURNING ✓")

    def test_insert_select_from_a_local_table(self):
        """INSERT ... SELECT from a local table into a remote one: the engine hands the rows to the backend."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        self.assert_equal(self.pg_count(f"INSERT INTO {PG_KEYED} (id, grp, qty) SELECT id, grp, qty FROM {NEW_ROWS}"), 2,
                          "both rows written")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), KEYED_ROWS + [(60, "n", 1), (61, "n", 1)],
                          "the copied rows are on the backend")
        print("  INSERT ... SELECT from a local table into PostgreSQL ✓")

    # ── cross-backend DML ─────────────────────────────────────────────────────
    def test_pg_update_with_a_subquery_over_another_backend(self):
        """UPDATE PostgreSQL rows selected by a MariaDB subquery changes exactly those rows."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        self.assert_equal(self.pg_count(f"UPDATE {PG_KEYED} SET qty = 9 WHERE id IN (SELECT id FROM {MYSQL_KEYED} WHERE grp = 'a')"), 3)
        self.assert_equal(self.postgres_rows("rowdml_keyed"), [(1, "a", 9), (2, "a", 9), (3, "b", 5), (4, "a", 9)])
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), KEYED_ROWS)

    def test_pg_delete_with_a_subquery_over_another_backend(self):
        """DELETE PostgreSQL rows selected by a MariaDB subquery removes exactly those rows."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        self.assert_equal(self.pg_count(f"DELETE FROM {PG_KEYED} WHERE id IN (SELECT id FROM {MYSQL_KEYED} WHERE grp = 'b')"), 1)
        self.assert_equal(self.postgres_rows("rowdml_keyed"), [(1, "a", 5), (2, "a", 5), (4, "a", 7)])
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), KEYED_ROWS)

    # ── ClickHouse ──────────────────────────────────────────────────────────
    def test_clickhouse_write_with_a_local_subquery_deletes_the_rows_it_picks(self):
        """ClickHouse: a DELETE whose WHERE reads a local table deletes exactly the orders the sub-query picks."""
        picked = (990901, 990902)
        kept = 990903
        with self.pg.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {CH_ORDERS}")
            before = int(cur.fetchall()[0][0])
            cur.execute(
                f"INSERT INTO {CH_ORDERS}"
                f" (order_id, campaign_id, product_id, customer_name, order_date, quantity, total_amount) VALUES"
                f" ({picked[0]}, 1, 1, 'Picked One', '2026-01-01 00:00:00', 1, 1.5),"
                f" ({picked[1]}, 1, 1, 'Picked Two', '2026-01-01 00:00:00', 1, 1.5),"
                f" ({kept}, 1, 1, 'Kept', '2026-01-01 00:00:00', 1, 1.5)")
            try:
                # A mutation: ClickHouse answers no count (0); the rows tell what it did.
                cur.execute(f"DELETE FROM {CH_ORDERS} WHERE order_id IN (SELECT id FROM {BIG_PICK})")
                cur.execute(f"SELECT order_id FROM {CH_ORDERS} WHERE order_id >= {picked[0]} AND order_id <= {kept}"
                            f" ORDER BY order_id")
                self.assert_equal([r[0] for r in cur.fetchall()], [kept], "only the picked orders are gone")
            finally:
                cur.execute(f"DELETE FROM {CH_ORDERS} WHERE order_id >= {picked[0]} AND order_id <= {kept}")
            cur.execute(f"SELECT count(*) FROM {CH_ORDERS}")
            self.assert_equal(int(cur.fetchall()[0][0]), before, "no other order was touched")
        print("  ClickHouse DELETE over a local sub-query: exactly the picked orders ✓")

    def test_clickhouse_write_inside_a_block_is_refused(self):
        """ClickHouse has no transactions: a write inside BEGIN is refused (and fails the block)."""
        with self.pg.cursor() as cur:
            cur.execute("BEGIN")
            self.pg_refused(f"DELETE FROM {CH_ORDERS} WHERE order_id = 990999",
                            "no transactions", "ClickHouse write inside a block", cur=cur)
            cur.execute("ROLLBACK")
        print("  ClickHouse write inside BEGIN refused (0A000) ✓")

    # ── transactions ────────────────────────────────────────────────────────
    def test_pg_remote_write_inside_a_block_follows_rollback_and_commit(self):
        """PostgreSQL: a remote write inside BEGIN is one backend transaction — ROLLBACK undoes it, COMMIT keeps it."""
        self.reset_postgres("rowdml_keyed", KEYED_ROWS)
        with self.pg.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(f"UPDATE {PG_KEYED} SET qty = 42 WHERE id = 3")
            self.assert_equal(cur.rowcount, 1, "UPDATE inside the block")
            cur.execute(f"INSERT INTO {PG_KEYED} (id, grp, qty) VALUES (70, 't', 1)")
            self.assert_equal(cur.rowcount, 1, "INSERT inside the block")
            self.assert_equal(self.postgres_rows("rowdml_keyed"), KEYED_ROWS,
                              "another backend connection sees nothing before the block ends")
            cur.execute("ROLLBACK")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), KEYED_ROWS, "ROLLBACK undid both writes on the backend")

        with self.pg.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(f"INSERT INTO {PG_KEYED} (id, grp, qty) VALUES (71, 'c', 1)")
            cur.execute("COMMIT")
        self.assert_equal(self.postgres_rows("rowdml_keyed"), KEYED_ROWS + [(71, "c", 1)], "COMMIT published the write")
        print("  PostgreSQL remote writes inside BEGIN: ROLLBACK undoes, COMMIT keeps ✓")

    def test_mysql_remote_write_inside_a_block_follows_rollback(self):
        """MariaDB: a remote write inside BEGIN is undone on the backend by ROLLBACK."""
        self.reset_mariadb("rowdml_keyed", KEYED_ROWS)
        with self.my.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(f"UPDATE {MYSQL_KEYED} SET qty = 42 WHERE id = 3")
            cur.execute(f"INSERT INTO {MYSQL_KEYED} (id, grp, qty) VALUES (70, 't', 1)")
            cur.execute("ROLLBACK")
        self.assert_equal(self.mariadb_rows("rowdml_keyed"), KEYED_ROWS, "ROLLBACK undid both writes on the backend")
        print("  MariaDB remote writes inside BEGIN undone by ROLLBACK ✓")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_mysql_pushed_update_counts_matched_rows()
            self.test_mysql_pushed_write_needs_no_primary_key()
            self.test_mysql_write_with_a_local_subquery_changes_the_rows_it_picks()
            self.test_mysql_returning_is_the_servers()
            self.test_pg_pushed_update_and_delete()
            self.test_pg_write_with_a_local_subquery_changes_the_rows_it_picks()
            self.test_pg_update_returning_answers_the_written_rows()
            self.test_insert_select_from_a_local_table()
            self.test_pg_update_with_a_subquery_over_another_backend()
            self.test_pg_delete_with_a_subquery_over_another_backend()
            self.test_clickhouse_write_with_a_local_subquery_deletes_the_rows_it_picks()
            self.test_clickhouse_write_inside_a_block_is_refused()
            self.test_pg_remote_write_inside_a_block_follows_rollback_and_commit()
            self.test_mysql_remote_write_inside_a_block_follows_rollback()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Remote DML: pushed whole or refused, transactions on the backend")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) for otterstax; the backends are reached by their "
                             "docker names, so the file needs the test network either way")
    args = parser.parse_args()

    tests = RemoteRowDmlTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - remote DML: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - remote DML\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
