# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Flight SQL: the GetTables listing and the session scope of a statement.

  * GetTables answers one listing of the engine's catalog, local and remote
    tables alike (frontend/flight_sql/session_engine.cpp, SessionEngine::tables):
    a catalog is a database as SQL writes it — `d` for a local one, `c.d` for
    the database d of the connection c (MySQL / ClickHouse: `def`) — and the
    schema is a schema of it: a database of the backend on MySQL / ClickHouse
    (`db1`), a schema of the configured database on PostgreSQL (`public`), so
    catalog.schema.table is the name the SQL path reads. The database
    `federation` (the remote-table spellings and the Kafka bookkeeping tables)
    and the system schemas are hidden. With include_schema each row carries
    its columns.
  * A statement run by text runs on a query session of its own, closed when the
    RPC returns (autocommit). A transaction opened there could never be ended,
    so a BEGIN — through the query RPC (GetFlightInfo) or the update RPC
    (DoPut) — is rolled back and refused; COMMIT / ROLLBACK by text end nothing
    and are accepted (frontend/flight_sql/session_engine.cpp,
    tests/system/test_flightsql_session_scope.cpp).
"""

import argparse
import sys

import pyarrow as pa
import pyarrow.ipc as ipc
from flightsql import FlightSQLClient

import config

# The local table: a schema of local (`local.flight_tables_db.local_items` —
# Flight SQL has no place field, so every table statement names it in full),
# listed by GetTables under catalog `local`, schema `flight_tables_db`.
LOCAL_CATALOG = "local"
SCHEMA = "flight_tables_db"
DB = f"local.{SCHEMA}"
TABLE = "local_items"

# A table every integration stack registers (tests/scripts/config.yaml:
# alias `campaigns` over MariaDB database db1, seeded by create_test_data.py).
# A MySQL connection's database is `def` (catalog::def_database) and db1 is the
# schema under it: campaigns.def / db1 / campaigns.
REMOTE_CATALOG = "campaigns.def"
REMOTE_SCHEMA = "db1"
REMOTE_TABLE = "campaigns"

BEGIN_REFUSAL = "a transaction cannot be opened over Flight SQL"


class FlightSqlTablesAndTransactionsTest:
    def __init__(self, local=False):
        self.host = config.get_host(local)
        self.client = None
        self.database_created = False
        self.table_created = False

    # ── helpers ─────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def update(self, sql):
        return self.client.execute_update(sql)

    def query(self, sql):
        info = self.client.execute(sql)
        return self.client.do_get(info.endpoints[0].ticket).read_all()

    def listing(self):
        """Every GetTables row (include_schema) as a dict, the schema deserialized."""
        info = self.client.get_tables(include_schema=True)
        table = self.client.do_get(info.endpoints[0].ticket).read_all()
        rows = []
        for i in range(table.num_rows):
            schema_bytes = table["table_schema"][i].as_py()
            rows.append({
                "catalog": table["catalog_name"][i].as_py(),
                "schema": table["db_schema_name"][i].as_py(),
                "table": table["table_name"][i].as_py(),
                "fields": None if schema_bytes is None else
                {f.name: f.type for f in ipc.read_schema(pa.py_buffer(schema_bytes))},
            })
        return rows

    def expect_refused(self, run, sql, needle, what):
        try:
            run(sql)
        except Exception as e:  # pyarrow.flight errors derive from ArrowException
            message = str(e)
            if needle not in message:
                raise AssertionError(f"{what}: the refusal of {sql!r} must mention {needle!r}: {message}")
            print(f"  {what}: refused as expected: {message.splitlines()[0]}")
            return
        raise AssertionError(f"{what}: {sql!r} must be refused")

    # ── setup / teardown ────────────────────────────────────────────────────
    def setup(self):
        self.client = FlightSQLClient(host=self.host, port=config.FLIGHT_PORT, insecure=True)
        self.update(f"CREATE SCHEMA {SCHEMA}")
        self.database_created = True
        self.update(f"CREATE TABLE {DB}.{TABLE} (id bigint, label string)")
        self.table_created = True

    def cleanup(self):
        if self.client is None:
            return
        try:
            if self.table_created:
                self.update(f"DROP TABLE {DB}.{TABLE}")
        except Exception as e:
            print(f"cleanup: DROP TABLE {DB}.{TABLE} failed: {e}")
        try:
            if self.database_created:
                self.update(f"DROP SCHEMA {SCHEMA}")
        except Exception as e:
            print(f"cleanup: DROP SCHEMA {SCHEMA} failed: {e}")

    # ── cases ───────────────────────────────────────────────────────────────
    def test_get_tables_lists_local_and_remote_tables(self):
        """GetTables holds the local table (catalog local, its schema) and a remote one (catalog c.d), each with its columns."""
        rows = self.listing()
        local = [r for r in rows if r["catalog"] == LOCAL_CATALOG and r["schema"] == SCHEMA
                 and r["table"] == TABLE]
        self.assert_equal(len(local), 1, f"the local table {LOCAL_CATALOG}.{DB}.{TABLE} is listed once")
        self.assert_equal(local[0]["fields"], {"id": pa.int64(), "label": pa.string()},
                          "the local table's columns")

        remote = [r for r in rows if r["catalog"] == REMOTE_CATALOG and r["schema"] == REMOTE_SCHEMA
                  and r["table"] == REMOTE_TABLE]
        self.assert_equal(len(remote), 1,
                          f"the remote table {REMOTE_CATALOG}.{REMOTE_SCHEMA}.{REMOTE_TABLE} is listed once")
        fields = remote[0]["fields"]
        if fields is None or fields.get("campaign_id") != pa.int32():
            raise AssertionError(f"the remote table's columns are missing or mistyped: {fields}")

        hidden = [r for r in rows if r["catalog"] == "federation"]
        self.assert_equal(hidden, [], "the federation database (remote spellings, Kafka bookkeeping) is not listed")
        print(f"  GetTables: {len(rows)} tables, {LOCAL_CATALOG}.{DB}.{TABLE} and "
              f"{REMOTE_CATALOG}.{REMOTE_SCHEMA}.{REMOTE_TABLE} among them ✓")

    def test_begin_through_the_query_rpc_is_refused(self):
        """BEGIN through GetFlightInfo is rolled back and refused."""
        self.expect_refused(self.client.execute, "BEGIN", BEGIN_REFUSAL, "BEGIN by GetFlightInfo")

    def test_begin_through_the_update_rpc_is_refused(self):
        """BEGIN through DoPut is rolled back and refused."""
        self.expect_refused(self.update, "BEGIN", BEGIN_REFUSAL, "BEGIN by DoPut")

    def test_statements_after_a_refused_begin_run_in_autocommit(self):
        """After the refused BEGIN every statement still commits on its own; COMMIT / ROLLBACK end nothing."""
        self.update(f"INSERT INTO {DB}.{TABLE} (id, label) VALUES (1, 'autocommit')")
        self.update("ROLLBACK")
        self.update("COMMIT")
        table = self.query(f"SELECT id, label FROM {DB}.{TABLE}")
        self.assert_equal(table.column("id").to_pylist(), [1], "the INSERT committed on its own session")
        print("  after a refused BEGIN: INSERT committed, ROLLBACK / COMMIT accepted ✓")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_get_tables_lists_local_and_remote_tables()
            self.test_begin_through_the_query_rpc_is_refused()
            self.test_begin_through_the_update_rpc_is_refused()
            self.test_statements_after_a_refused_begin_run_in_autocommit()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Flight SQL: GetTables listing and statement session scope")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()

    tests = FlightSqlTablesAndTransactionsTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - Flight SQL tables and transactions: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - Flight SQL tables and transactions\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
