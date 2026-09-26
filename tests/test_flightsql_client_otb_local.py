# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""FlightSQL over otterbrix-internal tables: the GetFlightInfo / DoGet contract.

GetFlightInfoStatement hands out the schema the Worker resolved at prepare time
and DoGetStatement streams record batches over exactly that schema; nothing is
re-derived from the result rows. Two things follow and are asserted here:

  * a local JOIN whose two sides share the key name streams BOTH key columns —
    four columns, `id` twice, every cell fed from its own source column — and
    the stream schema is the FlightInfo schema;
  * a statement whose schema cannot be handed out is refused by GetFlightInfo
    itself, before a ticket exists: a `$1` placeholder (nothing binds it on this
    path) and a column Arrow cannot carry (an unsigned 128-bit integer);
  * a DECIMAL and a HUGEINT column do stream: both ride Arrow's decimal128 —
    the DECIMAL under its own precision and scale, the HUGEINT at scale 0,
    where the stored unscaled integer is the value itself.

Everything goes through the FlightSQL port: DDL and DML via DoPut
(`execute_update`), queries via GetFlightInfo + DoGet.
"""

import sys
import argparse

import pyarrow as pa
from flightsql import FlightSQLClient

import config

DB = "flight_local_db"


class FlightSQLLocalTableTest:
    def __init__(self, local=False):
        self.host = config.get_host(local)
        self.client = None
        self.created_tables = []

    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    # ── fixture ─────────────────────────────────────────────────────────────
    def setup(self):
        """Two INT tables sharing the key name, plus one table per 128-bit-wide type."""
        self.client = FlightSQLClient(host=self.host, port=config.FLIGHT_PORT, insecure=True)
        self._update(f"CREATE DATABASE {DB}")
        self._create_table("a", "(id INT, x INT)")
        self._create_table("b", "(id INT, y INT)")
        self._create_table("wide", "(id INT, amount decimal(38, 10))")
        self._create_table("huge", "(id INT, big hugeint)")
        self._create_table("uhuge", "(id INT, big uhugeint)")
        self._update(f"INSERT INTO {DB}.a (id, x) VALUES (1, 10), (2, 20)")
        self._update(f"INSERT INTO {DB}.b (id, y) VALUES (1, 100), (2, 200)")

    def cleanup(self):
        if self.client is None:
            return
        for table in self.created_tables:
            try:
                self._update(f"DROP TABLE {DB}.{table}")
            except Exception as e:
                print(f"cleanup: DROP TABLE {DB}.{table} failed: {e}")
        try:
            self._update(f"DROP DATABASE {DB}")
        except Exception as e:
            print(f"cleanup: DROP DATABASE {DB} failed: {e}")

    # ── helpers ─────────────────────────────────────────────────────────────
    def _update(self, sql):
        return self.client.execute_update(sql)

    def _create_table(self, name, columns):
        self._update(f"CREATE TABLE {DB}.{name} {columns}")
        self.created_tables.append(name)

    def _query(self, sql):
        """GetFlightInfo + DoGet; returns (FlightInfo, table)."""
        info = self.client.execute(sql)
        reader = self.client.do_get(info.endpoints[0].ticket)
        return info, reader.read_all()

    def _assert_refused_at_get_flight_info(self, sql, *needles):
        """GetFlightInfo must fail with a message naming the cause; no ticket is issued."""
        try:
            self.client.execute(sql)
        except pa.ArrowException as e:
            message = str(e)
            for needle in needles:
                if needle not in message:
                    raise AssertionError(
                        f"GetFlightInfo error for {sql!r} does not mention {needle!r}: {message}")
            print(f"  refused as expected: {message.splitlines()[0]}")
            return
        raise AssertionError(f"GetFlightInfo accepted {sql!r}; it must be refused")

    # ── cases ───────────────────────────────────────────────────────────────
    def test_join_with_duplicate_key_names_streams_every_column(self):
        """A local JOIN keeps both `id` columns; the stream matches the FlightInfo schema."""
        info, table = self._query(f"SELECT * FROM {DB}.a JOIN {DB}.b ON a.id = b.id")
        names = table.schema.names
        print(f"  JOIN schema: {names}")
        self.assert_equal(len(names), 4, "a JOIN of (id, x) with (id, y) has four columns")
        self.assert_equal(names.count("id"), 2, "both key columns must survive")
        self.assert_equal(table.num_rows, 2, "JOIN row count")
        table.validate(full=True)

        # The schema handed out with the ticket IS the stream's schema.
        self.assert_equal(info.schema.names, names, "FlightInfo schema vs stream schema (names)")
        self.assert_equal([f.type for f in info.schema], [f.type for f in table.schema],
                          "FlightInfo schema vs stream schema (types)")

        # Every column is fed from its own source: (a.id, x, b.id, y) per row.
        id_cols = [i for i, n in enumerate(names) if n == "id"]
        x_col, y_col = names.index("x"), names.index("y")
        rows = sorted(zip(table.column(id_cols[0]).to_pylist(),
                          table.column(x_col).to_pylist(),
                          table.column(id_cols[1]).to_pylist(),
                          table.column(y_col).to_pylist()))
        self.assert_equal(rows, [(1, 10, 1, 100), (2, 20, 2, 200)], "JOIN rows")

    def test_placeholder_is_refused_before_a_ticket_is_issued(self):
        """A `$1` SELECT has no resolved schema at prepare; GetFlightInfo refuses it."""
        self._assert_refused_at_get_flight_info(
            f"SELECT id, x FROM {DB}.a WHERE id = $1", "unbound parameter")

    def test_decimal_column_is_carried_with_its_scale(self):
        """A DECIMAL rides decimal128 under its own precision and scale, not as an integer."""
        info, table = self._query(f"SELECT id, amount FROM {DB}.wide")
        self.assert_equal(table.schema.field("amount").type, pa.decimal128(38, 10),
                          "DECIMAL(38,10) keeps its scale on the stream")
        self.assert_equal(info.schema.field("amount").type, pa.decimal128(38, 10),
                          "and the FlightInfo schema announces the same type")
        self.assert_equal(table.num_rows, 0, "the wide table is empty")

    def test_hugeint_column_is_carried_as_an_unscaled_decimal(self):
        """Arrow has no 128-bit integer; a HUGEINT rides decimal128 at scale 0."""
        info, table = self._query(f"SELECT big FROM {DB}.huge")
        self.assert_equal(table.schema.field("big").type, pa.decimal128(38, 0),
                          "HUGEINT rides decimal128(38, 0)")
        self.assert_equal(info.schema.field("big").type, pa.decimal128(38, 0),
                          "and the FlightInfo schema announces the same type")

    def test_unsigned_hugeint_column_is_refused_before_a_ticket_is_issued(self):
        """An unsigned 128-bit integer has no Arrow carrier; the error names the column."""
        self._assert_refused_at_get_flight_info(
            f"SELECT big FROM {DB}.uhuge", "Arrow", "big")

    def test_mappable_columns_of_the_same_table_still_stream(self):
        """The refusal is per column: the INT column of the unsigned table streams normally."""
        info, table = self._query(f"SELECT id FROM {DB}.uhuge")
        self.assert_equal(table.schema.names, ["id"], "only the selected column")
        self.assert_equal(table.schema.field(0).type, pa.int32(), "INT maps to int32")
        self.assert_equal(info.schema.names, ["id"], "FlightInfo schema")
        self.assert_equal(table.num_rows, 0, "the uhuge table is empty")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_join_with_duplicate_key_names_streams_every_column()
            self.test_placeholder_is_refused_before_a_ticket_is_issued()
            self.test_decimal_column_is_carried_with_its_scale()
            self.test_hugeint_column_is_carried_as_an_unscaled_decimal()
            self.test_unsigned_hugeint_column_is_refused_before_a_ticket_is_issued()
            self.test_mappable_columns_of_the_same_table_still_stream()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(
        description="FlightSQL GetFlightInfo/DoGet schema contract over otterbrix-internal tables")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()

    tests = FlightSQLLocalTableTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"\033[91m❌ TEST FAILED - FlightSQL local-table schema contract: {e}\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - FlightSQL local-table schema contract\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
