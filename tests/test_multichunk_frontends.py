# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Results larger than one chunk, on every wire and through COPY ... TO.

The engine caps a chunk at 1024 rows and a result spans a VECTOR of chunks —
`session_payload` exposes size()/column_count() across the whole vector, and
every frontend and every writer has to walk it. A single-chunk result cannot
tell a correct walker from one that only ever reads chunks.front().

This file uses the 5000-row web_events.csv fixture — five chunks — on:

  * PG simple query protocol   (no parameters)
  * PG extended protocol       (Bind/Execute, forced by passing a parameter)
  * MySQL text protocol        (COM_QUERY)
  * MySQL binary protocol      (COM_STMT_PREPARE/COM_STMT_EXECUTE, binary resultset)
  * FlightSQL DoGet            (the multi-batch ChunkBatchReader)
  * COPY ... TO csv/ndjson/parquet, local file AND s3://, read back through a
    fresh external table and compared ROW BY ROW against the source table
    (a writer that emits the first chunk 5 times has the right count and the
    wrong content)

A silent regression here looks like exactly 1024 rows coming back.
"""

import argparse
import os
import sys
import time

import psycopg
import mysql.connector
from flightsql import FlightSQLClient

import config
from external_helpers import DATASETS, FILE_FIXTURE_DIR, S3_ALIAS, S3_BUCKET

DB = "multichunk_db"
TABLE = "web_events"
FIXTURE = next(ds for ds in DATASETS if ds["name"] == TABLE)
COLUMNS = FIXTURE["columns"]         # event_id, campaign_id, product_id, event_type, session_seconds, value
EXPECTED_ROWS = FIXTURE["rows"]      # 5000, tests/minio/fixtures/generate_external_fixtures.py
CHUNK_CAPACITY = 1024                # engine's per-chunk cap — the number to beat

# Guard on the fixture definition, not on runtime state: if the fixture ever
# shrinks below one chunk, every case below stops proving anything.
assert EXPECTED_ROWS > CHUNK_CAPACITY, "web_events fixture no longer spans multiple chunks"


class MultiChunkFrontendTest:
    def __init__(self, local=False):
        self.local = local
        self.host = config.get_host(local)
        self.mysql_conn = None
        self.created = []
        # Paths the server wrote for COPY ... TO. Unique per run so a file left
        # by an earlier run can never stand in for a COPY that wrote nothing.
        self.copied_paths = []
        self.run_id = f"{os.getpid()}_{int(time.time())}"
        self.source_rows = None

    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    # ── fixture ─────────────────────────────────────────────────────────────
    def setup(self):
        """Load the 5000-row csv into otterbrix-internal storage."""
        # No `database=`: every reference below is fully qualified, and the
        # external-table tests connect the same way (external_helpers.conn_cfg).
        self.mysql_conn = mysql.connector.connect(
            host=self.host, port=config.MYSQL_PORT,
            user="testuser", password="testpass")
        cur = self.mysql_conn.cursor()
        cur.execute(
            f"CREATE EXTERNAL TABLE {DB}.{TABLE} "
            f"WITH (location = '{FILE_FIXTURE_DIR}/{TABLE}.csv', format = 'csv')")
        self.created.append((DB, TABLE))
        cur.execute(f"SELECT COUNT(*) FROM {DB}.{TABLE}")
        loaded = int(cur.fetchall()[0][0])
        self.assert_equal(loaded, EXPECTED_ROWS, "fixture must load in full")
        self.source_rows = self._ordered_rows(cur, TABLE)
        self.assert_equal(len(self.source_rows), EXPECTED_ROWS, "source snapshot row count")
        cur.close()

    def cleanup(self):
        if self.mysql_conn is None:
            return
        try:
            cur = self.mysql_conn.cursor()
            for db, table in self.created:
                try:
                    cur.execute(f"DROP TABLE {db}.{table}")
                except Exception as e:
                    print(f"cleanup: DROP {db}.{table} failed: {e}")
            cur.close()
        finally:
            self.mysql_conn.close()
        # COPY ... TO writes on the SERVER's filesystem. With --local that is
        # this host, so the files are removed here; in the docker stack they
        # live in the test-otterstax container, which is discarded with the
        # stack, and there is nothing reachable to delete.
        for path in self.copied_paths:
            if os.path.exists(path):
                os.remove(path)
            elif self.local:
                print(f"cleanup: COPY output {path} not found on this host")

    # ── helpers ─────────────────────────────────────────────────────────────
    def _ordered_rows(self, cur, table):
        cur.execute(f"SELECT {', '.join(COLUMNS)} FROM {DB}.{table} ORDER BY event_id")
        return cur.fetchall()

    @staticmethod
    def _same_cell(a, b):
        # Formats disagree on how a number comes back (csv/ndjson re-infer
        # types, the MySQL wire may hand a decimal over as text); the content
        # check is about VALUES, so numerics compare as numbers and the rest
        # as their text form.
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return str(a) == str(b)
        return abs(fa - fb) <= 1e-6 * max(1.0, abs(fb))

    def _assert_same_rows(self, actual, label):
        self.assert_equal(len(actual), len(self.source_rows), f"{label}: row count")
        for idx, (got, want) in enumerate(zip(actual, self.source_rows)):
            self.assert_equal(len(got), len(want), f"{label}: column count at row {idx}")
            for col, (g, w) in zip(COLUMNS, zip(got, want)):
                if not self._same_cell(g, w):
                    raise AssertionError(
                        f"{label}: row {idx} column {col} differs: got {got!r}, want {want!r}")

    # ── PostgreSQL wire ─────────────────────────────────────────────────────
    def test_pg_simple_protocol_spans_chunks(self):
        """PG simple query: all 5000 rows, not the first chunk."""
        with psycopg.connect(host=self.host, port=config.PG_PORT, user="testuser",
                             password="testpass", dbname=DB, autocommit=True) as conn:
            conn.prepare_threshold = None
            with conn.cursor() as cur:
                cur.execute(f"SELECT event_id, campaign_id, value FROM {DB}.{TABLE}")
                rows = cur.fetchall()
        self.assert_equal(len(rows), EXPECTED_ROWS, "PG simple protocol row count")
        self.assert_equal(len(set(r[0] for r in rows)), EXPECTED_ROWS,
                          "event_id must be unique — a repeated chunk would collide")
        print(f"  PG simple: {len(rows)} rows across ≥{EXPECTED_ROWS // CHUNK_CAPACITY + 1} chunks")

    def test_pg_extended_protocol_spans_chunks(self):
        """PG Bind/Execute: same, through the row-limit-aware path."""
        with psycopg.connect(host=self.host, port=config.PG_PORT, user="testuser",
                             password="testpass", dbname=DB, autocommit=True) as conn:
            conn.prepare_threshold = None
            with conn.cursor() as cur:
                # A parameter switches psycopg3 to the extended protocol, which
                # counts emitted rows itself and honours the Execute row limit.
                cur.execute(
                    f"SELECT event_id, campaign_id, value FROM {DB}.{TABLE} WHERE campaign_id > %s",
                    (0,))
                rows = cur.fetchall()
                reported = cur.rowcount
        self.assert_equal(len(rows), EXPECTED_ROWS, "PG extended protocol row count")
        self.assert_equal(reported, EXPECTED_ROWS, "extended protocol must report every emitted row")
        print(f"  PG extended: {len(rows)} rows")

    # ── MySQL wire ──────────────────────────────────────────────────────────
    def test_mysql_text_protocol_spans_chunks(self):
        """MySQL COM_QUERY: all 5000 rows in the text resultset."""
        cur = self.mysql_conn.cursor()
        cur.execute(f"SELECT event_id, campaign_id, value FROM {DB}.{TABLE}")
        rows = cur.fetchall()
        cur.close()
        self.assert_equal(len(rows), EXPECTED_ROWS, "MySQL text protocol row count")
        self.assert_equal(len(set(r[0] for r in rows)), EXPECTED_ROWS,
                          "event_id must be unique — a repeated chunk would collide")
        print(f"  MySQL text: {len(rows)} rows")

    def test_mysql_prepared_protocol_spans_chunks(self):
        """MySQL COM_STMT_EXECUTE: the binary resultset walks every chunk."""
        cur = self.mysql_conn.cursor(prepared=True)
        cur.execute(
            f"SELECT event_id, campaign_id, value FROM {DB}.{TABLE} WHERE campaign_id > ?",
            (0,))
        rows = cur.fetchall()
        cur.close()
        self.assert_equal(len(rows), EXPECTED_ROWS, "MySQL binary protocol row count")
        self.assert_equal(len(set(r[0] for r in rows)), EXPECTED_ROWS,
                          "event_id must be unique — a repeated chunk would collide")
        print(f"  MySQL prepared: {len(rows)} rows")

    # ── FlightSQL ───────────────────────────────────────────────────────────
    def _flight_client(self):
        return FlightSQLClient(host=self.host, port=config.FLIGHT_PORT, insecure=True)

    def _flight_rows(self, sql):
        client = self._flight_client()
        info = client.execute(sql)
        reader = client.do_get(info.endpoints[0].ticket)
        return reader.read_all()

    def test_flightsql_spans_chunks(self):
        """FlightSQL DoGet: the multi-batch reader must emit every chunk."""
        table = self._flight_rows(f"SELECT event_id, campaign_id, value FROM {DB}.{TABLE}")
        print(f"  FlightSQL schema: {[f.name for f in table.schema]}")
        self.assert_equal(table.num_rows, EXPECTED_ROWS, "FlightSQL row count")
        # num_rows alone is not enough: a stream with the right row count and
        # ZERO columns is exactly what a missing prepared schema produced, and it
        # drops the data silently.
        self.assert_equal(table.num_columns, 3, "FlightSQL must carry the selected columns, not just rows")
        # Uniqueness by POSITION, not by name — this case is about chunk walking;
        # column naming is asserted by the FlightSQL schema tests.
        ids = table.column(0).to_pylist()
        self.assert_equal(len(set(ids)), EXPECTED_ROWS, "first column must be unique across batches")
        # Batch count is reported, not asserted: a reader that concatenates
        # batches is still correct, and the row count is what proves it walked
        # the whole chunk vector.
        print(f"  FlightSQL: {table.num_rows} rows in {table.column(0).num_chunks} batches")

    # ── COPY ... TO round-trips ─────────────────────────────────────────────
    def _copy_roundtrip(self, fmt, target, with_options, label):
        """COPY the whole table out, load it back, compare count AND content."""
        readback = f"{TABLE}_{label}_{fmt}_back"
        cur = self.mysql_conn.cursor()
        cur.execute(f"COPY (SELECT * FROM {DB}.{TABLE}) TO '{target}' WITH ({with_options}format = '{fmt}')")
        cur.execute(f"CREATE EXTERNAL TABLE {DB}.{readback} "
                    f"WITH ({with_options}location = '{target}', format = '{fmt}')")
        self.created.append((DB, readback))
        cur.execute(f"SELECT COUNT(*) FROM {DB}.{readback}")
        total = int(cur.fetchall()[0][0])
        self.assert_equal(total, EXPECTED_ROWS, f"COPY ... TO {label} {fmt} round-trip row count")
        rows = self._ordered_rows(cur, readback)
        cur.close()
        self._assert_same_rows(rows, f"COPY ... TO {label} {fmt} round-trip content")
        print(f"  COPY -> {label} {fmt} -> back: {total} rows, content identical")

    def _file_roundtrip(self, fmt):
        target = f"/tmp/multichunk_{self.run_id}_{fmt}.{fmt}"
        self.copied_paths.append(target)
        self._copy_roundtrip(fmt, target, "", "file")

    def _s3_roundtrip(self, fmt):
        target = f"s3://{S3_BUCKET}/exported/multichunk_{self.run_id}_{fmt}.{fmt}"
        self._copy_roundtrip(fmt, target, f"s3_alias = '{S3_ALIAS}', ", "s3")

    def test_copy_to_csv_spans_chunks(self):
        """COPY ... TO csv (local file) writes every chunk, not the first."""
        self._file_roundtrip("csv")

    def test_copy_to_ndjson_spans_chunks(self):
        """COPY ... TO ndjson (local file) writes every chunk."""
        self._file_roundtrip("ndjson")

    def test_copy_to_parquet_spans_chunks(self):
        """COPY ... TO parquet (local file) writes every chunk."""
        self._file_roundtrip("parquet")

    def test_copy_to_s3_csv_spans_chunks(self):
        """COPY ... TO 's3://...' csv uploads every chunk, not the first."""
        self._s3_roundtrip("csv")

    def test_copy_to_s3_ndjson_spans_chunks(self):
        """COPY ... TO 's3://...' ndjson uploads every chunk."""
        self._s3_roundtrip("ndjson")

    def test_copy_to_s3_parquet_spans_chunks(self):
        """COPY ... TO 's3://...' parquet uploads every chunk."""
        self._s3_roundtrip("parquet")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_pg_simple_protocol_spans_chunks()
            self.test_pg_extended_protocol_spans_chunks()
            self.test_mysql_text_protocol_spans_chunks()
            self.test_mysql_prepared_protocol_spans_chunks()
            self.test_flightsql_spans_chunks()
            self.test_copy_to_csv_spans_chunks()
            self.test_copy_to_ndjson_spans_chunks()
            self.test_copy_to_parquet_spans_chunks()
            self.test_copy_to_s3_csv_spans_chunks()
            self.test_copy_to_s3_ndjson_spans_chunks()
            self.test_copy_to_s3_parquet_spans_chunks()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(
        description="Multi-chunk (>1024 row) results: PG simple/extended, MySQL text/binary, "
                    "FlightSQL DoGet, COPY ... TO file and s3 (count + content)")
    parser.add_argument("--local", action="store_true",
                        help="Use local host (127.0.0.1) instead of test-otterstax")
    args = parser.parse_args()

    tests = MultiChunkFrontendTest(local=args.local)
    try:
        tests.run_all_tests()
    except Exception:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - multi-chunk frontends\033[0m")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("\033[92m✅ ALL TESTS PASSED - multi-chunk frontends\033[0m")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
