# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Spark Connect frontend (port 15002) through pyspark-client, behind the same
DB-API-shaped surface as the mysql and postgres connectors: connect() returns a
connection whose cursor runs `spark.sql(...)`.

A query's rows are fetched as Arrow (`DataFrame.toArrow()`), the wire format, and
read row by row only when a caller iterates them. A statement that is not a query
(INSERT, DDL, CREATE EXTERNAL TABLE, COPY) runs at once inside `spark.sql()`, as a
Spark server runs a command; Spark Connect reports no affected-row count, so
`rowcount` is -1."""
from pyspark.sql import SparkSession

FRONTEND = "spark"
DEFAULT_PORT = 15002


def _session(host, port):
    # create(), not getOrCreate(): every connection is a session of its own, as a
    # mysql / postgres connection is.
    return SparkSession.builder.remote(f"sc://{host}:{port}").create()


class _Rows:
    """An Arrow result read as a sequence of row tuples."""

    def __init__(self, table):
        self._table = table

    def __len__(self):
        return self._table.num_rows

    def __getitem__(self, i):
        return tuple(column[i].as_py() for column in self._table.columns)

    def __iter__(self):
        return zip(*(column.to_pylist() for column in self._table.columns))


class _Cursor:
    def __init__(self, spark, prepared=False):
        self._spark = spark
        self._prepared = prepared
        self._rows = _Rows(_empty_table())
        self.rowcount = -1

    def execute(self, sql):
        df = self._spark.sql(sql)
        if self._prepared:
            df.schema  # AnalyzePlan: OtterStax's prepare_schema, the describe probe
        self._rows = _Rows(df.toArrow())

    def fetchall(self):
        return self._rows

    def close(self):
        pass


def _empty_table():
    import pyarrow as pa

    return pa.table({})


class _Connection:
    def __init__(self, spark):
        self._spark = spark

    def cursor(self, prepared=False):
        return _Cursor(self._spark, prepared)

    def commit(self):
        pass

    def close(self):
        self._spark.stop()


def connect(host, port):
    return _Connection(_session(host, port))


def make_fetch_factory(host, port):
    # A session per fetch, as the mysql / postgres connectors reconnect per repetition.
    def make_fetch(sql):
        def fetch():
            conn = connect(host, port)
            try:
                cur = conn.cursor()
                cur.execute(sql)
                return cur.fetchall()
            finally:
                conn.close()

        return fetch

    return make_fetch


# ── the prepare test (benchmarks/backends/suite.py, <backend>_prepare) ────────
def prepare_connect(host, port):
    return connect(host, port)


def statement_cursor(conn, prepared):
    """Plain: spark.sql(...) then the rows. Prepared: df.schema first — the plan is
    described (AnalyzePlan → Scheduler::prepare_schema) before it runs."""
    return conn.cursor(prepared=prepared)
