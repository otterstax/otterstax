# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
import psycopg2

FRONTEND = "postgres"
DEFAULT_PORT = 8817


def make_fetch_factory(host, port):
    def make_fetch(sql):
        def fetch():
            conn = psycopg2.connect(
                host=host, port=port, user="testuser", password="testpass", dbname="default"
            )
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()
            return rows

        return fetch

    return make_fetch


def connect(host, port):
    """Persistent autocommit connection for the external-table benchmarks
    (external_load / external_join / external_dump run one statement per call)."""
    conn = psycopg2.connect(
        host=host, port=port, user="testuser", password="testpass", dbname="default"
    )
    conn.autocommit = True
    return conn


# ── the prepare test (benchmarks/backends/suite.py, <backend>_prepare) ────────
# psycopg2 has no protocol-level prepare, so both modes run on psycopg 3: plain is
# the extended protocol's unnamed statement, prepared a named Parse first.
def prepare_connect(host, port):
    import psycopg

    return psycopg.connect(host=host, port=port, user="testuser", password="testpass",
                           dbname="default", autocommit=True)


class _StatementCursor:
    def __init__(self, conn, prepared):
        self._cur = conn.cursor()
        self._prepared = prepared

    def execute(self, sql):
        self._cur.execute(sql, prepare=self._prepared)

    def fetchall(self):
        return self._cur.fetchall()

    def close(self):
        self._cur.close()


def statement_cursor(conn, prepared):
    return _StatementCursor(conn, prepared)
