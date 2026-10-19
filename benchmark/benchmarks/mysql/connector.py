# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
import mysql.connector as _mysql

FRONTEND = "mysql"
DEFAULT_PORT = 8816


def make_fetch_factory(host, port):
    def make_fetch(sql):
        def fetch():
            conn = _mysql.connect(host=host, port=port, user="testuser", password="testpass")
            cur = conn.cursor(buffered=True)
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()
            return rows

        return fetch

    return make_fetch


def connect(host, port):
    """Persistent autocommit connection for the external-table benchmarks
    (external_load / external_join / external_dump run one statement per call)."""
    return _mysql.connect(host=host, port=port, user="testuser",
                          password="testpass", autocommit=True)


# ── the prepare test (benchmarks/backends/suite.py, <backend>_prepare) ────────
def prepare_connect(host, port):
    return connect(host, port)


def statement_cursor(conn, prepared):
    """A cursor that runs one statement plain (COM_QUERY) or prepared
    (COM_STMT_PREPARE + COM_STMT_EXECUTE)."""
    return conn.cursor(prepared=True) if prepared else conn.cursor()
