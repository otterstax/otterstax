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
