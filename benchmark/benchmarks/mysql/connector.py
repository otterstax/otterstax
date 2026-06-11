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
