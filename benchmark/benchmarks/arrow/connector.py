# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
from flightsql import FlightSQLClient

FRONTEND = "arrow"
DEFAULT_PORT = 8815


def make_fetch_factory(host, port):
    def make_fetch(sql):
        def fetch():
            client = FlightSQLClient(host=host, port=port, insecure=True)
            info = client.execute(sql)
            reader = client.do_get(info.endpoints[0].ticket)
            return reader.read_all().to_pylist()

        return fetch

    return make_fetch


def connect(host, port):
    """External-table benchmarks (external_load / external_join / external_dump)
    drive CREATE EXTERNAL TABLE / COPY ... TO, which the s3/file test suite only
    exercises over the MySQL/PostgreSQL wire.  The FlightSQL DDL path is not
    covered, so there is no arrow external_* script (run_benchmark.sh skips the
    combination); fail fast should one call this anyway."""
    raise NotImplementedError(
        "external-table benchmarks support the mysql and postgres frontends only"
    )
