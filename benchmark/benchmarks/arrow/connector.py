# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
# TODO: OtterStax FlightSQL serialization crashes ("Array length did not match
# record batch length") whenever a JOIN result set exceeds ~1000 rows.  This
# makes all JOIN tests via the arrow frontend fail on rep 1, then OtterStax
# stays down for reps 2-N ("Connection refused").  Fix the Arrow IPC batch
# serializer in the C++ FlightSQL handler before re-enabling the arrow frontend
# for join_same_instance, join_cross_engine, and join_all.
# Arrow is excluded from the default frontend list in run_benchmark.sh until
# this bug is resolved.
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
    covered, so fail fast instead of silently producing misleading numbers."""
    raise NotImplementedError(
        "external-table benchmarks support the mysql and postgres frontends only"
    )
