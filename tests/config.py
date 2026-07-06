# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Shared configuration for integration tests."""

HOST_DOCKER = "test-otterstax"
HOST_LOCAL = "0.0.0.0"

MYSQL_PORT = 8816
PG_PORT = 8817
FLIGHT_PORT = 8815
HTTP_PORT = 8085
SPARK_CONNECT_PORT = 15002

# ClickHouse backend
CH_ALIAS = "chtest"
CH_DATABASE = "chdb"
CH_DOCKER_HOST = "clickhouse1"
CH_PORT = 9000


def get_host(local: bool) -> str:
    return HOST_LOCAL if local else HOST_DOCKER
