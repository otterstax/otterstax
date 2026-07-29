# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Shared configuration for integration tests."""

HOST_DOCKER = "test-otterstax"
HOST_LOCAL = "127.0.0.1"

MYSQL_PORT = 8816
PG_PORT = 8817
FLIGHT_PORT = 8815

# ClickHouse backend
CH_ALIAS = "chtest"
CH_DATABASE = "chdb"
CH_DOCKER_HOST = "clickhouse1"
CH_PORT = 9000

# Kafka broker (redpanda). In-network clients use the internal listener;
# host-run clients (--local) use the published external listener.
# Use the IPv4 literal (not "localhost") for the host listener: librdkafka
# prefers IPv6 ::1, but docker's IPv6 port-proxy black-holes it (accepts the
# SYN, never forwards to the IPv4-only container) — must match the broker's
# external advertised addr (127.0.0.1) in compose.test.yml.
KAFKA_BROKER_DOCKER = "kafka:9092"
KAFKA_BROKER_LOCAL = "127.0.0.1:19092"


def get_host(local: bool) -> str:
    return HOST_LOCAL if local else HOST_DOCKER


def get_kafka_broker(local: bool) -> str:
    return KAFKA_BROKER_LOCAL if local else KAFKA_BROKER_DOCKER
