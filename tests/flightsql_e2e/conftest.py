# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""e2e suite of the custom Flight SQL frontend: the ORIGINAL Apache drivers
(pyarrow.flight C++/gRPC C-core, adbc-driver-flightsql Go/gRPC-Go, the Go
database/sql driver) against the live otterstax server — the same instance the
rest of tests/ drives. The server is expected to be up already (docker-run-
tests.sh or a manual stack); the fixtures only seed their own database.

The python stubs of the Flight/FlightSql protobuf messages are (re)generated
from the very .proto files the server compiles in (frontend/flight_sql/format)
so the wire contract under test is the one shipped.
"""

import os
import socket
import subprocess
import sys
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FORMAT_DIR = os.path.join(ROOT, "frontend", "flight_sql", "format")

sys.path.insert(0, os.path.dirname(HERE))
import config  # noqa: E402


def wait_listening(host, port, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"otterstax FlightSQL is not listening on {host}:{port}")


@pytest.fixture(scope="session")
def server_uri(request):
    local = request.config.getoption("--local", default=False)
    host = config.get_host(local)
    wait_listening(host, config.FLIGHT_PORT)
    yield f"grpc+tcp://{host}:{config.FLIGHT_PORT}"


@pytest.fixture(scope="session")
def gen_path():
    """Path to the python Flight/FlightSql protobuf stubs."""
    path = os.path.join(HERE, "gen")
    if not os.path.exists(os.path.join(path, "Flight_pb2.py")):
        os.makedirs(path, exist_ok=True)
        subprocess.check_call(
            [sys.executable, "-m", "grpc_tools.protoc",
             f"-I{FORMAT_DIR}",
             f"--python_out={path}", f"--grpc_python_out={path}",
             os.path.join(FORMAT_DIR, "Flight.proto"),
             os.path.join(FORMAT_DIR, "FlightSql.proto")])
    return path
