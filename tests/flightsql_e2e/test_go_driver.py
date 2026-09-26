# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""The Go driver (github.com/apache/arrow-go, gRPC-Go stack) as a separate
harness: a third, fully independent client stack against the same server."""

import os
import shutil
import subprocess

import pytest

HARNESS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "flightsql_go")
# Prebuilt by Dockerfile.integration-test (compiling arrow-go from a cold
# module cache can exceed any sane test timeout).
HARNESS_BIN = "/usr/local/bin/flightsql_go"

pytestmark = pytest.mark.skipif(
    shutil.which("go") is None and not os.path.exists(HARNESS_BIN),
    reason="no Go toolchain and no prebuilt harness",
)


def test_go_driver_harness(server_uri):
    addr = server_uri.split("://", 1)[1]
    if os.path.exists(HARNESS_BIN):
        argv = [HARNESS_BIN, addr]
        cwd = None
    else:
        argv = ["go", "run", ".", addr]
        cwd = HARNESS
    result = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        pytest.fail(f"go harness failed:\n{result.stdout}\n{result.stderr}")
    assert "GO E2E OK" in result.stdout
