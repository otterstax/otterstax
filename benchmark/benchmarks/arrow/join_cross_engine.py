#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import benchmark_main
from queries import JOIN_CROSS_ENGINE
from connector import FRONTEND, DEFAULT_PORT, make_fetch_factory

if __name__ == "__main__":
    benchmark_main("join_cross_engine", FRONTEND, DEFAULT_PORT, JOIN_CROSS_ENGINE, make_fetch_factory)
