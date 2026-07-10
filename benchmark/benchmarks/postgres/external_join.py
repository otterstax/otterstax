#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from external_common import external_main
from connector import FRONTEND, DEFAULT_PORT, connect

if __name__ == "__main__":
    external_main("external_join", FRONTEND, DEFAULT_PORT, connect)
