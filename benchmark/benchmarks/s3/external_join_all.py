#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import frontend_from_argv
from external_common import external_main

if __name__ == "__main__":
    fe = frontend_from_argv()
    external_main("external_join_all", fe.FRONTEND, fe.DEFAULT_PORT, fe.connect)
