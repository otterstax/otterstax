# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# MySQL-wire schema test for local-file external tables: verifies the column
# schema (id, name) is exposed correctly for all formats (parquet, csv, ndjson).

import sys

from external_helpers import main

if __name__ == "__main__":
    sys.exit(main("schema", "file"))
