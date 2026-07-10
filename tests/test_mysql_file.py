# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# MySQL-wire end-to-end test of local-file external tables (CREATE EXTERNAL
# TABLE / COPY ... TO) for all formats (parquet, csv, ndjson). Fixtures are
# mounted into the otterstax container at /fixtures.

import sys

from external_helpers import main

if __name__ == "__main__":
    sys.exit(main("data", "file"))
