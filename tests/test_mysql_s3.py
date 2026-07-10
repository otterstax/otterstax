# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# MySQL-wire end-to-end test of s3 external tables (CREATE EXTERNAL TABLE /
# COPY ... TO) for all formats (parquet, csv, ndjson), against a seeded MinIO.
# Registers MinIO credentials through the HTTP API before running.

import sys

from external_helpers import main

if __name__ == "__main__":
    sys.exit(main("data", "s3"))
