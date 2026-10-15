# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# MySQL-wire end-to-end test of the s3 streaming contracts against the seeded
# RustFS: WITH (format = ...) names the format of an extension-less object key in
# both directions, and a COPY whose query fails publishes no object.

import sys

from external_helpers import main

if __name__ == "__main__":
    sys.exit(main("stream", "s3"))
