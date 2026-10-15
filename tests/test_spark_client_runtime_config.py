# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 OtterStax

"""Check effective configuration and mutation refusal through real PySpark clients."""

import argparse
import config
from pyspark.sql import SparkSession


def require_unsupported(operation):
    try:
        operation()
    except Exception as error:
        assert "Runtime configuration changes are not supported" in str(error), str(error)
    else:
        raise AssertionError("unsupported configuration mutation reported success")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()
    url = f"sc://{config.get_host(args.local)}:{config.SPARK_CONNECT_PORT}"
    # Session builders request best-effort SET. This must leave the effective
    # timezone unchanged and permit subsequent queries on every client version.
    spark = SparkSession.builder.remote(url).config("spark.sql.session.timeZone", "Europe/Paris").getOrCreate()
    try:
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        assert spark.conf.get("spark.sql.session.timeZone", "Europe/Paris") == "UTC"
        assert spark.conf.get("otterstax.unconfigured.key", "fallback") == "fallback"
        assert not spark.conf.isModifiable("spark.sql.session.timeZone")
        assert not spark.conf.isModifiable("otterstax.unconfigured.key")
        require_unsupported(lambda: spark.conf.set("spark.sql.session.timeZone", "Europe/Paris"))
        require_unsupported(lambda: spark.conf.unset("spark.sql.session.timeZone"))
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        assert spark.sql("SELECT 1").collect()[0][0] == 1
    finally:
        spark.stop()
    print("PASS: effective configuration, explicit mutation errors, and session builder")


if __name__ == "__main__":
    main()
