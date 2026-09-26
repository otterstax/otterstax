#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# Installs the PySpark Connect clients named as arguments (e.g. 3.5.0 4.2.0)
# side by side for run_spark_matrix.sh (image: Dockerfile.spark-test):
#
#   /opt/pyspark/<version>/     the client alone (pip --target --no-deps):
#                               3.5.x `pyspark` without its jars, 4.x `pyspark-client`
#   /opt/pyspark/venv-<set>/    the dependencies: venv-3.5, venv-4.0, venv-4.1-4.2
#   /opt/pyspark/manifest       "<version> <python>" per installed client
#
# A client runs as: PYTHONPATH=/opt/pyspark/<version> <python> script.py
# Every client is import-checked here, so a broken install fails the build.

set -euo pipefail

ROOT=/opt/pyspark
export PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore

# The dependency venv a client version runs in. 4.1 and 4.2 declare the same
# requirements (bar the pyarrow floor), so they share one.
venv_of() {
    case "$1" in
        3.5.*) echo 3.5 ;;
        4.0.*) echo 4.0 ;;
        4.1.* | 4.2.*) echo 4.1-4.2 ;;
        *) echo "no dependency set for PySpark $1" >&2; return 1 ;;
    esac
}

# Each set is the line's declared Connect requirements, plus two caps:
#  - pandas<3: every line caps it in its own dev/requirements.txt;
#  - protobuf on the major the line's *_pb2.py was generated for (3.5: protoc
#    21.7 = python 4.21, 4.0: 5.28, 4.1/4.2: 6.33), the only pairing protobuf's
#    cross-version guarantee covers and what the 4.x CI pins; unpinned, pip
#    picks 7.x for every line.
create_venv() {
    local name=$1 python
    local -a deps
    case "$name" in
        3.5)     # pyspark 3.5 supports Python 3.8-3.11 and numpy<2 only
                 python=python3.11
                 deps=(py4j==0.10.9.7 "numpy>=1.15,<2" "pandas>=1.0.5,<3" "pyarrow>=4.0.0"
                       "grpcio>=1.56.0" "grpcio-status>=1.56.0"
                       "googleapis-common-protos>=1.56.4" "protobuf<5") ;;
        4.0)     python=python3.12
                 deps=("numpy>=1.21" "pandas>=2.0.0,<3" "pyarrow>=11.0.0"
                       "grpcio>=1.67.0" "grpcio-status>=1.67.0"
                       "googleapis-common-protos>=1.65.0" "protobuf>=5.28.3,<6") ;;
        4.1-4.2) python=python3.12
                 deps=("numpy>=1.21" "pandas>=2.2.0,<3" "pyarrow>=18.0.0"
                       "grpcio>=1.76.0" "grpcio-status>=1.76.0"
                       "googleapis-common-protos>=1.71.0" "zstandard>=0.25.0"
                       "pyyaml>=3.11" "protobuf>=6.33.5,<7") ;;
    esac
    echo "=== venv-$name ($python): ${deps[*]}"
    "$python" -m venv "$ROOT/venv-$name"
    "$ROOT/venv-$name/bin/pip" install "${deps[@]}"
}

mkdir -p "$ROOT"
: > "$ROOT/manifest"

for version in "$@"; do
    venv=$(venv_of "$version")
    python="$ROOT/venv-$venv/bin/python"
    target="$ROOT/$version"
    [ -x "$python" ] || create_venv "$venv"

    echo "=== PySpark $version -> $target"
    case "$version" in
        3.5.*)
            "$python" -m pip install --no-deps --target "$target" "pyspark==$version"
            # The JVM side (jars, python/lib worker zips) and sample content are
            # never loaded by the Connect client; jars alone are ~320 MB.
            rm -rf "$target/pyspark/jars" "$target/pyspark/python/lib" \
                   "$target/pyspark/examples" "$target/pyspark/data" ;;
        *)
            "$python" -m pip install --no-deps --target "$target" "pyspark-client==$version" ;;
    esac

    # On stdin, not `python -c`: pyspark-client 4.2.0 takes a main module without
    # __file__ for a doctest run and exits looking for a SPARK_HOME.
    PYTHONPATH="$target" "$python" - <<EOF
import pyspark, pyspark.sql.connect.session
assert pyspark.__version__ == "$version", "installed pyspark " + pyspark.__version__
EOF
    echo "$version $python" >> "$ROOT/manifest"
    echo "=== PySpark $version OK ($python)"
done

echo "=== manifest"
cat "$ROOT/manifest"
