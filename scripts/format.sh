#!/usr/bin/env bash

set -eo pipefail


python3 scripts/tools/code-fromat.py ${1} \
catalog \
connectors \
examples \
frontend \
integration \
otterbrix \
scheduler \
tests \
types \
utility \
main.cpp