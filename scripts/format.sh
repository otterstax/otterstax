#!/usr/bin/env bash

set -eo pipefail


python3 scripts/tools/code-fromat.py ${1} \
catalog \
client_example \
connectors \
db_integration \
frontend \
otterbrix \
routes \
scheduler \
tests \
types \
utility \
main.cpp