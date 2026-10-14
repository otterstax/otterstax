#!/usr/bin/env bash

set -eo pipefail


python3 scripts/tools/code-fromat.py ${1} \
backends \
catalog \
config \
connectors \
examples \
frontend \
integration \
otterbrix \
runtime \
session \
tests \
types \
utility \
main.cpp