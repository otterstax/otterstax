#!/usr/bin/env bash
# Simple upload/download/list helper for the local MinIO test stack.
#
# Uses the minio/mc client inside a throwaway container, so nothing needs to be
# installed on the host beyond Docker. The MinIO stack must already be running:
#
#   docker compose -f tests/minio/docker-compose.yml up -d
#
# Usage:
#   ./s3cmd.sh ls   [prefix]                 # list objects (default: whole bucket)
#   ./s3cmd.sh up   <local-file> [key]       # upload  local -> bucket/key
#   ./s3cmd.sh down <key> [local-file]       # download bucket/key -> local
#   ./s3cmd.sh rm   <key>                     # delete  bucket/key
#
# Examples:
#   ./s3cmd.sh up   fixtures/people.parquet
#   ./s3cmd.sh down people.parquet /tmp/got.parquet
#   ./s3cmd.sh ls

set -euo pipefail

# ── config (override via env) ────────────────────────────────────────────────
ENDPOINT="${MINIO_ENDPOINT:-http://minio1:9000}"
ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
BUCKET="${MINIO_BUCKET:-test-bucket-1}"
NETWORK="${MINIO_NETWORK:-minio_minio_network}"   # <compose-dir>_<network>

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run mc in a container attached to the MinIO network, with $HERE mounted at /work.
mc() {
  docker run --rm --network "$NETWORK" -v "$HERE:/work" -w /work \
    --entrypoint /bin/sh minio/mc:latest -c \
    "mc alias set local $ENDPOINT $ACCESS_KEY $SECRET_KEY >/dev/null && mc $*"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  ls)
    mc "ls --recursive local/$BUCKET/${1:-}"
    ;;
  up)
    [ $# -ge 1 ] || { echo "usage: $0 up <local-file> [key]" >&2; exit 1; }
    local_file="$1"; key="${2:-$(basename "$1")}"
    mc "cp /work/$local_file local/$BUCKET/$key"
    echo "uploaded $local_file -> $BUCKET/$key"
    ;;
  down)
    [ $# -ge 1 ] || { echo "usage: $0 down <key> [local-file]" >&2; exit 1; }
    key="$1"; out="${2:-$(basename "$1")}"
    mc "cp local/$BUCKET/$key /work/$out"
    echo "downloaded $BUCKET/$key -> $out"
    ;;
  rm)
    [ $# -ge 1 ] || { echo "usage: $0 rm <key>" >&2; exit 1; }
    mc "rm local/$BUCKET/$1"
    echo "removed $BUCKET/$1"
    ;;
  *)
    echo "usage: $0 {ls|up|down|rm} ..." >&2
    echo "  ls   [prefix]            list objects" >&2
    echo "  up   <file> [key]        upload" >&2
    echo "  down <key>  [file]       download" >&2
    echo "  rm   <key>               delete" >&2
    exit 1
    ;;
esac
