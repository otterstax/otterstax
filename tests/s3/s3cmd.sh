#!/usr/bin/env bash
# Simple upload/download/list helper for the local RustFS test stack.
#
# Uses the aws-cli client inside a throwaway container, so nothing needs to be
# installed on the host beyond Docker. The RustFS stack must already be running:
#
#   docker compose -f tests/s3/docker-compose.yml up -d
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
ENDPOINT="${S3_ENDPOINT:-http://rustfs1:9000}"
ACCESS_KEY="${S3_ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${S3_SECRET_KEY:-rustfsadmin}"
BUCKET="${S3_BUCKET:-test-bucket-1}"
NETWORK="${S3_NETWORK:-s3_rustfs_network}"   # <compose-dir>_<network>

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run aws-cli in a container attached to the RustFS network, with $HERE mounted at /work.
s3() {
  docker run --rm --network "$NETWORK" -v "$HERE:/work" -w /work \
    -e AWS_ACCESS_KEY_ID="$ACCESS_KEY" -e AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    -e AWS_DEFAULT_REGION=us-east-1 \
    amazon/aws-cli:latest --endpoint-url "$ENDPOINT" s3 "$@"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  ls)
    s3 ls --recursive "s3://$BUCKET/${1:-}"
    ;;
  up)
    [ $# -ge 1 ] || { echo "usage: $0 up <local-file> [key]" >&2; exit 1; }
    local_file="$1"; key="${2:-$(basename "$1")}"
    s3 cp "/work/$local_file" "s3://$BUCKET/$key"
    echo "uploaded $local_file -> $BUCKET/$key"
    ;;
  down)
    [ $# -ge 1 ] || { echo "usage: $0 down <key> [local-file]" >&2; exit 1; }
    key="$1"; out="${2:-$(basename "$1")}"
    s3 cp "s3://$BUCKET/$key" "/work/$out"
    echo "downloaded $BUCKET/$key -> $out"
    ;;
  rm)
    [ $# -ge 1 ] || { echo "usage: $0 rm <key>" >&2; exit 1; }
    s3 rm "s3://$BUCKET/$1"
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
