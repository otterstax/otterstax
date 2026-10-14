#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# TEMPORARY workaround — remove once otterbrix cuts a release tag from
# feat/otterstax-embedding and the package is on https://conan.otterbrix.com
# (OTB-tag, docs/proposals/otterbrix-embedding.md).
#
# conanfile.py pins otterbrix/1.0.0b2-embedding.<sha>, which exists only in
# the developer's LOCAL Conan cache, so a Docker image build cannot fetch it.
# This script archives its RECIPE and SOURCES (no binaries: a macOS binary is
# useless in a Linux image) with `conan cache save` into the build context:
#
#   .conan-local/local_recipes.tgz    the archive (git-ignored)
#   .conan-local/local_recipes.refs   the exact ref#revision it holds
#
# Dockerfile / Dockerfile.test `conan cache restore` the archive when it is
# present and build otterbrix from source (--build="otterbrix/*"). With no
# archive they fall back to the remotes. actor-zeta/2.0.0 is NOT archived: it
# is on the conan remote (conan.otterbrix.com) like any other dependency.
#
# The ref is read from conanfile.py (the otterbrix pin including its recipe
# revision; the latest revision in the local cache if it is not pinned). The
# archive is rewritten only when that ref changes (or with --force): `conan
# cache save` is not byte-reproducible, and a new archive would invalidate the
# Docker layer cache and rebuild otterbrix from source.
#
# A ref that is not in the local cache is a WARNING (the image then resolves it
# from the remotes, as it will after OTB-tag); the script exits 1 only on real
# errors: no conan and no archive, or a failed save.
#
# Usage: scripts/conan/export_local_packages.sh [--force]
# Env:   CONAN (default: conan), CONAN_LOCAL_DIR (default: <repo>/.conan-local)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${CONAN_LOCAL_DIR:-$REPO_ROOT/.conan-local}"
ARCHIVE="$OUT_DIR/local_recipes.tgz"
STAMP="$OUT_DIR/local_recipes.refs"
CONAN="${CONAN:-conan}"
FORCE=false

for arg in "$@"; do
    case "$arg" in
        --force) FORCE=true ;;
        -h|--help) sed -n '5,32p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "export_local_packages: unknown option: $arg" >&2; exit 2 ;;
    esac
done

die() { echo "export_local_packages: ERROR: $*" >&2; exit 1; }
note() { echo "export_local_packages: $*"; }

# self.requires("otterbrix/<version>#<rrev>")
OTTERBRIX_REF="$(sed -nE 's/.*requires\("(otterbrix\/[^"]+)".*/\1/p' "$REPO_ROOT/conanfile.py" | head -1)"
[ -n "$OTTERBRIX_REF" ] || die "no otterbrix requirement found in conanfile.py"
# no pinned revision → take the latest one in the local cache
[[ "$OTTERBRIX_REF" == *"#"* ]] || OTTERBRIX_REF="${OTTERBRIX_REF}#latest"

_build_hint() {
    local version="${OTTERBRIX_REF#otterbrix/}"; version="${version%%#*}"
    cat >&2 <<EOF

  To build the local package (see docs/proposals/otterbrix-embedding.md):
    cd <otterbrix checkout> && git checkout ${version##*.}
    conan create . --version ${version} -pr:h profiles/release -pr:b default -o "otterbrix/*:shared=True"
  (its actor-zeta/2.0.0 comes from the conan remote.) Then re-run:
    scripts/conan/export_local_packages.sh
EOF
}

if ! command -v "$CONAN" >/dev/null 2>&1; then
    if [ -f "$ARCHIVE" ]; then
        note "'$CONAN' not found on the host; reusing the existing $ARCHIVE ($(tr '\n' ' ' < "$STAMP" 2>/dev/null))"
        exit 0
    fi
    die "'$CONAN' not found on the host and $ARCHIVE does not exist — the Docker image cannot get ${OTTERBRIX_REF%%#*} (it is not on the conan remote)."
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Resolve each pattern against the local cache; print "name/version#rrev" or nothing.
_resolve() {
    "$CONAN" list "$1" --format=json > "$2" 2>/dev/null || true
    python3 -I - "$2" <<'PY'
import json, sys
try:
    data = json.load(open(sys.argv[1])).get("Local Cache", {})
except Exception:
    sys.exit(0)
for ref, info in data.items():
    if isinstance(info, dict):
        for rrev in info.get("revisions", {}):
            print(f"{ref}#{rrev}")
PY
}

OTB_RESOLVED="$(_resolve "$OTTERBRIX_REF" "$TMP/otterbrix.json")"

# A ref that is not local is left to the remotes (that is the normal state once
# OTB-tag lands), so it is a warning, never a failure — exit 1 is kept for real
# errors (no conan, save failed).
NEW_REFS="$OTB_RESOLVED"
if [ -z "$NEW_REFS" ]; then
    echo "export_local_packages: WARNING: $OTTERBRIX_REF is not in the local Conan cache ($("$CONAN" config home)); the image build will try the conan remotes for it." >&2
    if [ -f "$ARCHIVE" ]; then
        note "WARNING: reusing the existing $ARCHIVE ($(tr '\n' ' ' < "$STAMP" 2>/dev/null))"
    else
        echo "export_local_packages: WARNING: no archive written; if the package is not on a remote yet, the image build will fail resolving it." >&2
        _build_hint
    fi
    exit 0
fi

if ! $FORCE && [ -f "$ARCHIVE" ] && [ -f "$STAMP" ] && [ "$(cat "$STAMP")" = "$NEW_REFS" ]; then
    note "up to date: $ARCHIVE ($(echo "$NEW_REFS" | tr '\n' ' '))"
    exit 0
fi

note "saving recipe + sources of: $NEW_REFS"
# Recipe-level package list (no ':*') → recipe export + export_sources + the
# source folder; binaries are left out on purpose.
"$CONAN" cache save -l "$TMP/otterbrix.json" --file "$TMP/local_recipes.tgz" >/dev/null \
    || die "conan cache save failed"

mkdir -p "$OUT_DIR"
mv -f "$TMP/local_recipes.tgz" "$ARCHIVE"
printf '%s\n' "$NEW_REFS" > "$STAMP"
note "wrote $ARCHIVE ($(du -h "$ARCHIVE" | cut -f1))"
