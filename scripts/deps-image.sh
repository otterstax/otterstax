#!/bin/bash
# The dependency image of a build variant (Dockerfile.deps): makes sure it is in
# the local Docker image store and prints its reference, the value of the
# DEPS_IMAGE build argument of Dockerfile / Dockerfile.test.
#
#   scripts/deps-image.sh [release|asan|tsan]   pull it, or build it when the
#                                               registry has none for this
#                                               platform; print the local ref
#   scripts/deps-image.sh --tag                 print the tag only
#   scripts/deps-image.sh --ref <variant>       print the registry reference
#                                               (ghcr.io/...:<tag>), nothing else
#
# The tag is a hash of the files the image is made of, so a change of
# conanfile.py or of a profile names a new image and an unchanged tree finds the
# one already published. The image is also tagged <name>:local, the default
# DEPS_IMAGE of both Dockerfiles, so a bare `docker build` / `docker compose
# build` works after one run of this script.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REGISTRY="ghcr.io/otterstax"
INPUTS=(Dockerfile.deps conanfile.py asan-deps.profile tsan-deps.profile
        cmake/conan-hooks/hook_librdkafka_c11threads_off.py)

_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi
}

deps_tag() {
    (cd "$ROOT" && cat "${INPUTS[@]}") | _sha256 | cut -c1-12
}

image_name() {
    case "$1" in
        release) echo "otterstax-deps" ;;
        asan)    echo "otterstax-deps-asan" ;;
        tsan)    echo "otterstax-deps-tsan" ;;
        *) echo "unknown variant '$1' (release, asan, tsan)" >&2; exit 2 ;;
    esac
}

case "${1:-release}" in
    --tag)
        deps_tag
        exit 0
        ;;
    --ref)
        echo "${REGISTRY}/$(image_name "${2:-release}"):$(deps_tag)"
        exit 0
        ;;
esac

VARIANT="${1:-release}"
NAME="$(image_name "$VARIANT")"
TAG="$(deps_tag)"
LOCAL="${NAME}:${TAG}"
REMOTE="${REGISTRY}/${LOCAL}"

# Everything but the reference goes to stderr: the caller reads stdout.
ensure() {
    local variant="$1" name local_ref remote_ref
    name="$(image_name "$variant")"
    local_ref="${name}:${TAG}"
    remote_ref="${REGISTRY}/${local_ref}"
    if docker image inspect "$local_ref" >/dev/null 2>&1; then
        return 0
    fi
    # The registry holds linux/amd64 only: on another platform a pull would
    # hand back an emulated image, so the image is built here instead.
    if [ "$(docker version --format '{{.Server.Arch}}')" = "amd64" ] &&
        docker pull "$remote_ref" >&2 2>/dev/null; then
        docker tag "$remote_ref" "$local_ref"
        return 0
    fi
    echo "building ${local_ref} (Dockerfile.deps, target ${variant})" >&2
    if [ "$variant" = "release" ]; then
        docker build -f "$ROOT/Dockerfile.deps" --target release -t "$local_ref" "$ROOT" >&2
    else
        ensure release
        docker build -f "$ROOT/Dockerfile.deps" --target "$variant" \
            --build-arg "RELEASE_IMAGE=otterstax-deps:${TAG}" -t "$local_ref" "$ROOT" >&2
    fi
}

ensure "$VARIANT"
docker tag "$LOCAL" "${NAME}:local"
echo "$LOCAL"
