#!/usr/bin/env bash
# Checks that every pinned version has ONE source and that the image pins are current.
#
# Python (no network):
#   - tests/requirements-client.txt pins every line `name==version`;
#   - benchmark/requirements.txt takes those versions as constraints: a package the
#     constraints pin is written bare there (a second == of it could diverge), a package
#     they do not pin carries its own == (a bare one would install unpinned).
# Images (Docker Hub, HEAD requests only: no pull, no rate-limit cost). Every compose file
# names an image as `<repo>:<version>@sha256:<digest>`: Docker pulls the digest, the version
# says which release that digest is. For each image below it fails when
#   - the compose files disagree (another version or digest, or a reference unpinned);
#   - the version tag no longer points at the digest (the tag was re-pushed);
#   - `latest` points elsewhere: a newer release is out and the pin is behind it.
# The fix is a bump in a commit of its own, with a full ./docker-run-tests.sh
# (tests/CLAUDE.md, "Pinned test dependencies").
#
# Needs curl.
set -euo pipefail

IMAGES=(rustfs/rustfs)
CONSTRAINTS=tests/requirements-client.txt
CONSTRAINED=(benchmark/requirements.txt)

cd "$(dirname "$0")/.."

failed=0 image_failed=0
fail() { echo "FAIL: $*" >&2; failed=1; image_failed=1; }

# requirement_lines <file>: "line<TAB>requirement" of every requirement (comments, blank
# lines and -r / -c options left out).
requirement_lines() {
    awk '{ sub(/#.*/, ""); gsub(/^[ \t]+|[ \t]+$/, "") }
         $0 != "" && $0 !~ /^-/ { print NR "\t" $0 }' "$1"
}
# PEP 503 normal form: case and the - _ . separators do not tell two packages apart.
normalize() { tr '[:upper:]' '[:lower:]' <<< "$1" | sed -E 's/[-_.]+/-/g'; }

# ---- Python -------------------------------------------------------------------------
declare -A pinned=()
while IFS=$'\t' read -r line req; do
    if [[ $req =~ ^([A-Za-z0-9][A-Za-z0-9._-]*)==([^=[:space:]]+)$ ]]; then
        name=$(normalize "${BASH_REMATCH[1]}")
        [[ -n ${pinned[$name]:-} ]] && fail "${CONSTRAINTS}:${line}: ${req} pinned twice"
        pinned[$name]=${BASH_REMATCH[2]}
    else
        fail "${CONSTRAINTS}:${line}: ${req} is not name==version"
    fi
done < <(requirement_lines "$CONSTRAINTS")

for file in "${CONSTRAINED[@]}"; do
    grep -qE "^-c[[:space:]]+\.\./${CONSTRAINTS}[[:space:]]*$" "$file" ||
        fail "${file}: no -c ../${CONSTRAINTS}"
    while IFS=$'\t' read -r line req; do
        if [[ $req =~ ^([A-Za-z0-9][A-Za-z0-9._-]*)==[^=[:space:]]+$ ]]; then
            name=$(normalize "${BASH_REMATCH[1]}")
            [[ -n ${pinned[$name]:-} ]] &&
                fail "${file}:${line}: ${req} repeats ${CONSTRAINTS}'s pin (${pinned[$name]}): write the name alone"
        elif [[ $req =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
            [[ -z ${pinned[$(normalize "$req")]:-} ]] &&
                fail "${file}:${line}: ${req} is pinned nowhere: add it to ${CONSTRAINTS} or write ${req}==<version>"
        else
            fail "${file}:${line}: ${req} is neither a bare name nor name==version"
        fi
    done < <(requirement_lines "$file")
done
[[ $failed -eq 0 ]] && echo "ok: python pins have one source (${CONSTRAINTS}, ${#pinned[@]} packages)"

# ---- Images -------------------------------------------------------------------------
mapfile -t COMPOSE_FILES < <(find . -path ./build -prune -o -path ./.git -prune -o \
    -type f \( -name '*compose*.yml' -o -name '*compose*.yaml' \) -print | sed 's#^\./##' | sort)

ACCEPT='application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json'

# remote_digest <repo> <tag>: the digest Docker Hub answers for <repo>:<tag>.
remote_digest() {
    local repo=$1 tag=$2 token
    token=$(curl -fsS "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${repo}:pull" |
        sed -E 's/.*"token":"([^"]+)".*/\1/')
    curl -fsS -I -H "Authorization: Bearer ${token}" -H "Accept: ${ACCEPT}" \
        "https://registry-1.docker.io/v2/${repo}/manifests/${tag}" |
        tr -d '\r' | awk 'tolower($1) == "docker-content-digest:" { print $2 }'
}

for repo in "${IMAGES[@]}"; do
    image_failed=0
    # Every `image:` line of this repo, as "file:line reference".
    mapfile -t refs < <(grep -nE "^\s*image:\s*['\"]?${repo}[:@]" "${COMPOSE_FILES[@]}" |
        sed -E "s/^([^:]+:[0-9]+):\s*image:\s*['\"]?([^'\" ]+).*/\1 \2/")
    if [[ ${#refs[@]} -eq 0 ]]; then
        fail "${repo}: no compose file names it"
        continue
    fi

    pin=""
    for entry in "${refs[@]}"; do
        where=${entry%% *} ref=${entry#* }
        if [[ ! $ref =~ ^${repo}:([^@:]+)@(sha256:[0-9a-f]{64})$ ]]; then
            fail "${where}: ${ref} is not ${repo}:<version>@sha256:<digest>"
            continue
        fi
        if [[ ${BASH_REMATCH[1]} == latest ]]; then
            fail "${where}: name the release the digest is, not latest"
        fi
        if [[ -z $pin ]]; then
            pin=${ref#"${repo}:"} first=$where
        elif [[ ${ref#"${repo}:"} != "$pin" ]]; then
            fail "${where}: ${ref} disagrees with ${first} (${repo}:${pin})"
        fi
    done
    [[ -z $pin ]] && continue

    version=${pin%@*} digest=${pin#*@}
    tag_digest=$(remote_digest "$repo" "$version") || { fail "${repo}:${version}: registry unreachable"; continue; }
    latest_digest=$(remote_digest "$repo" latest) || { fail "${repo}:latest: registry unreachable"; continue; }

    if [[ $tag_digest != "$digest" ]]; then
        fail "${repo}:${version} is now ${tag_digest}, the pin is ${digest} (the tag was re-pushed)"
    fi
    if [[ $latest_digest != "$digest" ]]; then
        fail "${repo}:latest moved: ${latest_digest}, the pin is ${version} (${digest}). Bump the pin in its own commit with a full ./docker-run-tests.sh"
    elif [[ $image_failed -eq 0 ]]; then
        echo "ok: ${repo}:${version} is latest (${digest}) in ${#refs[@]} compose files"
    fi
done

exit "$failed"
