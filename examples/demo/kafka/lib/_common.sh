#!/usr/bin/env bash
# Shared helpers for the Kafka streaming act (examples/demo/kafka/).
#
# Sourced by every step's run.sh and every *.sh sub-step. Provides:
#   title <text>                     — section banner
#   pause                            — wait for [Enter] (skipped when NONINTERACTIVE=1)
#   require_server                   — fail unless the otterstax PG wire answers SELECT 1
#   psql_run <file.sql>              — run a demo SQL file over the PG wire, passing
#                                      -v broker=<SQL_BROKER> for CREATE SOURCE/STREAM
#   wait_rows <table_expr> <n> [s]   — poll SELECT count(*) FROM <table_expr> until >= n
#   seed  --topic T [--fixture F] [--reset]   — (re)create a topic + produce a fixture
#   consume --topic T [--timeout S]           — drain a topic to stdout
#
# Every helper that can fail prints a red ❌ line on stderr and returns non-zero.
# A step's run.sh stops on the first failure (`helper … || exit $?`) and
# run_all.sh stops on the first failed step, so the exit code is honest.
#
# seed/consume shell out to `rpk` inside the demo-kafka (redpanda) container, so
# there is no host-side Kafka client dependency. The SQL broker address embedded
# in CREATE SOURCE/STREAM differs by mode:
#   docker (default) : demo-kafka:9092    (otterstax runs in-network)
#   --local          : 127.0.0.1:19093    (otterstax runs as a host binary)

set -uo pipefail

# ── paths ────────────────────────────────────────────────────────────────────
KAFKA_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_ROOT="$(cd "${KAFKA_LIB_DIR}/../.." && pwd)"   # examples/demo
FIXTURE_DIR="${DEMO_ROOT}/init/kafka"

# ── mode / broker ────────────────────────────────────────────────────────────
LOCAL=0
for _a in "$@"; do [ "${_a}" = "--local" ] && LOCAL=1; done
if [ "${LOCAL}" -eq 1 ]; then
    SQL_BROKER="127.0.0.1:19093"
else
    SQL_BROKER="demo-kafka:9092"
fi

KAFKA_CONTAINER="${KAFKA_CONTAINER:-demo-kafka}"

# ── PG wire (host-published in both modes) ───────────────────────────────────
PGHOST_="${PGHOST_:-localhost}"
PGPORT_="${PGPORT_:-8817}"
PGUSER_="${PGUSER_:-demo}"
PGDB_="${PGDB_:-demo}"
export PGPASSWORD="${PGPASSWORD:-demo}"
# A host that drops the connection attempt fails after this many seconds
# instead of hanging the step.
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-5}"

# ── colours ──────────────────────────────────────────────────────────────────
if [ -t 1 ]; then
    BOLD=$'\033[1m'; GREEN=$'\033[92m'; RED=$'\033[91m'; CYAN=$'\033[96m'; DIM=$'\033[2m'; RESET=$'\033[0m'
else
    BOLD=""; GREEN=""; RED=""; CYAN=""; DIM=""; RESET=""
fi

# ── docker (auto sudo -n, mirroring up.sh) ───────────────────────────────────
_DOCKER="docker"
if ! docker ps >/dev/null 2>&1; then
    if sudo -n docker ps >/dev/null 2>&1; then _DOCKER="sudo -n docker"; fi
fi
_rpk() { ${_DOCKER} exec "${KAFKA_CONTAINER}" rpk "$@"; }
_rpk_i() { ${_DOCKER} exec -i "${KAFKA_CONTAINER}" rpk "$@"; }

# ── ui helpers ───────────────────────────────────────────────────────────────
title() {
    echo
    echo "${BOLD}${GREEN}━━ $* ━━${RESET}"
}

pause() {
    [ "${NONINTERACTIVE:-0}" = "1" ] && return 0
    printf "%s" "${DIM}    [Enter] to continue…${RESET}"
    read -r _ || true
}

# fail <message> — the ❌ line of a failed helper (stderr); the caller returns non-zero.
fail() {
    echo "${BOLD}${RED}    ❌ $*${RESET}" >&2
}

# _indent <text> — echo a tool's own output under a ❌ line (stderr).
_indent() {
    [ -n "$1" ] && printf '%s\n' "$1" | sed 's/^/       /' >&2
    return 0
}

# _with_timeout <seconds> <cmd…> — GNU timeout semantics (exit 124 when the limit
# is hit, the command's own code otherwise) without requiring coreutils, which
# macOS does not ship.
_with_timeout() {
    local secs="$1"; shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "${secs}" "$@"
        return $?
    fi
    local flag pid watcher rc
    flag="$(mktemp -u)"
    "$@" &
    pid=$!
    # The watcher's output goes to /dev/null so its orphaned sleep never holds
    # the caller's pipe open.
    ( sleep "${secs}"; : > "${flag}"; kill -TERM "${pid}" 2>/dev/null ) >/dev/null 2>&1 &
    watcher=$!
    wait "${pid}"
    rc=$?
    kill "${watcher}" 2>/dev/null
    wait "${watcher}" 2>/dev/null
    if [ -e "${flag}" ]; then
        rc=124
        rm -f "${flag}"
    fi
    return "${rc}"
}

# ── SQL ──────────────────────────────────────────────────────────────────────
# require_server — returns non-zero (with the reason) unless the otterstax PG wire
# answers, so a missing server is reported once, before any step touches Kafka.
require_server() {
    if ! command -v psql >/dev/null 2>&1; then
        fail "psql client not found on PATH (the Kafka act drives otterstax through it)"
        return 1
    fi
    local out rc
    out="$(psql -X -h "${PGHOST_}" -p "${PGPORT_}" -U "${PGUSER_}" -tAc 'SELECT 1;' "${PGDB_}" 2>&1)"
    rc=$?
    [ "${rc}" -eq 0 ] && return 0
    fail "otterstax PG wire not reachable at ${PGHOST_}:${PGPORT_} (psql exit ${rc})"
    _indent "${out}"
    echo "       start the server first (examples/demo/up.sh, or with --local:" \
         "./build/server --config examples/demo/config_local.yaml)" >&2
    return 1
}

# psql_run <file.sql> — resolved relative to the caller's cwd (the step folder).
# Returns psql's exit code: 0 ok, 1 client error, 2 lost connection, 3 a failed
# statement (ON_ERROR_STOP=1 stops the file there).
psql_run() {
    local file="$1" rc
    echo "${CYAN}    psql ← ${file}   (broker=${SQL_BROKER})${RESET}"
    psql -h "${PGHOST_}" -p "${PGPORT_}" -U "${PGUSER_}" \
         -v ON_ERROR_STOP=1 -v "broker=${SQL_BROKER}" \
         -f "${file}" "${PGDB_}"
    rc=$?
    [ "${rc}" -eq 0 ] || fail "psql ${file} failed (exit ${rc})"
    return "${rc}"
}

# wait_rows <table_expr> <count> [timeout_s]
# Polls SELECT count(*) FROM <table_expr> (a full FROM clause, may include WHERE)
# until it reaches <count>. Returns 1 when the timeout elapses (printing the
# expected and the last observed count, and the last query error if any), and 2
# at once when the PG wire stops accepting connections.
wait_rows() {
    local expr="$1" want="$2" timeout_s="${3:-30}"
    local deadline=$(( $(date +%s) + timeout_s ))
    local got="" err="" out rc
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        out="$(psql -X -h "${PGHOST_}" -p "${PGPORT_}" -U "${PGUSER_}" -tAc \
               "SELECT count(*) FROM ${expr};" "${PGDB_}" 2>&1)"
        rc=$?
        if [ "${rc}" -eq 0 ]; then
            got="$(printf '%s' "${out}" | tr -d '[:space:]')"
            err=""
            if [ "${got}" -ge "${want}" ] 2>/dev/null; then
                echo "${DIM}    ${expr} → ${got} rows (≥ ${want}) ✅${RESET}"
                return 0
            fi
        elif [ "${rc}" -eq 2 ]; then
            fail "wait_rows ${expr}: lost the PG wire at ${PGHOST_}:${PGPORT_}"
            _indent "${out}"
            return 2
        else
            err="${out}"
        fi
        sleep 1
    done
    fail "wait_rows ${expr}: expected ≥ ${want} rows within ${timeout_s}s, got ${got:-no count}"
    [ -n "${err}" ] && { echo "       last query error:" >&2; _indent "${err}"; }
    return 1
}

# ── Kafka (rpk inside the redpanda container) ────────────────────────────────
# seed --topic <T> [--fixture <F.ndjson>] [--reset]
# Any rpk failure (no container, broker down, produce error) is reported with
# rpk's own output and returns 1. Tolerated: --reset deleting a topic that does
# not exist (rpk exits 0 and reports UNKNOWN_TOPIC_OR_PARTITION), and creating a
# topic that already exists without --reset; right after a --reset delete an
# "already exists" is retried while the broker finishes the removal.
seed() {
    local topic="" fixture="" reset=0
    while [ $# -gt 0 ]; do
        case "$1" in
            --topic)   topic="$2"; shift 2 ;;
            --fixture) fixture="$2"; shift 2 ;;
            --reset)   reset=1; shift ;;
            *) shift ;;
        esac
    done
    [ -z "${topic}" ] && { fail "seed: --topic required"; return 1; }

    local out rc tries=0
    if [ "${reset}" -eq 1 ]; then
        out="$(_rpk topic delete "${topic}" 2>&1)"
        rc=$?
        case "$(printf '%s\n' "${out}" | awk -v t="${topic}" '$1 == t { print $2; exit }')" in
            OK|UNKNOWN_TOPIC_OR_PARTITION:) ;;
            *) [ "${rc}" -eq 0 ] && rc=1 ;;
        esac
        if [ "${rc}" -ne 0 ]; then
            fail "seed: rpk topic delete ${topic} failed (exit ${rc})"
            _indent "${out}"
            return 1
        fi
    fi
    while :; do
        out="$(_rpk topic create "${topic}" -p 1 2>&1)"
        rc=$?
        [ "${rc}" -eq 0 ] && break
        case "${out}" in
            *TOPIC_ALREADY_EXISTS*)
                [ "${reset}" -eq 0 ] && break
                tries=$((tries + 1))
                if [ "${tries}" -lt 10 ]; then
                    sleep 1
                    continue
                fi
                ;;
        esac
        fail "seed: rpk topic create ${topic} failed (exit ${rc})"
        _indent "${out}"
        return 1
    done
    echo "${DIM}    topic ${topic} ready${RESET}"

    if [ -n "${fixture}" ]; then
        local path="${FIXTURE_DIR}/${fixture}"
        [ -f "${path}" ] || {
            fail "seed: fixture not found: ${path} (generated by examples/demo/generate_data.py)"
            return 1
        }
        local n
        n="$(grep -c . "${path}")"
        # rpk topic produce reads stdin, one message per line — ndjson maps 1:1.
        out="$(_rpk_i topic produce "${topic}" < "${path}" 2>&1)"
        rc=$?
        if [ "${rc}" -ne 0 ]; then
            fail "seed: rpk topic produce ${topic} failed (exit ${rc})"
            _indent "${out}"
            return 1
        fi
        echo "${DIM}    produced ${n} records → ${topic}${RESET}"
    fi
}

# consume --topic <T> [--timeout <S>]
# Streams the topic from the start for S seconds; the consumer never exits on
# its own, so reaching the timeout is the normal end. Returns 1 when rpk fails
# before that (no container, unknown topic) or when no record arrived.
consume() {
    local topic="" timeout_s=12
    while [ $# -gt 0 ]; do
        case "$1" in
            --topic)   topic="$2"; shift 2 ;;
            --timeout) timeout_s="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    [ -z "${topic}" ] && { fail "consume: --topic required"; return 1; }
    echo "${CYAN}    consume ${topic}  (up to ${timeout_s}s)${RESET}"
    local outf errf rc n
    outf="$(mktemp)"
    errf="$(mktemp)"
    # -o start = from the beginning; the timeout bounds the stream for the demo.
    _with_timeout "${timeout_s}" ${_DOCKER} exec "${KAFKA_CONTAINER}" \
        rpk topic consume "${topic}" -o start -f '%v\n' 2>"${errf}" | tee "${outf}"
    rc=${PIPESTATUS[0]}
    n="$(grep -c . "${outf}")"
    if [ "${rc}" -ne 0 ] && [ "${rc}" -ne 124 ]; then
        fail "consume ${topic}: rpk failed (exit ${rc})"
        _indent "$(cat "${errf}")"
        rm -f "${outf}" "${errf}"
        return 1
    fi
    rm -f "${outf}" "${errf}"
    if [ "${n}" -eq 0 ]; then
        fail "consume ${topic}: no record arrived within ${timeout_s}s"
        return 1
    fi
    echo "${DIM}    ${n} records read from ${topic}${RESET}"
}
