#!/usr/bin/env bash
# Shared helpers for the Kafka streaming act (examples/demo/kafka/).
#
# Sourced by every step's run.sh and every *.sh sub-step. Provides:
#   title <text>                     — section banner
#   pause                            — wait for [Enter] (skipped when NONINTERACTIVE=1)
#   psql_run <file.sql>              — run a demo SQL file over the PG wire, passing
#                                      -v broker=<SQL_BROKER> for CREATE SOURCE/STREAM
#   wait_rows <table_expr> <n> [s]   — poll SELECT count(*) FROM <table_expr> until >= n
#   seed  --topic T [--fixture F] [--reset]   — (re)create a topic + produce a fixture
#   consume --topic T [--timeout S] [--min N] — wait for >=N records, then drain
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
PGHOST_="${PGHOST_:-127.0.0.1}"
PGPORT_="${PGPORT_:-8817}"
PGUSER_="${PGUSER_:-demo}"
PGDB_="${PGDB_:-demo}"
export PGPASSWORD="${PGPASSWORD:-demo}"

# ── colours ──────────────────────────────────────────────────────────────────
if [ -t 1 ]; then
    BOLD=$'\033[1m'; GREEN=$'\033[92m'; CYAN=$'\033[96m'; DIM=$'\033[2m'; RESET=$'\033[0m'
else
    BOLD=""; GREEN=""; CYAN=""; DIM=""; RESET=""
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

# ── SQL ──────────────────────────────────────────────────────────────────────
# psql_run <file.sql> — resolved relative to the caller's cwd (the step folder).
psql_run() {
    local file="$1"
    echo "${CYAN}    psql ← ${file}   (broker=${SQL_BROKER})${RESET}"
    psql -h "${PGHOST_}" -p "${PGPORT_}" -U "${PGUSER_}" \
         -v ON_ERROR_STOP=1 -v "broker=${SQL_BROKER}" \
         -f "${file}" "${PGDB_}"
}

# wait_rows <table_expr> <count> [timeout_s]
# Polls SELECT count(*) FROM <table_expr> (a full FROM clause, may include WHERE)
# until it reaches <count> or the timeout elapses.
wait_rows() {
    local expr="$1" want="$2" timeout_s="${3:-30}"
    local deadline=$(( $(date +%s) + timeout_s ))
    local got=0
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        got="$(psql -h "${PGHOST_}" -p "${PGPORT_}" -U "${PGUSER_}" -tAc \
               "SELECT count(*) FROM ${expr};" "${PGDB_}" 2>/dev/null | tr -d '[:space:]')"
        [ -n "${got}" ] && [ "${got}" -ge "${want}" ] 2>/dev/null && {
            echo "${DIM}    ${expr} → ${got} rows (≥ ${want}) ✅${RESET}"; return 0; }
        sleep 1
    done
    echo "${DIM}    ${expr} → ${got:-0} rows after ${timeout_s}s (wanted ${want}) ⚠${RESET}"
    return 0
}

# ── Kafka (rpk inside the redpanda container) ────────────────────────────────
# seed --topic <T> [--fixture <F.ndjson>] [--reset]
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
    [ -z "${topic}" ] && { echo "seed: --topic required" >&2; return 1; }

    if [ "${reset}" -eq 1 ]; then
        _rpk topic delete "${topic}" >/dev/null 2>&1 || true
    fi
    _rpk topic create "${topic}" -p 1 >/dev/null 2>&1 || true
    echo "${DIM}    topic ${topic} ready${RESET}"

    if [ -n "${fixture}" ]; then
        local path="${FIXTURE_DIR}/${fixture}"
        [ -f "${path}" ] || { echo "seed: fixture not found: ${path}" >&2; return 1; }
        local n
        n="$(grep -c . "${path}")"
        # rpk topic produce reads stdin, one message per line — ndjson maps 1:1.
        _rpk_i topic produce "${topic}" >/dev/null 2>&1 < "${path}"
        echo "${DIM}    produced ${n} records → ${topic}${RESET}"
    fi
}

# topic_hwm <T> — sum of the high-watermarks across a topic's partitions (the
# number of records currently in the topic). Empty/absent topic → 0.
topic_hwm() {
    _rpk topic describe "$1" -p 2>/dev/null \
        | awk '$1 ~ /^[0-9]+$/ { sum += $NF } END { print sum + 0 }'
}

# consume --topic <T> [--timeout <S>] [--min <N>]
# The STREAM / fan-in workers produce asynchronously, so first WAIT (up to
# --timeout) until the topic holds at least --min records (default 1), then drain
# the backlog with `-o :end` — which reads up to the current high-watermark and
# exits cleanly. (A plain `-o start` bounded by an external `timeout` gets
# SIGTERM'd, and rpk's block-buffered stdout is DISCARDED on the kill — which
# printed nothing even when records existed.)
consume() {
    local topic="" timeout_s=12 min=1
    while [ $# -gt 0 ]; do
        case "$1" in
            --topic)   topic="$2"; shift 2 ;;
            --timeout) timeout_s="$2"; shift 2 ;;
            --min)     min="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    [ -z "${topic}" ] && { echo "consume: --topic required" >&2; return 1; }

    local deadline=$(( $(date +%s) + timeout_s )) hwm=0
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        hwm="$(topic_hwm "${topic}")"
        [ "${hwm:-0}" -ge "${min}" ] 2>/dev/null && break
        sleep 1
    done
    echo "${CYAN}    consume ${topic}  (${hwm:-0} record(s))${RESET}"
    # -o :end = from the start up to the current high-watermark, then exit.
    _rpk topic consume "${topic}" -o ':end' -f '%v\n' 2>/dev/null || true
}
