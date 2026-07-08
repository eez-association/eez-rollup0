#!/usr/bin/env bash
# Discover host-reachable enclave endpoints + keys for the OPTIONAL host-side
# helpers (reorg-scheduler.sh, smoke-rbuilder.sh). Everything the devnet needs to
# run is wired inside the enclave (main.star) — this is only for tools you run
# from your laptop that must reach published ports.
#
# Uses `kurtosis port print` (the idiomatic API), NOT inspect-scraping. Source it:
#   source infra/kurtosis/scripts/enclave-env.sh
# Already-set env vars win, so you can override any of these by exporting first.

ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
_ee_here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_ee_args="${KURTOSIS_ARGS_FILE:-$_ee_here/../args.yaml}"
KURTOSIS_L1_SERVICE="${KURTOSIS_L1_SERVICE:-el-1-reth-lighthouse}"
KURTOSIS_L1_RPC_PORT="${KURTOSIS_L1_RPC_PORT:-rpc}"
KURTOSIS_BUILDER_SERVICE="${KURTOSIS_BUILDER_SERVICE:-el-5-reth-builder-lighthouse}"
KURTOSIS_BUILDER_RPC_PORT="${KURTOSIS_BUILDER_RPC_PORT:-rbuilder-rpc}"

_ee_http() { case "$1" in http*) echo "$1";; "" ) echo "";; *) echo "http://$1";; esac; }
_ee_port() { kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || true; }
_ee_inspect() { kurtosis enclave inspect "$ENCLAVE" 2>/dev/null || true; }
_ee_services() {
    { _ee_inspect \
        | tr -cs '[:alnum:]_.-' '\n' \
        | grep -E '^[[:alnum:]_.-]+$' \
        | sort -u; } || true
}
_ee_first_port() {
    local port_id="$1"; shift
    local svc url
    for svc in "$@"; do
        [[ -n "$svc" ]] || continue
        url="$(_ee_port "$svc" "$port_id")"
        if [[ -n "$url" ]]; then
            echo "$url"
            return 0
        fi
    done
}
_ee_discover_l1_rpc() {
    local svc url services
    url="$(_ee_first_port "$KURTOSIS_L1_RPC_PORT" "$KURTOSIS_L1_SERVICE")"
    [[ -n "$url" ]] && { echo "$url"; return 0; }

    services="$(_ee_services)"
    for svc in $(printf '%s\n' "$services" | grep -E '^el-1-' || true); do
        case "$svc" in *builder*|*rbuilder*) continue;; esac
        url="$(_ee_port "$svc" "$KURTOSIS_L1_RPC_PORT")"
        [[ -n "$url" ]] && { echo "$url"; return 0; }
    done
    for svc in $(printf '%s\n' "$services" | grep -E '^el-[0-9]+-' || true); do
        case "$svc" in *builder*|*rbuilder*) continue;; esac
        url="$(_ee_port "$svc" "$KURTOSIS_L1_RPC_PORT")"
        [[ -n "$url" ]] && { echo "$url"; return 0; }
    done
}
_ee_discover_builder_rpc() {
    local svc port_id url services
    url="$(_ee_first_port "$KURTOSIS_BUILDER_RPC_PORT" "$KURTOSIS_BUILDER_SERVICE")"
    [[ -n "$url" ]] && { echo "$url"; return 0; }

    services="$(_ee_services)"
    for svc in $(printf '%s\n' "$services" | grep -Ei '(builder|rbuilder)' || true); do
        for port_id in "$KURTOSIS_BUILDER_RPC_PORT" rbuilder-rpc rpc http; do
            url="$(_ee_port "$svc" "$port_id")"
            [[ -n "$url" ]] && { echo "$url"; return 0; }
        done
    done
}
# Flat "key: value" lookup out of args.yaml (strips quotes + trailing comments).
_ee_yaml() {
    [[ -f "$_ee_args" ]] || return 0
    { grep -E "^[[:space:]]*$1:" "$_ee_args" | head -1 \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; } || true
}

: "${EEZ_L1_RPC_URL:=$(_ee_http "$(_ee_discover_l1_rpc)")}"
: "${EEZ_L1_BUILDER_RPC_URL:=$(_ee_http "$(_ee_discover_builder_rpc)")}"
: "${EEZ_DISRUPTOOR_URL:=$(_ee_http "$(_ee_port disruptoor http)")}"
: "${EEZ_L1_POSTER_KEY:=$(_ee_yaml poster_key)}"
: "${EEZ_PROOF_SIGNER_KEY:=$(_ee_yaml proof_signer_key)}"

export EEZ_L1_RPC_URL EEZ_L1_BUILDER_RPC_URL EEZ_DISRUPTOOR_URL \
       EEZ_L1_POSTER_KEY EEZ_PROOF_SIGNER_KEY \
       KURTOSIS_L1_SERVICE KURTOSIS_L1_RPC_PORT \
       KURTOSIS_BUILDER_SERVICE KURTOSIS_BUILDER_RPC_PORT
