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
# Flat "key: value" lookup out of args.yaml (strips quotes + trailing comments).
_ee_yaml() {
    [[ -f "$_ee_args" ]] || return 0
    { grep -E "^[[:space:]]*$1:" "$_ee_args" | head -1 \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; } || true
}

: "${EEZ_L1_RPC_URL:=$(_ee_http "$(_ee_port "$KURTOSIS_L1_SERVICE" "$KURTOSIS_L1_RPC_PORT")")}"
: "${EEZ_L1_BUILDER_RPC_URL:=$(_ee_http "$(_ee_port "$KURTOSIS_BUILDER_SERVICE" "$KURTOSIS_BUILDER_RPC_PORT")")}"
: "${EEZ_DISRUPTOOR_URL:=$(_ee_http "$(_ee_port disruptoor http)")}"
: "${EEZ_L1_POSTER_KEY:=$(_ee_yaml poster_key)}"
: "${EEZ_PROOF_SIGNER_KEY:=$(_ee_yaml proof_signer_key)}"

export EEZ_L1_RPC_URL EEZ_L1_BUILDER_RPC_URL EEZ_DISRUPTOOR_URL \
       EEZ_L1_POSTER_KEY EEZ_PROOF_SIGNER_KEY \
       KURTOSIS_L1_SERVICE KURTOSIS_L1_RPC_PORT \
       KURTOSIS_BUILDER_SERVICE KURTOSIS_BUILDER_RPC_PORT
