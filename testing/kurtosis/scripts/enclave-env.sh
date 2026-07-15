#!/usr/bin/env bash
# Resolve host-reachable endpoints and test keys from the CI enclave.

ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
_ee_here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_ee_args="${KURTOSIS_ARGS_FILE:-$_ee_here/../ci-args.yaml}"

_ee_http() { case "$1" in http*) echo "$1";; "" ) echo "";; *) echo "http://$1";; esac; }
_ee_port() { kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || true; }
# Flat "key: value" lookup out of args.yaml (strips quotes + trailing comments).
_ee_yaml() {
    [[ -f "$_ee_args" ]] || return 0
    { grep -E "^[[:space:]]*$1:" "$_ee_args" | head -1 \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; } || true
}

: "${EEZ_L1_RPC_URL:=$(_ee_http "$(_ee_port el-1-reth-lighthouse rpc)")}"
: "${EEZ_L1_BUILDER_RPC_URL:=$(_ee_http "$(_ee_port "${KURTOSIS_BUILDER_SERVICE:-el-2-reth-builder-lighthouse}" rpc)")}"
: "${EEZ_L1_POSTER_KEY:=$(_ee_yaml poster_key)}"
: "${EEZ_PROOF_SIGNER_KEY:=$(_ee_yaml proof_signer_key)}"
: "${EEZ_BUNDLE_PROBE_KEY:=$(_ee_yaml probe_key)}"
: "${EEZ_L1_BLOCK_TIME_MS:=$(_ee_yaml l1_block_time_ms)}"
: "${EEZ_L1_BLOCK_TIME_MS:=12000}"
: "${EEZ_L1_SLOT_SECONDS:=$((EEZ_L1_BLOCK_TIME_MS / 1000))}"

export EEZ_L1_RPC_URL EEZ_L1_BUILDER_RPC_URL EEZ_L1_POSTER_KEY \
       EEZ_PROOF_SIGNER_KEY EEZ_BUNDLE_PROBE_KEY EEZ_L1_BLOCK_TIME_MS \
       EEZ_L1_SLOT_SECONDS
