#!/usr/bin/env bash
# Exercise missed proposals and multi-blob catch-up, then verify canonical beacon DA.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
source "$HERE/../ports.sh" >/dev/null
DEPLOY_DIR="$(mktemp -d "${TMPDIR:-/tmp}/eez-blob-settlement.XXXXXX")"
SIGNER_STOPPED=0
VALIDATOR_STOPPED=0
cleanup() {
    local status=$?
    trap - EXIT
    if (( VALIDATOR_STOPPED )); then
        kurtosis service start "${KURTOSIS_ENCLAVE:-eez-ci}" vc-1-reth-lighthouse >/dev/null 2>&1 || true
    fi
    if (( SIGNER_STOPPED )); then
        kurtosis service start "${KURTOSIS_ENCLAVE:-eez-ci}" eez-proof-signer >/dev/null 2>&1 || true
    fi
    rm -rf "$DEPLOY_DIR"
    exit "$status"
}
trap cleanup EXIT
kurtosis files download "${KURTOSIS_ENCLAVE:-eez-ci}" eez-deployments "$DEPLOY_DIR" >/dev/null
set -a
source "$DEPLOY_DIR/deployments.env"
set +a
: "${EEZ_SETTLEMENT_WRAPPER_ADDRESS:?blob settlement wrapper is not deployed}"

export EEZ_L1_RPC_URL="$EEZ_DEVNET_L1_RPC"
export EEZ_L2_RPC_URL="$EEZ_DEVNET_L2_RPC"
export EEZ_L1_BEACON_URL="$EEZ_DEVNET_L1_BEACON"
if [[ -n "${EEZ_BLOB_VERIFIER_BIN:-}" ]]; then
    verifier="$EEZ_BLOB_VERIFIER_BIN"
elif [[ -n "${EEZ_PREBUILT_BIN_DIR:-}" ]]; then
    verifier="$EEZ_PREBUILT_BIN_DIR/eez-verify-blob-settlement"
else
    cargo build --locked --manifest-path "$REPO/Cargo.toml" -p eez-l1 --example verify_blob_settlement
    verifier="$REPO/target/debug/examples/verify_blob_settlement"
fi

result_dir="${EEZ_CI_RESULT_DIR:-$DEPLOY_DIR}"
mkdir -p "$result_dir"
verify_da() {
    deadline=$((SECONDS + ${EEZ_BLOB_WAIT_SECS:-600}))
    until "$verifier" >"$result_dir/blob-settlement.json"; do
        (( SECONDS < deadline )) || exit 1
        sleep 3
    done
    jq -e '.posts | length > 0' "$result_dir/blob-settlement.json" >/dev/null
}
# The relay must know the proposers before the builder can include a bundle.
relay="$(kurtosis port print "${KURTOSIS_ENCLAVE:-eez-ci}" mev-relay-api http)"
[[ "$relay" == http* ]] || relay="http://$relay"
deadline=$((SECONDS + ${EEZ_BLOB_WAIT_SECS:-600}))
until curl -fsS "$relay/relay/v1/builder/validators" | jq -e 'length > 0' >/dev/null; do
    (( SECONDS < deadline )) || { echo "relay has no registered validators" >&2; exit 1; }
    sleep 3
done
verify_da

source "$HERE/lib.sh"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
L1="$EEZ_DEVNET_L1_RPC"
L2="$EEZ_DEVNET_L2_RPC"
KEY="$(yaml_value poster_key)"
RECIPIENT=0x9999999999999999999999999999999999999999
WAIT_SECS="${EEZ_BLOB_RECOVERY_WAIT_SECS:-300}"
slot_seconds="$(curl -fsS "$EEZ_DEVNET_L1_BEACON/eth/v1/config/spec" | jq -er '.data.SECONDS_PER_SLOT | tonumber')"

echo "==> accumulating a multi-blob pure-L2 backlog with proving paused"
kurtosis service stop "$ENCLAVE" eez-proof-signer
SIGNER_STOPPED=1
payload="$(python3 -c 'print("0x" + "ff" * 60000)')"
hashes='[]'
minimum=0
for _ in 1 2 3; do
    receipt="$(cast send "$RECIPIENT" --value 1 --data "$payload" --gas-limit 4000000 \
        --private-key "$KEY" --rpc-url "$L2" --timeout "$WAIT_SECS" --json)"
    [[ "$(jq -r '.status' <<<"$receipt")" == 0x1 ]]
    hash="$(jq -er '.transactionHash' <<<"$receipt")"
    hashes="$(jq -cn --argjson hashes "$hashes" --arg hash "$hash" '$hashes + [$hash]')"
    height="$(jq -er '.blockNumber' <<<"$receipt")"
    (( height <= minimum )) || minimum=$((height))
done

echo "==> missing proposer slots while the blob backlog is pending"
kurtosis service stop "$ENCLAVE" vc-1-reth-lighthouse
VALIDATOR_STOPPED=1
sleep 2
held="$(cast block latest --rpc-url "$L1" --json)"
held_height="$(jq -er '.number' <<<"$held")"
held_timestamp="$(jq -er '.timestamp' <<<"$held")"
sleep "$((2 * slot_seconds + 2))"
[[ "$(cast block-number --rpc-url "$L1")" == "$((held_height))" ]]
kurtosis service start "$ENCLAVE" vc-1-reth-lighthouse
VALIDATOR_STOPPED=0
kurtosis service start "$ENCLAVE" eez-proof-signer
SIGNER_STOPPED=0

deadline=$((SECONDS + WAIT_SECS))
while (( SECONDS < deadline )); do
    next="$(cast block "$((held_height + 1))" --rpc-url "$L1" --json 2>/dev/null || true)"
    next_timestamp="$(jq -r '.timestamp // 0' <<<"${next:-null}" 2>/dev/null || echo 0)"
    (( next_timestamp > held_timestamp )) && break
    sleep 3
done
(( SECONDS < deadline && next_timestamp - held_timestamp >= 2 * slot_seconds ))
gap=$((next_timestamp - held_timestamp))

echo "==> requiring the queued L2 data to become safe after recovery"
deadline=$((SECONDS + WAIT_SECS))
while (( SECONDS < deadline )); do
    safe="$(cast block safe --rpc-url "$L2" --json | jq -er '.number')"
    (( safe >= minimum )) && break
    sleep 3
done
(( SECONDS < deadline ))

verify_da
jq -e --argjson hashes "$hashes" \
    'any(.posts[]; .blobs > 1 and (($hashes - .l2_transaction_hashes) | length) == 0)' \
    "$result_dir/blob-settlement.json" >/dev/null

echo "canonical blob DA and recovery PASS"
