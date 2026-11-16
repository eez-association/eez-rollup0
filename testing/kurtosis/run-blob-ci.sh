#!/usr/bin/env bash
# Kurtosis blob settlement lane, in its own enclave beside the calldata lane.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
export KURTOSIS_ENCLAVE="${KURTOSIS_ENCLAVE:-eez-blob-ci}"
[[ "$KURTOSIS_ENCLAVE" != eez-ci ]] || { echo "blob lane cannot use the calldata enclave eez-ci" >&2; exit 1; }
export KURTOSIS_PRIVILEGED=0
export EEZ_NODE_IMAGE="${EEZ_NODE_IMAGE:-eez-node:blob-ci}"
export EEZ_PROOF_SIGNER_IMAGE="${EEZ_PROOF_SIGNER_IMAGE:-eez-proof-signer:blob-ci}"
export EEZ_DEPLOY_IMAGE="${EEZ_DEPLOY_IMAGE:-eez-deploy:blob-ci}"
export EEZ_CI_RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/kurtosis-blob-e2e}"
mkdir -p "$EEZ_CI_RESULT_DIR/checks"
export KURTOSIS_ARGS_FILE="$EEZ_CI_RESULT_DIR/blob-args.yaml"

if kurtosis enclave inspect "$KURTOSIS_ENCLAVE" >/dev/null 2>&1; then
    echo "enclave $KURTOSIS_ENCLAVE already exists; choose a fresh blob enclave" >&2
    exit 1
fi
cleanup() {
    local status=$?
    trap - EXIT
    kurtosis enclave inspect "$KURTOSIS_ENCLAVE" >"$EEZ_CI_RESULT_DIR/enclave.txt" 2>&1 || true
    for service in eez-node eez-proof-signer eez-l2-follower; do
        timeout 30s kurtosis service logs "$KURTOSIS_ENCLAVE" "$service" \
            >"$EEZ_CI_RESULT_DIR/$service.log" 2>&1 || true
    done
    bash "$HERE/stop.sh" || true
    exit "$status"
}
trap cleanup EXIT

sed -e "s|eez_node_image:.*|eez_node_image: $EEZ_NODE_IMAGE|" \
    -e "s|proof_signer_image:.*|proof_signer_image: $EEZ_PROOF_SIGNER_IMAGE|" \
    -e "s|deploy_image:.*|deploy_image: $EEZ_DEPLOY_IMAGE|" \
    "$HERE/blob-args.yaml" >"$KURTOSIS_ARGS_FILE"
bash "$HERE/start.sh" "$KURTOSIS_ARGS_FILE"
# shellcheck disable=SC1091
source "$HERE/ports.sh" >/dev/null

deadline=$((SECONDS + ${EEZ_CI_READY_TIMEOUT_SECS:-900}))
until cast block-number --rpc-url "$EEZ_DEVNET_L2_RPC" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || { echo "blob L2 RPC did not become ready" >&2; exit 1; }
    sleep 5
done
L2="$EEZ_DEVNET_L2_RPC" bash "$HERE/scripts/verify-eezl2-deployment.sh"
bash "$HERE/scripts/verify-cross-chain-waves.sh"
bash "$HERE/scripts/verify-ingress-admission.sh" 2>&1 | tee "$EEZ_CI_RESULT_DIR/checks/ingress-admission.log"
bash "$HERE/scripts/verify-fault-recovery.sh" 2>&1 | tee "$EEZ_CI_RESULT_DIR/checks/fault-recovery.log"
# The node restart above moves its host ports.
# shellcheck disable=SC1091
source "$HERE/ports.sh" >/dev/null

# Every settlement must carry its DA in blobs: a calldata fallback would still
# derive, so the follower check alone cannot catch it.
kurtosis files download "$KURTOSIS_ENCLAVE" eez-deployments "$EEZ_CI_RESULT_DIR/deployments" >/dev/null
# shellcheck disable=SC1091
source "$EEZ_CI_RESULT_DIR/deployments/deployments.env"
posts="$(cast logs --json --from-block "$EEZ_REGISTRY_DEPLOY_BLOCK" --address "$EEZ_REGISTRY_ADDRESS" \
    'BatchPosted(bytes32,uint64[])' --rpc-url "$EEZ_DEVNET_L1_RPC" | jq -r '[.[].transactionHash] | unique[]')"
[[ -n "$posts" ]] || { echo "no settlement posts found" >&2; exit 1; }
for tx in $posts; do
    jq -e '.type == "0x3" and (.blobVersionedHashes | length > 0)' \
        <<<"$(cast tx "$tx" --json --rpc-url "$EEZ_DEVNET_L1_RPC")" >/dev/null \
        || { echo "settlement $tx is not a blob transaction" >&2; exit 1; }
    jq -e '.status == "0x1"' <<<"$(cast receipt "$tx" --json --rpc-url "$EEZ_DEVNET_L1_RPC")" >/dev/null \
        || { echo "settlement $tx reverted" >&2; exit 1; }
done
echo "$(wc -w <<<"$posts" | tr -d ' ') settlement posts are type-3 blob transactions" \
    | tee "$EEZ_CI_RESULT_DIR/checks/blob-carrier.log"

bash "$HERE/scripts/verify-follower.sh" 2>&1 | tee "$EEZ_CI_RESULT_DIR/checks/follower.log"
echo "blob settlement E2E PASS"
