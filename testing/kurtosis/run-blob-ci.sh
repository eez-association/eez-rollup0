#!/usr/bin/env bash
# Isolated blob settlement lane. The calldata runner retains its own defaults.
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
mkdir -p "$EEZ_CI_RESULT_DIR"
export EEZ_CI_RESULT_DIR="$(cd "$EEZ_CI_RESULT_DIR" && pwd)"
export EEZ_WAVE_LOG_DIR="$EEZ_CI_RESULT_DIR/waves"
export EEZ_SKIP_IMAGE_CHECK=1
export EEZ_USE_LOCAL_DEPLOYMENTS=0
export EEZ_SETTLEMENT_SCENARIOS="$EEZ_CI_RESULT_DIR/scenarios.json"
export EEZ_PRUNE_BUILD_CACHE=0
export KURTOSIS_ARGS_FILE="$EEZ_CI_RESULT_DIR/blob-args.yaml"
export EEZ_BLOB_EVIDENCE="$EEZ_CI_RESULT_DIR/blob-evidence.json"
rm -f "$EEZ_CI_RESULT_DIR/result.json" "$EEZ_BLOB_EVIDENCE"
if [[ -n "${EEZ_BLOB_TEST_ARCHIVE:-}" ]]; then
    [[ -f "$EEZ_BLOB_TEST_ARCHIVE" ]] || { echo "missing blob test archive" >&2; exit 1; }
fi
started=0
cleanup() {
    local status=$?
    trap - EXIT
    if (( started )); then
        kurtosis enclave inspect "$KURTOSIS_ENCLAVE" >"$EEZ_CI_RESULT_DIR/enclave.txt" 2>&1 || true
        for service in eez-node eez-proof-signer eez-follower; do
            timeout 30s kurtosis service logs "$KURTOSIS_ENCLAVE" "$service" \
                >"$EEZ_CI_RESULT_DIR/$service.log" 2>&1 || true
        done
        bash "$HERE/stop.sh" || true
    fi
    jq -n --argjson status "$status" \
        '{profile: "blob", exit_code: $status, result: (if $status == 0 then "pass" else "fail" end)}' \
        >"$EEZ_CI_RESULT_DIR/result.json"
    exit "$status"
}
trap cleanup EXIT

# Refuse to overwrite an existing enclave, including an earlier blob run.
if kurtosis enclave inspect "$KURTOSIS_ENCLAVE" >/dev/null 2>&1; then
    echo "enclave $KURTOSIS_ENCLAVE already exists; choose a fresh blob enclave" >&2
    exit 1
fi
sed -e "s|eez_node_image:.*|eez_node_image: $EEZ_NODE_IMAGE|" \
    -e "s|proof_signer_image:.*|proof_signer_image: $EEZ_PROOF_SIGNER_IMAGE|" \
    -e "s|deploy_image:.*|deploy_image: $EEZ_DEPLOY_IMAGE|" \
    "$HERE/blob-args.yaml" >"$KURTOSIS_ARGS_FILE"
started=1
bash "$HERE/start.sh" "$KURTOSIS_ARGS_FILE"
source "$HERE/ports.sh" >/dev/null
export L1="$EEZ_DEVNET_L1_RPC" L2="$EEZ_DEVNET_L2_RPC"
export L1F="$EEZ_DEVNET_L1_FRONT" L2F="$EEZ_DEVNET_L2_FRONT"
export EEZ_L1_RPC_URL="$EEZ_DEVNET_L1_RPC"
export EEZ_L1_TRACE_RPC_URL="$EEZ_DEVNET_L1_RPC"
beacon="$(kurtosis port print "$KURTOSIS_ENCLAVE" cl-1-lighthouse-reth http)"
case "$beacon" in http://*|https://*) export EEZ_L1_BEACON_URL="$beacon" ;; *) export EEZ_L1_BEACON_URL="http://$beacon" ;; esac
kurtosis files download "$KURTOSIS_ENCLAVE" eez-deployments "$EEZ_CI_RESULT_DIR/deployments" >/dev/null
set -a
source "$EEZ_CI_RESULT_DIR/deployments/deployments.env"
set +a
[[ "${EEZ_SETTLEMENT_PROFILE:-}" == blob ]] || {
    echo "enclave did not deploy the blob settlement profile" >&2; exit 1;
}
export EEZ_L1_CHAIN_ID
EEZ_L1_CHAIN_ID="$(cast chain-id --rpc-url "$EEZ_L1_RPC_URL")"

deadline=$((SECONDS + ${EEZ_CI_READY_TIMEOUT_SECS:-900}))
until cast block-number --rpc-url "$EEZ_DEVNET_L2_RPC" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || { echo "blob L2 RPC did not become ready" >&2; exit 1; }
    sleep 5
done
L2="$EEZ_DEVNET_L2_RPC" bash "$HERE/scripts/verify-eezl2-deployment.sh"
bash "$HERE/scripts/verify-cross-chain-waves.sh"
bash "$HERE/scripts/verify-ingress-admission.sh" 2>&1 | tee "$EEZ_CI_RESULT_DIR/checks/ingress-admission.log"
bash "$HERE/scripts/verify-fault-recovery.sh" 2>&1 | tee "$EEZ_CI_RESULT_DIR/checks/fault-recovery.log"
cd "$REPO"
if [[ -n "${EEZ_BLOB_TEST_ARCHIVE:-}" ]]; then
    cargo nextest run -P ci --archive-file "$EEZ_BLOB_TEST_ARCHIVE" \
        --workspace-remap "$REPO" -E 'binary(=blob_settlement)' --test-threads=1 \
        2>&1 | tee "$EEZ_CI_RESULT_DIR/blob-evidence.log"
else
    cargo test --locked -p eez-node-e2e --features blob-e2e --test blob_settlement -- --nocapture \
        2>&1 | tee "$EEZ_CI_RESULT_DIR/blob-evidence.log"
fi
echo "blob settlement E2E PASS"
