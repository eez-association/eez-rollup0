#!/usr/bin/env bash
# Start a disposable blob network and verify settlement against real beacon data.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export KURTOSIS_ENCLAVE="${KURTOSIS_ENCLAVE:-eez-blob-ci}"
export EEZ_CI_RESULT_DIR="${EEZ_CI_RESULT_DIR:-$HERE/../../artifacts/kurtosis-blob}"
mkdir -p "$EEZ_CI_RESULT_DIR"
export KURTOSIS_ARGS_FILE="$EEZ_CI_RESULT_DIR/blob-args.yaml"
sed \
    -e '/^eez:/a\
  deploy_candidate_v1: true' \
    -e '/      count: 1/i\
      supernode: true' \
    -e '/    electra_fork_epoch: 0/a\
    fulu_fork_epoch: 0\
    bpo_1_epoch: 0\
    bpo_1_max_blobs: 15\
    bpo_1_target_blobs: 10\
    bpo_1_base_fee_update_fraction: 8346193' \
    -e 's/enable_explorers: true/enable_explorers: false/' \
    -e "s|eez_node_image:.*|eez_node_image: ${EEZ_NODE_IMAGE:-eez-node:ci}|" \
    -e "s|proof_signer_image:.*|proof_signer_image: ${EEZ_PROOF_SIGNER_IMAGE:-eez-proof-signer:ci}|" \
    -e "s|deploy_image:.*|deploy_image: ${EEZ_DEPLOY_IMAGE:-eez-deploy:ci}|" \
    "$HERE/ci-args.yaml" >"$KURTOSIS_ARGS_FILE"

cleanup() {
    local status=$?
    trap - EXIT
    for service in eez-node eez-proof-signer; do
        timeout 30s kurtosis service logs "$KURTOSIS_ENCLAVE" "$service" \
            >"$EEZ_CI_RESULT_DIR/blob-$service.log" 2>&1 || true
    done
    bash "$HERE/stop.sh" || true
    exit "$status"
}
trap cleanup EXIT
bash "$HERE/start.sh" "$KURTOSIS_ARGS_FILE"
bash "$HERE/scripts/verify-blob-da.sh"
