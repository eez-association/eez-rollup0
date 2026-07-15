#!/usr/bin/env bash
# Production-path pull-request lane.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
export KURTOSIS_ENCLAVE="${KURTOSIS_ENCLAVE:-eez-pr-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export KURTOSIS_BUILDER_SERVICE="${KURTOSIS_BUILDER_SERVICE:-el-2-reth-builder-lighthouse}"
export KURTOSIS_PRIVILEGED=0

RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/production-path-ci}"
mkdir -p "$RESULT_DIR"
export EEZ_CI_RESULT_DIR="$RESULT_DIR"

ARGS_TEMPLATE="${KURTOSIS_ARGS_FILE:-$HERE/ci-args.yaml}"
export EEZ_NODE_IMAGE="${EEZ_NODE_IMAGE:-eez-node:ci-${GITHUB_SHA:-local}}"
export EEZ_DEPLOY_IMAGE="${EEZ_DEPLOY_IMAGE:-eez-deploy:ci-${GITHUB_SHA:-local}}"
export KURTOSIS_ARGS_FILE="$RESULT_DIR/ci-args.yaml"
sed \
    -e "s|^[[:space:]]*eez_node_image:.*|  eez_node_image: $EEZ_NODE_IMAGE|" \
    -e "s|^[[:space:]]*deploy_image:.*|  deploy_image: $EEZ_DEPLOY_IMAGE|" \
    "$ARGS_TEMPLATE" >"$KURTOSIS_ARGS_FILE"

cleanup() {
    status=$?
    kurtosis enclave inspect "$KURTOSIS_ENCLAVE" >"$RESULT_DIR/enclave.txt" 2>&1 || true
    for service in eez-node "$KURTOSIS_BUILDER_SERVICE" mev-relay-api; do
        kurtosis service logs "$KURTOSIS_ENCLAVE" "$service" \
            >"$RESULT_DIR/$service.log" 2>&1 || true
    done
    if (( status != 0 )); then
        echo "==> candidate node log tail (failure diagnostics)" >&2
        tail -n 200 "$RESULT_DIR/eez-node.log" >&2 || true
    fi
    bash "$HERE/stop.sh" || true
    exit "$status"
}
trap cleanup EXIT

bash "$HERE/start.sh" "$KURTOSIS_ARGS_FILE"

# Wait until the canonical L1 and candidate L2 RPCs answer.
deadline=$((SECONDS + ${EEZ_CI_READY_TIMEOUT_SECS:-900}))
while (( SECONDS < deadline )); do
    l1="$(kurtosis port print "$KURTOSIS_ENCLAVE" el-1-reth-lighthouse rpc 2>/dev/null || true)"
    l2="$(kurtosis port print "$KURTOSIS_ENCLAVE" eez-node l2-rpc 2>/dev/null || true)"
    if [[ -n "$l1" && -n "$l2" ]] \
        && cast block-number --rpc-url "http://$l1" >/dev/null 2>&1 \
        && cast block-number --rpc-url "http://$l2" >/dev/null 2>&1; then
        break
    fi
    sleep 5
done
(( SECONDS < deadline )) || { echo "CI network did not become RPC-ready" >&2; exit 1; }

warmup_block="${EEZ_CI_BUILDER_WARMUP_BLOCK:-130}"
while (( $(cast block-number --rpc-url "http://$l1") < warmup_block )); do
    (( SECONDS < deadline )) || { echo "builder did not warm up before block $warmup_block" >&2; exit 1; }
    sleep 5
done

bash "$HERE/scripts/verify-production-path.sh"

echo "production-path CI PASS"
