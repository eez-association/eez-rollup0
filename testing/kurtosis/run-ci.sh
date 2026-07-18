#!/usr/bin/env bash
# Production-path pull-request lane.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
if [[ -n "${GITHUB_RUN_ID:-}" ]]; then
    default_enclave="eez-pr-$GITHUB_RUN_ID-${GITHUB_RUN_ATTEMPT:-1}"
else
    default_enclave="eez-local-$(date +%s)-$$"
fi
export KURTOSIS_ENCLAVE="${KURTOSIS_ENCLAVE:-$default_enclave}"
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
http_url() {
    case "$1" in
        "") printf '\n' ;;
        http://* | https://*) printf '%s\n' "$1" ;;
        *) printf 'http://%s\n' "$1" ;;
    esac
}

deadline=$((SECONDS + ${EEZ_CI_READY_TIMEOUT_SECS:-900}))
while (( SECONDS < deadline )); do
    l1="$(http_url "$(kurtosis port print "$KURTOSIS_ENCLAVE" el-1-reth-lighthouse rpc 2>/dev/null || true)")"
    l2="$(http_url "$(kurtosis port print "$KURTOSIS_ENCLAVE" eez-node l2-rpc 2>/dev/null || true)")"
    if [[ -n "$l1" && -n "$l2" ]] \
        && cast block-number --rpc-url "$l1" >/dev/null 2>&1 \
        && cast block-number --rpc-url "$l2" >/dev/null 2>&1; then
        break
    fi
    sleep 5
done
(( SECONDS < deadline )) || {
    echo "CI network did not become RPC-ready (l1=$l1, l2=$l2)" >&2
    exit 1
}

while (( SECONDS < deadline )); do
    node_logs="$(timeout 10s kurtosis service logs "$KURTOSIS_ENCLAVE" eez-node 2>/dev/null || true)"
    if grep -qE 'bundle outcome observed.*Included' <<<"$node_logs"; then
        break
    fi
    sleep 5
done
(( SECONDS < deadline )) || {
    echo "no bundle inclusion observed before the readiness timeout" >&2
    exit 1
}
echo "==> production path ready: bundle inclusion observed"

bash "$HERE/scripts/verify-production-path.sh"

echo "production-path CI PASS"
