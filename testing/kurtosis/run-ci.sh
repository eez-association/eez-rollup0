#!/usr/bin/env bash
# Kurtosis end-to-end pull-request lane.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
export KURTOSIS_ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
export KURTOSIS_BUILDER_SERVICE="${KURTOSIS_BUILDER_SERVICE:-el-2-reth-builder-lighthouse}"
export KURTOSIS_PRIVILEGED=0

RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/kurtosis-e2e}"
mkdir -p "$RESULT_DIR"
export EEZ_CI_RESULT_DIR="$RESULT_DIR"
rm -f "$RESULT_DIR/result.json"

ARGS_TEMPLATE="${KURTOSIS_ARGS_FILE:-$HERE/ci-args.yaml}"
export EEZ_NODE_IMAGE="${EEZ_NODE_IMAGE:-eez-node:ci-${GITHUB_SHA:-local}}"
export EEZ_PROOF_SIGNER_IMAGE="${EEZ_PROOF_SIGNER_IMAGE:-eez-proof-signer:ci-${GITHUB_SHA:-local}}"
export EEZ_DEPLOY_IMAGE="${EEZ_DEPLOY_IMAGE:-eez-deploy:ci-${GITHUB_SHA:-local}}"
ROOT_COMMIT=unknown
PROTOCOL_COMMIT=unknown
signed_window_count=0
remote_attestation_count=0
export KURTOSIS_ARGS_FILE="$RESULT_DIR/ci-args.yaml"

SIGNED_WINDOW_EVENT='event_name="eez.proof_signer.window_signed"'
REMOTE_ATTESTATION_EVENT='event_name="eez.prover_client.attested"'

capture_service_log() {
    local service="$1"
    timeout 30s kurtosis service logs "$KURTOSIS_ENCLAVE" "$service" \
        >"$RESULT_DIR/$service.log" 2>&1
}

count_literal() {
    local needle="$1" file="$2"
    sed 's/\x1b\[[0-9;]*m//g' "$file" \
        | awk -v needle="$needle" 'index($0, needle) { count++ } END { print count + 0 }'
}

refresh_proof_counts() {
    if [[ -f "$RESULT_DIR/eez-proof-signer.log" ]]; then
        signed_window_count="$(count_literal "$SIGNED_WINDOW_EVENT" "$RESULT_DIR/eez-proof-signer.log")"
    fi
    if [[ -f "$RESULT_DIR/eez-node.log" ]]; then
        remote_attestation_count="$(count_literal "$REMOTE_ATTESTATION_EVENT" "$RESULT_DIR/eez-node.log")"
    fi
}

write_result() {
    local result="$1" exit_code="$2"
    jq -n \
        --arg result "$result" \
        --arg candidate_image "$EEZ_NODE_IMAGE" \
        --arg root_commit "$ROOT_COMMIT" \
        --arg protocol_commit "$PROTOCOL_COMMIT" \
        --arg node_image "$EEZ_NODE_IMAGE" \
        --arg signer_image "$EEZ_PROOF_SIGNER_IMAGE" \
        --arg deploy_image "$EEZ_DEPLOY_IMAGE" \
        --argjson exit_code "$exit_code" \
        --argjson signed_windows "$signed_window_count" \
        --argjson remote_attestations "$remote_attestation_count" \
        '{
            result: $result,
            exit_code: $exit_code,
            candidate_image: $candidate_image,
            commits: {
                root: $root_commit,
                protocol: $protocol_commit
            },
            images: {
                node: $node_image,
                proof_signer: $signer_image,
                deploy: $deploy_image
            },
            proof_flow: {
                signed_windows: $signed_windows,
                remote_attestations: $remote_attestations
            }
        } + if $result == "pass" then {
            modes: ["inbound", "outbound", "mixed", "mixed-pure"],
            cross_chain_convergence: "pass",
            state_chaining: "pass",
            l1_l2_root_divergence: 0,
            safe_head_convergence: "pass"
        } else {} end' >"$RESULT_DIR/result.json"
}

verify_real_proof_path() {
    local signer_log="$RESULT_DIR/eez-proof-signer.log"

    refresh_proof_counts
    (( signed_window_count > 0 )) || {
        echo "no successfully signed window found in the proof-signer log" >&2
        return 1
    }
    (( remote_attestation_count > 0 )) || {
        echo "no accepted remote attestation found in the node log" >&2
        return 1
    }

    local failure
    for failure in \
        'Prove request rejected while reading window' \
        'request pipeline rejected' \
        'request pipeline invariant failed' \
        'request pipeline worker failed' \
        'public-input hash signing failed'
    do
        if grep -Fq "$failure" "$signer_log"; then
            echo "proof-signer log contains failure: $failure" >&2
            return 1
        fi
    done
}

cleanup() {
    local status=$?
    trap - EXIT
    kurtosis enclave inspect "$KURTOSIS_ENCLAVE" >"$RESULT_DIR/enclave.txt" 2>&1 || true
    for service in eez-node eez-proof-signer "$KURTOSIS_BUILDER_SERVICE" mev-relay-api; do
        capture_service_log "$service" || true
    done
    refresh_proof_counts
    if (( status != 0 )); then
        write_result fail "$status" || true
        echo "==> candidate node log tail (failure diagnostics)" >&2
        tail -n 200 "$RESULT_DIR/eez-node.log" >&2 || true
        echo "==> proof signer log tail (failure diagnostics)" >&2
        tail -n 200 "$RESULT_DIR/eez-proof-signer.log" >&2 || true
    elif [[ ! -f "$RESULT_DIR/result.json" ]]; then
        status=1
        write_result fail "$status" || true
        echo "CI completed without producing a result" >&2
    fi
    bash "$HERE/stop.sh" || true
    exit "$status"
}
trap cleanup EXIT

ROOT_COMMIT="$(git -C "$REPO" rev-parse HEAD)"
PROTOCOL_COMMIT="$(git -C "$REPO/eez-core-protocol" rev-parse HEAD)"
sed \
    -e "s|^[[:space:]]*eez_node_image:.*|  eez_node_image: $EEZ_NODE_IMAGE|" \
    -e "s|^[[:space:]]*proof_signer_image:.*|  proof_signer_image: $EEZ_PROOF_SIGNER_IMAGE|" \
    -e "s|^[[:space:]]*deploy_image:.*|  deploy_image: $EEZ_DEPLOY_IMAGE|" \
    -e "s|^[[:space:]]*enable_explorers:.*|  enable_explorers: false|" \
    "$ARGS_TEMPLATE" >"$KURTOSIS_ARGS_FILE"

bash "$HERE/start.sh" "$KURTOSIS_ARGS_FILE"

# Wait until the canonical L1 and candidate L2 RPCs answer.
# shellcheck disable=SC1091
source "$HERE/ports.sh" >/dev/null
l1="$EEZ_DEVNET_L1_RPC"
l2="$EEZ_DEVNET_L2_RPC"

deadline=$((SECONDS + ${EEZ_CI_READY_TIMEOUT_SECS:-900}))
while (( SECONDS < deadline )); do
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

L2="$l2" bash "$HERE/scripts/verify-eezl2-deployment.sh"

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
echo "==> network ready: bundle inclusion observed"

bash "$HERE/scripts/verify-cross-chain-waves.sh"

capture_service_log eez-proof-signer
capture_service_log eez-node
verify_real_proof_path
write_result pass 0

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "### Kurtosis E2E result"
        echo
        echo "- Root commit: \`$ROOT_COMMIT\`"
        echo "- Protocol commit: \`$PROTOCOL_COMMIT\`"
        echo "- Node image: \`$EEZ_NODE_IMAGE\`"
        echo "- Proof-signer image: \`$EEZ_PROOF_SIGNER_IMAGE\`"
        echo "- Deploy image: \`$EEZ_DEPLOY_IMAGE\`"
        echo "- Inbound, outbound, mixed, and mixed-pure waves: pass"
        echo "- Inbound, outbound, and mixed-direction state chaining: pass"
        echo "- Signed windows observed: $signed_window_count"
        echo "- Remote attestations observed: $remote_attestation_count"
        echo "- L1/L2 root divergence: 0"
        echo "- L2 safe head: converged"
    } >>"$GITHUB_STEP_SUMMARY"
fi

echo "kurtosis e2e CI PASS"
