#!/usr/bin/env bash
# Verify that calls composed into one Sync block use accumulated state.
#
# Three setValue(1) calls target the same Value contract in each direction.
# Their correct return tuples are (true, 1), (false, 1), (false, 1).
# Simulating each call from the pre-slot state instead claims (true, 1) three
# times and makes execution or proof-signer validation reject the bundle.

set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

K="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
LOG_DIR="$REPO/datadir/smoke-logs"
mkdir -p "$LOG_DIR"

for t in cast forge jq curl kurtosis openssl; do command -v "$t" >/dev/null || { echo "$t not in PATH"; exit 1; }; done

_port() { kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || true; }
_http() { case "$1" in http*) echo "$1";; "") echo "";; *) echo "http://$1";; esac; }
: "${L1:=$(_http "$(_port el-1-reth-lighthouse rpc)")}"
: "${L2:=$(_http "$(_port eez-node l2-rpc)")}"
: "${L1F:=$(_http "$(_port eez-node l1-xchain)")}"
: "${L2F:=$(_http "$(_port eez-node l2-xchain)")}"
[[ -n "$L1" && -n "$L2" && -n "$L1F" && -n "$L2F" ]] \
    || { echo "could not resolve enclave ports — is '$ENCLAVE' up? (kurtosis enclave inspect $ENCLAVE)"; exit 1; }

NODE_LOG="${EEZ_NODE_LOG:-$LOG_DIR/sequential-bundle-node.log}"
SIGNER_LOG="${EEZ_PROOF_SIGNER_LOG:-$LOG_DIR/sequential-bundle-proof-signer.log}"
DEPLOY_DIR=$(mktemp -d "${TMPDIR:-/tmp}/eez-deployments-$ENCLAVE-sequential-bundle.XXXXXX")
trap 'rm -rf "$DEPLOY_DIR"' EXIT

if [[ "${EEZ_USE_LOCAL_DEPLOYMENTS:-0}" == "1" && -f "$REPO/deployments.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO/deployments.env"
    set +a
else
    kurtosis files download "$ENCLAVE" eez-deployments "$DEPLOY_DIR" >/dev/null 2>&1 \
        || { echo "kurtosis files download failed — is '$ENCLAVE' up and deployed?"; exit 1; }
    set -a
    # shellcheck disable=SC1091
    source "$DEPLOY_DIR/deployments.env"
    set +a
fi
[[ -n "${EEZ_REGISTRY_ADDRESS:-}" && -n "${EEZ_ROLLUP_ID:-}" ]] \
    || { echo "registry address or rollup id unset — deployments.env incomplete"; exit 1; }

HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
HH_KEY_2_ADDR=$(cast wallet address --private-key "$HH_KEY_2")
HH_KEY_IN="${EEZ_SEQUENTIAL_IN_KEY:-0x$(openssl rand -hex 32)}"
HH_ADDR_IN=$(cast wallet address --private-key "$HH_KEY_IN")
HH_KEY_OUT="${EEZ_SEQUENTIAL_OUT_KEY:-0x$(openssl rand -hex 32)}"
HH_ADDR_OUT=$(cast wallet address --private-key "$HH_KEY_OUT")

_yaml() { grep -E "^[[:space:]]*$1:" "${KURTOSIS_ARGS_FILE:-$K/args.yaml}" 2>/dev/null | head -1 \
    | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; }
FUND_FROM_KEY="${EEZ_FUND_FROM_KEY:-${EEZ_PROOF_SIGNER_KEY:-$(_yaml proof_signer_key)}}"
[[ -n "$FUND_FROM_KEY" ]] || { echo "could not resolve a funding key — set EEZ_FUND_FROM_KEY or check $K/args.yaml"; exit 1; }
L1_SETUP_KEY="${EEZ_L1_SETUP_KEY:-$FUND_FROM_KEY}"
EEZL2_ADDRESS="${EEZL2_ADDRESS:-0x4200000000000000000000000000000000000007}"
MAINNET_RID="${EEZ_L1_ROLLUP_ID:-0}"

echo "════════════════════════════════════════════════════════════════"
echo " SEQUENTIAL BUNDLE STATE TEST (kurtosis)"
echo "════════════════════════════════════════════════════════════════"
echo "    L1 (shared)  = $L1"
echo "    L2           = $L2"
echo "    L1 front     = $L1F   (Inbound)"
echo "    L2 front     = $L2F   (Outbound)"
echo "    registry     = $EEZ_REGISTRY_ADDRESS  rollupId=$EEZ_ROLLUP_ID"
echo "    users        = inbound:$HH_ADDR_IN outbound:$HH_ADDR_OUT"

retry() {
    local n=0 max="${RETRY_MAX:-6}" delay="${RETRY_DELAY:-3}" out rc
    while :; do
        if out=$("$@" 2>&1); then
            printf '%s' "$out"
            return 0
        else
            rc=$?
        fi
        (( ++n >= max )) && { echo "retry: '$*' failed after $n attempts: $out" >&2; return "$rc"; }
        sleep "$delay"
    done
}

L1_UP=$(cast block-number --rpc-url "$L1" 2>/dev/null || echo "")
[[ -n "$L1_UP" ]] || { echo "L1 RPC $L1 not reachable — is the enclave up?"; exit 1; }
L2_UP=$(cast block-number --rpc-url "$L2" 2>/dev/null || echo "")
[[ -n "$L2_UP" ]] || { echo "L2 RPC $L2 not reachable"; exit 1; }
echo "    L1=$L1_UP L2=$L2_UP"

PRIORITY_GAS_PRICE="${EEZ_TEST_PRIORITY_GAS_PRICE_WEI:-1}"

gas_price_for() { # <rpc> → max fee in wei
    local rpc="$1" gp base_hex base minimum
    gp=$(cast gas-price --rpc-url "$rpc" 2>/dev/null || echo 1000000000)
    gp="${EEZ_TEST_GAS_PRICE_WEI:-$gp}"
    base_hex=$(cast block latest --field baseFeePerGas --rpc-url "$rpc" 2>/dev/null || echo 0)
    base=$(cast to-dec "$base_hex" 2>/dev/null || echo 0)
    minimum=$((2 * base + PRIORITY_GAS_PRICE))
    (( gp < minimum )) && gp="$minimum"
    echo "$gp"
}

fund_l1() { # <address>
    local to="$1" nonce
    nonce=$(retry cast nonce "$(cast wallet address --private-key "$FUND_FROM_KEY")" --rpc-url "$L1")
    cast send "$to" --value 10ether --private-key "$FUND_FROM_KEY" --nonce "$nonce" \
        --gas-price "$(gas_price_for "$L1")" \
        --priority-gas-price "$PRIORITY_GAS_PRICE" --rpc-url "$L1" >/dev/null
}

fund_l2() { # <address>
    local to="$1" nonce
    nonce=$(retry cast nonce "$HH_KEY_2_ADDR" --rpc-url "$L2")
    cast send "$to" --value 10ether --private-key "$HH_KEY_2" --nonce "$nonce" \
        --gas-price "$(gas_price_for "$L2")" \
        --priority-gas-price "$PRIORITY_GAS_PRICE" --rpc-url "$L2" >/dev/null
}

forge_deploy() { # <rpc> <key> <script:contract> <sig> <args...> → forge stdout
    local rpc="$1" key="$2" sc="$3" sig="$4" gas_price out; shift 4
    gas_price=$(gas_price_for "$rpc")
    if ! out=$(cd "$REPO/contracts" && forge script "script/$sc" --sig "$sig" "$@" \
        --rpc-url "$rpc" --broadcast --private-key "$key" --gas-price "$gas_price" --skip-simulation 2>&1); then
        printf '%s\n' "$out" >&2
        return 1
    fi
    printf '%s\n' "$out"
}

grab() { grep -oE "$1=0x[0-9a-fA-F]{40}" | head -1 | cut -d= -f2; }
refresh_node_log() { kurtosis service logs -a "$ENCLAVE" eez-node >"$NODE_LOG" 2>&1 || true; }
refresh_signer_log() { kurtosis service logs -a "$ENCLAVE" eez-proof-signer >"$SIGNER_LOG" 2>&1 || true; }
strip_ansi() { sed 's/\x1b\[[0-9;]*m//g'; }

create_l2_proxy() { # <target_on_L1> → proxy address
    local target="$1" proxy code nonce raw l2_chain_id
    l2_chain_id=$(cast chain-id --rpc-url "$L2")
    proxy=$(cast call "$EEZL2_ADDRESS" 'computeCrossChainProxyAddress(address,uint64)(address)' \
        "$target" "$MAINNET_RID" --rpc-url "$L2" | tr -d '[:space:]')
    code=$(cast code "$proxy" --rpc-url "$L2" 2>/dev/null || echo 0x)
    if [[ "$code" == "0x" || -z "$code" ]]; then
        nonce=$(retry cast nonce "$HH_KEY_2_ADDR" --rpc-url "$L2")
        raw=$(cast mktx --rpc-url "$L2" --chain-id "$l2_chain_id" --private-key "$HH_KEY_2" --nonce "$nonce" \
            --gas-limit 1500000 --gas-price "$(gas_price_for "$L2")" \
            "$EEZL2_ADDRESS" 'createCrossChainProxy(address,uint64)' "$target" "$MAINNET_RID")
        curl -s -X POST "$L2" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}" >/dev/null
        for _ in $(seq 1 30); do
            code=$(cast code "$proxy" --rpc-url "$L2" 2>/dev/null || echo 0x)
            [[ "$code" != "0x" && -n "$code" ]] && break
            sleep 1
        done
    fi
    [[ "$code" != "0x" && -n "$code" ]] || return 1
    echo "$proxy"
}

send_front() { # <front> <raw_tx>
    local front="$1" raw="$2" resp result
    resp=$(curl -s -X POST "$front" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}")
    result=$(jq -er '.result // error("missing result")' <<<"$resp" 2>/dev/null || true)
    if [[ -z "$result" ]]; then
        echo "    ✗ front rejected tx: $resp" >&2
        return 1
    fi
}

receipt_json() { # <hash> <rpc>
    curl --max-time 3 -s -X POST -H 'Content-Type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$1\"],\"id\":1}" \
        "$2" 2>/dev/null
}

receipt_status() { # <hash> <rpc> → "1" mined-ok, "0x0" reverted, "missing"
    local receipt status
    receipt=$(receipt_json "$1" "$2" || true)
    status=$(jq -r '.result.status // "missing"' <<<"$receipt" 2>/dev/null || echo missing)
    [[ "$status" == "0x1" ]] && echo 1 || echo "${status:-missing}"
}

receipt_block() { # <hash> <rpc> → block number in hex
    local receipt block
    receipt=$(receipt_json "$1" "$2") || return 1
    block=$(jq -er '.result.blockNumber // error("receipt missing block number")' <<<"$receipt") || return 1
    echo "$block"
}

wait_for_sync_boundary() {
    local baseline deadline count=0
    refresh_node_log
    baseline=$(strip_ansi <"$NODE_LOG" | grep -Fc 'compose_sync_slot invoked' || true)
    deadline=$((SECONDS + ${EEZ_SYNC_BOUNDARY_WAIT_SECS:-60}))
    while (( SECONDS < deadline )); do
        refresh_node_log
        count=$(strip_ansi <"$NODE_LOG" | grep -Fc 'compose_sync_slot invoked' || true)
        (( count > baseline )) && return 0
        sleep 1
    done
    echo "timed out waiting for a Sync-slot boundary" >&2
    return 1
}

SIGNED_WINDOW_EVENT='event_name="eez.proof_signer.window_signed"'
REMOTE_ATTESTATION_EVENT='event_name="eez.prover_client.attested"'
BUNDLE_OBSERVED_EVENT='event_name="eez.composer.bundle.observed"'

run_direction() { # <inbound|outbound> <source_rpc> <front> <key> <address> <proxy> <target> <target_rpc>
    local direction="$1" source_rpc="$2" front="$3" key="$4" sender="$5"
    local proxy="$6" target="$7" target_rpc="$8"
    local node_evidence="${NODE_LOG%.log}-$direction-evidence.log"
    local signer_evidence="${SIGNER_LOG%.log}-$direction-evidence.log"
    local node_baseline signer_baseline chain_id nonce gas_price raw hash
    local built_line="" parent_block l2_block receipt_blocks event_blocks
    local wait_deadline confirmed=0 event_count=0 logs='[]' failure_line
    local settlement_deadline settled_line="" attested_hash signer_line
    local final_value root_deadline root_matched=0 l1_root l1_recheck safe_block l2_safe l2_root
    local tx_hashes=()

    echo
    echo "==> $direction: waiting for a fresh Sync-slot boundary"
    wait_for_sync_boundary
    refresh_signer_log
    node_baseline=$(wc -l <"$NODE_LOG")
    signer_baseline=$(wc -l <"$SIGNER_LOG")
    chain_id=$(cast chain-id --rpc-url "$source_rpc")
    nonce=$(retry cast nonce "$sender" --rpc-url "$source_rpc")
    gas_price=$(gas_price_for "$source_rpc")

    echo "==> $direction: submitting three setValue(1) calls to one proxy"
    for _ in 1 2 3; do
        raw=$(cast mktx --chain-id "$chain_id" --private-key "$key" --nonce "$nonce" \
            --gas-limit 600000 --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
            "$proxy" 'setValue(uint256)' 1)
        [[ "$raw" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "mktx failed: $raw"; return 1; }
        hash=$(cast keccak "$raw")
        tx_hashes+=("$hash")
        send_front "$front" "$raw"
        nonce=$((nonce + 1))
    done
    echo "    all three transactions accepted by the $direction front"

    wait_deadline=$((SECONDS + ${EEZ_SYNC_BUILD_WAIT_SECS:-60}))
    while (( SECONDS < wait_deadline )); do
        refresh_node_log
        sed -n "$((node_baseline + 1)),\$p" "$NODE_LOG" >"$node_evidence"
        built_line=$(strip_ansi <"$node_evidence" | grep -F 'built Sync block carrying' \
            | grep -E 'tx_count=3([^0-9]|$)' | tail -1 || true)
        [[ -n "$built_line" ]] && break
        sleep 1
    done
    [[ -n "$built_line" ]] || { echo "$direction: no Sync block with tx_count=3 observed"; return 1; }
    parent_block=$(grep -oE 'parent_number=[0-9]+' <<<"$built_line" | tail -1 | cut -d= -f2 || true)
    [[ -n "$parent_block" ]] || { echo "$direction: composer Sync parent missing from log"; return 1; }
    l2_block=$((parent_block + 1))
    echo "    ✓ $direction calls composed into L2 Sync block $l2_block"

    wait_deadline=$((SECONDS + ${EEZ_SEQUENTIAL_BUNDLE_WAIT_SECS:-300}))
    while (( SECONDS < wait_deadline )); do
        confirmed=0
        for hash in "${tx_hashes[@]}"; do
            [[ "$(receipt_status "$hash" "$source_rpc")" == "1" ]] && confirmed=$((confirmed + 1))
        done
        logs=$(cast logs --address "$target" --from-block 0 --to-block latest \
            'ValueSet(address,uint256)' --rpc-url "$target_rpc" --json 2>/dev/null || echo '[]')
        event_count=$(jq -r 'if type == "array" then length else 0 end' <<<"$logs")
        (( confirmed == 3 && event_count == 3 )) && break

        refresh_node_log
        refresh_signer_log
        sed -n "$((node_baseline + 1)),\$p" "$NODE_LOG" >"$node_evidence"
        sed -n "$((signer_baseline + 1)),\$p" "$SIGNER_LOG" >"$signer_evidence"
        if strip_ansi <"$signer_evidence" \
            | grep -Eq 'settling system transaction [0-9]+ reverted|top-level system transaction reverted'; then
            echo "$direction: proof signer rejected the sequential bundle" >&2
            return 1
        fi
        failure_line=$(strip_ansi <"$node_evidence" | grep -F "$BUNDLE_OBSERVED_EVENT" \
            | grep -F "sync_height=$l2_block" | grep -F 'settled=false' | tail -1 || true)
        [[ -z "$failure_line" ]] || { echo "$direction: bundle at Sync height $l2_block failed settlement"; return 1; }
        sleep 5
    done
    (( confirmed == 3 )) || { echo "$direction: only $confirmed/3 source transactions confirmed"; return 1; }
    (( event_count == 3 )) || { echo "$direction: found $event_count/3 ValueSet events"; return 1; }

    event_blocks=$(jq -r '[.[].blockNumber] | unique | .[]' <<<"$logs")
    [[ "$(wc -l <<<"$event_blocks" | tr -d ' ')" == "1" ]] \
        || { echo "$direction: ValueSet events landed in different target blocks: $event_blocks"; return 1; }
    if [[ "$direction" == inbound ]]; then
        [[ "$(cast to-dec "$event_blocks")" == "$l2_block" ]] \
            || { echo "inbound: target events did not land in Sync block $l2_block"; return 1; }
    else
        receipt_blocks=$(
            for hash in "${tx_hashes[@]}"; do
                printf '%s\n' "$(retry receipt_block "$hash" "$source_rpc")"
            done | sort -u
        )
        [[ "$(wc -l <<<"$receipt_blocks" | tr -d ' ')" == "1" \
            && "$(cast to-dec "$receipt_blocks")" == "$l2_block" ]] \
            || { echo "outbound: source transactions did not land together in Sync block $l2_block"; return 1; }
    fi
    echo "    ✓ all three $direction target calls executed in one block"

    final_value=$(retry cast call "$target" 'value()(uint256)' --rpc-url "$target_rpc")
    [[ "$final_value" == "1" ]] || { echo "$direction: Value.value is $final_value, expected 1"; return 1; }
    echo "    ✓ $direction target Value.value is 1"

    settlement_deadline=$((SECONDS + ${EEZ_SEQUENTIAL_SETTLEMENT_WAIT_SECS:-180}))
    while (( SECONDS < settlement_deadline )); do
        refresh_node_log
        sed -n "$((node_baseline + 1)),\$p" "$NODE_LOG" >"$node_evidence"
        settled_line=$(strip_ansi <"$node_evidence" | grep -F "$BUNDLE_OBSERVED_EVENT" \
            | grep -F "sync_height=$l2_block" | grep -F 'settled=true' | tail -1 || true)
        [[ -n "$settled_line" ]] && break
        sleep 3
    done
    [[ -n "$settled_line" ]] || { echo "$direction: bundle at Sync height $l2_block did not settle"; return 1; }
    echo "    ✓ $direction bundle at Sync height $l2_block settled"

    refresh_signer_log
    sed -n "$((signer_baseline + 1)),\$p" "$SIGNER_LOG" >"$signer_evidence"
    if strip_ansi <"$signer_evidence" \
        | grep -Eq 'settling system transaction [0-9]+ reverted|top-level system transaction reverted'; then
        echo "$direction: proof signer rejected sequential bundle execution" >&2
        return 1
    fi
    attested_hash=$(strip_ansi <"$node_evidence" | grep -F "$REMOTE_ATTESTATION_EVENT" \
        | grep -E "to=$l2_block([^0-9]|$)" \
        | grep -oE 'hash=0x[0-9a-fA-F]{64}' | tail -1 | cut -d= -f2 || true)
    [[ -n "$attested_hash" ]] || { echo "$direction: no accepted attestation observed"; return 1; }
    signer_line=$(strip_ansi <"$signer_evidence" | grep -F "$SIGNED_WINDOW_EVENT" \
        | grep -E "validated_to_block=$l2_block([^0-9]|$)" \
        | grep -F "recomputed_public_inputs_hash=$attested_hash" | tail -1 || true)
    [[ -n "$signer_line" ]] || { echo "$direction: no matching proof-signer validation found"; return 1; }
    echo "    ✓ proof signer accepted the $direction bundle window"

    root_deadline=$((SECONDS + ${EEZ_STATE_ROOT_WAIT_SECS:-30}))
    while (( SECONDS < root_deadline )); do
        l1_root=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
        safe_block=$(retry cast block safe --rpc-url "$L2" --json)
        l2_safe=$(jq -r '.number' <<<"$safe_block" | xargs cast to-dec)
        l2_root=$(jq -r '.stateRoot' <<<"$safe_block")
        l1_recheck=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
        if [[ "${l1_root,,}" == "${l1_recheck,,}" && "${l1_recheck,,}" == "${l2_root,,}" ]] \
            && (( l2_safe >= l2_block )); then
            root_matched=1
            break
        fi
        sleep 1
    done
    (( root_matched )) || { echo "$direction: L1/L2 roots did not converge at Sync height $l2_block"; return 1; }
    echo "    ✓ $direction roots converged at safe height $l2_safe"
}

echo "==> funding fresh source users"
fund_l1 "$HH_ADDR_IN"
fund_l2 "$HH_ADDR_OUT"

echo "==> deploying shared targets"
L2_VALUE=$(forge_deploy "$L2" "$HH_KEY_2" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab EEZ_VALUE_ADDRESS)
L1_VALUE=$(forge_deploy "$L1" "$L1_SETUP_KEY" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab EEZ_VALUE_ADDRESS)
[[ -n "$L2_VALUE" && -n "$L1_VALUE" ]] || { echo "Value target deployment failed"; exit 1; }
echo "    L2 Value=$L2_VALUE"
echo "    L1 Value=$L1_VALUE"

echo "==> creating cross-chain proxies"
IN_VALUE_PROXY=$(forge_deploy "$L1" "$L1_SETUP_KEY" CreateValueProxy.s.sol:CreateValueProxy \
    'run(address,address,uint64)' "$EEZ_REGISTRY_ADDRESS" "$L2_VALUE" "$EEZ_ROLLUP_ID" | grab EEZ_VALUE_PROXY)
OUT_VALUE_PROXY=$(create_l2_proxy "$L1_VALUE")
[[ -n "$IN_VALUE_PROXY" && -n "$OUT_VALUE_PROXY" ]] || { echo "proxy creation failed"; exit 1; }
echo "    inbound proxy=$IN_VALUE_PROXY"
echo "    outbound proxy=$OUT_VALUE_PROXY"

run_direction inbound "$L1" "$L1F" "$HH_KEY_IN" "$HH_ADDR_IN" "$IN_VALUE_PROXY" "$L2_VALUE" "$L2"
run_direction outbound "$L2" "$L2F" "$HH_KEY_OUT" "$HH_ADDR_OUT" "$OUT_VALUE_PROXY" "$L1_VALUE" "$L1"

echo
echo "==> SEQUENTIAL BUNDLE STATE TEST PASSED (inbound + outbound)"
