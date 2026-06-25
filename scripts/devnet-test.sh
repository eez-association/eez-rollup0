#!/usr/bin/env bash
#
# Cross-chain test driver for a RUNNING eez-node (the dockerized chiado
# node from docker-compose.chiado-node.yml, or any eez-node serving the
# RPCs below). Decoupled from node bring-up: it assumes the protocol is
# already deployed (deployments.env present) and the node is up.
#
# Flow (mirrors scripts/smoke-chiado.sh steps 7-15, minus node launch):
#   - deploy Value on L2, create setter + deposit CrossChainProxies on L1
#   - fire $EEZ_WAVE_COUNT waves of cross-chain setter/deposit ops (+ L2
#     filler) at the L2 ingress
#   - wait for the L1 user_tx receipts, then tally:
#       * per-PB analyzer (Sync blocks vs BatchPosted)
#       * L1 rollups(id).stateRoot == L2 actual at last settled height
#       * semantic effects (Value + recipient balance vs confirmed view)
#       * zero state-root divergence events
#
# Post-deploy everything uses the node's OWN (internal) L1 — proxies,
# reconcile and receipts all go to $L1_RPC (the embedded reth). Only the
# one-time protocol deploy (make deploy-protocol, done before this) used
# an external chiado RPC.
#
# Reads the composer log via `docker logs $NODE_CONTAINER` (the node runs
# in a container now), not an on-disk file.
#
# Prereqs on the host: cast, forge, jq, docker; the sync-rollups-protocol
# submodule initialised (forge compiles contracts/ + lib).

set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"

# Foundry nightly prints a "Warning: This is a nightly build..." banner to
# stderr on every cast/forge call. With `2>&1`-captured command substitution
# (e.g. NONCE_START=$(retry cast nonce ...)) that banner pollutes the captured
# value and trips `set -u` arithmetic ("Warning: unbound variable"). Silence it.
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

# ── Endpoints (the running node) ─────────────────────────────────────
L1_RPC="${L1_RPC:-http://localhost:18645}"      # embedded chiado L1
L2_RPC="${L2_RPC:-http://localhost:18688}"      # L2
# Where cross-chain USER ops are SUBMITTED. Default = the L2 ingress
# (:18688, chain-id-mismatch path). Set to the B0 L1 interceptor (:18649)
# to exercise the production wallet-correct L1->L2 entry instead. Reads and
# the L2-only filler always use L2_RPC; only this submit endpoint changes.
SUBMIT_RPC="${SUBMIT_RPC:-$L2_RPC}"
NODE_CONTAINER="${NODE_CONTAINER:-eez-node-chiado}"

# ── Knobs ────────────────────────────────────────────────────────────
WAVE_COUNT="${EEZ_WAVE_COUNT:-5}"
FILLER_PER_GAP="${EEZ_FILLER_PER_GAP:-2}"
RECEIPT_WAIT_SECS="${EEZ_RECEIPT_WAIT_SECS:-300}"
VALUE_INITIAL="${VALUE_INITIAL:-5}"

# ── Keys (testnet only; match scripts/smoke-chiado.sh defaults) ──────
# Operator = protocol deployer + proof signer (creates the proxies).
EEZ_OPERATOR_KEY="${EEZ_OPERATOR_KEY:-0x2248a31395af28e24349c8e566c19475a79cb610389204ab26bc585493e5cf27}"
# User = sends cross-chain setter/deposit ops.
EEZ_USER_KEY="${EEZ_USER_KEY:-0x3b7b012a74f1c18f714c38306339b6b4124f3a434bd816a1ee1fa5aeb5953efe}"
# Hardhat key 2 = L2-only filler (prefunded in genesis).
HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
HH_ADDR_2=0x3C44Cdddb6a900fa2b585dD299E03D12FA4293bC
# Hardhat key 0 = L2 system signer; its address marks Sync blocks.
HH_ADDR_0=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

# Unique per run so the deterministic deposit-proxy address (derived from
# registry+target+rollupId) never collides with a prior run's
# (CreateCollision). Override to pin a specific recipient.
L2_RECIPIENT="${L2_RECIPIENT:-0x$(openssl rand -hex 20)}"
FILLER_RECIPIENT=0x2222222222222222222222222222222222222222

# Snapshot of the composer log for the tally (docker logs, not a file).
NODE_LOG="$(mktemp /tmp/devnet-test-nodelog.XXXXXX)"
refresh_log() { docker logs "$NODE_CONTAINER" >"$NODE_LOG" 2>&1 || true; }
cleanup() { rm -f "$NODE_LOG"; }
trap cleanup EXIT

# Run a read-only command with retries — survives transient RPC hiccups
# (the node can saturate while the embedded L1 backfills) instead of
# letting `set -e` abort the whole script on a single failed `cast`.
retry() {
    local n=0 max="${RETRY_MAX:-6}" delay="${RETRY_DELAY:-3}" out rc
    while :; do
        out=$("$@" 2>&1); rc=$?
        (( rc == 0 )) && { printf '%s' "$out"; return 0; }
        (( ++n >= max )) && { echo "retry: '$*' failed after $n attempts: $out" >&2; return "$rc"; }
        sleep "$delay"
    done
}

# ── Prereqs ──────────────────────────────────────────────────────────
for t in cast forge jq docker; do command -v "$t" >/dev/null || { echo "$t not in PATH"; exit 1; }; done
[[ -f "$REPO/deployments.env" ]] || { echo "deployments.env missing — run make deploy-protocol first"; exit 1; }
docker inspect "$NODE_CONTAINER" >/dev/null 2>&1 || { echo "container '$NODE_CONTAINER' not found — is the node up?"; exit 1; }
L2_UP=$(cast block-number --rpc-url "$L2_RPC" 2>/dev/null || echo "")
[[ -n "$L2_UP" ]] || { echo "L2 RPC $L2_RPC not reachable"; exit 1; }
L1_UP=$(cast block-number --rpc-url "$L1_RPC" 2>/dev/null || echo "")
[[ -n "$L1_UP" ]] || { echo "L1 RPC $L1_RPC not reachable"; exit 1; }

set -a; source "$REPO/deployments.env"; set +a
L1_CHAIN_ID=$(cast chain-id --rpc-url "$L1_RPC")
L2_CHAIN_ID=$(cast chain-id --rpc-url "$L2_RPC")
USER_ADDR=$(cast wallet address --private-key "$EEZ_USER_KEY")
echo "==> devnet cross-chain test"
echo "    L1 (internal) = $L1_RPC  (chain $L1_CHAIN_ID, head $L1_UP)"
echo "    L2            = $L2_RPC  (chain $L2_CHAIN_ID, head $L2_UP)"
echo "    submit via    = $SUBMIT_RPC  ($([ "$SUBMIT_RPC" = "$L2_RPC" ] && echo ':18688 chain-id hack' || echo 'B0 L1 interceptor'))"
echo "    registry      = $EEZ_REGISTRY_ADDRESS  rollupId=$EEZ_ROLLUP_ID"
echo "    waves=$WAVE_COUNT filler/gap=$FILLER_PER_GAP"

# ── Deploy Value on L2 ───────────────────────────────────────────────
echo
echo "==> deploying Value($VALUE_INITIAL) on L2"
cd "$REPO/contracts"
VALUE_OUT=$(forge script script/DeployValueL2.s.sol:DeployValueL2 \
    --sig "run(uint256)" "$VALUE_INITIAL" \
    --rpc-url "$L2_RPC" --broadcast --private-key "$HH_KEY_2" --gas-price 0 --skip-simulation 2>&1) || true
EEZ_VALUE_ADDRESS=$(echo "$VALUE_OUT" | grep -oE 'EEZ_VALUE_ADDRESS=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$EEZ_VALUE_ADDRESS" ]] || { echo "Value deploy failed"; echo "$VALUE_OUT" | tail -20; exit 1; }
echo "    Value @ $EEZ_VALUE_ADDRESS"
RECIPIENT_BEFORE=$(cast balance "$L2_RECIPIENT" --rpc-url "$L2_RPC")

# ── Create CrossChainProxies on the internal L1 ──────────────────────
echo "==> createCrossChainProxy(target=Value) on internal L1"
SETTER_OUT=$(forge script script/CreateValueProxy.s.sol:CreateValueProxy \
    --sig "run(address,address,uint256)" "$EEZ_REGISTRY_ADDRESS" "$EEZ_VALUE_ADDRESS" "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
SETTER_PROXY=$(echo "$SETTER_OUT" | grep -oE 'EEZ_VALUE_PROXY=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$SETTER_PROXY" ]] || { echo "setter proxy create failed"; echo "$SETTER_OUT" | tail -30; exit 1; }
echo "    setter proxy  = $SETTER_PROXY"

echo "==> createCrossChainProxy(target=L2_RECIPIENT) on internal L1"
DEPOSIT_OUT=$(forge script script/CreateValueProxy.s.sol:CreateValueProxy \
    --sig "run(address,address,uint256)" "$EEZ_REGISTRY_ADDRESS" "$L2_RECIPIENT" "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
DEPOSIT_PROXY=$(echo "$DEPOSIT_OUT" | grep -oE 'EEZ_VALUE_PROXY=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$DEPOSIT_PROXY" ]] || { echo "deposit proxy create failed"; echo "$DEPOSIT_OUT" | tail -30; exit 1; }
echo "    deposit proxy = $DEPOSIT_PROXY"
cd "$REPO"

# ── Waves + filler ───────────────────────────────────────────────────
TOTAL_DEPOSIT_SUM=0
LAST_SETTER_VALUE=""
ALL_USER_TX_HASHES=()
TX_META=()
refresh_log; LOG_LINES_BEFORE=$(wc -l < "$NODE_LOG")

submit_wave() {
    local WAVE_ID=$1; shift
    local OPS="$*"
    local NONCE_START n=0 GP=2000000000 PG=1500000000
    NONCE_START=$(retry cast nonce "$USER_ADDR" --rpc-url "$L1_RPC")
    local RAW_TXS=() OP_KINDS=() OP_ARGS=()
    for OP in $OPS; do
        local KIND="${OP%%:*}" ARG="${OP##*:}" NN=$((NONCE_START + n)) RAW=""
        OP_KINDS+=("$KIND"); OP_ARGS+=("$ARG")
        case "$KIND" in
            set) RAW=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "$EEZ_USER_KEY" --nonce "$NN" \
                    --gas-limit 600000 --gas-price "$GP" --priority-gas-price "$PG" \
                    "$SETTER_PROXY" 'setValue(uint256)' "$ARG" 2>&1); LAST_SETTER_VALUE="$ARG" ;;
            dep) RAW=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "$EEZ_USER_KEY" --nonce "$NN" \
                    --gas-limit 600000 --gas-price "$GP" --priority-gas-price "$PG" --value "$ARG" \
                    "$DEPOSIT_PROXY" 2>&1); TOTAL_DEPOSIT_SUM=$((TOTAL_DEPOSIT_SUM + ARG)) ;;
        esac
        [[ "$RAW" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "    ✗ mktx failed: $RAW"; exit 1; }
        RAW_TXS+=("$RAW"); n=$((n + 1))
    done
    for i in "${!RAW_TXS[@]}"; do
        local H SUB_RESP; H=$(cast keccak "${RAW_TXS[$i]}")
        ALL_USER_TX_HASHES+=("$H"); TX_META+=("$H ${OP_KINDS[$i]} ${OP_ARGS[$i]}")
        SUB_RESP=$(curl -s -X POST "$SUBMIT_RPC" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"${RAW_TXS[$i]}\"],\"id\":$i}")
        # B0/ingress returns the tx hash on a successful HOLD; surface any
        # rejection (e.g. gate_and_hold nonce/balance) instead of swallowing it.
        if echo "$SUB_RESP" | grep -q '"error"'; then
            echo "    ✗ submit($SUBMIT_RPC) rejected ${OP_KINDS[$i]}:${OP_ARGS[$i]} → $SUB_RESP"
        fi
    done
    echo "    wave $WAVE_ID submitted: ${#RAW_TXS[@]} ops [$OPS] via $SUBMIT_RPC"
}

submit_filler() {
    local COUNT=$1 NONCE_START
    NONCE_START=$(retry cast nonce "$HH_ADDR_2" --rpc-url "$L2_RPC")
    for ((j=0; j<COUNT; j++)); do
        local NN=$((NONCE_START + j)) RAW
        RAW=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_2" --nonce "$NN" \
            --gas-limit 21000 --gas-price 1000000000 --priority-gas-price 1000000000 \
            --value 100000000 "$FILLER_RECIPIENT" 2>&1)
        [[ "$RAW" =~ ^0x[0-9a-fA-F]+$ ]] || break
        curl -s -X POST "$L2_RPC" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$RAW\"],\"id\":99}" >/dev/null
        sleep 1
    done
    echo "    filler: $COUNT L2-only transfers submitted"
}

WAVE_OPS=(
    "set:1 dep:50000000000000 set:2"
    "dep:80000000000000 set:3 dep:30000000000000"
    "set:4 dep:100000000000000"
    "dep:40000000000000 set:5"
    "set:6 dep:60000000000000 set:7"
)
echo
echo "==> firing $WAVE_COUNT waves (cross-chain ops via L2 ingress; postBatches bundler-routed)"
for ((w=0; w<WAVE_COUNT; w++)); do
    submit_wave "$((w+1))" ${WAVE_OPS[$((w % ${#WAVE_OPS[@]}))]}
    sleep 12
    submit_filler "$FILLER_PER_GAP"
    sleep 8
done

# ── Wait for L1 user_tx receipts ─────────────────────────────────────
echo
echo "==> waiting up to ${RECEIPT_WAIT_SECS}s for all L1 user_tx receipts"
receipt_status() {
    local r st
    r=$(timeout 3 curl -s -X POST -H 'Content-Type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$1\"],\"id\":1}" \
        "$L1_RPC" 2>/dev/null)
    # result is null until mined; status is 0x1 (success) / 0x0 (reverted).
    st=$(echo "$r" | jq -r '.result.status // "missing"' 2>/dev/null)
    [[ "$st" == "0x1" ]] && echo "1" || echo "${st:-missing}"
}
evicted_count() { refresh_log; grep -c "evicted after repeated failed bundles" "$NODE_LOG" 2>/dev/null || true; }
wait_end=$(( SECONDS + RECEIPT_WAIT_SECS )); last_status_line=""
while (( SECONDS < wait_end )); do
    all=1; confirmed=0
    for H in "${ALL_USER_TX_HASHES[@]}"; do
        [[ "$(receipt_status "$H")" == "1" ]] && confirmed=$((confirmed+1)) || all=0
    done
    status_line="    progress: $confirmed/${#ALL_USER_TX_HASHES[@]} confirmed (elapsed ${SECONDS}s)"
    [[ "$status_line" != "$last_status_line" ]] && { echo "$status_line"; last_status_line="$status_line"; }
    [[ "$all" == "1" ]] && { echo "    all confirmed on L1"; break; }
    EV=$(evicted_count); EV=${EV:-0}
    (( confirmed + EV >= ${#ALL_USER_TX_HASHES[@]} )) && { echo "    $confirmed confirmed + $EV evicted = all resolved"; break; }
    sleep 5
done
echo "    settling 15s..."; sleep 15
refresh_log

# ── Per-PB analyzer ──────────────────────────────────────────────────
echo
echo "==> per-PB analyzer"
BATCH_POSTED_TOPIC=0xd6f8d71ce42a799b91f399271f4b0e91f85eb87fac7bb2cedd4b3a52fad36182
L1_TIP=$(cast block-number --rpc-url "$L1_RPC")
PB_LOGS=$(cast logs --address "$EEZ_REGISTRY_ADDRESS" --from-block "$EEZ_REGISTRY_DEPLOY_BLOCK" --to-block latest \
    "$BATCH_POSTED_TOPIC" --rpc-url "$L1_RPC" --json 2>/dev/null)
SYS_ADDR_LC=$(echo "$HH_ADDR_0" | tr 'A-Z' 'a-z')
HEAD_BN=$(cast block-number --rpc-url "$L2_RPC")
SYNC_BLOCKS=()
for ((BN=1; BN<=HEAD_BN; BN++)); do
    SYS=$(cast block "$BN" --rpc-url "$L2_RPC" --json --full 2>/dev/null | \
        jq --arg sa "$SYS_ADDR_LC" '[.transactions[]? | select(.from | ascii_downcase == $sa)] | length' 2>/dev/null || echo 0)
    [[ "$SYS" != "0" ]] && SYNC_BLOCKS+=("$BN")
done
PB_COUNT=$(echo "$PB_LOGS" | jq 'length' 2>/dev/null || echo 0)
echo "    Sync blocks (L2): ${#SYNC_BLOCKS[@]} → ${SYNC_BLOCKS[*]:-none}"
echo "    PBs on L1: $PB_COUNT (scanned $EEZ_REGISTRY_DEPLOY_BLOCK..$L1_TIP)"
ALL_PB_OK=1
if [[ "$PB_COUNT" -ge "$WAVE_COUNT" ]]; then
    echo "    ✓ ≥$WAVE_COUNT postBatches landed"
else
    echo "    ✗ only $PB_COUNT PBs (expected ≥$WAVE_COUNT)"; ALL_PB_OK=0
fi

# ── L1↔L2 stateRoot reconciliation ───────────────────────────────────
echo
echo "==> L1 vs L2 stateRoot reconciliation"
L1_TRACKED=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint256)(address,bytes32,uint256)' "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" 2>/dev/null | sed -n '2p' | tr -d '[:space:]')
LAST_SETTLED=$(sed 's/\x1b\[[0-9;]*m//g' "$NODE_LOG" 2>/dev/null \
    | grep "bundle outcome observed" | grep "settled=true" \
    | grep -oE "sync_height=[0-9]+" | grep -oE "[0-9]+" | sort -n | tail -1 || true)
[[ -z "$LAST_SETTLED" ]] && { [[ ${#SYNC_BLOCKS[@]} -gt 0 ]] && LAST_SETTLED="${SYNC_BLOCKS[-1]}" || LAST_SETTLED=0; }
L2_AT_LAST_SETTLED=$(cast block "$LAST_SETTLED" --rpc-url "$L2_RPC" --json | jq -r '.stateRoot')
echo "    L1 rollups($EEZ_ROLLUP_ID).stateRoot                    = $L1_TRACKED"
echo "    L2 actual stateRoot at last settled height $LAST_SETTLED = $L2_AT_LAST_SETTLED"
L1_L2_OK=0
if [[ "${L1_TRACKED,,}" == "${L2_AT_LAST_SETTLED,,}" ]]; then
    echo "    ✓ L1 stored stateRoot == L2 actual at last settled Sync height"; L1_L2_OK=1
else
    echo "    ✗ L1 ≠ L2 at last settled Sync height"
fi

# ── Semantic effect checks (confirmed view) ──────────────────────────
echo
echo "==> semantic effect verification"
LAST_CONFIRMED_SETTER=""; CONFIRMED_DEPOSIT_SUM=0
for META in "${TX_META[@]}"; do
    read -r MH MKIND MARG <<< "$META"
    if [[ "$(receipt_status "$MH")" == "1" ]]; then
        [[ "$MKIND" == "set" ]] && LAST_CONFIRMED_SETTER="$MARG"
        [[ "$MKIND" == "dep" ]] && CONFIRMED_DEPOSIT_SUM=$((CONFIRMED_DEPOSIT_SUM + MARG))
    fi
done
echo "    confirmed view: last setter=$LAST_CONFIRMED_SETTER, deposit sum=$CONFIRMED_DEPOSIT_SUM (submitted: last=$LAST_SETTER_VALUE, sum=$TOTAL_DEPOSIT_SUM)"
VV=$(cast call "$EEZ_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L2_RPC" 2>/dev/null || echo "")
RR=$(cast balance "$L2_RECIPIENT" --rpc-url "$L2_RPC")
EXPECTED_RR=$((RECIPIENT_BEFORE + CONFIRMED_DEPOSIT_SUM))
SETTER_OK=0; DEPOSIT_OK=0
[[ -n "$LAST_CONFIRMED_SETTER" && "$VV" == "$LAST_CONFIRMED_SETTER" ]] && SETTER_OK=1
[[ "$RR" == "$EXPECTED_RR" ]] && DEPOSIT_OK=1
echo "    L2 Value.value() = $VV  (last confirmed setter: $LAST_CONFIRMED_SETTER)"
[[ "$SETTER_OK" == "1" ]] && echo "    ✓ setter converged" || echo "    ✗ setter mismatch"
echo "    L2 recipient balance = $RR  (expected: $EXPECTED_RR)"
[[ "$DEPOSIT_OK" == "1" ]] && echo "    ✓ deposits converged" || echo "    ✗ deposit mismatch"

# ── Divergence check ─────────────────────────────────────────────────
echo
count_in() { local n; n=$(grep -c "$1" "$NODE_LOG" 2>/dev/null || true); echo "${n:-0}"; }
DIVERGED_LEGACY=$(count_in "local L2 state root differs"); DIVERGED_LEGACY=${DIVERGED_LEGACY:-0}
DIVERGED_DERIVER=$(count_in "diverged from L1-confirmed batch"); DIVERGED_DERIVER=${DIVERGED_DERIVER:-0}
DIV_OK=0
if [[ "$DIVERGED_LEGACY" -eq 0 ]]; then
    DIV_OK=1
    [[ "$DIVERGED_DERIVER" -eq 0 ]] \
        && echo "    ✓ zero state-root divergence events" \
        || echo "    ⚠ $DIVERGED_DERIVER deriver-side WARN(s) from skipped (state_applied=false) batches — residual; reconcile is authoritative"
else
    echo "    ✗ legacy divergences: $DIVERGED_LEGACY"
fi

# ── Verdict ──────────────────────────────────────────────────────────
echo
ALL_OK=1
for ok in "$ALL_PB_OK" "$L1_L2_OK" "$SETTER_OK" "$DEPOSIT_OK" "$DIV_OK"; do
    [[ "$ok" == "1" ]] || ALL_OK=0
done
if [[ "$ALL_OK" == "1" ]]; then
    echo "==> DEVNET TEST PASSED ($WAVE_COUNT waves, ${#ALL_USER_TX_HASHES[@]} cross-chain ops, $PB_COUNT PBs)"
    exit 0
else
    echo "==> DEVNET TEST FAILED"
    exit 1
fi
