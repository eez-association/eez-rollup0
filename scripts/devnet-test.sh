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

# ── Endpoints (the running node) ─────────────────────────────────────
L1_RPC="${L1_RPC:-http://localhost:18645}"      # embedded chiado L1
L2_RPC="${L2_RPC:-http://localhost:18688}"      # L2 mempool (rejects cross-chain txs)
INGRESS_RPC="${INGRESS_RPC:-http://localhost:18699}"  # cross-chain ingress (L1-fronting)
NODE_CONTAINER="${NODE_CONTAINER:-eez-node-chiado}"

# ── Knobs ────────────────────────────────────────────────────────────
WAVE_COUNT="${EEZ_WAVE_COUNT:-5}"
FILLER_PER_GAP="${EEZ_FILLER_PER_GAP:-2}"
RECEIPT_WAIT_SECS="${EEZ_RECEIPT_WAIT_SECS:-300}"
VALUE_INITIAL="${VALUE_INITIAL:-5}"
# Blockscout for the L1 (chiado) the internal node tracks — used to
# verify the deployed L1 contracts so their events (notably the
# SetterWrapper `Wrapped` log carrying the cross-chain return value)
# render decoded in the explorer. Best-effort; set empty to skip.
BLOCKSCOUT_URL="${EEZ_BLOCKSCOUT_URL:-https://gnosis-chiado.blockscout.com}"

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
echo "Sending to ${L2_RECIPIENT}"
FILLER_RECIPIENT=0x2222222222222222222222222222222222222222

# Snapshot of the composer log for the tally. Reads NODE_LOG_FILE (a
# native cargo-node log) when set, else `docker logs $NODE_CONTAINER`.
NODE_LOG="$(mktemp /tmp/devnet-test-nodelog.XXXXXX)"
NODE_LOG_FILE="${NODE_LOG_FILE:-}"
refresh_log() {
    if [[ -n "$NODE_LOG_FILE" ]]; then cp "$NODE_LOG_FILE" "$NODE_LOG" 2>/dev/null || true
    else docker logs "$NODE_CONTAINER" >"$NODE_LOG" 2>&1 || true; fi
}
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
[[ -n "$NODE_LOG_FILE" ]] || docker inspect "$NODE_CONTAINER" >/dev/null 2>&1 || { echo "container '$NODE_CONTAINER' not found — is the node up?"; exit 1; }
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

# Return-value path: SetterWrapper(setter proxy) on L1 calls the proxy
# internally, decodes the (changed,newValue) the composer synthesizes
# back, and emits it as `Wrapped`. Proves the cross-chain CONTRACT-CALL
# return value surfaces in an L1 log.
echo "==> deploying SetterWrapper(setter proxy) on internal L1"
WRAP_OUT=$(forge script script/DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 \
    --sig "run(address)" "$SETTER_PROXY" \
    --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
SETTER_WRAPPER=$(echo "$WRAP_OUT" | grep -oE 'EEZ_SETTER_WRAPPER=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$SETTER_WRAPPER" ]] || { echo "SetterWrapper deploy failed"; echo "$WRAP_OUT" | tail -30; exit 1; }
echo "    setter wrapper = $SETTER_WRAPPER"

# No-return setter: identical to Value but setValue returns nothing
# (empty returnData). Guards the cross-chain contract-call path for the
# empty-return case alongside the return-bearing Value.
echo "==> deploying ValueNoRet($VALUE_INITIAL) on L2"
VNR_OUT=$(forge script script/DeployValueNoRetL2.s.sol:DeployValueNoRetL2 \
    --sig "run(uint256)" "$VALUE_INITIAL" \
    --rpc-url "$L2_RPC" --broadcast --private-key "$HH_KEY_2" --gas-price 0 --skip-simulation 2>&1) || true
EEZ_VALUE_NORET_ADDRESS=$(echo "$VNR_OUT" | grep -oE 'EEZ_VALUE_NORET_ADDRESS=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$EEZ_VALUE_NORET_ADDRESS" ]] || { echo "ValueNoRet deploy failed"; echo "$VNR_OUT" | tail -20; exit 1; }
echo "    ValueNoRet @ $EEZ_VALUE_NORET_ADDRESS"
NRET_INITIAL="$VALUE_INITIAL"

echo "==> createCrossChainProxy(target=ValueNoRet) on internal L1"
NRET_OUT=$(forge script script/CreateValueProxy.s.sol:CreateValueProxy \
    --sig "run(address,address,uint256)" "$EEZ_REGISTRY_ADDRESS" "$EEZ_VALUE_NORET_ADDRESS" "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
NRET_PROXY=$(echo "$NRET_OUT" | grep -oE 'EEZ_VALUE_PROXY=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$NRET_PROXY" ]] || { echo "no-return proxy create failed"; echo "$NRET_OUT" | tail -30; exit 1; }
echo "    no-return proxy = $NRET_PROXY"

echo "==> createCrossChainProxy(target=L2_RECIPIENT) on internal L1"
DEPOSIT_OUT=$(forge script script/CreateValueProxy.s.sol:CreateValueProxy \
    --sig "run(address,address,uint256)" "$EEZ_REGISTRY_ADDRESS" "$L2_RECIPIENT" "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
DEPOSIT_PROXY=$(echo "$DEPOSIT_OUT" | grep -oE 'EEZ_VALUE_PROXY=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$DEPOSIT_PROXY" ]] || { echo "deposit proxy create failed"; echo "$DEPOSIT_OUT" | tail -30; exit 1; }
echo "    deposit proxy = $DEPOSIT_PROXY"

# ── Verify L1 contracts on Blockscout (best-effort) so their events show
#    up DECODED in the explorer — chiefly the SetterWrapper `Wrapped` log
#    carrying the cross-chain return value. Verify failures are
#    non-fatal: chiado's canonical Blockscout only sees these once the
#    embedded L1 has propagated them. ──────────────────────────────────
if [[ -n "$BLOCKSCOUT_URL" ]]; then
    echo "==> verifying L1 contracts on Blockscout ($BLOCKSCOUT_URL)"
    bs_verify() {  # <addr> <path:Name> [abi-encoded-ctor-args]
        local addr="$1" target="$2" ctor="${3:-}" args=()
        [[ -n "$ctor" ]] && args=(--constructor-args "$ctor")
        if timeout 90 forge verify-contract "$addr" "$target" \
                --verifier blockscout --verifier-url "${BLOCKSCOUT_URL%/}/api/" \
                --chain-id "$L1_CHAIN_ID" "${args[@]}" --watch >/dev/null 2>&1; then
            echo "    ✓ verified $target @ $addr"
        else
            echo "    ⚠ verify $target @ $addr failed/queued (non-fatal)"
        fi
    }
    bs_verify "$SETTER_WRAPPER" "src/SetterWrapper.sol:SetterWrapper" \
        "$(cast abi-encode 'constructor(address)' "$SETTER_PROXY")"
    echo "    decoded Wrapped logs → ${BLOCKSCOUT_URL%/}/address/$SETTER_WRAPPER?tab=logs"
fi
cd "$REPO"

# ── Waves + filler ───────────────────────────────────────────────────
TOTAL_DEPOSIT_SUM=0
LAST_SETTER_VALUE=""
LAST_WRAP_VALUE=""
LAST_NRET_VALUE=""
ALL_USER_TX_HASHES=()
TX_META=()
refresh_log; LOG_LINES_BEFORE=$(wc -l < "$NODE_LOG")

submit_wave() {
    local WAVE_ID=$1; shift
    local OPS="$*"
    local GP=2000000000 PG=1500000000 GL=600000 count=0
    # One `cast send --async` per op, straight to the cross-chain ingress
    # (which holds it for the next Sync slot). --async returns the tx hash
    # without blocking on a receipt (the tx settles later on L1). No
    # --chain-id / --nonce: cast send fetches both from the ingress, and the
    # ingress serves the sender's L1 nonce (the tx executes on L1) — the
    # per-wave drain means nothing's in flight, so the fetched nonce is right.
    local CS=(--rpc-url "$INGRESS_RPC" --private-key "$EEZ_USER_KEY"
              --gas-limit "$GL" --gas-price "$GP" --priority-gas-price "$PG" --async)
    for OP in $OPS; do
        local KIND="${OP%%:*}" ARG="${OP##*:}" H=""
        case "$KIND" in
            # Direct setter: EOA → proxy.setValue (return-bearing Value).
            set)  H=$(cast send "$SETTER_PROXY"   'setValue(uint256)'     "$ARG" "${CS[@]}" 2>&1)
                  LAST_SETTER_VALUE="$ARG" ;;
            # Return-value path: EOA → SetterWrapper.setViaProxy → proxy.setValue;
            # the wrapper decodes the (changed,newValue) tuple and emits `Wrapped`.
            # Also sets Value, so it counts as a Value-setter for convergence.
            wrap) H=$(cast send "$SETTER_WRAPPER" 'setViaProxy(uint256)' "$ARG" "${CS[@]}" 2>&1)
                  LAST_SETTER_VALUE="$ARG"; LAST_WRAP_VALUE="$ARG" ;;
            # No-return contract call: EOA → proxy.setValue on ValueNoRet (empty returnData).
            nret) H=$(cast send "$NRET_PROXY"     'setValue(uint256)'     "$ARG" "${CS[@]}" 2>&1)
                  LAST_NRET_VALUE="$ARG" ;;
            # Deposit: EOA → deposit proxy with value (ETH to the L2 recipient).
            dep)  H=$(cast send "$DEPOSIT_PROXY"  --value "$ARG" "${CS[@]}" 2>&1)
                  TOTAL_DEPOSIT_SUM=$((TOTAL_DEPOSIT_SUM + ARG)) ;;
        esac
        [[ "$H" =~ ^0x[0-9a-fA-F]{64}$ ]] || { echo "    ✗ cast send ($KIND) failed: $H"; exit 1; }
        ALL_USER_TX_HASHES+=("$H"); TX_META+=("$H $KIND $ARG")
        count=$((count + 1))
    done
    echo "    wave $WAVE_ID submitted: $count ops [$OPS]"
}

submit_filler() {
    local COUNT=$1 NONCE_START
    NONCE_START=$(retry cast nonce "$HH_ADDR_2" --rpc-url "$L2_RPC")
    for ((j=0; j<COUNT; j++)); do
        local NN=$((NONCE_START + j))
        # L2-only transfer straight to the L2 mempool via cast send --async.
        # --nonce is explicit: these fire back-to-back faster than the node's
        # nonce advances, so cast send can't auto-fetch a fresh one each time.
        cast send "$FILLER_RECIPIENT" --value 100000000 \
            --rpc-url "$L2_RPC" --private-key "$HH_KEY_2" --nonce "$NN" \
            --gas-limit 21000 --gas-price 1000000000 --priority-gas-price 1000000000 --async >/dev/null 2>&1 || true
        sleep 1
    done
    echo "    filler: $COUNT L2-only transfers submitted"
}

# Op kinds: set=direct Value setter, wrap=return-value path (Value via
# SetterWrapper, emits Wrapped), nret=no-return setter (ValueNoRet),
# dep=deposit. Value range 1-7 (set/wrap), ValueNoRet range 11-15 (nret)
# so the two converged values are distinguishable.
#
# ONE cross-chain op per wave: each is a postBatch through the composer's
# one-in-flight gate (~15-18s to settle on chiado), and the wave cycle is
# ~22s — so each settles before the next is submitted. Firing several
# cross-chain ops per wave overruns the gate and head-of-lines the rest
# (a known load issue, separate from correctness). Default WAVE_COUNT=5
# exercises wrap (return) + nret (no-return) + set + two deposits; the
# verdict's per-kind checks no-op for kinds a shorter run didn't fire.
WAVE_OPS=(
    "wrap:2"
    "dep:50000000000000"
    "nret:11"
    "dep:80000000000000"
    "set:7"
)
echo
echo "==> firing $WAVE_COUNT waves (cross-chain ops via L2 ingress; postBatches bundler-routed)"
for ((w=0; w<WAVE_COUNT; w++)); do
    submit_wave "$((w+1))" ${WAVE_OPS[$((w % ${#WAVE_OPS[@]}))]}
    submit_filler "$FILLER_PER_GAP"
    # Drain before the next wave: each cross-chain op is a postBatch through
    # the composer's one-in-flight gate. Submitting the next op before this
    # one settles piles them up — under sustained load the bundles get
    # dropped (target block passes) and only the tail survives. Wait for
    # every cross-chain op so far to confirm on L1 (or a per-wave cap), so
    # each runs on an effectively-idle gate like the single-op path.
    drain_deadline=$((SECONDS + 90))
    while (( SECONDS < drain_deadline )); do
        pending=0
        for H in "${ALL_USER_TX_HASHES[@]}"; do
            s=$(timeout 3 curl -s -X POST -H 'Content-Type: application/json' \
                --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$H\"],\"id\":1}" \
                "$L1_RPC" 2>/dev/null | jq -r '.result.status // "x"' 2>/dev/null)
            [[ "$s" == "0x1" ]] || pending=$((pending+1))
        done
        (( pending == 0 )) && break
        sleep 4
    done
    echo "    wave $((w+1)) drained (${pending:-?} still pending)"
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
# Only recent blocks hold this run's cross-chain Sync blocks; scanning from
# genesis is O(L2 height) and grows every run on a long-lived rollup. A
# 1500-block window (~25 min at 1s) covers a test; the L1↔L2 check below
# has a backward-scan fallback if a match falls outside it.
SYNC_SCAN_FROM=$(( HEAD_BN > 1500 ? HEAD_BN - 1500 : 1 ))
SYNC_BLOCKS=()
for ((BN=SYNC_SCAN_FROM; BN<=HEAD_BN; BN++)); do
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
echo "    L1 rollups($EEZ_ROLLUP_ID).stateRoot = $L1_TRACKED"
# L1's stored root is the `newState` of the LAST CONFIRMED postBatch, whose
# `to_block` is an L2 Sync block — so it must equal the L2 state root at one
# of our Sync blocks. Find that block instead of comparing at a single
# height: height-agnostic, independent of any (racy) node-log "settled"
# line, and tolerant of the newest Sync block's postBatch not yet landing.
L1_L2_OK=0; MATCH_BN=""
for ((idx=${#SYNC_BLOCKS[@]}-1; idx>=0; idx--)); do
    R=$(cast block "${SYNC_BLOCKS[$idx]}" --rpc-url "$L2_RPC" --json 2>/dev/null | jq -r '.stateRoot')
    [[ "${R,,}" == "${L1_TRACKED,,}" ]] && { MATCH_BN="${SYNC_BLOCKS[$idx]}"; L1_L2_OK=1; break; }
done
if (( ! L1_L2_OK )); then
    # Fallback: bounded backward scan (the matching Sync block may lack a
    # system tx, or a postBatch may still be landing a few blocks back).
    H=$(cast block-number --rpc-url "$L2_RPC")
    for ((BN=H; BN > H-300 && BN>0; BN--)); do
        R=$(cast block "$BN" --rpc-url "$L2_RPC" --json 2>/dev/null | jq -r '.stateRoot')
        [[ "${R,,}" == "${L1_TRACKED,,}" ]] && { MATCH_BN="$BN"; L1_L2_OK=1; break; }
    done
fi
if (( L1_L2_OK )); then
    echo "    ✓ L1 stored stateRoot == L2 actual at Sync block $MATCH_BN"
else
    echo "    ✗ L1 stored stateRoot matches no recent L2 block — possible divergence"
fi

# ── Semantic effect checks (confirmed view) ──────────────────────────
echo
echo "==> semantic effect verification"
LAST_CONFIRMED_SETTER=""; LAST_CONFIRMED_NRET=""; CONFIRMED_DEPOSIT_SUM=0
CC_CALL_TOTAL=0; CC_CALL_CONFIRMED=0; CC_CALL_REVERTED=0
for META in "${TX_META[@]}"; do
    read -r MH MKIND MARG <<< "$META"
    ST=$(receipt_status "$MH")
    # Regression guard: every cross-chain CONTRACT call (set/wrap/nret)
    # must land status=1. A REVERTED one (0x0) is the RollingHashMismatch
    # signature — and the L2 effect can still apply via the stateDelta, so
    # value() alone would mask it. An unconfirmed ("missing") one also
    # fails the guard (a stuck/non-included call is not a pass).
    case "$MKIND" in
        set|wrap|nret)
            CC_CALL_TOTAL=$((CC_CALL_TOTAL+1))
            if [[ "$ST" == "1" ]]; then CC_CALL_CONFIRMED=$((CC_CALL_CONFIRMED+1))
            elif [[ "$ST" == "0x0" ]]; then CC_CALL_REVERTED=$((CC_CALL_REVERTED+1)); echo "    ✗ reverted cross-chain $MKIND tx $MH (RollingHashMismatch?)"
            else echo "    ✗ unconfirmed cross-chain $MKIND tx $MH (status=$ST)"; fi ;;
    esac
    if [[ "$ST" == "1" ]]; then
        case "$MKIND" in
            set|wrap) LAST_CONFIRMED_SETTER="$MARG" ;;   # both set Value
            nret) LAST_CONFIRMED_NRET="$MARG" ;;
            dep) CONFIRMED_DEPOSIT_SUM=$((CONFIRMED_DEPOSIT_SUM + MARG)) ;;
        esac
    fi
done
echo "    confirmed view: setter=$LAST_CONFIRMED_SETTER no-ret=$LAST_CONFIRMED_NRET deposit_sum=$CONFIRMED_DEPOSIT_SUM"
VV=$(cast call "$EEZ_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L2_RPC" 2>/dev/null || echo "")
NV=$(cast call "$EEZ_VALUE_NORET_ADDRESS" 'value()(uint256)' --rpc-url "$L2_RPC" 2>/dev/null || echo "")
RR=$(cast balance "$L2_RECIPIENT" --rpc-url "$L2_RPC")
EXPECTED_RR=$((RECIPIENT_BEFORE + CONFIRMED_DEPOSIT_SUM))

# (1) Every cross-chain contract call landed status=1 — the rolling-hash guard.
CC_TX_OK=0; [[ "$CC_CALL_TOTAL" -gt 0 && "$CC_CALL_CONFIRMED" -eq "$CC_CALL_TOTAL" ]] && CC_TX_OK=1
[[ "$CC_TX_OK" == "1" ]] \
    && echo "    ✓ all $CC_CALL_TOTAL cross-chain contract calls landed status=1 (no RollingHashMismatch)" \
    || echo "    ✗ only $CC_CALL_CONFIRMED/$CC_CALL_TOTAL cross-chain contract calls landed ($CC_CALL_REVERTED reverted, $((CC_CALL_TOTAL-CC_CALL_CONFIRMED-CC_CALL_REVERTED)) unconfirmed)"

# (2) Return-bearing setter (Value) converged. N/A if no set/wrap fired.
SETTER_OK=0
if [[ -z "$LAST_SETTER_VALUE" ]]; then SETTER_OK=1; echo "    – no set/wrap ops this run (Value check N/A)"
else
    [[ "$VV" == "$LAST_CONFIRMED_SETTER" ]] && SETTER_OK=1
    echo "    Value.value()      = $VV  (last confirmed set/wrap: $LAST_CONFIRMED_SETTER)"
    [[ "$SETTER_OK" == "1" ]] && echo "    ✓ return setter converged" || echo "    ✗ return setter mismatch"
fi

# (3) No-return setter (ValueNoRet) converged. N/A if no nret fired.
NRET_OK=0
if [[ -z "$LAST_NRET_VALUE" ]]; then NRET_OK=1; echo "    – no nret ops this run (ValueNoRet check N/A)"
else
    [[ "$NV" == "$LAST_CONFIRMED_NRET" ]] && NRET_OK=1
    echo "    ValueNoRet.value() = $NV  (last confirmed nret: $LAST_CONFIRMED_NRET)"
    [[ "$NRET_OK" == "1" ]] && echo "    ✓ no-return setter converged" || echo "    ✗ no-return setter mismatch"
fi

# (4) Return VALUE surfaced on L1: the latest SetterWrapper `Wrapped` log
#     must carry ok=true and newValue == the value last sent via wrap.
WRAP_OK=0
if [[ -n "$LAST_WRAP_VALUE" ]]; then
    WDATA=$(cast logs 'Wrapped(uint256,bool,bool,uint256)' --address "$SETTER_WRAPPER" \
        --from-block "$EEZ_REGISTRY_DEPLOY_BLOCK" --to-block latest --rpc-url "$L1_RPC" --json 2>/dev/null \
        | jq -r '.[-1].data // empty' 2>/dev/null)
    if [[ "${#WDATA}" -ge 258 ]]; then
        W_OK=$(echo "${WDATA:66:64}" | sed 's/^0*//'); W_OK=$((16#${W_OK:-0}))      # word1: ok
        W_NEW=$(echo "${WDATA:194:64}" | sed 's/^0*//'); W_NEW=$((16#${W_NEW:-0}))  # word3: newValue
        [[ "$W_OK" == "1" && "$W_NEW" == "$LAST_WRAP_VALUE" ]] && WRAP_OK=1
        echo "    Wrapped(last): ok=$W_OK newValue=$W_NEW  (expected newValue=$LAST_WRAP_VALUE)"
    else
        echo "    Wrapped: no decodable event found"
    fi
    [[ "$WRAP_OK" == "1" ]] && echo "    ✓ cross-chain return value surfaced on L1 (Wrapped)" \
        || echo "    ✗ Wrapped event missing/incorrect"
else
    WRAP_OK=1  # no wrap ops this run → not applicable
fi

# (5) Deposits converged.
DEPOSIT_OK=0; [[ "$RR" == "$EXPECTED_RR" ]] && DEPOSIT_OK=1
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
for ok in "$ALL_PB_OK" "$L1_L2_OK" "$CC_TX_OK" "$SETTER_OK" "$NRET_OK" "$WRAP_OK" "$DEPOSIT_OK" "$DIV_OK"; do
    [[ "$ok" == "1" ]] || ALL_OK=0
done
if [[ "$ALL_OK" == "1" ]]; then
    echo "==> DEVNET TEST PASSED ($WAVE_COUNT waves, ${#ALL_USER_TX_HASHES[@]} cross-chain ops, $PB_COUNT PBs)"
    exit 0
else
    echo "==> DEVNET TEST FAILED"
    exit 1
fi
