#!/usr/bin/env bash
# Drive a GENUINE partial consumption against the kurtosis rig.
#
# Two inbound cross-chain calls are queued per round from one sender:
#   direct — straight at the L1 inbound proxy; survives every L1 block
#   gated  — through ParityGate, which reverts on ODD L1 block numbers
# The composer simulates both against the ANCHOR block and the bundle lands one
# or more blocks later, so the gated call passes simulation and then reverts at
# inclusion. The submitter whitelists every user tx in `revertingTxHashes`, so
# the postBatch still lands, the gated entry goes unconsumed, and L1 halts at a
# prefix. Direct is queued first so that prefix is non-empty (anchor + direct),
# which is the settlement path rather than the anchor-only reorg path.
#
# This needs a relay that honours `revertingTxHashes`; anvil does not, which is
# why the e2e probe cannot reach this subject.
set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

K="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$K/.." && pwd)"
source "$K/scripts/lib.sh"
: "${L1:=http://127.0.0.1:8545}"
: "${L2:=http://127.0.0.1:18688}"
: "${L1F:=http://127.0.0.1:18999}"
ROUNDS="${EEZ_PARITY_ROUNDS:-10}"
SD="${EEZ_PARITY_OUT:-$REPO/datadir/smoke-logs}"; mkdir -p "$SD"
NODE_LOG="$SD/parity-node.log"

set -a; source "$REPO/deployments.env"; set +a
# Kurtosis prefunded account #0 — unused by the rig, so its nonce is never
# blocked behind a stuck composer or harness transaction.
FUND_KEY="${EEZ_FUND_FROM_KEY:-0xbcdf20249abf0ed6d944c0288fad489e33f66b3960d9e6229c1cd214ed3bbe31}"
: "${L1_FUND_RPC:=http://172.16.0.10:8545}"

L1_CHAIN_ID=$(cast chain-id --rpc-url "$L1")
HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
GAS=$(gas_price_for "$L1")

refresh_log() { docker logs eez-node-kurtosis >"$NODE_LOG" 2>&1 || true; }
count() { refresh_log; grep -c "$1" "$NODE_LOG" 2>/dev/null || true; }

echo "════════════════════════════════════════════════════════════"
echo " PARITY GATE — partial consumption (rounds=$ROUNDS)"
echo "════════════════════════════════════════════════════════════"

# The rig runs a prebuilt image. A stale one silently tests the PARENT branch:
# without `revertingTxHashes` on the wire the builder drops every bundle holding
# a reverting tx, so no prefix can ever form and the run fails for the wrong
# reason. Refuse to start unless the binary carries this branch's markers.
BIN=/tmp/eez-composer-under-test
docker cp eez-node-kurtosis:/usr/local/bin/eez-composer "$BIN" >/dev/null 2>&1 \
    || { echo "cannot read the running composer binary"; exit 1; }
# Extract once, then grep the FILE: `strings | grep -q` makes strings die of
# SIGPIPE on a match, and under `pipefail` that reads as failure.
strings "$BIN" > "$BIN.syms"
missing=""
for sym in revertingTxHashes EEZ_PARTIAL_CONSUMPTION eez.composer.recovery.settled_short; do
    grep -qF -- "$sym" "$BIN.syms" || missing="$missing $sym"
done
rm -f "$BIN" "$BIN.syms"
[[ -z "$missing" ]] || {
    echo "STALE IMAGE — the running node predates this branch; missing:$missing"
    echo "  rebuild with: docker build -f Dockerfile -t eez-node:local ."
    exit 1
}
echo "==> image carries the partial-consumption code"

# Two persistent senders, one per call. A gated call composed on an ODD anchor
# is evicted at compose time along with its nonce chain, so the on-chain nonce
# does not move and the next round reuses it — no permanent gap. Separate
# senders keep a gated eviction from taking the direct call down with it.
DKEY="0x$(openssl rand -hex 32)"; DADDR=$(cast wallet address --private-key "$DKEY")
GKEY="0x$(openssl rand -hex 32)"; GADDR=$(cast wallet address --private-key "$GKEY")
# Fund through the BUILDER's own RPC: el-1 holds txs that never reach el-2, and
# el-2 is the node that builds blocks.
FN=$(cast nonce "$(cast wallet address --private-key "$FUND_KEY")" --rpc-url "$L1_FUND_RPC")
cast send --rpc-url "$L1_FUND_RPC" --private-key "$FUND_KEY" --nonce "$FN"       --gas-price "$GAS" --async --value 20ether "$DADDR" >/dev/null
cast send --rpc-url "$L1_FUND_RPC" --private-key "$FUND_KEY" --nonce "$((FN+1))" --gas-price "$GAS" --async --value 20ether "$GADDR" >/dev/null
for _ in $(seq 1 60); do
    [[ "$(cast balance "$GADDR" --rpc-url "$L1")" != "0" ]] && break
    sleep 3
done
[[ "$(cast balance "$GADDR" --rpc-url "$L1")" != "0" ]] || { echo "funding did not land"; exit 1; }
echo "==> senders funded: direct=$DADDR gated=$GADDR"

# L2 target + its L1 inbound proxy — the call the gate forwards to.
L2_VALUE=$(cd "$REPO/contracts" && forge script script/DeployValueL2.s.sol:DeployValueL2 --sig 'run(uint256)' 0 \
    --rpc-url "$L2" --broadcast --private-key "$HH_KEY_2" --skip-simulation 2>&1 \
    | grep -oE 'EEZ_VALUE_ADDRESS=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$L2_VALUE" ]] || { echo "L2 Value deploy failed"; exit 1; }
PROXY=$(cd "$REPO/contracts" && forge script script/CreateValueProxy.s.sol:CreateValueProxy \
    --sig 'run(address,address,uint64)' "$EEZ_REGISTRY_ADDRESS" "$L2_VALUE" "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_FUND_RPC" --broadcast --private-key "$FUND_KEY" --gas-price "$GAS" --skip-simulation 2>&1 \
    | grep -oE 'EEZ_VALUE_PROXY=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
[[ -n "$PROXY" ]] || { echo "inbound proxy creation failed"; exit 1; }
echo "==> L2 Value=$L2_VALUE  inbound proxy=$PROXY"

# ParityGate(target=proxy) on L1, straight from the forge artifact.
GATE_BIN=$(jq -r '.bytecode.object' "$REPO/contracts/out/ParityGate.sol/ParityGate.json")
GATE=$(cast send --rpc-url "$L1_FUND_RPC" --private-key "$FUND_KEY" --gas-price "$GAS" --json \
    --create "${GATE_BIN}$(cast abi-encode 'c(address)' "$PROXY" | sed 's/^0x//')" \
    | jq -r '.contractAddress')
[[ "$GATE" =~ ^0x[0-9a-fA-F]{40}$ ]] || { echo "ParityGate deploy failed: $GATE"; exit 1; }
echo "==> ParityGate=$GATE (reverts on odd L1 blocks, forwards to $PROXY)"

BASE_SHORT=$(count 'eez.composer.recovery.settled_short')
BASE_PARTIAL=$(count 'eez.deriver.reconcile.partial_consumption')
BASE_BURNED=$(count 'eez.composer.recovery.nonce_burned')
BASE_DIVERGED=$(refresh_log; grep -cE 'eez\.deriver\.state\.diverged_(pre|post)' "$NODE_LOG" || true)
echo "==> baselines short=$BASE_SHORT partial=$BASE_PARTIAL burned=$BASE_BURNED diverged=$BASE_DIVERGED"
echo

for r in $(seq 1 "$ROUNDS"); do
    # Direct first: it survives every parity, so the surviving prefix is
    # non-empty — a gated-first order strands the direct entry too and yields an
    # anchor-only settlement, which is the reorg path rather than the prefix one.
    DIRECT=$(cast mktx --rpc-url "$L1" --chain-id "$L1_CHAIN_ID" --private-key "$DKEY" \
        --nonce "$(cast nonce "$DADDR" --rpc-url "$L1")" \
        --gas-limit 900000 --gas-price "$GAS" "$PROXY" 'setValue(uint256)' "$((100 + r))")
    GATED=$(cast mktx --rpc-url "$L1" --chain-id "$L1_CHAIN_ID" --private-key "$GKEY" \
        --nonce "$(cast nonce "$GADDR" --rpc-url "$L1")" \
        --gas-limit 900000 --gas-price "$GAS" "$GATE" 'setValue(uint256)' "$((200 + r))")
    # `send_front` waits out the front's startup backoff and fails LOUDLY on a
    # rejection; swallowing the response would report "no prefix" for a tx that
    # was never accepted.
    send_front "$L1F" "$DIRECT" "$(cast keccak "$DIRECT")" || exit 1
    send_front "$L1F" "$GATED" "$(cast keccak "$GATED")" || exit 1
    sleep 18
    S=$(count 'eez.composer.recovery.settled_short')
    P=$(count 'eez.deriver.reconcile.partial_consumption')
    B=$(count 'eez.composer.recovery.nonce_burned')
    printf '  round %2d/%s  short=%s partial=%s burned=%s  L1=%s\n' \
        "$r" "$ROUNDS" "$((S - BASE_SHORT))" "$((P - BASE_PARTIAL))" "$((B - BASE_BURNED))" \
        "$(cast block-number --rpc-url "$L1")"
done

echo
sleep 20
SHORT=$(( $(count 'eez.composer.recovery.settled_short') - BASE_SHORT ))
PARTIAL=$(( $(count 'eez.deriver.reconcile.partial_consumption') - BASE_PARTIAL ))
BURNED=$(( $(count 'eez.composer.recovery.nonce_burned') - BASE_BURNED ))
refresh_log
DIVERGED=$(( $(grep -cE 'eez\.deriver\.state\.diverged_(pre|post)' "$NODE_LOG" || true) - BASE_DIVERGED ))
FORWARDED=$(cast logs --rpc-url "$L1" --from-block 1 --address "$GATE" 2>/dev/null | grep -c 'blockNumber' || true)

ok=1
echo "──────────── RESULTS ────────────"
(( SHORT   > 0 )) && echo "  ✓ composer observed $SHORT short settlement(s)" || { echo "  ✗ no short settlement observed"; ok=0; }
(( PARTIAL > 0 )) && echo "  ✓ deriver truncated to the consumed prefix $PARTIAL time(s)" || { echo "  ✗ deriver never took the partial-consumption path"; ok=0; }
(( BURNED  > 0 )) && echo "  ✓ $BURNED reverted-with-receipt nonce(s) burned, not requeued" || echo "  ℹ no nonce-burn recorded"
(( DIVERGED == 0 )) && echo "  ✓ zero state divergence" || { echo "  ✗ $DIVERGED divergence event(s)"; ok=0; }
echo "  ℹ ParityGate Forwarded events (even-block passes): $FORWARDED"

# The decisive reconcile: L1's retained commitment must name a real L2 block and
# that block must be L2's safe head.
# Retry: a short settlement rolls the optimistic block back and re-derives, so
# L1's commitment runs a Sync slot ahead of L2's safe marker for a few seconds.
# A one-shot read reports that lag as a mismatch.
matched=0
deadline=$((SECONDS + ${EEZ_RECONCILE_WAIT_SECS:-90}))
while (( SECONDS < deadline )); do
    C=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
    SAFE=$(cast block safe --rpc-url "$L2" --json)
    SAFE_HASH=$(jq -r '.hash' <<<"$SAFE"); SAFE_NUM=$(jq -r '.number' <<<"$SAFE" | xargs cast to-dec)
    [[ "${C,,}" == "${SAFE_HASH,,}" ]] && { matched=1; break; }
    sleep 3
done
if (( matched )); then
    echo "  ✓ L1 commitment == L2 safe block hash at height $SAFE_NUM"
else
    echo "  ✗ L1 commitment $C != L2 safe block hash $SAFE_HASH at height $SAFE_NUM"
    # A commitment naming no L2 block is divergence; naming a canonical one is lag.
    if cast block "$C" --rpc-url "$L2" >/dev/null 2>&1; then
        echo "    (commitment IS a canonical L2 block — L2 safe had not caught up)"
    else
        echo "    (commitment names NO L2 block — real divergence)"
    fi
    ok=0
fi
echo
(( ok )) && echo "==> PARITY GATE TEST PASSED" || echo "==> PARITY GATE TEST FAILED"
(( ok ))
