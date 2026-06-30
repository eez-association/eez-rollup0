#!/usr/bin/env bash
#
# OUTBOUND (L2→L1) cross-chain test driver for a RUNNING eez-node (the
# dockerized chiado node from docker-compose.chiado-node.yml). The mirror
# of scripts/devnet-test.sh, which drives INBOUND (L1→L2).
#
# What it proves (the on-chain S2/S3 acceptance of crates/eez-node/tests/
# e2e_outbound.rs, against the LIVE stack):
#   1. deploy a `Value(0)` settlement TARGET on the embedded chiado L1
#      (:18645) — the L1 contract the cross-chain `setValue` mutates;
#   2. compute the L2 cross-chain PROXY  P = computeCrossChainProxyAddress(
#      L1_Value, MAINNET=0)  via the on-chain view on the L2 CCM
#      (0x4200..07) — byte-identical to eez-evm/src/outbound_gate.rs
#      `compute_cross_chain_proxy_address` (see explanation at EOF), then
#      `createCrossChainProxy` so the proxy bytecode exists on L2;
#   3. send the OUTBOUND user tx: an L2-chain-id EIP-1559 tx,
#      `to = P`, `data = setValue(N)`, signed by a funded L2 EOA, POSTed
#      to the L2 ingress (:18688) — the same shape `send_outbound_set_value`
#      builds in the in-process test;
#   4. WAIT for settlement: the L1 `Value.value() == N` (the cross-chain
#      call executed inside `postBatch` `_processNCalls`) AND
#      `rollups(rid).stateRoot` on L1 == the L2 safe-head state root (the
#      user-tx-inclusive settled root), AND zero state-root divergence in
#      the node log;
#   5. print a clear PASS/FAIL verdict.
#
# ── CRITICAL PRECONDITION (read before running) ──────────────────────
# The composer classifies a tx OUTBOUND iff `to ∈ EEZ_CROSS_CHAIN_PROXY_
# ADDRESSES` (eez-composer/src/ingress.rs::classify — the proxy set is the
# ONLY outbound trigger, parsed ONCE at startup from that env var into a
# static HashSet). The live docker-compose.chiado-node.yml sets only
# `EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS=10200` (INBOUND); it does NOT set
# `EEZ_CROSS_CHAIN_PROXY_ADDRESSES`, so the live node's outbound proxy set
# is EMPTY. A tx to a freshly-created proxy is therefore classified
# L2Only and mines as a plain L2 transfer — NO outbound settlement.
#
# This script computes P, then verifies P is in the live node's
# `EEZ_CROSS_CHAIN_PROXY_ADDRESSES`. If it is not, the script ABORTS with
# the exact remediation (add P to the env and restart the node) rather
# than firing a tx that silently does nothing. Set
# `OUTBOUND_SKIP_PROXY_ENV_CHECK=1` to bypass the check (e.g. you already
# restarted the node with P configured).
#
# Prereqs on the host: cast, forge, jq, docker; the sync-rollups-protocol
# submodule initialised; `forge build` artifacts present in contracts/out.

set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"

# Silence foundry's nightly banner (pollutes captured command output under
# `set -u`, same rationale as devnet-test.sh).
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

# ── Endpoints (the running node) ─────────────────────────────────────
L1_RPC="${L1_RPC:-http://localhost:18645}"      # embedded chiado L1
L2_RPC="${L2_RPC:-http://localhost:18688}"      # L2 (== outbound submit endpoint)
# Where the OUTBOUND user tx is SUBMITTED. Default = the L2 ingress
# (:18688). Outbound goes to the L2 RPC directly: the tx is signed for the
# L2 chain (id 1) and `to=P`; the ingress classifier tags it outbound by
# `to`, holds it, drains it at compose time. (The B0 interceptor :18649 is
# the INBOUND front and is NOT used here — confirmed against e2e_outbound.rs,
# which submits the outbound tx straight to the node's L2 RPC.)
SUBMIT_RPC="${SUBMIT_RPC:-$L2_RPC}"
NODE_CONTAINER="${NODE_CONTAINER:-eez-node-chiado}"

# ── Knobs ────────────────────────────────────────────────────────────
# The value the OUTBOUND user tx sets on the L1 Value (== e2e_outbound's 42,
# but unique-per-run by default so a re-run on the SAME, already-42 Value
# is still observable). Override OUTBOUND_VALUE to pin it.
OUTBOUND_VALUE="${OUTBOUND_VALUE:-$(( (RANDOM % 900) + 100 ))}"
MAINNET_ROLLUP_ID="${MAINNET_ROLLUP_ID:-0}"      # L1 target's rollup id (== outbound_gate MAINNET)
VALUE_INITIAL="${VALUE_INITIAL:-0}"              # L1 Value() ctor arg
SETTLE_WAIT_SECS="${OUTBOUND_SETTLE_WAIT_SECS:-300}"
CCM_L2_ADDRESS="${EEZ_CCM_L2_ADDRESS:-0x4200000000000000000000000000000000000007}"
SKIP_PROXY_ENV_CHECK="${OUTBOUND_SKIP_PROXY_ENV_CHECK:-0}"

# ── Keys (testnet only; match scripts/devnet-test.sh defaults) ───────
# Operator = protocol deployer + proof signer. Funded on the embedded L1
# (deploys the L1 Value target).
EEZ_OPERATOR_KEY="${EEZ_OPERATOR_KEY:-0x2248a31395af28e24349c8e566c19475a79cb610389204ab26bc585493e5cf27}"
# L2 outbound USER — hardhat key #2 (0x3C44…), prefunded in the L2 genesis
# alloc (verified ~1M ETH on the live L2). Signs the outbound user tx AND
# creates the L2 proxy (single EOA, sequential nonces — the proxy-create
# mines first, then the outbound tx).
HH_KEY_2="${HH_KEY_2:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"
HH_ADDR_2="${HH_ADDR_2:-0x3C44Cdddb6a900fa2b585dD299E03D12FA4293bC}"

# Snapshot of the composer log for the tally (docker logs, not a file).
NODE_LOG="$(mktemp /tmp/outbound-test-nodelog.XXXXXX)"
refresh_log() { docker logs "$NODE_CONTAINER" >"$NODE_LOG" 2>&1 || true; }
cleanup() { rm -f "$NODE_LOG"; }
trap cleanup EXIT

# Run a read-only command with retries — survives transient RPC hiccups.
retry() {
    local n=0 max="${RETRY_MAX:-6}" delay="${RETRY_DELAY:-3}" out rc
    while :; do
        out=$("$@" 2>&1); rc=$?
        (( rc == 0 )) && { printf '%s' "$out"; return 0; }
        (( ++n >= max )) && { echo "retry: '$*' failed after $n attempts: $out" >&2; return "$rc"; }
        sleep "$delay"
    done
}

# normalize an address to lowercase, 0x-prefixed, no surrounding whitespace.
lc() { echo "$1" | tr 'A-Z' 'a-z' | tr -d '[:space:]'; }

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

echo "==> OUTBOUND (L2→L1) cross-chain test"
echo "    L1 (internal) = $L1_RPC  (chain $L1_CHAIN_ID, head $L1_UP)"
echo "    L2            = $L2_RPC  (chain $L2_CHAIN_ID, head $L2_UP)"
echo "    submit via    = $SUBMIT_RPC  (L2 ingress; outbound is classified by to=P)"
echo "    registry      = $EEZ_REGISTRY_ADDRESS  rollupId=$EEZ_ROLLUP_ID"
echo "    L2 CCM        = $CCM_L2_ADDRESS"
echo "    target rollup = $MAINNET_ROLLUP_ID (MAINNET) ;  setValue($OUTBOUND_VALUE)"

# ── Sanity: the outbound user tx MUST be signed for the L2 chain ─────
# (e2e_outbound asserts chain_id == L2_CHAIN_ID before sending). A tx signed
# for the L1 chain would be classified INBOUND, not outbound.
echo "    outbound tx will be signed for L2 chainId=$L2_CHAIN_ID"

# ── 1. Deploy the L1 Value TARGET on the embedded chiado L1 ──────────
# Reuse DeployValueL2.s.sol (it is RPC-generic: `new Value(initial)`),
# pointed at the L1 RPC. Deploys from the operator EOA (funded on L1).
echo
# Pin an EXISTING L1 Value (so P is stable across runs — needed because the
# node's EEZ_CROSS_CHAIN_PROXY_ADDRESSES must be configured with P, which is
# deterministic from this address). When unset, deploy a fresh one.
if [[ -n "${L1_VALUE_PINNED:-}" ]]; then
    L1_VALUE_ADDRESS="$L1_VALUE_PINNED"
    echo "==> reusing pinned L1 Value @ $L1_VALUE_ADDRESS (L1_VALUE_PINNED set; skipping deploy)"
else
    echo "==> deploying Value($VALUE_INITIAL) TARGET on the embedded L1"
    cd "$REPO/contracts"
    VALUE_OUT=$(forge script script/DeployValueL2.s.sol:DeployValueL2 \
        --sig "run(uint256)" "$VALUE_INITIAL" \
        --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
    L1_VALUE_ADDRESS=$(echo "$VALUE_OUT" | grep -oE 'EEZ_VALUE_ADDRESS=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
    cd "$REPO"
    [[ -n "$L1_VALUE_ADDRESS" ]] || { echo "L1 Value deploy failed"; echo "$VALUE_OUT" | tail -25; exit 1; }
    echo "    L1 Value @ $L1_VALUE_ADDRESS"
fi
V0=$(retry cast call "$L1_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L1_RPC")
echo "    L1 Value.value() (pre) = $V0"

# ── 2. Compute the L2 cross-chain PROXY P (on-chain view) ────────────
# computeCrossChainProxyAddress(L1_Value, MAINNET) on the L2 CCM. This is
# the SAME CREATE2 address eez-evm/src/outbound_gate.rs computes (the gate's
# `create2_matches_onchain_proxy` test pins its embedded creationCode to
# exactly this contract's output) — so `tx.to == P` will satisfy the gate's
# 4th bind. Using the on-chain view (not a shell CREATE2 re-impl) is the
# drift-proof source of truth.
echo
echo "==> computing L2 cross-chain proxy P = computeCrossChainProxyAddress(Value, $MAINNET_ROLLUP_ID)"
PROXY=$(retry cast call "$CCM_L2_ADDRESS" \
    'computeCrossChainProxyAddress(address,uint256)(address)' \
    "$L1_VALUE_ADDRESS" "$MAINNET_ROLLUP_ID" --rpc-url "$L2_RPC")
PROXY=$(echo "$PROXY" | tr -d '[:space:]')
[[ "$PROXY" =~ ^0x[0-9a-fA-F]{40}$ ]] || { echo "computeCrossChainProxyAddress returned junk: $PROXY"; exit 1; }
echo "    P = $PROXY"

# ── 2b. Create the proxy on L2 so its bytecode exists ────────────────
# The outbound user tx CALLS P; P's `_fallback()` forwards to
# EEZL2.executeCrossChainCall. P must therefore be deployed on L2.
# createCrossChainProxy is idempotent-by-CREATE2: if P already exists
# (a prior run), the create reverts with CreateCollision — tolerate that
# by checking codesize after.
PROXY_CODE=$(retry cast code "$PROXY" --rpc-url "$L2_RPC")
if [[ "$PROXY_CODE" == "0x" || -z "$PROXY_CODE" ]]; then
    echo "==> creating the L2 proxy (createCrossChainProxy) from $HH_ADDR_2"
    CREATE_NONCE=$(retry cast nonce "$HH_ADDR_2" --rpc-url "$L2_RPC")
    CREATE_RAW=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_2" --nonce "$CREATE_NONCE" \
        --gas-limit 1500000 --gas-price 1000000000 --priority-gas-price 1000000000 \
        "$CCM_L2_ADDRESS" 'createCrossChainProxy(address,uint256)' "$L1_VALUE_ADDRESS" "$MAINNET_ROLLUP_ID" 2>&1) || true
    [[ "$CREATE_RAW" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "    proxy-create mktx failed: $CREATE_RAW"; exit 1; }
    curl -s -X POST "$L2_RPC" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$CREATE_RAW\"],\"id\":1}" >/dev/null
    # Wait for the proxy bytecode to appear.
    for _ in $(seq 1 30); do
        PROXY_CODE=$(cast code "$PROXY" --rpc-url "$L2_RPC" 2>/dev/null || echo "0x")
        [[ "$PROXY_CODE" != "0x" && -n "$PROXY_CODE" ]] && break
        sleep 2
    done
fi
if [[ "$PROXY_CODE" == "0x" || -z "$PROXY_CODE" ]]; then
    echo "    ✗ proxy P has no code on L2 after createCrossChainProxy — cannot route outbound"
    exit 1
fi
echo "    ✓ proxy P deployed on L2 (codesize > 0)"
# Cross-check the registry: authorizedProxies(P).originalAddress == L1 Value.
REG_ORIG=$(retry cast call "$CCM_L2_ADDRESS" 'authorizedProxies(address)(address,uint64)' "$PROXY" --rpc-url "$L2_RPC" | sed -n '1p' | tr -d '[:space:]')
if [[ "$(lc "$REG_ORIG")" == "$(lc "$L1_VALUE_ADDRESS")" ]]; then
    echo "    ✓ authorizedProxies(P).originalAddress == L1 Value"
else
    echo "    ⚠ authorizedProxies(P).originalAddress = $REG_ORIG (expected $L1_VALUE_ADDRESS)"
fi

# ── 2c. CRITICAL: is P in the live node's OUTBOUND proxy set? ─────────
# The classifier keys outbound ONLY on `to ∈ EEZ_CROSS_CHAIN_PROXY_ADDRESSES`
# (parsed once at startup). Verify the live container has P configured;
# otherwise the outbound tx mines as a plain L2 transfer and nothing settles.
echo
echo "==> verifying P is in the live node's EEZ_CROSS_CHAIN_PROXY_ADDRESSES"
LIVE_PROXY_ENV=$(docker exec "$NODE_CONTAINER" printenv EEZ_CROSS_CHAIN_PROXY_ADDRESSES 2>/dev/null || echo "")
PROXY_LC=$(lc "$PROXY")
if [[ "$SKIP_PROXY_ENV_CHECK" == "1" ]]; then
    echo "    (skipped via OUTBOUND_SKIP_PROXY_ENV_CHECK=1; live env = '${LIVE_PROXY_ENV:-<unset>}')"
elif [[ -z "$LIVE_PROXY_ENV" ]]; then
    cat >&2 <<EOF
    ✗ BLOCKER: the live node ($NODE_CONTAINER) has NO EEZ_CROSS_CHAIN_PROXY_ADDRESSES set.
      Its OUTBOUND classifier proxy set is EMPTY, so a tx to P=$PROXY is classified
      L2Only and mines as a plain transfer — NO outbound settlement is possible.

      REMEDIATION — add P to the node env and restart it:
        1. In docker-compose.chiado-node.yml, under the node service 'environment:', add:
             EEZ_CROSS_CHAIN_PROXY_ADDRESSES: "$PROXY"
        2. Recreate the node:
             docker compose -f docker-compose.chiado-node.yml up -d --force-recreate $NODE_CONTAINER
        3. Re-run this script with the SAME L1 Value so P is unchanged:
             L1_VALUE_ADDRESS is deployed fresh each run; to reuse P, pin the Value
             address. Easiest: run once to learn P, configure it, then re-run with
             OUTBOUND_SKIP_PROXY_ENV_CHECK=1 (P is deterministic from the L1 Value).

      (To run anyway and observe the negative result, set OUTBOUND_SKIP_PROXY_ENV_CHECK=1.)
EOF
    exit 2
elif ! echo "$LIVE_PROXY_ENV" | tr 'A-Z,' 'a-z\n' | grep -qx "$PROXY_LC"; then
    cat >&2 <<EOF
    ✗ BLOCKER: P=$PROXY is NOT in the live node's EEZ_CROSS_CHAIN_PROXY_ADDRESSES.
      Live value: '$LIVE_PROXY_ENV'
      The classifier won't tag a tx to P as outbound. Add P (comma-separated) to that
      env var and restart the node (see docker-compose.chiado-node.yml), then re-run.
EOF
    exit 2
else
    echo "    ✓ P is in the live node's outbound proxy set"
fi

# ── 3. Send the OUTBOUND user tx ─────────────────────────────────────
# L2-chain-id, to=P, data=setValue(N), from the funded L2 EOA, POSTed
# to the L2 ingress. cast mktx + raw eth_sendRawTransaction so we own the
# hash for the receipt wait (the ingress HOLDS the tx; it returns the hash
# on a successful hold, mirroring devnet-test.sh's submit path).
echo
echo "==> sending OUTBOUND user tx: to=P data=setValue($OUTBOUND_VALUE) chain=$L2_CHAIN_ID from=$HH_ADDR_2"
refresh_log
USER_NONCE=$(retry cast nonce "$HH_ADDR_2" --rpc-url "$L2_RPC")
USER_RAW=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_2" --nonce "$USER_NONCE" \
    --gas-limit 600000 --gas-price 1000000000 --priority-gas-price 1000000000 \
    "$PROXY" 'setValue(uint256)' "$OUTBOUND_VALUE" 2>&1) || true
[[ "$USER_RAW" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "    ✗ outbound mktx failed: $USER_RAW"; exit 1; }
USER_TX_HASH=$(cast keccak "$USER_RAW")
SUB_RESP=$(curl -s -X POST "$SUBMIT_RPC" -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$USER_RAW\"],\"id\":1}")
if echo "$SUB_RESP" | grep -q '"error"'; then
    echo "    ✗ outbound submit($SUBMIT_RPC) rejected → $SUB_RESP"
    exit 1
fi
echo "    outbound user tx submitted: hash=$USER_TX_HASH"

# ── 4. Wait for settlement on L1 ─────────────────────────────────────
# Acceptance (a): L1 Value.value() == N (the cross-chain setValue executed
# inside postBatch _processNCalls). This is the primary signal — exactly
# the e2e_outbound S3(a) assertion.
echo
echo "==> waiting up to ${SETTLE_WAIT_SECS}s for L1 Value.value() == $OUTBOUND_VALUE"
SETTLE_OK=0
wait_end=$(( SECONDS + SETTLE_WAIT_SECS )); last_line=""
while (( SECONDS < wait_end )); do
    VV=$(cast call "$L1_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L1_RPC" 2>/dev/null || echo "")
    line="    L1 Value.value() = ${VV:-?}  (target $OUTBOUND_VALUE, elapsed ${SECONDS}s)"
    [[ "$line" != "$last_line" ]] && { echo "$line"; last_line="$line"; }
    if [[ "$VV" == "$OUTBOUND_VALUE" ]]; then SETTLE_OK=1; echo "    ✓ L1 Value settled to $OUTBOUND_VALUE"; break; fi
    sleep 5
done
[[ "$SETTLE_OK" == "1" ]] || echo "    ✗ L1 Value did not reach $OUTBOUND_VALUE within ${SETTLE_WAIT_SECS}s"

echo "    settling 15s..."; sleep 15
refresh_log

# Acceptance (b): L1 rollups(rid).stateRoot == L2 safe-head state root
# (the user-tx-inclusive settled root). Both advance async — poll for
# convergence, like e2e_outbound S3(b) and devnet-test.sh's reconcile.
echo
echo "==> L1 vs L2 stateRoot reconciliation"
RECON_OK=0
recon_end=$(( SECONDS + 90 ))
while (( SECONDS < recon_end )); do
    L1_TRACKED=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint256)(address,bytes32,uint256)' "$EEZ_ROLLUP_ID" \
        --rpc-url "$L1_RPC" 2>/dev/null | sed -n '2p' | tr -d '[:space:]')
    L2_SAFE=$(cast block safe --rpc-url "$L2_RPC" --json 2>/dev/null | jq -r '.stateRoot // empty' 2>/dev/null)
    L2_SAFE_NUM=$(cast block safe --rpc-url "$L2_RPC" --json 2>/dev/null | jq -r '.number // empty' 2>/dev/null)
    if [[ -n "$L1_TRACKED" && -n "$L2_SAFE" && "$(lc "$L1_TRACKED")" == "$(lc "$L2_SAFE")" \
          && "$(lc "$L2_SAFE")" != "0x0000000000000000000000000000000000000000000000000000000000000000" ]]; then
        RECON_OK=1; break
    fi
    sleep 5
done
echo "    L1 rollups($EEZ_ROLLUP_ID).stateRoot = ${L1_TRACKED:-?}"
echo "    L2 safe-block stateRoot (#${L2_SAFE_NUM:-?}) = ${L2_SAFE:-?}"
if [[ "$RECON_OK" == "1" ]]; then
    echo "    ✓ L1 stored stateRoot == L2 safe-head root (user-tx-inclusive settled root)"
else
    echo "    ✗ L1 ≠ L2 safe-head root (no settled-root convergence)"
fi

# ── Divergence + outbound-failure marker check (node log) ────────────
echo
count_in() { local n; n=$(grep -c "$1" "$NODE_LOG" 2>/dev/null || true); echo "${n:-0}"; }
DIVERGED_LEGACY=$(count_in "local L2 state root differs"); DIVERGED_LEGACY=${DIVERGED_LEGACY:-0}
DIVERGED_DERIVER=$(count_in "diverged from L1-confirmed batch"); DIVERGED_DERIVER=${DIVERGED_DERIVER:-0}
# Composer outbound-compose failure markers (eez-composer/src/composer.rs):
# any of these means the outbound was detected but the entry could not be
# built/spliced — a real outbound bug, not a classification miss.
OUT_NO_L2ENTRY=$(count_in "outbound_no_l2_entry")
OUT_NO_ENTRIES=$(count_in "outbound_no_entries")
OUT_POISON=$(count_in "outbound_poison")
DIV_OK=0
if [[ "$DIVERGED_LEGACY" -eq 0 ]]; then
    DIV_OK=1
    if [[ "$DIVERGED_DERIVER" -eq 0 ]]; then
        echo "    ✓ zero state-root divergence events"
    else
        echo "    ⚠ $DIVERGED_DERIVER deriver-side WARN(s) from skipped batches — residual; reconcile is authoritative"
    fi
else
    echo "    ✗ legacy divergences: $DIVERGED_LEGACY"
fi
OUT_MARKERS_OK=1
if (( OUT_NO_L2ENTRY + OUT_NO_ENTRIES + OUT_POISON > 0 )); then
    OUT_MARKERS_OK=0
    echo "    ✗ composer outbound-compose failure markers: no_l2_entry=$OUT_NO_L2ENTRY no_entries=$OUT_NO_ENTRIES poison=$OUT_POISON"
else
    echo "    ✓ no composer outbound-compose failure markers"
fi

# ── Verdict ──────────────────────────────────────────────────────────
echo
ALL_OK=1
for ok in "$SETTLE_OK" "$RECON_OK" "$DIV_OK" "$OUT_MARKERS_OK"; do
    [[ "$ok" == "1" ]] || ALL_OK=0
done
if [[ "$ALL_OK" == "1" ]]; then
    echo "==> OUTBOUND TEST PASSED (L1 Value=$OUTBOUND_VALUE via P=$PROXY; L1↔L2 roots reconciled)"
    exit 0
else
    echo "==> OUTBOUND TEST FAILED"
    echo "    settle=$SETTLE_OK reconcile=$RECON_OK divergence_ok=$DIV_OK outbound_markers_ok=$OUT_MARKERS_OK"
    exit 1
fi
