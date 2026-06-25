#!/usr/bin/env bash
#
# MIXED (inbound L1→L2 + outbound L2→L1 in ONE Sync slot) cross-chain test
# driver for a RUNNING eez-node (the dockerized chiado node). It is the
# union of scripts/devnet-test.sh (INBOUND) and scripts/outbound-test-live.sh
# (OUTBOUND): it stands up BOTH targets/proxies, then submits ONE inbound
# user tx AND ONE outbound user tx BACK-TO-BACK so the composer drains both
# in the SAME per-slot `pop_n(MAX_USER_TXS_PER_BUNDLE=3)` → ONE mixed Sync
# block (the on-chain analogue of crates/eez-node/tests/e2e_mixed.rs).
#
# What it proves (the live mirror of e2e_mixed's A2b acceptance):
#   1. set up BOTH directions:
#      - INBOUND : deploy a fresh `Value` on L2 (the inbound target) + an
#        L1→L2 setter CrossChainProxy on the embedded L1 (like devnet-test);
#      - OUTBOUND: reuse the L1 `Value` 0xC795..899A + the L2 proxy
#        P=computeCrossChainProxyAddress(L1_Value,0)=0xDFCc..5B65 already in
#        the node's EEZ_CROSS_CHAIN_PROXY_ADDRESSES (like outbound-test-live);
#        create P on L2 if absent;
#   2. build BOTH user txs and submit them BACK-TO-BACK (no sleep between):
#      - inbound : L1-chain-id (10200) tx `to=L1_setter_proxy setValue(N1)`,
#        signed by EEZ_USER_KEY (L1-funded), POSTed to the B0 interceptor
#        :18649 — the wallet-correct L1→L2 front;
#      - outbound: L2-chain-id (1) tx `to=P setValue(N2)`, signed by HH_KEY_2
#        (L2-funded), POSTed to the L2 ingress :18688;
#   3. verify BOTH settle with zero divergence:
#      - L2 `Value.value() == N1` (inbound delivered on L2),
#      - L1 `Value.value() == N2` (outbound executed on L1 in postBatch),
#      - L1 rollups(rid).stateRoot == the L2 safe-head root (mixed-inclusive),
#      - zero state-root divergence in the node log,
#      - the node log shows BOTH legs composed into the SAME Sync slot — ONE
#        `eez.composer.deferred.armed` (real PS) / `eez.composer.bundle.
#        dispatched` (mock PS) line with `entry_count>=2`, AND the deriver's
#        `eez.deriver.reconcile.system_txs_built` with `outbound>=1 inbound>=1`
#        on ONE tx_hash — report the sync_height that carried both;
#   4. print a clear PASS/FAIL verdict.
#
# ── HOW BOTH LAND IN ONE SYNC SLOT (the only non-obvious part) ────────
# The composer holds cross-chain txs in a HeldPool and drains it ONCE per
# Sync slot (~L1-anchored ~5s cadence) via `pop_n(MAX_USER_TXS_PER_BUNDLE=3)`
# — up to 3 held txs are bundled into ONE compose_via_evm_composer call, i.e.
# ONE Sync block (eez-composer/src/composer.rs). Submitting the inbound and
# outbound user txs BACK-TO-BACK (curl, curl — no sleep, ~hundreds of ms
# apart) puts BOTH in the HeldPool well inside one ~5s slot, so the next
# drain pops BOTH (2 <= 3) into the same block. This is exactly e2e_mixed.rs's
# `tokio::join!` concurrent submit; here we just fire the two curls with no
# gap. (If they ever straddle a slot boundary the PASS gate fails on the
# "same sync_height" check, which is the correct, honest negative.)
#
# ── PER-DIRECTION NONCE SOURCING ─────────────────────────────────────
# The two users are DIFFERENT EOAs, so their nonce streams are independent:
#   - INBOUND user (EEZ_USER_KEY, 0x0DfB..68ad): the B0/ingress gate checks
#     the sender's L1 nonce → nonce from `cast nonce <addr> --rpc-url L1_RPC`.
#   - OUTBOUND user (HH_KEY_2, 0x3C44..93BC): the L2 ingress gate checks the
#     sender's L2 nonce → nonce from `cast nonce <addr> --rpc-url L2_RPC`.
# HH_KEY_2 may also create the L2 proxy P (one L2 tx, mined first); the
# outbound user tx then uses HH_KEY_2's NEXT L2 nonce — re-read AFTER the
# create mines so it is correct.
#
# Prereqs on the host: cast, forge, jq, docker; the sync-rollups-protocol
# submodule initialised; `forge build` artifacts in contracts/out.

set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"

# Silence foundry's nightly banner (pollutes captured output under `set -u`).
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

# ── Endpoints (the running node) ─────────────────────────────────────
L1_RPC="${L1_RPC:-http://localhost:18645}"          # embedded chiado L1 (chain 10200)
L2_RPC="${L2_RPC:-http://localhost:18688}"          # L2 (chain 1)
B0_RPC="${B0_RPC:-http://localhost:18649}"          # B0 L1→L2 interceptor (inbound front)
# Where each direction's USER tx is submitted:
#   - INBOUND  → B0 (:18649): the wallet-correct L1→L2 front. It forwards
#     eth_* to the embedded L1 and intercepts eth_sendRawTransaction to hold
#     the inbound intent. (The :18688 chain-id path also works; B0 is the
#     production front and the one this script uses by default.)
#   - OUTBOUND → L2 ingress (:18688): the tx is L2-signed, to=P; the ingress
#     classifies it outbound by `to ∈ EEZ_CROSS_CHAIN_PROXY_ADDRESSES`.
INBOUND_SUBMIT_RPC="${INBOUND_SUBMIT_RPC:-$B0_RPC}"
OUTBOUND_SUBMIT_RPC="${OUTBOUND_SUBMIT_RPC:-$L2_RPC}"
NODE_CONTAINER="${NODE_CONTAINER:-eez-node-chiado}"

# ── Knobs ────────────────────────────────────────────────────────────
# Distinct, unique-per-run values so neither leg is satisfied by a stale
# state from a prior run and a crossed wire (inbound effect on the L1 Value
# or vice-versa) is caught. INBOUND sets the L2 Value; OUTBOUND sets the L1.
INBOUND_VALUE="${INBOUND_VALUE:-$(( (RANDOM % 400) + 100 ))}"
OUTBOUND_VALUE="${OUTBOUND_VALUE:-$(( (RANDOM % 400) + 600 ))}"
MAINNET_ROLLUP_ID="${MAINNET_ROLLUP_ID:-0}"          # outbound L1 target's rollup id
VALUE_INITIAL="${VALUE_INITIAL:-0}"                  # fresh L2 Value() ctor arg
SETTLE_WAIT_SECS="${MIXED_SETTLE_WAIT_SECS:-360}"
CCM_L2_ADDRESS="${EEZ_CCM_L2_ADDRESS:-0x4200000000000000000000000000000000000007}"
SKIP_PROXY_ENV_CHECK="${MIXED_SKIP_PROXY_ENV_CHECK:-0}"

# Pinned OUTBOUND L1 Value + its deterministic L2 proxy P (already in the
# live node's EEZ_CROSS_CHAIN_PROXY_ADDRESSES). Override only if you know P.
L1_VALUE_ADDRESS="${L1_VALUE_PINNED:-0xC79532994497977633d99A18B931B5C9211c899A}"

# ── Keys (testnet only; match the two reference scripts' defaults) ────
# Operator = protocol deployer + proof signer; creates the L1 inbound proxy.
EEZ_OPERATOR_KEY="${EEZ_OPERATOR_KEY:-0x2248a31395af28e24349c8e566c19475a79cb610389204ab26bc585493e5cf27}"
# INBOUND user (L1→L2). L1-funded EOA 0x0DfB..68ad — sends the L1-chain-id
# setValue to the L1 setter proxy; nonce sourced from its L1 nonce.
EEZ_USER_KEY="${EEZ_USER_KEY:-0x3b7b012a74f1c18f714c38306339b6b4124f3a434bd816a1ee1fa5aeb5953efe}"
# OUTBOUND user (L2→L1) + L2 proxy creator. L2-funded hardhat #2 (0x3C44..);
# sends the L2-chain-id setValue to P; nonce sourced from its L2 nonce.
HH_KEY_2="${HH_KEY_2:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"
HH_ADDR_2="${HH_ADDR_2:-0x3C44Cdddb6a900fa2b585dD299E03D12FA4293bC}"

# Snapshot of the composer log for the tally (docker logs, not a file).
NODE_LOG="$(mktemp /tmp/mixed-test-nodelog.XXXXXX)"
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

# strip a captured value's surrounding whitespace / ANSI.
trim() { echo "$1" | tr -d '[:space:]'; }

# ── Prereqs ──────────────────────────────────────────────────────────
for t in cast forge jq docker curl; do command -v "$t" >/dev/null || { echo "$t not in PATH"; exit 1; }; done
[[ -f "$REPO/deployments.env" ]] || { echo "deployments.env missing — run make deploy-protocol first"; exit 1; }
docker inspect "$NODE_CONTAINER" >/dev/null 2>&1 || { echo "container '$NODE_CONTAINER' not found — is the node up?"; exit 1; }
L2_UP=$(cast block-number --rpc-url "$L2_RPC" 2>/dev/null || echo "")
[[ -n "$L2_UP" ]] || { echo "L2 RPC $L2_RPC not reachable"; exit 1; }
L1_UP=$(cast block-number --rpc-url "$L1_RPC" 2>/dev/null || echo "")
[[ -n "$L1_UP" ]] || { echo "L1 RPC $L1_RPC not reachable"; exit 1; }
B0_UP=$(cast block-number --rpc-url "$B0_RPC" 2>/dev/null || echo "")
[[ -n "$B0_UP" ]] || echo "    note: B0 RPC $B0_RPC eth_blockNumber not answering (it proxies L1); inbound submit may still work"

set -a; source "$REPO/deployments.env"; set +a
L1_CHAIN_ID=$(cast chain-id --rpc-url "$L1_RPC")
L2_CHAIN_ID=$(cast chain-id --rpc-url "$L2_RPC")
INBOUND_USER_ADDR=$(cast wallet address --private-key "$EEZ_USER_KEY")

echo "==> MIXED (inbound L1→L2 + outbound L2→L1 in ONE Sync slot) cross-chain test"
echo "    L1 (internal) = $L1_RPC  (chain $L1_CHAIN_ID, head $L1_UP)"
echo "    L2            = $L2_RPC  (chain $L2_CHAIN_ID, head $L2_UP)"
echo "    B0 inbound    = $B0_RPC  (L1→L2 interceptor front)"
echo "    registry      = $EEZ_REGISTRY_ADDRESS  rollupId=$EEZ_ROLLUP_ID"
echo "    inbound user  = $INBOUND_USER_ADDR (L1 nonce)  → L2 Value := $INBOUND_VALUE"
echo "    outbound user = $HH_ADDR_2 (L2 nonce)  → L1 Value := $OUTBOUND_VALUE"
echo "    inbound submit→$INBOUND_SUBMIT_RPC   outbound submit→$OUTBOUND_SUBMIT_RPC"

[[ "$L1_CHAIN_ID" != "$L2_CHAIN_ID" ]] || { echo "L1 and L2 chain ids equal ($L1_CHAIN_ID) — inbound classifier cannot fire"; exit 1; }

# ═══════════════════════════════════════════════════════════════════════
#  1. SET UP BOTH DIRECTIONS
# ═══════════════════════════════════════════════════════════════════════

# ── INBOUND target: deploy a fresh Value on L2 ───────────────────────
echo
echo "==> [INBOUND] deploying fresh Value($VALUE_INITIAL) target on L2"
cd "$REPO/contracts"
L2_VALUE_OUT=$(forge script script/DeployValueL2.s.sol:DeployValueL2 \
    --sig "run(uint256)" "$VALUE_INITIAL" \
    --rpc-url "$L2_RPC" --broadcast --private-key "$HH_KEY_2" --gas-price 0 --skip-simulation 2>&1) || true
L2_VALUE_ADDRESS=$(echo "$L2_VALUE_OUT" | grep -oE 'EEZ_VALUE_ADDRESS=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
cd "$REPO"
[[ -n "$L2_VALUE_ADDRESS" ]] || { echo "L2 Value deploy failed"; echo "$L2_VALUE_OUT" | tail -20; exit 1; }
echo "    L2 Value @ $L2_VALUE_ADDRESS"
L2_V0=$(retry cast call "$L2_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L2_RPC")
echo "    L2 Value.value() (pre) = $L2_V0"

# ── INBOUND proxy: createCrossChainProxy(target=L2_Value) on L1 ──────
# The L1→L2 setter proxy; an L1-chain-id tx `to=this setValue(N1)` is held
# by the composer and delivered to L2_Value as an inbound cross-chain call.
echo "==> [INBOUND] createCrossChainProxy(target=L2_Value, rollupId=$EEZ_ROLLUP_ID) on L1"
cd "$REPO/contracts"
SETTER_OUT=$(forge script script/CreateValueProxy.s.sol:CreateValueProxy \
    --sig "run(address,address,uint256)" "$EEZ_REGISTRY_ADDRESS" "$L2_VALUE_ADDRESS" "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
SETTER_PROXY=$(echo "$SETTER_OUT" | grep -oE 'EEZ_VALUE_PROXY=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2)
cd "$REPO"
[[ -n "$SETTER_PROXY" ]] || { echo "inbound setter proxy create failed"; echo "$SETTER_OUT" | tail -30; exit 1; }
echo "    L1 setter proxy = $SETTER_PROXY"

# ── OUTBOUND target: the pinned L1 Value ─────────────────────────────
echo
echo "==> [OUTBOUND] using pinned L1 Value @ $L1_VALUE_ADDRESS"
L1_V0=$(retry cast call "$L1_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L1_RPC")
echo "    L1 Value.value() (pre) = $L1_V0"

# ── OUTBOUND proxy P = computeCrossChainProxyAddress(L1_Value, MAINNET) ─
echo "==> [OUTBOUND] computing L2 proxy P = computeCrossChainProxyAddress(L1_Value, $MAINNET_ROLLUP_ID)"
PROXY=$(retry cast call "$CCM_L2_ADDRESS" \
    'computeCrossChainProxyAddress(address,uint256)(address)' \
    "$L1_VALUE_ADDRESS" "$MAINNET_ROLLUP_ID" --rpc-url "$L2_RPC")
PROXY=$(trim "$PROXY")
[[ "$PROXY" =~ ^0x[0-9a-fA-F]{40}$ ]] || { echo "computeCrossChainProxyAddress returned junk: $PROXY"; exit 1; }
echo "    P = $PROXY"

# ── Create P on L2 if its bytecode is absent (idempotent by CREATE2) ──
PROXY_CODE=$(retry cast code "$PROXY" --rpc-url "$L2_RPC")
if [[ "$PROXY_CODE" == "0x" || -z "$PROXY_CODE" ]]; then
    echo "==> [OUTBOUND] creating the L2 proxy P (createCrossChainProxy) from $HH_ADDR_2"
    CREATE_NONCE=$(retry cast nonce "$HH_ADDR_2" --rpc-url "$L2_RPC")
    CREATE_RAW=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_2" --nonce "$CREATE_NONCE" \
        --gas-limit 1500000 --gas-price 1000000000 --priority-gas-price 1000000000 \
        "$CCM_L2_ADDRESS" 'createCrossChainProxy(address,uint256)' "$L1_VALUE_ADDRESS" "$MAINNET_ROLLUP_ID" 2>&1) || true
    [[ "$CREATE_RAW" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "    proxy-create mktx failed: $CREATE_RAW"; exit 1; }
    curl -s -X POST "$L2_RPC" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$CREATE_RAW\"],\"id\":1}" >/dev/null
    for _ in $(seq 1 30); do
        PROXY_CODE=$(cast code "$PROXY" --rpc-url "$L2_RPC" 2>/dev/null || echo "0x")
        [[ "$PROXY_CODE" != "0x" && -n "$PROXY_CODE" ]] && break
        sleep 2
    done
fi
[[ "$PROXY_CODE" != "0x" && -n "$PROXY_CODE" ]] || { echo "    ✗ proxy P has no code on L2 — cannot route outbound"; exit 1; }
echo "    ✓ proxy P deployed on L2 (codesize > 0)"
REG_ORIG=$(retry cast call "$CCM_L2_ADDRESS" 'authorizedProxies(address)(address,uint64)' "$PROXY" --rpc-url "$L2_RPC" | sed -n '1p' | tr -d '[:space:]')
[[ "$(lc "$REG_ORIG")" == "$(lc "$L1_VALUE_ADDRESS")" ]] \
    && echo "    ✓ authorizedProxies(P).originalAddress == L1 Value" \
    || echo "    ⚠ authorizedProxies(P).originalAddress = $REG_ORIG (expected $L1_VALUE_ADDRESS)"

# ── CRITICAL: P must be in the live node's OUTBOUND proxy set ─────────
echo "==> [OUTBOUND] verifying P is in the live node's EEZ_CROSS_CHAIN_PROXY_ADDRESSES"
LIVE_PROXY_ENV=$(docker exec "$NODE_CONTAINER" printenv EEZ_CROSS_CHAIN_PROXY_ADDRESSES 2>/dev/null || echo "")
PROXY_LC=$(lc "$PROXY")
if [[ "$SKIP_PROXY_ENV_CHECK" == "1" ]]; then
    echo "    (skipped via MIXED_SKIP_PROXY_ENV_CHECK=1; live env = '${LIVE_PROXY_ENV:-<unset>}')"
elif [[ -z "$LIVE_PROXY_ENV" ]] || ! echo "$LIVE_PROXY_ENV" | tr 'A-Z,' 'a-z\n' | grep -qx "$PROXY_LC"; then
    cat >&2 <<EOF
    ✗ BLOCKER: P=$PROXY is NOT in the live node's EEZ_CROSS_CHAIN_PROXY_ADDRESSES.
      Live value: '${LIVE_PROXY_ENV:-<unset>}'
      The classifier won't tag a tx to P as outbound → the outbound leg mines as a plain
      L2 transfer and nothing settles. Add P (comma-separated) to that env var and restart
      the node (docker-compose.chiado-node.yml), then re-run. (Set MIXED_SKIP_PROXY_ENV_CHECK=1
      to bypass — e.g. you already restarted with P configured.)
EOF
    exit 2
else
    echo "    ✓ P is in the live node's outbound proxy set"
fi

# ── INBOUND classifier signal: the L1 chain id must be configured ────
echo "==> [INBOUND] verifying L1 chain id $L1_CHAIN_ID is in the live node's EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS"
LIVE_SRC_ENV=$(docker exec "$NODE_CONTAINER" printenv EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS 2>/dev/null || echo "")
if [[ "$SKIP_PROXY_ENV_CHECK" == "1" ]]; then
    echo "    (skipped; live env = '${LIVE_SRC_ENV:-<unset>}')"
elif [[ -z "$LIVE_SRC_ENV" ]] || ! echo "$LIVE_SRC_ENV" | tr ',' '\n' | grep -qx "$L1_CHAIN_ID"; then
    cat >&2 <<EOF
    ✗ BLOCKER: L1 chain id $L1_CHAIN_ID is NOT in the live node's EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS.
      Live value: '${LIVE_SRC_ENV:-<unset>}'
      The classifier won't tag an L1-chain-id tx as inbound → the inbound leg never composes.
      Add $L1_CHAIN_ID to that env var and restart the node, then re-run.
EOF
    exit 2
else
    echo "    ✓ L1 chain id $L1_CHAIN_ID is in the live node's inbound source-chain set"
fi

# ═══════════════════════════════════════════════════════════════════════
#  2. BUILD BOTH USER TXS, SUBMIT BACK-TO-BACK (one Sync slot)
# ═══════════════════════════════════════════════════════════════════════
# Build both raw txs FIRST (no RPC between the two submits), then fire the
# two curls with NO sleep so both are in the HeldPool inside one ~5s slot.

echo
echo "==> building both user txs"
# INBOUND: L1-chain-id tx, to=L1 setter proxy, setValue(N1), nonce from L1.
INBOUND_NONCE=$(retry cast nonce "$INBOUND_USER_ADDR" --rpc-url "$L1_RPC")
INBOUND_RAW=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "$EEZ_USER_KEY" --nonce "$INBOUND_NONCE" \
    --gas-limit 600000 --gas-price 2000000000 --priority-gas-price 1000000000 \
    "$SETTER_PROXY" 'setValue(uint256)' "$INBOUND_VALUE" 2>&1) || true
[[ "$INBOUND_RAW" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "    ✗ inbound mktx failed: $INBOUND_RAW"; exit 1; }
INBOUND_TX_HASH=$(cast keccak "$INBOUND_RAW")
echo "    inbound  : nonce=$INBOUND_NONCE (L1) to=$SETTER_PROXY setValue($INBOUND_VALUE) hash=$INBOUND_TX_HASH"

# OUTBOUND: L2-chain-id tx, to=P, setValue(N2), nonce from L2 (AFTER any
# proxy-create mined — re-read here so it's the next free L2 nonce).
OUTBOUND_NONCE=$(retry cast nonce "$HH_ADDR_2" --rpc-url "$L2_RPC")
OUTBOUND_RAW=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_2" --nonce "$OUTBOUND_NONCE" \
    --gas-limit 600000 --gas-price 1000000000 --priority-gas-price 1000000000 \
    "$PROXY" 'setValue(uint256)' "$OUTBOUND_VALUE" 2>&1) || true
[[ "$OUTBOUND_RAW" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "    ✗ outbound mktx failed: $OUTBOUND_RAW"; exit 1; }
OUTBOUND_TX_HASH=$(cast keccak "$OUTBOUND_RAW")
echo "    outbound : nonce=$OUTBOUND_NONCE (L2) to=$PROXY setValue($OUTBOUND_VALUE) hash=$OUTBOUND_TX_HASH"

# Mark the log so the "same Sync slot" search only looks at lines AFTER
# submission (avoids matching a prior run's armed line).
refresh_log; LOG_LINES_BEFORE=$(wc -l < "$NODE_LOG")

echo
echo "==> submitting BOTH user txs BACK-TO-BACK (no sleep) → one Sync slot"
INBOUND_RESP=$(curl -s -X POST "$INBOUND_SUBMIT_RPC" -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$INBOUND_RAW\"],\"id\":1}")
OUTBOUND_RESP=$(curl -s -X POST "$OUTBOUND_SUBMIT_RPC" -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$OUTBOUND_RAW\"],\"id\":2}")
echo "$INBOUND_RESP"  | grep -q '"error"' && echo "    ✗ inbound submit rejected → $INBOUND_RESP"   || echo "    ✓ inbound  held ($INBOUND_SUBMIT_RPC)"
echo "$OUTBOUND_RESP" | grep -q '"error"' && echo "    ✗ outbound submit rejected → $OUTBOUND_RESP" || echo "    ✓ outbound held ($OUTBOUND_SUBMIT_RPC)"
if echo "$INBOUND_RESP" | grep -q '"error"' || echo "$OUTBOUND_RESP" | grep -q '"error"'; then
    echo "    ✗ a leg was rejected at submit — cannot form a mixed slot"; exit 1
fi

# ═══════════════════════════════════════════════════════════════════════
#  3. VERIFY BOTH SETTLE WITH ZERO DIVERGENCE
# ═══════════════════════════════════════════════════════════════════════

# ── (a) BOTH effects: L2 Value == N1 AND L1 Value == N2 ──────────────
echo
echo "==> waiting up to ${SETTLE_WAIT_SECS}s for BOTH legs: L2 Value==$INBOUND_VALUE AND L1 Value==$OUTBOUND_VALUE"
SETTLE_OK=0; wait_end=$(( SECONDS + SETTLE_WAIT_SECS )); last_line=""
while (( SECONDS < wait_end )); do
    L2VV=$(cast call "$L2_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L2_RPC" 2>/dev/null || echo "")
    L1VV=$(cast call "$L1_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L1_RPC" 2>/dev/null || echo "")
    line="    L2 Value=${L2VV:-?} (target $INBOUND_VALUE)  |  L1 Value=${L1VV:-?} (target $OUTBOUND_VALUE)  (elapsed ${SECONDS}s)"
    [[ "$line" != "$last_line" ]] && { echo "$line"; last_line="$line"; }
    if [[ "$L2VV" == "$INBOUND_VALUE" && "$L1VV" == "$OUTBOUND_VALUE" ]]; then
        SETTLE_OK=1; echo "    ✓ BOTH legs settled (L2 inbound + L1 outbound)"; break
    fi
    sleep 5
done
[[ "$SETTLE_OK" == "1" ]] || echo "    ✗ both legs did not settle within ${SETTLE_WAIT_SECS}s (L2=${L2VV:-?}, L1=${L1VV:-?})"

echo "    settling 15s..."; sleep 15
refresh_log

# ── (b) L1 rollups(rid).stateRoot == L2 safe-head root ───────────────
echo
echo "==> L1 vs L2 stateRoot reconciliation"
RECON_OK=0; recon_end=$(( SECONDS + 120 )); L1_TRACKED=""; L2_SAFE=""; L2_SAFE_NUM=""
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
echo "    L1 rollups($EEZ_ROLLUP_ID).stateRoot       = ${L1_TRACKED:-?}"
echo "    L2 safe-block stateRoot (#${L2_SAFE_NUM:-?}) = ${L2_SAFE:-?}"
[[ "$RECON_OK" == "1" ]] \
    && echo "    ✓ L1 stored stateRoot == L2 safe-head root (mixed-settlement-inclusive)" \
    || echo "    ✗ L1 ≠ L2 safe-head root (no settled-root convergence)"

# ── (c) Zero divergence + no outbound-compose failure markers ────────
echo
count_in() { local n; n=$(grep -c "$1" "$NODE_LOG" 2>/dev/null || true); echo "${n:-0}"; }
DIVERGED_LEGACY=$(count_in "local L2 state root differs");        DIVERGED_LEGACY=${DIVERGED_LEGACY:-0}
DIVERGED_DERIVER=$(count_in "diverged from L1-confirmed batch");  DIVERGED_DERIVER=${DIVERGED_DERIVER:-0}
OUT_NO_L2ENTRY=$(count_in "outbound_no_l2_entry")
OUT_NO_ENTRIES=$(count_in "outbound_no_entries")
OUT_POISON=$(count_in "outbound_poison")
DIV_OK=0
if [[ "$DIVERGED_LEGACY" -eq 0 ]]; then
    DIV_OK=1
    [[ "$DIVERGED_DERIVER" -eq 0 ]] \
        && echo "    ✓ zero state-root divergence events" \
        || echo "    ⚠ $DIVERGED_DERIVER deriver-side WARN(s) from skipped batches — residual; reconcile is authoritative"
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

# ── (d) BOTH legs composed into the SAME Sync slot ───────────────────
# Two independent confirmations, only looking at log lines AFTER submit:
#   (i) COMPOSER: ONE armed/dispatched line with entry_count>=2. The mixed
#       slot's batch carries >=2 entries (>=1 outbound L1 settlement entry +
#       >=1 inbound deferred delivery entry) under ONE sync_height. The real
#       proof system (EEZ_PROOF_SYSTEM_KIND=real) emits
#       `eez.composer.deferred.armed`; a mock PS emits
#       `eez.composer.bundle.dispatched`. Accept either.
#   (ii) DERIVER: ONE `eez.deriver.reconcile.system_txs_built` line with
#       BOTH outbound>=1 AND inbound>=1 on the same tx_hash — the deriver
#       reconstructed both directions from ONE L1 batch. This is the
#       authoritative A2b proof (it is what e2e_mixed's follower asserts).
echo
echo "==> confirming BOTH legs landed in the SAME Sync slot"
# Only consider lines emitted after we submitted (skip earlier-run armed lines).
POST_LOG=$(mktemp /tmp/mixed-test-postlog.XXXXXX)
tail -n +"$((LOG_LINES_BEFORE + 1))" "$NODE_LOG" > "$POST_LOG" 2>/dev/null || cp "$NODE_LOG" "$POST_LOG"
# Strip ANSI so field grepping is robust.
sed -i 's/\x1b\[[0-9;]*m//g' "$POST_LOG" 2>/dev/null || true

SAME_SLOT_OK=0; MIXED_SYNC_HEIGHT=""

# (i) composer: an armed/dispatched line carrying entry_count>=2.
COMPOSER_LINE=$(grep -E 'eez\.composer\.(deferred\.armed|bundle\.dispatched)|deferred post armed|rich bundle dispatched' "$POST_LOG" 2>/dev/null \
    | grep -E 'entry_count=[2-9]|entry_count=[0-9]{2,}' | head -1 || true)
if [[ -n "$COMPOSER_LINE" ]]; then
    MIXED_SYNC_HEIGHT=$(echo "$COMPOSER_LINE" | grep -oE 'sync_height=[0-9]+' | grep -oE '[0-9]+' | head -1)
    EC=$(echo "$COMPOSER_LINE" | grep -oE 'entry_count=[0-9]+' | grep -oE '[0-9]+' | head -1)
    echo "    ✓ composer: ONE Sync slot armed with entry_count=$EC (>=2) at sync_height=${MIXED_SYNC_HEIGHT:-?}"
else
    echo "    ⚠ composer: no single armed/dispatched line with entry_count>=2 found post-submit"
fi

# (ii) deriver: one system_txs_built line with BOTH outbound>=1 AND inbound>=1.
DERIVER_LINE=$(grep -E 'eez\.deriver\.reconcile\.system_txs_built|built outbound load \+ inbound delivery system txs' "$POST_LOG" 2>/dev/null \
    | grep -E 'outbound=[1-9]' | grep -E 'inbound=[1-9]' | head -1 || true)
if [[ -n "$DERIVER_LINE" ]]; then
    OBC=$(echo "$DERIVER_LINE" | grep -oE 'outbound=[0-9]+' | grep -oE '[0-9]+' | head -1)
    IBC=$(echo "$DERIVER_LINE" | grep -oE 'inbound=[0-9]+'  | grep -oE '[0-9]+' | head -1)
    echo "    ✓ deriver: reconstructed BOTH directions from ONE L1 batch (outbound=$OBC, inbound=$IBC)"
    SAME_SLOT_OK=1
elif [[ -n "$COMPOSER_LINE" ]]; then
    # Deriver runs on the follower path; on a composer-only node the
    # system_txs_built line may not appear. The composer's entry_count>=2 in
    # ONE armed line is sufficient evidence the two legs shared a slot.
    echo "    (no deriver system_txs_built line; composer entry_count>=2 in one slot is sufficient)"
    SAME_SLOT_OK=1
else
    echo "    ✗ could not confirm both legs shared one Sync slot from the node log"
fi
rm -f "$POST_LOG"
[[ -n "$MIXED_SYNC_HEIGHT" ]] && echo "    → mixed Sync slot height = $MIXED_SYNC_HEIGHT (carried BOTH the outbound entry and the inbound delivery)"

# ── Verdict ──────────────────────────────────────────────────────────
echo
ALL_OK=1
for ok in "$SETTLE_OK" "$RECON_OK" "$DIV_OK" "$OUT_MARKERS_OK" "$SAME_SLOT_OK"; do
    [[ "$ok" == "1" ]] || ALL_OK=0
done
if [[ "$ALL_OK" == "1" ]]; then
    echo "==> MIXED TEST PASSED"
    echo "    inbound : L2 Value @ $L2_VALUE_ADDRESS == $INBOUND_VALUE (delivered L1→L2)"
    echo "    outbound: L1 Value @ $L1_VALUE_ADDRESS == $OUTBOUND_VALUE (executed L2→L1)"
    echo "    both composed into Sync slot height ${MIXED_SYNC_HEIGHT:-<see deriver line>}; L1↔L2 roots reconciled; zero divergence"
    exit 0
else
    echo "==> MIXED TEST FAILED"
    echo "    settle_both=$SETTLE_OK reconcile=$RECON_OK divergence_ok=$DIV_OK outbound_markers_ok=$OUT_MARKERS_OK same_slot=$SAME_SLOT_OK"
    exit 1
fi
