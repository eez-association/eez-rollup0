#!/usr/bin/env bash
#
# Comprehensive cross-chain WAVE test harness — KURTOSIS devnet edition.
#
# Adapted from the embedded-L1 harness (which booted its own eez-node + reth
# --chain dev + fronts). Here the whole devnet is already running inside the
# Kurtosis enclave (infra/kurtosis): eez-node runs the composer, the embedded
# L1, the L2 rollup, AND both cross-chain fronts. This harness does NOT launch
# anything — it ATTACHES to the enclave's published endpoints and drives waves.
#
# The wave loop + assertions + metrics logic is unchanged; it still lives in
# wave-lib.sh (alongside this file, under infra/kurtosis) and is sourced at the
# end with all addresses/keys in scope. Only the environment setup (endpoints,
# funding, protocol addresses) was reworked for the enclave.
#
# MODES (EEZ_WAVE_MODE, default "mixed"):
#   inbound     — L1→L2 only (deposit + setValue + setValueNoRet, direct + wrapper)
#   outbound    — L2→L1 only (withdraw + setValue + setValueNoRet, direct + wrapper)
#   mixed       — inbound AND outbound, submitted together so they share a Sync block
#   mixed-pure  — mixed + pure-L2 filler txs interleaved
#
# Cross-chain submission goes to the transparent FRONTS published by eez-node:
#   inbound  → L1 front  ($L1F, enclave port l1-xchain)  (held Inbound,  effect on L2)
#   outbound → L2 front  ($L2F, enclave port l2-xchain)  (held Outbound, effect on L1)
# Pure-L2 txs go to the normal L2 RPC mempool ($L2).
#
# PREREQS: just `bash infra/kurtosis/up.sh` (settled) — this script discovers
# everything else itself (endpoints via `kurtosis port print`, protocol
# deployment via `kurtosis files download`). cast, forge, jq, curl, kurtosis on
# PATH; sync-rollups-protocol submodule initialised.

set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

K="$(cd "$(dirname "$0")" && pwd)"          # infra/kurtosis
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
LOG_DIR="$REPO/datadir/smoke-logs"
mkdir -p "$LOG_DIR"

MODE="${EEZ_WAVE_MODE:-mixed}"
WAVES="${EEZ_WAVE_COUNT:-3}"

for t in cast forge jq curl kurtosis; do command -v "$t" >/dev/null || { echo "$t not in PATH"; exit 1; }; done

# ── Endpoints — resolved straight from the running enclave, no separate
# discovery step. Already-exported vars win (e.g. to point at a different
# enclave without re-deriving everything).
# L1 = the CANONICAL shared chain (el-1 reth) where proxies/targets/receipts and
# rollups(id).stateRoot live. The composer's embedded reth mirrors it in-process,
# so anything created here is visible to composition. NOT the embedded reth.
_port() { kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || true; }
: "${L1:=http://$(_port el-1-reth-lighthouse rpc)}"
: "${L2:=http://$(_port eez-node l2-rpc)}"
: "${L1F:=http://$(_port eez-node l1-xchain)}"
: "${L2F:=http://$(_port eez-node l2-xchain)}"
[[ "$L1" != "http://" && "$L2" != "http://" && "$L1F" != "http://" && "$L2F" != "http://" ]] \
    || { echo "could not resolve enclave ports — is '$ENCLAVE' up? (kurtosis enclave inspect $ENCLAVE)"; exit 1; }

NODE_LOG="${EEZ_NODE_LOG:-$LOG_DIR/wave-$MODE-node.log}"
DEPLOY_DIR="$(mktemp -d /tmp/eez-deployments.XXXXXX)"
trap 'rm -rf "$DEPLOY_DIR"' EXIT

# ── Protocol deployment (registry/rollup id/CCM/…) ────────────────────
# Prefer an already-placed $REPO/deployments.env; otherwise pull the artifact
# fresh from the enclave so there's no separate "download it yourself" step.
if [[ -f "$REPO/deployments.env" ]]; then
    set -a; source "$REPO/deployments.env"; set +a
else
    kurtosis files download "$ENCLAVE" eez-deployments "$DEPLOY_DIR" >/dev/null 2>&1 \
        || { echo "kurtosis files download failed — is '$ENCLAVE' up and deployed?"; exit 1; }
    set -a; source "$DEPLOY_DIR/deployments.env"; set +a
fi
[[ -n "${EEZ_REGISTRY_ADDRESS:-}" ]] || { echo "EEZ_REGISTRY_ADDRESS unset — deployments.env incomplete"; exit 1; }

# ── Keys ─────────────────────────────────────────────────────────────
# The standard hardhat mnemonic accounts are prefunded on the L2 genesis (all
# of them), but on the KURTOSIS canonical L1 only the poster is funded, so the
# L1-side actors below are funded from the poster at startup (see funding step).
# The poster/proof-signer are the node's own keys — this harness never posts.
HH_KEY_0=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80   # deployer/owner (L1 targets/proxies/wrapper)
HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a   # L2 contract deployer / L2 proxy creator
# Cross-chain users (distinct EOAs so per-direction nonce chains don't collide):
HH_KEY_IN=0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d   # #1 inbound user  (submits to L1 front → L1 gas → funded from poster)
HH_ADDR_IN=0x70997970C51812dc3A010C7d01b50e0d17dc79C8
HH_KEY_OUT=0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6  # #3 outbound user (submits to L2 front → L2 tx → funded on L2 genesis)
HH_ADDR_OUT=0x90F79bf6EB2c4f870365E785982E1f101E93b906
# Pure-L2 filler user = the L2-deployer key (acct #2), idle at wave time so its
# pure-L2 tx can't collide with a pooled cross-chain tx's L2 nonce.
HH_KEY_PURE=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a  # #2 (L2 deployer, idle at wave time)

# EOAs funded on L1 from the poster so they can pay gas on the shared chain.
# Read straight out of args.yaml — the same file `up.sh` filled in on first run.
L1_FUNDED_KEYS=("$HH_KEY_0" "$HH_KEY_IN")
_yaml() { grep -E "^[[:space:]]*$1:" "$K/args.yaml" 2>/dev/null | head -1 \
    | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; }
FUND_FROM_KEY="${EEZ_L1_POSTER_KEY:-$(_yaml poster_key)}"
[[ -n "$FUND_FROM_KEY" ]] || { echo "could not resolve the poster key — set EEZ_L1_POSTER_KEY or check $K/args.yaml"; exit 1; }

EEZ_CCM_L2_PREDEPLOY="${EEZ_CCM_L2_ADDRESS:-0x4200000000000000000000000000000000000007}"
SYS_ADDR="${EEZ_L2_SYSTEM_ADDRESS:-0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266}"
MAINNET_RID="${EEZ_L1_ROLLUP_ID:-0}"   # L1's rollup id (outbound proxy target)

# Deposit/withdraw recipient EOAs (not in genesis alloc → start at 0).
L2_DEP_RECIPIENT=0x2222222222222222222222222222222222222222   # inbound deposit lands here on L2
L1_WD_RECIPIENT=0x3333333333333333333333333333333333333333   # outbound withdraw lands here on L1

echo "════════════════════════════════════════════════════════════════"
echo " WAVE TEST (kurtosis) — mode=$MODE waves=$WAVES"
echo "════════════════════════════════════════════════════════════════"
echo "    L1 (shared)  = $L1"
echo "    L2           = $L2"
echo "    L1 front     = $L1F   (Inbound)"
echo "    L2 front     = $L2F   (Outbound)"
echo "    registry     = $EEZ_REGISTRY_ADDRESS  rollupId=${EEZ_ROLLUP_ID:-?}"

# Retry a read-only command (survives transient RPC hiccups under load).
retry() {
    local n=0 max="${RETRY_MAX:-6}" delay="${RETRY_DELAY:-3}" out rc
    while :; do
        out=$("$@" 2>&1); rc=$?
        (( rc == 0 )) && { printf '%s' "$out"; return 0; }
        (( ++n >= max )) && { echo "retry: '$*' failed after $n attempts: $out" >&2; return "$rc"; }
        sleep "$delay"
    done
}

# ── Reachability ─────────────────────────────────────────────────────
L1_UP=$(cast block-number --rpc-url "$L1" 2>/dev/null || echo "")
[[ -n "$L1_UP" ]] || { echo "L1 RPC $L1 not reachable — is the enclave up?"; exit 1; }
L2_UP=$(cast block-number --rpc-url "$L2" 2>/dev/null || echo "")
[[ -n "$L2_UP" ]] || { echo "L2 RPC $L2 not reachable"; exit 1; }
echo "    L1=$L1_UP L2=$L2_UP"

# ── Fund L1-side actors from the poster ──────────────────────────────
for k in "${L1_FUNDED_KEYS[@]}"; do
    a=$(cast wallet address --private-key "$k")
    if [[ "$(cast balance "$a" --rpc-url "$L1" 2>/dev/null || echo 0)" == "0" ]]; then
        echo "==> funding $a on L1 (10 ETH from poster)"
        cast send "$a" --value 10ether --private-key "$FUND_FROM_KEY" --rpc-url "$L1" >/dev/null \
            || { echo "failed to fund $a — is the poster funded on L1?"; exit 1; }
    fi
done

forge_deploy() { # <rpc> <key> <script:contract> <sig> <args...>  → echoes forge stdout
    local rpc="$1" key="$2" sc="$3" sig="$4"; shift 4
    (cd "$REPO/contracts" && forge script "script/$sc" --sig "$sig" "$@" \
        --rpc-url "$rpc" --broadcast --private-key "$key" --gas-price 1000000000 --skip-simulation 2>&1)
}
grab() { grep -oE "$1=0x[0-9a-fA-F]{40}" | head -1 | cut -d= -f2; }

# ── Deploy L2 targets (Value + ValueNoRet) ───────────────────────────
echo "==> deploying L2 targets (Value, ValueNoRet)"
L2_VALUE=$(forge_deploy "$L2" "$HH_KEY_2" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab EEZ_VALUE_ADDRESS)
L2_VALUE_NORET=$(forge_deploy "$L2" "$HH_KEY_2" DeployValueNoRetL2.s.sol:DeployValueNoRetL2 'run(uint256)' 0 | grab EEZ_VALUE_NORET_ADDRESS)
[[ -n "$L2_VALUE" && -n "$L2_VALUE_NORET" ]] || { echo "L2 target deploy failed"; exit 1; }
echo "    L2 Value=$L2_VALUE  ValueNoRet=$L2_VALUE_NORET"

# ── Deploy L1 outbound targets (Value + ValueNoRet on L1) ────────────
if [[ "$MODE" == outbound || "$MODE" == mixed || "$MODE" == mixed-pure ]]; then
    echo "==> deploying L1 outbound targets (Value, ValueNoRet on L1)"
    L1_VALUE=$(forge_deploy "$L1" "$HH_KEY_0" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab EEZ_VALUE_ADDRESS)
    L1_VALUE_NORET=$(forge_deploy "$L1" "$HH_KEY_0" DeployValueNoRetL2.s.sol:DeployValueNoRetL2 'run(uint256)' 0 | grab EEZ_VALUE_NORET_ADDRESS)
    [[ -n "$L1_VALUE" && -n "$L1_VALUE_NORET" ]] || { echo "L1 target deploy failed"; exit 1; }
    echo "    L1 Value=$L1_VALUE  ValueNoRet=$L1_VALUE_NORET"
fi

L1_CHAIN_ID=$(cast chain-id --rpc-url "$L1")
L2_CHAIN_ID=$(cast chain-id --rpc-url "$L2")
HH_KEY_2_ADDR=$(cast wallet address --private-key "$HH_KEY_2")

# ── Helpers: create an L1 (inbound) proxy and an L2 (outbound) proxy ──
# L1 proxy = createCrossChainProxy(target_on_L2, rid=EEZ_ROLLUP_ID) on the L1 EEZ.
create_l1_proxy() { # <target_on_L2> → proxy addr
    forge_deploy "$L1" "$HH_KEY_0" CreateValueProxy.s.sol:CreateValueProxy \
        'run(address,address,uint256)' "$EEZ_REGISTRY_ADDRESS" "$1" "$EEZ_ROLLUP_ID" | grab EEZ_VALUE_PROXY
}
# L2 proxy = computeCrossChainProxyAddress(target_on_L1, MAINNET) then
# createCrossChainProxy on the L2 CCM (a PURE L2 tx → normal L2 RPC).
create_l2_proxy() { # <target_on_L1> → proxy addr
    local tgt="$1" p code nonce raw
    p=$(cast call "$EEZ_CCM_L2_PREDEPLOY" 'computeCrossChainProxyAddress(address,uint256)(address)' "$tgt" "$MAINNET_RID" --rpc-url "$L2" | tr -d '[:space:]')
    code=$(cast code "$p" --rpc-url "$L2" 2>/dev/null || echo 0x)
    if [[ "$code" == "0x" || -z "$code" ]]; then
        nonce=$(cast nonce "$HH_KEY_2_ADDR" --rpc-url "$L2")
        raw=$(cast mktx --rpc-url "$L2" --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_2" --nonce "$nonce" \
            --gas-limit 1500000 --gas-price 1000000000 \
            "$EEZ_CCM_L2_PREDEPLOY" 'createCrossChainProxy(address,uint256)' "$tgt" "$MAINNET_RID")
        curl -s -X POST "$L2" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}" >/dev/null
        for _ in $(seq 1 30); do
            code=$(cast code "$p" --rpc-url "$L2" 2>/dev/null || echo 0x)
            [[ "$code" != "0x" && -n "$code" ]] && break
            sleep 1
        done
    fi
    echo "$p"
}

echo "==> creating cross-chain proxies for the active mode"
if [[ "$MODE" == inbound || "$MODE" == mixed || "$MODE" == mixed-pure || "$MODE" == adversarial ]]; then
    IN_VALUE_PROXY=$(create_l1_proxy "$L2_VALUE")
    IN_NORET_PROXY=$(create_l1_proxy "$L2_VALUE_NORET")
    IN_DEP_PROXY=$(create_l1_proxy "$L2_DEP_RECIPIENT")
    echo "    inbound proxies: setter=$IN_VALUE_PROXY noret=$IN_NORET_PROXY deposit=$IN_DEP_PROXY"
    # Inbound wrapper on L1 over the setter proxy.
    IN_WRAPPER=$(forge_deploy "$L1" "$HH_KEY_0" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$IN_VALUE_PROXY" | grab EEZ_SETTER_WRAPPER)
    echo "    inbound wrapper (L1) = $IN_WRAPPER"
fi
if [[ "$MODE" == outbound || "$MODE" == mixed || "$MODE" == mixed-pure ]]; then
    OUT_VALUE_PROXY=$(create_l2_proxy "$L1_VALUE")
    OUT_NORET_PROXY=$(create_l2_proxy "$L1_VALUE_NORET")
    OUT_WD_PROXY=$(create_l2_proxy "$L1_WD_RECIPIENT")
    echo "    outbound proxies: setter=$OUT_VALUE_PROXY noret=$OUT_NORET_PROXY withdraw=$OUT_WD_PROXY"
    # Outbound wrapper on L2 over the outbound setter proxy.
    OUT_WRAPPER=$(forge_deploy "$L2" "$HH_KEY_2" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$OUT_VALUE_PROXY" | grab EEZ_SETTER_WRAPPER)
    echo "    outbound wrapper (L2) = $OUT_WRAPPER"
fi

echo
echo "==> setup complete; wave execution is driven by wave-lib.sh (sourced)"
# The wave loop + assertions + metrics live in wave-lib.sh so this harness
# stays readable; it is sourced here with all the addresses/keys in scope.
source "$K/wave-lib.sh"
run_waves
