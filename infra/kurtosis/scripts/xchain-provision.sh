#!/usr/bin/env bash
# Provision the on-chain resources the eez-xchain spamoor scenario needs, so
# spammers can be started without running the whole wave-test harness.
#
# Idempotent: creates the targets, cross-chain proxies, and wrappers per
# direction plus a dedicated funded outbound (L2) key, then caches every
# address/key to
#   $REPO/datadir/xchain-provision.env
# Re-running reuses whatever already exists (proxies with code on-chain, keys
# already funded) and only fills the gaps. Intended to be sourced by
# spammers.sh, but runs standalone for inspection too.
#
# This mints the FULL op set the eez-xchain plugin can drive, per direction:
#   setter proxy (setValue), noret proxy (ValueNoRet), a recipient proxy for
#   value transfers (deposit inbound / withdraw outbound), and a wrapper
#   contract over the setter proxy — the same resources wave-test.sh creates.
# The plugin's `ops` config selects which of these each spammer actually uses.
#
# Env knobs:
#   KURTOSIS_ENCLAVE     enclave name (default eez-devnet)
#   EEZ_OUT_FUND_ETH     L2 funding for the outbound key       (default 600;
#                        covers the auto-sized 50-wallet pool at 5 ETH each)
#   EEZ_TEST_PRIORITY_GAS_PRICE_WEI  priority fee cap (default 1; L2 is sub-gwei)
set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

HERE="$(cd "$(dirname "$0")" && pwd)"
K="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
CACHE="$REPO/datadir/xchain-provision.env"
mkdir -p "$REPO/datadir"

for t in cast forge jq curl kurtosis openssl; do command -v "$t" >/dev/null || { echo "$t not in PATH" >&2; exit 1; }; done

# ── Endpoints (fixed enclave-internal names the plugin uses are separate; these
#    host-mapped ones are for provisioning from the host) ──────────────────────
_port() { kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || true; }
_http() { case "$1" in http*) echo "$1";; "") echo "";; *) echo "http://$1";; esac; }
L1="$(_http "$(_port el-1-reth-lighthouse rpc)")"
L2="$(_http "$(_port eez-node l2-rpc)")"
[[ -n "$L1" && -n "$L2" ]] || { echo "could not resolve enclave ports — is '$ENCLAVE' up?" >&2; exit 1; }

# ── Deployment artifact (registry, rollup id) ────────────────────────────────
DEPLOY_DIR="$(mktemp -d /tmp/eez-provision.XXXXXX)"
trap 'rm -rf "$DEPLOY_DIR"' EXIT
kurtosis files download "$ENCLAVE" eez-deployments "$DEPLOY_DIR" >/dev/null 2>&1 \
    || { echo "kurtosis files download failed — is '$ENCLAVE' up and deployed?" >&2; exit 1; }
set -a; source "$DEPLOY_DIR/deployments.env"; set +a
[[ -n "${EEZ_REGISTRY_ADDRESS:-}" && -n "${EEZ_ROLLUP_ID:-}" ]] \
    || { echo "EEZ_REGISTRY_ADDRESS/EEZ_ROLLUP_ID unset — deployments.env incomplete" >&2; exit 1; }

# ── Keys (mirrors wave-test.sh) ──────────────────────────────────────────────
HH_KEY_0=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80   # L1 deployer/owner
HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a   # L2 deployer
HH_KEY_2_ADDR=$(cast wallet address --private-key "$HH_KEY_2")

EEZ_CCM_L2_PREDEPLOY="${EEZ_CCM_L2_ADDRESS:-0x4200000000000000000000000000000000000007}"
MAINNET_RID="${EEZ_L1_ROLLUP_ID:-0}"
L1_CHAIN_ID=$(cast chain-id --rpc-url "$L1")
L2_CHAIN_ID=$(cast chain-id --rpc-url "$L2")

# ── Gas helpers (mirrors wave-test.sh) ───────────────────────────────────────
gas_price_for() { local gp; gp=$(cast gas-price --rpc-url "$1" 2>/dev/null || echo 1000000000); echo "${EEZ_TEST_GAS_PRICE_WEI:-$gp}"; }
priority_fee_for() { local pg="${EEZ_TEST_PRIORITY_GAS_PRICE_WEI:-1}"; (( pg > $1 )) && pg=$1; echo "$pg"; }
retry() { local n=0 max=6 out rc; while :; do out=$("$@" 2>&1); rc=$?; (( rc==0 )) && { printf '%s' "$out"; return 0; }; (( ++n>=max )) && { echo "retry '$*' failed: $out" >&2; return "$rc"; }; sleep 3; done; }

forge_deploy() { local rpc="$1" key="$2" sc="$3" sig="$4" gp; shift 4; gp=$(gas_price_for "$rpc")
    (cd "$REPO/contracts" && forge script "script/$sc" --sig "$sig" "$@" --rpc-url "$rpc" --broadcast --private-key "$key" --gas-price "$gp" --skip-simulation 2>&1); }
grab() { grep -oE "$1=0x[0-9a-fA-F]{40}" | head -1 | cut -d= -f2; }
has_code() { local c; c=$(cast code "$1" --rpc-url "$2" 2>/dev/null || echo 0x); [[ "$c" != "0x" && -n "$c" ]]; }

create_l1_proxy() { forge_deploy "$L1" "$HH_KEY_0" CreateValueProxy.s.sol:CreateValueProxy \
        'run(address,address,uint256)' "$EEZ_REGISTRY_ADDRESS" "$1" "$EEZ_ROLLUP_ID" | grab EEZ_VALUE_PROXY; }
create_l2_proxy() { local tgt="$1" p nonce raw
    p=$(cast call "$EEZ_CCM_L2_PREDEPLOY" 'computeCrossChainProxyAddress(address,uint256)(address)' "$tgt" "$MAINNET_RID" --rpc-url "$L2" | tr -d '[:space:]')
    if ! has_code "$p" "$L2"; then
        nonce=$(cast nonce "$HH_KEY_2_ADDR" --rpc-url "$L2")
        raw=$(cast mktx --rpc-url "$L2" --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_2" --nonce "$nonce" \
            --gas-limit 1500000 --gas-price "$(gas_price_for "$L2")" \
            "$EEZ_CCM_L2_PREDEPLOY" 'createCrossChainProxy(address,uint256)' "$tgt" "$MAINNET_RID")
        curl -s -X POST "$L2" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}" >/dev/null
        for _ in $(seq 1 30); do has_code "$p" "$L2" && break; sleep 1; done
    fi
    echo "$p"; }

# Wrapper: setViaProxy(uint256) forwarding to the setter proxy (L1 in, L2 out).
create_l1_wrapper() { forge_deploy "$L1" "$HH_KEY_0" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$1" | grab EEZ_SETTER_WRAPPER; }
create_l2_wrapper() { forge_deploy "$L2" "$HH_KEY_2" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$1" | grab EEZ_SETTER_WRAPPER; }

# provision_intact: every cached contract still has code (proxies, not the EOAs).
provision_intact() {
    has_code "${INBOUND_PROXY:-0x}" "$L1" && has_code "${INBOUND_NORET_PROXY:-0x}" "$L1" \
        && has_code "${INBOUND_DEP_PROXY:-0x}" "$L1" && has_code "${INBOUND_WRAPPER:-0x}" "$L1" \
        && has_code "${OUTBOUND_PROXY:-0x}" "$L2" && has_code "${OUTBOUND_NORET_PROXY:-0x}" "$L2" \
        && has_code "${OUTBOUND_WD_PROXY:-0x}" "$L2" && has_code "${OUTBOUND_WRAPPER:-0x}" "$L2"
}

# ── Reuse cached provisioning when it's still valid ──────────────────────────
if [[ -f "$CACHE" ]]; then
    set -a; source "$CACHE"; set +a
    if provision_intact; then
        echo "==> reusing cached provisioning ($CACHE)"
    else
        echo "==> cache stale (some resources missing on-chain); reprovisioning"
        rm -f "$CACHE"
    fi
fi

if [[ ! -f "$CACHE" ]]; then
    echo "==> deploying targets (Value + ValueNoRet on both chains)"
    L2_VALUE=$(forge_deploy "$L2" "$HH_KEY_2" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab EEZ_VALUE_ADDRESS)
    L1_VALUE=$(forge_deploy "$L1" "$HH_KEY_0" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab EEZ_VALUE_ADDRESS)
    L2_VALUE_NORET=$(forge_deploy "$L2" "$HH_KEY_2" DeployValueNoRetL2.s.sol:DeployValueNoRetL2 'run(uint256)' 0 | grab EEZ_VALUE_NORET_ADDRESS)
    L1_VALUE_NORET=$(forge_deploy "$L1" "$HH_KEY_0" DeployValueNoRetL2.s.sol:DeployValueNoRetL2 'run(uint256)' 0 | grab EEZ_VALUE_NORET_ADDRESS)
    [[ -n "$L2_VALUE" && -n "$L1_VALUE" && -n "$L2_VALUE_NORET" && -n "$L1_VALUE_NORET" ]] \
        || { echo "target deploy failed" >&2; exit 1; }
    echo "    L2 Value=$L2_VALUE NoRet=$L2_VALUE_NORET"
    echo "    L1 Value=$L1_VALUE NoRet=$L1_VALUE_NORET"

    # Deposit/withdraw recipient EOAs. Random, cached so repeat runs reuse the
    # same recipients (value-transfer balances accumulate predictably).
    L2_DEP_RECIPIENT=0x$(openssl rand -hex 20)   # inbound value lands here on L2
    L1_WD_RECIPIENT=0x$(openssl rand -hex 20)    # outbound value lands here on L1

    echo "==> creating inbound proxies (L1->L2): setter, noret, deposit"
    INBOUND_PROXY=$(create_l1_proxy "$L2_VALUE")
    INBOUND_NORET_PROXY=$(create_l1_proxy "$L2_VALUE_NORET")
    INBOUND_DEP_PROXY=$(create_l1_proxy "$L2_DEP_RECIPIENT")
    for v in INBOUND_PROXY INBOUND_NORET_PROXY INBOUND_DEP_PROXY; do
        has_code "${!v}" "$L1" || { echo "inbound proxy $v creation failed (${!v:-empty})" >&2; exit 1; }
    done
    echo "==> creating inbound wrapper (L1, over setter proxy)"
    INBOUND_WRAPPER=$(create_l1_wrapper "$INBOUND_PROXY")
    has_code "$INBOUND_WRAPPER" "$L1" || { echo "inbound wrapper creation failed (${INBOUND_WRAPPER:-empty})" >&2; exit 1; }

    echo "==> creating outbound proxies (L2->L1): setter, noret, withdraw"
    OUTBOUND_PROXY=$(create_l2_proxy "$L1_VALUE")
    OUTBOUND_NORET_PROXY=$(create_l2_proxy "$L1_VALUE_NORET")
    OUTBOUND_WD_PROXY=$(create_l2_proxy "$L1_WD_RECIPIENT")
    for v in OUTBOUND_PROXY OUTBOUND_NORET_PROXY OUTBOUND_WD_PROXY; do
        has_code "${!v}" "$L2" || { echo "outbound proxy $v creation failed (${!v:-empty})" >&2; exit 1; }
    done
    echo "==> creating outbound wrapper (L2, over setter proxy)"
    OUTBOUND_WRAPPER=$(create_l2_wrapper "$OUTBOUND_PROXY")
    has_code "$OUTBOUND_WRAPPER" "$L2" || { echo "outbound wrapper creation failed (${OUTBOUND_WRAPPER:-empty})" >&2; exit 1; }

    # Dedicated outbound key: the plugin builds the L2 pool from it, so it's
    # funded on L2 below, cached, and must NOT be the eez-node system key. (The
    # inbound pool's root is the daemon's -p key, prefunded via up.sh/main.star.)
    OUT_KEY=0x$(openssl rand -hex 32); OUT_ADDR=$(cast wallet address --private-key "$OUT_KEY")

    {
        echo "# eez-xchain provisioning cache — sourced by spammers.sh. Do not edit by hand."
        echo "INBOUND_PROXY=$INBOUND_PROXY"
        echo "INBOUND_NORET_PROXY=$INBOUND_NORET_PROXY"
        echo "INBOUND_DEP_PROXY=$INBOUND_DEP_PROXY"
        echo "INBOUND_WRAPPER=$INBOUND_WRAPPER"
        echo "OUTBOUND_PROXY=$OUTBOUND_PROXY"
        echo "OUTBOUND_NORET_PROXY=$OUTBOUND_NORET_PROXY"
        echo "OUTBOUND_WD_PROXY=$OUTBOUND_WD_PROXY"
        echo "OUTBOUND_WRAPPER=$OUTBOUND_WRAPPER"
        echo "L2_DEP_RECIPIENT=$L2_DEP_RECIPIENT"
        echo "L1_WD_RECIPIENT=$L1_WD_RECIPIENT"
        echo "OUT_KEY=$OUT_KEY"
        echo "OUT_ADDR=$OUT_ADDR"
    } > "$CACHE"
    echo "==> cached provisioning -> $CACHE"
fi

# ── Fund the dedicated keys up to target (idempotent: only tops up shortfalls) ─
fund_to() { # <addr> <target_eth> <rpc> <from_key>
    local addr="$1" want_eth="$2" rpc="$3" fkey="$4" have want gp nonce faddr
    have=$(retry cast balance "$addr" --rpc-url "$rpc")
    want=$(cast to-wei "$want_eth" ether)
    # wei values exceed 64-bit; compare as big ints via python3 (bash would overflow).
    if python3 -c "import sys; sys.exit(0 if int('$have') >= int('$want') else 1)"; then
        echo "    $addr already funded ($have wei on $rpc)"; return 0
    fi
    gp=$(gas_price_for "$rpc"); faddr=$(cast wallet address --private-key "$fkey"); nonce=$(retry cast nonce "$faddr" --rpc-url "$rpc")
    echo "    funding $addr with ${want_eth} ETH on $rpc"
    cast send "$addr" --value "${want_eth}ether" --private-key "$fkey" --nonce "$nonce" \
        --gas-price "$gp" --priority-gas-price "$(priority_fee_for "$gp")" --rpc-url "$rpc" >/dev/null
}

echo "==> funding outbound key on L2"
fund_to "$OUT_ADDR" "${EEZ_OUT_FUND_ETH:-600}" "$L2" "$HH_KEY_2"

echo
echo "==> provisioning complete"
echo "    inbound  (L1->L2): setter=$INBOUND_PROXY noret=$INBOUND_NORET_PROXY deposit=$INBOUND_DEP_PROXY wrapper=$INBOUND_WRAPPER"
echo "    outbound (L2->L1): setter=$OUTBOUND_PROXY noret=$OUTBOUND_NORET_PROXY withdraw=$OUTBOUND_WD_PROXY wrapper=$OUTBOUND_WRAPPER"
echo "    outbound key addr= $OUT_ADDR (L2, outbound pool root)"
echo "    inbound pool root= spamoor-eez daemon -p key (set at bring-up; not provisioned here)"
echo "    cache            = $CACHE"
