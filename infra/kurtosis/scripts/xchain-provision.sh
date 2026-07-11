#!/usr/bin/env bash
# Provision the on-chain resources the eez-xchain spamoor scenario needs, so
# spammers can be started without running the whole wave-test harness.
#
# Idempotent: creates a Value target + one setter CrossChainProxy per direction,
# and a dedicated, funded key per direction, then caches every address/key to
#   $REPO/datadir/xchain-provision.env
# Re-running reuses whatever already exists (proxies with code on-chain, keys
# already funded) and only fills the gaps. Intended to be sourced by
# spammers.sh, but runs standalone for inspection too.
#
# This mints only the SETTER proxies (setValue) the plugin drives — not the
# noret/deposit/withdraw/wrapper variants wave-test.sh also creates.
#
# Env knobs:
#   KURTOSIS_ENCLAVE     enclave name (default eez-devnet)
#   EEZ_DAEMON_FUND_ETH  L1 funding for the inbound daemon key (default 500)
#   EEZ_OUT_FUND_ETH     L2 funding for the outbound key       (default 300)
#   EEZ_TEST_PRIORITY_GAS_PRICE_WEI  priority fee cap (default 1; L2 is sub-gwei)
set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

HERE="$(cd "$(dirname "$0")" && pwd)"
K="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$K/.." && pwd)"
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
[[ -n "${EEZ_REGISTRY_ADDRESS:-}" ]] || { echo "EEZ_REGISTRY_ADDRESS unset — deployments.env incomplete" >&2; exit 1; }

# ── Keys (mirrors wave-test.sh) ──────────────────────────────────────────────
HH_KEY_0=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80   # L1 deployer/owner
HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a   # L2 deployer
HH_KEY_2_ADDR=$(cast wallet address --private-key "$HH_KEY_2")
_yaml() { grep -E "^[[:space:]]*$1:" "$K/args.yaml" 2>/dev/null | head -1 | grep -oE '0x[0-9a-fA-F]{64}' | head -1; }
FUND_FROM_KEY="${EEZ_FUND_FROM_KEY:-${EEZ_PROOF_SIGNER_KEY:-$(_yaml proof_signer_key)}}"
[[ -n "$FUND_FROM_KEY" ]] || { echo "could not resolve an L1 funding key — set EEZ_FUND_FROM_KEY" >&2; exit 1; }

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

# ── Reuse cached provisioning when it's still valid ──────────────────────────
if [[ -f "$CACHE" ]]; then
    set -a; source "$CACHE"; set +a
    if has_code "${INBOUND_PROXY:-0x}" "$L1" && has_code "${OUTBOUND_PROXY:-0x}" "$L2"; then
        echo "==> reusing cached provisioning ($CACHE)"
    else
        echo "==> cache stale (proxies missing on-chain); reprovisioning"
        rm -f "$CACHE"
    fi
fi

if [[ ! -f "$CACHE" ]]; then
    echo "==> deploying setter targets"
    L2_VALUE=$(forge_deploy "$L2" "$HH_KEY_2" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab EEZ_VALUE_ADDRESS)
    L1_VALUE=$(forge_deploy "$L1" "$HH_KEY_0" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab EEZ_VALUE_ADDRESS)
    [[ -n "$L2_VALUE" && -n "$L1_VALUE" ]] || { echo "target deploy failed" >&2; exit 1; }
    echo "    L2 Value=$L2_VALUE  L1 Value=$L1_VALUE"

    echo "==> creating setter proxies (inbound L1->L2, outbound L2->L1)"
    INBOUND_PROXY=$(create_l1_proxy "$L2_VALUE")
    OUTBOUND_PROXY=$(create_l2_proxy "$L1_VALUE")
    [[ -n "$INBOUND_PROXY" ]] && has_code "$INBOUND_PROXY" "$L1" || { echo "inbound proxy creation failed" >&2; exit 1; }
    [[ -n "$OUTBOUND_PROXY" ]] && has_code "$OUTBOUND_PROXY" "$L2" || { echo "outbound proxy creation failed" >&2; exit 1; }
    echo "    inbound_proxy=$INBOUND_PROXY  outbound_proxy=$OUTBOUND_PROXY"

    # Dedicated keys: inbound daemon key (L1) must NOT collide with the batch
    # poster; outbound key (L2) must NOT be the eez-node system key. Generated
    # once and cached so re-runs don't churn keys or re-fund.
    DAEMON_KEY=0x$(openssl rand -hex 32); DAEMON_ADDR=$(cast wallet address --private-key "$DAEMON_KEY")
    OUT_KEY=0x$(openssl rand -hex 32);    OUT_ADDR=$(cast wallet address --private-key "$OUT_KEY")

    {
        echo "INBOUND_PROXY=$INBOUND_PROXY"
        echo "OUTBOUND_PROXY=$OUTBOUND_PROXY"
        echo "DAEMON_KEY=$DAEMON_KEY"
        echo "DAEMON_ADDR=$DAEMON_ADDR"
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

echo "==> funding dedicated keys"
fund_to "$DAEMON_ADDR" "${EEZ_DAEMON_FUND_ETH:-500}" "$L1" "$FUND_FROM_KEY"
fund_to "$OUT_ADDR"    "${EEZ_OUT_FUND_ETH:-300}"    "$L2" "$HH_KEY_2"

echo
echo "==> provisioning complete"
echo "    inbound_proxy    = $INBOUND_PROXY"
echo "    outbound_proxy   = $OUTBOUND_PROXY"
echo "    daemon key addr  = $DAEMON_ADDR (L1)"
echo "    outbound key addr= $OUT_ADDR (L2)"
echo "    cache            = $CACHE"
