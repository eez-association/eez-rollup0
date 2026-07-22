#!/usr/bin/env bash
# Cross-chain wave test for a running Kurtosis devnet.
#
# Comprehensive cross-chain WAVE test harness — KURTOSIS devnet edition.
#
# Adapted from the embedded-L1 harness (which booted its own eez-node + reth
# --chain dev + fronts). Here the whole devnet is already running inside the
# Kurtosis enclave (infra/kurtosis): eez-node runs the composer, the embedded
# L1, the L2 rollup, AND both cross-chain fronts. This harness does NOT launch
# anything — it ATTACHES to the enclave's published endpoints and drives waves.
#
# The script attaches to a running enclave and drives setup, waves, assertions,
# and metrics in one place.
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

K="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
LOG_DIR="$REPO/datadir/smoke-logs"
mkdir -p "$LOG_DIR"

MODE="${EEZ_WAVE_MODE:-mixed}"
WAVES="${EEZ_WAVE_COUNT:-3}"

for t in cast forge jq curl kurtosis openssl; do command -v "$t" >/dev/null || { echo "$t not in PATH"; exit 1; }; done

# L1 is the canonical shared chain; fronts are published by eez-node.
_port() { kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || true; }
_http() { case "$1" in http*) echo "$1";; "") echo "";; *) echo "http://$1";; esac; }
: "${L1:=$(_http "$(_port el-1-reth-lighthouse rpc)")}"
: "${L2:=$(_http "$(_port eez-node l2-rpc)")}"
: "${L1F:=$(_http "$(_port eez-node l1-xchain)")}"
: "${L2F:=$(_http "$(_port eez-node l2-xchain)")}"
[[ -n "$L1" && -n "$L2" && -n "$L1F" && -n "$L2F" ]] \
    || { echo "could not resolve enclave ports — is '$ENCLAVE' up? (kurtosis enclave inspect $ENCLAVE)"; exit 1; }

NODE_LOG="${EEZ_NODE_LOG:-$LOG_DIR/wave-$MODE-node.log}"
DEPLOY_DIR="$(mktemp -d /tmp/eez-deployments.XXXXXX)"
trap 'rm -rf "$DEPLOY_DIR"' EXIT

# Pull the deployment artifact from the enclave by default.
if [[ "${EEZ_USE_LOCAL_DEPLOYMENTS:-0}" == "1" && -f "$REPO/deployments.env" ]]; then
    set -a; source "$REPO/deployments.env"; set +a
else
    kurtosis files download "$ENCLAVE" eez-deployments "$DEPLOY_DIR" >/dev/null 2>&1 \
        || { echo "kurtosis files download failed — is '$ENCLAVE' up and deployed?"; exit 1; }
    set -a; source "$DEPLOY_DIR/deployments.env"; set +a
fi
[[ -n "${EEZ_REGISTRY_ADDRESS:-}" ]] || { echo "EEZ_REGISTRY_ADDRESS unset — deployments.env incomplete"; exit 1; }

# Hardhat accounts are funded on L2; L1 actors are funded below.
HH_KEY_0=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80   # deployer/owner (L1 targets/proxies/wrapper)
HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a   # L2 contract deployer / L2 proxy creator
# Fresh users avoid stale held-pool nonce state from earlier interrupted runs.
HH_KEY_IN="${EEZ_WAVE_IN_KEY:-0x$(openssl rand -hex 32)}"
HH_ADDR_IN=$(cast wallet address --private-key "$HH_KEY_IN")
HH_KEY_OUT="${EEZ_WAVE_OUT_KEY:-0x$(openssl rand -hex 32)}"
HH_ADDR_OUT=$(cast wallet address --private-key "$HH_KEY_OUT")
# Pure-L2 filler user.
HH_KEY_PURE=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a  # #2 (L2 deployer, idle at wave time)
HH_KEY_2_ADDR=$(cast wallet address --private-key "$HH_KEY_2")

# EOAs funded on L1 so they can pay gas on the shared chain.
L1_FUNDED_KEYS=("$HH_KEY_0" "$HH_KEY_IN")
_yaml() { grep -E "^[[:space:]]*$1:" "$K/args.yaml" 2>/dev/null | head -1 \
    | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; }
FUND_FROM_KEY="${EEZ_FUND_FROM_KEY:-${EEZ_PROOF_SIGNER_KEY:-$(_yaml proof_signer_key)}}"
[[ -n "$FUND_FROM_KEY" ]] || { echo "could not resolve a funding key — set EEZ_FUND_FROM_KEY or check $K/args.yaml"; exit 1; }

EEZ_CCM_L2_PREDEPLOY="${EEZ_CCM_L2_ADDRESS:-0x4200000000000000000000000000000000000007}"
SYS_ADDR="${EEZ_L2_SYSTEM_ADDRESS:-0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266}"
MAINNET_RID="${EEZ_L1_ROLLUP_ID:-0}"   # L1's rollup id (outbound proxy target)

# Deposit/withdraw recipient EOAs. Random by default to avoid deterministic
# proxy collisions across repeated runs on the same enclave.
L2_DEP_RECIPIENT="${L2_DEP_RECIPIENT:-0x$(openssl rand -hex 20)}"
L1_WD_RECIPIENT="${L1_WD_RECIPIENT:-0x$(openssl rand -hex 20)}"

echo "════════════════════════════════════════════════════════════════"
echo " WAVE TEST (kurtosis) — mode=$MODE waves=$WAVES"
echo "════════════════════════════════════════════════════════════════"
echo "    L1 (shared)  = $L1"
echo "    L2           = $L2"
echo "    L1 front     = $L1F   (Inbound)"
echo "    L2 front     = $L2F   (Outbound)"
echo "    registry     = $EEZ_REGISTRY_ADDRESS  rollupId=${EEZ_ROLLUP_ID:-?}"
echo "    users        = inbound:$HH_ADDR_IN outbound:$HH_ADDR_OUT"

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

gas_price_for() { # <rpc> -> wei
    local gp
    gp=$(cast gas-price --rpc-url "$1" 2>/dev/null || echo 1000000000)
    echo "${EEZ_TEST_GAS_PRICE_WEI:-$gp}"
}

# priority_fee_for <max_fee_wei> -> wei
# Caps the priority fee to the live max fee (gas_price_for) for the same
# chain — priority > max fee is rejected by the RPC. A fixed default can't
# work across both chains: a quiet/idle L2 can sit at a live gas price of a
# few wei, well below any single hardcoded constant.
priority_fee_for() {
    local pg="${EEZ_TEST_PRIORITY_GAS_PRICE_WEI:-1000000000}"
    (( pg > $1 )) && pg=$1
    echo "$pg"
}

fund_l1() {
    local to="$1" from_addr nonce gp
    from_addr=$(cast wallet address --private-key "$FUND_FROM_KEY")
    nonce=$(retry cast nonce "$from_addr" --rpc-url "$L1")
    gp=$(gas_price_for "$L1")
    cast send "$to" --value 10ether --private-key "$FUND_FROM_KEY" --nonce "$nonce" \
        --gas-price "$gp" --priority-gas-price "$(priority_fee_for "$gp")" --rpc-url "$L1" >/dev/null
}

fund_l2() {
    local to="$1" nonce gp
    nonce=$(retry cast nonce "$HH_KEY_2_ADDR" --rpc-url "$L2")
    gp=$(gas_price_for "$L2")
    cast send "$to" --value 10ether --private-key "$HH_KEY_2" --nonce "$nonce" \
        --gas-price "$gp" --priority-gas-price "$(priority_fee_for "$gp")" --rpc-url "$L2" >/dev/null
}

# ── Fund L1-side actors ──────────────────────────────────────────────
for k in "${L1_FUNDED_KEYS[@]}"; do
    a=$(cast wallet address --private-key "$k")
    if [[ "$(cast balance "$a" --rpc-url "$L1" 2>/dev/null || echo 0)" == "0" ]]; then
        echo "==> funding $a on L1 (10 ETH)"
        fund_l1 "$a" || { echo "failed to fund $a — is the funding key funded on L1?"; exit 1; }
    fi
done

if [[ "$(cast balance "$HH_ADDR_OUT" --rpc-url "$L2" 2>/dev/null || echo 0)" == "0" ]]; then
    echo "==> funding $HH_ADDR_OUT on L2 (10 ETH)"
    fund_l2 "$HH_ADDR_OUT" || { echo "failed to fund $HH_ADDR_OUT on L2"; exit 1; }
fi

forge_deploy() { # <rpc> <key> <script:contract> <sig> <args...>  → echoes forge stdout
    local rpc="$1" key="$2" sc="$3" sig="$4" gas_price; shift 4
    gas_price=$(gas_price_for "$rpc")
    (cd "$REPO/contracts" && forge script "script/$sc" --sig "$sig" "$@" \
        --rpc-url "$rpc" --broadcast --private-key "$key" --gas-price "$gas_price" --skip-simulation 2>&1)
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
            --gas-limit 1500000 --gas-price "$(gas_price_for "$L2")" \
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
if [[ "$MODE" == inbound || "$MODE" == mixed || "$MODE" == mixed-pure ]]; then
    IN_VALUE_PROXY=$(create_l1_proxy "$L2_VALUE")
    IN_NORET_PROXY=$(create_l1_proxy "$L2_VALUE_NORET")
    IN_DEP_PROXY=$(create_l1_proxy "$L2_DEP_RECIPIENT")
    [[ -n "$IN_VALUE_PROXY" && -n "$IN_NORET_PROXY" && -n "$IN_DEP_PROXY" ]] \
        || { echo "inbound proxy creation failed"; exit 1; }
    echo "    inbound proxies: setter=$IN_VALUE_PROXY noret=$IN_NORET_PROXY deposit=$IN_DEP_PROXY"
    # Inbound wrapper on L1 over the setter proxy.
    IN_WRAPPER=$(forge_deploy "$L1" "$HH_KEY_0" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$IN_VALUE_PROXY" | grab EEZ_SETTER_WRAPPER)
    echo "    inbound wrapper (L1) = $IN_WRAPPER"
fi
if [[ "$MODE" == outbound || "$MODE" == mixed || "$MODE" == mixed-pure ]]; then
    OUT_VALUE_PROXY=$(create_l2_proxy "$L1_VALUE")
    OUT_NORET_PROXY=$(create_l2_proxy "$L1_VALUE_NORET")
    OUT_WD_PROXY=$(create_l2_proxy "$L1_WD_RECIPIENT")
    [[ -n "$OUT_VALUE_PROXY" && -n "$OUT_NORET_PROXY" && -n "$OUT_WD_PROXY" ]] \
        || { echo "outbound proxy creation failed"; exit 1; }
    echo "    outbound proxies: setter=$OUT_VALUE_PROXY noret=$OUT_NORET_PROXY withdraw=$OUT_WD_PROXY"
    # Outbound wrapper on L2 over the outbound setter proxy.
    OUT_WRAPPER=$(forge_deploy "$L2" "$HH_KEY_2" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$OUT_VALUE_PROXY" | grab EEZ_SETTER_WRAPPER)
    echo "    outbound wrapper (L2) = $OUT_WRAPPER"
fi

echo
echo
echo "==> setup complete; running waves"
RECEIPT_WAIT_SECS="${EEZ_RECEIPT_WAIT_SECS:-300}"
WAVE_GAP_SECS="${EEZ_WAVE_GAP_SECS:-20}"
FILLER_PER_GAP="${EEZ_FILLER_PER_GAP:-2}"
PURE_RECIPIENT=0x2222222222222222222222222222222222222222

refresh_node_log() { kurtosis service logs "$ENCLAVE" eez-node >"$NODE_LOG" 2>&1 || true; }
strip_ansi() { sed 's/\x1b\[[0-9;]*m//g'; }

# receipt_status <hash> <rpc> → "1" mined-ok, "0x0" reverted, "missing"
receipt_status() {
    local r st
    r=$(timeout 3 curl -s -X POST -H 'Content-Type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$1\"],\"id\":1}" \
        "$2" 2>/dev/null)
    st=$(echo "$r" | jq -r '.result.status // "missing"' 2>/dev/null)
    [[ "$st" == "0x1" ]] && echo "1" || echo "${st:-missing}"
}

wait_nonce_at_least() {
    local rpc="$1" addr="$2" want="$3" label="$4"
    local wait_end=$(( SECONDS + RECEIPT_WAIT_SECS )) got
    while (( SECONDS < wait_end )); do
        got=$(retry cast nonce "$addr" --rpc-url "$rpc")
        (( got >= want )) && return 0
        sleep 5
    done
    echo "    ✗ timed out waiting for $label nonce >= $want" >&2
    return 1
}

# send_front <front_url> <raw_tx> — eth_sendRawTransaction to a cross-chain
# front; fails loud if the admission gate rejects (invariant 7 is LOUD).
send_front() {
    local resp
    resp=$(curl -s -X POST "$1" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$2\"],\"id\":1}")
    if grep -q '"error"' <<<"$resp"; then
        echo "    ✗ front rejected tx: $resp" >&2
        return 1
    fi
}

run_waves() {
    local do_in=0 do_out=0 do_pure=0
    case "$MODE" in
        inbound)    do_in=1 ;;
        outbound)   do_out=1 ;;
        mixed)      do_in=1; do_out=1 ;;
        mixed-pure) do_in=1; do_out=1; do_pure=1 ;;
        *) echo "wave-test: unknown mode '$MODE'"; exit 1 ;;
    esac

    # ── Baselines (deltas asserted at the end) ───────────────────────
    local DEP_BEFORE=0 WD_BEFORE=0
    (( do_in ))  && DEP_BEFORE=$(retry cast balance "$L2_DEP_RECIPIENT" --rpc-url "$L2")
    (( do_out )) && WD_BEFORE=$(retry cast balance "$L1_WD_RECIPIENT" --rpc-url "$L1")

    # ── Local nonce chains (see header) ──────────────────────────────
    local IN_NONCE OUT_NONCE PURE_NONCE PURE_ADDR
    (( do_in ))  && IN_NONCE=$(retry cast nonce "$HH_ADDR_IN" --rpc-url "$L1")
    (( do_out )) && OUT_NONCE=$(retry cast nonce "$HH_ADDR_OUT" --rpc-url "$L2")
    if (( do_pure )); then
        PURE_ADDR=$(cast wallet address --private-key "$HH_KEY_PURE")
        PURE_NONCE=$(retry cast nonce "$PURE_ADDR" --rpc-url "$L2")
    fi
    local IN_WAVE_TARGET=0 OUT_WAVE_TARGET=0

    # Per-tx metadata for the confirmed-view tally: "hash|side|kind|arg".
    # side=in|out; kind=set|noret|wrap|dep|wd.
    local TX_META=()
    local IN_HASHES=() OUT_HASHES=()

    # mk_and_send <side> <kind> <arg>
    #   in  set/noret/wrap/dep → L1-signed tx via the L1 front
    #   out set/noret/wrap/wd  → L2-signed tx via the L2 front
    mk_and_send() {
        local side="$1" kind="$2" arg="$3" raw="" hash
        local GP PG GP2 PG2
        GP=$(gas_price_for "$L1"); PG=$(priority_fee_for "$GP")
        GP2=$(gas_price_for "$L2"); PG2=$(priority_fee_for "$GP2")
        case "$side:$kind" in
            in:set)   raw=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "$HH_KEY_IN" --nonce "$IN_NONCE" \
                        --gas-limit 600000 --gas-price "$GP" --priority-gas-price "$PG" \
                        "$IN_VALUE_PROXY" 'setValue(uint256)' "$arg") ;;
            in:noret) raw=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "$HH_KEY_IN" --nonce "$IN_NONCE" \
                        --gas-limit 600000 --gas-price "$GP" --priority-gas-price "$PG" \
                        "$IN_NORET_PROXY" 'setValue(uint256)' "$arg") ;;
            in:wrap)  raw=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "$HH_KEY_IN" --nonce "$IN_NONCE" \
                        --gas-limit 800000 --gas-price "$GP" --priority-gas-price "$PG" \
                        "$IN_WRAPPER" 'setViaProxy(uint256)' "$arg") ;;
            in:dep)   raw=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "$HH_KEY_IN" --nonce "$IN_NONCE" \
                        --gas-limit 600000 --gas-price "$GP" --priority-gas-price "$PG" --value "$arg" \
                        "$IN_DEP_PROXY") ;;
            out:set)   raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 600000 --gas-price "$GP2" --priority-gas-price "$PG2" \
                        "$OUT_VALUE_PROXY" 'setValue(uint256)' "$arg") ;;
            out:noret) raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 600000 --gas-price "$GP2" --priority-gas-price "$PG2" \
                        "$OUT_NORET_PROXY" 'setValue(uint256)' "$arg") ;;
            out:wrap)  raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 800000 --gas-price "$GP2" --priority-gas-price "$PG2" \
                        "$OUT_WRAPPER" 'setViaProxy(uint256)' "$arg") ;;
            out:wd)    raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 600000 --gas-price "$GP2" --priority-gas-price "$PG2" --value "$arg" \
                        "$OUT_WD_PROXY") ;;
            *) echo "wave-test: bad op $side:$kind"; exit 1 ;;
        esac
        [[ "$raw" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "    ✗ mktx failed ($side:$kind): $raw"; exit 1; }
        hash=$(cast keccak "$raw")
        if [[ "$side" == in ]]; then
            send_front "$L1F" "$raw" || exit 1
            IN_HASHES+=("$hash"); IN_NONCE=$((IN_NONCE + 1))
        else
            send_front "$L2F" "$raw" || exit 1
            OUT_HASHES+=("$hash"); OUT_NONCE=$((OUT_NONCE + 1))
        fi
        TX_META+=("$hash|$side|$kind|$arg")
    }

    submit_pure_filler() {
        local count="$1" j raw gp
        gp=$(gas_price_for "$L2")
        for ((j=0; j<count; j++)); do
            raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_PURE" --nonce "$PURE_NONCE" \
                --gas-limit 21000 --gas-price "$gp" --priority-gas-price "$(priority_fee_for "$gp")" \
                --value 100000000 "$PURE_RECIPIENT" 2>&1)
            [[ "$raw" =~ ^0x[0-9a-fA-F]+$ ]] || break
            curl -s -X POST "$L2" -H 'Content-Type: application/json' \
                -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":9}" >/dev/null
            PURE_NONCE=$((PURE_NONCE + 1))
            sleep 1
        done
    }

    # ── Waves ─────────────────────────────────────────────────────────
    # Per wave: direct setter, noret setter, value transfer, then the wrapper
    # LAST so the wrapper's value is the expected final Value.value() (both
    # write the same target through the same proxy, in submission order).
    local w
    echo
    echo "==> firing $WAVES wave(s), mode=$MODE"
    for ((w=1; w<=WAVES; w++)); do
        echo "── wave $w/$WAVES"
        if (( do_in )); then
            mk_and_send in set   $((100 + w))
            mk_and_send in noret $((200 + w))
            mk_and_send in dep   $((w * 10000000000000))          # w * 1e13 wei
            mk_and_send in wrap  $((300 + w))
            IN_WAVE_TARGET="$IN_NONCE"
            echo "    inbound: 4 ops via L1 front (set/noret/dep/wrap)"
        fi
        if (( do_out )); then
            mk_and_send out set   $((400 + w))
            mk_and_send out noret $((500 + w))
            mk_and_send out wd    $((w * 20000000000000))         # w * 2e13 wei
            mk_and_send out wrap  $((600 + w))
            OUT_WAVE_TARGET="$OUT_NONCE"
            echo "    outbound: 4 ops via L2 front (set/noret/wd/wrap)"
        fi
        (( do_pure )) && { submit_pure_filler "$FILLER_PER_GAP"; echo "    pure: $FILLER_PER_GAP L2 filler txs"; }
        if (( w < WAVES )); then
            if (( do_in )); then
                wait_nonce_at_least "$L1" "$HH_ADDR_IN" "$IN_WAVE_TARGET" "inbound sender" || exit 1
            fi
            if (( do_out )); then
                wait_nonce_at_least "$L2" "$HH_ADDR_OUT" "$OUT_WAVE_TARGET" "outbound sender" || exit 1
            fi
        fi
        sleep "$WAVE_GAP_SECS"
    done

    # ── Wait for inclusion ─────────────────────────────────────────────
    # inbound → L1 receipts, outbound → L2 receipts; evictions count as
    # resolved (the harness then judges convergence on the CONFIRMED view).
    local total=$(( ${#IN_HASHES[@]} + ${#OUT_HASHES[@]} ))
    echo
    echo "==> waiting up to ${RECEIPT_WAIT_SECS}s for $total cross-chain inclusions"
    local wait_end=$(( SECONDS + RECEIPT_WAIT_SECS )) confirmed evicted h last_line=""
    while (( SECONDS < wait_end )); do
        confirmed=0
        for h in "${IN_HASHES[@]:-}";  do [[ -n "$h" && "$(receipt_status "$h" "$L1")" == "1" ]] && confirmed=$((confirmed+1)); done
        for h in "${OUT_HASHES[@]:-}"; do [[ -n "$h" && "$(receipt_status "$h" "$L2")" == "1" ]] && confirmed=$((confirmed+1)); done
        refresh_node_log
        evicted=$(grep -c "evicted" "$NODE_LOG" 2>/dev/null || true); evicted=${evicted:-0}
        local line="    progress: $confirmed/$total confirmed, $evicted eviction log line(s) (elapsed ${SECONDS}s)"
        [[ "$line" != "$last_line" ]] && { echo "$line"; last_line="$line"; }
        (( confirmed >= total )) && { echo "    all confirmed"; break; }
        sleep 5
    done
    echo "    settling 15s..."; sleep 15
    refresh_node_log

    # ── Confirmed view (only receipt-confirmed ops count) ──────────────
    local m mh mside mkind marg
    local IN_LAST_VALUE="" IN_LAST_NORET="" IN_DEP_SUM=0
    local OUT_LAST_VALUE="" OUT_LAST_NORET="" OUT_WD_SUM=0
    for m in "${TX_META[@]:-}"; do
        [[ -n "$m" ]] || continue
        IFS='|' read -r mh mside mkind marg <<<"$m"
        if [[ "$mside" == in ]]; then
            [[ "$(receipt_status "$mh" "$L1")" == "1" ]] || continue
            case "$mkind" in
                set|wrap) IN_LAST_VALUE="$marg" ;;
                noret)    IN_LAST_NORET="$marg" ;;
                dep)      IN_DEP_SUM=$((IN_DEP_SUM + marg)) ;;
            esac
        else
            [[ "$(receipt_status "$mh" "$L2")" == "1" ]] || continue
            case "$mkind" in
                set|wrap) OUT_LAST_VALUE="$marg" ;;
                noret)    OUT_LAST_NORET="$marg" ;;
                wd)       OUT_WD_SUM=$((OUT_WD_SUM + marg)) ;;
            esac
        fi
    done

    # ── Assertions ──────────────────────────────────────────────────────
    echo
    echo "==> assertions"
    local ok_all=1

    check_eq() { # <label> <actual> <expected>
        if [[ "$2" == "$3" && -n "$3" ]]; then
            echo "    ✓ $1: $2"
        else
            echo "    ✗ $1: actual=$2 expected=$3"; ok_all=0
        fi
    }

    if (( do_in )); then
        local v n d
        v=$(retry cast call "$L2_VALUE" 'value()(uint256)' --rpc-url "$L2")
        n=$(retry cast call "$L2_VALUE_NORET" 'value()(uint256)' --rpc-url "$L2")
        d=$(retry cast balance "$L2_DEP_RECIPIENT" --rpc-url "$L2")
        check_eq "inbound setter converged (L2 Value.value)"       "$v" "$IN_LAST_VALUE"
        check_eq "inbound noret converged (L2 ValueNoRet.value)"   "$n" "$IN_LAST_NORET"
        check_eq "inbound deposits converged (L2 recipient bal)"   "$d" "$((DEP_BEFORE + IN_DEP_SUM))"
    fi
    if (( do_out )); then
        local v n d
        v=$(retry cast call "$L1_VALUE" 'value()(uint256)' --rpc-url "$L1")
        n=$(retry cast call "$L1_VALUE_NORET" 'value()(uint256)' --rpc-url "$L1")
        d=$(retry cast balance "$L1_WD_RECIPIENT" --rpc-url "$L1")
        check_eq "outbound setter converged (L1 Value.value)"      "$v" "$OUT_LAST_VALUE"
        check_eq "outbound noret converged (L1 ValueNoRet.value)"  "$n" "$OUT_LAST_NORET"
        check_eq "outbound withdrawals converged (L1 recipient)"   "$d" "$((WD_BEFORE + OUT_WD_SUM))"
    fi

    # postBatches actually landed on L1 (the original bundle-drop symptom).
    local PB_COUNT
    PB_COUNT=$(cast logs --address "$EEZ_REGISTRY_ADDRESS" \
        --from-block "${EEZ_REGISTRY_DEPLOY_BLOCK:-0}" --to-block latest \
        "BatchPosted(uint256)" --rpc-url "$L1" --json 2>/dev/null | jq 'length' 2>/dev/null || echo 0)
    if (( PB_COUNT >= WAVES )); then
        echo "    ✓ postBatches on L1: $PB_COUNT (≥ $WAVES waves)"
    else
        echo "    ✗ postBatches on L1: $PB_COUNT (expected ≥ $WAVES)"; ok_all=0
    fi

    # L1's stored state root == L2's actual root at the last SETTLED Sync height.
    # Pin the L1 read to the l1_block that settled this sync_height (same log
    # line) instead of "latest" — the L1 tip keeps advancing while this runs,
    # so a live read can race ahead of the log-derived height.
    local LAST_SETTLED_LINE LAST_SETTLED LAST_SETTLED_L1_BLOCK L1_TRACKED L2_ROOT
    LAST_SETTLED_LINE=$(strip_ansi <"$NODE_LOG" | grep "bundle outcome observed" | grep "settled=true" \
        | awk '{ if (match($0, /sync_height=[0-9]+/)) print substr($0, RSTART+12, RLENGTH-12)"\t"$0 }' \
        | sort -n -k1,1 | tail -1 | cut -f2- || true)
    LAST_SETTLED=$(echo "$LAST_SETTLED_LINE" | grep -oE "sync_height=[0-9]+" | grep -oE "[0-9]+" || true)
    LAST_SETTLED_L1_BLOCK=$(echo "$LAST_SETTLED_LINE" | grep -oE "l1_block: [0-9]+" | grep -oE "[0-9]+" || true)
    if [[ -n "$LAST_SETTLED" ]]; then
        if [[ -n "$LAST_SETTLED_L1_BLOCK" ]]; then
            L1_TRACKED=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint256)(address,bytes32,uint256)' \
                "$EEZ_ROLLUP_ID" --rpc-url "$L1" --block "$LAST_SETTLED_L1_BLOCK" | sed -n '2p' | tr -d '[:space:]')
        else
            L1_TRACKED=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint256)(address,bytes32,uint256)' \
                "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
        fi
        L2_ROOT=$(cast block "$LAST_SETTLED" --rpc-url "$L2" --json | jq -r '.stateRoot')
        if [[ "${L1_TRACKED,,}" == "${L2_ROOT,,}" ]]; then
            echo "    ✓ L1 rollups($EEZ_ROLLUP_ID).stateRoot == L2 root at settled height $LAST_SETTLED"
        else
            echo "    ✗ L1 stateRoot $L1_TRACKED != L2 $L2_ROOT at height $LAST_SETTLED"; ok_all=0
        fi
    else
        echo "    ✗ no settled bundle found in the node log (grep 'settled=true')"; ok_all=0
    fi

    # Zero divergence (legacy check is hard; deriver-side WARNs are residual).
    local DIVERGED
    DIVERGED=$(grep -c "local L2 state root differs" "$NODE_LOG" 2>/dev/null || true); DIVERGED=${DIVERGED:-0}
    if (( DIVERGED == 0 )); then
        echo "    ✓ zero state-root divergence events"
    else
        echo "    ✗ $DIVERGED state-root divergence event(s)"; ok_all=0
    fi

    # Dropped-bundle telemetry.
    local DROPS
    DROPS=$(grep -c "bundle dropped" "$NODE_LOG" 2>/dev/null || true); DROPS=${DROPS:-0}
    echo "    ℹ dropped-bundle log lines: $DROPS"

    echo
    if (( ok_all )); then
        echo "==> WAVE TEST PASSED (mode=$MODE waves=$WAVES, $total cross-chain ops, $PB_COUNT PBs)"
        exit 0
    else
        echo "==> WAVE TEST FAILED (mode=$MODE)"
        exit 1
    fi
}
run_waves
