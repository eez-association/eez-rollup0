#!/usr/bin/env bash
# Run cross-chain waves against the CI enclave.
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
# Requires cast, forge, jq, curl, kurtosis, and openssl.

set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

K="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
LOG_DIR="$REPO/datadir/smoke-logs"
mkdir -p "$LOG_DIR"

MODE="${EEZ_WAVE_MODE:-mixed}"
WAVES="${EEZ_WAVE_COUNT:-3}"

for t in cast forge jq curl kurtosis openssl; do command -v "$t" >/dev/null || { echo "$t not in PATH"; exit 1; }; done

# L1 is the canonical shared chain; fronts are published by eez-node.
# shellcheck disable=SC1091
source "$K/ports.sh" >/dev/null
# shellcheck disable=SC1091
source "$K/scripts/lib.sh"
: "${L1:=$EEZ_DEVNET_L1_RPC}"
: "${L2:=$EEZ_DEVNET_L2_RPC}"
: "${L1F:=$EEZ_DEVNET_L1_FRONT}"
: "${L2F:=$EEZ_DEVNET_L2_FRONT}"

NODE_LOG="${EEZ_NODE_LOG:-$LOG_DIR/wave-$MODE-node.log}"
SIGNER_LOG="${EEZ_PROOF_SIGNER_LOG:-$LOG_DIR/wave-$MODE-proof-signer.log}"
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
HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a   # L2 contract deployer / L2 proxy creator
# Fresh users avoid stale held-pool nonce state from earlier interrupted runs.
HH_KEY_REV_IN="0x$(openssl rand -hex 32)"
HH_ADDR_REV_IN=$(cast wallet address --private-key "$HH_KEY_REV_IN")
HH_KEY_REV_OUT="0x$(openssl rand -hex 32)"
HH_ADDR_REV_OUT=$(cast wallet address --private-key "$HH_KEY_REV_OUT")
HH_KEY_IN="${EEZ_WAVE_IN_KEY:-0x$(openssl rand -hex 32)}"
HH_ADDR_IN=$(cast wallet address --private-key "$HH_KEY_IN")
HH_KEY_OUT="${EEZ_WAVE_OUT_KEY:-0x$(openssl rand -hex 32)}"
HH_ADDR_OUT=$(cast wallet address --private-key "$HH_KEY_OUT")
# Pure-L2 filler user.
HH_KEY_PURE=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a  # #2 (L2 deployer, idle at wave time)

# EOAs funded on L1 so they can pay gas on the shared chain.
L1_FUNDED_KEYS=("$HH_KEY_IN")
FUND_FROM_KEY="${EEZ_FUND_FROM_KEY:-$(yaml_value poster_key)}"
[[ -n "$FUND_FROM_KEY" ]] || { echo "could not resolve a funding key — set EEZ_FUND_FROM_KEY or eez.poster_key"; exit 1; }
L1_SETUP_KEY="${EEZ_L1_SETUP_KEY:-$FUND_FROM_KEY}"

EEZL2_ADDRESS="${EEZL2_ADDRESS:-0x4200000000000000000000000000000000000007}"
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

# ── Reachability ─────────────────────────────────────────────────────
L1_UP=$(cast block-number --rpc-url "$L1" 2>/dev/null || echo "")
[[ -n "$L1_UP" ]] || { echo "L1 RPC $L1 not reachable — is the enclave up?"; exit 1; }
L2_UP=$(cast block-number --rpc-url "$L2" 2>/dev/null || echo "")
[[ -n "$L2_UP" ]] || { echo "L2 RPC $L2 not reachable"; exit 1; }
echo "    L1=$L1_UP L2=$L2_UP"

fund_l1() { fund "$L1" "$FUND_FROM_KEY" "$1"; }
fund_l2() { fund "$L2" "$HH_KEY_2" "$1"; }

# ── Fund L1-side actors ──────────────────────────────────────────────
for k in "${L1_FUNDED_KEYS[@]}"; do
    a=$(cast wallet address --private-key "$k")
    if [[ "$(cast balance "$a" --rpc-url "$L1" 2>/dev/null || echo 0)" == "0" ]]; then
        echo "==> funding $a on L1 (10 ETH)"
        fund_l1 "$a" || { echo "failed to fund $a — is the funding key funded on L1?"; exit 1; }
    fi
done

if [[ "${EEZ_INCLUDE_REVERTS:-0}" == "1" ]]; then
    echo "==> funding revert senders (L1 $HH_ADDR_REV_IN / L2 $HH_ADDR_REV_OUT)"
    fund_l1 "$HH_ADDR_REV_IN" || { echo "failed to fund revert sender on L1"; exit 1; }
    fund_l2 "$HH_ADDR_REV_OUT" || { echo "failed to fund revert sender on L2"; exit 1; }
fi

if [[ "$(cast balance "$HH_ADDR_OUT" --rpc-url "$L2" 2>/dev/null || echo 0)" == "0" ]]; then
    echo "==> funding $HH_ADDR_OUT on L2 (10 ETH)"
    fund_l2 "$HH_ADDR_OUT" || { echo "failed to fund $HH_ADDR_OUT on L2"; exit 1; }
fi

# ── Deploy L2 targets (Value + ValueNoRet) ───────────────────────────
echo "==> deploying L2 targets (Value, ValueNoRet)"
L2_VALUE=$(forge_deploy "$L2" "$HH_KEY_2" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab_address EEZ_VALUE_ADDRESS)
L2_VALUE_NORET=$(forge_deploy "$L2" "$HH_KEY_2" DeployValueNoRetL2.s.sol:DeployValueNoRetL2 'run(uint256)' 0 | grab_address EEZ_VALUE_NORET_ADDRESS)
[[ -n "$L2_VALUE" && -n "$L2_VALUE_NORET" ]] || { echo "L2 target deploy failed"; exit 1; }
echo "    L2 Value=$L2_VALUE  ValueNoRet=$L2_VALUE_NORET"

# ── Deploy L1 outbound targets (Value + ValueNoRet on L1) ────────────
if [[ "$MODE" == outbound || "$MODE" == mixed || "$MODE" == mixed-pure ]]; then
    echo "==> deploying L1 outbound targets (Value, ValueNoRet on L1)"
    L1_VALUE=$(forge_deploy "$L1" "$L1_SETUP_KEY" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 | grab_address EEZ_VALUE_ADDRESS)
    L1_VALUE_NORET=$(forge_deploy "$L1" "$L1_SETUP_KEY" DeployValueNoRetL2.s.sol:DeployValueNoRetL2 'run(uint256)' 0 | grab_address EEZ_VALUE_NORET_ADDRESS)
    [[ -n "$L1_VALUE" && -n "$L1_VALUE_NORET" ]] || { echo "L1 target deploy failed"; exit 1; }
    echo "    L1 Value=$L1_VALUE  ValueNoRet=$L1_VALUE_NORET"
fi

L1_CHAIN_ID=$(cast chain-id --rpc-url "$L1")
L2_CHAIN_ID=$(cast chain-id --rpc-url "$L2")
# ── Helpers: create an L1 (inbound) proxy and an L2 (outbound) proxy ──
# L1 proxy = createCrossChainProxy(target_on_L2, rid=EEZ_ROLLUP_ID) on the L1 EEZ.
create_l1_proxy() { # <target_on_L2> → proxy addr
    forge_deploy "$L1" "$L1_SETUP_KEY" CreateValueProxy.s.sol:CreateValueProxy \
        'run(address,address,uint64)' "$EEZ_REGISTRY_ADDRESS" "$1" "$EEZ_ROLLUP_ID" | grab_address EEZ_VALUE_PROXY
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
    IN_WRAPPER=$(forge_deploy "$L1" "$L1_SETUP_KEY" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$IN_VALUE_PROXY" | grab_address EEZ_SETTER_WRAPPER)
    echo "    inbound wrapper (L1) = $IN_WRAPPER"
fi
if [[ "$MODE" == outbound || "$MODE" == mixed || "$MODE" == mixed-pure ]]; then
    OUT_VALUE_PROXY=$(create_l2_proxy "$L1_VALUE" "$HH_KEY_2" "$MAINNET_RID")
    OUT_NORET_PROXY=$(create_l2_proxy "$L1_VALUE_NORET" "$HH_KEY_2" "$MAINNET_RID")
    OUT_WD_PROXY=$(create_l2_proxy "$L1_WD_RECIPIENT" "$HH_KEY_2" "$MAINNET_RID")
    [[ -n "$OUT_VALUE_PROXY" && -n "$OUT_NORET_PROXY" && -n "$OUT_WD_PROXY" ]] \
        || { echo "outbound proxy creation failed"; exit 1; }
    echo "    outbound proxies: setter=$OUT_VALUE_PROXY noret=$OUT_NORET_PROXY withdraw=$OUT_WD_PROXY"
    # Outbound wrapper on L2 over the outbound setter proxy.
    OUT_WRAPPER=$(forge_deploy "$L2" "$HH_KEY_2" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$OUT_VALUE_PROXY" | grab_address EEZ_SETTER_WRAPPER)
    echo "    outbound wrapper (L2) = $OUT_WRAPPER"
fi

echo
echo
echo "==> setup complete; running waves"
RECEIPT_WAIT_SECS="${EEZ_RECEIPT_WAIT_SECS:-300}"
WAVE_GAP_SECS="${EEZ_WAVE_GAP_SECS:-20}"
FILLER_PER_GAP="${EEZ_FILLER_PER_GAP:-2}"
# One reverting cross-chain call per side per wave (bogus selector, no
# fallback). They must NOT settle, and must not disturb the rest.
INCLUDE_REVERTS="${EEZ_INCLUDE_REVERTS:-0}"
PURE_RECIPIENT=0x2222222222222222222222222222222222222222

refresh_node_log() { docker logs "$(docker ps --format "{{.Names}}" | grep -m1 "eez-node--")" >"$NODE_LOG" 2>&1 || true; }
refresh_signer_log() { docker logs "$(docker ps --format "{{.Names}}" | grep -m1 "eez-proof-signer--")" >"$SIGNER_LOG" 2>&1 || true; }

# The relay needs a few L1 slots before it includes anything. Firing into that
# window burns MAX_BUNDLE_ATTEMPTS and evicts the ops as poison — a harness
# artifact that looks exactly like a node bug.
# Count a registry event from a block. Baselined per run: several modes share
# one enclave, so counting from the deploy block would count their events too.
registry_events() { # <event-sig> [from-block]
    cast logs --address "$EEZ_REGISTRY_ADDRESS" \
        --from-block "${2:-${EEZ_REGISTRY_DEPLOY_BLOCK:-0}}" --to-block latest \
        "$1" --rpc-url "$L1" --json 2>/dev/null | jq 'length' 2>/dev/null || echo 0
}

settled_count() { refresh_node_log; strip_ansi <"$NODE_LOG" | grep -c "settled=true" || true; }

wait_for_builder() {
    local deadline=$(( SECONDS + ${EEZ_BUILDER_WARM_SECS:-600} )) base hits
    # Baseline first: an earlier mode's settlements are still in the log.
    base=$(settled_count)
    echo "==> waiting for the builder to include a bundle"
    while :; do
        # `-c` not `-q`: -q exits on first match, SIGPIPEs sed, and pipefail
        # then reports the successful pipeline as failed.
        hits=$(settled_count)
        if (( ${hits:-0} > ${base:-0} )); then
            echo "    ✓ builder is including bundles ($((hits - base)) new this run)"; return 0
        fi
        (( SECONDS < deadline )) || {
            echo "    ✗ no bundle included in ${EEZ_BUILDER_WARM_SECS:-600}s"; return 1
        }
        sleep 10
    done
}

# The signal fires only after the held-pool chain eviction completes, so the
# receipt check below cannot race it.
wait_for_poison_eviction() {
    local hash="$1" rpc="$2" direction="$3" label="$4"
    local deadline=$((SECONDS + ${EEZ_REVERT_EVICTION_WAIT_SECS:-60})) evidence
    while (( SECONDS < deadline )); do
        refresh_node_log
        evidence=$(strip_ansi <"$NODE_LOG" \
            | grep -F 'eez.composer.cc_compose.poison_eviction_completed' \
            | grep -E "tx_hash=$hash|\"tx_hash\":\"$hash\"" \
            | grep -E "direction=\"?$direction\"?|\"direction\":\"$direction\"" \
            | tail -1 || true)
        if [[ -n "$evidence" && "$(receipt_status "$hash" "$rpc")" == "missing" ]]; then
            return 0
        fi
        sleep 2
    done
    echo "    ✗ $label was not poison-evicted without a source receipt: $hash" >&2
    return 1
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

run_waves() {
    local do_in=0 do_out=0 do_pure=0
    case "$MODE" in
        inbound)    do_in=1 ;;
        outbound)   do_out=1 ;;
        mixed)      do_in=1; do_out=1 ;;
        mixed-pure) do_in=1; do_out=1; do_pure=1 ;;
        *) echo "cross-chain wave: unknown mode '$MODE'"; exit 1 ;;
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
    local REV_IN_HASHES=() REV_OUT_HASHES=()
    local REV_IN_NONCE=0 REV_OUT_NONCE=0

    # mk_and_send <side> <kind> <arg>
    #   in  set/noret/wrap/dep → L1-signed tx via the L1 front
    #   out set/noret/wrap/wd  → L2-signed tx via the L2 front
    mk_and_send() {
        local side="$1" kind="$2" arg="$3" raw="" hash
        local GP PG
        GP=$(gas_price_for "$L1")
        PG="$PRIORITY_GAS_PRICE"
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
                        --gas-limit 600000 --gas-price "$(gas_price_for "$L2")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
                        "$OUT_VALUE_PROXY" 'setValue(uint256)' "$arg") ;;
            out:noret) raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 600000 --gas-price "$(gas_price_for "$L2")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
                        "$OUT_NORET_PROXY" 'setValue(uint256)' "$arg") ;;
            out:wrap)  raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 800000 --gas-price "$(gas_price_for "$L2")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
                        "$OUT_WRAPPER" 'setViaProxy(uint256)' "$arg") ;;
            out:wd)    raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 600000 --gas-price "$(gas_price_for "$L2")" --priority-gas-price "$PRIORITY_GAS_PRICE" --value "$arg" \
                        "$OUT_WD_PROXY") ;;
            in:rev)   raw=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "$HH_KEY_REV_IN" --nonce "$REV_IN_NONCE" \
                        --gas-limit 600000 --gas-price "$GP" --priority-gas-price "$PG" \
                        "$IN_VALUE_PROXY" 'noSuchFunction()') ;;
            out:rev)  raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_REV_OUT" --nonce "$REV_OUT_NONCE" \
                        --gas-limit 600000 --gas-price "$(gas_price_for "$L2")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
                        "$OUT_VALUE_PROXY" 'noSuchFunction()') ;;
            *) echo "cross-chain wave: bad op $side:$kind"; exit 1 ;;
        esac
        [[ "$raw" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "    ✗ mktx failed ($side:$kind): $raw"; exit 1; }
        hash=$(cast keccak "$raw")
        if [[ "$side" == in ]]; then
            send_front "$L1F" "$raw" "$hash" || exit 1
            if [[ "$kind" == rev ]]; then REV_IN_HASHES+=("$hash"); REV_IN_NONCE=$((REV_IN_NONCE + 1));
            else IN_HASHES+=("$hash"); IN_NONCE=$((IN_NONCE + 1)); fi
        else
            send_front "$L2F" "$raw" "$hash" || exit 1
            if [[ "$kind" == rev ]]; then REV_OUT_HASHES+=("$hash"); REV_OUT_NONCE=$((REV_OUT_NONCE + 1));
            else OUT_HASHES+=("$hash"); OUT_NONCE=$((OUT_NONCE + 1)); fi
        fi
        TX_META+=("$hash|$side|$kind|$arg")
    }

    submit_pure_filler() {
        local count="$1" j raw
        for ((j=0; j<count; j++)); do
            raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_PURE" --nonce "$PURE_NONCE" \
                --gas-limit 21000 --gas-price "$(gas_price_for "$L2")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
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
    # Baseline for the per-run event counts below.
    # +1: the current head is already mined, so its events predate this run.
    L1_FIRST_COUNTED_BLOCK=$(( $(retry cast block-number --rpc-url "$L1") + 1 ))
    # Here, not at setup: the helpers it calls are defined above this point.
    wait_for_builder || return 1
    echo
    echo "==> firing $WAVES wave(s), mode=$MODE"
    for ((w=1; w<=WAVES; w++)); do
        echo "── wave $w/$WAVES"
        if (( do_in )); then
            mk_and_send in set   $((100 + w))
            mk_and_send in noret $((200 + w))
            mk_and_send in dep   $((w * 10000000000000))          # w * 1e13 wei
            mk_and_send in wrap  $((300 + w))
            (( INCLUDE_REVERTS && w == 1 )) && mk_and_send in rev 0
            IN_WAVE_TARGET="$IN_NONCE"
            echo "    inbound: 4 ops via L1 front (set/noret/dep/wrap)"
        fi
        if (( do_out )); then
            mk_and_send out set   $((400 + w))
            mk_and_send out noret $((500 + w))
            mk_and_send out wd    $((w * 5000000000000))          # w * 5e12 wei
            mk_and_send out wrap  $((600 + w))
            (( INCLUDE_REVERTS && w == 1 )) && mk_and_send out rev 0
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
    # inbound → L1 receipts, outbound → L2 receipts.
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
    if (( confirmed != total )); then
        echo "    ✗ only $confirmed/$total cross-chain transactions succeeded" >&2
        exit 1
    fi
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
    local ok_all=1 signer_ok=0 attested_hash=""

    # A deterministic destination failure must be evicted before source-chain
    # execution, leaving no receipt and releasing the sender nonce.
    if (( INCLUDE_REVERTS )); then
        local rh rev_total=0 rev_ok=0
        for rh in "${REV_IN_HASHES[@]:-}"; do
            [[ -n "$rh" ]] || continue
            rev_total=$((rev_total+1))
            if wait_for_poison_eviction "$rh" "$L1" Inbound "inbound revert op"; then
                rev_ok=$((rev_ok+1))
            fi
        done
        for rh in "${REV_OUT_HASHES[@]:-}"; do
            [[ -n "$rh" ]] || continue
            rev_total=$((rev_total+1))
            if wait_for_poison_eviction "$rh" "$L2" Outbound "outbound revert op"; then
                rev_ok=$((rev_ok+1))
            fi
        done
        if (( rev_total > 0 && rev_ok == rev_total )); then
            echo "    ✓ reverting cross-chain calls were poison-evicted: $rev_ok/$rev_total"
        else
            echo "    ✗ reverting-call handling: $rev_ok/$rev_total behaved correctly"; ok_all=0
        fi
    fi

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
    # Counted from THIS run's starting block, not the deploy block.
    local PB_COUNT
    PB_COUNT=$(registry_events "BatchPosted(uint256)" "$L1_FIRST_COUNTED_BLOCK")
    if (( PB_COUNT >= WAVES )); then
        echo "    ✓ postBatches on L1 this run: $PB_COUNT (≥ $WAVES waves)"
    else
        echo "    ✗ postBatches on L1 this run: $PB_COUNT (expected ≥ $WAVES)"; ok_all=0
    fi

    local EXECUTION_COUNT
    EXECUTION_COUNT=$(registry_events "L2ExecutionPerformed(uint64,bytes32)" "$L1_FIRST_COUNTED_BLOCK")
    if (( EXECUTION_COUNT > 0 )); then
        echo "    ✓ L2 execution events on L1 this run: $EXECUTION_COUNT"
    else
        echo "    ✗ no L2ExecutionPerformed event found"; ok_all=0
    fi

    # L1's stored state root must converge with the current L2 safe block.
    local LAST_SETTLED="" L1_TRACKED="" L1_RECHECK="" L2_ROOT="" L2_SAFE=0 SAFE_BLOCK=""
    local root_deadline=$((SECONDS + ${EEZ_STATE_ROOT_WAIT_SECS:-30})) root_matched=0
    LAST_SETTLED=$(strip_ansi <"$NODE_LOG" | grep "bundle outcome observed" | grep "settled=true" \
        | grep -oE "sync_height=[0-9]+" | grep -oE "[0-9]+" | sort -n | tail -1 || true)
    if [[ -n "$LAST_SETTLED" ]]; then
        while (( SECONDS < root_deadline )); do
            L1_TRACKED=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
                "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
            SAFE_BLOCK=$(retry cast block safe --rpc-url "$L2" --json)
            L2_SAFE=$(jq -r '.number' <<<"$SAFE_BLOCK" | xargs cast to-dec)
            L2_ROOT=$(jq -r '.stateRoot' <<<"$SAFE_BLOCK")
            L1_RECHECK=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
                "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
            if [[ "${L1_TRACKED,,}" == "${L1_RECHECK,,}" \
                && "${L1_RECHECK,,}" == "${L2_ROOT,,}" ]]; then
                root_matched=1
                break
            fi
            sleep 1
        done
        if (( root_matched )); then
            echo "    ✓ L1 rollups($EEZ_ROLLUP_ID).stateRoot == L2 safe root at height $L2_SAFE"
        else
            echo "    ✗ L1 stateRoot $L1_RECHECK != L2 safe root $L2_ROOT at height $L2_SAFE"; ok_all=0
        fi
        if (( L2_SAFE >= LAST_SETTLED )); then
            echo "    ✓ L2 safe head reached settled height: $L2_SAFE"
        else
            echo "    ✗ L2 safe head $L2_SAFE is below settled height $LAST_SETTLED"; ok_all=0
        fi
    else
        echo "    ✗ no settled bundle found in the node log (grep 'settled=true')"; ok_all=0
    fi

    # Zero production deriver divergence errors.
    local DIVERGED
    DIVERGED=$(grep -c "diverged from L1-confirmed batch" "$NODE_LOG" 2>/dev/null || true); DIVERGED=${DIVERGED:-0}
    if (( DIVERGED == 0 )); then
        echo "    ✓ zero state-root divergence events"
    else
        echo "    ✗ $DIVERGED state-root divergence event(s)"; ok_all=0
    fi

    # Dropped-bundle telemetry.
    local DROPS
    DROPS=$(grep -c "bundle dropped" "$NODE_LOG" 2>/dev/null || true); DROPS=${DROPS:-0}
    echo "    ℹ dropped-bundle log lines: $DROPS"

    # Correlate the composer's accepted attestation with the signer's completed
    # validation pipeline.
    local signer_line=""
    refresh_node_log
    refresh_signer_log
    attested_hash=$(strip_ansi <"$NODE_LOG" | grep 'remote prover attested the window' \
        | grep -oE 'hash=0x[0-9a-fA-F]{64}' | tail -1 | cut -d= -f2 || true)
    if [[ -n "$attested_hash" ]]; then
        signer_line=$(strip_ansi <"$SIGNER_LOG" \
            | grep -F "recomputed_public_inputs_hash=$attested_hash" | tail -1 || true)
    fi
    if [[ "$signer_line" == *"window validated and signed"* ]]; then
        signer_ok=1
    fi

    echo
    if (( ok_all )); then
        echo "==> WAVE TEST PASSED (mode=$MODE waves=$WAVES, $total cross-chain ops, $PB_COUNT PBs)"
    else
        echo "==> WAVE TEST FAILED (mode=$MODE)"
    fi
    if (( signer_ok )); then
        echo "==> PROOF SIGNER TEST PASSED (publicInputsHash=$attested_hash)"
    else
        echo "==> PROOF SIGNER TEST FAILED (no matching validated signer attestation)"
    fi
    if (( ok_all && signer_ok )); then
        exit 0
    else
        exit 1
    fi
}
run_waves
