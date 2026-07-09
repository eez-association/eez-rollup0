#!/usr/bin/env bash
#
# Continuous inbound load generator for a running EEZ devnet.
#
# It deploys a fresh L2 Value contract, creates L1 cross-chain proxies, then
# sends signed L1 transactions to the inbound cross-chain front for a fixed
# duration. At the end it waits for receipts and checks the L2 effects.

set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"

# Running node endpoints.
L1_RPC="${L1_RPC:-http://localhost:18645}"
L2_RPC="${L2_RPC:-http://localhost:18688}"
XCHAIN_L1="${XCHAIN_L1:-http://localhost:18999}"

# Load profile.
DURATION_SECS="${EEZ_LOAD_DURATION_SECS:-600}"
INTERVAL_SECS="${EEZ_LOAD_INTERVAL_SECS:-5}"
BATCH_SIZE="${EEZ_LOAD_BATCH_SIZE:-1}"
WAIT_RECEIPTS="${EEZ_LOAD_WAIT_RECEIPTS:-1}"
RECEIPT_WAIT_SECS="${EEZ_LOAD_RECEIPT_WAIT_SECS:-900}"
SETTLE_AFTER_RECEIPTS_SECS="${EEZ_LOAD_SETTLE_AFTER_RECEIPTS_SECS:-15}"
ROOT_MATCH_SCAN_BACK="${EEZ_ROOT_MATCH_SCAN_BACK:-5000}"
SENDER_STUCK_SECS="${EEZ_LOAD_SENDER_STUCK_SECS:-180}"
GENERATED_SENDER_COUNT="${EEZ_LOAD_GENERATED_SENDERS:-15}"
GENERATED_SENDER_BASE="${EEZ_LOAD_GENERATED_SENDER_BASE:-0xEE5000}"
NODE_CONTAINER="${NODE_CONTAINER:-eez-node-chiado}"
EVICTION_LOG_TAIL="${EEZ_LOAD_EVICTION_LOG_TAIL:-5000}"
VALUE_INITIAL="${VALUE_INITIAL:-5}"
DEPOSIT_WEI="${EEZ_LOAD_DEPOSIT_WEI:-50000000000000}"
LOAD_FUND_SENDERS="${EEZ_LOAD_FUND_SENDERS:-1}"
LOAD_MIN_BALANCE_WEI="${EEZ_LOAD_MIN_BALANCE_WEI:-50000000000000000}"
LOAD_FUND_WEI="${EEZ_LOAD_FUND_WEI:-500000000000000000}"
GAS_PRICE_WEI="${EEZ_LOAD_GAS_PRICE_WEI:-2000000000}"
PRIORITY_GAS_PRICE_WEI="${EEZ_LOAD_PRIORITY_GAS_PRICE_WEI:-1500000000}"
RESEND_GAS_BUMP_WEI="${EEZ_LOAD_RESEND_GAS_BUMP_WEI:-1000000}"

# Testnet keys, matching scripts/devnet-test.sh defaults.
EEZ_OPERATOR_KEY="${EEZ_OPERATOR_KEY:-0x2248a31395af28e24349c8e566c19475a79cb610389204ab26bc585493e5cf27}"
EEZ_USER_KEY="${EEZ_USER_KEY:-0x3b7b012a74f1c18f714c38306339b6b4124f3a434bd816a1ee1fa5aeb5953efe}"
HH_KEY_2="${HH_KEY_2:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"
if [[ -n "${EEZ_LOAD_PRIVATE_KEYS:-}" ]]; then
    read -r -a SENDER_KEYS <<< "$EEZ_LOAD_PRIVATE_KEYS"
else
    SENDER_KEYS=()
    for ((i=1; i<=GENERATED_SENDER_COUNT; i++)); do
        SENDER_KEYS+=("0x$(printf '%064x' $((GENERATED_SENDER_BASE + i)))")
    done
fi

L2_RECIPIENT="${L2_RECIPIENT:-0x$(openssl rand -hex 20)}"
RUN_LOG="${EEZ_LOAD_LOG:-/tmp/eez-inbound-load.$(date +%Y%m%d-%H%M%S).log}"

retry() {
    local n=0 max="${RETRY_MAX:-6}" delay="${RETRY_DELAY:-3}" out rc
    while :; do
        out=$("$@" 2>&1); rc=$?
        (( rc == 0 )) && { printf '%s' "$out"; return 0; }
        (( ++n >= max )) && { echo "retry: '$*' failed after $n attempts: $out" >&2; return "$rc"; }
        sleep "$delay"
    done
}

pending_nonce() {
    local addr="$1"
    retry cast rpc eth_getTransactionCount "$addr" pending --rpc-url "$L1_RPC" | tr -d '"[:space:]'
}

receipt_status() {
    local tx="$1" r
    r=$(timeout 3 curl -s -X POST -H 'Content-Type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$tx\"],\"id\":1}" \
        "$L1_RPC" 2>/dev/null || true)
    echo "$r" | jq -r '.result.status // "missing"' 2>/dev/null
}

evicted_hash() {
    local tx="$1"
    command -v docker >/dev/null || return 1
    docker logs --tail "$EVICTION_LOG_TAIL" "$NODE_CONTAINER" 2>/dev/null \
        | grep -F "$tx" \
        | grep -q "evicted after MAX_BUNDLE_ATTEMPTS"
}

find_l2_root_height() {
    local want="${1,,}" head floor bn root
    [[ -n "$want" ]] || return 1
    head=$(retry cast block-number --rpc-url "$L2_RPC") || return 1
    floor=0
    (( head > ROOT_MATCH_SCAN_BACK )) && floor=$((head - ROOT_MATCH_SCAN_BACK))
    for ((bn=head; bn>=floor; bn--)); do
        root=$(cast block "$bn" --rpc-url "$L2_RPC" --json 2>/dev/null \
            | jq -r '.stateRoot // empty' 2>/dev/null || true)
        [[ "${root,,}" == "$want" ]] && { echo "$bn"; return 0; }
    done
    return 1
}

for t in cast forge jq curl openssl; do
    command -v "$t" >/dev/null || { echo "$t not in PATH"; exit 1; }
done
[[ -f "$REPO/deployments.env" ]] || { echo "deployments.env missing"; exit 1; }

set -a; source "$REPO/deployments.env"; set +a

L1_CHAIN_ID=$(retry cast chain-id --rpc-url "$L1_RPC")
L2_CHAIN_ID=$(retry cast chain-id --rpc-url "$L2_RPC")
USER_ADDR=$(cast wallet address --private-key "$EEZ_USER_KEY")
OPERATOR_ADDR=$(cast wallet address --private-key "$EEZ_OPERATOR_KEY")

echo "==> inbound load"
echo "    duration=${DURATION_SECS}s interval=${INTERVAL_SECS}s batch_size=$BATCH_SIZE"
echo "    L1=$L1_RPC chain=$L1_CHAIN_ID"
echo "    L2=$L2_RPC chain=$L2_CHAIN_ID"
echo "    inbound front=$XCHAIN_L1"
echo "    senders=${#SENDER_KEYS[@]}"
echo "    run log=$RUN_LOG"

echo
echo "==> deploying Value($VALUE_INITIAL) on L2"
cd "$REPO/contracts"
VALUE_OUT=$(forge script script/DeployValueL2.s.sol:DeployValueL2 \
    --sig "run(uint256)" "$VALUE_INITIAL" \
    --rpc-url "$L2_RPC" --broadcast --private-key "$HH_KEY_2" --gas-price 0 --skip-simulation 2>&1) || true
EEZ_VALUE_ADDRESS=$(echo "$VALUE_OUT" | grep -oE 'EEZ_VALUE_ADDRESS=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2 || true)
[[ -n "$EEZ_VALUE_ADDRESS" ]] || { echo "Value deploy failed"; echo "$VALUE_OUT" | tail -30; exit 1; }
echo "    Value @ $EEZ_VALUE_ADDRESS"
RECIPIENT_BEFORE=$(retry cast balance "$L2_RECIPIENT" --rpc-url "$L2_RPC")

echo "==> creating inbound proxies on L1"
SETTER_OUT=$(forge script script/CreateValueProxy.s.sol:CreateValueProxy \
    --sig "run(address,address,uint256)" "$EEZ_REGISTRY_ADDRESS" "$EEZ_VALUE_ADDRESS" "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
SETTER_PROXY=$(echo "$SETTER_OUT" | grep -oE 'EEZ_VALUE_PROXY=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2 || true)
[[ -n "$SETTER_PROXY" ]] || { echo "setter proxy create failed"; echo "$SETTER_OUT" | tail -30; exit 1; }

DEPOSIT_OUT=$(forge script script/CreateValueProxy.s.sol:CreateValueProxy \
    --sig "run(address,address,uint256)" "$EEZ_REGISTRY_ADDRESS" "$L2_RECIPIENT" "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" --broadcast --private-key "$EEZ_OPERATOR_KEY" --skip-simulation 2>&1) || true
DEPOSIT_PROXY=$(echo "$DEPOSIT_OUT" | grep -oE 'EEZ_VALUE_PROXY=0x[0-9a-fA-F]{40}' | head -1 | cut -d= -f2 || true)
[[ -n "$DEPOSIT_PROXY" ]] || { echo "deposit proxy create failed"; echo "$DEPOSIT_OUT" | tail -30; exit 1; }
cd "$REPO"

echo "    setter proxy=$SETTER_PROXY"
echo "    deposit proxy=$DEPOSIT_PROXY"
echo "    recipient=$L2_RECIPIENT"

SENDER_ADDRS=()
NEXT_NONCES=()
BUSY_HASHES=()
BUSY_NONCES=()
BUSY_SINCE=()
BUSY_WARNED=()
RESEND_BUMPS=()
for idx in "${!SENDER_KEYS[@]}"; do
    addr=$(cast wallet address --private-key "${SENDER_KEYS[$idx]}")
    SENDER_ADDRS[$idx]="$addr"
    bal=$(retry cast balance "$addr" --rpc-url "$L1_RPC")
    if [[ "$LOAD_FUND_SENDERS" == "1" && "$addr" != "$OPERATOR_ADDR" && "$bal" -lt "$LOAD_MIN_BALANCE_WEI" ]]; then
        echo "    funding sender[$idx]=$addr balance=$bal value=$LOAD_FUND_WEI"
        cast send "$addr" --value "$LOAD_FUND_WEI" --rpc-url "$L1_RPC" \
            --private-key "$EEZ_OPERATOR_KEY" >/dev/null
    fi
    nonce_hex=$(pending_nonce "$addr")
    NEXT_NONCES[$idx]=$((nonce_hex))
    BUSY_HASHES[$idx]=""
    BUSY_NONCES[$idx]=""
    BUSY_SINCE[$idx]=0
    BUSY_WARNED[$idx]=0
    RESEND_BUMPS[$idx]=0
    echo "    sender[$idx]=$addr nonce=${NEXT_NONCES[$idx]}"
done

TX_HASHES=()
TX_KINDS=()
TX_ARGS=()
DROPPED_HASHES=()
LAST_SETTER_VALUE=""
TOTAL_DEPOSIT_SUM=0
SENT=0
FAILED=0
NEXT_SENDER=0
LAST_BUSY_LOG=0
END_AT=$((SECONDS + DURATION_SECS))

refresh_sender() {
    local idx="$1" h st nonce_hex age
    h="${BUSY_HASHES[$idx]:-}"
    [[ -z "$h" ]] && return 0
    st=$(receipt_status "$h")
    if [[ "$st" == "0x1" ]]; then
        BUSY_HASHES[$idx]=""
        BUSY_NONCES[$idx]=""
        BUSY_SINCE[$idx]=0
        BUSY_WARNED[$idx]=0
        nonce_hex=$(pending_nonce "${SENDER_ADDRS[$idx]}")
        NEXT_NONCES[$idx]=$((nonce_hex))
        return 0
    fi
    age=$((SECONDS - BUSY_SINCE[$idx]))
    if (( age >= SENDER_STUCK_SECS )); then
        if evicted_hash "$h"; then
            echo "    WARN sender=$idx tx evicted after ${age}s; freeing lane hash=$h nonce=${BUSY_NONCES[$idx]}"
            DROPPED_HASHES+=("$h")
            RESEND_BUMPS[$idx]=$((RESEND_BUMPS[$idx] + 1))
            BUSY_HASHES[$idx]=""
            BUSY_NONCES[$idx]=""
            BUSY_SINCE[$idx]=0
            BUSY_WARNED[$idx]=0
            nonce_hex=$(pending_nonce "${SENDER_ADDRS[$idx]}")
            NEXT_NONCES[$idx]=$((nonce_hex))
            return 0
        elif [[ "${BUSY_WARNED[$idx]}" != "1" ]]; then
            echo "    sender=$idx still held/no receipt after ${age}s; keeping lane busy hash=$h nonce=${BUSY_NONCES[$idx]}"
            BUSY_WARNED[$idx]=1
        fi
    fi
    return 1
}

is_dropped_hash() {
    local h="$1" d
    for d in "${DROPPED_HASHES[@]}"; do
        [[ "$d" == "$h" ]] && return 0
    done
    return 1
}

send_one() {
    local idx="$1" kind arg target raw h resp result err nonce bump gas_price priority_gas_price
    kind="set"
    arg=$((VALUE_INITIAL + SENT + 1))
    target="$SETTER_PROXY"
    local sig_and_args=( 'setValue(uint256)' "$arg" )
    local value_args=()
    if (( (SENT + 1) % 5 == 0 )); then
        kind="dep"
        arg="$DEPOSIT_WEI"
        target="$DEPOSIT_PROXY"
        sig_and_args=()
        value_args=( --value "$arg" )
    fi

    nonce="${NEXT_NONCES[$idx]}"
    bump="${RESEND_BUMPS[$idx]:-0}"
    gas_price=$((GAS_PRICE_WEI + bump * RESEND_GAS_BUMP_WEI))
    priority_gas_price=$((PRIORITY_GAS_PRICE_WEI + bump * RESEND_GAS_BUMP_WEI))
    raw=$(cast mktx --chain-id "$L1_CHAIN_ID" --private-key "${SENDER_KEYS[$idx]}" --nonce "$nonce" \
        --gas-limit 600000 --gas-price "$gas_price" --priority-gas-price "$priority_gas_price" \
        "${value_args[@]}" "$target" "${sig_and_args[@]}" 2>&1) || raw=""
    if [[ ! "$raw" =~ ^0x[0-9a-fA-F]+$ ]]; then
        echo "    ✗ mktx failed sender=$idx nonce=$nonce: $raw"
        FAILED=$((FAILED + 1))
        return 1
    fi

    h=$(cast keccak "$raw")
    resp=$(curl -s -X POST "$XCHAIN_L1" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":$SENT}" || true)
    result=$(echo "$resp" | jq -r '.result // empty' 2>/dev/null)
    if [[ "$result" == "$h" ]]; then
        TX_HASHES+=("$h")
        TX_KINDS+=("$kind")
        TX_ARGS+=("$arg")
        [[ "$kind" == "set" ]] && LAST_SETTER_VALUE="$arg"
        [[ "$kind" == "dep" ]] && TOTAL_DEPOSIT_SUM=$((TOTAL_DEPOSIT_SUM + arg))
        printf '%s %s %s sender=%s nonce=%s bump=%s\n' "$h" "$kind" "$arg" "$idx" "$nonce" "$bump" >>"$RUN_LOG"
        SENT=$((SENT + 1))
        NEXT_NONCES[$idx]=$((nonce + 1))
        BUSY_HASHES[$idx]="$h"
        BUSY_NONCES[$idx]="$nonce"
        BUSY_SINCE[$idx]="$SECONDS"
        BUSY_WARNED[$idx]=0
        return 0
    fi

    err=$(echo "$resp" | jq -r '.error.message // empty' 2>/dev/null)
    if [[ "$err" =~ expected[[:space:]]+([0-9]+) ]]; then
        NEXT_NONCES[$idx]="${BASH_REMATCH[1]}"
    else
        nonce_hex=$(pending_nonce "${SENDER_ADDRS[$idx]}" || true)
        [[ -n "${nonce_hex:-}" ]] && NEXT_NONCES[$idx]=$((nonce_hex))
    fi
    echo "    ✗ send failed sender=$idx nonce=$nonce hash=$h response=$resp"
    FAILED=$((FAILED + 1))
    return 1
}

echo
echo "==> sending inbound txs"
: >"$RUN_LOG"
while (( SECONDS < END_AT )); do
    for idx in "${!SENDER_KEYS[@]}"; do refresh_sender "$idx" || true; done
    made_progress=0
    for ((i=0; i<BATCH_SIZE && SECONDS < END_AT; i++)); do
        chosen=""
        for ((attempt=0; attempt<${#SENDER_KEYS[@]}; attempt++)); do
            idx=$(((NEXT_SENDER + attempt) % ${#SENDER_KEYS[@]}))
            if [[ -z "${BUSY_HASHES[$idx]:-}" ]]; then
                chosen="$idx"
                break
            fi
        done
        [[ -z "$chosen" ]] && break
        NEXT_SENDER=$(((chosen + 1) % ${#SENDER_KEYS[@]}))
        send_one "$chosen" && made_progress=1 || true
        (( SENT == 1 || SENT % 10 == 0 )) && echo "    sent=$SENT failed=$FAILED elapsed=${SECONDS}s"
    done
    if [[ "$made_progress" == "0" && $((SECONDS - LAST_BUSY_LOG)) -ge 30 ]]; then
        echo "    all senders busy; sent=$SENT failed=$FAILED elapsed=${SECONDS}s"
        LAST_BUSY_LOG="$SECONDS"
    fi
    sleep "$INTERVAL_SECS"
done

echo "    submitted=$SENT failed=$FAILED"

if [[ "$WAIT_RECEIPTS" != "1" ]]; then
    echo "==> receipt wait disabled"
    exit 0
fi

echo
echo "==> waiting up to ${RECEIPT_WAIT_SECS}s for receipts"
wait_end=$((SECONDS + RECEIPT_WAIT_SECS))
confirmed=0
dropped=0
while (( SECONDS < wait_end )); do
    for idx in "${!SENDER_KEYS[@]}"; do refresh_sender "$idx" || true; done
    confirmed=0
    dropped=0
    for H in "${TX_HASHES[@]}"; do
        if [[ "$(receipt_status "$H")" == "0x1" ]]; then
            confirmed=$((confirmed + 1))
        elif is_dropped_hash "$H"; then
            dropped=$((dropped + 1))
        fi
    done
    echo "    confirmed=$confirmed/${#TX_HASHES[@]} dropped=$dropped elapsed=${SECONDS}s"
    (( confirmed + dropped == ${#TX_HASHES[@]} )) && break
    sleep 10
done

if (( SETTLE_AFTER_RECEIPTS_SECS > 0 )); then
    echo "    settling ${SETTLE_AFTER_RECEIPTS_SECS}s..."
    sleep "$SETTLE_AFTER_RECEIPTS_SECS"
fi

LAST_CONFIRMED_SETTER=""
CONFIRMED_DEPOSIT_SUM=0
for idx in "${!TX_HASHES[@]}"; do
    if [[ "$(receipt_status "${TX_HASHES[$idx]}")" == "0x1" ]]; then
        [[ "${TX_KINDS[$idx]}" == "set" ]] && LAST_CONFIRMED_SETTER="${TX_ARGS[$idx]}"
        [[ "${TX_KINDS[$idx]}" == "dep" ]] && CONFIRMED_DEPOSIT_SUM=$((CONFIRMED_DEPOSIT_SUM + TX_ARGS[$idx]))
    fi
done

echo
echo "==> semantic checks"
VV=$(retry cast call "$EEZ_VALUE_ADDRESS" 'value()(uint256)' --rpc-url "$L2_RPC")
RR=$(retry cast balance "$L2_RECIPIENT" --rpc-url "$L2_RPC")
EXPECTED_RR=$((RECIPIENT_BEFORE + CONFIRMED_DEPOSIT_SUM))
echo "    confirmed setter=$LAST_CONFIRMED_SETTER deposit_sum=$CONFIRMED_DEPOSIT_SUM"
echo "    L2 Value.value()=$VV"
echo "    L2 recipient balance=$RR expected=$EXPECTED_RR"

SEM_OK=1
[[ -n "$LAST_CONFIRMED_SETTER" && "$VV" == "$LAST_CONFIRMED_SETTER" ]] || SEM_OK=0
[[ "$RR" == "$EXPECTED_RR" ]] || SEM_OK=0

echo
echo "==> root check"
L1_TRACKED=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint256)(address,bytes32,uint256)' "$EEZ_ROLLUP_ID" \
    --rpc-url "$L1_RPC" | sed -n '2p' | tr -d '[:space:]')
ROOT_MATCH_HEIGHT=$(find_l2_root_height "$L1_TRACKED" || true)
echo "    L1 rollups($EEZ_ROLLUP_ID).stateRoot=$L1_TRACKED"
if [[ -n "$ROOT_MATCH_HEIGHT" ]]; then
    echo "    matched L2 block=$ROOT_MATCH_HEIGHT"
else
    echo "    no recent L2 root match found scan_back=$ROOT_MATCH_SCAN_BACK"
fi

if [[ "$SEM_OK" == "1" && "$confirmed" == "${#TX_HASHES[@]}" && "$FAILED" == "0" && ${#DROPPED_HASHES[@]} -eq 0 && -n "$ROOT_MATCH_HEIGHT" ]]; then
    echo
    echo "==> INBOUND LOAD PASSED submitted=$SENT confirmed=$confirmed"
    exit 0
fi

echo
echo "==> INBOUND LOAD FAILED submitted=$SENT confirmed=$confirmed dropped=${#DROPPED_HASHES[@]} semantic_ok=$SEM_OK"
exit 1
