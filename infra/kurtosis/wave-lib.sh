# shellcheck shell=bash
# wave-lib.sh — wave loop + assertions + metrics for wave-test.sh.
#
# SOURCED by wave-test.sh with all endpoints/keys/addresses in scope:
#   MODE WAVES  L1 L2 L1F L2F  NODE_LOG ENCLAVE
#   L1_CHAIN_ID L2_CHAIN_ID  retry()
#   HH_KEY_IN/HH_ADDR_IN (inbound user, funded on L1)
#   HH_KEY_OUT/HH_ADDR_OUT (outbound user, funded on L2 genesis)
#   HH_KEY_PURE (pure-L2 filler)
#   inbound:  IN_VALUE_PROXY IN_NORET_PROXY IN_DEP_PROXY IN_WRAPPER
#             (targets on L2: L2_VALUE L2_VALUE_NORET L2_DEP_RECIPIENT)
#   outbound: OUT_VALUE_PROXY OUT_NORET_PROXY OUT_WD_PROXY OUT_WRAPPER
#             (targets on L1: L1_VALUE L1_VALUE_NORET L1_WD_RECIPIENT)
#   EEZ_REGISTRY_ADDRESS EEZ_ROLLUP_ID EEZ_REGISTRY_DEPLOY_BLOCK
#
# Submission goes to the FRONTS (L1F Inbound, L2F Outbound). The front's
# admission gate expects nonce == on_chain(pending) + held_count(sender,dir),
# so each user's nonce chain is tracked LOCALLY from one initial on-chain
# read and never re-read — a held tx isn't visible in the source mempool,
# and re-reading mid-run would mint colliding nonces.
#
# Inclusion side per direction:
#   inbound  → receipts on the CANONICAL L1 (bundle mate of the postBatch)
#   outbound → receipts on L2 (carried by the Sync block)

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
        *) echo "wave-lib: unknown mode '$MODE'"; exit 1 ;;
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

    # Per-tx metadata for the confirmed-view tally: "hash|side|kind|arg".
    # side=in|out; kind=set|noret|wrap|dep|wd.
    local TX_META=()
    local IN_HASHES=() OUT_HASHES=()

    # mk_and_send <side> <kind> <arg>
    #   in  set/noret/wrap/dep → L1-signed tx via the L1 front
    #   out set/noret/wrap/wd  → L2-signed tx via the L2 front
    mk_and_send() {
        local side="$1" kind="$2" arg="$3" raw="" hash
        local GP=2000000000 PG=1500000000    # L1-side fees (devnet basefee ~floor)
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
                        --gas-limit 600000 --gas-price 1000000000 --priority-gas-price 1000000000 \
                        "$OUT_VALUE_PROXY" 'setValue(uint256)' "$arg") ;;
            out:noret) raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 600000 --gas-price 1000000000 --priority-gas-price 1000000000 \
                        "$OUT_NORET_PROXY" 'setValue(uint256)' "$arg") ;;
            out:wrap)  raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 800000 --gas-price 1000000000 --priority-gas-price 1000000000 \
                        "$OUT_WRAPPER" 'setViaProxy(uint256)' "$arg") ;;
            out:wd)    raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_OUT" --nonce "$OUT_NONCE" \
                        --gas-limit 600000 --gas-price 1000000000 --priority-gas-price 1000000000 --value "$arg" \
                        "$OUT_WD_PROXY") ;;
            *) echo "wave-lib: bad op $side:$kind"; exit 1 ;;
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
        local count="$1" j raw
        for ((j=0; j<count; j++)); do
            raw=$(cast mktx --chain-id "$L2_CHAIN_ID" --private-key "$HH_KEY_PURE" --nonce "$PURE_NONCE" \
                --gas-limit 21000 --gas-price 1000000000 --priority-gas-price 1000000000 \
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
            echo "    inbound: 4 ops via L1 front (set/noret/dep/wrap)"
        fi
        if (( do_out )); then
            mk_and_send out set   $((400 + w))
            mk_and_send out noret $((500 + w))
            mk_and_send out wd    $((w * 20000000000000))         # w * 2e13 wei
            mk_and_send out wrap  $((600 + w))
            echo "    outbound: 4 ops via L2 front (set/noret/wd/wrap)"
        fi
        (( do_pure )) && { submit_pure_filler "$FILLER_PER_GAP"; echo "    pure: $FILLER_PER_GAP L2 filler txs"; }
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
    local LAST_SETTLED L1_TRACKED L2_ROOT
    LAST_SETTLED=$(strip_ansi <"$NODE_LOG" | grep "bundle outcome observed" | grep "settled=true" \
        | grep -oE "sync_height=[0-9]+" | grep -oE "[0-9]+" | sort -n | tail -1 || true)
    if [[ -n "$LAST_SETTLED" ]]; then
        L1_TRACKED=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint256)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
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

    # Dropped-bundle telemetry (informational: steady drops after warmup are
    # the bug this harness exists to catch; a handful from skipped slots is normal).
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
