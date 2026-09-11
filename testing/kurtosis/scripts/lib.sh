#!/usr/bin/env bash
# Shared helpers for the kurtosis verification scripts. Source, do not execute.
#
# Callers must already have set: K (the testing/kurtosis dir), REPO (repo root),
# L2 and EEZL2_ADDRESS if they use create_l2_proxy.
# Requires cast, forge, jq, and curl in PATH.

# Priority fee every helper signs with; scripts may override before sourcing.
PRIORITY_GAS_PRICE="${PRIORITY_GAS_PRICE:-${EEZ_TEST_PRIORITY_GAS_PRICE_WEI:-1}}"

yaml_value() { # <key> → scalar from the kurtosis args file
    # `-m1` not `| head -1`: head closes the pipe, grep takes SIGPIPE, and
    # pipefail then fails the whole pipeline.
    grep -m1 -E "^[[:space:]]*$1:" "${KURTOSIS_ARGS_FILE:-$K/args.yaml}" 2>/dev/null \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'
}

retry() { # <cmd...> → stdout; survives transient RPC hiccups under load
    local attempts=0 max="${RETRY_MAX:-6}" delay="${RETRY_DELAY:-3}" output rc
    while :; do
        # `if`, not a bare `out=$(...)`: under `set -e` a failure would abort
        # the script instead of reaching the retry below.
        if output=$("$@" 2>&1); then
            printf '%s' "$output"
            return 0
        else
            rc=$?
        fi
        (( ++attempts >= max )) && {
            echo "retry: '$*' failed after $attempts attempts: $output" >&2
            return "$rc"
        }
        sleep "$delay"
    done
}

gas_price_for() { # <rpc> → max fee in wei
    local rpc="$1" gas_price base_hex base minimum
    gas_price=$(cast gas-price --rpc-url "$rpc" 2>/dev/null || echo 1000000000)
    gas_price="${EEZ_TEST_GAS_PRICE_WEI:-$gas_price}"
    base_hex=$(cast block latest --field baseFeePerGas --rpc-url "$rpc" 2>/dev/null || echo 0)
    base=$(cast to-dec "$base_hex" 2>/dev/null || echo 0)
    minimum=$((2 * base + PRIORITY_GAS_PRICE))
    (( gas_price < minimum )) && gas_price="$minimum"
    echo "$gas_price"
}

fund() { # <rpc> <key> <to> — 10 ETH from <key>'s account
    local rpc="$1" key="$2" to="$3" from nonce
    from=$(cast wallet address --private-key "$key")
    nonce=$(retry cast nonce "$from" --rpc-url "$rpc")
    cast send "$to" --value 10ether --private-key "$key" --nonce "$nonce" \
        --gas-price "$(gas_price_for "$rpc")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
        --rpc-url "$rpc" >/dev/null
}

forge_deploy() { # <rpc> <key> <script:contract> <sig> <args...> → echoes forge stdout
    local rpc="$1" key="$2" script="$3" signature="$4" gas_price output
    shift 4
    gas_price=$(gas_price_for "$rpc")
    if ! output=$(cd "$REPO/contracts" && forge script "script/$script" --sig "$signature" "$@" \
        --rpc-url "$rpc" --broadcast --private-key "$key" --gas-price "$gas_price" \
        --skip-simulation 2>&1); then
        printf '%s\n' "$output" >&2
        return 1
    fi
    printf '%s\n' "$output"
}

grab_address() { grep -m1 -oE "$1=0x[0-9a-fA-F]{40}" | cut -d= -f2; }
strip_ansi() { sed 's/\x1b\[[0-9;]*m//g'; }

receipt_json() { # <hash> <rpc>
    curl -s --max-time 3 -X POST -H 'Content-Type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$1\"],\"id\":1}" \
        "$2" 2>/dev/null
}

receipt_status() { # <hash> <rpc> → "1" mined-ok, "0x0" reverted, "missing" not mined
    jq -r '.result.status // "missing"' <<<"$(receipt_json "$1" "$2")" 2>/dev/null \
        | sed 's/^0x1$/1/'
}

send_front() { # <front_url> <raw_tx> <expected_hash> — eth_sendRawTransaction to a front
    # Fronts refuse submissions until the node reconciles with L1, so wait that
    # out; any other error — or a changed hash — is fatal (invariant 7 is LOUD).
    local front="$1" raw="$2" expected="$3" resp rc returned i
    for ((i = 0; i < 120; i++)); do
        resp=$(curl -sS --max-time 10 -X POST "$front" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}" 2>/dev/null); rc=$?
        # Empty body = no answer. Without this the checks below miss and a tx
        # that was NEVER SENT reports success.
        (( rc == 0 )) && [[ -n "$resp" ]] \
            || { echo "    ✗ submit failed (curl rc=$rc, ${#resp} byte body)" >&2; return 1; }
        if grep -q '"error"' <<<"$resp"; then
            grep -q 'starting up' <<<"$resp" \
                || { echo "    ✗ front rejected tx: $resp" >&2; return 1; }
            sleep 1
            continue
        fi
        returned=$(jq -er '.result // error("missing transaction hash")' <<<"$resp" 2>/dev/null || true)
        [[ "${returned,,}" == "${expected,,}" ]] \
            || { echo "    ✗ front changed the submitted transaction: $resp" >&2; return 1; }
        return 0
    done
    echo "    ✗ front still starting up after 120s" >&2
    return 1
}

create_l2_proxy() { # <target_on_L1> <deployer_key> <rollup_id> → proxy address
    # computeCrossChainProxyAddress then createCrossChainProxy on the L2 EEZL2 —
    # a PURE L2 tx, so it goes to the normal L2 RPC, not a front.
    local target="$1" key="$2" rollup_id="$3" proxy code nonce raw chain_id deployer response
    deployer=$(cast wallet address --private-key "$key")
    chain_id=$(cast chain-id --rpc-url "$L2")
    proxy=$(cast call "$EEZL2_ADDRESS" 'computeCrossChainProxyAddress(address,uint64)(address)' \
        "$target" "$rollup_id" --rpc-url "$L2" | tr -d '[:space:]')
    code=$(cast code "$proxy" --rpc-url "$L2" 2>/dev/null || echo 0x)
    if [[ "$code" == "0x" || -z "$code" ]]; then
        nonce=$(retry cast nonce "$deployer" --rpc-url "$L2")
        raw=$(cast mktx --rpc-url "$L2" --chain-id "$chain_id" --private-key "$key" \
            --nonce "$nonce" --gas-limit 1500000 --gas-price "$(gas_price_for "$L2")" \
            --priority-gas-price "$PRIORITY_GAS_PRICE" \
            "$EEZL2_ADDRESS" 'createCrossChainProxy(address,uint64)' "$target" "$rollup_id")
        [[ "$raw" =~ ^0x[0-9a-fA-F]+$ ]] \
            || { echo "could not build the L2 proxy creation transaction" >&2; return 1; }
        response=$(curl -sS --max-time 10 -X POST "$L2" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}")
        jq -e '.result != null' <<<"$response" >/dev/null \
            || { echo "L2 proxy creation was rejected: $response" >&2; return 1; }
        for _ in $(seq 1 30); do
            code=$(cast code "$proxy" --rpc-url "$L2" 2>/dev/null || echo 0x)
            [[ "$code" != "0x" && -n "$code" ]] && break
            sleep 1
        done
    fi
    # Never echo an address with no code — callers treat a return as usable.
    [[ "$code" != "0x" && -n "$code" ]] || return 1
    echo "$proxy"
}
