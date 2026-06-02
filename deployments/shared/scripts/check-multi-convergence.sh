#!/usr/bin/env bash
set -euo pipefail

RPC_LIST="${EEZ_CONVERGENCE_RPCS:-${1:-http://127.0.0.1:9645,http://127.0.0.1:9647,http://127.0.0.1:9649,http://127.0.0.1:9651}}"
NAME_LIST="${EEZ_CONVERGENCE_NAMES:-sequencer1,sequencer2,sequencer3,sequencer4}"
INTERVAL_SECS="${EEZ_CONVERGENCE_INTERVAL_SECS:-10}"
ROUNDS="${EEZ_CONVERGENCE_ROUNDS:-0}"
LAG_BLOCKS="${EEZ_CONVERGENCE_LAG_BLOCKS:-8}"
START_BLOCK="${EEZ_CONVERGENCE_START_BLOCK:-1}"
SAFE_LAG_LIMIT="${EEZ_CONVERGENCE_SAFE_LAG_LIMIT:-24}"
EXIT_ON_DIVERGENCE="${EEZ_CONVERGENCE_EXIT_ON_DIVERGENCE:-1}"
EXPECT_DIVERGENCE="${EEZ_CONVERGENCE_EXPECT_DIVERGENCE:-0}"

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "check-multi-convergence: $1 is required" >&2
        exit 1
    }
}

trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
}

to_dec() {
    local hex="$1"
    hex="${hex#0x}"
    if [[ -z "$hex" || "$hex" == "null" ]]; then
        echo "-1"
    else
        printf '%d\n' "0x$hex"
    fi
}

block_json() {
    local rpc="$1"
    local tag="$2"
    cast rpc --rpc-url "$rpc" eth_getBlockByNumber "$tag" false
}

block_field() {
    local json="$1"
    local field="$2"
    jq -r ".$field // empty" <<<"$json"
}

hex_height() {
    local height="$1"
    printf '0x%x' "$height"
}

is_true() {
    [[ "$1" == "1" || "$1" == "true" || "$1" == "yes" ]]
}

require cast
require jq

IFS=',' read -r -a RAW_RPCS <<< "$RPC_LIST"
IFS=',' read -r -a RAW_NAMES <<< "$NAME_LIST"

RPCS=()
NAMES=()
for i in "${!RAW_RPCS[@]}"; do
    rpc="$(trim "${RAW_RPCS[$i]}")"
    [[ -z "$rpc" ]] && continue
    RPCS+=("$rpc")
    if [[ -n "${RAW_NAMES[$i]:-}" ]]; then
        NAMES+=("$(trim "${RAW_NAMES[$i]}")")
    else
        NAMES+=("node$((i + 1))")
    fi
done

if (( ${#RPCS[@]} < 2 )); then
    echo "check-multi-convergence: configure at least two RPCs" >&2
    exit 1
fi

echo "check-multi-convergence: targets"
for i in "${!RPCS[@]}"; do
    echo "  - ${NAMES[$i]} ${RPCS[$i]}"
done
echo "check-multi-convergence: rounds=${ROUNDS} interval=${INTERVAL_SECS}s lag_blocks=${LAG_BLOCKS} safe_lag_limit=${SAFE_LAG_LIMIT}"

round=0
divergence_seen=0
while true; do
    round=$((round + 1))
    if (( ROUNDS > 0 && round > ROUNDS )); then
        if is_true "$EXPECT_DIVERGENCE"; then
            echo "check-multi-convergence: no divergence observed after ${ROUNDS} rounds" >&2
            exit 1
        fi
        echo "check-multi-convergence: completed ${ROUNDS} rounds without divergence"
        exit 0
    fi

    latest_numbers=()
    safe_numbers=()
    finalized_numbers=()
    latest_hashes=()
    safe_hashes=()
    finalized_hashes=()

    min_latest=
    min_safe=
    max_safe=

    echo
    echo "check-multi-convergence: round ${round}"

    for i in "${!RPCS[@]}"; do
        rpc="${RPCS[$i]}"
        name="${NAMES[$i]}"

        latest_json="$(block_json "$rpc" latest)"
        safe_json="$(block_json "$rpc" safe)"
        finalized_json="$(block_json "$rpc" finalized)"

        latest_number="$(to_dec "$(block_field "$latest_json" number)")"
        safe_number="$(to_dec "$(block_field "$safe_json" number)")"
        finalized_number="$(to_dec "$(block_field "$finalized_json" number)")"

        latest_hash="$(block_field "$latest_json" hash)"
        safe_hash="$(block_field "$safe_json" hash)"
        finalized_hash="$(block_field "$finalized_json" hash)"

        latest_numbers+=("$latest_number")
        safe_numbers+=("$safe_number")
        finalized_numbers+=("$finalized_number")
        latest_hashes+=("$latest_hash")
        safe_hashes+=("$safe_hash")
        finalized_hashes+=("$finalized_hash")

        if [[ -z "$min_latest" || "$latest_number" -lt "$min_latest" ]]; then
            min_latest="$latest_number"
        fi
        if [[ -z "$min_safe" || "$safe_number" -lt "$min_safe" ]]; then
            min_safe="$safe_number"
        fi
        if [[ -z "$max_safe" || "$safe_number" -gt "$max_safe" ]]; then
            max_safe="$safe_number"
        fi

        printf '  %-10s latest=%-5s safe=%-5s finalized=%-5s latest_hash=%s safe_hash=%s\n' \
            "$name" "$latest_number" "$safe_number" "$finalized_number" "$latest_hash" "$safe_hash"
    done

    round_diverged=0

    if (( min_latest >= START_BLOCK )); then
        compare_height=$((min_latest - LAG_BLOCKS))
        if (( compare_height < START_BLOCK )); then
            compare_height="$START_BLOCK"
        fi

        compare_tag="$(hex_height "$compare_height")"
        compare_hashes=()
        compare_roots=()
        baseline_hash=
        baseline_root=

        echo "  comparing canonical block ${compare_height} (${compare_tag})"
        for i in "${!RPCS[@]}"; do
            rpc="${RPCS[$i]}"
            name="${NAMES[$i]}"
            compare_json="$(block_json "$rpc" "$compare_tag")"
            compare_hash="$(block_field "$compare_json" hash)"
            compare_root="$(block_field "$compare_json" stateRoot)"
            compare_hashes+=("$compare_hash")
            compare_roots+=("$compare_root")
            [[ -z "$baseline_hash" ]] && baseline_hash="$compare_hash"
            [[ -z "$baseline_root" ]] && baseline_root="$compare_root"
            printf '    %-10s hash=%s stateRoot=%s\n' "$name" "$compare_hash" "$compare_root"
        done

        for i in "${!compare_hashes[@]}"; do
            if [[ "${compare_hashes[$i]}" != "$baseline_hash" || "${compare_roots[$i]}" != "$baseline_root" ]]; then
                round_diverged=1
            fi
        done

        if (( round_diverged == 1 )); then
            echo "check-multi-convergence: canonical divergence at block ${compare_height}" >&2
        fi
    else
        echo "  waiting for all nodes to reach block ${START_BLOCK}"
    fi

    safe_diverged=0
    if (( max_safe - min_safe > SAFE_LAG_LIMIT )); then
        safe_diverged=1
        echo "check-multi-convergence: safe head lag exceeds limit (${min_safe}..${max_safe}, limit ${SAFE_LAG_LIMIT})" >&2
    fi

    baseline_safe_number="${safe_numbers[0]}"
    baseline_safe_hash="${safe_hashes[0]}"
    baseline_finalized_number="${finalized_numbers[0]}"
    baseline_finalized_hash="${finalized_hashes[0]}"
    for i in "${!RPCS[@]}"; do
        if [[ "${safe_numbers[$i]}" != "$baseline_safe_number" || "${safe_hashes[$i]}" != "$baseline_safe_hash" ]]; then
            safe_diverged=1
        fi
        if [[ "${finalized_numbers[$i]}" != "$baseline_finalized_number" || "${finalized_hashes[$i]}" != "$baseline_finalized_hash" ]]; then
            safe_diverged=1
        fi
    done
    if (( safe_diverged == 1 )); then
        echo "check-multi-convergence: safe/finalized heads are not aligned" >&2
    fi

    if (( round_diverged == 1 || safe_diverged == 1 )); then
        divergence_seen=1
        if is_true "$EXPECT_DIVERGENCE"; then
            echo "check-multi-convergence: divergence observed as expected"
            exit 0
        fi
        if is_true "$EXIT_ON_DIVERGENCE"; then
            exit 1
        fi
    else
        echo "check-multi-convergence: converged"
    fi

    sleep "$INTERVAL_SECS"
done
