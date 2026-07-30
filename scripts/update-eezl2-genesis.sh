#!/usr/bin/env bash

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ROLLUP_ID=1
SYSTEM_ADDRESS="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
USE_GAS_LEFT=false

usage() {
    echo "usage: $0 [--check]" >&2
}

mode="update"
case "${1:-}" in
    "") ;;
    --check) mode="check" ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        usage
        exit 2
        ;;
esac
if (( $# > 1 )); then
    usage
    exit 2
fi

forge_output="$({
    cd "$REPO/contracts"
    forge script script/GenerateEEZL2Runtime.s.sol:GenerateEEZL2Runtime \
        --sig "run(uint64,address,bool)" \
        "$ROLLUP_ID" "$SYSTEM_ADDRESS" "$USE_GAS_LEFT"
} 2>&1)"
runtime="$(sed -n 's/^  \(0x[0-9a-fA-F]*\)$/\1/p' <<<"$forge_output")"

if [[ -z "$runtime" || "$runtime" == *$'\n'* ]]; then
    printf '%s\n' "$forge_output" >&2
    echo "failed to extract exactly one EEZL2 runtime from Foundry output" >&2
    exit 1
fi

predeploy="0x4200000000000000000000000000000000000007"
genesis_files=(
    "$REPO/genesis.json"
    "$REPO/crates/eez-node/tests/fixtures/genesis.json"
)
genesis_temps=()
cleanup() {
    rm -f "${genesis_temps[@]}" "${kurtosis_tmp:-}"
}
trap cleanup EXIT

for genesis in \
    "${genesis_files[@]}"
do
    tmp="$(mktemp "${genesis}.tmp.XXXXXX")"
    genesis_temps+=("$tmp")
    if ! jq -e --arg predeploy "$predeploy" \
        '.alloc | type == "object" and has($predeploy) and (.[$predeploy].code | type == "string")' \
        "$genesis" >/dev/null
    then
        echo "$genesis does not contain EEZL2 predeploy code at $predeploy" >&2
        exit 1
    fi
    jq --arg predeploy "$predeploy" --arg runtime "$runtime" \
        '.alloc[$predeploy].code = $runtime' "$genesis" >"$tmp"
    chmod --reference="$genesis" "$tmp"
done

state_root="$(
    cd "$REPO"
    cargo run --quiet --locked --package eez-node --example genesis_state_root -- \
        "${genesis_temps[0]}"
)"
kurtosis="$REPO/testing/kurtosis/main.star"
if [[ "$(grep -c '^L2_GENESIS_STATE_ROOT = ' "$kurtosis")" != 1 ]]; then
    echo "expected exactly one L2_GENESIS_STATE_ROOT in $kurtosis" >&2
    exit 1
fi
kurtosis_tmp="$(mktemp "${kurtosis}.tmp.XXXXXX")"
sed "s/^L2_GENESIS_STATE_ROOT = .*/L2_GENESIS_STATE_ROOT = \"$state_root\"/" \
    "$kurtosis" >"$kurtosis_tmp"
chmod --reference="$kurtosis" "$kurtosis_tmp"

if [[ "$mode" == "check" ]]; then
    stale=0
    for index in "${!genesis_files[@]}"; do
        if ! cmp -s "${genesis_temps[$index]}" "${genesis_files[$index]}"; then
            echo "stale generated EEZL2 runtime: ${genesis_files[$index]}" >&2
            stale=1
        fi
    done
    if ! cmp -s "$kurtosis_tmp" "$kurtosis"; then
        echo "stale generated L2_GENESIS_STATE_ROOT: $kurtosis" >&2
        stale=1
    fi
    if (( stale != 0 )); then
        echo "run scripts/update-eezl2-genesis.sh to update generated artifacts" >&2
        exit 1
    fi
    echo "EEZL2 genesis artifacts are current; state root: $state_root"
    exit 0
fi

for index in "${!genesis_files[@]}"; do
    mv "${genesis_temps[$index]}" "${genesis_files[$index]}"
done
mv "$kurtosis_tmp" "$kurtosis"

echo "updated both EEZL2 genesis predeploys; state root: $state_root"
