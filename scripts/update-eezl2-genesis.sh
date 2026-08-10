#!/usr/bin/env bash

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PROTOCOL="$REPO/eez-core-protocol"
GENERATOR="$REPO/scripts/solidity/GenerateEEZL2Runtime.s.sol"
FIXTURE_PROFILE="$REPO/testing/kurtosis/l2-genesis-profile.json"
EXPECTED_FOUNDRY_VERSION="1.7.1"
USE_GAS_LEFT=false
EEZL2_ADDRESS="0x4200000000000000000000000000000000000007"
DEFAULT_SYSTEM_BALANCE="0xd3c21bcecceda1000000"

# Independent fixed vector for the canonical CrossChainProxy CREATE2 formula.
PROXY_VECTOR_ORIGINAL_ADDRESS="0x11223344556677889900aabbccddeeff00112233"
PROXY_VECTOR_ORIGINAL_ROLLUP_ID=0
EXPECTED_PROXY_SALT="0x7c432a8c413604983b9f189bb5b6650220b6b9b038267802fef07276a796e168"
EXPECTED_PROXY_INIT_CODE_HASH="0x7c63e527554027dc06e5f9aebc6a153ec5e54dbf588ac2238a2776bf3c9fdc31"
EXPECTED_PROXY_ADDRESS="0x0093d413a54e6e751f882d69785d90fb5d0646a4"

usage() {
    cat >&2 <<'EOF'
usage:
  scripts/update-eezl2-genesis.sh
  scripts/update-eezl2-genesis.sh --check
  scripts/update-eezl2-genesis.sh --render --rollup-id ID --system-address ADDRESS \
      --output PATH [--base PATH] [--profile-output PATH] [--system-balance HEX]

The default mode regenerates the committed test genesis files from
testing/kurtosis/l2-genesis-profile.json. --render creates a deployment-specific
genesis without modifying committed files. Private keys are never accepted;
the deployment derives the public system address before invoking this script.
EOF
}

mode="update"
rollup_id=""
system_address=""
system_balance=""
base_genesis=""
output=""
profile_output=""

while (( $# > 0 )); do
    case "$1" in
        --check)
            [[ "$mode" == "update" ]] || { usage; exit 2; }
            mode="check"
            shift
            ;;
        --render)
            [[ "$mode" == "update" ]] || { usage; exit 2; }
            mode="render"
            shift
            ;;
        --rollup-id)
            rollup_id="${2:-}"
            shift 2
            ;;
        --system-address)
            system_address="${2:-}"
            shift 2
            ;;
        --system-balance)
            system_balance="${2:-}"
            shift 2
            ;;
        --base)
            base_genesis="${2:-}"
            shift 2
            ;;
        --output)
            output="${2:-}"
            shift 2
            ;;
        --profile-output)
            profile_output="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

if [[ "$mode" == "render" ]]; then
    [[ -n "$rollup_id" && -n "$system_address" && -n "$output" ]] || {
        usage
        exit 2
    }
    base_genesis="${base_genesis:-$REPO/genesis.json}"
    system_balance="${system_balance:-$DEFAULT_SYSTEM_BALANCE}"
else
    [[ -z "$rollup_id$system_address$system_balance$base_genesis$output$profile_output" ]] || {
        usage
        exit 2
    }
    [[ -f "$FIXTURE_PROFILE" ]] || {
        echo "fixture profile is missing: $FIXTURE_PROFILE" >&2
        exit 1
    }
    rollup_id="$(jq -er '.rollupId | tostring' "$FIXTURE_PROFILE")"
    system_address="$(jq -er '.systemAddress' "$FIXTURE_PROFILE")"
    system_balance="$(jq -er '.systemBalance' "$FIXTURE_PROFILE")"
fi

python3 - "$rollup_id" <<'PY'
import sys

try:
    value = int(sys.argv[1], 10)
except ValueError:
    raise SystemExit("rollup ID must be a decimal integer")
if not 0 < value <= (1 << 64) - 1:
    raise SystemExit("rollup ID must be in 1..=u64::MAX")
PY

if [[ ! "$system_address" =~ ^0x[0-9a-fA-F]{40}$ ]]; then
    echo "system address must contain exactly 20 hexadecimal bytes" >&2
    exit 1
fi
system_address="$(cast to-check-sum-address "$system_address")"
if [[ "${system_address,,}" == "0x0000000000000000000000000000000000000000" ]]; then
    echo "system address must be non-zero" >&2
    exit 1
fi
if [[ ! "$system_balance" =~ ^0x[0-9a-fA-F]+$ ]] || [[ "$system_balance" =~ ^0x0+$ ]]; then
    echo "system balance must be a non-zero hexadecimal quantity" >&2
    exit 1
fi

forge_version="$(forge --version | sed -n 's/^forge Version: //p')"
if [[ "$forge_version" != "$EXPECTED_FOUNDRY_VERSION" ]]; then
    echo "Foundry $EXPECTED_FOUNDRY_VERSION is required; found ${forge_version:-unknown}" >&2
    exit 1
fi

for required in "$PROTOCOL/foundry.toml" "$PROTOCOL/remappings.txt" "$PROTOCOL/src" \
    "$PROTOCOL/lib" "$GENERATOR"
do
    if [[ ! -e "$required" ]]; then
        echo "required canonical protocol input is missing: $required" >&2
        exit 1
    fi
done

build_root="$(mktemp -d)"
cleanup() {
    rm -rf "$build_root"
}
trap cleanup EXIT

cp "$PROTOCOL/foundry.toml" "$PROTOCOL/remappings.txt" "$build_root/"
[[ ! -f "$PROTOCOL/foundry.lock" ]] || cp "$PROTOCOL/foundry.lock" "$build_root/"
cp -a "$PROTOCOL/src" "$build_root/src"
cp -a "$PROTOCOL/lib" "$build_root/lib"
mkdir "$build_root/script"
cp "$GENERATOR" "$build_root/script/GenerateEEZL2Runtime.s.sol"

forge_output="$({
    cd "$build_root"
    forge script script/GenerateEEZL2Runtime.s.sol:GenerateEEZL2Runtime \
        --sig "run(uint64,address,bool)" \
        "$rollup_id" "$system_address" "$USE_GAS_LEFT"
} 2>&1)"
runtime="$(sed -n 's/^  \(0x[0-9a-fA-F]*\)$/\1/p' <<<"$forge_output")"

if [[ -z "$runtime" || "$runtime" == *$'\n'* ]]; then
    printf '%s\n' "$forge_output" >&2
    echo "failed to extract exactly one EEZL2 runtime from Foundry output" >&2
    exit 1
fi

artifact="$build_root/out/EEZL2.sol/EEZL2.json"
if ! jq -e \
    '.metadata.settings.compilationTarget == {"src/L2/EEZL2.sol": "EEZL2"}' \
    "$artifact" >/dev/null
then
    echo "EEZL2 was not compiled under the canonical protocol source path" >&2
    exit 1
fi
runtime_hash="$(cast keccak "$runtime")"

# Bind the canonical proxy creation bytecode, constructor encoding, and CREATE2
# formula to an independent fixed vector.
proxy_artifact="$build_root/out/CrossChainProxy.sol/CrossChainProxy.json"
proxy_creation_code="$(jq -er \
    '.bytecode.object | select(type == "string" and startswith("0x"))' \
    "$proxy_artifact")"
proxy_salt="$(cast keccak "$(
    cast abi-encode --packed "f(uint64,address)" \
        "$PROXY_VECTOR_ORIGINAL_ROLLUP_ID" "$PROXY_VECTOR_ORIGINAL_ADDRESS"
)")"
proxy_constructor_args="$(cast abi-encode "f(address)" "$EEZL2_ADDRESS")"
proxy_init_code="$(cast concat-hex "$proxy_creation_code" "$proxy_constructor_args")"
proxy_init_code_hash="$(cast keccak "$proxy_init_code")"
create2_preimage="$(cast concat-hex \
    0xff "$EEZL2_ADDRESS" "$proxy_salt" "$proxy_init_code_hash")"
proxy_digest="$(cast keccak "$create2_preimage")"
proxy_address="0x${proxy_digest: -40}"

for actual_expected_label in \
    "$proxy_salt|$EXPECTED_PROXY_SALT|CrossChainProxy salt" \
    "$proxy_init_code_hash|$EXPECTED_PROXY_INIT_CODE_HASH|CrossChainProxy init-code hash" \
    "$proxy_address|$EXPECTED_PROXY_ADDRESS|CrossChainProxy CREATE2 address"
do
    IFS='|' read -r actual expected label <<<"$actual_expected_label"
    if [[ "$actual" != "$expected" ]]; then
        echo "unexpected $label: $actual" >&2
        echo "expected: $expected" >&2
        exit 1
    fi
done

genesis_state_root() {
    local genesis="$1"
    if command -v eez-genesis-state-root >/dev/null 2>&1; then
        eez-genesis-state-root "$genesis"
    else
        (
            cd "$REPO"
            cargo run --quiet --locked --package eez-node --example genesis_state_root -- "$genesis"
        )
    fi
}

render_genesis() {
    local base="$1" destination="$2"
    [[ -f "$base" ]] || { echo "base genesis is missing: $base" >&2; exit 1; }
    mkdir -p "$(dirname "$destination")"
    local tmp
    tmp="$(mktemp "$build_root/genesis.XXXXXX")"
    if ! jq -e --arg predeploy "${EEZL2_ADDRESS,,}" \
        '.alloc | type == "object" and has($predeploy) and (.[$predeploy].code | type == "string")' \
        "$base" >/dev/null
    then
        echo "$base does not contain EEZL2 predeploy code at $EEZL2_ADDRESS" >&2
        exit 1
    fi
    jq \
        --arg predeploy "${EEZL2_ADDRESS,,}" \
        --arg runtime "$runtime" \
        --arg system_address "${system_address,,}" \
        --arg system_balance "$system_balance" \
        '.alloc[$predeploy].code = $runtime
         | .alloc[$system_address].balance = $system_balance' \
        "$base" >"$tmp"
    chmod --reference="$base" "$tmp"
    printf '%s\n' "$tmp"
}

write_profile() {
    local destination="$1" state_root="$2"
    mkdir -p "$(dirname "$destination")"
    local tmp
    tmp="$(mktemp "$build_root/profile.XXXXXX")"
    jq -n \
        --argjson rollup_id "$rollup_id" \
        --arg system_address "$system_address" \
        --arg system_balance "$system_balance" \
        --arg eezl2_address "$EEZL2_ADDRESS" \
        --arg runtime_hash "$runtime_hash" \
        --arg state_root "$state_root" \
        --arg foundry_version "$EXPECTED_FOUNDRY_VERSION" \
        '{
            schemaVersion: 1,
            rollupId: $rollup_id,
            systemAddress: $system_address,
            systemBalance: $system_balance,
            useGasLeft: false,
            eezL2Address: $eezl2_address,
            runtimeCodeHash: $runtime_hash,
            genesisStateRoot: $state_root,
            foundryVersion: $foundry_version
        }' >"$tmp"
    if [[ -e "$destination" ]]; then
        chmod --reference="$destination" "$tmp"
    else
        chmod 0644 "$tmp"
    fi
    printf '%s\n' "$tmp"
}

if [[ "$mode" == "render" ]]; then
    rendered="$(render_genesis "$base_genesis" "$output")"
    state_root="$(genesis_state_root "$rendered")"
    mv "$rendered" "$output"
    if [[ -n "$profile_output" ]]; then
        rendered_profile="$(write_profile "$profile_output" "$state_root")"
        mv "$rendered_profile" "$profile_output"
    fi
    printf 'EEZ_L2_SYSTEM_ADDRESS=%s\n' "$system_address"
    printf 'EEZ_L2_EEZL2_CODE_HASH=%s\n' "$runtime_hash"
    printf 'EEZ_INITIAL_STATE_ROOT=%s\n' "$state_root"
    exit 0
fi

genesis_files=(
    "$REPO/genesis.json"
    "$REPO/crates/eez-node/tests/fixtures/genesis.json"
)
rendered_files=()
for genesis in "${genesis_files[@]}"; do
    rendered_files+=("$(render_genesis "$genesis" "$genesis")")
done
state_root="$(genesis_state_root "${rendered_files[0]}")"
rendered_profile="$(write_profile "$FIXTURE_PROFILE" "$state_root")"

if [[ "$mode" == "check" ]]; then
    stale=0
    for index in "${!genesis_files[@]}"; do
        if ! cmp -s "${rendered_files[$index]}" "${genesis_files[$index]}"; then
            echo "stale generated EEZL2 runtime or system funding: ${genesis_files[$index]}" >&2
            stale=1
        fi
    done
    if ! cmp -s "$rendered_profile" "$FIXTURE_PROFILE"; then
        echo "stale generated EEZL2 fixture profile: $FIXTURE_PROFILE" >&2
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
    mv "${rendered_files[$index]}" "${genesis_files[$index]}"
done
mv "$rendered_profile" "$FIXTURE_PROFILE"
echo "updated both EEZL2 genesis predeploys; state root: $state_root"
