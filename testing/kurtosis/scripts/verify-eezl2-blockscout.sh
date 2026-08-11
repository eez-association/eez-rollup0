#!/usr/bin/env bash
# Source-verify the genesis-installed EEZL2 contract in the L2 Blockscout.

set -euo pipefail

K="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"

for tool in cast curl forge jq; do
    command -v "$tool" >/dev/null || { echo "$tool not in PATH" >&2; exit 1; }
done

: "${EEZ_BLOCKSCOUT_URL:?set EEZ_BLOCKSCOUT_URL to the L2 Blockscout API URL}"
: "${EEZ_L2_RPC_URL:?set EEZ_L2_RPC_URL to the L2 RPC URL}"
: "${EEZ_DEPLOYMENTS_FILE:?set EEZ_DEPLOYMENTS_FILE to the downloaded deployments.env}"

PROTOCOL="${EEZ_PROTOCOL_DIR:-$REPO/eez-core-protocol}"
WAIT_SECS="${EEZ_BLOCKSCOUT_WAIT_SECS:-120}"
URL="${EEZ_BLOCKSCOUT_URL%/}"
BLOCKSCOUT_CURL_ARGS=(
    --connect-timeout "${EEZ_BLOCKSCOUT_CONNECT_TIMEOUT_SECS:-5}"
    --max-time "${EEZ_BLOCKSCOUT_REQUEST_TIMEOUT_SECS:-30}"
)

[[ -f "$EEZ_DEPLOYMENTS_FILE" ]] || {
    echo "deployment bindings not found: $EEZ_DEPLOYMENTS_FILE" >&2
    exit 1
}
[[ -f "$PROTOCOL/foundry.toml" ]] || {
    echo "EEZ protocol Foundry project not found: $PROTOCOL" >&2
    exit 1
}

# shellcheck disable=SC1090
source "$EEZ_DEPLOYMENTS_FILE"

for name in EEZL2_ADDRESS EEZ_L2_SYSTEM_ADDRESS EEZ_L2_EEZL2_CODE_HASH EEZ_ROLLUP_ID; do
    [[ -n "${!name:-}" ]] || { echo "$name missing from $EEZ_DEPLOYMENTS_FILE" >&2; exit 1; }
done

runtime="$(cast code "$EEZL2_ADDRESS" --rpc-url "$EEZ_L2_RPC_URL")"
runtime_hash="$(cast keccak "$runtime")"
[[ "$runtime" != "0x" ]] || { echo "no L2 code at $EEZL2_ADDRESS" >&2; exit 1; }
[[ "${runtime_hash,,}" == "${EEZ_L2_EEZL2_CODE_HASH,,}" ]] || {
    echo "live EEZL2 runtime hash mismatch: $runtime_hash != $EEZ_L2_EEZL2_CODE_HASH" >&2
    exit 1
}

address_info=""
for ((attempt = 0; attempt < WAIT_SECS; attempt++)); do
    address_info="$(curl "${BLOCKSCOUT_CURL_ARGS[@]}" -fsS \
        "$URL/api/v2/addresses/$EEZL2_ADDRESS" 2>/dev/null || true)"
    if [[ "$(jq -r '.is_contract // false' <<<"$address_info" 2>/dev/null)" == "true" ]]; then
        break
    fi
    sleep 1
done

if [[ "$(jq -r '.is_contract // false' <<<"$address_info" 2>/dev/null)" != "true" ]]; then
    echo "Blockscout has not classified $EEZL2_ADDRESS as a contract" >&2
    echo "recreate the devnet so the L2 explorer imports l2-genesis.json" >&2
    exit 1
fi

if [[ "$(jq -r '.is_verified // false' <<<"$address_info")" == "true" ]]; then
    echo "EEZL2 already verified @ $EEZL2_ADDRESS"
    exit 0
fi

(cd "$PROTOCOL" && forge build --silent)
artifact="$PROTOCOL/out/EEZL2.sol/EEZL2.json"
[[ -f "$artifact" ]] || { echo "EEZL2 build artifact not found: $artifact" >&2; exit 1; }

compiler_version="$(jq -er '.metadata.compiler.version' "$artifact")"
contract_id="$(jq -er '
    .metadata.settings.compilationTarget
    | to_entries
    | select(length == 1)
    | "\(.[0].key):\(.[0].value)"
' "$artifact")"
constructor_args="$(cast abi-encode \
    'constructor(uint64,address,bool)' \
    "$EEZ_ROLLUP_ID" "$EEZ_L2_SYSTEM_ADDRESS" false)"
standard_input="$(
    cd "$PROTOCOL"
    forge verify-contract \
        "$EEZL2_ADDRESS" "$contract_id" \
        --constructor-args "$constructor_args" \
        --show-standard-json-input
)"

# Forge's submission path expects a creation transaction, which a genesis
# predeploy cannot have. Submit the generated Standard JSON directly instead.
# `constructorArguements` is the Etherscan-compatible API's historical spelling.
response="$(curl "${BLOCKSCOUT_CURL_ARGS[@]}" -fsS -X POST "$URL/api" \
    --data module=contract \
    --data action=verifysourcecode \
    --data codeformat=solidity-standard-json-input \
    --data "contractaddress=$EEZL2_ADDRESS" \
    --data "contractname=$contract_id" \
    --data-urlencode "compilerversion=v$compiler_version" \
    --data-urlencode "sourceCode=$standard_input" \
    --data "constructorArguements=${constructor_args#0x}")"

guid="$(jq -r 'select(.status == "1") | .result' <<<"$response")"
if [[ -z "$guid" ]]; then
    echo "Blockscout rejected EEZL2 verification: $(jq -r '.result // .message // "unknown error"' <<<"$response")" >&2
    exit 1
fi

result=""
for ((attempt = 0; attempt < WAIT_SECS; attempt++)); do
    response="$(curl "${BLOCKSCOUT_CURL_ARGS[@]}" -fsS --get "$URL/api" \
        --data module=contract \
        --data action=checkverifystatus \
        --data-urlencode "guid=$guid")"
    result="$(jq -r '.result // empty' <<<"$response")"
    case "$result" in
        "Pending in queue") sleep 1 ;;
        "Pass - Verified") break ;;
        *)
            echo "Blockscout failed to verify EEZL2: ${result:-unknown error}" >&2
            exit 1
            ;;
    esac
done

[[ "$result" == "Pass - Verified" ]] || {
    echo "timed out waiting for Blockscout to verify EEZL2" >&2
    exit 1
}

address_info="$(curl "${BLOCKSCOUT_CURL_ARGS[@]}" -fsS \
    "$URL/api/v2/addresses/$EEZL2_ADDRESS")"
[[ "$(jq -r '.is_verified // false' <<<"$address_info")" == "true" ]] || {
    echo "Blockscout accepted verification but did not mark EEZL2 as verified" >&2
    exit 1
}

echo "EEZL2 source verified @ $EEZL2_ADDRESS"
