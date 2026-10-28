#!/usr/bin/env bash
# Deploy the reference ETH bridge after L2 has started.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${EEZ_ENV_FILE:-$REPO/.env}"
OUT_FILE="${EEZ_DEPLOYMENTS_FILE:-$REPO/deployments.env}"
# shellcheck disable=SC1090
[[ ! -f "$ENV_FILE" ]] || source "$ENV_FILE"
# shellcheck disable=SC1090
source "$OUT_FILE"
: "${EEZ_L2_RPC_URL:?set EEZ_L2_RPC_URL to the running L2 RPC}"
: "${EEZ_ROLLUP_ID:?missing rollup ID}"
: "${EEZL2_ADDRESS:?missing EEZL2 address}"
key="${EEZ_L2_DEPLOY_KEY:-${EEZ_DEPLOY_KEY:-${EEZ_L1_POSTER_KEY:?set a funded deployment key}}}"
admin="$(cast wallet address --private-key "$key")"
[[ "$(cast code "$EEZL2_ADDRESS" --rpc-url "$EEZ_L2_RPC_URL")" != 0x \
   && "$(cast call "$EEZL2_ADDRESS" 'ROLLUP_ID()(uint64)' --rpc-url "$EEZ_L2_RPC_URL")" == "$EEZ_ROLLUP_ID" ]] || {
    echo "L2 RPC does not serve the configured EEZL2 rollup" >&2
    exit 1
}

if [[ -n "${EEZ_L2_BRIDGE_ADDRESS:-}" ]]; then
    [[ "$(cast code "$EEZ_L2_BRIDGE_ADDRESS" --rpc-url "$EEZ_L2_RPC_URL")" != 0x ]] || {
        echo "recorded L2 bridge has no code; use fresh deployment artifacts for a new chain" >&2
        exit 1
    }
    [[ "$(cast call "$EEZ_L2_BRIDGE_ADDRESS" 'manager()(address)' --rpc-url "$EEZ_L2_RPC_URL" | tr '[:upper:]' '[:lower:]')" == "$(printf '%s' "$EEZL2_ADDRESS" | tr '[:upper:]' '[:lower:]')" \
       && "$(cast call "$EEZ_L2_BRIDGE_ADDRESS" 'rollupId()(uint64)' --rpc-url "$EEZ_L2_RPC_URL")" == "$EEZ_ROLLUP_ID" ]] || {
        echo "recorded L2 bridge configuration does not match deployments.env" >&2
        exit 1
    }
    echo "L2 reference bridge already deployed: $EEZ_L2_BRIDGE_ADDRESS"
    exit 0
fi

output="$(cd "$REPO/contracts" && forge script script/DeployBridge.s.sol:DeployBridge \
    --sig 'run(address,uint64,address)' "$EEZL2_ADDRESS" "$EEZ_ROLLUP_ID" "$admin" \
    --rpc-url "$EEZ_L2_RPC_URL" --private-key "$key" --broadcast)"
bridge="$(printf '%s\n' "$output" | sed -n 's/.*BRIDGE=\(0x[0-9a-fA-F]\{40\}\).*/\1/p' | head -1)"
[[ -n "$bridge" ]] || { echo "$output" >&2; echo "missing L2 bridge address" >&2; exit 1; }
printf '\nEEZ_L2_BRIDGE_ADDRESS=%s\n' "$bridge" >>"$OUT_FILE"
echo "L2 reference ETH bridge: $bridge"
