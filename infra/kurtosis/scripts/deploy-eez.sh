#!/usr/bin/env bash
# Deploy the EEZ contracts + L2 genesis onto the shared L1 via the live Kurtosis
# EL RPC (endpoints.env), before eez-node starts. Writes deployments.env +
# datadir/genesis.json.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENV_FILE="${KURTOSIS_ENV_FILE:-$REPO/infra/kurtosis/.env}"
ENDPOINTS="${KURTOSIS_ENDPOINTS_FILE:-$REPO/infra/kurtosis/endpoints.env}"
MERGED="$(mktemp)"

trap 'rm -f "$MERGED"' EXIT

for t in cast forge; do
    command -v "$t" >/dev/null || { echo "$t not found in PATH" >&2; exit 1; }
done

[[ -f "$ENV_FILE" ]] || {
    echo "missing $ENV_FILE (cp infra/kurtosis/eez.env.example)" >&2
    exit 1
}

# .env first, endpoints.env appended second so its live Kurtosis EL RPC wins
# over the .env placeholder (deploy runs before eez-node's own :18545 exists).
cat "$ENV_FILE" >"$MERGED"
[[ -f "$ENDPOINTS" ]] && cat "$ENDPOINTS" >>"$MERGED"
echo "EEZ_GENESIS_OUT=$REPO/datadir/genesis.json" >>"$MERGED"

# shellcheck disable=SC1090
source "$MERGED"
: "${EEZ_L1_RPC_URL:?}"
: "${EEZ_L1_POSTER_KEY:?}"
: "${EEZ_PROOF_SIGNER_KEY:?}"

for i in $(seq 1 60); do
    cast block-number --rpc-url "$EEZ_L1_RPC_URL" >/dev/null 2>&1 && break
    (( i == 60 )) && { echo "L1 RPC not ready: $EEZ_L1_RPC_URL" >&2; exit 1; }
    sleep 2
done

EEZ_ENV_FILE="$MERGED" EEZ_DEPLOYMENTS_FILE="$REPO/deployments.env" \
    "$REPO/scripts/deploy.sh"
