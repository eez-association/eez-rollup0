#!/usr/bin/env bash
# Start Kurtosis L1 devnet (ethereum-package). Requires Docker + kurtosis CLI.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
PACKAGE="${KURTOSIS_PACKAGE:-github.com/ethpandaops/ethereum-package}"
ARGS_FILE="${KURTOSIS_ARGS_FILE:-$REPO/infra/kurtosis/network_params.yaml}"

command -v kurtosis >/dev/null || { echo "kurtosis not found" >&2; exit 1; }
[[ -f "$ARGS_FILE" ]] || { echo "missing $ARGS_FILE" >&2; exit 1; }

PRIV_FLAG=()
[[ "${KURTOSIS_PRIVILEGED:-1}" =~ ^(1|true|yes)$ ]] && PRIV_FLAG=(--privileged)

kurtosis run "${PRIV_FLAG[@]}" "$PACKAGE" \
    --args-file "$ARGS_FILE" \
    --enclave "$ENCLAVE" \
    --image-download always
