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

set +e
kurtosis run "${PRIV_FLAG[@]}" "$PACKAGE" \
    --args-file "$ARGS_FILE" \
    --enclave "$ENCLAVE" \
    --image-download always
run_rc=$?
set -e

inspect="$(kurtosis enclave inspect "$ENCLAVE" 2>/dev/null || true)"
if ! echo "$inspect" | grep -qE 'el-.*rpc:'; then
    echo "enclave '$ENCLAVE' has no EL RPC services (Starlark likely failed)." >&2
    echo "run: bash infra/kurtosis/scripts/kurtosis-down.sh && retry kurtosis-up.sh" >&2
    exit "${run_rc:-1}"
fi

exit "$run_rc"
