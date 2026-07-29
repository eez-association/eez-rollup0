#!/usr/bin/env bash
# Remove the CI test enclave.
set -euo pipefail

ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"

command -v kurtosis >/dev/null || { echo "kurtosis not found in PATH" >&2; exit 1; }

echo "==> removing enclave '$ENCLAVE'"
kurtosis enclave rm -f "$ENCLAVE" 2>/dev/null || true

echo "CI test enclave removed."
