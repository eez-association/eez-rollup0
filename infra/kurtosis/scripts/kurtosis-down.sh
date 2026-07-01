#!/usr/bin/env bash
set -euo pipefail

ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
REPO="$(cd "$(dirname "$0")/../../.." && pwd)"

kurtosis enclave rm -f "$ENCLAVE"
rm -f "$REPO/infra/kurtosis/endpoints.env"
