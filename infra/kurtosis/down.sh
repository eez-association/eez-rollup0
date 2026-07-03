#!/usr/bin/env bash
# Tear down the EEZ cross-chain devnet. Everything (both pairs) lives in one
# Kurtosis enclave now, so this is a single enclave removal — no host process,
# no docker-compose, no on-disk artifacts to clean.
#
# Usage:  bash infra/kurtosis/down.sh
set -euo pipefail

ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"

command -v kurtosis >/dev/null || { echo "kurtosis not found in PATH" >&2; exit 1; }

echo "==> removing enclave '$ENCLAVE'"
kurtosis enclave rm -f "$ENCLAVE" 2>/dev/null || true

echo "✓ devnet torn down."
echo "  (Locally-built images eez-node:dev / eez-deploy:dev are kept; remove with"
echo "   'docker rmi eez-node:dev eez-deploy:dev' if you want to reclaim the space.)"
