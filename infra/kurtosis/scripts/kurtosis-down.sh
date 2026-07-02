#!/usr/bin/env bash
# Tear down the follower beacon (Pair A), the Kurtosis enclave (Pair B), and
# generated artifacts. down.sh wraps this and also stops the host eez-node.
set -euo pipefail

ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
K="$REPO/infra/kurtosis"

# Pair A follower beacon (ignore errors if it was never started).
docker compose -f "$K/docker-compose.eez-l1.yml" down -v 2>/dev/null || true

# Pair B enclave.
kurtosis enclave rm -f "$ENCLAVE" 2>/dev/null || true

# Generated artifacts. .env is left in place — it holds your poster/proof keys.
rm -rf "$K/eez-l1-data"
rm -f "$K/endpoints.env" "$REPO/deployments.env"

echo "enclave, follower beacon, and artifacts removed."
