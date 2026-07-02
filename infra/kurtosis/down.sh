#!/usr/bin/env bash
# Tear down the whole private cross-chain devnet in one shot:
#   - the host eez-node (Pair A, started by eez-up.sh via cargo run)
#   - the follower beacon (docker compose)   ─┐ both handled by
#   - the Kurtosis enclave (Pair B)           ─┤ scripts/kurtosis-down.sh
#   - generated artifacts (eez-l1-data, endpoints.env, deployments.env) ─┘
#
# Your infra/kurtosis/.env (poster/proof keys) is left in place.
#
# Usage:  bash infra/kurtosis/down.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

# Stop the host eez-node first (it holds the embedded L1 ports the beacon dials).
# eez-up.sh runs `cargo run -p eez-node -- node …`; match the compiled binary and
# the wrapper. Best-effort — fine if it was already Ctrl-C'd.
if pgrep -f 'run-eez-node\.sh' >/dev/null 2>&1 || pgrep -f 'eez-node.* node' >/dev/null 2>&1; then
    echo "==> stopping host eez-node"
    pkill -f 'run-eez-node\.sh' 2>/dev/null || true
    pkill -f 'eez-node.* node'      2>/dev/null || true
fi

# Stop any foreign lighthouse left dialing the embedded reth's engine port
# (e.g. a chiado follower from separate testing). Left running, it hits authrpc
# :18551 each slot with its own JWT — the "Invalid JWT" noise in the eez-node log.
for c in chiado-lighthouse-eezrollup eez-chiado-lighthouse; do
    docker rm -f "$c" >/dev/null 2>&1 || true
done

# Follower beacon compose + Kurtosis enclave + generated artifacts.
bash "$HERE/scripts/kurtosis-down.sh"

echo "✓ private devnet torn down."
