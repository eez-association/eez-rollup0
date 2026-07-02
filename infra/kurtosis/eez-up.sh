#!/usr/bin/env bash
# Bring up the EEZ node pair (Pair A): the follower Lighthouse beacon + eez-node
# with its EMBEDDED L1 reth. This is the second developer entry point — run it
# AFTER l1-up.sh (which starts the L1 pair and deploys the protocol).
#
# Two processes:
#   - follower beacon   docker compose (docker-compose.eez-l1.yml), detached.
#                       Joins the Kurtosis CL network and drives the embedded
#                       reth via the engine API.
#   - eez-node          run-eez-node.sh, FOREGROUND in this terminal. Its
#                       embedded reth backfills L1 history from EEZ_L1_TRUSTED_PEERS
#                       and executes every block the follower feeds it; the
#                       composer reads that L1 state in-process (cross-chain).
#
# eez-node runs on the host via `cargo run` (recompiles on first run), so keep
# this terminal open — Ctrl-C stops eez-node. Stop the beacon with down.sh.
#
# Usage:  bash infra/kurtosis/eez-up.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
S="$HERE/scripts"
REPO="$(cd "$HERE/../.." && pwd)"
ENV_FILE="${KURTOSIS_ENV_FILE:-$HERE/.env}"
DATA_DIR="${EEZ_L1_DATA_DIR:-$HERE/eez-l1-data}"

# Fail early with a pointer to l1-up.sh if its artifacts are missing.
missing=""
[[ -f "$ENV_FILE" ]]                      || missing+="  $ENV_FILE (cp eez.env.example)\n"
[[ -f "$DATA_DIR/genesis.json" ]]         || missing+="  $DATA_DIR/genesis.json (extract-genesis)\n"
[[ -f "$DATA_DIR/cl-bootnode.env" ]]      || missing+="  $DATA_DIR/cl-bootnode.env (get-cl-bootnode)\n"
[[ -f "$REPO/deployments.env" ]]          || missing+="  $REPO/deployments.env (deploy-eez)\n"
if [[ -n "$missing" ]]; then
    echo "missing artifacts from the L1 pair — run l1-up.sh first:" >&2
    printf "%b" "$missing" >&2
    exit 1
fi

echo "==> starting follower beacon (docker compose, detached)"
docker compose \
    --env-file "$ENV_FILE" \
    --env-file "$DATA_DIR/cl-bootnode.env" \
    -f "$HERE/docker-compose.eez-l1.yml" up -d

echo "==> starting eez-node (embedded L1, foreground). Ctrl-C to stop."
echo "    logs also tee'd to /tmp/eez-node.log"
bash "$S/run-eez-node.sh" 2>&1 | tee /tmp/eez-node.log
