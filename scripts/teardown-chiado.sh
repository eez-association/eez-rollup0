#!/usr/bin/env bash
# Tear down the dockerized chiado stack (docker-compose.chiado-node.yml):
# eez-node, eez-proof-signer, lighthouse. Idempotent — safe to call even if
# nothing is running.
set -uo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"; cd "$REPO" || exit 1
ENV_FILE="${EEZ_CHIADO_ENV_FILE:-.env.chiado}"
COMPOSE_FILE="docker-compose.chiado-node.yml"

[[ -f "$ENV_FILE" ]] || { echo "✗ $ENV_FILE missing — nothing to tear down (or wrong dir)"; exit 1; }

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" down

left="$(docker ps --filter 'name=eez-node-chiado' --filter 'name=eez-proof-signer-chiado' --filter 'name=eez-chiado-lighthouse' --format '{{.Names}}')"
if [[ -n "$left" ]]; then
    echo "ERROR: cleanup incomplete. still running=[$left]"
    exit 1
fi
echo "✓ chiado stack torn down (L1/L2 datadirs on disk are untouched)"
