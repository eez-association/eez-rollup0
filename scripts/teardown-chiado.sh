#!/usr/bin/env bash
# Tear down anything left over from a prior smoke-chiado run.
# Idempotent: safe to call even if nothing is running. Used before
# every smoke restart so the next launch starts from a clean slate.
set -uo pipefail
EEZ_NODE_BIN="${EEZ_NODE_BIN:-/root/eez-rollup0/target/debug/eez-node}"
COMPOSE_FILE="${EEZ_CHIADO_COMPOSE:-/root/eez-rollup0/docker-compose.chiado.yml}"
DATADIR_L1="${CHIADO_L1_DATADIR:-/tmp/eez-chiado-l1}"

straggler_pids() { pgrep -f "$EEZ_NODE_BIN" 2>/dev/null | tr '\n' ' ' || true ; }
running_smokes() { pgrep -f "smoke-chiado.sh" 2>/dev/null | tr '\n' ' ' || true ; }

# Smoke wrappers first
for pid in $(running_smokes); do kill -TERM "$pid" 2>/dev/null; done

# eez-node graceful then forceful
for pid in $(straggler_pids); do kill -TERM "$pid" 2>/dev/null; done
for _ in $(seq 1 15); do
    [[ -z "$(straggler_pids)" ]] && break
    sleep 1
done
for pid in $(straggler_pids); do kill -KILL "$pid" 2>/dev/null; done
sleep 2

# Lighthouse via compose
EEZ_L1_JWT_SECRET=/tmp/eez-chiado-jwt.hex \
EEZ_CHIADO_LIGHTHOUSE_DATA=/tmp/eez-chiado-lighthouse \
EEZ_CHIADO_CONFIG_DIR=/root/configs/chiado \
EEZ_L1_AUTH_PORT=18651 \
docker compose -f "$COMPOSE_FILE" down >/dev/null 2>&1

# Orphan lock — only remove if NO process holds it
if [[ -f "$DATADIR_L1/db/lock" ]]; then
    if fuser "$DATADIR_L1/db/lock" >/dev/null 2>&1; then
        echo "ERROR: $DATADIR_L1/db/lock still held; can't tear down safely"
        fuser -v "$DATADIR_L1/db/lock" 2>&1
        exit 1
    fi
    rm -f "$DATADIR_L1/db/lock"
fi

# Verify clean
left_procs="$(straggler_pids)"
left_lighthouse="$(docker ps --filter 'name=chiado-lighthouse-eezrollup' --format '{{.Names}}')"
if [[ -n "$left_procs" || -n "$left_lighthouse" ]]; then
    echo "ERROR: cleanup incomplete. procs=[$left_procs] lighthouse=[$left_lighthouse]"
    exit 1
fi
echo "✓ chiado smoke environment torn down clean"
