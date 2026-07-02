#!/usr/bin/env bash
# Generate Lighthouse-compatible validator keystores for the devnet mnemonic.
# The ethereum-genesis-generator `all` target does NOT write keystores — only
# EL+CL genesis. eth2-val-tools (bundled in the same image) must be run
# separately with the SAME mnemonic + count as config/values.env, or the VC
# keys won't match the genesis validator set.
#
# Safe to run standalone if metadata/ already exists (does not wipe genesis).
#
# Usage:  bash infra/devnet-l1/scripts/gen-validator-keys.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
CONFIG_DIR="$ROOT/config"
DATA_DIR="$ROOT/data"
GENESIS_GEN_IMAGE="${GENESIS_GEN_IMAGE:-ethpandaops/ethereum-genesis-generator:4.0.0}"

[[ -f "$CONFIG_DIR/values.env" ]] || { echo "missing $CONFIG_DIR/values.env" >&2; exit 1; }

set -a
# shellcheck disable=SC1090
source "$CONFIG_DIR/values.env"
set +a

NUM="${NUMBER_OF_VALIDATORS:-64}"
MNEMONIC="${EL_AND_CL_MNEMONIC:?set EL_AND_CL_MNEMONIC in config/values.env}"

rm -rf "$DATA_DIR/validator-keys"
mkdir -p "$DATA_DIR/validator-keys"

echo "==> generating $NUM validator keystores (indices 0..$((NUM - 1)))"
# eth2-val-tools aborts with "output for assignments already exists" if any
# of its output subdirs are present in --out-loc — and a pre-created,
# bind-mounted /output dir trips that check. So generate into a FRESH
# container-internal path (/tmp/vk, guaranteed not to exist) and copy the
# result out to the mounted dir afterward. Mnemonic is passed via env
# (VMNEMONIC) so spaces don't need quoting gymnastics through `sh -c`.
docker run --rm \
    --entrypoint sh \
    -e VNUM="$NUM" \
    -e VMNEMONIC="$MNEMONIC" \
    -v "$DATA_DIR/validator-keys:/output" \
    "$GENESIS_GEN_IMAGE" -c '
        set -e
        eth2-val-tools keystores --insecure \
            --out-loc=/tmp/vk \
            --source-min=0 \
            --source-max="$VNUM" \
            --source-mnemonic="$VMNEMONIC"
        cp -a /tmp/vk/. /output/
    '

# Written as root inside the container (bind mount) — reclaim so the host
# user can read/rm these without sudo.
docker run --rm -v "$DATA_DIR/validator-keys:/output" alpine:3 \
    chown -R "$(id -u):$(id -g)" /output

count="$(find "$DATA_DIR/validator-keys/keys" -name voting-keystore.json 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$count" -lt 1 ]]; then
    echo "gen-validator-keys: no voting-keystore.json under $DATA_DIR/validator-keys/keys" >&2
    exit 1
fi

echo "gen-validator-keys: wrote $count keystores under $DATA_DIR/validator-keys/"
