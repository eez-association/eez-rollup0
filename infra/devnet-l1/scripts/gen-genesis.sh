#!/usr/bin/env bash
# Generate a matched EL + CL genesis (and validator keys + JWT) for the EEZ
# private PoS L1 devnet, using ethpandaops/ethereum-genesis-generator — the
# same tool the Kurtosis ethereum-package uses internally, so the genesis
# shape is identical across the standalone (this) and Kurtosis (Phase 3)
# harnesses.
#
# Outputs (under infra/devnet-l1/data/):
#   metadata/genesis.json              -> EL genesis  (eez-node EEZ_L1_CHAIN_PATH)
#   metadata/{config.yaml,genesis.ssz,deposit_contract_block.txt,...}
#                                      -> CL testnet-dir (lighthouse --testnet-dir)
#   validator-keys/                    -> validator keystores + secrets (lighthouse VC)
#   jwt/jwtsecret                      -> shared engine-API JWT (eez-node + lighthouse)
#
# Re-running regenerates from scratch (fresh genesis time). Stop the CL +
# eez-node and wipe their datadirs before regenerating, or they'll reject
# the new genesis.
#
# Usage:  bash infra/devnet-l1/scripts/gen-genesis.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
CONFIG_DIR="$ROOT/config"
DATA_DIR="$ROOT/data"

# Pin for reproducible genesis. Bump deliberately.
GENESIS_GEN_IMAGE="${GENESIS_GEN_IMAGE:-ethpandaops/ethereum-genesis-generator:4.0.0}"

if [ ! -f "$CONFIG_DIR/values.env" ]; then
    echo "missing $CONFIG_DIR/values.env" >&2
    exit 1
fi

# The generator image writes as root inside the container, so everything
# under $DATA_DIR ends up root-owned on the host (bind mount, not a
# volume). Reclaim ownership after every docker run that touches it, so a
# plain `rm -rf` works next time instead of failing with "Permission
# denied" (docker run as root is the fix, not sudo on the host).
reclaim_ownership() {
    docker run --rm -v "$DATA_DIR:/data" alpine:3 \
        chown -R "$(id -u):$(id -g)" /data
}

echo "==> wiping previous genesis under $DATA_DIR"
if ! rm -rf "$DATA_DIR" 2>/dev/null; then
    echo "    previous run left root-owned files — reclaiming via docker, then retrying"
    mkdir -p "$DATA_DIR"
    reclaim_ownership
    rm -rf "$DATA_DIR"
fi
mkdir -p "$DATA_DIR"

# Wall-clock genesis base time, computed on the HOST and passed as a plain
# integer. The generator does genesis_time = GENESIS_TIMESTAMP + GENESIS_DELAY.
# Must NOT be set via $(date +%s) inside values.env — that file is sourced
# inside the container where the substitution mangles to a far-future value.
GENESIS_TIMESTAMP="${GENESIS_TIMESTAMP:-$(date +%s)}"
echo "==> generating EL+CL genesis with $GENESIS_GEN_IMAGE (GENESIS_TIMESTAMP=$GENESIS_TIMESTAMP)"
# 'all' = EL + CL genesis only (NOT validator keystores — see next step).
docker run --rm \
    -e GENESIS_TIMESTAMP="$GENESIS_TIMESTAMP" \
    -v "$CONFIG_DIR/values.env:/config/values.env:ro" \
    -v "$DATA_DIR:/data" \
    "$GENESIS_GEN_IMAGE" all
reclaim_ownership

echo "==> generating validator keystores (eth2-val-tools in the same image)"
bash "$HERE/gen-validator-keys.sh"
reclaim_ownership

echo "==> generating shared engine-API JWT"
mkdir -p "$DATA_DIR/jwt"
if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32 | tr -d '\n' > "$DATA_DIR/jwt/jwtsecret"
else
    head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$DATA_DIR/jwt/jwtsecret"
fi

echo "==> verifying CL testnet artifacts"
required=(
    metadata/genesis.json
    metadata/config.yaml
    metadata/genesis.ssz
    metadata/deposit_contract_block.txt
    metadata/deposit_contract.txt
)
for rel in "${required[@]}"; do
    if [[ ! -e "$DATA_DIR/$rel" ]]; then
        echo "gen-genesis: missing $DATA_DIR/$rel — CL/EL generation likely failed" >&2
        exit 1
    fi
done
if [[ ! -d "$DATA_DIR/validator-keys/keys" ]]; then
    echo "gen-genesis: missing $DATA_DIR/validator-keys/keys" >&2
    exit 1
fi

echo
echo "genesis ready:"
echo "  EL genesis : $DATA_DIR/metadata/genesis.json"
echo "  CL testnet : $DATA_DIR/metadata/"
echo "  val keys   : $DATA_DIR/validator-keys/"
echo "  jwt        : $DATA_DIR/jwt/jwtsecret"
echo
echo "next: bash infra/devnet-l1/scripts/run-devnet-l1.sh"
