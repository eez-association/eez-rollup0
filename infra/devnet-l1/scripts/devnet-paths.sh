#!/usr/bin/env bash
# Resolve devnet-l1 paths from infra/devnet-l1/.env.
#
# Paths in .env are relative to infra/devnet-l1/ (same as docker compose).
# Scripts invoked from the repo root (run-devnet-l1.sh, deploy-eez.sh) source
# this after loading .env to turn ./data/... into absolute paths.
DEVNET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

devnet_resolve_path() {
    local p="$1"
    if [[ "$p" == ./* ]]; then
        echo "$DEVNET_ROOT/${p#./}"
    else
        echo "$p"
    fi
}

devnet_resolve_paths() {
    DEVNET_DATA_DIR="$(devnet_resolve_path "${DEVNET_DATA_DIR:-./data}")"
    DEVNET_JWT_FILE="$(devnet_resolve_path "${DEVNET_JWT_FILE:-./data/jwt/jwtsecret}")"
    EEZ_L1_CHAIN_PATH="$(devnet_resolve_path "${EEZ_L1_CHAIN_PATH:-./data/metadata/genesis.json}")"
    EEZ_L1_JWT_SECRET="$(devnet_resolve_path "${EEZ_L1_JWT_SECRET:-./data/jwt/jwtsecret}")"
    export DEVNET_DATA_DIR DEVNET_JWT_FILE EEZ_L1_CHAIN_PATH EEZ_L1_JWT_SECRET
}

devnet_verify_cl_testnet_dir() {
    local dir="${1:-$DEVNET_DATA_DIR/metadata}"
    local missing=0
    for f in config.yaml genesis.ssz deposit_contract_block.txt deposit_contract.txt; do
        if [[ ! -f "$dir/$f" ]]; then
            echo "devnet-l1: missing $dir/$f" >&2
            missing=1
        fi
    done
    if (( missing )); then
        echo "devnet-l1: CL testnet dir incomplete — rerun gen-genesis.sh or fix DEVNET_DATA_DIR in .env" >&2
        echo "  (docker compose resolves ./data relative to infra/devnet-l1/, not the repo root)" >&2
        return 1
    fi
}
