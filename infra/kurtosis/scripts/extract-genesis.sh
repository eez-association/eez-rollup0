#!/usr/bin/env bash
# Extract the canonical genesis ethereum-package generated for the enclave, so
# Pair A joins the same chain as Pair B, and mint a local engine-API JWT shared
# by Pair A's reth and its follower CL. Outputs under eez-l1-data/.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
DEST="${EEZ_L1_DATA_DIR:-$REPO/infra/kurtosis/eez-l1-data}"
# Override if the package version names the genesis artifact differently.
ARTIFACT="${KURTOSIS_GENESIS_ARTIFACT:-el_cl_genesis_data}"

command -v kurtosis >/dev/null || { echo "kurtosis not found in PATH" >&2; exit 1; }
kurtosis enclave inspect "$ENCLAVE" >/dev/null 2>&1 || {
    echo "enclave '$ENCLAVE' not running — run kurtosis-up.sh first" >&2
    exit 1
}

RAW="$DEST/_artifact"
rm -rf "$DEST"
mkdir -p "$RAW"

echo "==> downloading '$ARTIFACT' from enclave '$ENCLAVE'"
kurtosis files download "$ENCLAVE" "$ARTIFACT" "$RAW"

# Layout varies by package version, so locate the files defensively.
echo "==> locating EL genesis + CL testnet-dir in the artifact"

# EL genesis: prefer genesis.json; else the first *.json with "config"+"alloc".
el_genesis=""
if [[ -f "$RAW/genesis.json" ]]; then
    el_genesis="$RAW/genesis.json"
else
    while IFS= read -r f; do
        if grep -ql '"alloc"' "$f" 2>/dev/null && grep -ql '"config"' "$f" 2>/dev/null; then
            el_genesis="$f"; break
        fi
    done < <(find "$RAW" -type f -name '*.json' | sort)
fi
[[ -n "$el_genesis" ]] || { echo "extract-genesis: no EL genesis.json found under $RAW" >&2; exit 1; }

# CL testnet-dir: the directory that contains config.yaml (+ genesis.ssz).
cl_config="$(find "$RAW" -type f -name config.yaml | head -1 || true)"
[[ -n "$cl_config" ]] || { echo "extract-genesis: no CL config.yaml found under $RAW" >&2; exit 1; }
cl_dir="$(dirname "$cl_config")"

mkdir -p "$DEST/cl" "$DEST/jwt"
cp "$el_genesis" "$DEST/genesis.json"
# Copy the whole CL dir so lighthouse --testnet-dir sees every sibling file
# (genesis.ssz, deposit_contract*.txt, boot_enr.yaml if present, etc.).
cp -a "$cl_dir/." "$DEST/cl/"

echo "==> verifying CL testnet artifacts"
required=(config.yaml genesis.ssz deposit_contract_block.txt)
for rel in "${required[@]}"; do
    if [[ ! -e "$DEST/cl/$rel" ]]; then
        echo "extract-genesis: missing $DEST/cl/$rel — artifact layout differs; inspect $RAW and adjust" >&2
        exit 1
    fi
done

echo "==> minting local engine-API JWT (Pair A reth <-> follower CL)"
if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32 | tr -d '\n' > "$DEST/jwt/jwtsecret"
else
    head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$DEST/jwt/jwtsecret"
fi

chain_id="$(grep -o '"chainId"[[:space:]]*:[[:space:]]*[0-9]*' "$DEST/genesis.json" | grep -o '[0-9]*$' || true)"

echo
echo "genesis ready (shared with Kurtosis Pair B):"
echo "  EL genesis : $DEST/genesis.json   (chainId=${chain_id:-?})"
echo "  CL testnet : $DEST/cl/"
echo "  jwt        : $DEST/jwt/jwtsecret"
echo "  (raw artifact kept at $RAW for inspection)"
echo
echo "next: bash infra/kurtosis/scripts/get-cl-bootnode.sh"
