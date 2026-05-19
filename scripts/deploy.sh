#!/usr/bin/env bash
#
# Deploys the upstream EEZ + ECDSAProofSystem + Rollup manager + creates
# our rollupId — mirrors the 5-step sequence in
# /root/rollup-node/scripts/devnet-compose-up.sh.
#
# Reads from .env (poster key, proof signer key, RPC url, etc.).
# Writes deployments.env with the addresses + rollupId + deploy block.
# `eez-node` auto-loads BOTH .env and deployments.env (via dotenvy at
# startup), so a successful run of this script leaves the workspace
# ready for `make run-node`.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$REPO/.env"
OUT_FILE="$REPO/deployments.env"

# ── Inputs from .env ──────────────────────────────────────────────────
[[ -f "$ENV_FILE" ]] || { echo "deploy: $ENV_FILE not found — copy .env.example to .env first" >&2; exit 1; }
# shellcheck disable=SC1090
source "$ENV_FILE"

: "${EEZ_L1_RPC_URL:?EEZ_L1_RPC_URL not set in .env}"
: "${EEZ_L1_POSTER_KEY:?EEZ_L1_POSTER_KEY not set in .env}"
: "${EEZ_PROOF_SIGNER_KEY:?EEZ_PROOF_SIGNER_KEY not set in .env}"

# Genesis state root for the rollup. Defaults to bytes32(0) — the same
# value the rollup-node devnet uses for a fresh L2.
EEZ_INITIAL_STATE_ROOT="${EEZ_INITIAL_STATE_ROOT:-0x0000000000000000000000000000000000000000000000000000000000000000}"

# Derive addresses from keys.
AUTHORIZED_SIGNER="$(cast wallet address --private-key "$EEZ_PROOF_SIGNER_KEY")"
OWNER="$(cast wallet address --private-key "$EEZ_L1_POSTER_KEY")"

echo "deploy: RPC                  = $EEZ_L1_RPC_URL"
echo "deploy: poster / owner       = $OWNER"
echo "deploy: authorized signer    = $AUTHORIZED_SIGNER"
echo "deploy: initial state root   = $EEZ_INITIAL_STATE_ROOT"
echo

CONTRACTS="$REPO/contracts"
RPC="--rpc-url $EEZ_L1_RPC_URL"
KEY="--private-key $EEZ_L1_POSTER_KEY"

# Each Foundry deploy script logs `KEY=VALUE` lines via `console.log`.
# `forge script --silent` still emits these via the script's stdout
# stream; we capture and grep them out per step.

# Helper: parse `KEY=value` line out of forge output, lowercase the
# value (cast prints checksummed; lowercase keeps comparisons easy).
extract() {
    local key="$1" out="$2"
    grep -oE "${key}=0x[0-9a-fA-F]+" <<< "$out" | tail -1 | cut -d= -f2 | tr '[:upper:]' '[:lower:]'
}
extract_uint() {
    local key="$1" out="$2"
    grep -oE "${key}=[0-9]+" <<< "$out" | tail -1 | cut -d= -f2
}

# ── 1/5 DeployEEZ ────────────────────────────────────────────────────
echo "[1/5] DeployEEZ"
OUT="$(cd "$CONTRACTS" && forge script script/DeployEEZ.s.sol:DeployEEZ $RPC $KEY --broadcast 2>&1)"
EEZ_REGISTRY_ADDRESS="$(extract EEZ "$OUT")"
[[ -n "$EEZ_REGISTRY_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture EEZ address" >&2; exit 1; }
EEZ_REGISTRY_DEPLOY_BLOCK="$(cast block-number --rpc-url "$EEZ_L1_RPC_URL")"
echo "      EEZ        = $EEZ_REGISTRY_ADDRESS"
echo "      deployBlock= $EEZ_REGISTRY_DEPLOY_BLOCK"

# ── 2/5 DeployECDSAProofSystem ──────────────────────────────────────
echo "[2/5] DeployECDSAProofSystem(authorizedSigner=$AUTHORIZED_SIGNER)"
OUT="$(cd "$CONTRACTS" && forge script script/DeployECDSAProofSystem.s.sol:DeployECDSAProofSystem \
    --sig "run(address)" "$AUTHORIZED_SIGNER" $RPC $KEY --broadcast 2>&1)"
EEZ_ECDSA_PROOF_SYSTEM_ADDRESS="$(extract ECDSA_PS "$OUT")"
[[ -n "$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture ECDSA_PS address" >&2; exit 1; }
echo "      ECDSA_PS   = $EEZ_ECDSA_PROOF_SYSTEM_ADDRESS"

# ── 3/5 BurnRollupZero ──────────────────────────────────────────────
echo "[3/5] BurnRollupZero"
OUT="$(cd "$CONTRACTS" && forge script script/BurnRollupZero.s.sol:BurnRollupZero \
    --sig "run(address,address,address,address)" \
    "$EEZ_REGISTRY_ADDRESS" "$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS" "$AUTHORIZED_SIGNER" "$OWNER" \
    $RPC $KEY --broadcast 2>&1)"
EEZ_BURN_ROLLUP_ADDRESS="$(extract BURN_ROLLUP "$OUT")"
[[ -n "$EEZ_BURN_ROLLUP_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: BurnRollupZero failed" >&2; exit 1; }
echo "      burnRollup = $EEZ_BURN_ROLLUP_ADDRESS"

# ── 4/5 DeployRollup ────────────────────────────────────────────────
echo "[4/5] DeployRollup"
OUT="$(cd "$CONTRACTS" && forge script script/DeployRollup.s.sol:DeployRollup \
    --sig "run(address,address,address,address)" \
    "$EEZ_REGISTRY_ADDRESS" "$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS" "$AUTHORIZED_SIGNER" "$OWNER" \
    $RPC $KEY --broadcast 2>&1)"
EEZ_ROLLUP_MANAGER_ADDRESS="$(extract ROLLUP_CONTRACT "$OUT")"
[[ -n "$EEZ_ROLLUP_MANAGER_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: DeployRollup failed" >&2; exit 1; }
echo "      rollupMgr  = $EEZ_ROLLUP_MANAGER_ADDRESS"

# ── 5/5 RegisterRollup ──────────────────────────────────────────────
echo "[5/5] RegisterRollup(initialState=$EEZ_INITIAL_STATE_ROOT)"
OUT="$(cd "$CONTRACTS" && forge script script/RegisterRollup.s.sol:RegisterRollup \
    --sig "run(address,address,bytes32)" \
    "$EEZ_REGISTRY_ADDRESS" "$EEZ_ROLLUP_MANAGER_ADDRESS" "$EEZ_INITIAL_STATE_ROOT" \
    $RPC $KEY --broadcast 2>&1)"
EEZ_ROLLUP_ID="$(extract_uint L2_ROLLUP_ID "$OUT")"
[[ -n "$EEZ_ROLLUP_ID" ]] || { echo "$OUT" >&2; echo "deploy: RegisterRollup failed" >&2; exit 1; }
echo "      rollupId   = $EEZ_ROLLUP_ID"

# ── Write deployments.env ───────────────────────────────────────────
cat > "$OUT_FILE" <<EOF
# Generated by scripts/deploy.sh — do not edit by hand. Gitignored.
# Sourced automatically at eez-node startup (alongside .env) via dotenvy.

EEZ_REGISTRY_ADDRESS=$EEZ_REGISTRY_ADDRESS
EEZ_REGISTRY_DEPLOY_BLOCK=$EEZ_REGISTRY_DEPLOY_BLOCK
EEZ_ECDSA_PROOF_SYSTEM_ADDRESS=$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS
EEZ_ROLLUP_MANAGER_ADDRESS=$EEZ_ROLLUP_MANAGER_ADDRESS
EEZ_BURN_ROLLUP_ADDRESS=$EEZ_BURN_ROLLUP_ADDRESS
EEZ_ROLLUP_ID=$EEZ_ROLLUP_ID
EOF

echo
echo "deploy: wrote $OUT_FILE"
echo "deploy: ready. \`make run-node\` will pick these up automatically."
