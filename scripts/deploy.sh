#!/usr/bin/env bash
#
# Deploys EEZ + ECDSAProofSystem + Rollup manager, registers the rollup,
# and deploys the L1 bridge contracts.
#
# Reads from .env (poster key, proof signer key, RPC url, etc.).
# Writes deployments.env with the addresses + rollupId + deploy block.
# `eez-node` auto-loads BOTH .env and deployments.env (via dotenvy at
# startup), so a successful run of this script leaves the workspace
# ready for `make run-node`.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${EEZ_ENV_FILE:-$REPO/.env}"
OUT_FILE="${EEZ_DEPLOYMENTS_FILE:-$REPO/deployments.env}"

# Timestamp marker — used at the end to find broadcast/ entries produced
# by *this* run (vs stale ones from prior chain ids or earlier deploys).
START_MARKER="$(mktemp -t eez-deploy.XXXXXX)"
GENESIS_TIMESTAMP_TMP=""
cleanup() {
    rm -f "$START_MARKER"
    [[ -z "$GENESIS_TIMESTAMP_TMP" ]] || rm -f "$GENESIS_TIMESTAMP_TMP"
}
trap cleanup EXIT

# ── Inputs from .env ──────────────────────────────────────────────────
[[ -f "$ENV_FILE" ]] || { echo "deploy: $ENV_FILE not found — copy .env.example to .env first" >&2; exit 1; }
# shellcheck disable=SC1090
source "$ENV_FILE"

: "${EEZ_L1_RPC_URL:?EEZ_L1_RPC_URL not set in .env}"
: "${EEZ_L1_POSTER_KEY:?EEZ_L1_POSTER_KEY not set in .env}"
: "${EEZ_PROOF_SIGNER_KEY:?EEZ_PROOF_SIGNER_KEY not set in .env}"
: "${EEZ_L2_SYSTEM_KEY:?EEZ_L2_SYSTEM_KEY not set in .env}"

configured_initial_state_root="${EEZ_INITIAL_STATE_ROOT:-}"

# Deploy/own key, decoupled from the composer's poster so posting can't advance
# the deployer's nonce or shift the CREATE addresses. Defaults to the poster.
EEZ_DEPLOY_KEY="${EEZ_DEPLOY_KEY:-$EEZ_L1_POSTER_KEY}"

# Derive addresses from keys.
AUTHORIZED_SIGNER="$(cast wallet address --private-key "$EEZ_PROOF_SIGNER_KEY")"
PROOF_SYSTEM_VKEY="0x000000000000000000000000${AUTHORIZED_SIGNER#0x}"
OWNER="$(cast wallet address --private-key "$EEZ_DEPLOY_KEY")"
EEZ_L2_SYSTEM_ADDRESS="$(cast wallet address --private-key "$EEZ_L2_SYSTEM_KEY")"
if [[ "${AUTHORIZED_SIGNER,,}" == "${EEZ_L2_SYSTEM_ADDRESS,,}" ]]; then
    echo "deploy: proof attestation and L2 system keys must be different" >&2
    exit 1
fi

echo "deploy: RPC                  = $EEZ_L1_RPC_URL"
echo "deploy: deployer / owner     = $OWNER"
echo "deploy: authorized signer    = $AUTHORIZED_SIGNER"
echo "deploy: L2 system address    = $EEZ_L2_SYSTEM_ADDRESS"
echo

CONTRACTS="$REPO/contracts"
RPC="--rpc-url $EEZ_L1_RPC_URL"
KEY="--private-key $EEZ_DEPLOY_KEY"

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

# Runs a forge script command, capturing combined stdout+stderr. On
# non-zero exit (e.g., RPC down, gas insufficient, simulation revert),
# prints the captured output before bailing — without this wrapper,
# `set -e` would abort the `OUT=$(...)` assignment and forge's error
# message would be lost.
run_forge() {
    local label="$1"; shift
    local extra=()
    # On flaky public RPCs (chiado endpoints), forge's pre-broadcast
    # simulation refetches state at a block the RPC hasn't finalized
    # yet → "block not found". Smokes set EEZ_DEPLOY_SKIP_SIMULATION=1
    # to bypass that phase. Local dev L1 leaves it unset.
    if [[ "${EEZ_DEPLOY_SKIP_SIMULATION:-0}" == "1" ]]; then
        extra+=(--skip-simulation)
    fi
    if ! OUT="$(cd "$CONTRACTS" && "$@" "${extra[@]}" 2>&1)"; then
        echo "$OUT" >&2
        echo "deploy: $label failed (forge non-zero exit)" >&2
        exit 1
    fi
}

compute_genesis_state_root() {
    local genesis="$1"
    if command -v eez-genesis-state-root >/dev/null 2>&1; then
        eez-genesis-state-root "$genesis"
    else
        (
            cd "$REPO"
            cargo run --quiet --locked --package eez-node --example genesis_state_root -- "$genesis"
        )
    fi
}

# ── 1/5 DeployEEZ ────────────────────────────────────────────────────
echo "[1/5] DeployEEZ"
run_forge "DeployEEZ" forge script script/DeployEEZ.s.sol:DeployEEZ \
    --sig "run(address)" "$OWNER" $RPC $KEY --broadcast
EEZ_REGISTRY_ADDRESS="$(extract EEZ "$OUT")"
[[ -n "$EEZ_REGISTRY_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture EEZ address" >&2; exit 1; }
EEZ_REGISTRY_DEPLOY_BLOCK="$(cast block-number --rpc-url "$EEZ_L1_RPC_URL")"
echo "      EEZ        = $EEZ_REGISTRY_ADDRESS"
echo "      deployBlock= $EEZ_REGISTRY_DEPLOY_BLOCK"

# ── 2/5 DeployECDSAProofSystem ───────────────────────────────────────
echo "[2/5] DeployECDSAProofSystem(authorizedSigner=$AUTHORIZED_SIGNER)"
run_forge "DeployECDSAProofSystem" forge script script/DeployECDSAProofSystem.s.sol:DeployECDSAProofSystem \
    --sig "run(address)" "$AUTHORIZED_SIGNER" $RPC $KEY --broadcast
EEZ_ECDSA_PROOF_SYSTEM_ADDRESS="$(extract ECDSA_PS "$OUT")"
[[ -n "$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture ECDSA_PS address" >&2; exit 1; }
echo "      ECDSA_PS   = $EEZ_ECDSA_PROOF_SYSTEM_ADDRESS"

# ── 3/5 DeployRollup ────────────────────────────────────────────────
echo "[3/5] DeployRollup"
run_forge "DeployRollup" forge script script/DeployRollup.s.sol:DeployRollup \
    --sig "run(address,address,address,address)" \
    "$EEZ_REGISTRY_ADDRESS" "$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS" "$AUTHORIZED_SIGNER" "$OWNER" \
    $RPC $KEY --broadcast
EEZ_ROLLUP_MANAGER_ADDRESS="$(extract ROLLUP_CONTRACT "$OUT")"
[[ -n "$EEZ_ROLLUP_MANAGER_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture ROLLUP_CONTRACT address" >&2; exit 1; }
echo "      rollupMgr  = $EEZ_ROLLUP_MANAGER_ADDRESS"

# ── Generate deployment-bound L2 genesis ────────────────────────────
# This deploy creates a fresh registry, so the first L2 rollup ID is 1.
# Generate EEZL2 with that ID and the address derived from the secret system
# key before registering the resulting state root on L1.
ROLLUP_COUNTER="$(cast call "$EEZ_REGISTRY_ADDRESS" "rollupCounter()(uint256)" \
    --rpc-url "$EEZ_L1_RPC_URL" | awk '{print $1}')"
[[ "$ROLLUP_COUNTER" == "0" ]] || {
    echo "deploy: fresh EEZ registry unexpectedly has rollupCounter=$ROLLUP_COUNTER" >&2
    exit 1
}
EXPECTED_ROLLUP_ID=1
GENESIS_OUT="${EEZ_GENESIS_OUT:-$REPO/datadir/genesis.json}"
GENESIS_PROFILE_OUT="${EEZ_GENESIS_PROFILE_OUT:-${GENESIS_OUT%.json}.profile.json}"
GENESIS_BASE="${EEZ_GENESIS_BASE:-$REPO/genesis.json}"

echo "      rendering L2 genesis (rollupId=$EXPECTED_ROLLUP_ID, system=$EEZ_L2_SYSTEM_ADDRESS)"
"$REPO/scripts/update-eezl2-genesis.sh" --render \
    --rollup-id "$EXPECTED_ROLLUP_ID" \
    --system-address "$EEZ_L2_SYSTEM_ADDRESS" \
    --base "$GENESIS_BASE" \
    --output "$GENESIS_OUT" \
    --profile-output "$GENESIS_PROFILE_OUT"

EEZ_INITIAL_STATE_ROOT="$(jq -er '.genesisStateRoot' "$GENESIS_PROFILE_OUT")"
EEZ_L2_EEZL2_CODE_HASH="$(jq -er '.runtimeCodeHash' "$GENESIS_PROFILE_OUT")"
if [[ -n "$configured_initial_state_root" \
    && "${configured_initial_state_root,,}" != "${EEZ_INITIAL_STATE_ROOT,,}" ]]
then
    echo "deploy: configured EEZ_INITIAL_STATE_ROOT does not match the rendered L2 genesis" >&2
    echo "configured: $configured_initial_state_root" >&2
    echo "rendered:   $EEZ_INITIAL_STATE_ROOT" >&2
    exit 1
fi
echo "      stateRoot  = $EEZ_INITIAL_STATE_ROOT"
echo "      runtimeHash= $EEZ_L2_EEZL2_CODE_HASH"

# ── 4/5 RegisterRollup ──────────────────────────────────────────────
echo "[4/5] RegisterRollup(initialState=$EEZ_INITIAL_STATE_ROOT)"
run_forge "RegisterRollup" forge script script/RegisterRollup.s.sol:RegisterRollup \
    --sig "run(address,address,bytes32)" \
    "$EEZ_REGISTRY_ADDRESS" "$EEZ_ROLLUP_MANAGER_ADDRESS" "$EEZ_INITIAL_STATE_ROOT" \
    $RPC $KEY --broadcast
EEZ_ROLLUP_ID="$(extract_uint L2_ROLLUP_ID "$OUT")"
[[ -n "$EEZ_ROLLUP_ID" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture L2_ROLLUP_ID" >&2; exit 1; }
[[ "$EEZ_ROLLUP_ID" == "$EXPECTED_ROLLUP_ID" ]] || {
    echo "deploy: registry assigned rollupId=$EEZ_ROLLUP_ID; generated EEZL2 expects $EXPECTED_ROLLUP_ID" >&2
    exit 1
}
echo "      rollupId   = $EEZ_ROLLUP_ID"

# ── 5/5 DeployBridgeL1 ──────────────────────────────────────────────
# Creates the L1 CrossChainProxy (representing the L2 BridgeReceiver
# predeploy at `0x4200…0008`) and a user-facing BridgeSender. The
# composer's first cross-chain smoke routes a deposit through these.
EEZ_L2_BRIDGE_RECEIVER_DEFAULT="0x4200000000000000000000000000000000000008"
EEZ_L2_BRIDGE_RECEIVER="${EEZ_L2_BRIDGE_RECEIVER:-$EEZ_L2_BRIDGE_RECEIVER_DEFAULT}"
echo "[5/5] DeployBridgeL1(l2Dest=$EEZ_L2_BRIDGE_RECEIVER, rollupId=$EEZ_ROLLUP_ID)"
EEZ_REGISTRY_ADDRESS="$EEZ_REGISTRY_ADDRESS" \
EEZ_L2_BRIDGE_RECEIVER="$EEZ_L2_BRIDGE_RECEIVER" \
EEZ_ROLLUP_ID="$EEZ_ROLLUP_ID" \
run_forge "DeployBridgeL1" forge script script/DeployBridgeL1.s.sol:DeployBridgeL1 $RPC $KEY --broadcast
EEZ_L1_L2_PROXY="$(extract L2_PROXY "$OUT")"
EEZ_L1_BRIDGE_SENDER="$(extract BRIDGE_SENDER "$OUT")"
[[ -n "$EEZ_L1_L2_PROXY"     ]] || { echo "$OUT" >&2; echo "deploy: failed to capture L2_PROXY" >&2; exit 1; }
[[ -n "$EEZ_L1_BRIDGE_SENDER" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture BRIDGE_SENDER" >&2; exit 1; }
echo "      L2 proxy   = $EEZ_L1_L2_PROXY"
echo "      L1 bridge  = $EEZ_L1_BRIDGE_SENDER"

# ── L2 genesis with deploy-aligned timestamp ────────────────────────
# Reth's `--chain dev` prebaked genesis has timestamp = June 2023.
# The Sequencer's greedy backfill loop produces blocks at
# `parent.timestamp + 2s` until wall-clock — from a 2023 genesis
# that's ~47M blocks of pure timestamp catch-up to reach 2026.
# Useless work. We write a per-deploy genesis with timestamp set to
# the L1 block observed after deploying the EEZ registry, so catch-up
# only bridges deployment time to now. Timestamp and fork activation do not
# change the genesis state root, but verify that explicitly before publishing.
DEPLOY_BLOCK_TS_HEX="$(cast block "$EEZ_REGISTRY_DEPLOY_BLOCK" --rpc-url "$EEZ_L1_RPC_URL" --json | jq -r '.timestamp')"
[[ -n "$DEPLOY_BLOCK_TS_HEX" && "$DEPLOY_BLOCK_TS_HEX" != "null" ]] || {
    echo "deploy: failed to capture L1 block timestamp for $EEZ_REGISTRY_DEPLOY_BLOCK" >&2
    exit 1
}
GENESIS_TIMESTAMP_TMP="$(mktemp "${GENESIS_OUT}.tmp.XXXXXX")"
jq --arg timestamp "$DEPLOY_BLOCK_TS_HEX" '
    .timestamp = $timestamp
    | .config += {
        homesteadBlock: 0,
        eip150Block: 0,
        eip155Block: 0,
        eip158Block: 0,
        byzantiumBlock: 0,
        constantinopleBlock: 0,
        petersburgBlock: 0,
        istanbulBlock: 0,
        muirGlacierBlock: 0,
        berlinBlock: 0,
        londonBlock: 0,
        arrowGlacierBlock: 0,
        grayGlacierBlock: 0,
        mergeNetsplitBlock: 0,
        shanghaiTime: 0,
        cancunTime: 0,
        pragueTime: 0,
        osakaTime: 0,
        terminalTotalDifficulty: 0,
        terminalTotalDifficultyPassed: true
    }
' "$GENESIS_OUT" >"$GENESIS_TIMESTAMP_TMP"
chmod --reference="$GENESIS_OUT" "$GENESIS_TIMESTAMP_TMP"
mv "$GENESIS_TIMESTAMP_TMP" "$GENESIS_OUT"

FINAL_STATE_ROOT="$(compute_genesis_state_root "$GENESIS_OUT")"
if [[ "${FINAL_STATE_ROOT,,}" != "${EEZ_INITIAL_STATE_ROOT,,}" ]]; then
    echo "deploy: finalized genesis state root changed after registration" >&2
    echo "registered: $EEZ_INITIAL_STATE_ROOT" >&2
    echo "finalized:  $FINAL_STATE_ROOT" >&2
    exit 1
fi
echo "      genesis.ts = $DEPLOY_BLOCK_TS_HEX ($(printf %d $DEPLOY_BLOCK_TS_HEX))"

# ── Write deployments.env ───────────────────────────────────────────
cat > "$OUT_FILE" <<EOF
# Generated by scripts/deploy.sh — do not edit by hand. Gitignored.
# Sourced automatically at eez-node startup (alongside .env) via dotenvy.

EEZ_REGISTRY_ADDRESS=$EEZ_REGISTRY_ADDRESS
EEZ_REGISTRY_DEPLOY_BLOCK=$EEZ_REGISTRY_DEPLOY_BLOCK
EEZ_ECDSA_PROOF_SYSTEM_ADDRESS=$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS
EEZ_PROOF_SYSTEM_KIND=real
EEZ_PROOF_SYSTEM=$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS
EEZ_VKEY=$PROOF_SYSTEM_VKEY
EEZ_ATTESTER_ADDRESS=$AUTHORIZED_SIGNER
EEZ_ROLLUP_MANAGER_ADDRESS=$EEZ_ROLLUP_MANAGER_ADDRESS
EEZ_ROLLUP_ID=$EEZ_ROLLUP_ID
EEZ_INITIAL_STATE_ROOT=$EEZ_INITIAL_STATE_ROOT
EEZ_L2_GENESIS_PATH=$GENESIS_OUT
EEZ_L2_GENESIS_PROFILE_PATH=$GENESIS_PROFILE_OUT

# L1 cross-chain bridge contracts (DeployBridgeL1).
EEZ_L1_L2_PROXY=$EEZ_L1_L2_PROXY
EEZ_L1_BRIDGE_SENDER=$EEZ_L1_BRIDGE_SENDER
EEZ_L2_BRIDGE_RECEIVER=$EEZ_L2_BRIDGE_RECEIVER

# EEZL2 predeploy baked into genesis.json.
EEZL2_ADDRESS=0x4200000000000000000000000000000000000007
# Public deployment binding derived from EEZ_L2_SYSTEM_KEY. The secret key is
# deliberately not written to this file.
EEZ_L2_SYSTEM_ADDRESS=$EEZ_L2_SYSTEM_ADDRESS
EEZ_L2_EEZL2_CODE_HASH=$EEZ_L2_EEZL2_CODE_HASH
EOF

echo
echo "deploy: wrote $OUT_FILE"

# ── Optional: Blockscout verification ────────────────────────────────
# Delegated to scripts/verify-blockscout.sh (best-effort; no-op unless
# EEZ_BLOCKSCOUT_URL is set). START_MARKER (created at script start)
# scopes it to THIS run's fresh CREATE entries so a re-run doesn't
# re-submit stale addresses. Never fails the deploy.
echo
EEZ_BLOCKSCOUT_URL="${EEZ_BLOCKSCOUT_URL:-}" EEZ_CONTRACTS_DIR="$CONTRACTS" \
EEZ_L1_RPC_URL="$EEZ_L1_RPC_URL" \
    "$REPO/scripts/verify-blockscout.sh" "$START_MARKER" || true

echo
echo "deploy: ready. \`make run-node\` will pick these up automatically."
