#!/usr/bin/env bash
#
# Deploys the upstream EEZ + the proof system (EEZ_PROOF_SYSTEM=mock|real,
# the latter = the ECDSAProofSystem that recovers over the real publicInputsHash)
# + Rollup manager +
# creates our rollupId — 4 steps.
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
trap 'rm -f "$START_MARKER"' EXIT

# ── Inputs from .env ──────────────────────────────────────────────────
[[ -f "$ENV_FILE" ]] || { echo "deploy: $ENV_FILE not found — copy .env.example to .env first" >&2; exit 1; }
# shellcheck disable=SC1090
source "$ENV_FILE"

: "${EEZ_L1_RPC_URL:?EEZ_L1_RPC_URL not set in .env}"
: "${EEZ_L1_POSTER_KEY:?EEZ_L1_POSTER_KEY not set in .env}"
: "${EEZ_PROOF_SIGNER_KEY:?EEZ_PROOF_SIGNER_KEY not set in .env}"

# Genesis state root for the rollup — what the contract stores as
# `rollups[rid].stateRoot` at RegisterRollup time. The first batch's
# `StateDelta.currentState` must match this value, so it has to equal
# the L2 reth genesis state root.
#
# Pinned to our `genesis.json` (SystemAccount predeposit per Rollup-1.md §3.1 +
# the EIP-2935 history contract so empty blocks advance state). Recompute when
# the alloc changes; override via the env var for a different L2 chain spec
# (e.g. the live chiado deploy passes EEZ_INITIAL_STATE_ROOT=0x037cf8a5… for its
# 2935-baked genesis).
EEZ_INITIAL_STATE_ROOT="${EEZ_INITIAL_STATE_ROOT:-0x49ecae4742217e1f57d67acf2b9dcfa7fcae5ad7a841423c18ad0a9acd6e4eba}"

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

# ── 1/4 DeployEEZ ────────────────────────────────────────────────────
echo "[1/4] DeployEEZ"
run_forge "DeployEEZ" forge script script/DeployEEZ.s.sol:DeployEEZ $RPC $KEY --broadcast
EEZ_REGISTRY_ADDRESS="$(extract EEZ "$OUT")"
[[ -n "$EEZ_REGISTRY_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture EEZ address" >&2; exit 1; }
EEZ_REGISTRY_DEPLOY_BLOCK="$(cast block-number --rpc-url "$EEZ_L1_RPC_URL")"
echo "      EEZ        = $EEZ_REGISTRY_ADDRESS"
echo "      deployBlock= $EEZ_REGISTRY_DEPLOY_BLOCK"

# ── 2/4 Proof system ────────────────────────────────────────────────
# EEZ_PROOF_SYSTEM=mock (dev: recovers over a FIXED digest) | real (the
# ECDSAProofSystem that recovers over the ACTUAL publicInputsHash — the
# out-of-process prover `eez-proverd` signs exactly that).
EEZ_PROOF_SYSTEM="${EEZ_PROOF_SYSTEM:-mock}"
if [[ "$EEZ_PROOF_SYSTEM" == "real" ]]; then
    echo "[2/4] DeployECDSAProofSystem (REAL — owner=$OWNER signer=$AUTHORIZED_SIGNER)"
    run_forge "DeployECDSAProofSystem" forge script script/DeployECDSAProofSystem.s.sol:DeployECDSAProofSystem \
        --sig "run(address,address)" "$OWNER" "$AUTHORIZED_SIGNER" $RPC $KEY --broadcast
    PROOF_SYSTEM_ADDRESS="$(extract ECDSA_PS "$OUT")"
    [[ -n "$PROOF_SYSTEM_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture ECDSA_PS address" >&2; exit 1; }
elif [[ "$EEZ_PROOF_SYSTEM" == "mock" ]]; then
    echo "[2/4] DeployMockECDSAProofSystem(authorizedSigner=$AUTHORIZED_SIGNER)"
    run_forge "DeployMockECDSAProofSystem" forge script script/DeployMockECDSAProofSystem.s.sol:DeployMockECDSAProofSystem \
        --sig "run(address)" "$AUTHORIZED_SIGNER" $RPC $KEY --broadcast
    PROOF_SYSTEM_ADDRESS="$(extract MOCK_PS "$OUT")"
    [[ -n "$PROOF_SYSTEM_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture MOCK_PS address" >&2; exit 1; }
else
    echo "deploy: EEZ_PROOF_SYSTEM must be 'mock' or 'real', got '$EEZ_PROOF_SYSTEM'" >&2; exit 1
fi
# The composer still reads EEZ_MOCK_PROOF_SYSTEM_ADDRESS today; point it at the
# selected proof system (Phase 2 switches the composer to EEZ_PROOF_SYSTEM_ADDRESS).
EEZ_MOCK_PROOF_SYSTEM_ADDRESS="$PROOF_SYSTEM_ADDRESS"
echo "      proofSystem= $PROOF_SYSTEM_ADDRESS ($EEZ_PROOF_SYSTEM)"

# ── 3/4 DeployRollup ────────────────────────────────────────────────
echo "[3/4] DeployRollup(proofSystem=$PROOF_SYSTEM_ADDRESS)"
run_forge "DeployRollup" forge script script/DeployRollup.s.sol:DeployRollup \
    --sig "run(address,address,address,address)" \
    "$EEZ_REGISTRY_ADDRESS" "$PROOF_SYSTEM_ADDRESS" "$AUTHORIZED_SIGNER" "$OWNER" \
    $RPC $KEY --broadcast
EEZ_ROLLUP_MANAGER_ADDRESS="$(extract ROLLUP_CONTRACT "$OUT")"
[[ -n "$EEZ_ROLLUP_MANAGER_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture ROLLUP_CONTRACT address" >&2; exit 1; }
echo "      rollupMgr  = $EEZ_ROLLUP_MANAGER_ADDRESS"

# ── 4/5 RegisterRollup ──────────────────────────────────────────────
echo "[4/5] RegisterRollup(initialState=$EEZ_INITIAL_STATE_ROOT)"
run_forge "RegisterRollup" forge script script/RegisterRollup.s.sol:RegisterRollup \
    --sig "run(address,address,bytes32)" \
    "$EEZ_REGISTRY_ADDRESS" "$EEZ_ROLLUP_MANAGER_ADDRESS" "$EEZ_INITIAL_STATE_ROOT" \
    $RPC $KEY --broadcast
EEZ_ROLLUP_ID="$(extract_uint L2_ROLLUP_ID "$OUT")"
[[ -n "$EEZ_ROLLUP_ID" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture L2_ROLLUP_ID" >&2; exit 1; }
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
# the L1 block that confirmed RegisterRollup, so catch-up only
# bridges deploy-time to now.
GENESIS_OUT="${EEZ_GENESIS_OUT:-$REPO/datadir/genesis.json}"
mkdir -p "$REPO/datadir"
DEPLOY_BLOCK_TS_HEX="$(cast block "$EEZ_REGISTRY_DEPLOY_BLOCK" --rpc-url "$EEZ_L1_RPC_URL" --json | jq -r '.timestamp')"
[[ -n "$DEPLOY_BLOCK_TS_HEX" && "$DEPLOY_BLOCK_TS_HEX" != "null" ]] || {
    echo "deploy: failed to capture L1 block timestamp for $EEZ_REGISTRY_DEPLOY_BLOCK" >&2
    exit 1
}
python3 -c "
import json, sys
g = json.load(open('$REPO/genesis.json'))
g['timestamp'] = '$DEPLOY_BLOCK_TS_HEX'
# Mirror reth --chain dev's hardfork activation (all forks at genesis).
# Without these the chain spec defaults to no-forks, the produced
# blocks omit Cancun/Shanghai header fields, and the Deriver's STF
# replay (which sets those fields based on its own NextBlockEnvAttributes)
# disagrees on block-hash.
g['config'].update({
    'homesteadBlock': 0, 'eip150Block': 0, 'eip155Block': 0,
    'eip158Block': 0, 'byzantiumBlock': 0, 'constantinopleBlock': 0,
    'petersburgBlock': 0, 'istanbulBlock': 0, 'muirGlacierBlock': 0,
    'berlinBlock': 0, 'londonBlock': 0, 'arrowGlacierBlock': 0,
    'grayGlacierBlock': 0, 'mergeNetsplitBlock': 0,
    'shanghaiTime': 0, 'cancunTime': 0, 'pragueTime': 0, 'osakaTime': 0,
    'terminalTotalDifficulty': 0, 'terminalTotalDifficultyPassed': True,
})
json.dump(g, open('$GENESIS_OUT', 'w'), indent=2)
" || { echo "deploy: failed to write $GENESIS_OUT" >&2; exit 1; }
echo "      genesis.ts = $DEPLOY_BLOCK_TS_HEX ($(printf %d $DEPLOY_BLOCK_TS_HEX))"

# ── Write deployments.env ───────────────────────────────────────────
cat > "$OUT_FILE" <<EOF
# Generated by scripts/deploy.sh — do not edit by hand. Gitignored.
# Sourced automatically at eez-node startup (alongside .env) via dotenvy.

EEZ_REGISTRY_ADDRESS=$EEZ_REGISTRY_ADDRESS
EEZ_REGISTRY_DEPLOY_BLOCK=$EEZ_REGISTRY_DEPLOY_BLOCK
# The rollup's registered proof system ($EEZ_PROOF_SYSTEM). Phase 2 reads
# EEZ_PROOF_SYSTEM_ADDRESS; EEZ_MOCK_PROOF_SYSTEM_ADDRESS is the same value for
# backward-compat with the current composer wiring.
EEZ_PROOF_SYSTEM_KIND=$EEZ_PROOF_SYSTEM
EEZ_PROOF_SYSTEM_ADDRESS=$PROOF_SYSTEM_ADDRESS
EEZ_MOCK_PROOF_SYSTEM_ADDRESS=$EEZ_MOCK_PROOF_SYSTEM_ADDRESS
EEZ_ROLLUP_MANAGER_ADDRESS=$EEZ_ROLLUP_MANAGER_ADDRESS
EEZ_ROLLUP_ID=$EEZ_ROLLUP_ID
EEZ_INITIAL_STATE_ROOT=$EEZ_INITIAL_STATE_ROOT
EEZ_L2_GENESIS_PATH=$GENESIS_OUT

# L1 cross-chain bridge contracts (DeployBridgeL1).
EEZ_L1_L2_PROXY=$EEZ_L1_L2_PROXY
EEZ_L1_BRIDGE_SENDER=$EEZ_L1_BRIDGE_SENDER
EEZ_L2_BRIDGE_RECEIVER=$EEZ_L2_BRIDGE_RECEIVER

# L2 CCM-L2 (predeploy baked into genesis.json).
EEZ_CCM_L2_ADDRESS=0x4200000000000000000000000000000000000007
# Smoke deviation from Rollup-1.md §3: SystemAddress is a real funded
# EOA (hardhat #0) so the L2 system tx can be signed normally — vanilla
# reth doesn't support type-0x7E (OP-Stack deposit) txs. Replace with
# 0xdead…dead + NodePrimitives extension when type-0x7E lands.
EEZ_L2_SYSTEM_ADDRESS=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
EOF

echo
echo "deploy: wrote $OUT_FILE"

# ── Optional: Blockscout verification ────────────────────────────────
# Best-effort: walk forge's broadcast/ for fresh CREATE entries and
# submit each to Blockscout via `forge verify-contract`. Failures are
# logged but never fail the deploy — Blockscout is an inspector tool,
# not part of the protocol surface. Skipped silently if
# `EEZ_BLOCKSCOUT_URL` isn't set or `jq` isn't installed. Dedup is
# delegated to Blockscout (re-submitting an already-verified address
# is a fast no-op for forge).
verify_on_blockscout() {
    [[ -n "${EEZ_BLOCKSCOUT_URL:-}" ]] || return 0
    if ! command -v jq >/dev/null 2>&1; then
        echo "blockscout: jq not found; skipping verification"
        return 0
    fi

    local broadcast="$CONTRACTS/broadcast"
    [[ -d "$broadcast" ]] || return 0

    local chain
    chain="$(cast chain-id --rpc-url "$EEZ_L1_RPC_URL" 2>/dev/null)" || return 0

    echo
    echo "blockscout: verifying via $EEZ_BLOCKSCOUT_URL (chain $chain)"

    local count=0 fail=0 addr name
    while IFS=$'\t' read -r addr name; do
        [[ -z "$addr" || -z "$name" ]] && continue
        count=$((count + 1))
        if ( cd "$CONTRACTS" && forge verify-contract \
                --watch --guess-constructor-args \
                --rpc-url "$EEZ_L1_RPC_URL" \
                --verifier blockscout \
                --verifier-url "$EEZ_BLOCKSCOUT_URL/api/" \
                "$addr" "$name" ) >/dev/null 2>&1
        then
            echo "  verified $name @ $addr"
        else
            fail=$((fail + 1))
            echo "  failed   $name @ $addr"
        fi
    done < <(
        # Filter to broadcasts under <script>/<chain_id>/run-latest.json
        # that were rewritten by this deploy run (newer than START_MARKER)
        # and whose <chain_id> matches the current L1 RPC. The chain-id
        # filter handles people redeploying against a different L1; the
        # -newer filter handles re-runs against the same chain (so we
        # don't re-verify stale CREATE entries from prior deploys).
        find "$broadcast" -name run-latest.json -newer "$START_MARKER" -print0 2>/dev/null \
        | while IFS= read -r -d '' f; do
            ch="$(basename "$(dirname "$f")")"
            [[ "$ch" == "$chain" ]] || continue
            jq -r '.transactions[]
                   | select(.transactionType=="CREATE" or .transactionType=="CREATE2")
                   | "\(.contractAddress)\t\(.contractName)"' "$f" 2>/dev/null
        done
    )

    if (( count == 0 )); then
        echo "  no fresh CREATE entries found"
    elif (( fail == 0 )); then
        echo "blockscout: $count/$count verified"
    else
        echo "blockscout: $((count - fail))/$count verified ($fail failed)"
    fi
}
verify_on_blockscout || true

echo
echo "deploy: ready. \`make run-node\` will pick these up automatically."
