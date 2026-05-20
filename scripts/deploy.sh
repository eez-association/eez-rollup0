#!/usr/bin/env bash
#
# Deploys the upstream EEZ + MockECDSAProofSystem + Rollup manager +
# creates our rollupId — 4 steps.
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

# Runs a forge script command, capturing combined stdout+stderr. On
# non-zero exit (e.g., RPC down, gas insufficient, simulation revert),
# prints the captured output before bailing — without this wrapper,
# `set -e` would abort the `OUT=$(...)` assignment and forge's error
# message would be lost.
run_forge() {
    local label="$1"; shift
    if ! OUT="$(cd "$CONTRACTS" && "$@" 2>&1)"; then
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

# ── 2/4 DeployMockECDSAProofSystem ──────────────────────────────────
echo "[2/4] DeployMockECDSAProofSystem(authorizedSigner=$AUTHORIZED_SIGNER)"
run_forge "DeployMockECDSAProofSystem" forge script script/DeployMockECDSAProofSystem.s.sol:DeployMockECDSAProofSystem \
    --sig "run(address)" "$AUTHORIZED_SIGNER" $RPC $KEY --broadcast
EEZ_MOCK_PROOF_SYSTEM_ADDRESS="$(extract MOCK_PS "$OUT")"
[[ -n "$EEZ_MOCK_PROOF_SYSTEM_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture MOCK_PS address" >&2; exit 1; }
echo "      MOCK_PS    = $EEZ_MOCK_PROOF_SYSTEM_ADDRESS"

# ── 3/4 DeployRollup ────────────────────────────────────────────────
echo "[3/4] DeployRollup"
run_forge "DeployRollup" forge script script/DeployRollup.s.sol:DeployRollup \
    --sig "run(address,address,address,address)" \
    "$EEZ_REGISTRY_ADDRESS" "$EEZ_MOCK_PROOF_SYSTEM_ADDRESS" "$AUTHORIZED_SIGNER" "$OWNER" \
    $RPC $KEY --broadcast
EEZ_ROLLUP_MANAGER_ADDRESS="$(extract ROLLUP_CONTRACT "$OUT")"
[[ -n "$EEZ_ROLLUP_MANAGER_ADDRESS" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture ROLLUP_CONTRACT address" >&2; exit 1; }
echo "      rollupMgr  = $EEZ_ROLLUP_MANAGER_ADDRESS"

# ── 4/4 RegisterRollup ──────────────────────────────────────────────
echo "[4/4] RegisterRollup(initialState=$EEZ_INITIAL_STATE_ROOT)"
run_forge "RegisterRollup" forge script script/RegisterRollup.s.sol:RegisterRollup \
    --sig "run(address,address,bytes32)" \
    "$EEZ_REGISTRY_ADDRESS" "$EEZ_ROLLUP_MANAGER_ADDRESS" "$EEZ_INITIAL_STATE_ROOT" \
    $RPC $KEY --broadcast
EEZ_ROLLUP_ID="$(extract_uint L2_ROLLUP_ID "$OUT")"
[[ -n "$EEZ_ROLLUP_ID" ]] || { echo "$OUT" >&2; echo "deploy: failed to capture L2_ROLLUP_ID" >&2; exit 1; }
echo "      rollupId   = $EEZ_ROLLUP_ID"

# ── Write deployments.env ───────────────────────────────────────────
cat > "$OUT_FILE" <<EOF
# Generated by scripts/deploy.sh — do not edit by hand. Gitignored.
# Sourced automatically at eez-node startup (alongside .env) via dotenvy.

EEZ_REGISTRY_ADDRESS=$EEZ_REGISTRY_ADDRESS
EEZ_REGISTRY_DEPLOY_BLOCK=$EEZ_REGISTRY_DEPLOY_BLOCK
EEZ_MOCK_PROOF_SYSTEM_ADDRESS=$EEZ_MOCK_PROOF_SYSTEM_ADDRESS
EEZ_ROLLUP_MANAGER_ADDRESS=$EEZ_ROLLUP_MANAGER_ADDRESS
EEZ_ROLLUP_ID=$EEZ_ROLLUP_ID
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
