#!/usr/bin/env bash
#
# Verify freshly-deployed L1 contracts on Blockscout (best-effort).
#
# Walks Foundry's broadcast/ for CREATE/CREATE2 entries on the L1 chain-id
# and submits each to Blockscout via `forge verify-contract`. NEVER fails the
# caller — Blockscout is an inspector tool, not part of the protocol surface,
# so a verification miss must not abort a deploy or a smoke run. Constructor
# calldata is recovered from the exact creation input rather than guessed via
# an explorer API. No-op when EEZ_BLOCKSCOUT_URL is unset, or a required tool is
# missing.
#
# Usage: verify-blockscout.sh [since_marker]
#   since_marker  optional file; only run-latest.json newer than it are scanned,
#                 so a re-run doesn't re-verify stale CREATE entries from prior
#                 deploys. Omit to scan every run-latest.json for the chain.
#
# Env:
#   EEZ_BLOCKSCOUT_URL   Blockscout base URL (e.g. https://gnosis-chiado.blockscout.com)
#   EEZ_L1_RPC_URL       L1 RPC used for the chain-id filter and verification
#   EEZ_CONTRACTS_DIR    Foundry project dir (default: <repo>/contracts)
#   EEZ_BROADCAST_DIR    Foundry broadcast dir (default: <contracts>/broadcast)

set -uo pipefail

[[ -n "${EEZ_BLOCKSCOUT_URL:-}" ]] || { echo "blockscout: EEZ_BLOCKSCOUT_URL unset; skipping verification"; exit 0; }
for t in jq cast curl forge; do command -v "$t" >/dev/null 2>&1 || { echo "blockscout: $t not found; skipping verification"; exit 0; }; done
: "${EEZ_L1_RPC_URL:?verify-blockscout: EEZ_L1_RPC_URL not set}"

REPO="$(cd "$(dirname "$0")/.." && pwd)"
CONTRACTS="${EEZ_CONTRACTS_DIR:-$REPO/contracts}"
BROADCAST="${EEZ_BROADCAST_DIR:-$CONTRACTS/broadcast}"
SINCE="${1:-}"

[[ -d "$BROADCAST" ]] || { echo "blockscout: no broadcast dir ($BROADCAST); nothing to verify"; exit 0; }

chain="$(cast chain-id --rpc-url "$EEZ_L1_RPC_URL" 2>/dev/null)" || { echo "blockscout: chain-id lookup failed; skipping"; exit 0; }
URL="${EEZ_BLOCKSCOUT_URL%/}/api/"

echo "blockscout: verifying via $EEZ_BLOCKSCOUT_URL (chain $chain)"

# `timeout` guards against a Blockscout backlog hanging `--watch` forever.
TO=(); command -v timeout >/dev/null 2>&1 && TO=(timeout 180)

# Only scan run-latest.json under <script>/<chain_id>/ matching the L1 chain
# (handles redeploys against a different L1); optionally restrict to files
# rewritten since SINCE (handles re-runs against the same chain).
find_broadcasts() {
    local args=("$BROADCAST" -name run-latest.json)
    [[ -n "$SINCE" && -e "$SINCE" ]] && args+=(-newer "$SINCE")
    find "${args[@]}" -print0 2>/dev/null | while IFS= read -r -d '' f; do
        [[ "$(basename "$(dirname "$f")")" == "$chain" ]] || continue
        jq -r '.transactions[]
               | select(.transactionType=="CREATE" or .transactionType=="CREATE2")
               | select(.contractName != null and .contractAddress != null and .transaction.input != null)
               | "\(.contractAddress)\t\(.contractName)\t\(.transaction.input)"' "$f" 2>/dev/null
    done
}

is_verified() {
    curl -fsS "${EEZ_BLOCKSCOUT_URL%/}/api/v2/addresses/$1" 2>/dev/null \
        | jq -e '.is_verified == true' >/dev/null 2>&1
}

constructor_args() {
    local name="$1" creation_input="${2#0x}" bytecode
    bytecode="$(cd "$CONTRACTS" && forge inspect "$name" bytecode 2>/dev/null)" || return 1
    bytecode="${bytecode#0x}"

    # Creation input is creation bytecode followed by the ABI-encoded
    # constructor arguments. Refuse to guess if the local build is not the
    # artifact that was actually deployed.
    [[ -n "$bytecode" && "$creation_input" == "$bytecode"* ]] || return 1
    printf '%s\n' "${creation_input:${#bytecode}}"
}

count=0; fail=0
while IFS=$'\t' read -r addr name creation_input; do
    [[ -z "$addr" || -z "$name" ]] && continue
    count=$((count + 1))

    if is_verified "$addr"; then
        echo "  verified $name @ $addr"
        continue
    fi

    args="$(constructor_args "$name" "$creation_input")" || {
        fail=$((fail + 1))
        echo "  failed   $name @ $addr (local creation bytecode differs from the deployment)"
        continue
    }

    verify_args=(
        --watch
        --rpc-url "$EEZ_L1_RPC_URL"
        --verifier blockscout
        --verifier-url "$URL"
    )
    [[ -n "$args" ]] && verify_args+=(--constructor-args "$args")

    output="$(cd "$CONTRACTS" && "${TO[@]}" forge verify-contract \
        "${verify_args[@]}" "$addr" "$name" 2>&1)" || true

    if is_verified "$addr"; then
        echo "  verified $name @ $addr"
    else
        fail=$((fail + 1))
        detail="$(grep -E '^(Error:|Details:)' <<< "$output" | tail -1)"
        if [[ -n "$detail" ]]; then
            echo "  failed   $name @ $addr ($detail)"
        else
            echo "  failed   $name @ $addr"
        fi
    fi
done < <(find_broadcasts)

if   (( count == 0 )); then echo "  no fresh CREATE entries found"
elif (( fail  == 0 )); then echo "blockscout: $count/$count verified"
else echo "blockscout: $((count - fail))/$count verified ($fail failed)"; fi
exit 0
