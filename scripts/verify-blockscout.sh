#!/usr/bin/env bash
#
# Verify freshly-deployed L1 contracts on Blockscout (best-effort).
#
# Walks Foundry's broadcast/ for CREATE/CREATE2 entries on the L1 chain-id
# and submits each to Blockscout via `forge verify-contract`. NEVER fails the
# caller — Blockscout is an inspector tool, not part of the protocol surface,
# so a verification miss must not abort a deploy or a smoke run. Dedup is
# delegated to Blockscout (re-submitting an already-verified address is a fast
# no-op). No-op when EEZ_BLOCKSCOUT_URL is unset, or jq/cast/forge are missing.
#
# Usage: verify-blockscout.sh [since_marker]
#   since_marker  optional file; only run-latest.json newer than it are scanned,
#                 so a re-run doesn't re-verify stale CREATE entries from prior
#                 deploys. Omit to scan every run-latest.json for the chain.
#
# Env:
#   EEZ_BLOCKSCOUT_URL   Blockscout base URL (e.g. https://gnosis-chiado.blockscout.com)
#   EEZ_L1_RPC_URL       L1 RPC — chain-id filter + creation-tx source for --guess-constructor-args
#   EEZ_CONTRACTS_DIR    Foundry project dir (default: <repo>/contracts)

set -uo pipefail

[[ -n "${EEZ_BLOCKSCOUT_URL:-}" ]] || { echo "blockscout: EEZ_BLOCKSCOUT_URL unset; skipping verification"; exit 0; }
for t in jq cast forge; do command -v "$t" >/dev/null 2>&1 || { echo "blockscout: $t not found; skipping verification"; exit 0; }; done
: "${EEZ_L1_RPC_URL:?verify-blockscout: EEZ_L1_RPC_URL not set}"

REPO="$(cd "$(dirname "$0")/.." && pwd)"
CONTRACTS="${EEZ_CONTRACTS_DIR:-$REPO/contracts}"
BROADCAST="$CONTRACTS/broadcast"
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
               | select(.contractName != null and .contractAddress != null)
               | "\(.contractAddress)\t\(.contractName)"' "$f" 2>/dev/null
    done
}

count=0; fail=0
while IFS=$'\t' read -r addr name; do
    [[ -z "$addr" || -z "$name" ]] && continue
    count=$((count + 1))
    if ( cd "$CONTRACTS" && "${TO[@]}" forge verify-contract \
            --watch --guess-constructor-args \
            --rpc-url "$EEZ_L1_RPC_URL" \
            --verifier blockscout --verifier-url "$URL" \
            "$addr" "$name" ) >/dev/null 2>&1
    then
        echo "  verified $name @ $addr"
    else
        fail=$((fail + 1))
        echo "  failed   $name @ $addr"
    fi
done < <(find_broadcasts)

if   (( count == 0 )); then echo "  no fresh CREATE entries found"
elif (( fail  == 0 )); then echo "blockscout: $count/$count verified"
else echo "blockscout: $((count - fail))/$count verified ($fail failed)"; fi
exit 0
