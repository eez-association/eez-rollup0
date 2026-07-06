#!/usr/bin/env bash
# Scheduled L1 reorgs: poll block height and, at configured intervals, drive
# Disruptoor to partition the CL P2P network (minority vs majority); healing lets
# fork-choice reorg the losing side out. Reports the observed depth after each
# heal. Requires disruptoor enabled + reachable. All knobs are env-overridable:
#   EEZ_REORG_SCHEDULES ("name:depth:every", deepest match wins),
#   _MAJORITY / _MINORITY (comma-separated Kurtosis service names — the two CL
#   groups to split; see defaults below), _SCOPE (cl_p2p), _HEAL_MARGIN_S (6),
#   _POLL_SECONDS (4), EEZ_L1_SLOT_SECONDS (12), EEZ_REORG_DRY_RUN=1 (log only).
#
# Disruptoor wire format (v1 /v1/state): groups are LABEL-MATCH selectors
# (com.kurtosistech.id = service name), and the layer field is "scope"
# (cl_p2p/el_p2p), NOT the older "components". Older index/participant-based
# bodies are rejected with `unknown field "components"`.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

# Discover the L1 RPC + disruptoor URL from the running enclave (env already-set
# wins). See enclave-env.sh — uses `kurtosis port print`, no scraping.
# shellcheck disable=SC1091
source "$HERE/enclave-env.sh"

L1_RPC="${EEZ_L1_RPC_URL:?could not resolve EEZ_L1_RPC_URL — is the '$ENCLAVE' enclave up? (kurtosis port print $ENCLAVE el-1-reth-lighthouse rpc)}"
DISRUPTOOR="${EEZ_DISRUPTOOR_URL:-http://127.0.0.1:36000}"
SCHEDULES="${EEZ_REORG_SCHEDULES:-shallow:1:20 medium:5:100 deep:15:1000}"
# The two CL groups to split, as comma-separated Kurtosis service names
# (matched on the com.kurtosistech.id label). Defaults: MAJORITY = validators
# 1,2,3 + the builder CL (the heavier, winning side); MINORITY = validator 4 +
# the eez-node follower. Putting the follower in the LOSING minority is
# deliberate — on heal, fork-choice discards the minority branch, so eez-node's
# embedded reth actually experiences the L1 reorg (the thing we're testing).
# Every CL must be in one group, or an ungrouped node bridges the split and no
# fork forms.
MAJORITY="${EEZ_REORG_MAJORITY:-cl-1-lighthouse-reth,cl-2-lighthouse-reth,cl-3-lighthouse-reth,cl-5-lighthouse-reth-builder}"
MINORITY="${EEZ_REORG_MINORITY:-cl-4-lighthouse-reth,eez-follower}"
# Which P2P layer(s) disruptoor cuts. cl_p2p induces CL forks (el_p2p also valid).
SCOPE="${EEZ_REORG_SCOPE:-cl_p2p}"
SLOT_SECONDS="${EEZ_L1_SLOT_SECONDS:-12}"
HEAL_MARGIN_S="${EEZ_REORG_HEAL_MARGIN_S:-6}"
POLL_SECONDS="${EEZ_REORG_POLL_SECONDS:-4}"
DRY_RUN="${EEZ_REORG_DRY_RUN:-0}"

for t in cast curl; do
    command -v "$t" >/dev/null || { echo "reorg-scheduler: $t not found in PATH" >&2; exit 1; }
done

log() { echo "$(date -u +%H:%M:%S) reorg-scheduler: $*"; }

# CSV "a,b" -> JSON array of strings '["a","b"]'  (also handles a single item)
csv_to_json_strs() {
    local s="$1"
    echo "[\"${s//,/\",\"}\"]"
}

# Heal = clear all partitions by PUTting an empty state (confirmed on this
# disruptoor version; the older POST /v1/state/clear may not exist).
heal() {
    [[ "$DRY_RUN" == 1 ]] && { log "[dry-run] would clear partition"; return 0; }
    curl -fsS -X PUT "$DISRUPTOOR/v1/state" -H 'Content-Type: application/json' \
        -d '{"partitions":[]}' -o /dev/null \
        || log "WARN heal failed (is disruptoor up at $DISRUPTOOR?)"
}
# Heal on any exit so we never leave the network partitioned.
trap 'heal' EXIT INT TERM

# The PUT /v1/state schema (participant index vs container id) can vary by
# disruptoor version; a non-2xx is logged with the response body to debug.
partition() {
    local name="$1"
    local body resp http_code
    body="$(cat <<JSON
{"partitions":[{"name":"$name","scope":$(csv_to_json_strs "$SCOPE"),"groups":[{"com.kurtosistech.id":$(csv_to_json_strs "$MAJORITY")},{"com.kurtosistech.id":$(csv_to_json_strs "$MINORITY")}]}]}
JSON
)"
    if [[ "$DRY_RUN" == 1 ]]; then
        log "[dry-run] would PUT $DISRUPTOOR/v1/state: $body"
        return 0
    fi
    resp="$(curl -sS -w $'\n%{http_code}' -X PUT "$DISRUPTOOR/v1/state" \
        -H 'Content-Type: application/json' -d "$body")" \
        || { log "ERROR partition PUT failed (network error — is $DISRUPTOOR reachable?)"; return 1; }
    http_code="${resp##*$'\n'}"
    if [[ "$http_code" != 2* ]]; then
        log "ERROR partition PUT rejected (HTTP $http_code): ${resp%$'\n'*}"
        log "  request shape may not match this disruptoor version's schema — see the NOTE above partition()"
        return 1
    fi
    log "partition accepted (HTTP $http_code)"
}

block_number() { cast block-number --rpc-url "$L1_RPC"; }
block_hash()   { cast block "$1" --rpc-url "$L1_RPC" --field hash 2>/dev/null; }

# After a heal, count how many blocks at/below `anchor` changed hash vs the
# snapshot we took before partitioning. Returns observed reorg depth.
observed_depth() {
    local anchor="$1" snap_json="$2" depth=0 h
    for ((h = anchor; h > anchor - 64 && h >= 0; h--)); do
        local before after
        before="$(echo "$snap_json" | sed -n "s/^$h=//p")"
        [[ -n "$before" ]] || break
        after="$(block_hash "$h" || true)"
        if [[ -n "$after" && "$after" != "$before" ]]; then
            depth=$((anchor - h + 1))
        elif [[ -n "$after" && "$after" == "$before" ]]; then
            break   # hashes match here → nothing reorged at/below this height
        fi
    done
    echo "$depth"
}

trigger_reorg() {
    local name="$1" depth="$2" anchor="$3"
    log "trigger '$name' depth=$depth at L1 height=$anchor (partition [${MAJORITY}] | [${MINORITY}] on ${SCOPE})"

    # Snapshot hashes for the window we expect to be rewritten.
    local snap="" h hh
    for ((h = anchor; h > anchor - depth - 2 && h >= 0; h--)); do
        hh="$(block_hash "$h" || true)"
        [[ -n "$hh" ]] && snap+="$h=$hh"$'\n'
    done

    partition "$name" || return 0
    local hold=$((depth * SLOT_SECONDS + HEAL_MARGIN_S))
    log "partition held for ${hold}s (~${depth} slots)"
    sleep "$hold"
    heal
    # Give fork-choice a couple slots to converge, then measure.
    sleep $((SLOT_SECONDS * 2))
    local got
    got="$(observed_depth "$anchor" "$snap")"
    if [[ "$got" -gt 0 ]]; then
        log "OK reorg observed: depth=$got (target $depth) — watch eez-node for l1.reorg markers"
    else
        log "NOTE no reorg observed at height $anchor (target $depth). PoS depth is weight-dependent; try longer hold or larger minority."
    fi
}

# Pick the deepest schedule whose interval divides this height (so at height
# 1000, deep(1000) wins over medium(100) and shallow(20)).
best_match() {
    local height="$1" best_depth=-1 best=""
    for entry in $SCHEDULES; do
        IFS=: read -r n d e <<<"$entry"
        [[ -n "$n" && -n "$d" && -n "$e" && "$e" -gt 0 ]] || continue
        if (( height % e == 0 )) && (( d > best_depth )); then
            best_depth="$d"; best="$n:$d"
        fi
    done
    echo "$best"
}

log "watching $L1_RPC | disruptoor=$DISRUPTOOR | schedules='$SCHEDULES' | dry_run=$DRY_RUN"
if [[ "$DRY_RUN" != 1 ]] && ! curl -fsS "$DISRUPTOOR/v1/healthz" -o /dev/null; then
    log "WARN disruptoor healthz check failed at $DISRUPTOOR — partitions will likely fail later."
    log "  verify EEZ_DISRUPTOOR_URL (kurtosis port print ${ENCLAVE:-eez-devnet} disruptoor http)."
fi
last_handled=-1
while true; do
    height="$(block_number 2>/dev/null || true)"
    if [[ -n "$height" && "$height" != "$last_handled" ]]; then
        match="$(best_match "$height")"
        if [[ -n "$match" ]]; then
            IFS=: read -r mname mdepth <<<"$match"
            trigger_reorg "$mname" "$mdepth" "$height"
        fi
        last_handled="$height"
    fi
    sleep "$POLL_SECONDS"
done
