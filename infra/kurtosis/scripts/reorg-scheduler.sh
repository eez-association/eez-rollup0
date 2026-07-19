#!/usr/bin/env bash
# Scheduled L1 reorgs for the EEZ devnet. Polls L1 height and, at configured
# intervals, drives Disruptoor to split the CL P2P network into two groups; on
# heal, fork-choice discards the lighter branch. The follower (and thus
# eez-node) sits on the losing side, so eez-node experiences a real L1 reorg —
# which is the behaviour under test. Confirmation is read from eez-node's own
# log, not from L1 hashes (el-1 stays canonical and never reorgs).
#
# Env knobs (all optional):
#   EEZ_REORG_SCHEDULES   "name:depth:every" list; deepest match at a height wins
#   EEZ_REORG_MAJORITY    comma-separated service names — heavier, winning group
#   EEZ_REORG_MINORITY    comma-separated service names — lighter, losing group
#                         (must include eez-follower so eez-node sees the reorg)
#   EEZ_REORG_SCOPE       disruptoor layer(s) to cut: cl_p2p (default) / el_p2p
#   EEZ_L1_MINORITY_RPC_URL  embedded L1 on the losing side (auto-discovered)
#   EEZ_REORG_TIMEOUT_S   maximum wait for the requested minority depth (900)
#   EEZ_L1_SLOT_SECONDS   slot time (default 12);  EEZ_REORG_POLL_SECONDS (4)
#   EEZ_REORG_DRY_RUN=1   log intended partitions only, touch nothing
#
# Disruptoor wire format (v1 /v1/state): groups are label-match selectors
# (com.kurtosistech.id = service name) and the layer field is "scope"
# (cl_p2p/el_p2p). The older "components"/participant-index body is rejected.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

# L1 RPC + disruptoor URL discovered from the running enclave (already-set env
# wins). See enclave-env.sh — uses `kurtosis port print`, no scraping.
# shellcheck disable=SC1091
source "$HERE/enclave-env.sh"

L1_RPC="${EEZ_L1_RPC_URL:?could not resolve EEZ_L1_RPC_URL — is the '$ENCLAVE' enclave up? (kurtosis port print $ENCLAVE el-1-reth-lighthouse rpc)}"
MINORITY_RPC="${EEZ_L1_MINORITY_RPC_URL:-}"
DISRUPTOOR="${EEZ_DISRUPTOOR_URL:-http://127.0.0.1:36000}"
SCHEDULES="${EEZ_REORG_SCHEDULES:-shallow:1:20 medium:5:100 deep:15:1000}"
# Defaults: MAJORITY = validators 1,2,3 + the builder CL (heavier, wins);
# MINORITY = validator 4 + the eez-node follower (lighter, loses). Every CL must
# be in one group or an ungrouped node bridges the split and no fork forms.
MAJORITY="${EEZ_REORG_MAJORITY:-cl-1-lighthouse-reth,cl-2-lighthouse-reth,cl-3-lighthouse-reth,cl-5-lighthouse-reth-builder}"
MINORITY="${EEZ_REORG_MINORITY:-cl-4-lighthouse-reth,eez-follower}"
SCOPE="${EEZ_REORG_SCOPE:-cl_p2p}"
SLOT_SECONDS="${EEZ_L1_SLOT_SECONDS:-12}"
DEPTH_TIMEOUT_S="${EEZ_REORG_TIMEOUT_S:-900}"
POLL_SECONDS="${EEZ_REORG_POLL_SECONDS:-4}"
DRY_RUN="${EEZ_REORG_DRY_RUN:-0}"
EEZ_NODE_SERVICE="${EEZ_NODE_SERVICE:-eez-node}"

for t in cast curl kurtosis; do
    command -v "$t" >/dev/null || { echo "reorg-scheduler: $t not found in PATH" >&2; exit 1; }
done

log() { echo "$(date -u +%H:%M:%S) reorg-scheduler: $*"; }

# CSV "a,b" -> JSON array of strings '["a","b"]' (also handles a single item).
csv_to_json_strs() { local s="$1"; echo "[\"${s//,/\",\"}\"]"; }

# Heal = clear all partitions with an empty state (this disruptoor version has no
# POST /v1/state/clear). Runs on any exit so we never leave the net partitioned.
guard_pid=""
heal() {
    if [[ -n "$guard_pid" ]]; then
        kill "$guard_pid" 2>/dev/null || true
        wait "$guard_pid" 2>/dev/null || true
        guard_pid=""
    fi
    [[ "$DRY_RUN" == 1 ]] && { log "[dry-run] would clear partition"; return 0; }
    curl -fsS -X PUT "$DISRUPTOOR/v1/state" -H 'Content-Type: application/json' \
        -d '{"partitions":[]}' -o /dev/null \
        || log "WARN heal failed (is disruptoor up at $DISRUPTOOR?)"
}
trap 'heal' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Keep a tiny independent guard alive while partitioned. If this shell is
# SIGKILLed, the guard observes its disappearance and heals the network.
start_heal_guard() {
    [[ "$DRY_RUN" == 1 ]] && return 0
    local scheduler_pid="$BASHPID"
    (
        trap '' HUP
        while kill -0 "$scheduler_pid" 2>/dev/null; do sleep 1; done
        curl -fsS -X PUT "$DISRUPTOOR/v1/state" -H 'Content-Type: application/json' \
            -d '{"partitions":[]}' -o /dev/null || true
    ) &
    guard_pid=$!
}

partition() {
    local name="$1" body resp http_code
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
        || { log "ERROR partition PUT failed (network — is $DISRUPTOOR reachable?)"; return 1; }
    http_code="${resp##*$'\n'}"
    if [[ "$http_code" != 2* ]]; then
        log "ERROR partition PUT rejected (HTTP $http_code): ${resp%$'\n'*}"
        return 1
    fi
    log "partition accepted (HTTP $http_code)"
}

block_number() { cast block-number --rpc-url "$L1_RPC"; }
minority_block_number() { cast block-number --rpc-url "$MINORITY_RPC"; }
block_hash() { cast block "$2" --rpc-url "$1" --field hash 2>/dev/null; }

# Print the losing branch's current distance from the last block it shares with
# the canonical chain. Merely counting elapsed blocks is wrong when the follower
# receives one last canonical block after the partition request.
minority_fork_depth() {
    local max_scan="${1:-64}" minority_tip canonical_tip cursor lower minority_hash canonical_hash
    minority_tip="$(minority_block_number 2>/dev/null || true)"
    canonical_tip="$(block_number 2>/dev/null || true)"
    [[ "$minority_tip" =~ ^[0-9]+$ && "$canonical_tip" =~ ^[0-9]+$ ]] || return 1
    cursor=$minority_tip
    (( cursor > canonical_tip )) && cursor=$canonical_tip
    lower=$(( cursor - max_scan ))
    (( lower < 0 )) && lower=0
    while (( cursor >= lower )); do
        minority_hash="$(block_hash "$MINORITY_RPC" "$cursor")"
        canonical_hash="$(block_hash "$L1_RPC" "$cursor")"
        [[ -n "$minority_hash" && -n "$canonical_hash" ]] || return 1
        if [[ "${minority_hash,,}" == "${canonical_hash,,}" ]]; then
            echo $(( minority_tip - cursor ))
            return 0
        fi
        ((cursor--)) || true
    done
    return 1
}

# eez-node logs 'reorg — rewinding ring to common ancestor …' when its embedded
# L1 reorgs. Count occurrences (before vs after a partition) to confirm a reorg,
# and parse the depth from the newest line. This is the source of truth — el-1
# is always canonical, so L1-hash comparison there would never show a reorg.
REORG_MARKER="rewinding ring to common ancestor"
reorg_count() { kurtosis service logs "$ENCLAVE" "$EEZ_NODE_SERVICE" 2>/dev/null | grep -c "$REORG_MARKER" || true; }
latest_reorg() { kurtosis service logs "$ENCLAVE" "$EEZ_NODE_SERVICE" 2>/dev/null | grep "$REORG_MARKER" | tail -1; }

trigger_reorg() {
    local name="$1" depth="$2" anchor="$3" before after line old_tip common
    local minority_anchor observed_depth deadline achieved=0
    before="$(reorg_count)"
    log "trigger '$name' (target depth $depth) at L1 height=$anchor — partition [${MAJORITY}] | [${MINORITY}] on ${SCOPE}"

    if [[ "$DRY_RUN" != 1 ]]; then
        minority_anchor="$(minority_block_number 2>/dev/null || true)"
        if [[ ! "$minority_anchor" =~ ^[0-9]+$ ]]; then
            log "ERROR cannot read minority L1 at $MINORITY_RPC; refusing to create an unmeasurable partition"
            return 0
        fi
    else
        minority_anchor="$anchor"
    fi
    partition "$name" || return 0
    start_heal_guard
    if [[ "$DRY_RUN" == 1 ]]; then
        log "[dry-run] would hold until minority advances exactly $depth block(s) from $minority_anchor"
        heal
        return 0
    fi

    deadline=$(( SECONDS + DEPTH_TIMEOUT_S ))
    log "holding until the observed minority fork is $depth block(s) deep (timeout ${DEPTH_TIMEOUT_S}s)"
    while (( SECONDS < deadline )); do
        observed_depth="$(minority_fork_depth "$((depth + 32))" 2>/dev/null || true)"
        if [[ "$observed_depth" =~ ^[0-9]+$ ]] && (( observed_depth >= depth )); then
            achieved="$observed_depth"
            break
        fi
        sleep "$POLL_SECONDS"
    done
    heal
    if (( achieved < depth )); then
        log "WARN minority reached only depth $achieved of $depth before timeout; network healed"
        return 0
    fi
    # Let fork-choice + eez-node reconcile, then check eez-node's own log.
    sleep $(( SLOT_SECONDS * 3 ))
    after="$(reorg_count)"
    if (( after > before )); then
        line="$(latest_reorg)"
        old_tip="$(sed -n 's/.*old_tip_number=\([0-9]*\).*/\1/p' <<<"$line")"
        common="$(sed -n 's/.*common_ancestor_number=\([0-9]*\).*/\1/p' <<<"$line")"
        log "OK eez-node reorged: depth=$(( ${old_tip:-0} - ${common:-0} )) (old_tip=${old_tip:-?} common_ancestor=${common:-?})"
    else
        log "NOTE no eez-node reorg this cycle — check the configured partition groups and minority RPC."
    fi
}

# Deepest schedule whose interval divides this height wins (at 1000, deep(1000)
# beats medium(100) and shallow(20)).
best_match() {
    local height="$1" best_depth=-1 best="" n d e
    (( height > 0 )) || { echo ""; return 0; }
    for entry in $SCHEDULES; do
        IFS=: read -r n d e <<<"$entry"
        [[ -n "$n" && -n "$d" && -n "$e" && "$e" -gt 0 ]] || continue
        if (( height % e == 0 )) && (( d > best_depth )); then
            best_depth="$d"; best="$n:$d"
        fi
    done
    echo "$best"
}

log "watching $L1_RPC | minority=$MINORITY_RPC | disruptoor=$DISRUPTOOR | schedules='$SCHEDULES' | dry_run=$DRY_RUN"
if [[ "$DRY_RUN" != 1 && -z "$MINORITY_RPC" ]]; then
    log "ERROR minority RPC unavailable (expected: kurtosis port print $ENCLAVE eez-node l1-rpc)"
    exit 1
fi

# Fail early when topology overrides are stale instead of running ineffective
# partitions and suggesting that a longer hold is the cure.
for service in ${MAJORITY//,/ } ${MINORITY//,/ }; do
    if ! kurtosis service inspect "$ENCLAVE" "$service" >/dev/null 2>&1; then
        log "ERROR partition service '$service' does not exist in enclave '$ENCLAVE'; set EEZ_REORG_MAJORITY/EEZ_REORG_MINORITY for this topology"
        exit 1
    fi
done
if [[ "$DRY_RUN" != 1 ]] && ! curl -fsS "$DISRUPTOOR/v1/healthz" -o /dev/null; then
    log "WARN disruptoor healthz failed at $DISRUPTOOR — partitions will likely fail."
    log "  check EEZ_DISRUPTOOR_URL (kurtosis port print ${ENCLAVE:-eez-devnet} disruptoor http)."
fi
last_scanned="$(block_number 2>/dev/null || true)"
[[ "$last_scanned" =~ ^[0-9]+$ ]] || last_scanned=0
while true; do
    height="$(block_number 2>/dev/null || true)"
    if [[ "$height" =~ ^[0-9]+$ ]] && (( height > last_scanned )); then
        # Scan every crossed boundary. A long/deep reorg may block this loop,
        # but ticks reached during it are queued here instead of disappearing.
        for ((candidate=last_scanned + 1; candidate<=height; candidate++)); do
            match="$(best_match "$candidate")"
            [[ -n "$match" ]] || continue
            IFS=: read -r mname mdepth <<<"$match"
            trigger_reorg "$mname" "$mdepth" "$candidate"
        done
        last_scanned="$height"
    fi
    sleep "$POLL_SECONDS"
done
