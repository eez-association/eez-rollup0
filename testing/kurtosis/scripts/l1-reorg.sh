#!/usr/bin/env bash
# Run an L1-only devnet (l1-reorg-args.yaml) and inject reorgs of random depth
# at random block intervals.
#
#   bash testing/kurtosis/scripts/l1-reorg.sh up     # launch the enclave
#   bash testing/kurtosis/scripts/l1-reorg.sh run    # inject reorgs
#   bash testing/kurtosis/scripts/l1-reorg.sh heal   # undo an interrupted run
#
# Requires kurtosis, docker (with NET_ADMIN: Linux or Docker Desktop), jq, and curl.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-l1-reorg}"
ARGS="${KURTOSIS_ARGS_FILE:-$REPO/testing/kurtosis/l1-reorg-args.yaml}"
# Same pin and reason as scripts/12s-l1-up.sh (rbuilder cannot parse ENR bootnodes).
PACKAGE="${ETHEREUM_PACKAGE:-github.com/ethpandaops/ethereum-package@199620b24ac979c676010c5a68b2893c2bce4f1f}"
NET="kt-$ENCLAVE"

REORG_DEPTH_MIN="${REORG_DEPTH_MIN:-1}"       # R_DEPTH is random in [MIN, MAX], inclusive
REORG_DEPTH_MAX="${REORG_DEPTH_MAX:-3}"
REORG_INTERVAL_MIN="${REORG_INTERVAL_MIN:-5}" # a reorg every R observed blocks, MIN < R < MAX
REORG_INTERVAL_MAX="${REORG_INTERVAL_MAX:-15}"
REORG_COUNT="${REORG_COUNT:-0}"               # stop after N reorgs; 0 runs forever
REORG_PARTITION_TIMEOUT_SECS="${REORG_PARTITION_TIMEOUT_SECS:-1800}" # to build both forks
REORG_HEAL_TIMEOUT_SECS="${REORG_HEAL_TIMEOUT_SECS:-300}"            # for the reorg to land
REORG_STALL_TIMEOUT_SECS="${REORG_STALL_TIMEOUT_SECS:-300}"          # for the observed head to advance

SIDECAR_IMAGE=nicolaka/netshoot:v0.13 # provides iptables
CHAIN=EEZREORG                        # iptables chain holding the partition rules
# Participant 1 is the observed side (1/3 of the validators, plus the MEV
# builder and relay); point the system under test at el-1. Participants 2-3 are
# the hidden side (2/3). The hidden side keeps its own mev-boost so its
# proposers get a fast "no bid" instead of a builder timeout that makes their
# blocks late.
HIDDEN_RE='^(el|cl|vc|mev-boost)-(2|3)-'
HIDDEN_NET_RE='^(el|cl)-(2|3)-' # the containers that get firewalled

log() { printf '%s [l1-reorg] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { printf '%s [l1-reorg] ✗ %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; exit 1; }
require() { for t in "$@"; do command -v "$t" >/dev/null || die "$t not found in PATH"; done; }

# ── Enclave ──────────────────────────────────────────────────────────────────

# "<service> <container>" per container on the enclave network, stopped ones
# included so a paused vc-1 stays discoverable. Containers are "<service>--<uuid>".
services() {
    docker ps -a --filter "network=$NET" --format '{{.Names}}' | while read -r name; do
        if [[ "$name" == *--* ]]; then printf '%s %s\n' "${name%%--*}" "$name"; fi
    done
}
# awk reads all input: exiting early would SIGPIPE `services` under pipefail.
service_matching() { services | awk -v re="$1" '$1 ~ re && !f++ { print $1 }'; }
container_of() { services | awk -v s="$1" '$1 == s && !f++ { print $2 }'; }
running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == true ]]; }
ip_of() { docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$1"; }

port_url() { # <service> <port-id> → http://127.0.0.1:PORT
    local url
    url=$(kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null) || die "cannot resolve $2 port of $1"
    [[ "$url" == http* ]] || url="http://$url"
    echo "${url%/}"
}

# ── Chain queries ────────────────────────────────────────────────────────────

rpc() { # <url> <method> <params-json> → .result
    curl -fsS --max-time 5 -H 'content-type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}" "$1" | jq -er '.result'
}
head_number() { local h; h=$(rpc "$1" eth_blockNumber '[]') && echo $((16#${h#0x})); }
hash_at() { rpc "$1" eth_getBlockByNumber "[\"$(printf '0x%x' "$2")\",false]" | jq -r '.hash // empty'; }
block_hash() { local h; h=$(hash_at "$@") && [[ -n "$h" ]] && echo "$h"; } # fails on empty

retry() { # <cmd...>: retry a read for 30s, plus any in-flight request
    local end=$((SECONDS + 30))
    while (( SECONDS < end )); do
        "$@" && return 0
        sleep 1
    done
    return 1
}

common_ancestor() { # <el-a> <el-b> → highest block number both agree on
    local a="$1" b="$2" n ha hb
    ha=$(head_number "$a") && hb=$(head_number "$b") || return 1
    n=$(( ha < hb ? ha : hb ))
    while (( n > 0 )); do
        # A failed lookup must not read as "both empty, so equal".
        ha=$(hash_at "$a" "$n") && hb=$(hash_at "$b" "$n") && [[ -n "$ha" && -n "$hb" ]] || return 1
        [[ "$ha" == "$hb" ]] && break
        n=$((n - 1))
    done
    echo "$n"
}

# Fork-choice weight (Gwei) of the block with execution payload <el-hash>, which
# for the first block after the fork point is the weight of its whole branch.
branch_weight() { # <beacon-url> <el-hash>
    curl -fsS --max-time 5 "$1/eth/v1/debug/fork_choice" | jq -er --arg h "$2" \
        '[.fork_choice_nodes[] | select(.execution_block_hash == $h) | .weight][0] // error("missing")' 2>/dev/null
}

# ── Partition ────────────────────────────────────────────────────────────────

sidecar() { # <container> <sh-script> [args...]
    local target="$1"
    shift
    docker run --rm --network "container:$target" --cap-add NET_ADMIN --entrypoint sh "$SIDECAR_IMAGE" -c "$@"
}

partition() { # firewall the hidden EL/CL from every observed-side container
    local svc name blocked=() hidden=()
    while read -r svc name; do
        [[ "$svc" == kurtosis-* ]] || ! running "$name" && continue # stopped: no address
        if [[ "$svc" =~ $HIDDEN_RE ]]; then
            [[ "$svc" =~ $HIDDEN_NET_RE ]] && hidden+=("$name")
        else
            blocked+=("$(ip_of "$name")")
        fi
    done < <(services)
    (( ${#hidden[@]} > 0 && ${#blocked[@]} > 0 )) || die "enclave $ENCLAVE has no hidden or observed containers"
    for name in "${hidden[@]}"; do
        # shellcheck disable=SC2016 # expanded by the sidecar's sh
        sidecar "$name" '
            set -e
            c="$1"; shift
            iptables -N "$c"
            for ip in "$@"; do iptables -A "$c" -s "$ip" -j DROP; iptables -A "$c" -d "$ip" -j DROP; done
            iptables -I INPUT -j "$c"
            iptables -I OUTPUT -j "$c"
        ' sh "$CHAIN" "${blocked[@]}" || die "failed to partition $name"
    done
    log "partitioned ${#hidden[@]} hidden containers from ${#blocked[@]} others"
}

clear_partition() {
    local svc name listed state rc=0
    listed=$(services) || return 1
    while read -r svc name; do
        [[ "$svc" =~ $HIDDEN_NET_RE ]] || continue
        if ! state=$(docker inspect -f '{{.State.Running}}' "$name"); then
            rc=1
            continue
        fi
        [[ "$state" == true ]] || continue
        # Missing rules are OK; check the remaining rules after deletion.
        # shellcheck disable=SC2016 # expanded by the sidecar's sh
        sidecar "$name" '
            c="$1"
            while iptables -D INPUT -j "$c" 2>/dev/null; do :; done
            while iptables -D OUTPUT -j "$c" 2>/dev/null; do :; done
            iptables -F "$c" 2>/dev/null; iptables -X "$c" 2>/dev/null
            rules=$(iptables -S) || exit 1
            printf "%s\n" "$rules" | awk -v chain="$c" '\''
                { for (i = 1; i <= NF; i++) if ($i == chain) found = 1 }
                END { exit found }
            '\''
        ' sh "$CHAIN" || { log "warning: could not clear rules in $name"; rc=1; }
    done <<< "$listed"
    return "$rc"
}

start_vc() {
    local svc name
    svc=$(service_matching '^vc-1-') || return 1
    [[ -n "$svc" ]] || { log "warning: observed validator client not found"; return 1; }
    name=$(container_of "$svc") && [[ -n "$name" ]] || return 1
    running "$name" && return 0
    kurtosis service start "$ENCLAVE" "$svc" >/dev/null || { log "warning: could not start $svc"; return 1; }
    running "$name" || { log "warning: $svc is not running after start"; return 1; }
}

# Idempotent cleanup, also used by the EXIT trap.
restore() { # clear the partition rules and make sure vc-1 is running
    local rc=0
    clear_partition || rc=1
    start_vc || rc=1
    return "$rc"
}

# ── One reorg ────────────────────────────────────────────────────────────────

reorg() { # <target-depth> <observed-el> <hidden-el> <observed-cl> <hidden-cl>
    local depth="$1" obs="$2" hid="$3" obs_cl="$4" hid_cl="$5"
    local vc deadline fork ho hh lo lh ow hw obs_hash hid_hash held=0
    local ancestor orphan=() i h orphaned new_head
    vc=$(service_matching '^vc-1-')
    [[ -n "$vc" ]] || die "observed validator client (vc-1-*) not found"

    log "reorg: target depth $depth"
    partition # each side now builds its own fork
    deadline=$((SECONDS + REORG_PARTITION_TIMEOUT_SECS))
    while :; do
        (( SECONDS < deadline )) || die "forks not built within ${REORG_PARTITION_TIMEOUT_SECS}s"
        if fork=$(common_ancestor "$obs" "$hid") && ho=$(head_number "$obs") && hh=$(head_number "$hid"); then
            lo=$((ho - fork))
            lh=$((hh - fork))
            if (( lo >= depth && !held )); then
                log "observed fork at $lo blocks; pausing $vc until the hidden branch outweighs it"
                kurtosis service stop "$ENCLAVE" "$vc" >/dev/null || die "could not stop $vc"
                held=1 # Re-read the heads next iteration to count in-flight proposals.
            elif (( lo >= depth )); then
                # Heal only once the hidden branch outweighs the observed one: block
                # counts are not enough, since a hidden block without votes loses to
                # observed blocks with votes. Each beacon node only knows its own
                # branch's votes; the 1.5x margin sits below the 2x stake ratio.
                if (( lh >= 1 )) \
                    && obs_hash=$(block_hash "$obs" $((fork + 1))) \
                    && hid_hash=$(block_hash "$hid" $((fork + 1))) \
                    && [[ "$obs_hash" != "$hid_hash" ]] \
                    && ow=$(branch_weight "$obs_cl" "$obs_hash") \
                    && hw=$(branch_weight "$hid_cl" "$hid_hash") \
                    && (( 2 * hw > 3 * ow )); then
                    # Snapshot the observed fork so its orphaning can be proven after the heal.
                    ancestor=$(retry block_hash "$obs" "$fork") || die "cannot read observed block $fork"
                    orphan=()
                    for ((i = fork + 1; i <= ho; i++)); do
                        h=$(retry block_hash "$obs" "$i") || die "cannot read observed block $i"
                        orphan+=("$h")
                    done
                    if [[ "${orphan[0]}" == "$obs_hash" ]] \
                        && h=$(head_number "$obs") && (( h == ho )) \
                        && h=$(block_hash "$obs" "$ho") && [[ "$h" == "${orphan[lo - 1]}" ]]; then
                        break
                    fi
                fi
            elif (( held )); then # the fork point moved up, so the observed side must grow again
                log "observed fork dropped to $lo blocks; resuming $vc"
                start_vc || die "could not start $vc"
                held=0
            fi
        fi
        sleep 1
    done
    (( lo == depth )) || log "warning: observed fork overshot to $lo blocks (target $depth)"

    log "fork at $fork: observed +$lo blocks (weight $ow), hidden +$lh blocks (weight $hw); healing"
    clear_partition || die "could not clear the partition" # fork choice now moves the observed side onto the hidden fork

    deadline=$((SECONDS + REORG_HEAL_TIMEOUT_SECS))
    while :; do
        (( SECONDS < deadline )) || die "observed side did not reorg within ${REORG_HEAL_TIMEOUT_SECS}s"
        orphaned=0
        for i in "${!orphan[@]}"; do
            # Empty = no block at that height yet (or an RPC hiccup), not orphaned.
            h=$(hash_at "$obs" $((fork + 1 + i)) || true)
            if [[ -n "$h" && "$h" != "${orphan[$i]}" ]]; then orphaned=$((orphaned + 1)); fi
        done
        if (( orphaned == ${#orphan[@]} )) && [[ "$(hash_at "$obs" "$fork")" == "$ancestor" ]] \
            && new_head=$(head_number "$obs") && (( new_head > fork )); then
            break
        fi
        sleep 1
    done
    start_vc || die "could not start $vc"
    log "reorged: depth $lo at block $((fork + 1)) (old head $ho, new head $new_head)"
}

# ── Commands ─────────────────────────────────────────────────────────────────

cmd_run() {
    local v
    for v in REORG_DEPTH_MIN REORG_DEPTH_MAX REORG_INTERVAL_MIN REORG_INTERVAL_MAX REORG_COUNT \
        REORG_PARTITION_TIMEOUT_SECS REORG_HEAL_TIMEOUT_SECS REORG_STALL_TIMEOUT_SECS; do
        # No leading zeros: (( )) would read them as octal.
        [[ "${!v}" =~ ^(0|[1-9][0-9]*)$ ]] \
            || die "$v must be a non-negative decimal integer without leading zeros (got '${!v}')"
    done
    (( REORG_DEPTH_MIN >= 1 && REORG_DEPTH_MIN <= REORG_DEPTH_MAX )) \
        || die "need 1 <= REORG_DEPTH_MIN <= REORG_DEPTH_MAX"
    (( REORG_INTERVAL_MAX - REORG_INTERVAL_MIN >= 2 )) \
        || die "need REORG_INTERVAL_MIN < R < REORG_INTERVAL_MAX to have a solution (MAX - MIN >= 2)"

    local obs_svc hid_svc obs hid obs_cl hid_cl
    obs_svc=$(service_matching '^el-1-')
    hid_svc=$(service_matching '^el-2-')
    [[ -n "$obs_svc" && -n "$hid_svc" ]] || die "observed/hidden services not found in enclave $ENCLAVE"
    obs=$(port_url "$obs_svc" rpc)
    hid=$(port_url "$hid_svc" rpc)
    obs_cl=$(port_url "$(service_matching '^cl-1-')" http)
    hid_cl=$(port_url "$(service_matching '^cl-2-')" http)

    trap restore EXIT
    trap 'exit 130' INT TERM
    restore || die "could not undo a previous run" # undo anything an interrupted run left behind
    log "observed $obs_svc ($obs), hidden $hid_svc ($hid)"
    log "depth [$REORG_DEPTH_MIN, $REORG_DEPTH_MAX], interval ($REORG_INTERVAL_MIN, $REORG_INTERVAL_MAX), count $REORG_COUNT"

    local n=0 interval depth start head last moved
    while (( REORG_COUNT == 0 || n < REORG_COUNT )); do
        interval=$((REORG_INTERVAL_MIN + 1 + RANDOM % (REORG_INTERVAL_MAX - REORG_INTERVAL_MIN - 1)))
        depth=$((REORG_DEPTH_MIN + RANDOM % (REORG_DEPTH_MAX - REORG_DEPTH_MIN + 1)))
        start=$(retry head_number "$obs") || die "observed RPC unreachable"
        log "next reorg (depth $depth) after $interval blocks from $start"
        last=$start
        moved=$SECONDS
        while :; do
            if head=$(head_number "$obs"); then
                (( head >= start + interval )) && break
                if (( head > last )); then
                    last=$head
                    moved=$SECONDS
                fi
            else
                log "warning: could not read observed head"
            fi
            (( SECONDS - moved < REORG_STALL_TIMEOUT_SECS )) \
                || die "observed head stuck at $last for ${REORG_STALL_TIMEOUT_SECS}s"
            sleep 2
        done
        reorg "$depth" "$obs" "$hid" "$obs_cl" "$hid_cl"
        n=$((n + 1))
    done
}

case "${1:-}" in
up)
    require docker kurtosis
    kurtosis enclave rm -f "$ENCLAVE" 2>/dev/null || true
    kurtosis run "$PACKAGE" --enclave "$ENCLAVE" --args-file "$ARGS" || die "failed to launch enclave $ENCLAVE"
    svc=$(service_matching '^el-1-') && [[ -n "$svc" ]] || die "observed execution client not found"
    url=$(port_url "$svc" rpc) || exit 1
    log "observed L1 RPC: $url"
    ;;
run) require docker kurtosis jq curl; cmd_run ;;
heal) require docker kurtosis; restore || die "could not fully clear the partition"; log "partition cleared" ;;
*) echo "usage: $0 up|run|heal" >&2; exit 2 ;;
esac
