#!/usr/bin/env bash
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
OUT="${KURTOSIS_ENDPOINTS_FILE:-$REPO/infra/kurtosis/endpoints.env}"

command -v kurtosis >/dev/null || { echo "kurtosis not found" >&2; exit 1; }

INSPECT="$(kurtosis enclave inspect "$ENCLAVE")" || {
    echo "enclave '$ENCLAVE' not running" >&2
    exit 1
}

to_http() {
    local u="${1#"${1%%[![:space:]]*}"}"
    u="${u%"${u##*[![:space:]]}"}"
    [[ "$u" =~ ^https?:// ]] && echo "$u" || echo "http://$u"
}

# Prefer kurtosis port print (stable across inspect layout changes).
try_port() {
    kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || return 1
}

discover_el_service() {
    echo "$INSPECT" | awk '
        /^[=]* User Services/ { in_services=1; next }
        in_services && /^[=]/ { exit }
        in_services && /el-[0-9]+-reth/ {
            match($0, /el-[0-9]+-reth[a-z0-9-]*/);
            if (RSTART > 0) { print substr($0, RSTART, RLENGTH); exit }
        }
    '
}

discover_builder_service() {
    echo "$INSPECT" | awk '
        /^[=]* User Services/ { in_services=1; next }
        in_services && /^[=]/ { exit }
        in_services && /el-[0-9]+-reth-builder/ {
            match($0, /el-[0-9]+-reth-builder[a-z0-9-]*/);
            if (RSTART > 0) { print substr($0, RSTART, RLENGTH); exit }
        }
    '
}

# Execution JSON-RPC (8545), not engine-rpc (8551).
parse_el_from_inspect() {
    echo "$INSPECT" | grep -E '[[:space:]]rpc: 8545/tcp' | head -1 \
        | sed -n 's/.*->[[:space:]]*\(http:\/\/\)*\([^[:space:]]*\).*/\2/p'
}

parse_builder_from_inspect() {
    echo "$INSPECT" | grep -E 'rbuilder-rpc:' | head -1 \
        | sed -n 's/.*->[[:space:]]*\(http:\/\/\)*\([^[:space:]]*\).*/\2/p'
}

el_raw=""
for svc in el-1-reth-lighthouse "$(discover_el_service)"; do
    [[ -n "$svc" ]] || continue
    if el_raw="$(try_port "$svc" rpc)"; then
        break
    fi
done
[[ -n "$el_raw" ]] || el_raw="$(parse_el_from_inspect || true)"

builder_raw=""
for svc in el-5-reth-builder-lighthouse "$(discover_builder_service)"; do
    [[ -n "$svc" ]] || continue
    if builder_raw="$(try_port "$svc" rbuilder-rpc)"; then
        break
    fi
done
[[ -n "$builder_raw" ]] || builder_raw="$(parse_builder_from_inspect || true)"

if [[ -z "$el_raw" ]]; then
    echo "could not find EL RPC for enclave '$ENCLAVE'" >&2
    echo "try: kurtosis port print $ENCLAVE el-1-reth-lighthouse rpc" >&2
    echo "or:  kurtosis enclave inspect $ENCLAVE | grep -E 'rpc: 8545'" >&2
    exit 1
fi

el_rpc="$(to_http "$el_raw")"
# rbuilder's bundle endpoint (port id "rbuilder-rpc"/8645) publishes in the EL
# range, NOT the mev range. If discovery failed, fall back to the EL host and
# warn — a wrong builder URL silently drops every bundle, so make it loud.
if [[ -z "$builder_raw" ]]; then
    echo "note: rbuilder-rpc not auto-found; set EEZ_L1_BUILDER_RPC_URL by hand" >&2
    echo "  (kurtosis port print $ENCLAVE el-5-reth-builder-lighthouse rbuilder-rpc)" >&2
    builder_raw="$el_raw"
fi
builder_rpc="$(to_http "$builder_raw")"

# Additional-services ports (published from public_port_start: 36000). Names
# vary by ethereum-package version, so try `port print` then fall back to
# grepping the inspect output. Empty is fine — reorg-scheduler.sh has its own
# default and these are only used when disruptoor/dora are enabled.
port_by_grep() {  # $1 = service-name regex, $2 = port-name
    echo "$INSPECT" | grep -E "$1" -A6 | grep -E "[[:space:]]$2:" | head -1 \
        | sed -n 's/.*->[[:space:]]*\(http:\/\/\)*\([^[:space:]]*\).*/\2/p'
}

disruptoor_raw=""
disruptoor_raw="$(try_port disruptoor http || true)"
[[ -n "$disruptoor_raw" ]] || disruptoor_raw="$(port_by_grep 'disruptoor' 'http' || true)"

dora_raw=""
dora_raw="$(try_port dora http || true)"
[[ -n "$dora_raw" ]] || dora_raw="$(port_by_grep '(^|[[:space:]])dora' 'http' || true)"

{
    echo "EEZ_L1_RPC_URL=$el_rpc"
    echo "EEZ_L1_TARGET_RPC_URL=$el_rpc"
    echo "EEZ_L1_BUILDER_RPC_URL=$builder_rpc"
    [[ -n "$disruptoor_raw" ]] && echo "EEZ_DISRUPTOOR_URL=$(to_http "$disruptoor_raw")"
    [[ -n "$dora_raw" ]] && echo "EEZ_DORA_URL=$(to_http "$dora_raw")"
} >"$OUT"

echo "wrote $OUT"
cat "$OUT"
[[ -n "$disruptoor_raw" ]] || echo "note: disruptoor URL not auto-found; set EEZ_DISRUPTOOR_URL by hand (see 'kurtosis enclave inspect $ENCLAVE')" >&2
