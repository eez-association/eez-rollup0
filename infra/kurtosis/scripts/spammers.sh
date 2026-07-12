#!/usr/bin/env bash
# One-command cross-chain spammer orchestrator for the EEZ Kurtosis devnet.
#
# You declare which spammers you want in spammers.yaml; this provisions the
# proxies/wrappers + outbound key, injects them per spammer, and starts them via
# the spamoor-eez daemon API. You never copy a proxy address or a key.
#
# Usage:
#   bash infra/kurtosis/scripts/spammers.sh up       # provision + start enabled spammers
#   bash infra/kurtosis/scripts/spammers.sh down     # stop + remove the spammers this tool started
#   bash infra/kurtosis/scripts/spammers.sh status   # list eez-xchain spammers and their state
#   bash infra/kurtosis/scripts/spammers.sh verify    # sanity-check: statuses + L1/L2 blockspace utilization
#
# Intent file: infra/kurtosis/spamoor-plugins/spammers.yaml (gitignored).
#   `up` copies it from spammers.example.yaml on first run. Override the path
#   with EEZ_SPAMMERS_FILE.
#
# Env knobs:
#   KURTOSIS_ENCLAVE     enclave name (default eez-devnet)
#   EEZ_SPAMMERS_FILE    intent file path (default the plugin's spammers.yaml)
#   EEZ_OUT_FUND_ETH     L2 funding for the outbound key (passed to xchain-provision.sh)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
K="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
PLUGIN_DIR="$K/spamoor-plugins"
INTENT="${EEZ_SPAMMERS_FILE:-$PLUGIN_DIR/spammers.yaml}"
CACHE="$REPO/datadir/xchain-provision.env"

die() { echo "error: $*" >&2; exit 1; }

for t in kurtosis curl jq cast python3; do
    command -v "$t" >/dev/null || die "$t not in PATH"
done
python3 -c "import yaml" >/dev/null 2>&1 || die "python3 'yaml' module not found (pip install pyyaml) — needed to parse the intent file"

# ── Endpoints ────────────────────────────────────────────────────────────────
_port() { kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || true; }
_http() { case "$1" in http*) echo "$1";; "") echo "";; *) echo "http://$1";; esac; }

daemon_url() {
    local u; u="$(_http "$(_port spamoor-eez http)")"
    [[ -n "$u" ]] || die "could not resolve the spamoor-eez daemon URL — is enclave '$ENCLAVE' up with spamoor_eez enabled? (kurtosis enclave inspect $ENCLAVE)"
    echo "$u"
}
api_base() { echo "$(daemon_url)/api"; }

# ── Intent file ──────────────────────────────────────────────────────────────
ensure_intent() {
    if [[ ! -f "$INTENT" ]]; then
        [[ -f "$PLUGIN_DIR/spammers.example.yaml" ]] || die "no intent file at $INTENT and no spammers.example.yaml to seed it from"
        echo "==> $INTENT not found; creating it from spammers.example.yaml (edit it and re-run)"
        cp "$PLUGIN_DIR/spammers.example.yaml" "$INTENT"
    fi
}

# render_payloads emits one POST /api/spammer JSON object per enabled spammer,
# injecting the provisioned addresses/key per direction.
render_payloads() {
    python3 - "$INTENT" "$CACHE" <<'PYEOF'
import sys, yaml, json

intent_path, cache_path = sys.argv[1], sys.argv[2]

with open(intent_path) as f:
    doc = yaml.safe_load(f) or {}
spammers = doc.get("spammers", [])
if not isinstance(spammers, list):
    sys.stderr.write("intent file: top-level 'spammers' must be a list\n"); sys.exit(2)

cache = {}
with open(cache_path) as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cache[k] = v

def need(key):
    v = cache.get(key, "")
    if not v:
        sys.stderr.write("provisioning cache is missing %s — re-run provisioning (xchain-provision.sh)\n" % key)
        sys.exit(3)
    return v

# op -> (config field, provisioning cache var) per direction.
OP_FIELDS = {
    "inbound": {
        "set": ("inbound_proxy", "INBOUND_PROXY"),
        "noret": ("inbound_noret_proxy", "INBOUND_NORET_PROXY"),
        "value": ("inbound_deposit_proxy", "INBOUND_DEP_PROXY"),
        "wrapper": ("inbound_wrapper", "INBOUND_WRAPPER"),
    },
    "outbound": {
        "set": ("outbound_proxy", "OUTBOUND_PROXY"),
        "noret": ("outbound_noret_proxy", "OUTBOUND_NORET_PROXY"),
        "value": ("outbound_withdraw_proxy", "OUTBOUND_WD_PROXY"),
        "wrapper": ("outbound_wrapper", "OUTBOUND_WRAPPER"),
    },
}
# Knobs copied straight through from the intent entry into the scenario config.
PASSTHROUGH = ["throughput", "total_count", "attack", "max_wallets", "max_pending",
               "value_max", "inbound_weight", "outbound_weight",
               "base_fee", "tip_fee", "base_fee_wei", "tip_fee_wei",
               "gas_limit", "timeout", "log_txs"]

out = []
outbound_count = 0
for e in spammers:
    if not isinstance(e, dict) or not e.get("enabled", True):
        continue
    name = e.get("name")
    if not name:
        sys.stderr.write("a spammer entry is missing 'name'\n"); sys.exit(4)
    mode = e.get("mode", "mixed")
    if mode not in ("inbound", "outbound", "mixed"):
        sys.stderr.write("spammer %r: invalid mode %r (want inbound|outbound|mixed)\n" % (name, mode)); sys.exit(5)

    cfg = {"mode": mode}
    for k in PASSTHROUGH:
        if e.get(k) is not None:
            cfg[k] = e[k]
    ops = ["set"] if e.get("attack") else (e.get("ops") or ["set"])
    if e.get("ops"):
        cfg["ops"] = e["ops"]

    inbound = mode in ("inbound", "mixed") or bool(e.get("inbound_weight"))
    outbound = mode in ("outbound", "mixed") or bool(e.get("outbound_weight"))
    # One provisioned outbound key backs every outbound pool, so two outbound
    # spammers would collide funding it. Allow at most one.
    if outbound:
        outbound_count += 1
        if outbound_count > 1:
            sys.stderr.write("more than one enabled outbound/mixed spammer — they share the single provisioned outbound key and collide funding it; combine into one entry\n"); sys.exit(7)
    # Inject only the addresses the enabled ops actually use, per active direction.
    for direction, active in (("inbound", inbound), ("outbound", outbound)):
        if not active:
            continue
        if direction == "outbound":
            cfg["outbound_private_key"] = need("OUT_KEY")
        for op in ops:
            if op not in OP_FIELDS[direction]:
                sys.stderr.write("spammer %r: invalid op %r\n" % (name, op)); sys.exit(6)
            fld, ck = OP_FIELDS[direction][op]
            cfg[fld] = need(ck)

    out.append({
        "name": "eez:" + str(name),
        "scenario": "eez-xchain",
        "description": "eez cross-chain load (managed by spammers.sh)",
        "config": yaml.safe_dump(cfg, default_flow_style=False, sort_keys=False),
        "startImmediately": True,
    })

for p in out:
    print(json.dumps(p))
PYEOF
}

provision() {
    echo "==> provisioning cross-chain resources (idempotent)"
    bash "$HERE/xchain-provision.sh"
    [[ -f "$CACHE" ]] || die "provisioning did not produce $CACHE"
}

# ── Commands ─────────────────────────────────────────────────────────────────
cmd_up() {
    ensure_intent
    provision
    local base; base="$(api_base)"

    # Build payloads up front so a bad intent file fails loudly here.
    local payloads
    if ! payloads="$(render_payloads)"; then
        die "could not build spammer configs from $INTENT (see error above)"
    fi

    local existing
    existing="$(curl -fsS "$base/spammers" 2>/dev/null | jq -r '.[].name' 2>/dev/null || true)"

    local created=0 skipped=0 failed=0
    while IFS= read -r payload; do
        [[ -n "$payload" ]] || continue
        local name; name="$(jq -r '.name' <<<"$payload")"
        if [[ -n "$existing" ]] && grep -qxF "$name" <<<"$existing"; then
            echo "  = $name already exists — skipping (run 'down' first to recreate)"
            skipped=$((skipped + 1)); continue
        fi
        local resp id
        if resp="$(curl -fsS -X POST "$base/spammer" -H 'Content-Type: application/json' -d "$payload" 2>/dev/null)"; then
            id="$(jq -r 'if type=="object" then (.id // .) else . end' <<<"$resp" 2>/dev/null || echo "$resp")"
            echo "  ✓ started $name (id=$id)"
            created=$((created + 1))
        else
            echo "  ✗ failed to create $name (POST $base/spammer)"
            failed=$((failed + 1))
        fi
    done <<<"$payloads"

    echo
    echo "==> $created started, $skipped skipped, $failed failed"
    echo "    daemon UI: $(daemon_url)   (tune throughput live; 'spammers.sh verify' to sanity-check)"
    (( failed == 0 )) || exit 1
}

cmd_down() {
    local base; base="$(api_base)"
    local rows
    rows="$(curl -fsS "$base/spammers" | jq -r '.[] | select(.name|startswith("eez:")) | "\(.id)\t\(.name)"')"
    if [[ -z "$rows" ]]; then echo "==> no eez-xchain spammers to remove"; return 0; fi
    local removed=0
    while IFS=$'\t' read -r id name; do
        [[ -n "$id" ]] || continue
        curl -fsS -X POST "$base/spammer/$id/pause" >/dev/null 2>&1 || true
        if curl -fsS -X DELETE "$base/spammer/$id" >/dev/null 2>&1; then
            echo "  ✓ removed $name (id=$id)"; removed=$((removed + 1))
        else
            echo "  ✗ failed to remove $name (id=$id)"
        fi
    done <<<"$rows"
    echo "==> removed $removed spammer(s)"
}

cmd_status() {
    local base; base="$(api_base)"
    echo "==> eez-xchain spammers on $ENCLAVE:"
    curl -fsS "$base/spammers" \
        | jq -r '.[] | select(.name|startswith("eez:")) | "  \(.id)\t\(.status)\t\(.name)"' \
        || die "could not query $base/spammers"
}

# util <rpc> — mean gasUsed/gasLimit % over the last few blocks.
util() {
    local rpc="$1" latest used=0 lim=0 b bn u l
    latest="$(cast block-number --rpc-url "$rpc" 2>/dev/null || echo 0)"
    for b in 0 1 2 3 4; do
        bn=$((latest - b)); (( bn < 0 )) && continue
        u="$(cast block "$bn" --field gasUsed --rpc-url "$rpc" 2>/dev/null || echo 0)"
        l="$(cast block "$bn" --field gasLimit --rpc-url "$rpc" 2>/dev/null || echo 0)"
        used=$((used + u)); lim=$((lim + l))
    done
    (( lim > 0 )) && echo "$((used * 100 / lim))%" || echo "?"
}

cmd_verify() {
    local base L1 L2
    base="$(api_base)"
    L1="$(_http "$(_port el-1-reth-lighthouse rpc)")"
    L2="$(_http "$(_port eez-node l2-rpc)")"

    echo "==> spammer statuses:"
    local rows
    rows="$(curl -fsS "$base/spammers" | jq -r '.[] | select(.name|startswith("eez:")) | "  \(.id)\t\(.status)\t\(.name)"' || true)"
    if [[ -z "$rows" ]]; then
        echo "  (none — run 'spammers.sh up' first)"
    else
        echo "$rows"
    fi

    echo "==> blockspace utilization (last ~5 blocks):"
    [[ -n "$L1" ]] && echo "  L1 ($L1): $(util "$L1")"
    [[ -n "$L2" ]] && echo "  L2 ($L2): $(util "$L2")"

    cat <<'EOF'

Note: cross-chain ops drain at the settlement rate. Pending pinned at
max_pending is a backlog (throughput > drain rate), not a stall — lower
throughput to find the sustainable rate. Outbound drains slower than inbound.
EOF
}

usage() {
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
    up)      cmd_up ;;
    down)    cmd_down ;;
    status)  cmd_status ;;
    verify)  cmd_verify ;;
    ""|-h|--help|help) usage ;;
    *) die "unknown command '$1' (want up|down|status|verify)" ;;
esac
