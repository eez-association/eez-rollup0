#!/usr/bin/env bash
#
# Cross-chain test driver for a RUNNING dockerized eez-node (chiado) — the
# "new format" successor to scripts/devnet-test.sh. Instead of poking the raw
# L2 RPC, it drives the two transparent ingress FRONTS:
#     L1 front :18999  → L1→L2 (Inbound)      L2 front :18998 → L2→L1 (Outbound)
# and exercises both directions × op types (setValue / setValueNoRet / deposit /
# withdraw) × direct + wrapper, then tallies settled successes + pipeline metrics.
#
# Bring the node up first (scripts/chiado-up.sh), then:
#
#   bash scripts/xchain-test.sh                              # MATRIX (default)
#   EEZ_MODE=load EEZ_IN_N=100 EEZ_OUT_N=100 bash scripts/xchain-test.sh
#   EEZ_MODE=load EEZ_PACE_N=10 EEZ_PACE_INTERVAL=10 ...     # paced: ~1 tx/s
#   EEZ_RESTART=1 ...                                        # docker-restart mid-run
#
# Modes:
#   matrix (default) — EEZ_WAVE_COUNT waves of the full cc matrix (both dirs,
#                      direct + wrapper, deposit/withdraw) + pure-L2 + 1 poison;
#                      asserts semantic effects + value conservation.
#   load             — EEZ_IN_N inbound + EEZ_OUT_N outbound setValue from DISTINCT
#                      ephemeral senders (optionally paced); counts settled successes.
#
# Every run reports: settled inbound/outbound, N+1 next-slot hit-rate,
# consecutive-L1-slot landing, bundle drops/evictions, L1↔L2 reconcile, divergence.
#
# Knobs: EEZ_MODE, EEZ_WAVE_COUNT(3), EEZ_IN_N(100), EEZ_OUT_N(100),
#        EEZ_PACE_N(0=burst), EEZ_PACE_INTERVAL(10), EEZ_RESTART(0),
#        EEZ_MAX_USER_TXS_PER_BUNDLE (informational; set on the node, not here),
#        NODE_CONTAINER(eez-node-chiado).

set -uo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1
REPO="$(cd "$(dirname "$0")/.." && pwd)"; cd "$REPO"

# ── Endpoints (the running node) ─────────────────────────────────────
L1=http://localhost:18645          # embedded chiado L1 (composer watches + posts)
L2=http://localhost:18688          # L2 RPC
L1F=http://localhost:18999         # L1 front (Inbound)
L2F=http://localhost:18998         # L2 front (Outbound)
CCM=0x4200000000000000000000000000000000000007; MAINNET_RID=0
NODE_CONTAINER="${NODE_CONTAINER:-eez-node-chiado}"

# ── Knobs ────────────────────────────────────────────────────────────
MODE="${EEZ_MODE:-matrix}"
WAVES="${EEZ_WAVE_COUNT:-3}"
IN_N="${EEZ_IN_N:-100}"; OUT_N="${EEZ_OUT_N:-100}"
PACE_N="${EEZ_PACE_N:-0}"; PACE_INT="${EEZ_PACE_INTERVAL:-10}"
DO_RESTART="${EEZ_RESTART:-0}"

# ── Keys (testnet only) ──────────────────────────────────────────────
OP=0x2248a31395af28e24349c8e566c19475a79cb610389204ab26bc585493e5cf27       # funded operator (L1 deploys + funding)
HH_KEY_2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a  # L2 deployer / pure sender (genesis alloc)
HH_ADDR_2=0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC
# Fresh random recipients per run — a FIXED recipient's cross-chain proxy would
# persist across runs (stale "already exists") and its balance would accumulate
# (dirty deltas). Minted fresh so every proxy is new and every delta starts at 0.
rand_addr(){ echo "0x$(python3 -c 'import secrets;print(secrets.token_hex(20))')"; }
L2_DEP_RECIPIENT=$(rand_addr); L1_WD_RECIPIENT=$(rand_addr)

for t in cast forge jq curl docker python3; do command -v "$t" >/dev/null || { echo "✗ $t not in PATH"; exit 1; }; done
docker inspect "$NODE_CONTAINER" >/dev/null 2>&1 || { echo "✗ container '$NODE_CONTAINER' not up — run scripts/chiado-up.sh"; exit 1; }
[[ "$(cast chain-id --rpc-url "$L1" 2>/dev/null)" == "10200" ]] || { echo "✗ embedded L1 not on :18645"; exit 1; }
[[ -f "$REPO/deployments.env" ]] || { echo "✗ deployments.env missing — deploy first"; exit 1; }
set -a; source "$REPO/deployments.env"; set +a
L1_CID=$(cast chain-id --rpc-url "$L1"); L2_CID=$(cast chain-id --rpc-url "$L2")
NODE_LOG="$(mktemp)"; SINCE_TS="$(date +%s)"       # scope log metrics to THIS run
refresh_log(){ docker logs --since "$SINCE_TS" "$NODE_CONTAINER" >"$NODE_LOG" 2>&1 || true; }
trap 'rm -f "$NODE_LOG"' EXIT
CAP="$(docker exec "$NODE_CONTAINER" printenv EEZ_MAX_USER_TXS_PER_BUNDLE 2>/dev/null || echo 3)"
echo "════ XCHAIN TEST ════  mode=$MODE  bundle_cap=$CAP  restart=$DO_RESTART  (L2=$(cast block-number --rpc-url "$L2"))"

# ── Shared helpers ───────────────────────────────────────────────────
grab(){ grep -oE "$1=0x[0-9a-fA-F]{40}" | head -1 | cut -d= -f2; }
# Non-blocking receipt check (raw RPC + timeout). `cast receipt` WITHOUT --async
# blocks forever on a never-mined hash.
receipt_ok(){ [[ "$(timeout 5 curl -s -X POST "$1" -H 'Content-Type: application/json' -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$2\"],\"id\":1}" 2>/dev/null | jq -r '.result.status // "x"' 2>/dev/null)" == "0x1" ]]; }
count_ok(){ local rpc="$1"; shift; local h n=0; for h in "$@"; do receipt_ok "$rpc" "$h" && n=$((n+1)); done; echo "$n"; }
# Build a signed tx offline (explicit nonce/gas) + POST to a front. Echoes the
# tx hash on a successful hold, "" on reject.
submit_front(){ # <front> <key> <nonce> <cid> <gas> <value> <to> <sig|""> [args...]
  local front="$1" key="$2" nonce="$3" cid="$4" gas="$5" val="$6" to="$7" sig="$8"; shift 8
  local raw resp
  if [[ -n "$sig" ]]; then
    raw=$(cast mktx --rpc-url "$([[ "$cid" == "$L1_CID" ]] && echo "$L1" || echo "$L2")" --chain-id "$cid" --private-key "$key" --nonce "$nonce" --gas-limit "$gas" --gas-price 5000000000 --priority-gas-price 1000000000 --value "$val" "$to" "$sig" "$@" 2>/dev/null)
  else
    raw=$(cast mktx --rpc-url "$([[ "$cid" == "$L1_CID" ]] && echo "$L1" || echo "$L2")" --chain-id "$cid" --private-key "$key" --nonce "$nonce" --gas-limit "$gas" --gas-price 5000000000 --priority-gas-price 1000000000 --value "$val" "$to" 2>/dev/null)
  fi
  [[ "$raw" =~ ^0x ]] || { echo ""; return; }
  resp=$(curl -s -X POST "$front" -H 'Content-Type: application/json' -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}")
  echo "$resp" | grep -q '"result"' && cast keccak "$raw" || echo ""
}
onchain_nonce(){ cast nonce "$(cast wallet address --private-key "$1")" --rpc-url "$2" 2>/dev/null || echo 0; }
# Robust pool funding: async submit + verify-count + re-fund nonce-gap stragglers.
fund_pool(){ # <funder_key> <rpc> <amount> <key...>
  local fk="$1" rpc="$2" amt="$3"; shift 3; local keys=("$@") faddr n0 i sent=0 BATCH="${EEZ_FUND_BATCH:-12}"
  faddr=$(cast wallet address --private-key "$fk")
  # Batch to stay under reth's per-account pending-tx limit: submit BATCH, wait
  # for them to MINE (funder nonce advances), then the next batch. Firing all
  # 100 from one funder at once exceeds the limit → excess rejected → nonce gap
  # → the whole pool stalls (the bug this replaces).
  while [[ "$sent" -lt "${#keys[@]}" ]]; do
    n0=$(cast nonce "$faddr" --rpc-url "$rpc"); i=0
    while [[ "$i" -lt "$BATCH" && $((sent+i)) -lt "${#keys[@]}" ]]; do
      cast send "$(cast wallet address --private-key "${keys[$((sent+i))]}")" --value "$amt" --private-key "$fk" --rpc-url "$rpc" --nonce "$((n0+i))" --gas-price 2000000000 --priority-gas-price 1500000000 --async >/dev/null 2>&1
      i=$((i+1))
    done
    local ok=0; for _ in $(seq 1 45); do [[ "$(cast nonce "$faddr" --rpc-url "$rpc" 2>/dev/null||echo "$n0")" -ge "$((n0+i))" ]] && { ok=1; break; }; sleep 2; done
    [[ "$ok" == 1 ]] || { echo "    ⚠ batch stalled at $sent/${#keys[@]} (funder txs not mining — node not including mempool?)"; return 1; }
    sent=$((sent+i)); echo "    funded $sent/${#keys[@]}"
  done; return 0; }

fdep(){ local rpc="$1" key="$2" sc="$3" sig="$4"; shift 4; local addr n0
  addr=$(cast wallet address --private-key "$key"); n0=$(cast nonce "$addr" --rpc-url "$rpc" 2>/dev/null); n0=${n0:-0}
  (cd "$REPO/contracts" && forge script "script/$sc" --sig "$sig" "$@" --rpc-url "$rpc" --broadcast --private-key "$key" --skip-simulation --gas-price 2000000000 --priority-gas-price 1500000000 2>&1)
  for _ in $(seq 1 40); do [[ "$(cast nonce "$addr" --rpc-url "$rpc" 2>/dev/null||echo "$n0")" -gt "$n0" ]] && break; sleep 1; done; }
dep_l1(){ local a; a=$(fdep "$L1" "$OP" "$1" "$2" "$3" | grab "$4"); local c=0x
  for _ in $(seq 1 90); do c=$(cast code "$a" --rpc-url "$L1" 2>/dev/null||echo 0x); [[ "$c" != "0x" && -n "$c" ]] && break; sleep 1; done; echo "$a"; }
deploy_l2(){ local full="$1${2#0x}" nonce raw txh ca; nonce=$(cast nonce "$HH_ADDR_2" --rpc-url "$L2")
  raw=$(cast mktx --rpc-url "$L2" --chain-id "$L2_CID" --private-key "$HH_KEY_2" --nonce "$nonce" --gas-limit 3000000 --gas-price 2000000000 --create "$full" 2>&1); [[ "$raw" =~ ^0x ]] || { echo ""; return; }
  txh=$(curl -s -X POST "$L2" -H 'Content-Type: application/json' -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}"|jq -r '.result//empty')
  for _ in $(seq 1 30); do ca=$(cast receipt "$txh" --rpc-url "$L2" --async --json 2>/dev/null|jq -r '.contractAddress//empty'); [[ -n "$ca" && "$ca" != null ]] && break; sleep 2; done; echo "$ca"; }
l2_proxy(){ local tgt="$1" p code nonce raw
  p=$(cast call "$CCM" 'computeCrossChainProxyAddress(address,uint256)(address)' "$tgt" "$MAINNET_RID" --rpc-url "$L2"|tr -d '[:space:]'); code=$(cast code "$p" --rpc-url "$L2" 2>/dev/null||echo 0x)
  if [[ "$code" == "0x" || -z "$code" ]]; then nonce=$(cast nonce "$HH_ADDR_2" --rpc-url "$L2")
    raw=$(cast mktx --rpc-url "$L2" --chain-id "$L2_CID" --private-key "$HH_KEY_2" --nonce "$nonce" --gas-limit 1500000 --gas-price 1000000000 "$CCM" 'createCrossChainProxy(address,uint256)' "$tgt" "$MAINNET_RID")
    curl -s -X POST "$L2" -H 'Content-Type: application/json' -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}">/dev/null
    for _ in $(seq 1 30); do code=$(cast code "$p" --rpc-url "$L2" 2>/dev/null||echo 0x); [[ "$code" != "0x" && -n "$code" ]] && break; sleep 1; done; fi; echo "$p"; }
# Idempotent L1 proxy: reuse the deterministic address if it already has code
# (a fixed recipient's proxy persists across runs), else create it.
l1_proxy(){ local tgt="$1" p code
  p=$(cast call "$EEZ_REGISTRY_ADDRESS" 'computeCrossChainProxyAddress(address,uint256)(address)' "$tgt" "$EEZ_ROLLUP_ID" --rpc-url "$L1" 2>/dev/null|tr -d '[:space:]')
  code=$(cast code "$p" --rpc-url "$L1" 2>/dev/null||echo 0x)
  if [[ "$code" == "0x" || -z "$code" ]]; then
    fdep "$L1" "$OP" CreateValueProxy.s.sol:CreateValueProxy 'run(address,address,uint256)' "$EEZ_REGISTRY_ADDRESS" "$tgt" "$EEZ_ROLLUP_ID" >/dev/null
    for _ in $(seq 1 30); do code=$(cast code "$p" --rpc-url "$L1" 2>/dev/null||echo 0x); [[ "$code" != "0x" && -n "$code" ]] && break; sleep 1; done; fi
  echo "$p"; }

# ── Deploy targets + proxies + wrappers (both directions) ────────────
echo "==> deploying targets + proxies + wrappers"
VALUE_BC=$(cd "$REPO/contracts" && forge inspect Value bytecode)
NORET_BC=$(cd "$REPO/contracts" && forge inspect ValueNoRet bytecode)
WRAP_BC=$(cd "$REPO/contracts" && forge inspect SetterWrapper bytecode)
L2_VALUE=$(deploy_l2 "$VALUE_BC" "$(cast abi-encode 'c(uint256)' 0)")
L2_NORET=$(deploy_l2 "$NORET_BC" "$(cast abi-encode 'c(uint256)' 0)")
L1_VALUE=$(dep_l1 DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 EEZ_VALUE_ADDRESS)
L1_NORET=$(dep_l1 DeployValueNoRetL2.s.sol:DeployValueNoRetL2 'run(uint256)' 0 EEZ_VALUE_NORET_ADDRESS)
IN_VALUE_PROXY=$(fdep "$L1" "$OP" CreateValueProxy.s.sol:CreateValueProxy 'run(address,address,uint256)' "$EEZ_REGISTRY_ADDRESS" "$L2_VALUE" "$EEZ_ROLLUP_ID"|grab EEZ_VALUE_PROXY)
IN_NORET_PROXY=$(fdep "$L1" "$OP" CreateValueProxy.s.sol:CreateValueProxy 'run(address,address,uint256)' "$EEZ_REGISTRY_ADDRESS" "$L2_NORET" "$EEZ_ROLLUP_ID"|grab EEZ_VALUE_PROXY)
IN_DEP_PROXY=$(fdep "$L1" "$OP" CreateValueProxy.s.sol:CreateValueProxy 'run(address,address,uint256)' "$EEZ_REGISTRY_ADDRESS" "$L2_DEP_RECIPIENT" "$EEZ_ROLLUP_ID"|grab EEZ_VALUE_PROXY)
IN_WRAPPER=$(fdep "$L1" "$OP" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 'run(address)' "$IN_VALUE_PROXY"|grab EEZ_SETTER_WRAPPER)
OUT_VALUE_PROXY=$(l2_proxy "$L1_VALUE"); OUT_NORET_PROXY=$(l2_proxy "$L1_NORET"); OUT_WD_PROXY=$(l2_proxy "$L1_WD_RECIPIENT")
OUT_WRAPPER=$(deploy_l2 "$WRAP_BC" "$(cast abi-encode 'c(address)' "$OUT_VALUE_PROXY")")
echo "    inbound:  setter=$IN_VALUE_PROXY dep=$IN_DEP_PROXY wrapper=$IN_WRAPPER"
echo "    outbound: setter=$OUT_VALUE_PROXY wd=$OUT_WD_PROXY wrapper=$OUT_WRAPPER"
[[ -n "$IN_VALUE_PROXY" && -n "$IN_WRAPPER" && -n "$OUT_VALUE_PROXY" && -n "$OUT_WRAPPER" ]] || { echo "✗ proxy/wrapper setup failed"; exit 1; }

DEP_BASE=$(cast balance "$L2_DEP_RECIPIENT" --rpc-url "$L2"); WD_BASE=$(cast balance "$L1_WD_RECIPIENT" --rpc-url "$L1")
maybe_restart(){ [[ "$DO_RESTART" == 1 ]] || return 0
  local pre; pre=$(cast block-number --rpc-url "$L2"); echo "==> RESTART: docker restart $NODE_CONTAINER (L2 was $pre)"
  local t0; t0=$(date +%s); docker restart "$NODE_CONTAINER" >/dev/null 2>&1
  for _ in $(seq 1 90); do [[ "$(cast block-number --rpc-url "$L2" 2>/dev/null||echo 0)" -gt "$pre" ]] && { echo "    ✓ resumed in $(( $(date +%s)-t0 ))s"; return; }; sleep 4; done
  echo "    ✗ did not resume within 360s"; }

# ══════════════════════════ MODE: load ═══════════════════════════════
if [[ "$MODE" == load ]]; then
  echo "==> LOAD: $IN_N inbound + $OUT_N outbound setValue (distinct senders$([[ "$PACE_N" -gt 0 ]] && echo ", paced $PACE_N/${PACE_INT}s" || echo ", burst"))"
  declare -a INK=() OUTK=() INH=() OUTH=(); IN_ACC=0 OUT_ACC=0 fired=0
  for _ in $(seq 1 "$IN_N");  do INK+=("$(cast wallet new 2>/dev/null|awk '/Private key/{print $3}')"); done
  for _ in $(seq 1 "$OUT_N"); do OUTK+=("$(cast wallet new 2>/dev/null|awk '/Private key/{print $3}')"); done
  echo "==> funding senders"; fund_pool "$OP" "$L1" 20000000000000000 "${INK[@]}"; fund_pool "$HH_KEY_2" "$L2" 20000000000000000 "${OUTK[@]}"
  maybe_restart
  echo "==> firing"
  N=$(( IN_N > OUT_N ? IN_N : OUT_N ))
  for ((i=0;i<N;i++)); do
    if (( i < IN_N )); then h=$(submit_front "$L1F" "${INK[$i]}" 0 "$L1_CID" 2000000 0 "$IN_VALUE_PROXY" 'setValue(uint256)' "$((1000+i))"); [[ -n "$h" ]] && { INH+=("$h"); IN_ACC=$((IN_ACC+1)); }; fi
    if (( i < OUT_N )); then h=$(submit_front "$L2F" "${OUTK[$i]}" 0 "$L2_CID" 2000000 0 "$OUT_VALUE_PROXY" 'setValue(uint256)' "$((2000+i))"); [[ -n "$h" ]] && { OUTH+=("$h"); OUT_ACC=$((OUT_ACC+1)); }; fi
    if [[ "$PACE_N" -gt 0 ]]; then fired=$((fired+2)); (( fired % PACE_N == 0 )) && sleep "$PACE_INT"; fi
  done
  echo "    front-accepted: inbound $IN_ACC/$IN_N  outbound $OUT_ACC/$OUT_N"
  printf '%s\n' "${INH[@]}" > "$REPO/datadir/smoke-logs/xchain-in-hashes.txt"; printf '%s\n' "${OUTH[@]}" > "$REPO/datadir/smoke-logs/xchain-out-hashes.txt"
  echo "==> draining (up to ~12m)"
  prev=-1 stable=0
  for _ in $(seq 1 80); do
    im=$(count_ok "$L1" "${INH[@]}"); om=$(count_ok "$L2" "${OUTH[@]}"); echo "    settled: inbound $im/$IN_ACC  outbound $om/$OUT_ACC"
    [[ $((im+om)) -ge $((IN_ACC+OUT_ACC)) ]] && break
    [[ $((im+om)) -eq "$prev" ]] && stable=$((stable+1)) || stable=0; [[ "$stable" -ge 5 ]] && { echo "    (no progress ~75s → remaining dropped/omitted)"; break; }
    prev=$((im+om)); sleep 15
  done
  IN_OK=$(count_ok "$L1" "${INH[@]}"); OUT_OK=$(count_ok "$L2" "${OUTH[@]}"); OK=1

# ══════════════════════════ MODE: matrix ═════════════════════════════
else
  # Fresh senders per run — no shared hardhat pool, so no accumulated nonces and
  # no reuse. One tx per sender (nonce 0); fund on BOTH chains so any sender can
  # do inbound (L1) or outbound (L2). nextk sets $PK IN PLACE — NOT via $(...),
  # which runs in a subshell and would lose the index increment.
  NEED=$((WAVES*6 + 3)); declare -a SND=()
  for _ in $(seq 1 "$NEED"); do SND+=("$(cast wallet new 2>/dev/null|awk '/Private key/{print $3}')"); done
  SI=0; PK=""; nextk(){ PK="${SND[$SI]:?matrix sender pool exhausted; raise NEED}"; SI=$((SI+1)); }
  echo "==> funding $NEED fresh matrix senders (L1+L2)"; fund_pool "$OP" "$L1" 30000000000000000 "${SND[@]}"; fund_pool "$HH_KEY_2" "$L2" 30000000000000000 "${SND[@]}"
  # PREFUND must fit the sender's funding (0.03) minus gas — it's just withdrawal
  # liquidity (needs only >= total withdrawals = WAVES*WD_AMT).
  DEP_AMT=1000000000000000; WD_AMT=1000000000000000; PREFUND=10000000000000000; EXP_DEP=0 EXP_WD=0
  PURE_RECIP=$(rand_addr); POISON_ADDR=$(rand_addr)
  # prefund L1 withdrawal liquidity via one inbound deposit (fresh sender @ nonce 0)
  nextk; submit_front "$L1F" "$PK" 0 "$L1_CID" 2000000 "$PREFUND" "$IN_DEP_PROXY" "" >/dev/null; EXP_DEP=$((EXP_DEP+PREFUND))
  for _ in $(seq 1 60); do [[ "$(python3 -c "print($(cast balance "$L2_DEP_RECIPIENT" --rpc-url "$L2")-$DEP_BASE)")" -ge "$PREFUND" ]] && break; sleep 3; done
  echo "==> pure-L2 burst (8 transfers → fresh recipient)"; pn=$(cast nonce "$HH_ADDR_2" --rpc-url "$L2")
  for i in $(seq 0 7); do r=$(cast mktx --rpc-url "$L2" --chain-id "$L2_CID" --private-key "$HH_KEY_2" --nonce "$((pn+i))" --gas-limit 30000 --gas-price 2000000000 "$PURE_RECIP" --value 100000000000000 2>/dev/null); curl -s -X POST "$L2" -H 'Content-Type: application/json' -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$r\"],\"id\":1}">/dev/null; done
  echo "==> $WAVES matrix waves (both dirs + wrapper + dep/wd) + poison"
  for w in $(seq 1 "$WAVES"); do v=$((40+w)); echo "    --- wave $w (v=$v) ---"
    nextk; submit_front "$L1F" "$PK" 0 "$L1_CID" 2000000 0 "$IN_VALUE_PROXY" 'setValue(uint256)' "$v" >/dev/null
    nextk; submit_front "$L2F" "$PK" 0 "$L2_CID" 2000000 0 "$OUT_VALUE_PROXY" 'setValue(uint256)' "$v" >/dev/null
    nextk; submit_front "$L1F" "$PK" 0 "$L1_CID" 2000000 "$DEP_AMT" "$IN_DEP_PROXY" "" >/dev/null; EXP_DEP=$((EXP_DEP+DEP_AMT))
    nextk; submit_front "$L2F" "$PK" 0 "$L2_CID" 2000000 "$WD_AMT" "$OUT_WD_PROXY" "" >/dev/null; EXP_WD=$((EXP_WD+WD_AMT))
    nextk; submit_front "$L1F" "$PK" 0 "$L1_CID" 2000000 0 "$IN_WRAPPER" 'setViaProxy(uint256)' "$((v+100))" >/dev/null
    nextk; submit_front "$L2F" "$PK" 0 "$L2_CID" 2000000 0 "$OUT_WRAPPER" 'setViaProxy(uint256)' "$((v+100))" >/dev/null
    [[ "$w" == 1 ]] && { nextk; submit_front "$L1F" "$PK" 0 "$L1_CID" 2000000 0 "$POISON_ADDR" 'setValue(uint256)' 9 >/dev/null; echo "      (1 poison → non-proxy addr)"; }
    sleep 8
  done
  maybe_restart
  echo "==> settling (up to 300s)"; EXP_V=$((40+WAVES+100)); OK=1
  for _ in $(seq 1 100); do [[ "$(cast call "$L1_VALUE" 'value()(uint256)' --rpc-url "$L1" 2>/dev/null|awk '{print $1}')" == "$EXP_V" && "$(cast call "$L2_VALUE" 'value()(uint256)' --rpc-url "$L2" 2>/dev/null|awk '{print $1}')" == "$EXP_V" ]] && break; sleep 3; done
  echo "    --- effect assertions ---"
  chk(){ [[ "$1" == "$2" ]] && echo "    ✓ $3 == $1" || { echo "    ✗ $3 got=$1 want=$2"; OK=0; }; }
  chk "$(cast call "$L2_VALUE" 'value()(uint256)' --rpc-url "$L2"|awk '{print $1}')" "$EXP_V" "L2 Value (inbound wrapper)"
  chk "$(cast call "$L1_VALUE" 'value()(uint256)' --rpc-url "$L1"|awk '{print $1}')" "$EXP_V" "L1 Value (outbound wrapper)"
  chk "$(python3 -c "print($(cast balance "$L2_DEP_RECIPIENT" --rpc-url "$L2")-$DEP_BASE)")" "$EXP_DEP" "L2 deposit Δ (value conserved)"
  chk "$(python3 -c "print($(cast balance "$L1_WD_RECIPIENT" --rpc-url "$L1")-$WD_BASE)")" "$EXP_WD" "L1 withdraw Δ (value conserved)"
  IN_N=0 OUT_N=0 IN_ACC=0 OUT_ACC=0 IN_OK=0 OUT_OK=0
fi

# ── Metrics (both modes) ─────────────────────────────────────────────
refresh_log; clean=$(sed -r 's/\x1b\[[0-9;]*[mK]//g' "$NODE_LOG")
# N+1 next-slot hit-rate: join dispatch breadcrumb (tx_hash→target_block) with observed Included (tx_hash,l1_block).
n1=$(python3 - <<PY
import re
tgt={}
for l in """$(grep 'dispatching bundle to builder' <<<"$clean")""".splitlines():
    h=re.search(r'tx_hash=(0x[0-9a-f]{64})',l); b=re.search(r'target_block=(\d+)',l)
    if h and b: tgt[h.group(1)]=int(b.group(1))
hit=chk=0
for l in """$(grep 'bundle outcome observed' <<<"$clean"|grep Included)""".splitlines():
    h=re.search(r'tx_hash: (0x[0-9a-f]{64})',l); b=re.search(r'l1_block: (\d+)',l)
    if h and b and h.group(1) in tgt: chk+=1; hit+= (int(b.group(1))==tgt[h.group(1)])
print(f"{hit}/{chk} ({0 if not chk else round(100*hit/chk)}%)")
PY
)
pbb=$(grep -oE 'l1_block: [0-9]+' <<<"$clean"|grep -oE '[0-9]+$'|sort -n|uniq); consec=0 gap=0 prev=""
for b in $pbb; do [[ -n "$prev" ]] && { [[ $((b-prev)) -eq 1 ]] && consec=$((consec+1)) || gap=$((gap+1)); }; prev=$b; done
drops=$(grep -c 'target block passed without inclusion' <<<"$clean"); evict=$(grep -c 'evicted after MAX_BUNDLE_ATTEMPTS' <<<"$clean")
div=$(grep -cE 'diverged from L1-confirmed|local L2 state root differs' <<<"$clean")
l1r=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint256)(address,bytes32,uint256)' "$EEZ_ROLLUP_ID" --rpc-url "$L1" 2>/dev/null|sed -n '2p'|tr -d '[:space:]')
l2r=$(cast block safe --rpc-url "$L2" --json 2>/dev/null|jq -r '.stateRoot//empty'); recon=$([[ -n "$l1r" && "${l1r,,}" == "${l2r,,}" ]] && echo PASS || echo FAIL)

echo; echo "════════════════════ RESULTS ($MODE) ════════════════════"
if [[ "$MODE" == load ]]; then
  printf "  INBOUND  submitted=%s accepted=%s SETTLED=%s\n" "$IN_N" "$IN_ACC" "$IN_OK"
  printf "  OUTBOUND submitted=%s accepted=%s SETTLED=%s\n" "$OUT_N" "$OUT_ACC" "$OUT_OK"
  printf "  TOTAL settled: %s/%s\n" "$((IN_OK+OUT_OK))" "$((IN_N+OUT_N))"
fi
echo "  N+1 next-slot hit-rate: $n1   |  postBatch L1 blocks: consecutive=$consec gapped=$gap"
echo "  bundle target-misses(drops)=$drops  evictions(3-strike)=$evict  |  divergence=$div  reconcile=$recon"
echo "═══════════════════════════════════════════════════════════"
[[ "${OK:-1}" == 1 && "$div" -eq 0 && "$recon" == PASS ]] && { echo "✓ PASS (sound: 0 divergence, L1==L2)"; exit 0; } || { echo "✗ CHECK (see above)"; exit 1; }
