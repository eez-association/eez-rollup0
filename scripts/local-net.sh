#!/usr/bin/env bash
#
# Fully-local eez network — no Chiado, no docker, no anvil.
# Mirrors the e2e DevnetCfg (crates/eez-node/tests/common/mod.rs): ONE
# eez-node process in composer mode boots the embedded dev L1 (fixture
# genesis, chainId 1337, 5s auto-mine, mock eth_sendBundle) AND the L2
# (chainId 1, 1s blocks) plus both cross-chain ingress fronts. Contracts
# deploy AFTER boot onto the embedded L1 at deterministic
# CREATE(deployer, 0/1/2) addresses, which the node env references
# before they exist.
#
#   scripts/local-net.sh up        # build + boot composer node (fresh chain)
#   scripts/local-net.sh deploy    # deploy protocol onto the embedded L1
#   scripts/local-net.sh wave      # smoke: full bidirectional cross-chain matrix
#   scripts/local-net.sh follower  # boot a follower deriving purely from L1
#   scripts/local-net.sh status    # heads, batch counts, reconcile check
#   scripts/local-net.sh down      # kill everything
#
# State lives in EEZ_NET_DIR (default /tmp/eez-local-net; put it on a big
# disk if you keep it running). Requires: foundry (cast/forge), jq, python3.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
NET="${EEZ_NET_DIR:-/tmp/eez-local-net}"
L1=http://127.0.0.1:18545; L2=http://127.0.0.1:18688
L1F=http://127.0.0.1:18999; L2F=http://127.0.0.1:18998
FOLLOWER=http://127.0.0.1:18788

# Hardhat keys: #0 poster + L2 system signer, #1 deployer + proof signer,
# #2 inbound user, #3 target-contract deployer, #4 outbound user.
HH0=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
HH1=0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d
HH2=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
HH3=0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6
HH4=0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a
HH2_ADDR=0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC
HH4_ADDR=0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65

# CREATE(hardhat#1, nonce 0/1/2) — deploy order EEZ, MockPS, Rollup.
EEZ_ADDR=0x8464135c8F25Da09e49BC8782676a84730C318bC
MOCK_PS=0x71C95911E9a5D330f4D621842EC243EE1343292e
ROLLUP_MGR=0x948B3c65b89DF0B4894ABE91E6D02FE579834F8F
CCM=0x4200000000000000000000000000000000000007

bin() { echo "${CARGO_TARGET_DIR:-$REPO/target}/debug/eez-node"; }

node_env() {
    # Mirrors DevnetCfg::env(). EEZ_L1_RPC_URL round-trips into the
    # node's own embedded L1; the same URL serves as the "builder".
    cat <<EOF
export EEZ_L1_EMBEDDED=1
export EEZ_L1_CHAIN=testing
export EEZ_L1_CHAIN_ID=1337
export EEZ_L1_RPC_URL=$L1
export EEZ_L1_BUILDER_RPC_URL=$L1
export EEZ_L1_XCHAIN_PORT=18999
export EEZ_L2_XCHAIN_PORT=18998
export EEZ_L1_HTTP_PORT=18545
export EEZ_L1_AUTH_PORT=18551
export EEZ_L1_P2P_PORT=30444
export EEZ_L1_DISCV5_PORT=30454
export EEZ_L1_DATADIR=$NET/l1-data
export EEZ_L1_CHAIN_PATH=$NET/l1-genesis.json
export EEZ_L1_POSTER_KEY=$HH0
export EEZ_PROOF_SIGNER_KEY=$HH1
export EEZ_L2_SYSTEM_KEY=$HH0
export EEZ_CCM_L2_ADDRESS=$CCM
export EEZ_L1_BLOCK_TIME_MS=5000
export EEZ_L2_BLOCK_TIME_MS=1000
export EEZ_PROOF_TIME_MS=1000
export EEZ_SUBMISSION_SLACK_MS=100
export EEZ_REGISTRY_ADDRESS=$EEZ_ADDR
export EEZ_REGISTRY_DEPLOY_BLOCK=0
export EEZ_MOCK_PROOF_SYSTEM_ADDRESS=$MOCK_PS
export EEZ_ROLLUP_MANAGER_ADDRESS=$ROLLUP_MGR
export EEZ_ROLLUP_ID=1
export EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES=false
export EEZ_L2_RPC_URL=$L2
export RUST_LOG=\${RUST_LOG:-warn,eez_node=info,eez_l1=info,eez_composer=info,eez_driver=info,eez_deriver=info}
EOF
}

stamp_genesis() {
    # Fixture genesis restamped to wall-clock now: the sequencer's
    # defer-on-lateness gate reads a stale genesis as perpetually late.
    # Timestamp is a header field — the state root (= registered
    # initialState) is unchanged.
    python3 - "$REPO" "$NET" <<'EOF'
import json, sys, time
repo, net = sys.argv[1], sys.argv[2]
g = json.load(open(f'{repo}/crates/eez-node/tests/fixtures/genesis.json'))
g['timestamp'] = hex(int(time.time()))
json.dump(g, open(f'{net}/l2-genesis.json', 'w'))
g['config']['chainId'] = 1337
json.dump(g, open(f'{net}/l1-genesis.json', 'w'))
EOF
}

wait_rpc() { # <url> <timeout_s>
    timeout "$2" bash -c "until cast block-number --rpc-url $1 >/dev/null 2>&1; do sleep 2; done" \
        || { echo "RPC $1 did not come up in $2 s" >&2; exit 1; }
}

up() {
    command -v cast >/dev/null || { echo "foundry not installed" >&2; exit 1; }
    ( cd "$REPO" && cargo build -p eez-node )
    mkdir -p "$NET"; rm -rf "$NET/l1-data" "$NET/l2-data"
    stamp_genesis
    # cwd must NOT have a .env on its ancestor path: dotenvy walks up and
    # a stray EEZ_PROOF_SIGNER_KEY silently flips follower→composer mode.
    { node_env; cat <<EOF
cd $NET
exec $(bin) node \
  --chain $NET/l2-genesis.json --datadir $NET/l2-data \
  --http --http.addr 127.0.0.1 --http.port 18688 \
  --ws.port 18689 --authrpc.port 18651 \
  --port 30388 --disable-discovery --ipcdisable
EOF
    } > "$NET/composer-cmd.sh"
    tmux kill-session -t eez-net 2>/dev/null || true
    tmux new-session -d -s eez-net "bash $NET/composer-cmd.sh 2>&1 | tee $NET/composer.log"
    wait_rpc $L1 120; wait_rpc $L2 120
    echo "up: L1=$L1 L2=$L2 fronts=$L1F/$L2F (tmux: eez-net). Next: scripts/local-net.sh deploy"
}

deploy() {
    # deploy.sh derives the initial state root from the live L2's block 0
    # (EEZ_L2_RPC_URL) — that root must match what registerRollup pins.
    cat > "$NET/deploy.env" <<EOF
EEZ_L1_RPC_URL=$L1
EEZ_L1_POSTER_KEY=$HH0
EEZ_DEPLOY_KEY=$HH1
EEZ_PROOF_SIGNER_KEY=$HH1
EEZ_L2_RPC_URL=$L2
EOF
    EEZ_ENV_FILE="$NET/deploy.env" EEZ_DEPLOYMENTS_FILE="$NET/deployments.env" \
        EEZ_GENESIS_OUT="$NET/deploy-genesis-unused.json" bash "$REPO/scripts/deploy.sh"
    grep -q "$(echo $EEZ_ADDR | tr 'A-F' 'a-f')" "$NET/deployments.env" \
        || { echo "FATAL: EEZ landed off-prediction — was the L1 chain not fresh?" >&2; exit 1; }
    echo "deploy: addresses match predictions. Next: scripts/local-net.sh wave"
}

send_front() { # <front> <key> <nonce> <gas> <value> <to> [sig args...]
    local front=$1 key=$2 nonce=$3 gas=$4 val=$5 to=$6; shift 6
    cast send --rpc-url "$front" --private-key "$key" --nonce "$nonce" \
        --gas-limit "$gas" --gas-price 100000000000 --priority-gas-price 1000000000 \
        --value "$val" --async "$to" "$@"
}

wave() {
    export FOUNDRY_DISABLE_NIGHTLY_WARNING=1
    local C="$REPO/contracts" v=$((RANDOM % 1000 + 1000)) dep=1000000000000000
    local DEP_R WD_R
    DEP_R=0x$(python3 -c 'import secrets;print(secrets.token_hex(20))')
    WD_R=0x$(python3 -c 'import secrets;print(secrets.token_hex(20))')
    echo "wave: v=$v deposit=$dep dep_recipient=$DEP_R wd_recipient=$WD_R"

    local L2V L1V ISP IDP OSP OWP
    L2V=$(cd "$C" && forge create src/Value.sol:Value --rpc-url $L2 --private-key $HH3 --broadcast --json --constructor-args 0 | jq -r .deployedTo)
    L1V=$(cd "$C" && forge create src/Value.sol:Value --rpc-url $L1 --private-key $HH3 --broadcast --json --constructor-args 0 | jq -r .deployedTo)
    ISP=$(cast send $EEZ_ADDR 'createCrossChainProxy(address,uint256)' "$L2V" 1 --rpc-url $L1 --private-key $HH1 --json | jq -r '.logs[0].topics[1]' | sed 's/0x000000000000000000000000/0x/')
    IDP=$(cast send $EEZ_ADDR 'createCrossChainProxy(address,uint256)' "$DEP_R" 1 --rpc-url $L1 --private-key $HH1 --json | jq -r '.logs[0].topics[1]' | sed 's/0x000000000000000000000000/0x/')
    OSP=$(cast call $CCM 'computeCrossChainProxyAddress(address,uint256)(address)' "$L1V" 0 --rpc-url $L2 | tr -d '[:space:]')
    cast send $CCM 'createCrossChainProxy(address,uint256)' "$L1V" 0 --rpc-url $L2 --private-key $HH3 --json >/dev/null
    OWP=$(cast call $CCM 'computeCrossChainProxyAddress(address,uint256)(address)' "$WD_R" 0 --rpc-url $L2 | tr -d '[:space:]')
    cast send $CCM 'createCrossChainProxy(address,uint256)' "$WD_R" 0 --rpc-url $L2 --private-key $HH3 --json >/dev/null

    local n1 n2
    n1=$(cast nonce $HH2_ADDR --rpc-url $L1); n2=$(cast nonce $HH4_ADDR --rpc-url $L2)
    send_front $L1F $HH2 "$n1"       600000 0    "$ISP" 'setValue(uint256)' "$v" >/dev/null
    send_front $L1F $HH2 "$((n1+1))" 600000 $dep "$IDP" >/dev/null
    send_front $L2F $HH4 "$n2"       900000 0    "$OSP" 'setValue(uint256)' "$v" >/dev/null
    send_front $L2F $HH4 "$((n2+1))" 900000 $dep "$OWP" >/dev/null
    echo "wave: 4 txs held by the fronts; polling convergence (usually <30s)"

    for i in $(seq 1 30); do
        sleep 5
        local vl2 vl1 db wb
        vl2=$(cast call "$L2V" 'value()(uint256)' --rpc-url $L2); vl1=$(cast call "$L1V" 'value()(uint256)' --rpc-url $L1)
        db=$(cast balance "$DEP_R" --rpc-url $L2); wb=$(cast balance "$WD_R" --rpc-url $L1)
        echo "  t=$((i*5))s L2val=$vl2 L1val=$vl1 dep=$db wd=$wb"
        if [[ "$vl2" == "$v" && "$vl1" == "$v" && "$db" == "$dep" && "$wb" == "$dep" ]]; then
            echo "wave: CONVERGED — inbound + outbound setters and value transfers all settled"
            return 0
        fi
    done
    echo "wave: FAILED to converge in 150s — check $NET/composer.log" >&2; exit 1
}

follower() {
    rm -rf "$NET/follower-data"; mkdir -p "$NET/follower-data"
    # No EEZ_PROOF_SIGNER_KEY → follower mode. cwd = its own datadir so
    # dotenvy can't pick up the deploy env from an ancestor directory.
    { node_env | grep -v -e EEZ_PROOF_SIGNER_KEY -e EEZ_L1_XCHAIN_PORT -e EEZ_L2_XCHAIN_PORT \
        -e EEZ_L1_EMBEDDED -e EEZ_L1_HTTP_PORT -e EEZ_L1_AUTH_PORT -e EEZ_L1_CHAIN= -e EEZ_L1_CHAIN_PATH -e EEZ_L1_DATADIR
      echo "export EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES=true"
      cat <<EOF
cd $NET/follower-data
exec $(bin) node \
  --chain $NET/l2-genesis.json --datadir $NET/follower-data \
  --http --http.addr 127.0.0.1 --http.port 18788 \
  --ws.port 18789 --authrpc.port 18751 \
  --port 30389 --disable-discovery --ipcdisable
EOF
    } > "$NET/follower-cmd.sh"
    tmux kill-session -t eez-follower 2>/dev/null || true
    tmux new-session -d -s eez-follower "bash $NET/follower-cmd.sh 2>&1 | tee $NET/follower.log"
    wait_rpc $FOLLOWER 180
    echo "follower: deriving from L1 at $FOLLOWER; compare with: scripts/local-net.sh status"
}

status() {
    echo "L1: $(cast block-number --rpc-url $L1 2>/dev/null || echo down)  composer L2: $(cast block-number --rpc-url $L2 2>/dev/null || echo down)  follower: $(cast block-number --rpc-url $FOLLOWER 2>/dev/null || echo 'not running')"
    local from
    from=$(grep EEZ_REGISTRY_DEPLOY_BLOCK "$NET/deployments.env" 2>/dev/null | cut -d= -f2); from=${from:-1}
    echo "BatchPosted: $(cast logs --rpc-url $L1 --from-block "$from" --address $EEZ_ADDR 'BatchPosted(uint256)' 2>/dev/null | grep -c blockNumber)  ImmediateEntrySkipped: $(cast logs --rpc-url $L1 --from-block "$from" --address $EEZ_ADDR 'ImmediateEntrySkipped(uint256,bytes)' 2>/dev/null | grep -c blockNumber)"
    local l1r l2r
    l1r=$(cast call $EEZ_ADDR 'rollups(uint256)(address,bytes32,uint256)' 1 --rpc-url $L1 2>/dev/null | sed -n 2p | tr -d '[:space:]')
    l2r=$(cast block safe --rpc-url $L2 --json 2>/dev/null | jq -r '.stateRoot // empty')
    echo "reconcile: L1-stored=$l1r"
    echo "           L2-safe  =$l2r"
    if [[ -n "$(cast block-number --rpc-url $FOLLOWER 2>/dev/null || true)" ]]; then
        local cs fs
        cs=$(cast block safe --rpc-url $L2 --json | jq -r .number); fs=$(cast block safe --rpc-url $FOLLOWER --json | jq -r .number)
        echo "safe heads: composer=$cs follower=$fs $([[ "$cs" == "$fs" ]] && echo MATCH)"
    fi
    echo "divergence log lines: $(grep -cE 'diverged from L1-confirmed|local L2 state root differs' "$NET/composer.log" 2>/dev/null || echo 0)"
}

verdict() {
    # Machine-checkable session verdict from the composer log + chain
    # state. Prints a fixed RESULTS block and exits 0 (PASS) / 1 (FAIL).
    # Criteria: 0 divergence, 0 skipped entries, 0 bundle drops, exact
    # L1↔L2 root reconcile, N+1 next-slot hit-rate ≥ 90%.
    local metrics l1r l2r skipped from recon
    metrics=$(sed -r 's/\x1b\[[0-9;]*[mK]//g' "$NET/composer.log" | python3 -c '
import re, sys
log = sys.stdin.read()
tgt = {}
for l in log.splitlines():
    if "dispatching bundle to builder" in l:
        h = re.search(r"tx_hash=(0x[0-9a-f]{64})", l); b = re.search(r"target_block=(\d+)", l)
        if h and b: tgt[h.group(1)] = int(b.group(1))
hit = chk = 0
for l in log.splitlines():
    if "bundle outcome observed" in l and "Included" in l:
        h = re.search(r"tx_hash: (0x[0-9a-f]{64})", l); b = re.search(r"l1_block: (\d+)", l)
        if h and b and h.group(1) in tgt:
            chk += 1; hit += int(b.group(1)) == tgt[h.group(1)]
drops = log.count("target block passed without inclusion")
evict = log.count("evicting")
div = len(re.findall(r"diverged from L1-confirmed|local L2 state root differs", log))
rate = 100 if not chk else round(100 * hit / chk)
print(f"{hit} {chk} {rate} {drops} {evict} {div}")')
    read -r hit chk rate drops evict div <<< "$metrics"
    from=$(grep EEZ_REGISTRY_DEPLOY_BLOCK "$NET/deployments.env" 2>/dev/null | cut -d= -f2); from=${from:-1}
    skipped=$(cast logs --rpc-url $L1 --from-block "$from" --address $EEZ_ADDR 'ImmediateEntrySkipped(uint256,bytes)' 2>/dev/null | grep -c blockNumber || true)
    l1r=$(cast call $EEZ_ADDR 'rollups(uint256)(address,bytes32,uint256)' 1 --rpc-url $L1 2>/dev/null | sed -n 2p | tr -d '[:space:]')
    l2r=$(cast block safe --rpc-url $L2 --json 2>/dev/null | jq -r '.stateRoot // empty')
    recon=$([[ -n "$l1r" && "${l1r,,}" == "${l2r,,}" ]] && echo PASS || echo FAIL)
    echo "════════════════ LOCAL-NET VERDICT ════════════════"
    echo "  N+1 next-slot hit-rate : $hit/$chk (${rate}%)   [≥90% required]"
    echo "  bundle drops           : $drops                 [0 required]"
    echo "  evictions              : $evict                 [poison-only expected]"
    echo "  state divergence       : $div                   [0 required]"
    echo "  ImmediateEntrySkipped  : $skipped               [0 required]"
    echo "  L1↔L2 root reconcile   : $recon                 [exact match required]"
    echo "═══════════════════════════════════════════════════"
    if [[ "$div" == 0 && "$skipped" == 0 && "$drops" == 0 && "$recon" == PASS && "$rate" -ge 90 ]]; then
        echo "✓ PASS"; exit 0
    else
        echo "✗ FAIL — see $NET/composer.log"; exit 1
    fi
}

down() {
    tmux kill-session -t eez-net 2>/dev/null || true
    tmux kill-session -t eez-follower 2>/dev/null || true
    echo "down. State kept in $NET (rm -rf it for a clean slate)."
}

case "${1:-}" in
    up|deploy|wave|follower|status|verdict|down) "$1" ;;
    *) sed -n '3,20p' "$0"; exit 1 ;;
esac
