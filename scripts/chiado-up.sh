#!/usr/bin/env bash
#
# Clean chiado bring-up — one command, start to finish:
#   1. deploy the protocol on chiado (skipped if deployments.env already
#      exists — re-running this must not mint new contracts over a live one)
#   2. prepare the L2 + lighthouse datadirs (fresh, since a fresh deploy means
#      a new genesis — old chain data under it would just diverge)
#   3. start the docker compose stack (eez-node + eez-proof-signer + lighthouse)
#   4. wait until the pipeline is healthy: L2 producing AND a batch has
#      actually settled on L1 (not just "the RPCs answer")
#   5. print the RPC URLs + deployed addresses
#
# Config (see README):
#   .env         — deploy inputs: EEZ_L1_RPC_URL (a TIP chiado RPC), keys,
#                  optionally EEZ_BLOCKSCOUT_URL.
#   .env.chiado  — docker host paths + funded keys + bundler URL. On first run
#                  these inputs render the eez-composer config file.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"; cd "$REPO"
COMPOSE=(docker compose --env-file .env.chiado -f docker-compose.chiado-node.yml)

for t in docker cast make; do command -v "$t" >/dev/null || { echo "✗ $t not in PATH"; exit 1; }; done
[[ -f .env ]]        || { echo "✗ .env missing — cp .env.example .env and fill it in first"; exit 1; }
[[ -f .env.chiado ]] || { echo "✗ .env.chiado missing — cp .env.chiado.example .env.chiado and fill it in first"; exit 1; }

set -a; source .env; source .env.chiado; set +a
for v in L1_SNAPSHOT_DIR JWT_FILE CHIADO_CONFIG_DIR; do
    [[ -e "${!v:-}" ]] || { echo "✗ $v=${!v:-<unset>} missing — finish the One-time setup steps first"; exit 1; }
done
: "${L2_DATA_DIR:?set L2_DATA_DIR in .env.chiado}"
: "${LIGHTHOUSE_DATA:?set LIGHTHOUSE_DATA in .env.chiado}"
: "${EEZ_COMPOSER_CONFIG:?set EEZ_COMPOSER_CONFIG in .env.chiado}"

# ── 1. Deploy the protocol ─────────────────────────────────────────────
if [[ -f deployments.env ]]; then
    echo "==> [1/5] deployments.env already present — skipping deploy (rm it first to redeploy)"
    fresh_deploy=0
else
    echo "==> [1/5] deploying protocol on chiado"
    EEZ_DEPLOY_SKIP_SIMULATION="${EEZ_DEPLOY_SKIP_SIMULATION:-1}" make deploy-protocol
    fresh_deploy=1
fi
set -a; source deployments.env; set +a
: "${EEZ_REGISTRY_ADDRESS:?deploy did not write EEZ_REGISTRY_ADDRESS}"

if [[ ! -f "$EEZ_COMPOSER_CONFIG" ]]; then
    echo "    rendering $EEZ_COMPOSER_CONFIG"
    mkdir -p "$(dirname "$EEZ_COMPOSER_CONFIG")"
    (
      umask 077
      cat > "$EEZ_COMPOSER_CONFIG" <<EOF
l2_system_key = "$EEZ_L2_SYSTEM_KEY"
expect_external_batches = true
max_speculative_depth = 0

[l1]
rpc_url = "http://127.0.0.1:18645"
chain_id = 10200
registry_address = "$EEZ_REGISTRY_ADDRESS"
registry_deploy_block = $EEZ_REGISTRY_DEPLOY_BLOCK
rollup_id = $EEZ_ROLLUP_ID

[timing]
l1_block_time_ms = 5000
l2_block_time_ms = 1000
proof_time_ms = ${EEZ_PROOF_TIME_MS:-2500}
submission_slack_ms = ${EEZ_SUBMISSION_SLACK_MS:-1300}

[prover]
url = "http://127.0.0.1:${EEZ_SIGNER_PORT:-50061}"
attester_address = "$EEZ_ATTESTER_ADDRESS"

[submission]
builder_rpc_url = "$EEZ_L1_BUILDER_RPC_URL"
poster_key = "$EEZ_L1_POSTER_KEY"
proof_system_address = "$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS"

[cross_chain]
l1_port = 18999
l2_port = 18998

[embedded_l1]
kind = "chiado"
datadir = "/l1"
http_port = 18645
auth_port = ${L1_AUTH_PORT:-18551}
p2p_port = 30544
jwt_secret = "/jwt/jwt.hex"

[limits]
max_user_txs_per_bundle = ${EEZ_MAX_USER_TXS_PER_BUNDLE:-3}
EOF
    )
    if [[ -n "${EEZ_L1_TARGET_RPC_URL:-}" ]]; then
        sed -i.bak "/^poster_key/i\\
target_rpc_url = \"$EEZ_L1_TARGET_RPC_URL\"" "$EEZ_COMPOSER_CONFIG"
        rm -f "$EEZ_COMPOSER_CONFIG.bak"
    fi
fi

# ── 2. Prepare datadirs ─────────────────────────────────────────────────
echo "==> [2/5] preparing datadirs"
if [[ "$fresh_deploy" == 1 ]]; then
    echo "    fresh deploy → wiping L2/lighthouse datadirs (new genesis, old chain data would diverge)"
    rm -rf "$L2_DATA_DIR" "$LIGHTHOUSE_DATA"
fi
mkdir -p "$L2_DATA_DIR" "$LIGHTHOUSE_DATA"

# ── 3. Start the stack ──────────────────────────────────────────────────
echo "==> [3/5] docker compose up (eez-node + eez-proof-signer + lighthouse)"
"${COMPOSE[@]}" up -d --remove-orphans
"${COMPOSE[@]}" ps

# ── 4. Wait for a healthy pipeline ──────────────────────────────────────
echo "==> [4/5] waiting for L1 catch-up + L2 production + first settle (up to ~15 min)"
L1=http://localhost:18645
L2=http://localhost:18688
GEN_ROOT="${EEZ_INITIAL_STATE_ROOT:-}"
healthy=0
for i in $(seq 1 90); do
    l2h=$(cast block-number --rpc-url "$L2" 2>/dev/null || echo 0)
    l1r=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' "${EEZ_ROLLUP_ID:-1}" \
            --rpc-url "$L1" 2>/dev/null | sed -n '2p' | tr -d '[:space:]' || echo "")
    echo "    [$i] L2 head=$l2h  L1 settled root=${l1r:0:14}…"
    if [[ "$l2h" -gt 3 && -n "$l1r" && "$l1r" != "$GEN_ROOT" ]]; then
        healthy=1; echo "    ✓ pipeline healthy"; break
    fi
    sleep 10
done
[[ "$healthy" == 1 ]] || { echo "✗ not healthy in window; check: ${COMPOSE[*]} logs --tail=100 eez-node"; exit 1; }

# ── 5. Report ────────────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════════════════════════"
echo " chiado L2 is UP"
echo "════════════════════════════════════════════════════════════════"
echo " RPC endpoints:"
echo "   L2 RPC                          $L2"
echo "   Embedded chiado L1 RPC          $L1"
echo "   L1→L2 front (Inbound)           http://localhost:18999"
echo "   L2→L1 front (Outbound)          http://localhost:18998"
echo
echo " Deployment (rollupId ${EEZ_ROLLUP_ID:-1}, deploy block ${EEZ_REGISTRY_DEPLOY_BLOCK:-?}):"
printf "   %-20s %s\n" "EEZ registry"      "$EEZ_REGISTRY_ADDRESS"
printf "   %-20s %s\n" "Rollup manager"    "${EEZ_ROLLUP_MANAGER_ADDRESS:-?}"
printf "   %-20s %s\n" "ECDSA proof system" "${EEZ_ECDSA_PROOF_SYSTEM_ADDRESS:-?}"
printf "   %-20s %s\n" "L1 bridge sender"  "${EEZ_L1_BRIDGE_SENDER:-?}"
if [[ -n "${EEZ_BLOCKSCOUT_URL:-}" ]]; then
    printf "   Blockscout: %s/address/%s\n" "${EEZ_BLOCKSCOUT_URL%/}" "$EEZ_REGISTRY_ADDRESS"
fi
echo
echo " Next:"
echo "   ${COMPOSE[*]} logs -f eez-node          # follow the node"
echo "   EEZ_WAVE_COUNT=5 bash scripts/xchain-test.sh   # exercise cross-chain"
echo "   bash scripts/teardown-chiado.sh                # stop the stack"
echo "════════════════════════════════════════════════════════════════"
