#!/usr/bin/env bash
# Bring up the WHOLE L1 pair (Pair B) and deploy the EEZ protocol onto it.
#
# This is one of the two developer entry points for the private cross-chain
# devnet (the other is eez-up.sh, which starts the eez node pair / Pair A).
# It runs the granular scripts in scripts/ in order — each still exists on its
# own for debugging, but you shouldn't need to call them by hand:
#
#   1. kurtosis-up      Kurtosis enclave: reth + validators + rbuilder + relay
#                       + spamoor. Generates the ONE shared genesis.
#   2. parse-endpoints  Discover dynamic EL RPC + rbuilder-rpc  -> endpoints.env
#   3. extract-genesis  Pull the shared genesis + mint a local JWT -> eez-l1-data/
#   4. get-cl-bootnode  A Kurtosis CL libp2p multiaddr (follower peers to it)
#   5. get-el-bootnode  A Kurtosis reth enode (embedded EL backfills history)
#   6. deploy-eez       Deploy registry + proof system + L2 genesis -> deployments.env
#
# When this finishes, everything eez-up.sh needs is on disk. Then run eez-up.sh.
#
# Usage:  bash infra/kurtosis/l1-up.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
S="$HERE/scripts"
ENV_FILE="${KURTOSIS_ENV_FILE:-$HERE/.env}"

if [[ ! -f "$ENV_FILE" ]]; then
    cat >&2 <<EOF
missing $ENV_FILE

The deploy step needs your poster/proof keys. Create it first:
  cp infra/kurtosis/eez.env.example infra/kurtosis/.env
  \$EDITOR infra/kurtosis/.env   # set EEZ_L1_POSTER_KEY / EEZ_PROOF_SIGNER_KEY
EOF
    exit 1
fi

step() { echo; echo "════════════════════════════════════════"; echo "  $*"; echo "════════════════════════════════════════"; }

step "1/6  Kurtosis L1 enclave (reth + validators + rbuilder + relay + spamoor)"
bash "$S/kurtosis-up.sh"

step "2/6  Discovering dynamic endpoints (EL RPC + rbuilder-rpc)"
bash "$S/parse-endpoints.sh"

step "3/6  Extracting shared genesis + minting local JWT"
bash "$S/extract-genesis.sh"

step "4/6  Fetching a Kurtosis CL bootnode (for the follower beacon)"
bash "$S/get-cl-bootnode.sh"

step "5/6  Fetching a Kurtosis reth enode (for embedded-EL backfill)"
bash "$S/get-el-bootnode.sh"

step "6/6  Deploying EEZ contracts + L2 genesis onto the shared L1"
bash "$S/deploy-eez.sh"

cat <<EOF

════════════════════════════════════════
  L1 pair is up and the protocol is deployed.
════════════════════════════════════════
Next: start the eez node pair (Pair A):
  bash infra/kurtosis/eez-up.sh

Tear everything down with:
  bash infra/kurtosis/down.sh
EOF
