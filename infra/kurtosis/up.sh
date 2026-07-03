#!/usr/bin/env bash
# Bring up the WHOLE EEZ cross-chain devnet in one enclave: Pair B
# (ethereum-package: L1 + rbuilder + relay + spamoor) AND Pair A (eez-node +
# follower). Builds the local images, then `kurtosis run`. One command.
#
# eez-node is BUILT from the repo Dockerfile (tagged per eez.eez_node_image in
# the args file, default eez-node:dev). Release-image pull support will be added
# later; until then everything is built locally.
#
# Usage:
#   bash infra/kurtosis/up.sh [args-file]        # default: args.yaml
#
# Env knobs:
#   EEZ_SKIP_NODE_BUILD=1    reuse an existing eez-node image (skip the slow build)
#   EEZ_SKIP_DEPLOY_BUILD=1  reuse an existing eez-deploy image
#   KURTOSIS_ENCLAVE=name    enclave name (default: eez-devnet)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
ARGS_FILE="${1:-$HERE/args.yaml}"

command -v kurtosis >/dev/null || { echo "kurtosis not found in PATH" >&2; exit 1; }
command -v docker   >/dev/null || { echo "docker not found in PATH" >&2; exit 1; }

if [[ ! -f "$ARGS_FILE" ]]; then
    cat >&2 <<EOF
missing args file: $ARGS_FILE
  cp $HERE/args.example.yaml $HERE/args.yaml
  \$EDITOR $HERE/args.yaml   # set eez.poster_key / eez.proof_signer_key
EOF
    exit 1
fi

# Flat "key: value" lookup out of the args file (strips quotes + comments) so the
# images we build are tagged exactly as main.star will reference them.
yv() { grep -E "^[[:space:]]*$1:" "$ARGS_FILE" | head -1 \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; }
NODE_IMAGE="$(yv eez_node_image)";  NODE_IMAGE="${NODE_IMAGE:-eez-node:dev}"
DEPLOY_IMAGE="$(yv deploy_image)";  DEPLOY_IMAGE="${DEPLOY_IMAGE:-eez-deploy:dev}"

export DOCKER_BUILDKIT=1

if [[ "${EEZ_SKIP_NODE_BUILD:-0}" != "1" ]]; then
    echo "==> building $NODE_IMAGE (repo Dockerfile, fast devnet profile)"
    # Fast profile for a test node: parallel codegen + no LTO + no debug info.
    # Cuts the final compile from Cargo.toml's production release profile
    # (codegen-units=1, lto=thin) by a lot, and shrinks disk use. Set
    # EEZ_OPTIMIZED_BUILD=1 to build the full optimized binary instead.
    node_build_args=()
    if [[ "${EEZ_OPTIMIZED_BUILD:-0}" != "1" ]]; then
        node_build_args=(
            --build-arg CARGO_PROFILE_RELEASE_LTO=false
            --build-arg CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
            --build-arg CARGO_PROFILE_RELEASE_DEBUG=0
        )
    fi
    docker build "${node_build_args[@]}" -t "$NODE_IMAGE" "$REPO"
fi

if [[ "${EEZ_SKIP_DEPLOY_BUILD:-0}" != "1" ]]; then
    echo "==> building $DEPLOY_IMAGE (foundry + contracts)"
    docker build -f "$HERE/deploy.Dockerfile" -t "$DEPLOY_IMAGE" "$REPO"
fi

echo "==> kurtosis run (enclave: $ENCLAVE)"
# --privileged: disruptoor needs it for the CL P2P partition (reorg-scheduler.sh).
kurtosis run --privileged --enclave "$ENCLAVE" "$HERE" --args-file "$ARGS_FILE"

cat <<EOF

════════════════════════════════════════
  EEZ cross-chain devnet is up.
════════════════════════════════════════
Inspect  : kurtosis enclave inspect $ENCLAVE
Node log : kurtosis service logs -f $ENCLAVE eez-node
Reorgs   : bash infra/kurtosis/scripts/reorg-scheduler.sh   (drives disruptoor)
Tear down: bash infra/kurtosis/down.sh
EOF
