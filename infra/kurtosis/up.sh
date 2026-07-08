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
# ethereum-package's standard prefunded dev mnemonic. Indices 1/2 are unused by
# the package itself (0=builder coinbase, 3=tx_fuzz, 12=deployer, 13=spamoor).
DEV_MNEMONIC="giant issue aisle success illegal bike spike question tent bar rely arctic volcano long crawl hungry vocal artwork sniff fantasy very lucky have athlete"

command -v kurtosis >/dev/null || { echo "kurtosis not found in PATH" >&2; exit 1; }
command -v docker   >/dev/null || { echo "docker not found in PATH" >&2; exit 1; }

# First run: create args.yaml from the example so there's nothing to hand-copy.
if [[ ! -f "$ARGS_FILE" ]]; then
    echo "==> $ARGS_FILE not found, creating it from args.example.yaml"
    cp "$HERE/args.example.yaml" "$ARGS_FILE"
fi

# Flat "key: value" lookup out of the args file (strips quotes + comments) so the
# images we build are tagged exactly as main.star will reference them.
yv() { grep -E "^[[:space:]]*$1:" "$ARGS_FILE" | head -1 \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; }

# First run: derive the poster/proof-signer keys from the dev mnemonic instead
# of asking for a manual `cast wallet private-key` + paste step.
for pair in "poster_key:1" "proof_signer_key:2"; do
    key="${pair%%:*}"; index="${pair##*:}"
    if [[ "$(yv "$key")" == "0xCHANGE_ME" ]]; then
        command -v cast >/dev/null || {
            echo "cast not found in PATH — needed to auto-derive eez.$key." >&2
            echo "Install foundry, or set eez.$key in $ARGS_FILE by hand." >&2
            exit 1
        }
        derived="$(cast wallet private-key --mnemonic "$DEV_MNEMONIC" --mnemonic-index "$index")"
        sed -i.bak -E "s|^([[:space:]]*${key}:).*|\\1 \"${derived}\"|" "$ARGS_FILE"
        rm -f "$ARGS_FILE.bak"
        echo "==> derived eez.$key from the dev mnemonic (index $index)"
    fi
done

# Migrate superseded uncustomized timing presets.
if [[ "$(yv l1_block_time_ms)" == "12000" \
   && "$(yv l2_block_time_ms)" == "2000" \
   && "$(yv proof_time_ms)" == "5000" \
   && "$(yv submission_slack_ms)" == "1500" ]]; then
    sed -i.bak -E "s|^([[:space:]]*l2_block_time_ms:).*|\\1 12000|" "$ARGS_FILE"
    rm -f "$ARGS_FILE.bak"
    echo "==> migrated eez.l2_block_time_ms from 2000 to 12000 (bootstrap K=1)"
fi

# The 5000ms proof budget gives rbuilder enough lead without changing block cadence.
if [[ "$(yv l1_block_time_ms)" == "12000" \
   && "$(yv l2_block_time_ms)" == "2000" \
   && "$(yv proof_time_ms)" == "4000" \
   && "$(yv submission_slack_ms)" == "1500" ]]; then
    sed -i.bak -E \
        -e "s|^([[:space:]]*proof_time_ms:).*|\\1 5000|" \
        -e "s|^([[:space:]]*l2_block_time_ms:).*|\\1 12000|" \
        "$ARGS_FILE"
    rm -f "$ARGS_FILE.bak"
    echo "==> migrated eez.proof_time_ms 4000→5000 and l2_block_time_ms 2000→12000"
fi

if [[ "$(yv l1_block_time_ms)" == "12000" \
   && "$(yv l2_block_time_ms)" == "2000" \
   && "$(yv proof_time_ms)" == "7000" \
   && "$(yv submission_slack_ms)" == "1500" ]]; then
    sed -i.bak -E \
        -e "s|^([[:space:]]*proof_time_ms:).*|\\1 5000|" \
        -e "s|^([[:space:]]*l2_block_time_ms:).*|\\1 12000|" \
        "$ARGS_FILE"
    rm -f "$ARGS_FILE.bak"
    echo "==> migrated eez.proof_time_ms 7000→5000 and l2_block_time_ms 2000→12000"
fi

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
