#!/usr/bin/env bash
# Build the candidate images and start the CI test network.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
ARGS_FILE="${1:-$HERE/ci-args.yaml}"

command -v kurtosis >/dev/null || { echo "kurtosis not found in PATH" >&2; exit 1; }
command -v docker   >/dev/null || { echo "docker not found in PATH" >&2; exit 1; }

PROTOCOL_DIR="$REPO/sync-rollups-protocol"

# Fail before building images if the protocol submodule is missing or stale.
if [[ ! -d "$PROTOCOL_DIR/.git" && ! -f "$PROTOCOL_DIR/.git" ]]; then
    echo "sync-rollups-protocol submodule is not initialized." >&2
    echo "Run: git submodule update --init --recursive sync-rollups-protocol" >&2
    exit 1
fi

if ! grep -q "ExpectedLookup\\[\\] expectedLookups" "$PROTOCOL_DIR/src/interfaces/IEEZ.sol" 2>/dev/null \
    || ! grep -q "expectedStateRoots" "$PROTOCOL_DIR/src/interfaces/IEEZ.sol" 2>/dev/null
then
    echo "sync-rollups-protocol is too old for this eez-node checkout." >&2
    echo "Missing postAndVerifyBatch ABI fields expected by this checkout." >&2
    echo "Run: git submodule update --init --recursive sync-rollups-protocol" >&2
    echo "Current submodule status:" >&2
    git -C "$REPO" submodule status sync-rollups-protocol >&2 || true
    exit 1
fi

echo "==> protocol submodule: $(git -C "$PROTOCOL_DIR" rev-parse --short HEAD)"

[[ -f "$ARGS_FILE" ]] || { echo "Kurtosis args not found: $ARGS_FILE" >&2; exit 1; }

# Flat key lookup for this simple args template.
yv() { grep -E "^[[:space:]]*$1:" "$ARGS_FILE" | head -1 \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; }

NODE_IMAGE="$(yv eez_node_image)";                NODE_IMAGE="${NODE_IMAGE:-eez-node:dev}"
PROOF_SIGNER_IMAGE="$(yv proof_signer_image)";    PROOF_SIGNER_IMAGE="${PROOF_SIGNER_IMAGE:-eez-proof-signer:dev}"
DEPLOY_IMAGE="$(yv deploy_image)";                DEPLOY_IMAGE="${DEPLOY_IMAGE:-eez-deploy:dev}"

export DOCKER_BUILDKIT=1

# Fast local build; set EEZ_OPTIMIZED_BUILD=1 for the full release profile.
release_build_args=()
if [[ "${EEZ_OPTIMIZED_BUILD:-0}" != "1" ]]; then
    release_build_args=(
        --build-arg CARGO_PROFILE_RELEASE_LTO=false
        --build-arg CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
        --build-arg CARGO_PROFILE_RELEASE_DEBUG=0
    )
fi

if [[ "${EEZ_SKIP_NODE_BUILD:-0}" != "1" ]]; then
    echo "==> building $NODE_IMAGE (fast CI profile)"
    docker build "${release_build_args[@]}" -t "$NODE_IMAGE" "$REPO"
fi

if [[ "${EEZ_SKIP_PROOF_SIGNER_BUILD:-0}" != "1" ]]; then
    echo "==> building $PROOF_SIGNER_IMAGE (fast CI profile)"
    docker build "${release_build_args[@]}" \
        -f "$REPO/Dockerfile.signer" \
        -t "$PROOF_SIGNER_IMAGE" "$REPO"
else
    echo "==> reusing $PROOF_SIGNER_IMAGE (EEZ_SKIP_PROOF_SIGNER_BUILD=1)"
fi

if [[ "${EEZ_SKIP_DEPLOY_BUILD:-0}" != "1" ]]; then
    echo "==> building $DEPLOY_IMAGE (foundry + contracts)"
    docker build -f "$HERE/Dockerfile.deploy" -t "$DEPLOY_IMAGE" "$REPO"
else
    echo "==> reusing $DEPLOY_IMAGE (EEZ_SKIP_DEPLOY_BUILD=1)"
fi

if [[ "${EEZ_PRUNE_BUILD_CACHE:-0}" == "1" ]]; then
    echo "==> pruning Docker build cache"
    docker builder prune --all --force
fi

echo "==> kurtosis run (enclave: $ENCLAVE)"
kurtosis_flags=()
if [[ "${KURTOSIS_PRIVILEGED:-1}" == "1" ]]; then
    kurtosis_flags+=(--privileged)
fi
kurtosis run "${kurtosis_flags[@]}" --enclave "$ENCLAVE" "$HERE" --args-file "$ARGS_FILE"

cat <<EOF

════════════════════════════════════════
  EEZ CI test network is up.
════════════════════════════════════════
Inspect  : kurtosis enclave inspect $ENCLAVE
Node log : kurtosis service logs -f $ENCLAVE eez-node
Signer log: kurtosis service logs -f $ENCLAVE eez-proof-signer
Tear down: bash testing/kurtosis/stop.sh
EOF
