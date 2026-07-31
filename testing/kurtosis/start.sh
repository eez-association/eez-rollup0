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

NODE_IMAGE="$(yv eez_node_image)";  NODE_IMAGE="${NODE_IMAGE:-eez-node:dev}"
PROVER_IMAGE="$(yv prover_image)";  PROVER_IMAGE="${PROVER_IMAGE:-eez-proverd:dev}"
DEPLOY_IMAGE="$(yv deploy_image)";  DEPLOY_IMAGE="${DEPLOY_IMAGE:-eez-deploy:dev}"

export DOCKER_BUILDKIT=1

reclaim_ci_builder_cache() {
    if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
        echo "==> reclaiming unused BuildKit cache on the ephemeral CI runner"
        docker builder prune --force
    fi
}

if [[ "${EEZ_SKIP_NODE_BUILD:-0}" != "1" ]]; then
    echo "==> building $NODE_IMAGE (fast CI profile)"
    # Fast local build; set EEZ_OPTIMIZED_BUILD=1 for the full release profile.
    node_build_args=()
    if [[ "${EEZ_OPTIMIZED_BUILD:-0}" != "1" ]]; then
        node_build_args=(
            --build-arg CARGO_PROFILE_RELEASE_LTO=false
            --build-arg CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
            --build-arg CARGO_PROFILE_RELEASE_DEBUG=0
        )
    fi
    docker build "${node_build_args[@]}" -t "$NODE_IMAGE" "$REPO"
    reclaim_ci_builder_cache
fi

if [[ "${EEZ_SKIP_PROVER_BUILD:-0}" != "1" ]]; then
    echo "==> building $PROVER_IMAGE (prover + native validator)"
    docker build -f "$REPO/Dockerfile.proverd" -t "$PROVER_IMAGE" "$REPO"
    reclaim_ci_builder_cache
else
    echo "==> reusing $PROVER_IMAGE (EEZ_SKIP_PROVER_BUILD=1)"
fi

if [[ "${EEZ_SKIP_DEPLOY_BUILD:-0}" != "1" ]]; then
    echo "==> building $DEPLOY_IMAGE (foundry + contracts)"
    docker build -f "$HERE/Dockerfile.deploy" -t "$DEPLOY_IMAGE" "$REPO"
else
    echo "==> reusing $DEPLOY_IMAGE (EEZ_SKIP_DEPLOY_BUILD=1)"
fi

# The CI enclave name is stable. Remove a stale copy left by an interrupted
# previous run before creating the replacement.
echo "==> removing any stale enclave named '$ENCLAVE'"
kurtosis enclave rm -f "$ENCLAVE" >/dev/null 2>&1 || true

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
Inspect   : kurtosis enclave inspect $ENCLAVE
Node log  : kurtosis service logs -f $ENCLAVE eez-node
Prover log: kurtosis service logs -f $ENCLAVE eez-prover
Tear down : bash testing/kurtosis/stop.sh
EOF
