#!/usr/bin/env bash
# Build the selected images and start the local Kurtosis devnet.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
ARGS_FILE="${1:-$HERE/ci-args.yaml}"

command -v kurtosis >/dev/null || { echo "kurtosis not found in PATH" >&2; exit 1; }
command -v docker   >/dev/null || { echo "docker not found in PATH" >&2; exit 1; }

PROTOCOL_DIR="$REPO/eez-core-protocol"

# Fail before building images if the protocol submodule is missing or stale.
if [[ ! -d "$PROTOCOL_DIR/.git" && ! -f "$PROTOCOL_DIR/.git" ]]; then
    echo "eez-core-protocol submodule is not initialized." >&2
    echo "Run: git submodule update --init --recursive eez-core-protocol" >&2
    exit 1
fi

EXPECTED_PROTOCOL_COMMIT="$(git -C "$REPO" ls-files -s eez-core-protocol | awk '{print $2}')"
ACTUAL_PROTOCOL_COMMIT="$(git -C "$PROTOCOL_DIR" rev-parse HEAD)"
if [[ -z "$EXPECTED_PROTOCOL_COMMIT" || "$ACTUAL_PROTOCOL_COMMIT" != "$EXPECTED_PROTOCOL_COMMIT" ]]; then
    echo "eez-core-protocol is not at the commit pinned by this checkout." >&2
    echo "Run: git submodule update --init --recursive eez-core-protocol" >&2
    echo "Current submodule status:" >&2
    git -C "$REPO" submodule status eez-core-protocol >&2 || true
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
PREBUILT_BIN_DIR="${EEZ_PREBUILT_BIN_DIR:-}"

export DOCKER_BUILDKIT=1

# Source builds can opt into the GitHub Actions cache backend so dependency
# layers survive across commits. CI Kurtosis sets EEZ_PREBUILT_BIN_DIR instead
# and builds thin images from the shared e2e-build artifact.
# Local runs keep using the ordinary Docker builder and its local cache.
docker_build() {
    local cache_scope="$1"
    shift
    if [[ "$cache_scope" == "deploy" ]]; then
        docker build "$@"
        return
    fi
    if [[ "${EEZ_DOCKER_CACHE:-}" == "gha" ]]; then
        local cache_args=(
            --cache-from "type=gha,scope=eez-$cache_scope"
            --cache-to "type=gha,mode=max,scope=eez-$cache_scope"
        )
        if [[ "$cache_scope" != "node" ]]; then
            cache_args+=(--cache-from "type=gha,scope=eez-node")
        fi
        docker buildx build --load \
            "${cache_args[@]}" \
            "$@"
    else
        docker build "$@"
    fi
}

# The default `release` profile is already the fast build; set
# EEZ_OPTIMIZED_BUILD=1 for production (maxperf) binaries.
release_build_args=()
if [[ "${EEZ_OPTIMIZED_BUILD:-0}" == "1" ]]; then
    release_build_args=(--build-arg BUILD_PROFILE=maxperf)
fi

if [[ -n "$PREBUILT_BIN_DIR" ]]; then
    PREBUILT_BIN_DIR="$(cd "$PREBUILT_BIN_DIR" && pwd)"
    for binary in eez-composer eez-follower eez-genesis-state-root eez-proof-signer; do
        if [[ ! -x "$PREBUILT_BIN_DIR/$binary" ]]; then
            echo "prebuilt binary is missing or not executable: $PREBUILT_BIN_DIR/$binary" >&2
            exit 1
        fi
    done
fi

if [[ "${EEZ_SKIP_NODE_BUILD:-0}" != "1" ]]; then
    if [[ -n "$PREBUILT_BIN_DIR" ]]; then
        echo "==> building $NODE_IMAGE from prebuilt CI binaries"
        docker build \
            -f "$HERE/Dockerfile.prebuilt" --target node \
            -t "$NODE_IMAGE" \
            "$PREBUILT_BIN_DIR"
    else
        echo "==> building $NODE_IMAGE (fast development profile)"
        docker_build node "${release_build_args[@]}" -t "$NODE_IMAGE" "$REPO"
    fi
fi

if [[ "${EEZ_SKIP_PROOF_SIGNER_BUILD:-0}" != "1" ]]; then
    if [[ -n "$PREBUILT_BIN_DIR" ]]; then
        echo "==> building $PROOF_SIGNER_IMAGE from prebuilt CI binaries"
        docker build \
            -f "$HERE/Dockerfile.prebuilt" --target proof-signer \
            -t "$PROOF_SIGNER_IMAGE" \
            "$PREBUILT_BIN_DIR"
    else
        echo "==> building $PROOF_SIGNER_IMAGE (fast development profile)"
        docker_build signer "${release_build_args[@]}" \
            -f "$REPO/Dockerfile" --target proof-signer \
            -t "$PROOF_SIGNER_IMAGE" "$REPO"
    fi
else
    echo "==> reusing $PROOF_SIGNER_IMAGE (EEZ_SKIP_PROOF_SIGNER_BUILD=1)"
fi

if [[ "${EEZ_SKIP_DEPLOY_BUILD:-0}" != "1" ]]; then
    echo "==> building $DEPLOY_IMAGE (foundry + contracts)"
    docker_build deploy \
        --build-arg "EEZ_NODE_IMAGE=$NODE_IMAGE" \
        -f "$HERE/Dockerfile.deploy" \
        -t "$DEPLOY_IMAGE" \
        "$REPO"
else
    echo "==> reusing $DEPLOY_IMAGE (EEZ_SKIP_DEPLOY_BUILD=1)"
fi

if [[ "${EEZ_PRUNE_BUILD_CACHE:-0}" == "1" ]]; then
    echo "==> pruning Docker build cache"
    docker builder prune --all --force
fi

echo "==> kurtosis run (enclave: $ENCLAVE)"
kurtosis_flags=()
# The included topology runs without privileged package execution. Keep this
# opt-in for custom argument files or future package changes that require it.
if [[ "${KURTOSIS_PRIVILEGED:-0}" == "1" ]]; then
    kurtosis_flags+=(--privileged)
fi
kurtosis run "${kurtosis_flags[@]}" --enclave "$ENCLAVE" "$HERE" --args-file "$ARGS_FILE"

cat <<EOF

════════════════════════════════════════
  EEZ Kurtosis devnet is up.
════════════════════════════════════════
Inspect  : kurtosis enclave inspect $ENCLAVE
Node log : kurtosis service logs -f $ENCLAVE eez-node
Signer log: kurtosis service logs -f $ENCLAVE eez-proof-signer
Tear down: bash testing/kurtosis/stop.sh
EOF

if ! KURTOSIS_ENCLAVE="$ENCLAVE" bash "$HERE/ports.sh"; then
    echo "warning: devnet started, but its endpoint summary could not be resolved" >&2
fi
