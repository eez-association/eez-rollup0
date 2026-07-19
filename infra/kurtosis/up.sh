#!/usr/bin/env bash
# Build local images and run the whole EEZ Kurtosis devnet.
#
# Usage:
#   bash infra/kurtosis/up.sh [args-file]    # default: infra/kurtosis/args.yaml
#
# Env knobs:
#   EEZ_SKIP_NODE_BUILD=1    reuse an existing eez-node image
#   EEZ_SKIP_DEPLOY_BUILD=1  reuse an existing eez-deploy image
#   EEZ_OPTIMIZED_BUILD=1    build eez-node in release mode
#   KURTOSIS_ENCLAVE=name    enclave name, default eez-devnet
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
ARGS_FILE="${1:-$HERE/args.yaml}"
# ethereum-package dev mnemonic.
DEV_MNEMONIC="giant issue aisle success illegal bike spike question tent bar rely arctic volcano long crawl hungry vocal artwork sniff fantasy very lucky have athlete"
SPAMOOR_IMAGE="ethpandaops/spamoor@sha256:24818bf7ab76696b2dccb0c59cb419cce358cf1b4326a545012b031afd11658b"

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

# First run: create args.yaml from the example.
if [[ ! -f "$ARGS_FILE" ]]; then
    echo "==> $ARGS_FILE not found, creating it from args.example.yaml"
    cp "$HERE/args.example.yaml" "$ARGS_FILE"
fi

if grep -qE '^[[:space:]]*image:[[:space:]]*"?ethpandaops/spamoor:master"?[[:space:]]*$' "$ARGS_FILE"; then
    tmp_args="$(mktemp "${ARGS_FILE}.tmp.XXXXXX")"
    awk -v image="$SPAMOOR_IMAGE" '
        /^[[:space:]]*image:[[:space:]]*"?ethpandaops\/spamoor:master"?[[:space:]]*$/ {
            match($0, /[^[:space:]]/)
            print substr($0, 1, RSTART - 1) "image: \"" image "\""
            next
        }
        { print }
    ' "$ARGS_FILE" > "$tmp_args"
    mv "$tmp_args" "$ARGS_FILE"
    echo "==> pinned spamoor image to the tested e214fd1 build"
fi

# Flat key lookup for this simple args template.
yv() { grep -E "^[[:space:]]*$1:" "$ARGS_FILE" | head -1 \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'; }

# Migrate args.yaml files created before the outbound daemon existed.
if ! grep -qE '^[[:space:]]*outbound_private_key:' "$ARGS_FILE"; then
    if grep -qE '^[[:space:]]*inbound_private_key:' "$ARGS_FILE"; then
        tmp_args="$(mktemp "${ARGS_FILE}.tmp.XXXXXX")"
        awk '
            { print }
            /^[[:space:]]*inbound_private_key:/ && !inserted {
                match($0, /[^[:space:]]/)
                print substr($0, 1, RSTART - 1) "outbound_private_key: \"0xCHANGE_ME\""
                inserted = 1
            }
        ' "$ARGS_FILE" > "$tmp_args"
        mv "$tmp_args" "$ARGS_FILE"
    else
        echo "cannot migrate $ARGS_FILE: inbound_private_key is missing." >&2
        echo "Add the spamoor_eez block from infra/kurtosis/args.example.yaml." >&2
        exit 1
    fi
    echo "==> added eez.spamoor_eez.outbound_private_key to existing args.yaml"
fi

if command -v python3 >/dev/null && python3 -c "import yaml" >/dev/null 2>&1; then
    python3 -c 'import sys, yaml; yaml.safe_load(open(sys.argv[1]))' "$ARGS_FILE" \
        || { echo "$ARGS_FILE is not valid YAML" >&2; exit 1; }
fi

# Derive separate daemon keys and fund them in the L1 genesis config.
DERIVED_ADDRS=()
for pair in "poster_key:1" "proof_signer_key:2" "inbound_private_key:3" "outbound_private_key:4"; do
    key="${pair%%:*}"; index="${pair##*:}"
    if [[ "$(yv "$key")" == "0xCHANGE_ME" ]]; then
        command -v cast >/dev/null || {
            echo "cast not found in PATH — needed to auto-derive eez.$key." >&2
            echo "Install foundry, or set eez.$key in $ARGS_FILE by hand." >&2
            exit 1
        }
        derived="$(cast wallet private-key --mnemonic "$DEV_MNEMONIC" --mnemonic-index "$index")"
        derived_addr="$(cast wallet address --private-key "$derived")"
        sed -i.bak -E "s|^([[:space:]]*${key}:).*|\\1 \"${derived}\"|" "$ARGS_FILE"
        rm -f "$ARGS_FILE.bak"
        echo "==> derived eez.$key from the dev mnemonic (index $index) -> $derived_addr"
        DERIVED_ADDRS+=("$derived_addr")
    fi
done

# Prefer structural YAML edits when pyyaml is available.
if [[ "${#DERIVED_ADDRS[@]}" -gt 0 ]]; then
    if command -v python3 >/dev/null && python3 -c "import yaml" >/dev/null 2>&1; then
        python3 - "$ARGS_FILE" "${DERIVED_ADDRS[@]}" <<'PYEOF'
import sys, yaml

args_file = sys.argv[1]
addrs = sys.argv[2:]

with open(args_file) as f:
    data = yaml.safe_load(f) or {}

eth = data.setdefault("ethereum_package", {})
np = eth.setdefault("network_params", {})
pf = np.get("prefunded_accounts")
pf_was_string = isinstance(pf, str)

if pf_was_string:
    import json
    pf = json.loads(pf) if pf.strip() else {}
elif pf is None:
    pf = {}

changed = False
for addr in addrs:
    if addr not in pf:
        pf[addr] = {"balance": "1000ETH"}
        changed = True

np["prefunded_accounts"] = json.dumps(pf) if pf_was_string else pf

if changed:
    with open(args_file, "w") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    print(f"==> added {len(addrs)} derived address(es) to network_params.prefunded_accounts")
else:
    print("==> derived addresses already present in network_params.prefunded_accounts")
PYEOF
    else
        echo "⚠️  python3/pyyaml not found — cannot auto-inject prefunded_accounts." >&2
        echo "    Add these addresses to ethereum_package.network_params.prefunded_accounts in $ARGS_FILE manually:" >&2
        for addr in "${DERIVED_ADDRS[@]}"; do
            echo "      \"$addr\": { balance: \"1000ETH\" }" >&2
        done
        echo "    Without this, postBatch simulation will fail with insufficient funds" >&2
        echo "    and eez-node bundles will be dropped on every target block." >&2
    fi
fi

NODE_IMAGE="$(yv eez_node_image)";  NODE_IMAGE="${NODE_IMAGE:-eez-node:dev}"
DEPLOY_IMAGE="$(yv deploy_image)";  DEPLOY_IMAGE="${DEPLOY_IMAGE:-eez-deploy:dev}"

export DOCKER_BUILDKIT=1

if [[ "${EEZ_SKIP_NODE_BUILD:-0}" != "1" ]]; then
    echo "==> building $NODE_IMAGE (repo Dockerfile, fast devnet profile)"
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
fi

if [[ "${EEZ_SKIP_DEPLOY_BUILD:-0}" != "1" ]]; then
    echo "==> building $DEPLOY_IMAGE (foundry + contracts)"
    docker build -f "$HERE/deploy.Dockerfile" -t "$DEPLOY_IMAGE" "$REPO"
else
    echo "==> reusing $DEPLOY_IMAGE (EEZ_SKIP_DEPLOY_BUILD=1)"
fi

echo "==> kurtosis run (enclave: $ENCLAVE)"
# disruptoor needs privileged mode for P2P partitions.
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
