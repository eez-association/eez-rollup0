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
# of asking for a manual `cast wallet private-key` + paste step. Also derive
# their ADDRESSES and register them in network_params.prefunded_accounts so
# genesis actually gives them a balance — without this, postBatch simulation
# fails "insufficient funds" and every bundle gets silently dropped by rbuilder.
DERIVED_ADDRS=()
for pair in "poster_key:1" "proof_signer_key:2"; do
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

# Make sure the derived addresses are present in network_params.prefunded_accounts.
# Uses python3+pyyaml if available (safe structural edit); falls back to a plain
# warning + manual instructions if python/pyyaml isn't present, rather than risking
# a corrupt YAML file via sed on a nested map.
if [[ "${#DERIVED_ADDRS[@]}" -gt 0 ]]; then
    if command -v python3 >/dev/null && python3 -c "import yaml" >/dev/null 2>&1; then
        python3 - "$ARGS_FILE" "${DERIVED_ADDRS[@]}" <<'PYEOF'
import sys, yaml

args_file = sys.argv[1]
addrs = sys.argv[2:]

with open(args_file) as f:
    data = yaml.safe_load(f) or {}

np = data.setdefault("network_params", {})
pf = np.get("prefunded_accounts")

if isinstance(pf, str):
    import json
    pf = json.loads(pf) if pf.strip() else {}
elif pf is None:
    pf = {}

changed = False
for addr in addrs:
    if addr not in pf:
        pf[addr] = {"balance": "1000ETH"}
        changed = True

np["prefunded_accounts"] = pf

if changed:
    with open(args_file, "w") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    print(f"==> added {len(addrs)} derived address(es) to network_params.prefunded_accounts")
else:
    print("==> derived addresses already present in network_params.prefunded_accounts")
PYEOF
    else
        echo "⚠️  python3/pyyaml not found — cannot auto-inject prefunded_accounts." >&2
        echo "    Add these addresses to network_params.prefunded_accounts in $ARGS_FILE manually:" >&2
        for addr in "${DERIVED_ADDRS[@]}"; do
            echo "      \"$addr\": { balance: \"1000ETH\" }" >&2
        done
        echo "    Without this, postBatch simulation will fail with insufficient funds" >&2
        echo "    and eez-node bundles will be dropped on every target block." >&2
    fi
fi

# l2_block_time_ms MUST divide l1_block_time_ms evenly with K = l1/l2 >= 2
# (RollupTiming::validate() refuses K < 2 at eez-node startup). The default
# preset (l1=12000, l2=2000) gives K=6, which is valid — no migration needed.

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
