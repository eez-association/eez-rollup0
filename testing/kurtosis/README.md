# Kurtosis local devnet

This package runs a disposable local EEZ network for development and testing.
It deploys the protocol contracts, generates a deployment-bound L2 genesis, and
starts the node, proof signer, private PoS L1, and MEV builder stack.

The included configuration contains deterministic private-network keys. Never
use these keys on a public network or fund them with real assets.

## Quick start

Run every command from the repository root.

### 1. Prepare the host

The local network requires:

- A Linux host with [Docker](https://docs.docker.com/engine/install/) running and
  enough free space to build three images.
- The [Kurtosis CLI](https://docs.kurtosis.com/install/).
- [Foundry](https://getfoundry.sh/introduction/installation/) v1.7.1 (`cast` and
  `forge`).
- Bash, Git, GNU `timeout`, `jq`, `curl`, and `openssl`.
- The `eez-core-protocol` submodule at the commit pinned by this checkout.
- Network access to pull container images and the pinned Ethereum package.

Initialize the submodule and start the Kurtosis engine:

```bash
git submodule update --init --recursive eez-core-protocol
kurtosis engine start
```

Optional preflight checks:

```bash
docker info
kurtosis version
forge --version
cast --version
jq --version
```

### 2. Start the network

Keep these variables exported in every shell used to operate or test the
network:

```bash
export KURTOSIS_ENCLAVE=eez-dev
export KURTOSIS_ARGS_FILE="$PWD/testing/kurtosis/ci-args.yaml"

bash testing/kurtosis/start.sh "$KURTOSIS_ARGS_FILE"
```

All lifecycle and workload scripts fall back to the enclave name `eez-ci` when
`KURTOSIS_ENCLAVE` is unset. Always export the intended name in a new shell,
especially before running the destructive `stop.sh` command.

`start.sh` does not request privileged package execution by default. Set
`KURTOSIS_PRIVILEGED=1` only when using a custom topology that requires it.

The first start builds the node, proof-signer, and deployment images and pulls
the Ethereum client images. It can take several minutes. The command returns
after Kurtosis has deployed the contracts and started the network services. It
leaves the enclave running.

### 3. Check the deployment

```bash
kurtosis enclave inspect "$KURTOSIS_ENCLAVE"
bash testing/kurtosis/scripts/verify-eezl2-deployment.sh
```

The verification checks the live EEZL2 system address, rollup ID,
`USE_GAS_LEFT` setting, runtime code hash, and genesis state root against the
generated deployment bindings.

### 4. Connect to the RPC endpoints

`start.sh` prints an endpoint summary after starting the network. To print it
again and export the URLs into the current shell, source the port helper:

```bash
source testing/kurtosis/ports.sh
```

The summary has this shape:

```text
EEZ Kurtosis devnet endpoints (enclave: eez-dev)
  Canonical L1 RPC:      http://127.0.0.1:<port>
  L2 RPC:                http://127.0.0.1:<port>
  L1 cross-chain front:  http://127.0.0.1:<port>
  L2 cross-chain front:  http://127.0.0.1:<port>
  Proof signer gRPC:     127.0.0.1:<port>
  L1 explorer:           http://127.0.0.1:<port>
  L2 explorer:           http://127.0.0.1:<port>
  L1 explorer API:       http://127.0.0.1:<port>
  L2 explorer API:       http://127.0.0.1:<port>
```

The sourced helper exports:

```text
EEZ_DEVNET_L1_RPC
EEZ_DEVNET_L2_RPC
EEZ_DEVNET_L1_FRONT
EEZ_DEVNET_L2_FRONT
EEZ_DEVNET_PROOF_SIGNER_GRPC
EEZ_DEVNET_L1_EXPLORER
EEZ_DEVNET_L2_EXPLORER
EEZ_DEVNET_L1_EXPLORER_API
EEZ_DEVNET_L2_EXPLORER_API
```

The HTTP URLs can be reused with `cast`, `curl`, or an application under
development:

```bash
cast chain-id --rpc-url "$EEZ_DEVNET_L1_RPC"
cast block-number --rpc-url "$EEZ_DEVNET_L2_RPC"
```

`EEZ_DEVNET_L1_FRONT` accepts inbound L1-to-L2 raw transactions, and
`EEZ_DEVNET_L2_FRONT` accepts outbound L2-to-L1 raw transactions. Ordinary L2
transactions go to `EEZ_DEVNET_L2_RPC`.

The explorer variables are exported only when `enable_explorers` is enabled in
the selected arguments file.

### 5. Stop the network

```bash
bash testing/kurtosis/stop.sh
```

This force-removes the enclave named by `KURTOSIS_ENCLAVE`, including its
ephemeral chain state. Download any required artifacts or logs before stopping
it.

## What the package deploys

The local topology contains:

- A canonical private PoS L1 with one reth/Lighthouse participant.
- An rbuilder, relay, MEV-Boost, and proposer path for atomic bundle inclusion.
- A deployment task that deploys the L1 contracts, derives the configured L2
  system address, renders the EEZL2 runtime and funded L2 genesis, and registers
  the resulting genesis state root.
- An `eez-node` running the L2, composer, cross-chain RPC fronts, and an
  embedded L1 execution client.
- An `eez-follower` Lighthouse beacon node that follows the canonical L1 and
  drives the embedded execution client through the Engine API.
- An `eez-proof-signer` that validates L2 windows and returns attestations to
  the node.
- Optional L1 and L2 Blockscout instances, each with its own backend, contract
  verifier, and ephemeral PostgreSQL database. They are enabled in the included
  local profile.

The topology deliberately omits dedicated load-generation, reorg, metrics, and
distributed-tracing services. The included shell workloads generate their own
test traffic.

## Operate the network

### Inspect services

Show the service state and published ports:

```bash
kurtosis enclave inspect "$KURTOSIS_ENCLAVE"
```

Follow the main service logs:

```bash
kurtosis service logs -f "$KURTOSIS_ENCLAVE" eez-node
kurtosis service logs -f "$KURTOSIS_ENCLAVE" eez-proof-signer
```

Other useful services are:

```text
eez-follower
el-1-reth-lighthouse
el-2-reth-builder-lighthouse
mev-relay-api
l1-blockscout-frontend
l2-blockscout-frontend
```

Replace the service name in the log command to inspect one of them. Use `-a`
instead of `-f` to print the available log history without following it.

### Query the chains

After resolving `EEZ_DEVNET_L1_RPC` and `EEZ_DEVNET_L2_RPC` as shown above:

```bash
cast block-number --rpc-url "$EEZ_DEVNET_L1_RPC"
cast block latest --rpc-url "$EEZ_DEVNET_L1_RPC"
cast block-number --rpc-url "$EEZ_DEVNET_L2_RPC"
cast block safe --rpc-url "$EEZ_DEVNET_L2_RPC"
```

### Browse blocks, transactions, and internal calls

The included local profile starts one Blockscout explorer per chain. After
sourcing the endpoint helper, open:

```bash
echo "$EEZ_DEVNET_L1_EXPLORER"
echo "$EEZ_DEVNET_L2_EXPLORER"
```

The L1 explorer indexes the canonical L1, and the L2 explorer indexes the EEZ
L2. Internal calls are available from a transaction's internal-transactions
view after Blockscout indexes its execution trace.

To run without the explorers, set this in the selected arguments file:

```yaml
eez:
  enable_explorers: false
```

When explorers are disabled, `ports.sh` omits their URLs and does not export the
explorer variables.

### Verify contract sources in Blockscout

The deployment artifact preserves the Foundry broadcasts created inside the
Kurtosis deployment task. Download it and submit those exact deployments to the
L1 Blockscout verifier:

```bash
source testing/kurtosis/ports.sh

EEZ_DEVNET_VERIFY_DIR="$(mktemp -d)"
kurtosis files download \
  "$KURTOSIS_ENCLAVE" eez-deployments "$EEZ_DEVNET_VERIFY_DIR"

EEZ_BLOCKSCOUT_URL="$EEZ_DEVNET_L1_EXPLORER_API" \
EEZ_L1_RPC_URL="$EEZ_DEVNET_L1_RPC" \
EEZ_BROADCAST_DIR="$EEZ_DEVNET_VERIFY_DIR/foundry-broadcast" \
  bash scripts/verify-blockscout.sh
```

Then verify the genesis-installed `EEZL2` contract in the L2 explorer:

```bash
EEZ_BLOCKSCOUT_URL="$EEZ_DEVNET_L2_EXPLORER_API" \
EEZ_L2_RPC_URL="$EEZ_DEVNET_L2_RPC" \
EEZ_DEPLOYMENTS_FILE="$EEZ_DEVNET_VERIFY_DIR/deployments.env" \
EEZ_PROTOCOL_DIR="$PWD/eez-core-protocol" \
  bash testing/kurtosis/scripts/verify-eezl2-blockscout.sh
```

The L1 helper submits each saved `CREATE`/`CREATE2` deployment. The L2 helper
uses Standard JSON because the genesis predeploy has no creation transaction.
Both print their result without modifying either chain. If an explorer is still
indexing, wait briefly and run the corresponding command again.

### Use a funded development account

The following public Hardhat account has 1,000,000 test ETH in both the
canonical L1 configuration and the L2 genesis:

```text
Private key: 0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
Address:     0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC
```

Export it and confirm its balances:

```bash
export EEZ_DEVNET_TEST_KEY=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
export EEZ_DEVNET_TEST_ACCOUNT="$(
  cast wallet address --private-key "$EEZ_DEVNET_TEST_KEY"
)"

cast balance "$EEZ_DEVNET_TEST_ACCOUNT" --ether \
  --rpc-url "$EEZ_DEVNET_L1_RPC"
cast balance "$EEZ_DEVNET_TEST_ACCOUNT" --ether \
  --rpc-url "$EEZ_DEVNET_L2_RPC"
```

For example, submit an ordinary L2 transfer:

```bash
cast send 0x1111111111111111111111111111111111111111 \
  --value 1wei \
  --private-key "$EEZ_DEVNET_TEST_KEY" \
  --rpc-url "$EEZ_DEVNET_L2_RPC"
```

This account is public and insecure. Use it only on this disposable local
network. The workload harness also uses it for L2 setup and filler
transactions, so avoid sending manual transactions from it while a workload is
running.

### Download deployment artifacts

The `eez-deployments` Kurtosis artifact contains:

- `deployments.env`, with public contract addresses and deployment bindings.
- `l2-genesis.json`, used to start the L2.
- `l2-genesis.profile.json`, with the generated runtime hash and genesis root.
- `foundry-broadcast`, with the L1 creation metadata used for Blockscout source
  verification.

Download it into a new temporary directory:

```bash
EEZ_DEVNET_DEPLOYMENTS_DIR="$(mktemp -d)"
kurtosis files download \
  "$KURTOSIS_ENCLAVE" eez-deployments "$EEZ_DEVNET_DEPLOYMENTS_DIR"
ls -la "$EEZ_DEVNET_DEPLOYMENTS_DIR"
```

Load the public deployment values into the current shell when needed:

```bash
set -a
source "$EEZ_DEVNET_DEPLOYMENTS_DIR/deployments.env"
set +a
```

`deployments.env` does not contain the L2 system private key. The deterministic
private keys used by this local profile remain in `ci-args.yaml`.

### Capture diagnostics

Before removing a network, save its service specifications and logs to a new
directory:

```bash
mkdir -p "$PWD/artifacts"
export EEZ_DEVNET_DUMP_DIR="$PWD/artifacts/kurtosis-dev-dump"
kurtosis enclave dump "$KURTOSIS_ENCLAVE" "$EEZ_DEVNET_DUMP_DIR"
```

Choose a different `EEZ_DEVNET_DUMP_DIR` if that destination already exists.

### Reset or rebuild the network

Protocol deployment and genesis generation happen when the enclave is created.
To redeploy after changing the node, contracts, genesis generation, or package
configuration, remove the current enclave and start it again:

```bash
bash testing/kurtosis/stop.sh
bash testing/kurtosis/start.sh "$KURTOSIS_ARGS_FILE"
```

Stopping is destructive. Use a different `KURTOSIS_ENCLAVE` value if the old
network must remain available.

## Generate test traffic

The workload scripts discover the enclave ports and deployment artifact
automatically. They also read the deterministic L1 funding key from
`KURTOSIS_ARGS_FILE`, so that variable must point to the arguments file used to
start the network.

### Run one cross-chain workload

```bash
EEZ_WAVE_MODE=mixed EEZ_WAVE_COUNT=1 \
  bash testing/kurtosis/scripts/cross-chain-wave.sh
```

Available modes are:

| Mode | Workload |
| --- | --- |
| `inbound` | L1-to-L2 calls, including a deposit and direct/wrapped contract calls. |
| `outbound` | L2-to-L1 calls, including a withdrawal and direct/wrapped contract calls. |
| `mixed` | Inbound and outbound calls submitted for the same Sync block. |
| `mixed-pure` | Mixed traffic plus ordinary L2 mempool transactions between waves. |

Each workload verifies transaction inclusion, cross-chain state convergence,
`postBatch` settlement on L1, L1/L2 state-root agreement, L2 safe-head progress,
and correlation between the proof signer's result and the node's accepted
attestation.

Individual workload logs are written under `datadir/smoke-logs`. Override their
locations with `EEZ_NODE_LOG` and `EEZ_PROOF_SIGNER_LOG`.

Useful workload controls include:

- `EEZ_WAVE_COUNT`: number of waves; the script default is three.
- `EEZ_WAVE_GAP_SECS`: delay between waves; the default is 20 seconds.
- `EEZ_FILLER_PER_GAP`: pure L2 transactions between `mixed-pure` waves; the
  default is two.
- `EEZ_RECEIPT_WAIT_SECS`: transaction inclusion timeout; the default is 300
  seconds.
- `EEZ_STATE_ROOT_WAIT_SECS`: state-root convergence timeout; the default is 30
  seconds.

### Run all included workloads

The suite requires an output directory:

```bash
export EEZ_CI_RESULT_DIR="$PWD/artifacts/kurtosis-dev"
mkdir -p "$EEZ_CI_RESULT_DIR"

bash testing/kurtosis/scripts/verify-cross-chain-waves.sh
```

Despite the variable's historical `CI` name, this command operates on the
already-running local enclave and leaves it running. It executes one `inbound`,
`outbound`, and `mixed` wave, followed by three `mixed-pure` waves. Per-mode
output is stored under `$EEZ_CI_RESULT_DIR/checks`. Override the stress count
with `EEZ_MIXED_PURE_WAVE_COUNT`.

## Customize the network

### Arguments file

`ci-args.yaml` is the included local profile. To modify it without changing the
committed file, copy it and use the copy for both startup and testing:

```bash
export KURTOSIS_ARGS_FILE=/tmp/eez-dev-args.yaml
cp testing/kurtosis/ci-args.yaml "$KURTOSIS_ARGS_FILE"

# Edit $KURTOSIS_ARGS_FILE, then start the network with it.
bash testing/kurtosis/start.sh "$KURTOSIS_ARGS_FILE"
```

The `ethereum_package` section configures the canonical L1 participant, chain,
slot time, prefunded accounts, and MEV stack. The `eez` section configures:

- Node, proof-signer, deployment, and follower images.
- Poster, proof-signer, and L2 system test keys.
- L1 and L2 block timing, proof time, and submission slack.
- Maximum speculative depth and proposer fee recipient.
- L1/L2 explorers and their images.

Keep the private-network keys distinct, and never reuse them outside a
disposable local environment.

### Build or reuse images

`start.sh` reads the three service image tags from the selected arguments
file and builds them by default. The default release profile is optimized for a
faster development build.

Set `EEZ_OPTIMIZED_BUILD=1` to build the production `maxperf` binaries:

```bash
EEZ_OPTIMIZED_BUILD=1 \
  bash testing/kurtosis/start.sh "$KURTOSIS_ARGS_FILE"
```

To reuse images that already exist locally, put their tags in the arguments
file and skip the corresponding builds:

```bash
EEZ_SKIP_NODE_BUILD=1 \
EEZ_SKIP_PROOF_SIGNER_BUILD=1 \
EEZ_SKIP_DEPLOY_BUILD=1 \
  bash testing/kurtosis/start.sh "$KURTOSIS_ARGS_FILE"
```

The deployment image is built from the selected node image because it copies
the `eez-genesis-state-root` utility from that image. If the node changes,
rebuild the deployment image as well.

Set `EEZ_PRUNE_BUILD_CACHE=1` to run
`docker builder prune --all --force` after building the images. This deletes
the entire host's unused BuildKit cache, not only cache entries created by this
repository, so enable it deliberately.

### Run multiple local networks

Each network needs a unique enclave name:

```bash
export KURTOSIS_ENCLAVE=eez-dev-2
bash testing/kurtosis/start.sh "$KURTOSIS_ARGS_FILE"
```

Kurtosis assigns different host ports automatically. Remember that
`stop.sh` removes whichever enclave is currently selected by
`KURTOSIS_ENCLAVE`.

## Troubleshooting

- **The submodule is missing or stale:** run
  `git submodule update --init --recursive eez-core-protocol`.
- **The enclave name already exists:** inspect it before deciding whether to
  reuse a different name or remove it with `stop.sh`.
- **A service is not ready:** run `kurtosis enclave inspect` and inspect the
  node, proof-signer, builder, relay, or follower logs.
- **Ports cannot be resolved:** ensure the current shell has the same
  `KURTOSIS_ENCLAVE` value used by `start.sh`. If it is unset, the scripts
  silently target their `eez-ci` fallback.
- **A workload cannot resolve its funding key:** ensure `KURTOSIS_ARGS_FILE`
  points to the arguments file used to start the enclave.
- **EEZL2 deployment verification fails:** inspect `eez-node` and the downloaded
  `deployments.env` before resetting the network.
- **L2 source verification says EEZL2 is not a contract:** recreate the devnet;
  an explorer started by an older package did not import the L2 genesis.
- **Docker runs out of space:** inspect Docker disk usage and remove unused
  resources deliberately before rebuilding. `EEZ_PRUNE_BUILD_CACHE=1` removes
  all unused BuildKit cache from the host after the image builds.

## Developer-facing files

- `main.star` and `kurtosis.yml`: network definition and package manifest.
- `blockscout.star`: prefix-aware L1 and L2 Blockscout service definitions.
- `ci-args.yaml`: default local topology, image selections, timing, and test
  keys.
- `l2-genesis-profile.json`: reproducible public inputs and hashes for the
  committed test genesis; it contains no private key.
- `Dockerfile.deploy`: deployment image containing Foundry, contracts, scripts,
  and the genesis state-root utility copied from the selected node image.
- `start.sh` and `stop.sh`: local network lifecycle.
- `ports.sh`: endpoint discovery, summary, and shell exports.
- `scripts/verify-eezl2-deployment.sh`: live EEZL2 deployment verification.
- `scripts/verify-eezl2-blockscout.sh`: L2 Blockscout source verification for
  the genesis-installed EEZL2 contract.
- `scripts/cross-chain-wave.sh`: individual cross-chain workload modes.
- `scripts/verify-cross-chain-waves.sh`: complete workload suite.
