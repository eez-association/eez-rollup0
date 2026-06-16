# eez-rollup0

An L2 rollup where L1 and L2 can call into each other as part of the same
flow. Three components do the work: a **sequencer** produces L2 blocks (it
drives a stock [reth](https://github.com/paradigmxyz/reth) node), a
**composer** posts those blocks to the `EEZ` contract on L1, and a
**deriver** can rebuild the whole L2 chain from L1 alone. The supported L1 is
Gnosis **Chiado** (a testnet).

## What it does

- **Produces L2 blocks** on a schedule tied to L1: ordinary blocks most of
  the time, plus one special **Sync block** per L1 block that carries the
  cross-chain work.
- **Routes cross-chain calls.** A transaction aimed at a cross-chain proxy
  (or sent from L1) is held aside, simulated, and packed into the next Sync
  block. The matching L1 call (`postBatch`) and the user's L1 transactions
  are submitted together so they all land in the *same* L1 block — all or
  nothing.
- **Commits first, repairs if needed.** The Sync block is added to L2 right
  away; the L1 submission is watched in the background, and if it doesn't make
  it onto L1 the L2 block is rolled back. L1 is the source of truth — L2 only
  keeps what L1 confirms.
- **Can be re-derived from L1.** A **follower** rebuilds the identical L2
  chain just by reading L1 (the `BatchPosted` events) and re-running the same
  transactions — no need to trust the sequencer.

Proofs are a mock for now (`MockECDSAProofSystem`, an ECDSA-signature
stand-in). The `EEZ` contract enforces the chain of state roots itself, so
the mock is enough to test liveness and consistency — real validity proofs
come later.

## Run a chiado L2 (Docker)

Runs two containers: `eez-node` (which embeds a Chiado L1 node alongside the
L2 + composer) and a **lighthouse** consensus client that drives the L1.
There's no separate L1 node to run. Cross-chain batches are submitted to
Chiado's block builder; the L1 block to aim them at is read from the embedded
L1 once it has caught up to the chain tip.

### One-time setup

```bash
git submodule update --init --recursive

# 1. Build the node image (~30 min cold; cargo-chef caches the reth deps).
docker build -t eez-node:local .

# 2. Download a minimal chiado L1 snapshot (skips syncing from genesis).
mkdir -p data
docker run --rm -v "$PWD/data/chiado-l1:/data" \
  ghcr.io/gnosischain/reth_gnosis:v2.0.0 \
  download --chain chiado --minimal --datadir /data

# 3. Shared engine-API JWT for eez-node <-> lighthouse.
openssl rand -hex 32 > data/jwt.hex

# 4. Chiado consensus config (config.yaml, bootnodes, deploy_block.txt).
git clone https://github.com/gnosischain/configs.git /tmp/gnosis-configs
mkdir -p configs && cp -r /tmp/gnosis-configs/chiado configs/chiado

# 5. Fund the operator/poster keys with xDAI (https://faucet.chiadochain.net).
```

### Deploy the protocol (once, against a fully-synced chiado RPC)

The embedded L1 is still syncing at this point, so run the deploy against a
Chiado RPC that's already at the chain tip — the public endpoint, or a
standalone chiado-reth.

```bash
cp .env.example .env
#   EEZ_L1_RPC_URL=<tip chiado RPC>   EEZ_L1_POSTER_KEY=<operator key>
#   EEZ_PROOF_SIGNER_KEY=<operator key>   (its address becomes the proof system's authorizedSigner)
EEZ_DEPLOY_SKIP_SIMULATION=1 make deploy-protocol

cp datadir/genesis.json ./data/genesis-fresh.json
```

This deploys EEZ + MockECDSAProofSystem + the rollup manager, registers the
rollup, deploys the L1 bridge contracts, and writes **`deployments.env`**
(registry, proof system, rollup id, deploy block, bridge + CCM-L2 addresses)
plus the L2 **`datadir/genesis.json`** whose timestamp is pinned to the deploy
block. The container loads `deployments.env` automatically; `.env.chiado`'s
`FRESH_GENESIS` points at that genesis (default `./datadir/genesis.json`), so
**deploy must run before `up`** — there is no separate genesis-creation step.
(Set `EEZ_BLOCKSCOUT_URL` first to also verify the contracts on Blockscout —
opt-in.)

### Configure and start

```bash
cp .env.chiado.example .env.chiado    # host paths + funded keys + bundler URL
docker compose --env-file .env.chiado -f docker-compose.chiado-node.yml up
```

The embedded L1 checkpoint-syncs (lighthouse, ~5 min) and catches up past the
deploy block; then the L2 sequencer + composer run. Health checks:

```bash
cast block-number --rpc-url http://localhost:18645   # embedded chiado L1 climbing
cast block-number --rpc-url http://localhost:18688   # L2 producing
```

### Exercise it

Deploys a `Value` test contract and its cross-chain proxies, fires several
rounds of cross-chain setter/deposit calls at the running node, then checks
that L1 and L2 agree on the state root and that the calls actually took
effect:

```bash
EEZ_WAVE_COUNT=5 bash scripts/devnet-test.sh
```

## Build, test, teardown

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                  # Rust; `cd contracts && forge test` for Solidity
bash scripts/teardown-chiado.sh         # stop node + lighthouse, release the L1 datadir
```
