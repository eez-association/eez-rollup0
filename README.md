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

## Followers

Two ways to follow the chain. Both rebuild L2 from L1, so neither has to
trust the sequencer for finality.

### L1-derived follower (`eez-node`, env-driven)

The same **`eez-node` binary, run in follower mode** — chosen purely by which
env vars are set: `EEZ_L1_RPC_URL` present + `EEZ_PROOF_SIGNER_KEY` **absent**
⇒ it runs just the L1 watcher + deriver (no sequencer, no composer). It
replays the whole L2 chain from L1 `BatchPosted` events, rebuilding the
cross-chain system transactions from `EEZ_L2_SYSTEM_KEY`. No sequencer
connection, no peers — the trust-minimised path.

**Heads-up:** `eez-node` automatically loads the repo's `.env` (and
`deployments.env`) on startup, and that `.env` holds `EEZ_PROOF_SIGNER_KEY` —
which would flip the follower back into *composer* mode (and try to start an
embedded L1). So run the follower from an **empty directory** (no `.env` to
pick up) and pass everything explicitly. Build once (`cargo build -p
eez-node`), then:

```bash
cargo build -p eez-node                    # run from the repo root
NODE=$PWD/target/debug/eez-node
set -a; source deployments.env; set +a     # export registry/rollup/genesis/ccm/system-address

rm -rf /tmp/eez-follower-l2                 # clean catch-up from genesis

( cd /tmp                                   # escape the repo .env (composer creds)
  unset EEZ_PROOF_SIGNER_KEY EEZ_L1_EMBEDDED            # ⇒ follower mode, no embedded L1
  export EEZ_L1_RPC_URL=http://localhost:18645          # any chiado RPC with the BatchPosted history
  export EEZ_L1_BUILDER_RPC_URL=$EEZ_L1_RPC_URL         # parsed but unused (a follower never posts)
  export EEZ_L1_POSTER_KEY=0x<any-key>                  # parsed but never signs
  export EEZ_L2_SYSTEM_KEY=0x<same-as-composer>         # MUST match the composer (system-tx rebuild)
  export EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS=10200
  export EEZ_L1_BLOCK_TIME_MS=5000 EEZ_L2_BLOCK_TIME_MS=1000 \
         EEZ_PROOF_TIME_MS=2000 EEZ_SUBMISSION_SLACK_MS=500   # MUST match the composer
  export EEZ_L2_DATADIR=/tmp/eez-follower-l2             # distinct from the composer
  exec "$NODE" node \
    --chain "$EEZ_L2_GENESIS_PATH" --datadir "$EEZ_L2_DATADIR" \
    --http --http.port 28688 --http.api eth,net,web3 \
    --authrpc.port 28684 --port 31640 --disable-discovery --ipcdisable \
    --engine.persistence-threshold 4 --engine.memory-block-buffer-target 1 )
```

The two `--engine.*` flags are not optional: the deriver replays each batch and
then reads the parent block it just produced (to compute the next batch's
system-tx nonce), so derived blocks must be flushed to disk promptly.
Without them reth keeps the head in memory and the deriver wedges with
`local L2 header at parent N missing`.

(The `( cd /tmp … )` subshell is the trick: from there `eez-node` finds no
`.env` to load, so only your explicit exports apply.)
`scripts/smoke-chiado-follower.sh` automates all of this against a running
composer and checks that the follower ends up with the same L2 state (the
`Value`, the recipient balance, and the per-block state roots) using chiado L1
alone.

### Sequencer-assisted follower (`eez-follower`)

The dedicated `eez-follower` binary does the same L1 derivation but also keeps
a faster *latest* head: it asks a sequencer over RPC (`--sequencer-rpc`, or
`EEZ_SEQUENCER_RPC`) and peers with it, accepting that head only while it
stays consistent with what L1 has already confirmed.

```bash
cargo run -p eez-follower -- node --chain "$EEZ_L2_GENESIS_PATH" \
  --datadir /tmp/eez-follower-data \
  --sequencer-rpc http://127.0.0.1:18688 \
  --trusted-peers <enode-from-sequencer-startup-log> \
  --http --http.port 18788 --port 30403 --authrpc.port 18784 \
  --disable-discovery
```

Omit `--sequencer-rpc` and the peer flags to make it L1-derived-only too.

Logs to watch (either follower): `eez.deriver.catch_up.start` on boot,
`eez.deriver.safe.advanced` / `eez.deriver.finalized.advanced` as L1
batches/finality are accepted, and (sequencer-assisted)
`eez.follower.head.advanced` / `.head.inconsistent`. Confirm convergence with
`cast block-number`, `cast block safe`, and `cast block finalized` against the
follower RPC.

## Build, test, teardown

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                  # Rust; `cd contracts && forge test` for Solidity
bash scripts/teardown-chiado.sh         # stop node + lighthouse, release the L1 datadir
```
