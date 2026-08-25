# eez-rollup0

An L2 rollup where L1 and L2 can call into each other as part of the same
flow. Four components do the work: a **sequencer** produces L2 blocks (it
drives a stock [reth](https://github.com/paradigmxyz/reth) node), a
**composer** posts those blocks to the `EEZ` contract on L1, and a
**proof signer** independently re-executes and attests them. A **deriver** can
rebuild the whole L2 chain from L1 alone. The supported L1 is Gnosis **Chiado**
(a testnet).

## What it does

- **Produces L2 blocks** on a schedule tied to L1: ordinary blocks most of
  the time, plus one special **Sync block** per L1 block that carries the
  cross-chain work.
- **Routes cross-chain calls.** A transaction aimed at a cross-chain proxy
  (or sent from L1) is held aside, simulated, and packed into the next Sync
  block. The matching L1 `postAndVerifyBatch` call and the user's L1 transactions
  are submitted together so they all land in the *same* L1 block — all or
  nothing.
- **Commits first, repairs if needed.** The Sync block is added to L2 right
  away; the L1 submission is watched in the background, and if it doesn't make
  it onto L1 the L2 block is rolled back. L1 is the source of truth — L2 only
  keeps what L1 confirms.
- **Can be re-derived from L1.** A **follower** rebuilds the identical L2
  chain just by reading L1 (the `BatchPosted` events) and re-running the same
  transactions — no need to trust the sequencer for safe state. Before L1
  inclusion, it can follow complete sequencer-signed payloads over libp2p.

`eez-proof-signer` statelessly re-executes each proposed batch and validates
its settlement effects before signing the recomputed public-input hash.
`ECDSAProofSystem` verifies that hash-bound attestation on L1. This is not yet
a succinct validity proof, but the deployed verifier no longer accepts an
unbound mock signature.

## Run a chiado L2 (Docker)

Runs three containers: `eez-node` (whose entrypoint is the `eez-composer`
binary, embedding a Chiado L1 node alongside the L2), `eez-proof-signer`, and a
**lighthouse** consensus client that drives the L1.
There's no separate L1 node to run. Cross-chain batches are submitted to
Chiado's block builder; the L1 block to aim them at is read from the embedded
L1 once it has caught up to the chain tip.

> **One command** (after the one-time setup + `.env`/`.env.chiado`): `bash
> scripts/chiado-up.sh` deploys the protocol (skipped if already deployed),
> prepares the datadirs, starts the stack, waits until it's healthy, and
> prints the RPC URLs. The numbered steps below are that same flow done by
> hand.

### One-time setup

```bash
git submodule update --init --recursive

# 1. Build the node and proof-signer images.
docker build -t eez-node:local .
docker build -f Dockerfile.signer -t eez-proof-signer:local .

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
#   EEZ_L2_SYSTEM_KEY=<separate L2 system-transaction key>
#   EEZ_UNSAFE_BLOCK_SIGNER_KEY=<separate unsafe-payload signing key>
EEZ_DEPLOY_SKIP_SIMULATION=1 make deploy-protocol
```

This deploys EEZ + ECDSAProofSystem + the rollup manager, registers the
rollup, deploys the L1 bridge contracts, and writes **`deployments.env`**
(registry, proof system, rollup id, deploy block, bridge, and EEZL2 addresses).
The deploy derives the public L2 system address from `EEZ_L2_SYSTEM_KEY`,
generates and funds its canonical EEZL2 genesis, and registers that exact state
root; the private key is never written to `deployments.env`. It also writes the
L2 **`datadir/genesis.json`** whose timestamp is pinned to the deploy block. The
container loads `deployments.env` automatically; `.env.chiado`'s
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

### Endpoints & cross-chain fronts

| Endpoint | URL | Use |
|---|---|---|
| L2 RPC | `http://localhost:18688` | L2 `eth_*` |
| Embedded chiado L1 RPC | `http://localhost:18645` | L1 `eth_*` (the composer's L1 view) |
| **L1→L2 front** (Inbound) | `http://localhost:18999` | send L1-origin cross-chain txs here |
| **L2→L1 front** (Outbound) | `http://localhost:18998` | send L2-origin cross-chain txs here |

The two **cross-chain ingress fronts** are transparent proxies:
`eth_sendRawTransaction` sent to a front is held and composed into the next Sync
block; every other `eth_*` is forwarded to that front's source-chain RPC. They
use the compose env `EEZ_L1_XCHAIN_PORT` / `EEZ_L2_XCHAIN_PORT`; both ports are
required by `eez-composer`. The `eez-follower` and `eez-dev-node` binaries do not
start cross-chain ingress fronts. Upstreams are `EEZ_L1_RPC_URL` /
`EEZ_L2_RPC_URL` respectively.

`EEZ_MAX_USER_TXS_PER_BUNDLE` (compose, default `3`) caps how many user
cross-chain txs ride in one `postBatch` bundle. Raise it only against a builder
proven to include larger bundles atomically — rbuilder-chiado silently drops the
excess beyond ~3, which is lost tx inclusion, so measure before bumping.

### Signed unsafe-block P2P

`eez-composer` signs each canonicalized execution payload with
`EEZ_UNSAFE_BLOCK_SIGNER_KEY` and publishes it on the chain-scoped libp2p
topic `/eez/<chain-id>/4/blocks`. `eez-follower` requires the corresponding
`EEZ_UNSAFE_BLOCK_SIGNER_ADDRESS`; it verifies the chain-bound signature,
payload block hash, and safe-chain ancestry before importing through Engine
API. It never fetches unsafe heads from the sequencer RPC.

Configure `EEZ_P2P_LISTEN_ADDR` (default `/ip4/0.0.0.0/tcp/9300`) on each
node and give followers one or more comma-separated composer multiaddrs in
`EEZ_P2P_PEERS`. Gossip uses Snappy-compressed, libp2p-signed messages. A
bounded 256-payload request/response cache fills short gaps for late or
temporarily disconnected followers; L1 derivation remains the durable source
for safe history.

### Exercise it

`scripts/xchain-test.sh` is the cross-chain test driver (new-format successor to
`devnet-test.sh`). It deploys fresh targets/proxies/wrappers, drives the ingress
fronts through **both directions × all op types** (`setValue` / `setValueNoRet` /
`deposit` / `withdraw`) × **direct + wrapper**, then checks L1↔L2 reconciliation +
semantic effects and reports pipeline metrics (N+1 next-slot hit-rate,
consecutive-L1-slot landing, bundle drops/evictions, divergence). Every run mints
recipients + senders fresh, so re-runs never collide with stale state.

```bash
# MATRIX (default): waves of the full cross-chain matrix + pure-L2 + poison
EEZ_WAVE_COUNT=5 bash scripts/xchain-test.sh

# LOAD: high volume from distinct fresh senders, optionally paced + node restart
EEZ_MODE=load EEZ_IN_N=100 EEZ_OUT_N=100 bash scripts/xchain-test.sh            # burst
EEZ_MODE=load EEZ_IN_N=100 EEZ_OUT_N=100 EEZ_PACE_N=10 EEZ_PACE_INTERVAL=10 \
  bash scripts/xchain-test.sh                                                  # ~1 tx/s
EEZ_RESTART=1 EEZ_MODE=load ... bash scripts/xchain-test.sh                    # restart mid-run
```

(`scripts/devnet-test.sh` is the earlier, simpler driver — setter+deposit only,
raw-RPC — kept for reference.)

## Build, test, teardown

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                  # Rust; `cd contracts && forge test` for Solidity
bash scripts/teardown-chiado.sh         # stop eez-node + eez-proof-signer + lighthouse (datadirs untouched)
```
