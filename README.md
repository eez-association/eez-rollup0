# eez-rollup0

An L2 rollup where L1 and L2 can call into each other as part of the same
flow. Four components do the work: a **sequencer** produces L2 blocks (it
drives a stock [reth](https://github.com/paradigmxyz/reth) node), a
**composer** posts those blocks to the `EEZ` contract on L1, a **prover**
(`eez-proverd`) re-executes each settling window and attests it, and a
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
- **Proves each settling window.** Before the composer posts, `eez-proverd`
  stateless-re-executes the window (`native-validate`), runs the settlement
  gates, and ECDSA-signs the recomputed `publicInputsHash`. The composer holds
  no signing key — it only verifies the attestation recovers to the registered
  attester, then posts. The on-chain `ECDSAProofSystem` re-checks the same
  signature, so settlement is gated by independent re-execution.
- **Commits first, repairs if needed.** The Sync block is added to L2 right
  away; the L1 submission is watched in the background, and if it doesn't make
  it onto L1 the L2 block is rolled back. L1 is the source of truth — L2 only
  keeps what L1 confirms.
- **Can be re-derived from L1.** A **follower** rebuilds the identical L2
  chain just by reading L1 (the `BatchPosted` events) and re-running the same
  transactions — no need to trust the sequencer.

> **Prover modes.** The default is the **real prover**: the out-of-process
> `eez-proverd` attests over the actual `publicInputsHash`, matched by the
> real `ECDSAProofSystem` on L1. For a quick liveness-only run there is a
> **mock** shortcut (`MockECDSAProofSystem` + an in-process ECDSA signer over a
> fixed digest, no `eez-proverd`) — see [Mock-prover shortcut](#mock-prover-shortcut).
> The `EEZ` contract enforces the chain of state roots itself, so both modes
> are sound for testing liveness and consistency; the real prover additionally
> gates settlement on independent re-execution.

## Run a chiado L2 (Docker)

Runs three containers: `eez-node` (which embeds a Chiado L1 node alongside the
L2 + composer), `eez-proverd` (the real prover), and a **lighthouse** consensus
client that drives the L1. There's no separate L1 node to run. Cross-chain
batches are submitted to Chiado's block builder; the L1 block to aim them at is
read from the embedded L1 once it has caught up to the chain tip.

The numbered steps below are the canonical bring-up. (`bash scripts/chiado-up.sh`
is a convenience wrapper for the **mock** quick-start only — it does not run the
prover; use the explicit steps for the real-prover stack.)

### One-time setup

```bash
git submodule update --init --recursive

# 1. Build the node image (~30 min cold; cargo-chef caches the reth deps).
docker build -t eez-node:local .

# 2. Build the prover image: eez-proverd + native-validate, both from source
#    (native-validate is compiled from our zisk-eth-client fork — see
#    Dockerfile.proverd; ~30 min cold, shares the cargo cache with step 1).
docker build -f Dockerfile.proverd -t eez-proverd:local .

# 3. Download a minimal chiado L1 snapshot (skips syncing from genesis).
mkdir -p data
docker run --rm -v "$PWD/data/chiado-l1:/data" \
  ghcr.io/gnosischain/reth_gnosis:v2.0.0 \
  download --chain chiado --minimal --datadir /data

# 4. Shared engine-API JWT for eez-node <-> lighthouse.
openssl rand -hex 32 > data/jwt.hex

# 5. Chiado consensus config (config.yaml, bootnodes, deploy_block.txt).
git clone https://github.com/gnosischain/configs.git /tmp/gnosis-configs
mkdir -p configs && cp -r /tmp/gnosis-configs/chiado configs/chiado

# 6. Fund the operator/poster keys with xDAI (https://faucet.chiadochain.net).
```

### Deploy the protocol (once, against a fully-synced chiado RPC)

The embedded L1 is still syncing at this point, so run the deploy against a
Chiado RPC that's already at the chain tip — the public endpoint, or a
standalone chiado-reth.

```bash
cp .env.example .env
#   EEZ_L1_RPC_URL=<tip chiado RPC>   EEZ_L1_POSTER_KEY=<poster key>
#   EEZ_PROOF_SIGNER_KEY=<attester key>   (its address becomes the proof
#                                          system's authorizedSigner == the
#                                          key eez-proverd signs with)
EEZ_PROOF_SYSTEM=real EEZ_DEPLOY_SKIP_SIMULATION=1 make deploy-protocol

cp datadir/genesis.json ./data/genesis-fresh.json
```

`EEZ_PROOF_SYSTEM=real` deploys the real `ECDSAProofSystem` (binds the actual
`publicInputsHash`); this deploys EEZ + the rollup manager, registers the
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

# The composer verifies each attestation recovers to the proof system's
# authorizedSigner. Set it to the address of EEZ_PROOF_SIGNER_KEY:
echo "EEZ_ATTESTER_ADDRESS=$(cast wallet address --private-key <EEZ_PROOF_SIGNER_KEY>)" >> .env.chiado

docker compose --env-file .env.chiado \
  -f docker-compose.chiado-node.yml \
  -f docker-compose.driven.override.yml up -d
```

The `docker-compose.driven.override.yml` overlay adds the `eez-proverd`
container and flips the node into remote-prover mode (`EEZ_PROVER_URL` +
`EEZ_ATTESTER_ADDRESS`, no local signing key). The embedded L1 checkpoint-syncs
(lighthouse, ~5 min) and catches up past the deploy block; then the L2
sequencer + composer + prover run. Health checks:

```bash
cast block-number --rpc-url http://localhost:18645          # embedded chiado L1 climbing
cast block-number --rpc-url http://localhost:18688          # L2 producing
docker logs eez-proverd 2>&1 | grep -m1 "re-executed"       # prover attesting windows
docker logs eez-node-chiado 2>&1 | grep -m1 "attested"      # composer accepting attestations
```

### Endpoints & cross-chain fronts

| Endpoint | URL | Use |
|---|---|---|
| L2 RPC | `http://localhost:18688` | L2 `eth_*` |
| Embedded chiado L1 RPC | `http://localhost:18645` | L1 `eth_*` (the composer's L1 view) |
| **L1→L2 front** (Inbound) | `http://localhost:18999` | send L1-origin cross-chain txs here |
| **L2→L1 front** (Outbound) | `http://localhost:18998` | send L2-origin cross-chain txs here |
| Prover `Prove` gRPC | `127.0.0.1:50061` | composer → `eez-proverd` (internal) |

The two **cross-chain ingress fronts** are transparent proxies:
`eth_sendRawTransaction` sent to a front is held and composed into the next Sync
block; every other `eth_*` is forwarded to that front's source-chain RPC. They
are enabled by the compose env `EEZ_L1_XCHAIN_PORT` / `EEZ_L2_XCHAIN_PORT`
(**unset ⇒ that front is disabled** — there is no default port). Upstreams are
`EEZ_L1_RPC_URL` / `EEZ_L2_RPC_URL` respectively.

`EEZ_MAX_USER_TXS_PER_BUNDLE` (compose, default `3`) caps how many user
cross-chain txs ride in one `postBatch` bundle. Raise it only against a builder
proven to include larger bundles atomically — rbuilder-chiado silently drops the
excess beyond ~3, which is lost tx inclusion, so measure before bumping.

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

### Mock-prover shortcut

For a quick liveness run without the prover, deploy the mock proof system and
bring up the base stack (no override, no `eez-proverd`):

```bash
make deploy-protocol                                    # EEZ_PROOF_SYSTEM=mock (default)
#   .env.chiado: EEZ_PROOF_SIGNER_KEY set (in-process signer); EEZ_ATTESTER_ADDRESS unneeded.
docker compose --env-file .env.chiado -f docker-compose.chiado-node.yml up -d
# or the wrapper (deploy + datadirs + up + health, mock only):
bash scripts/chiado-up.sh
```

The node signs the fixed mock digest in-process (`EEZ_PROOF_SIGNER_KEY`); the
`MockECDSAProofSystem` accepts it. This path does **not** re-execute windows, so
use the real prover for anything beyond a liveness smoke.

## Build, test, teardown

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                  # Rust; `cd contracts && forge test` for Solidity

# Tear down the stack (stops eez-node + eez-proverd + lighthouse):
docker compose --env-file .env.chiado \
  -f docker-compose.chiado-node.yml \
  -f docker-compose.driven.override.yml down
```
