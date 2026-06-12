# eez-rollup0

L2 rollup node with a custom sequencer driving a near-vanilla reth via the engine API.

## Status

Stages 1 and 2 done — `eez-node` produces L2 blocks every 2s and posts contiguous batches to `EEZ.postAndVerifyBatch` on the configured L1 every minute. Batches carry only L2 user txs in stage 2; cross-chain entries arrive in stage 4.

## Run

```bash
git submodule update --init --recursive

cp .env.example .env                 # fill in L1 RPC, poster + proof-signer keys, datadir
make deploy-protocol                 # deploys EEZ + MockECDSAProofSystem + Rollup manager + creates rollupId
                                     # paste each printed address into .env
make run-node                        # spawns sequencer + composer (composer drives the submitter)
```

Run without `.env` (sequencer-only smoke test):

```bash
cargo run -p eez-node -- node --chain dev --datadir /tmp/eez-rollup0-data
```

Logs you should see: `eez.sequencer.block.produced` every 2s; `eez.composer.batch.posted` every 60s when L1 config is present.

### Follower

Runs a reth node with the PR #5 L1 watcher + deriver. L1 `BatchPosted` events are decoded, reconciled against local blocks, replayed into reth when needed, and used to advance the FCU `safe` / `finalized` anchors. The follower can also poll a sequencer RPC for a faster unsafe `head`; that RPC head is only accepted while compatible with the L1-derived anchors.

L1 config is mandatory: `EEZ_L1_RPC_URL`, `EEZ_REGISTRY_ADDRESS`, `EEZ_ROLLUP_ID`, and `EEZ_REGISTRY_DEPLOY_BLOCK` must be set. `EEZ_L1_POSTER_KEY` is optional for the follower because it only reads from L1. Start the sequencer first if you want sequencer-RPC unsafe head, and grab its enode URL from the startup log.

```bash
cargo run -p eez-follower -- node --chain dev \
  --datadir /tmp/eez-follower-data \
  --trusted-peers <enode-from-sequencer-startup-log> \
  --sequencer-rpc http://127.0.0.1:8545 \
  --http --http.port 8645 \
  --port 30403 --authrpc.port 8651 \
  --disable-discovery
```

To run L1-derived-only, omit `--sequencer-rpc` and peer flags. In that mode the head advances from L1 batches rather than the sequencer RPC.

Logs you should see: `eez.deriver.catch_up.start` on boot, `eez.deriver.safe.advanced` as L1 batches are accepted, `eez.deriver.finalized.advanced` as L1 finality catches up, and, when sequencer RPC is enabled, `eez.follower.head.advanced`, `.head.syncing`, or `.head.inconsistent` for unsafe-head polling. Confirm convergence with `cast block-number`, `cast block safe`, and `cast block finalized` against the follower RPC.

## Roadmap

### Done

- [x] **Stage 1** — sequenced reth (Sequencer + Scheduler; engine-API consumer loop)
- [x] **Stage 2** — postBatch submission (upstream `EEZ.sol` + `MockECDSAProofSystem` + `eez-payload-codec` + `eez-prover` + `eez-l1::{Composer, Submitter}`; stateless, restart-safe, reorg-safe)

### To do

- [ ] **Stage 3** — fuller follower hardening (follower binary now runs L1-derived safe/finalized and optional sequencer-RPC unsafe head; remaining work includes deeper recovery behavior and production-grade validation)
- [ ] **Stage 4** — cross-chain composer (sync blocks with system txs, proof, full L1↔L2)
