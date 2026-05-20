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

Tracks the sequencer's chain. Unsafe `head` comes from the sequencer's standard JSON-RPC; `safe` and `finalized` come from tailing `EEZ.BatchPosted` events on L1 and mapping each batch's L1 inclusion block against L1's own `safe` / `finalized` block tags. Block bodies and receipts flow over reth's existing devp2p (eth/68) — the FCU just points reth at the head and pipeline-sync handles the rest.

L1 is mandatory — all `EEZ_*` env vars used by the sequencer must be set in the follower's environment too (the easiest path is to source the same `.env` + `deployments.env`). Start the sequencer first and grab its enode URL from the startup log.

```bash
cargo run -p eez-follower -- node --chain dev \
  --datadir /tmp/eez-follower-data \
  --trusted-peers <enode-from-sequencer-startup-log> \
  --sequencer-rpc http://127.0.0.1:8545 \
  --http --http.port 8645 \
  --port 30403 --authrpc.port 8651 \
  --disable-discovery
```

Logs you should see: `eez.follower.l1.bootstrap.complete` once at startup, `eez.follower.l1.batch.observed` per posted batch, `eez.follower.l1.safe.advanced` and `.finalized.advanced` when L1 tags cross batch boundaries, and `eez.follower.head.syncing` (or `.head.advanced`) each 2 s tick. Confirm convergence via `cast block-number` and `cast block safe` / `cast block finalized` on the follower's RPC (`http://127.0.0.1:8645`) vs the sequencer's.

## Roadmap

### Done

- [x] **Stage 1** — sequenced reth (Sequencer + Scheduler; engine-API consumer loop)
- [x] **Stage 2** — postBatch submission (upstream `EEZ.sol` + `MockECDSAProofSystem` + `eez-payload-codec` + `eez-prover` + `eez-l1::{Composer, Submitter}`; stateless, restart-safe, reorg-safe)

### To do

- [ ] **Stage 3** — reorg handling + full `eez-follower` derivation (follower binary itself ships now with L1-derived `safe`/`finalized`; trustless full derivation from L1 batches and reorg handling still open)
- [ ] **Stage 4** — cross-chain composer (sync blocks with system txs, proof, full L1↔L2)

