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

## Roadmap

### Done

- [x] **Stage 1** — sequenced reth (Sequencer + Scheduler; engine-API consumer loop)
- [x] **Stage 2** — postBatch submission (upstream `EEZ.sol` + `MockECDSAProofSystem` + `eez-payload-codec` + `eez-prover` + `eez-l1::{Composer, Submitter}`; stateless, restart-safe, reorg-safe)

### To do

- [ ] **Stage 3** — reorg handling + `eez-follower` (derivation-based fullnode)
- [ ] **Stage 4** — cross-chain composer (sync blocks with system txs, proof, full L1↔L2)

