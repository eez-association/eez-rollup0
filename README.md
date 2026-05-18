# eez-rollup0

L2 rollup node with a custom sequencer driving a near-vanilla reth via the engine API.

## Status

Stage 1 only — produces an L2 block every 2 seconds on a dev chain. No L1, no postBatch, no cross-chain yet. The Sequencer + Scheduler shape is in place; subsequent stages bolt new tasks alongside without rewriting the core.

## Run

```bash
cargo run -p eez-node -- node --chain dev --datadir /tmp/eez-rollup0-data
```

You should see an `eez.sequencer.block.produced` event every 2 seconds. `eth_blockNumber` over JSON-RPC advances by 1 per tick.

## Roadmap

### Done

- [x] **Stage 1** — sequenced reth (Sequencer + Scheduler + Eth attributes; engine-API consumer loop)

### To do

- [ ] **Stage 2** — postBatch submission (L2-only txs batched + posted to L1)
- [ ] **Stage 3** — reorg handling + `eez-follower` (derivation-based fullnode)
- [ ] **Stage 4** — cross-chain composer (sync blocks with system txs, proof, full L1↔L2)

See [`docs/plans/IMPLEMENTATION.md`](docs/plans/IMPLEMENTATION.md) for the full plan, per-stage breakdowns, and open spec items.
