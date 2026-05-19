# eez-rollup0

L2 rollup node with a custom sequencer driving a near-vanilla reth via the engine API.

## Status

Stage 1 only — produces an L2 block every 2 seconds on a dev chain. No L1, no postBatch, no cross-chain yet. The Sequencer + Scheduler shape is in place; subsequent stages bolt new tasks alongside without rewriting the core.

## Run

### Sequencer

```bash
cargo run -p eez-node -- node --chain dev --datadir /tmp/eez-rollup0-data \
  --http --http.port 8545
```

You should see an `eez.sequencer.block.produced` event every 2 seconds. `eth_blockNumber` over JSON-RPC advances by 1 per tick. The follower section below requires this HTTP port to be open.

### Follower

Tracks the sequencer's chain by polling its JSON-RPC for the current head hash and driving local reth via FCU; block bodies and receipts flow over reth's existing devp2p (eth/68). Start the sequencer first and grab its enode URL from the startup log.

`--disable-discovery` keeps the follower from binding discv5 on the same machine as the sequencer (overriding only `--discovery.port` leaves discv5 on its default and collides); the follower reaches the sequencer via `--trusted-peers` instead.

```bash
cargo run -p eez-follower -- node --chain dev \
  --datadir /tmp/eez-follower-data \
  --trusted-peers <enode-from-sequencer-startup-log> \
  --sequencer-rpc http://127.0.0.1:8545 \
  --http --http.port 8645 \
  --port 30403 --discovery.port 30403 --authrpc.port 8651 \
  --disable-discovery
```

Expect an `eez.follower.head.syncing` event every 2 seconds once the sequencer is producing. The label is reth's own engine-API response code — in steady state every FCU points at a block reth has not yet imported (the sequencer always wins the race) — but the chain is advancing. Confirm via reth's `Block added to canonical chain` log lines, or by polling `eth_blockNumber` on `http://127.0.0.1:8645` and comparing against the sequencer.

## Roadmap

### Done

- [x] **Stage 1** — sequenced reth (Sequencer + Scheduler + Eth attributes; engine-API consumer loop)

### To do

- [ ] **Stage 2** — postBatch submission (L2-only txs batched + posted to L1)
- [ ] **Stage 3** — reorg handling + `eez-follower` derivation (the follower binary itself ships ahead of Stage 3; derivation + reorg handling are still open)
- [ ] **Stage 4** — cross-chain composer (sync blocks with system txs, proof, full L1↔L2)

See [`docs/plans/IMPLEMENTATION.md`](docs/plans/IMPLEMENTATION.md) for the full plan, per-stage breakdowns, and open spec items.
