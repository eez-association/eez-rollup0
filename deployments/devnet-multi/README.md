# eez-rollup0 multi-sequencer devnet

Experimental Stage 2 devnet with one local L1 and four independent L2
sequencers registered against the same rollup. The sequencers use separate
datadirs and separate L1 poster keys, share the deploy-time proof signer, and
run with `EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES=true`.

This is useful for exercising contention, external-batch observation, and
reorg/replay behavior. It is not a production consensus topology.

## Quick Start

```bash
git submodule update --init --recursive
cp deployments/devnet-multi/.env.example deployments/devnet-multi/.env
docker compose -f deployments/devnet-multi/docker-compose.yml up --build
```

The `tx-sender` service starts automatically and sends simple L2 transfers to
all four sequencers.

RPC endpoints:

| Endpoint | URL |
| --- | --- |
| L1 HTTP | `http://127.0.0.1:9655` |
| L1 WS | `ws://127.0.0.1:9656` |
| Sequencer 1 HTTP | `http://127.0.0.1:9645` |
| Sequencer 2 HTTP | `http://127.0.0.1:9647` |
| Sequencer 3 HTTP | `http://127.0.0.1:9649` |
| Sequencer 4 HTTP | `http://127.0.0.1:9651` |

Watch all sequencers and sender:

```bash
docker compose -f deployments/devnet-multi/docker-compose.yml logs -f \
  sequencer1 sequencer2 sequencer3 sequencer4 tx-sender
```

Check block production:

```bash
cast block-number --rpc-url http://127.0.0.1:9645
cast block-number --rpc-url http://127.0.0.1:9647
cast block-number --rpc-url http://127.0.0.1:9649
cast block-number --rpc-url http://127.0.0.1:9651
```

## Reset

```bash
docker compose -f deployments/devnet-multi/docker-compose.yml down -v
```

Use `down -v` for a fresh L1, fresh contracts, and fresh sequencer datadirs.
