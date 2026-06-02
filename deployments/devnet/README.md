# eez-rollup0 devnet

Local Stage 2 devnet: starts an L1 `reth --dev`, deploys the EEZ registry,
mock proof system, rollup manager, and rollup id with `scripts/deploy.sh`,
then starts the L2 sequencer/composer against that L1.

## Quick Start

```bash
git submodule update --init --recursive
cp deployments/devnet/.env.example deployments/devnet/.env
docker compose -f deployments/devnet/docker-compose.yml up --build
```

RPC endpoints:

| Endpoint | URL |
| --- | --- |
| L1 HTTP | `http://127.0.0.1:9555` |
| L1 WS | `ws://127.0.0.1:9556` |
| L2 HTTP | `http://127.0.0.1:9545` |
| L2 WS | `ws://127.0.0.1:9546` |

Check L2 block production:

```bash
cast block-number --rpc-url http://127.0.0.1:9545
```

Watch batch posting:

```bash
docker compose -f deployments/devnet/docker-compose.yml logs -f sequencer
```

Run with simple L2 transfer traffic:

```bash
docker compose -f deployments/devnet/docker-compose.yml --profile traffic up --build
```

Check the live devnet:

```bash
deployments/shared/scripts/check-devnet.sh
```

## Reset

```bash
docker compose -f deployments/devnet/docker-compose.yml down -v
```

The `deploy` service skips work when the shared deployment volume already has
`deployments.env` and `genesis-l2.json`. Use `down -v` for a fresh L1,
fresh contracts, and fresh L2 datadir.

## Build Notes

The compose stack builds the sequencer runtime image inside Docker so the
container always gets a Linux `eez-node` binary. The first build is slow on a
cold cache; subsequent builds reuse Docker BuildKit cache mounts for Cargo
registry, git, and target data.

The deploy image also needs the `sync-rollups-protocol` submodule and its
nested Foundry dependencies. If Forge reports missing `sync-rollups-protocol`
or `forge-std` imports, run:

```bash
git submodule update --init --recursive
```
