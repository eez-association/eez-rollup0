# EEZ private PoS L1 devnet (Phase 1)

A self-contained L1 where **eez-node's embedded reth is driven by a real
external Lighthouse CL** on a genesis we generate ourselves. Unlike the
Kurtosis harness (`infra/kurtosis/`), the L1 EL runs *in-process* with
eez-node — so `EvmComposer` gets direct `StateProviderFactory` access and
**cross-chain composition works**, while block production is real PoS
fork-choice (a prerequisite for MEV/rbuilder and reorg testing later).

## Why this exists

`EvmComposer` (cross-chain) requires an in-process reth provider — it cannot
run over JSON-RPC. The Kurtosis L1 is remote containers, so it can only test
settlement, never cross-chain. This harness keeps the EL in-process (like the
Chiado embedded mode) but on a **private, resettable, single-machine** chain,
so we can eventually layer rbuilder + spamoor + scheduled reorgs on top and
test cross-chain *and* settlement together.

## Architecture

```
        ┌─────────────────── your host ───────────────────┐
        │                                                  │
        │  eez-node (composer, EEZ_L1_EMBEDDED=1,          │
        │            EEZ_L1_CHAIN=devnet)                  │
        │  ┌────────────────────────────────────┐         │
        │  │ embedded reth (EthereumNode)        │         │
        │  │   • in-process provider ──► LocalChainClient  │
        │  │                          ──► EvmComposer ✓    │
        │  │   • JSON-RPC :18545                 │         │
        │  │   • engine API :18546 ◄──JWT──┐     │         │
        │  └───────────────────────────────┼─────┘         │
        │            ▲ postBatch            │               │
        │            │ (mempool in P1)      │  docker compose│
        │  ┌─────────┴──────┐      ┌────────┴─────────────┐ │
        │  │ eez-node L2    │      │ Lighthouse BN + VC   │ │
        │  │ reth (:18688)  │      │ (64 genesis vals)    │ │
        │  └────────────────┘      └──────────────────────┘ │
        └──────────────────────────────────────────────────┘
```

Phase 2 inserts an rbuilder + mev-boost between eez-node's submitter and the
EL; Phase 3 folds the whole thing into Kurtosis for spamoor + multi-node
reorgs. Neither changes the Phase 1 code path — they attach to a node that,
to them, is just a reth+CL pair.

## What Phase 1 proves

1. An external CL produces + finalizes blocks on the embedded EL over engine
   API (no auto-mine).
2. `EvmComposer` initializes and runs against that CL-driven embedded L1
   (the cross-chain path the Kurtosis harness structurally cannot exercise).
3. EEZ posts batches and `BatchPosted` events appear on the private L1.

## Prereqs

Docker, `cargo`, `cast`, `openssl`. The genesis generator and Lighthouse run
in Docker; eez-node runs on the host.

## Run

```bash
# 1. Generate matched EL+CL genesis, validator keys, and the shared JWT.
bash infra/devnet-l1/scripts/gen-genesis.sh

# 2. Configure. Fund poster/proof keys from the mnemonic in config/values.env
#    (index 0 is the prefunded deployer/faucet).
cp infra/devnet-l1/.env.example infra/devnet-l1/.env
$EDITOR infra/devnet-l1/.env

# 3. Start the CL (beacon + validator). It idles until GENESIS_DELAY passes.
docker compose --env-file infra/devnet-l1/.env \
  -f infra/devnet-l1/docker-compose.cl.yml up -d

# 4. Start eez-node with the embedded devnet L1 (separate terminal).
bash infra/devnet-l1/scripts/run-devnet-l1.sh
```

## Verify

```bash
# L1 advancing = CL is driving the embedded EL (NOT auto-mine).
watch -n2 cast block-number --rpc-url http://127.0.0.1:18545

# eez-node log must show EvmComposer wired (cross-chain alive), NOT the
# "embedded L1 not active; cross-chain EvmComposer disabled" branch:
rg "embedded L1 reth \(EthereumNode\) ready|cross-chain composer" /path/to/eez-node.log

# BatchPosted on the private L1 (needs protocol deployed + rollup registered):
cast logs --from-block 0 --address "$EEZ_REGISTRY_ADDRESS" \
  "BatchPosted(uint256)" --rpc-url http://127.0.0.1:18545 | wc -l
```

If L1 height stays 0: the CL isn't driving the EL. Check `docker compose logs
beacon` for engine-API auth errors (JWT mismatch) or genesis mismatch (EL
`genesis.json` chainId != CL `config.yaml`) — both mean gen-genesis.sh output
and the eez-node `EEZ_L1_CHAIN_PATH` / `EEZ_L1_JWT_SECRET` have drifted apart;
regenerate and wipe `EEZ_L1_DATADIR` + the compose `*-data` dirs.

## Reset

```bash
docker compose --env-file infra/devnet-l1/.env -f infra/devnet-l1/docker-compose.cl.yml down -v
rm -rf infra/devnet-l1/data /tmp/eez-devnet-l1 /tmp/eez-devnet-l2
bash infra/devnet-l1/scripts/gen-genesis.sh   # fresh genesis time
```

## Status / caveats

- **Code side is done and compiles**: `L1ChainKind::Devnet` +
  `build_devnet_node_config` in `crates/eez-node/src/l1_embedded.rs`, wired in
  `main.rs` (reuses the `EmbeddedL1::Dev` handle type, so EvmComposer needs no
  changes).
- **Infra side (this dir) needs a live validation pass on a Docker host** — it
  has not been run in CI here. The Lighthouse flags mirror the proven
  `docker-compose.chiado.yml`; the genesis-generator image tag and its exact
  output layout (`metadata/`, `validator-keys/keys/`) should be confirmed
  against the pinned image and adjusted if the paths differ. Treat the first
  `up` as a bring-up/debug session, not a one-shot.
- **Phase 1 has no rbuilder**: postBatch bundles degrade to ordered mempool
  submission on the EL RPC. Adding the rbuilder (so `eth_sendBundle` is used)
  is Phase 2.
