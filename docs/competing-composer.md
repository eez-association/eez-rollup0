# Joining as a competing composer

The eez rollup is **already deployed** on chiado (driven + deferred-post, real
`ECDSAProofSystem`). This is about **JOINING it from your own machine** — not deploying.
(Operator deploy → Appendix.)

## Model

A **based rollup**: the L2 is re-derived from L1 `postBatch` calldata, and any number of
composers may **compete** to post the next batch. The contract serializes via
`StateRootMismatch` (first to land wins); the others re-derive and try the next slot.
`mark_settled_on_l1` advances every composer's verified frontier by transitivity on **any**
landed batch — **no cross-composer coordination**. The live stack is **driven + deferred-post**
(`EEZ_PROOF_SYSTEM_KIND=real` switches on deferred-post; `EEZ_COMPOSER_DRIVEN=1` drives the
out-of-process `eez-proverd`), so every bring-up includes `-f docker-compose.driven.override.yml`.

| Shared (from the operator) | Yours |
|---|---|
| `deployments.env`, `datadir/genesis.json` (exact files) | a **distinct funded** `EEZ_L1_POSTER_KEY` |
| `EEZ_PROOF_SIGNER_KEY` (the one registered signer) | own `data/` volumes (fresh L2 datadir) |

Live network: registry `0xcb808454…`, real PS `0x86b8f7d3…`, `rollupId 1`.

## Join the rollup

```bash
# prereqs
docker run --rm -v "$PWD/data/chiado-l1:/data" ghcr.io/gnosischain/reth_gnosis:v2.0.0 \
  download --chain chiado --minimal --datadir /data
openssl rand -hex 32 > data/jwt.hex

# .env.chiado (from .env.chiado.example): your funded EEZ_L1_POSTER_KEY, the shared
# EEZ_PROOF_SIGNER_KEY, the relay URL. Place the operator's deployments.env + datadir/genesis.json.

docker compose --env-file .env.chiado \
  -f docker-compose.chiado-node.yml -f docker-compose.driven.override.yml build eez-proverd

BUILD_PROFILE=release-fast docker compose --env-file .env.chiado \
  -f docker-compose.chiado-node.yml -f docker-compose.chiado-node.dev.yml \
  -f docker-compose.driven.override.yml up -d
```

The node re-derives the rollup from L1 (fresh L2 datadir → `catch_up()` from the deploy block)
and starts competing — **no redeploy**. Success:

```bash
docker logs eez-node-chiado 2>&1 | grep -E "prover dispatch ENABLED|settled=true"
docker logs eez-proverd     2>&1 | grep -E "received verify directive|ATTESTED"
```

Each competing composer needs its **own funded** poster EOA (sharing one collides on L1 nonces)
and a **fresh** L2 datadir (each re-derives independently from L1).

## Appendix — One-time operator deploy (already done for the live network)

```bash
# .env: EEZ_L1_RPC_URL, a funded EEZ_L1_POSTER_KEY, EEZ_PROOF_SIGNER_KEY, EEZ_PROOF_SYSTEM=real
EEZ_PROOF_SYSTEM=real ./scripts/deploy.sh   # → deployments.env + datadir/genesis.json
```

Hand `deployments.env` + `datadir/genesis.json` + `EEZ_PROOF_SIGNER_KEY` to joiners.

> Offline driven-path check (no chiado): `bash scripts/driven-dispatch-e2e.sh`.
> Operational note: the chiado rbuilder relay has been seen dropping this rollup's bundles — treat
> persistent non-inclusion as a relay issue, not a config bug.
