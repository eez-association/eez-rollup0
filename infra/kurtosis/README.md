# Kurtosis EEZ devnet

Private L1 via [ethpandaops/ethereum-package](https://github.com/ethpandaops/ethereum-package) (12s slots, `reth-rbuilder`, spamoor). EEZ runs with `EEZ_L1_EMBEDDED=0`, so L1 is external and `eez-node` runs L2 only.

**Prereqs:** Kurtosis, Docker, `cast`, `forge`, `cargo`.

```bash
bash infra/kurtosis/scripts/kurtosis-up.sh
bash infra/kurtosis/scripts/parse-endpoints.sh
cp infra/kurtosis/eez.env.example infra/kurtosis/.env   # set poster/proof keys
bash infra/kurtosis/scripts/deploy-eez.sh
bash infra/kurtosis/scripts/run-eez-node.sh             # separate terminal
```

Teardown: `bash infra/kurtosis/scripts/kurtosis-down.sh`

> **Wait for MEV warmup before judging batch posting.** With `mev_type: flashbots`,
> the relay only proposes rbuilder blocks after ~epoch 4 (~25 min post-genesis):
> validators register after epoch 1, the relay ingests builder payloads after
> epoch 3, and proposers use relay headers after epoch 4. Before that, every EEZ
> bundle is dropped and the composer just retries — this is the usual cause of
> "one `BatchPosted` then nothing". Don't deploy/measure until L1 is past ~block 130.

> **Poster / proof keys — avoid reused prefunded indices.** ethereum-package
> prefunds 21 accounts from its standard mnemonic, but several indices are already
> claimed: 0 = builder coinbase, 3 = tx_fuzz, 8 = assertoor, 11/13 = flood/spamoor,
> 12 = contract deployer, 14 = rakoon. Point `EEZ_L1_POSTER_KEY` /
> `EEZ_PROOF_SIGNER_KEY` at an **unused** index (e.g. 1, 2, 4, 5) to avoid nonce
> and coinbase collisions.

## Verify it

Run these in another terminal while `run-eez-node.sh` is active:

```bash
source infra/kurtosis/endpoints.env
source deployments.env

# L1 and L2 should advance
cast block-number --rpc-url "$EEZ_L1_RPC_URL"
cast block-number --rpc-url "http://127.0.0.1:18688"

# Rollup registered on EEZ registry
cast call "$EEZ_REGISTRY_ADDRESS" "rollupCounter()(uint256)" --rpc-url "$EEZ_L1_RPC_URL"
cast call "$EEZ_REGISTRY_ADDRESS" "rollups(uint256)(address,bytes32,uint256)" "$EEZ_ROLLUP_ID" --rpc-url "$EEZ_L1_RPC_URL"

# Composer liveness: count BatchPosted events on the EEZ registry (not Rollup manager)
cast logs --from-block "$EEZ_REGISTRY_DEPLOY_BLOCK" \
  --address "$EEZ_REGISTRY_ADDRESS" \
  "BatchPosted(uint256)" \
  --rpc-url "$EEZ_L1_RPC_URL" | wc -l
sleep 20
cast logs --from-block "$EEZ_REGISTRY_DEPLOY_BLOCK" \
  --address "$EEZ_REGISTRY_ADDRESS" \
  "BatchPosted(uint256)" \
  --rpc-url "$EEZ_L1_RPC_URL" | wc -l
```

After `parse-endpoints.sh`, merge `infra/kurtosis/endpoints.env` into `.env` if ports differ from the defaults in `eez.env.example`.

Timing env (`EEZ_L1_BLOCK_TIME_MS=12000`, `EEZ_L2_BLOCK_TIME_MS=2000`, etc.) matches the `mainnet()` profile in `crates/eez-driver/src/timing.rs`.

For fewer nodes locally, lower `count` in `network_params.yaml`. Disruptoor needs `kurtosis run --privileged` (default in `kurtosis-up.sh`).

## Prove the rbuilder path (timestamp-pin correctness)

EEZ's `BundleTarget::Exact` pins `minTimestamp`/`maxTimestamp` so a batch settles in exactly the L1 slot its Sync block anchored to. A real rbuilder must *enforce* that pin — if it doesn't, batches land in the wrong slot silently. The Python test stub can't check this; `smoke-rbuilder.sh` does, against the live builder:

```bash
bash infra/kurtosis/scripts/smoke-rbuilder.sh      # run after MEV warmup (~block 130+)
```

It sends two value-0 bundles from the poster key: one pinned to the correct slot timestamp (must land) and one pinned to an impossible past timestamp (must be dropped). `PASS` = bundles land and the pin is enforced; `FAIL` = the builder ignores the pin (don't trust `Exact` on that image); `INCONCLUSIVE` = relay not warmed up or rbuilder not winning. Run this once per devnet before trusting settlement.

## Monitoring

`network_params.yaml` enables **Dora** (block/reorg explorer) and **Forkmon** (fork-choice monitor). `parse-endpoints.sh` writes their host URLs into `endpoints.env` when discoverable:

```bash
source infra/kurtosis/endpoints.env
echo "$EEZ_DORA_URL"          # open in a browser: block gas-used + reorg view
kurtosis enclave inspect eez-devnet   # fallback: list all published ports
```

Use Dora's gas-used panel to tune spamoor and to confirm reorg depth after a partition.

## Spamoor (L1 blockspace load)

`spamoor_params` runs one `eoatx` spammer at `throughput: 150` txs/block (~5% of the 60M gas limit) — low on purpose so EEZ `postBatch` bundles never get starved. Rough targets @ 60M / 12s (21k gas per eoatx):

| Target gas | Gas/block | eoatx/block |
|-----------|-----------|-------------|
| 10% | 6M  | ~285 |
| 30% | 18M | ~857 |
| 50% | 30M | ~1428 |

Raise `throughput` in `network_params.yaml` (needs re-`up`), or adjust live via the spamoor web UI (published under `additional_services`, see `endpoints.env` / `enclave inspect`). Keep block gas under ~50% so batches keep landing.

## Scheduled L1 reorgs

`scripts/reorg-scheduler.sh` polls L1 height and drives Disruptoor to partition the CL P2P network on a schedule, then heals so fork-choice reorgs the losing side out. Run it in its own terminal (after `parse-endpoints.sh` has populated `EEZ_DISRUPTOOR_URL`):

```bash
# Dry-run first — logs intended partitions without touching Disruptoor:
EEZ_REORG_DRY_RUN=1 bash infra/kurtosis/scripts/reorg-scheduler.sh

# Start with the shallow schedule only (calibrate before enabling deeper ones):
EEZ_REORG_SCHEDULES="shallow:1:20" bash infra/kurtosis/scripts/reorg-scheduler.sh

# Full schedule (default): 1-blk/20, 5-blk/100, 15-blk/1000
bash infra/kurtosis/scripts/reorg-scheduler.sh
```

Schedule format is `name:depth:every_blocks` (space-separated; the deepest schedule whose interval divides the current height wins). All knobs are env-overridable — see the header of `reorg-scheduler.sh` (`EEZ_REORG_MINORITY`/`MAJORITY`, `EEZ_REORG_HEAL_MARGIN_S`, etc.). The script always heals on exit, so Ctrl-C never leaves the network partitioned.

**Validate recovery:** after a reorg fires, `eez-node` logs should show L1 reorg markers (`l1.reorg` / retreat) and `BatchPosted` counts should keep growing. Confirm the observed depth in Dora/Forkmon. PoS reorg depth is attestation-weight dependent, so start shallow and calibrate the hold/minority split before enabling `medium`/`deep`. Requires `--privileged` (default in `kurtosis-up.sh`).

> **Disruptoor caveats.** It's a real upstream service (`disruptoor_params`,
> `src/disruptoor/`) but is **Docker-only and needs `--privileged`** (default in
> `kurtosis-up.sh`) — it won't run on Kubernetes. Whether its HTTP API is published
> to the host, and under which port name, varies by package version; if
> `parse-endpoints.sh` can't find `EEZ_DISRUPTOOR_URL`, get it from
> `kurtosis enclave inspect eez-devnet` and set it by hand. The scheduler probes
> `/v1/healthz` on startup and logs the full PUT response so you can confirm the
> request shape matched before trusting the schedule (see the note in the script).
> Pin the package version for reproducible reorg behavior.
