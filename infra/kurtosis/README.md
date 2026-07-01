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

## Verify it

Run these in another terminal while `run-eez-node.sh` is active:

```bash
source infra/kurtosis/endpoints.env
source deployments.env

# L1 and L2 should advance
cast block-number --rpc-url "$EEZ_L1_RPC_URL"
cast block-number --rpc-url "http://127.0.0.1:18688"

# Composer liveness: batchesPosted should increase over time
cast call "$EEZ_ROLLUP_MANAGER_ADDRESS" "batchesPosted(uint256)(uint256)" "$EEZ_ROLLUP_ID" --rpc-url "$EEZ_L1_RPC_URL"
sleep 20
cast call "$EEZ_ROLLUP_MANAGER_ADDRESS" "batchesPosted(uint256)(uint256)" "$EEZ_ROLLUP_ID" --rpc-url "$EEZ_L1_RPC_URL"
```

After `parse-endpoints.sh`, merge `infra/kurtosis/endpoints.env` into `.env` if ports differ from the defaults in `eez.env.example`.

Timing env (`EEZ_L1_BLOCK_TIME_MS=12000`, `EEZ_L2_BLOCK_TIME_MS=2000`, etc.) matches the `mainnet()` profile in `crates/eez-driver/src/timing.rs`.

For fewer nodes locally, lower `count` in `network_params.yaml`. Disruptoor needs `kurtosis run --privileged` (default in `kurtosis-up.sh`).
