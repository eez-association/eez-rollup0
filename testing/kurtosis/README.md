# Kurtosis E2E CI

This package is the pull-request gate for changes that affect composition,
sequencing, proving, settlement, or cross-chain execution. It runs the candidate
`eez-node` image against a private PoS L1, rbuilder, relay, proposer, and
embedded-L1 follower.

The PR topology has one validator pair and omits load generation, reorg tooling,
and observability services. Larger topologies belong in scheduled soak tests.

## CI flow

`run-ci.sh` owns the lifecycle:

1. Render `ci-args.yaml` with commit-specific candidate image tags.
2. Build the node and deployment images.
3. Start the reduced network and wait for a settled bundle inclusion.
4. Run the single cross-chain wave harness in `inbound`, `outbound`, and
   `mixed` modes.
5. Check convergence, settlement, L1/L2 state roots, and the L2 safe head in
   each mode.
6. Save a JSON result and service logs, then remove the enclave.

The workflow runs this package for relevant pull requests on a GitHub-hosted
Ubuntu runner. It installs and starts Kurtosis, uploads
`artifacts/kurtosis-e2e`, and has a separate unconditional cleanup step.

## Run on a CI-equivalent host

The host needs Docker, Kurtosis, Foundry, `jq`, `curl`, `openssl`, and the
initialized `sync-rollups-protocol` submodule.

```bash
bash testing/kurtosis/run-ci.sh
```

Useful overrides:

- `KURTOSIS_ENCLAVE`: enclave name.
- `EEZ_NODE_IMAGE`, `EEZ_DEPLOY_IMAGE`: candidate image tags.
- `EEZ_SKIP_NODE_BUILD=1`, `EEZ_SKIP_DEPLOY_BUILD=1`: reuse images.
- `EEZ_CI_RESULT_DIR`: result and diagnostic directory.
- `EEZ_CI_READY_TIMEOUT_SECS`: RPC readiness timeout.

## Layout

- `main.star` and `kurtosis.yml`: network definition.
- `ci-args.yaml`: reduced topology and private test keys.
- `run-ci.sh`: CI lifecycle entry point.
- `start.sh` and `stop.sh`: local lifecycle helpers.
- `scripts/verify-cross-chain-waves.sh`: runs the wave harness in all three modes.
- `scripts/cross-chain-wave.sh`: inbound, outbound, and mixed cross-chain workload.
