# Production-path CI

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
3. Start the reduced network and wait for rbuilder warmup.
4. Check timestamp bounds and atomic rejection at the builder.
5. Run a healthy mixed bidirectional wave after the rejected bundle.
6. Check composer bundle receipts for all-or-none, ordered inclusion.
7. Save a JSON result and service logs, then remove the enclave.

The workflow runs this package for relevant pull requests on a GitHub-hosted
Ubuntu runner. It installs and starts Kurtosis, uploads
`artifacts/production-path-ci`, and has a separate unconditional cleanup step.

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
- `EEZ_CI_BUILDER_WARMUP_BLOCK`: first builder probe block.

## Layout

- `main.star` and `kurtosis.yml`: network definition.
- `ci-args.yaml`: reduced topology and private test keys.
- `run-ci.sh`: CI lifecycle entry point.
- `start.sh` and `stop.sh`: local lifecycle helpers.
- `scripts/verify-production-path.sh`: invariant entry point.
- `scripts/assert-*.sh`: focused builder and receipt checks.
- `scripts/cross-chain-wave.sh`: mixed cross-chain workload.
