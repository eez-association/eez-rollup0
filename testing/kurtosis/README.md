# Kurtosis E2E CI

This package is the pull-request gate for changes that affect composition,
sequencing, proving, settlement, or cross-chain execution. It runs the candidate
`eez-node` and `eez-proof-signer` images against a private PoS L1, rbuilder,
relay, proposer, and embedded-L1 follower.

The PR topology has one validator pair and omits load generation, reorg tooling,
and observability services. Larger topologies belong in scheduled soak tests.

## CI flow

`run-ci.sh` owns the lifecycle:

1. Render `ci-args.yaml` with commit-specific candidate image tags.
2. Build the node, proof-signer, and deployment images.
3. Derive the configured L2 system address, generate its EEZL2 runtime and
   funded genesis, register that exact state root, and start the reduced network.
   The live EEZL2 immutables, code hash, and genesis root are checked before
   workloads run.
4. Run the cross-chain wave harness once in `inbound`, `outbound`, and `mixed`
   modes, followed by a three-wave `mixed-pure` stress run.
5. Check convergence, settlement, L1/L2 state roots, and the L2 safe head in
   each mode.
6. Require evidence that the proof signer signed at least one window and the
   node accepted at least one remote attestation, while rejecting signer
   validation, invariant, or signing failures.
7. Save a JSON result with source commits, candidate images, proof-flow counts,
   and service logs, then remove the enclave.

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
- `EEZ_NODE_IMAGE`, `EEZ_PROOF_SIGNER_IMAGE`, `EEZ_DEPLOY_IMAGE`: candidate image tags.
- `EEZ_SKIP_NODE_BUILD=1`, `EEZ_SKIP_PROOF_SIGNER_BUILD=1`,
  `EEZ_SKIP_DEPLOY_BUILD=1`: reuse images.
- `EEZ_PRUNE_BUILD_CACHE=1`: discard BuildKit cache after creating the images.
- `EEZ_CI_RESULT_DIR`: result and diagnostic directory.
- `EEZ_CI_READY_TIMEOUT_SECS`: RPC readiness timeout.
- `EEZ_MIXED_PURE_WAVE_COUNT`: number of `mixed-pure` stress waves (default: 3).

## Layout

- `main.star` and `kurtosis.yml`: network definition.
- `ci-args.yaml`: reduced topology and private test keys.
- `l2-genesis-profile.json`: reproducible public inputs and hashes for the
  committed test genesis; it contains no private key.
- `run-ci.sh`: CI lifecycle, proof-flow gate, and result owner.
- `start.sh` and `stop.sh`: local lifecycle helpers.
- `scripts/verify-cross-chain-waves.sh`: runs the three focused modes and the
  `mixed-pure` stress mode; it does not decide or write the final CI result.
- `scripts/cross-chain-wave.sh`: inbound, outbound, mixed, and `mixed-pure`
  cross-chain workload.
- `scripts/verify-eezl2-deployment.sh`: compares live EEZL2 getters and code
  against the generated deployment bindings.
