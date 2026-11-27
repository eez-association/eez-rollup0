# Provers

Both ECDSA backends live in [eez-association/provers](https://github.com/eez-association/provers),
pinned here as the `provers/` submodule. Each has its own Cargo workspace and lockfile:

| Workspace | Binary | Execution |
| --- | --- | --- |
| `provers/stateless-ecdsa/` | `eez-proof-signer` (`eez-prover-stateless` package) | Witness replay using `eez-prover-evm`, revm 43 and reth 2.7 |
| `provers/stateful-ecdsa/` | `eez-prover-stateful` | Default follower and existing stateful backend using `eez-evm`, evm2 and reth 2.11 |

The ordinary `eez-follower` library and binary stay in this repository. Stateful
imports that library from this checkout for integrated builds and from a pinned
Git revision for standalone builds. `eez_follower::run_with` validates
caller configuration before node startup and provides handles after historical
synchronization and L1 anchoring. Stateful attaches its service there and drains
it during node shutdown. The hook does not customize execution: both binaries
run the same default follower.

The execution-independent `eez-proof-signer` library also stays here. Both prover
workspaces import it for admission, settlement checks, streaming gRPC, public
inputs and attestations. The follower has no dependency on that library or either
prover. There is no root-to-prover Cargo dependency.

```mermaid
flowchart LR
  subgraph root[eez-rollup0 workspace]
    F[eez-follower] --> NC[eez-node-common / eez-deriver]
    NC --> E[eez-evm / evm2]
    PS[eez-proof-signer library] --> P[protocol crates]
  end
  subgraph stateful[provers/stateful-ecdsa workspace]
    SF[eez-prover-stateful]
  end
  subgraph stateless[provers/stateless-ecdsa workspace]
    SL[eez-prover-stateless] --> PE[eez-prover-evm / revm]
  end
  SF -->|checkout or standalone Git pin| F
  SF --> E
  SF --> PS
  SL --> PS
```

## First-window timing

The Composer defaults to `EEZ_PROVER_EARLY_WINDOW=true`. When it knows the
predecessor, it can send all available blocks and `Finalize` while the signer
confirms the anchor or checkpoint. Background prevalidation requires an exact
tip match. A proof that preempts background capture can reuse its last completed
checkpoint. After an interrupted proof or when the checkpoint is unknown,
ordinary `Resume` discovers the retained prefix.

Set `EEZ_PROVER_EARLY_WINDOW=false` to wait for `Ready` on each new attachment.
In both modes the Composer sends subsequent blocks as transport capacity allows.
It checks each reply in order and returns the first rejection, including when
later blocks or `Finalize` are already in flight.

## Building and testing here

```bash
git submodule update --init provers
scripts/provers-cargo.sh build --target-dir target -p eez-prover-stateless --bins
scripts/provers-cargo.sh --stateful build --target-dir target -p eez-prover-stateful --bins
scripts/provers-cargo.sh nextest run --target-dir target --workspace
scripts/provers-cargo.sh --stateful nextest run --target-dir target --workspace
```

The helper redirects every `eez-rollup0` package in the selected prover's
lockfile to this checkout. Stateful therefore builds and tests the current
follower and EVM, while stateless keeps its own execution stack in `provers/`.
CI checks both the source of root packages and execution dependency isolation.
Root's Docker builds and the e2e harness use the same helper. The helper restores
the selected lockfile on exit; redirected builds cannot use `--locked`.

For reproducible standalone builds, run Cargo inside the selected prover
directory with `--locked`; all root dependencies then use its pinned Git revision.
When root upgrades reth or evm2, stateful's direct dependencies and patches in
`stateful-ecdsa/Cargo.toml` must remain compatible with the current follower.
Integration CI should expose any required prover update. Advance stateful's root
Git pin to carry follower changes into standalone builds as well.

Execution-rule changes, including `0x45` native transactions, need matching
behavior in root `eez-evm` and stateless `eez-prover-evm`. Both replay
`provers/stateless-ecdsa/testing/evm-golden/execution.json`; the stateless workspace
owns the fixtures. Stateful uses root's EVM directly; carry any shared-signing
API and receipt-evidence changes into its backend too. Refresh stateless's copies
of streaming conformance vectors when the wire contract changes.

The e2e harness locates both executables beside the test profile or builds them
through the helper. `EEZ_TEST_PROOF_SIGNER_BIN` and
`EEZ_TEST_STATEFUL_PROVER_BIN` override their paths. Docker targets are
`proof-signer` and `stateful-prover`. Kurtosis `prover_backend: stateful` uses
the `stateful_prover_image` setting and runs `eez-prover-stateful node ...`.

## Migrating an existing stateful signer

Replace `eez-follower node ...` with `eez-prover-stateful node ...`, preserving
the chain, compatible datadir, L1 endpoints and `EEZ_STATEFUL_PROOF_SIGNER_*`
settings. The signer address and key are required; the ordinary follower rejects
them with a migration error. `EEZ_SEQUENCER_ADDRESS` is forbidden for proving.
Peer-assisted historical synchronization remains supported; `EEZ_SYNC_L1_ONLY`
is optional. L1 may be remote over RPC, with trace RPC and beacon access as
required by the posted data. The prover still owns its local L2 database.
