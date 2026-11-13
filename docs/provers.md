# Provers

The stateless proof signer (`eez-proof-signer`) lives in
[eez-association/provers](https://github.com/eez-association/provers) under
`stateless-ecdsa/`, pinned here as the `provers/` submodule. It runs reth v2.7.0
(`fa9e76a`), revm 43 and the stateless fork
(`eez-association/stateless@935f25a`) independently of the reth and EVM the
node links. The node and the signer meet only at the `prove.v1`/`prove.v2` gRPC
boundary, so the signer re-executes every window with its own EVM.

The signer pipeline itself (`crates/eez-proof-signer`: gRPC service, window
validation, settlement checks, attestation) stays here with the other protocol
crates. Both backends plug into it: the stateless backend in provers, the
stateful backend in `eez-follower`.

## Crates provers uses

`provers/stateless-ecdsa` takes `eez-primitives`, `eez-protocol`,
`eez-proof-signer`, `eez-payload-codec`, `eez-blob`, `eez-control-rpc`,
`eez-prover` and `eez-prover-client` as git dependencies on one eez-rollup0
commit. It compiles them against its own lockfile, so they follow two rules:

- **No git-pinned reth, EVM or stateless crate.** They may use crates.io crates
  whose version range both workspaces accept (alloy, reth-core's
  `reth-primitives-traits`/`reth-codecs`/`reth-rpc-traits`, `alloy-hardforks`).
  Node-only types live in node crates (`eez-engine` holds the Engine API types).
- **Requirements are floors both sides accept.** A shared crate inherits its
  dependency requirements from the root `[workspace.dependencies]`, even when
  provers builds it.

`eez-primitives` keeps the revm `TxEnv` conversions for `EezTxEnvelope` behind
its off-by-default `revm` feature. The orphan rule keeps them in the crate that
owns the envelope. provers enables the feature; the node's EVM crate enables it
only while it is itself revm-based. Cargo lists optional dependencies in every
lockfile, so the root `Cargo.lock` names revm even when nothing builds it.

## Building the signer here

`scripts/provers-cargo.sh` runs cargo on `provers/stateless-ecdsa` with those
crates redirected to this checkout, so a change to them is built and tested
with the signer in the same PR. CI, the e2e harness and the Docker
`proof-signer` target build the signer this way.

```bash
git submodule update --init provers
scripts/provers-cargo.sh build --target-dir target -p eez-prover-stateless --bin eez-proof-signer
scripts/provers-cargo.sh nextest run --target-dir target --workspace
```

The redirect rewrites the signer's `Cargo.lock`, so these runs cannot use
`--locked`; the script restores the lockfile on exit. `--target-dir target`
puts the binary in the root `target/<profile>/`, where the e2e harness looks
for it. `EEZ_TEST_PROOF_SIGNER_BIN` overrides the path.

## Keeping the two repositories in step

- A change to the shared crates lands here first, tested with the signer as
  above. provers then moves its eez-rollup0 pin to that commit on `main`, and
  this repository bumps the submodule when the signer itself changes.
- A change to the EEZ execution rules (0x76 native transactions, the fee
  collector, the block executor) lands in `crates/eez-evm` here and in provers'
  `crates/eez-prover-evm`.
- Both EVMs replay provers' golden fixtures
  (`provers/stateless-ecdsa/testing/evm-golden/execution.json`) and must match
  them exactly; `crates/eez-evm/tests/golden.rs` reads them through the
  submodule. Only provers records them.
- provers keeps copies of the `prove.v2` conformance vectors and replayer in
  `crates/eez-control-rpc/conformance/` for its streaming RPC test. A change to
  them is copied there.
- `[patch]` is read only from the root of the workspace being built, so provers
  carries its own reth patch.
