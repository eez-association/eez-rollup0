# eez-prover-stateless

`eez-prover-stateless` is the witness-backed implementation of the shared
`eez-proof-signer` service. It loads an operator-selected chain document,
re-executes each Composer block with Stateless/Reth, and returns execution
evidence to the shared settlement and attestation pipeline.

The package produces the existing `eez-proof-signer` binary so deployment
commands and container entrypoints remain compatible.

Backend implementation:

- [`src/backend.rs`](src/backend.rs) performs witness-backed replay;
- [`src/backend/chain_config.rs`](src/backend/chain_config.rs) loads the chain
  trust input; and
- [`src/config.rs`](src/config.rs) owns standalone CLI and environment parsing.

The wire contract remains [`../eez-proof-signer/SPEC.md`](../eez-proof-signer/SPEC.md).
