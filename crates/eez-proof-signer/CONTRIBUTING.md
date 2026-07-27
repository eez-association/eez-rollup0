# eez-proof-signer contributor guide

`eez-proof-signer` is a security-sensitive Rust daemon. It re-executes one
Composer-streamed L2 block window, binds a supported `PostBatch` to the derived
execution evidence, and signs only the independently recomputed public-input
hash.

## Authority and reading order

1. [`SPEC.md`](SPEC.md) is the normative protocol and compatibility contract.
2. [`docs/README.md`](docs/README.md) links the non-normative implementation
   explainers.
3. Source and tests show how the current Rust implementation satisfies that
   contract.

If these disagree, do not loosen a check to make the disagreement disappear.
Identify whether the implementation or the normative requirement is wrong,
then update code, tests, and documentation deliberately.

Code comments must explain the current invariant or design reason in plain
language. Do not cite spec section numbers from source comments, narrate old
implementations, or leave migration history that will become stale. Links to
the spec are appropriate in repository documentation.

## Current implementation

- Production always uses the in-process Stateless backend. Stateful and ZisK
  backends are future work and are not selectable; the stub backend exists only
  in tests.
- Exactly one request may be active across all connections. An overlap is
  rejected, not queued. The request permit covers ingestion through response
  construction and remains with a running detached worker until it exits.
- Validation and settlement run in one blocking task. Cancellation is
  cooperative between non-interruptible units; graceful shutdown waits for
  detached work rather than terminating it halfway through execution.
- The active settlement profile accepts an anchor followed by zero or more
  supported outbound effects and then zero or more supported successful inbound
  effects. Richer or ambiguous shapes fail closed.
- Every successful `BackendWindowOutput` associates each block's computed hash,
  post-state root, receipt coverage, selected checkpoints, and settlement
  evidence in one `BackendBlockOutput`. Preceding-block checkpoint vectors are
  empty; the settling vector is the exact locally derived selection and may
  also be empty.
- Shared checks consume the backend output and admitted Composer input to
  produce one `ValidatedWindow`. Settlement receives that normalized value
  rather than parallel, unchecked block/output vectors.

See [architecture](docs/architecture.md),
[data provenance](docs/data-provenance.md),
[request lifecycle](docs/request-lifecycle.md),
[validation evidence](docs/validation-evidence.md), and the
[settlement pipeline](docs/settlement-pipeline.md) for the implementation model.

## Non-negotiable invariants

- Treat every Composer field as untrusted, including range and hash claims,
  block RLP, witnesses, `PostBatch` calldata, and the wire
  `public_inputs_hash`.
- Never choose a range, skip a streamed block, generate a zk proof, submit an
  L1 transaction, or sign after any failed gate.
- Re-derive block identity, execution results, roots, transaction outcomes,
  effect evidence, DA bytes, and the public-input hash before signing.
- Use fork-aware sender facts retained by Stateless. Settlement must not
  independently recover the same signatures under potentially different fork
  rules.
- Keep the attestation key, the L2 system-transaction key, the proof-system
  vkey, and the proof-system address as independent operator-configured bindings.
  Never log or preserve private-key input in diagnostics.
- An attestation does not claim canonical ancestry, sequencer authority, live
  L1 applicability, successful future L1 execution, dispatch mode, or the code
  identity and funding mechanism at the pinned EEZL2 address.

## Reuse canonical implementations

Use the shared workspace definitions instead of locally reproducing protocol
algorithms, including:

- `eez_evm::entries::{decode_postbatch, encode_postbatch, decode_inbound,
  outbound_ether_out}`;
- `eez_evm::public_inputs::public_inputs_hashes`;
- `eez_evm::{cross_chain_call_hash, EcdsaProofSigner, SYSTEM_ADDRESS}`;
- `eez_evm::settlement::pair_end_positions`;
- `eez_evm::system_tx::{build_cross_chain_sync_pairs,
  interleave_sync_block_txs}`; and
- the `prove.v1` schema in `crates/eez-control-rpc/proto/prove.proto`.

System classification itself uses the backend's fork-aware recovered-sender
evidence plus the exact transaction recipient. Do not replace it with a second
settlement-side recovery merely to call a helper.

## Pinned Stateless fork

The production dependency pins an exact commit from
[`eez-association/stateless`](https://github.com/eez-association/stateless)
directly in this crate's `Cargo.toml`. The fork exposes the computed pre-state
and post-state roots and selected transaction-state checkpoints while
preserving upstream Stateless/Reth consensus and execution validation.

Keep changes to this fork narrow. Do not duplicate or replace upstream block,
transaction-root, ommer, withdrawal, receipt, gas, or final-state validation.
A base revision or pinned validator change is a security-sensitive upgrade and
needs focused fixture coverage.

## Change discipline

- Put protocol-neutral backend checks in `validate.rs`, Stateless-specific
  behavior in `validate/stateless.rs`, and settlement rules in the focused
  `settlement/` module that owns the gate.
- Preserve exact decoding and byte equality at trust boundaries. Do not replace
  a canonical comparison with a looser semantic approximation.
- Keep untrusted DA parsing bounded and borrowed; do not allocate collections
  from Composer-controlled list lengths.
- Classify malformed backend success as an internal contract violation and
  Composer-controlled semantic rejection as a failed precondition.
- Add focused tests beside the owning module and a service-level regression
  when orchestration, status mapping, timeout, cancellation, or signing changes.
- This crate requires Rust 1.93.

## Verify

From the workspace root:

```bash
cargo fmt --package eez-proof-signer -- --check
cargo check --package eez-proof-signer --all-targets
cargo clippy --package eez-proof-signer --all-targets --all-features -- -D warnings
cargo test --package eez-proof-signer
RUSTDOCFLAGS='-D warnings' cargo doc --package eez-proof-signer --no-deps
```

Before updating the dependency pin, verify the fork change from a checkout of
the fork repository:

```bash
cargo test --package stateless
cargo clippy --package stateless --all-targets -- -D warnings
```

After updating the pin, rerun the signer checks above.
