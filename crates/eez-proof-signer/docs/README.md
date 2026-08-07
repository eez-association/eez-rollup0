# eez-proof-signer documentation

> These are implementation guides. [`SPEC.md`](../SPEC.md) is authoritative
> for protocol behavior and compatibility requirements.

The guides explain how the current Rust code is organized and how evidence
moves through it. They intentionally link to the specification for exact wire
rules, accepted effect shapes, hash formulas, and failure requirements instead
of maintaining a second copy.

## Reading paths

| If you want to understand... | Start with |
| --- | --- |
| The daemon as a whole and where code belongs | [Architecture](architecture.md) |
| Which values are submitted, computed, validated, or authorized | [Data provenance](data-provenance.md) |
| Single-request admission, deadlines, cancellation, and shutdown | [Request lifecycle](request-lifecycle.md) |
| What Stateless proves and what reaches settlement | [Validation evidence](validation-evidence.md) |
| How a validated window becomes a signed digest | [Settlement pipeline](settlement-pipeline.md) |
| Operator configuration and production behavior | [Operations](operations.md) |

For a first pass, read Architecture, Data provenance, Validation evidence, and
Settlement pipeline in that order. Request lifecycle is most useful when
changing `service/`; Operations is aimed at deployment and incident diagnosis.

## Shared vocabulary

- **Window:** the inclusive L2 block span streamed in one `Prove` RPC.
- **Batch:** the `PostBatch.abi_calldata` decoded as an `EvmBatch`.
- **Stateless / stateless:** `Stateless` names the upstream witness-backed
  validator; lowercase “stateless service” means that this daemon persists no
  request-derived protocol state.
- **Settling block:** the final block in the streamed window; its transactions
  carry the effects bound to the posted batch.
- **Effect candidate:** a locally derived transaction-framing boundary that may
  correspond to an effect. It is not authorization by itself.
- **Transaction-state checkpoint:** a state root locally recomputed immediately
  after a selected transaction and before post-block processing. Settlement
  later binds it to a submitted state-update claim.
- **DA sidecar / Sync block:** a sidecar is the canonical derivation projection
  for one effect; the Sync block is the terminal L2 block reconstructed from
  those projections and retained user transactions.
- **Admission:** cheap stream-shape, identity, and quota checks. Admission does
  not establish consensus or execution correctness.
- **Validation:** Stateless/Reth re-execution plus shared backend-contract
  checks.
- **Settlement:** gates that join the decoded batch to validated execution
  evidence and exact DA bytes.
- **Attestation:** the raw-digest ECDSA signature over the independently
  recomputed public-input hash.

## Documentation boundaries

- Protocol requirements belong in [`SPEC.md`](../SPEC.md).
- Contributor constraints belong in [`CONTRIBUTING.md`](../CONTRIBUTING.md).
- Current implementation explanations belong here.
- Validator-fork implementation details belong in
  [`eez-association/stateless`](https://github.com/eez-association/stateless).
- Focused behavior belongs close to the code and tests that own it.

When code and an explainer diverge, correct the explainer. When code and the
specification diverge, decide which is wrong before changing either; never
silently weaken a security check to make them agree.

## Principal test suites

Unit tests live beside the module they exercise. Larger suites use a directory
only when one flat test file would obscure the same ownership boundary:

- [`src/window/tests.rs`](../src/window/tests.rs) covers structural admission
  and quotas;
- [`src/config/tests.rs`](../src/config/tests.rs) covers startup parsing,
  precedence, limits, and secret redaction;
- [`src/validate/tests.rs`](../src/validate/tests.rs) covers the backend-neutral
  backend-output contract;
- [`src/validate/stateless/tests.rs`](../src/validate/stateless/tests.rs) covers
  the production adapter and evidence;
- [`src/validate/stateless/chain_config/tests.rs`](../src/validate/stateless/chain_config/tests.rs)
  covers strict operator-configured chain-document loading;
- [`src/settlement/tests/`](../src/settlement/tests) mirrors the focused
  settlement gate modules;
- [`src/service/tests/`](../src/service/tests) covers in-process gRPC
  orchestration, runtime behavior, attestation, and recorded regressions;
- inline tests in [`src/attest.rs`](../src/attest.rs) cover key separation and
  signature encoding; and
- the pinned [EEZ Stateless fork](https://github.com/eez-association/stateless)
  maintains the checkpoint-extension tests in its own repository.

This layout keeps production modules discoverable at `src/` while letting a
large test suite scale without turning one source file into a test container.
