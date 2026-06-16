# eez-evm

EVM implementation of the chain-agnostic `eez-protocol` traits.
This is where "a cross-chain call" stops being a trait-generic concept
and becomes Solidity ABI types, 20-byte addresses, 32-byte roots, and
keccak-derived cross-chain call hashes.

## Where it fits

```
eez-protocol   ←  async traits, chain-agnostic
        ↑
eez-evm        ←  YOU ARE HERE  (EvmProtocol + ABI + entry building)
        ↑
eez-evm-inspector · eez-evm-grpc
        ↑
eez-composer           ←  reth integration, wiring
```

## What it exports

- **`EvmProtocol`** — unit struct, `impl ChainProtocol`. Stateless; every
  operation takes the rollup ids it needs as arguments. Per-tx state
  lives on `CompositionBuilder<EvmProtocol>` in `eez-protocol`.
- **Slot constants and helpers** (`ROLLUPS_AUTHORIZED_PROXIES_SLOT`
  = 0, `CCM_AUTHORIZED_PROXIES_SLOT` = 0, `proxy_mapping_key`,
  `decode_proxy_value`) — storage-slot math for the
  `authorizedProxies` mapping. The live-state lookup that consumes
  them lives in `eez-evm-inspector`'s inspector.
- **`ProxyInfo`** — decoded mapping entry (`original_address`,
  `original_rollup_id`).
- **`EvmRecordedCall`** — `RecordedCall<EvmProtocol>` alias.
- **ABI types** (`ExecutionEntrySol`, `ActionSol`, `StateDeltaSol`, …) —
  generated from the Solidity sources via the `sol!` macro.
- **Cross-chain call hashing** (`cross_chain_call_hash`,
  `compute_state_root_slot`) — 6-field hash (`targetRollupId`,
  `targetAddress`, `value`, `data`, `sourceAddress`,
  `sourceRollupId`); mirrors `EEZ.computeCrossChainCallHash`.
- **Batch building** — `entries::build_batch` walks the preorder
  `recorded[..]` slice once, classifies each call (top-level /
  nested-success / nested-failed / lookup), folds per-entry rolling
  hashes, and emits an [`EvmBatch`] wrapping the on-chain
  `ProofSystemBatchPerVerificationEntriesSol`. `encode_postbatch` /
  `encode_load_table` produce the dialect-specific calldata
  wrapper; `encode_follower_trigger` (in `dialect.rs`) emits the
  per-target execute payload.
- **`ChainDialect` enum** (`EvmL1Style` | `EvmL2Style`) selects the
  per-rollup ABI shape: L1-style routes table-loading through
  `EEZ.postVerifyAndExecuteOrSaveExecutionsFromBatch`, L2-style
  through `CrossChainManagerL2.loadExecutionTable`.

[`EvmBatch`]: src/batch.rs

## Reading order

1. Crate-level rustdoc in [`src/lib.rs`](src/lib.rs) — 30-second orientation.
2. [`EvmProtocol`](src/lib.rs) — top-level `impl ChainProtocol`.
3. [`entries/mod.rs`](src/entries/mod.rs) — how `EvmBatch` is built
   from recorded calls (per-rollup chaining lives here).
4. [`action.rs`](src/action.rs) — action-hash derivation, byte-identical
   with the reference protocol's Solidity implementation.
5. Composition orchestration (CCM verification + finalize) lives on
   `CompositionBuilder<P>` in `eez-protocol`.

## Running tests

```bash
# Unit tests for this crate (entries + session)
cargo test -p eez-evm

# Against all workspace crates
cargo test --workspace

# Clippy at workspace discipline
cargo clippy --workspace -- -D warnings
```

## Zero reth

This crate has no reth / revm dependency. It consumes
`RecordedCall<EvmProtocol>` values and produces ABI-encoded bytes;
*how* those calls are detected (revm inspector in the composer) and
*where* state is read from (reth providers in `eez-composer`) are
concerns of other crates. That separation is deliberate — see
`docs/ARCHITECTURE.md` for the full crate-layering rationale.

## Related docs

- [`docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md) — workspace layering,
  data flows, transport polymorphism.
- [`docs/CROSSCHAIN_EVM.md`](../../docs/CROSSCHAIN_EVM.md) — focused
  explainer on the EVM-specific design decisions.
