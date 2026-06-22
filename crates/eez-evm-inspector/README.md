# eez-evm-inspector

EVM-specific glue for the cross-chain composer.

This crate is deliberately tiny. It contributes:

- **`SessionInspector`** — a `revm::Inspector` impl that detects
  cross-chain proxy calls during source-chain EVM execution and
  dispatches each one through a borrowed
  `CompositionBuilder<EvmProtocol>`.
- **Type aliases** binding the chain-agnostic `Composer<P>` /
  `TargetConfig<P>` to `EvmProtocol` so downstream callers don't see
  the generic parameter.

Everything else (orchestration, composition state, config types, the
composer error families) lives in
[`eez-protocol`](../eez-protocol/) as chain-agnostic
generics; this crate only supplies the EVM-specific inspector and the
type aliases.

## Where it fits

```
eez-protocol   (traits + types + generic Composer<P>, CompositionBuilder<P>)
        ↑
eez-evm        (EvmProtocol + ABI + entry building)
        ↑
eez-evm-inspector   ← YOU ARE HERE  (SessionInspector + EVM aliases)
        ↑
eez-composer           (reth integration, wiring)
```

## What this crate exports

```rust
// EVM-specific inspector + factory:
pub use inspector::{SessionInspector, SessionInspectorFactory};

// Overlay channel for shared-source-state nested dispatch:
pub use inspector::{OverlayChannel, OverlayChannelHandle, new_overlay_channel};

// Overlay diff-apply + state-clone primitives:
pub use overlay::{apply_overlay_diff, clone_state, OverlayError};

// Type aliases over the generic orchestrator:
pub type Composer     = eez_protocol::Composer<EvmProtocol>;
pub type TargetConfig = eez_protocol::TargetConfig<EvmProtocol>;

// Re-exports (chain-agnostic things callers shouldn't have to reach for):
pub use eez_protocol::{
    ComposerError, ComposerResult, DEFAULT_CCM_GAS_LIMIT, ProxyLookupConfig,
};
```

Downstream callers (e.g. `eez-composer`) `use eez_evm_inspector::{Composer, TargetConfig, ...}` and never deal with the `<EvmProtocol>` generic.

## Running tests

```bash
cargo test -p eez-evm-inspector
```

Composer / composition-builder tests live in `eez-protocol` (the
crate that owns the generic code). This crate's tests cover only
`SessionInspector`-specific behavior.

## Related docs

- [`docs/CROSSCHAIN_EVM_COMPOSER.md`](../../docs/CROSSCHAIN_EVM_COMPOSER.md)
  — design notes (why this crate is separate, what `OverlayChannel`
  is, journal-aware snapshot rationale).
- [`docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md) — workspace
  layering, data flows, transport polymorphism.
