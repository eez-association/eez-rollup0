# crosschain-evm-composer

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
[`crosschain-protocol`](../crosschain-protocol/) as chain-agnostic
generics; this crate only supplies the EVM-specific inspector and the
type aliases.

## Where it fits

```
crosschain-protocol   (traits + types + generic Composer<P>, CompositionBuilder<P>)
        ↑
crosschain-evm        (EvmProtocol + ABI + entry building)
        ↑
crosschain-evm-composer   ← YOU ARE HERE  (SessionInspector + EVM aliases)
        ↑
rollup-node           (reth integration, wiring)
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
pub type Composer     = crosschain_protocol::Composer<EvmProtocol>;
pub type TargetConfig = crosschain_protocol::TargetConfig<EvmProtocol>;

// Re-exports (chain-agnostic things callers shouldn't have to reach for):
pub use crosschain_protocol::{
    ComposerError, ComposerResult, DEFAULT_CCM_GAS_LIMIT, ProxyLookupConfig,
};
```

Downstream callers (e.g. `rollup-node`) `use crosschain_evm_composer::{Composer, TargetConfig, ...}` and never deal with the `<EvmProtocol>` generic.

## Running tests

```bash
cargo test -p crosschain-evm-composer
```

Composer / composition-builder tests live in `crosschain-protocol` (the
crate that owns the generic code). This crate's tests cover only
`SessionInspector`-specific behavior.

## Related docs

- [`docs/CROSSCHAIN_EVM_COMPOSER.md`](../../docs/CROSSCHAIN_EVM_COMPOSER.md)
  — design notes (why this crate is separate, what `OverlayChannel`
  is, journal-aware snapshot rationale).
- [`docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md) — workspace
  layering, data flows, transport polymorphism.
