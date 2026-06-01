//! EVM-specific composer glue.
//!
//! The chain-agnostic orchestration ([`Composer<P>`], [`CompositionBuilder<P>`],
//! configs, errors) lives in [`crosschain_protocol`]. This crate contributes
//! the one genuinely EVM-coupled piece — [`SessionInspector`], a
//! `revm::Inspector` impl — plus type aliases that bind the generic
//! orchestrator to [`EvmProtocol`] for downstream convenience.
//!
//! ```text
//! crosschain-protocol
//!   traits, types, generic Composer<P>, CompositionBuilder<P>
//!         ↑
//! crosschain-evm            (EvmProtocol, ABI, entry building)
//!         ↑
//! crosschain-evm-composer   ← you are here (inspector + aliases)
//!         ↑
//! rollup-node               (reth integration, wiring)
//! ```
//!
//! Revm is confined to this crate so downstream readers of EVM protocol
//! types (verifiers, external tools) can depend on `crosschain-evm`
//! without pulling in the full EVM execution stack.
//!
//! [`SessionInspector`]: crate::SessionInspector
//! [`Composer<P>`]: crosschain_protocol::Composer
//! [`CompositionBuilder<P>`]: crosschain_protocol::CompositionBuilder
//! [`EvmProtocol`]: crosschain_evm::EvmProtocol

pub mod inspector;
pub mod overlay;
pub mod post_batch_submitter;

pub use inspector::{
    OverlayChannel, OverlayChannelHandle, SessionInspector, SessionInspectorFactory,
    new_overlay_channel,
};
pub use overlay::{OverlayError, apply_overlay_diff, clone_state};

// Re-export chain-agnostic types so callers can `use crosschain_evm_composer::*`
// without reaching into the protocol crate for everyday composition needs.
pub use crosschain_protocol::{
    ComposerError, ComposerResult, DEFAULT_CCM_GAS_LIMIT, ProxyLookupConfig,
};

use crosschain_evm::EvmProtocol;

/// EVM-bound composer. Alias for `crosschain_protocol::Composer<EvmProtocol>`.
pub type Composer = crosschain_protocol::Composer<EvmProtocol>;

/// EVM-bound per-target config. Alias for `crosschain_protocol::TargetConfig<EvmProtocol>`.
pub type TargetConfig = crosschain_protocol::TargetConfig<EvmProtocol>;
