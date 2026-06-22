//! EVM-specific composer glue.
//!
//! The chain-agnostic orchestration ([`Composer<P>`], [`CompositionBuilder<P>`],
//! configs, errors) lives in [`eez_protocol`]. This crate contributes
//! the one genuinely EVM-coupled piece — [`SessionInspector`], a
//! `revm::Inspector` impl — plus type aliases that bind the generic
//! orchestrator to [`EvmProtocol`] for downstream convenience.
//!
//! ```text
//! eez-protocol
//!   traits, types, generic Composer<P>, CompositionBuilder<P>
//!         ↑
//! eez-evm            (EvmProtocol, ABI, entry building)
//!         ↑
//! eez-evm-inspector   ← you are here (inspector + aliases)
//!         ↑
//! eez-composer               (reth integration, wiring)
//! ```
//!
//! Revm is confined to this crate so downstream readers of EVM protocol
//! types (verifiers, external tools) can depend on `eez-evm`
//! without pulling in the full EVM execution stack.
//!
//! [`SessionInspector`]: crate::SessionInspector
//! [`Composer<P>`]: eez_protocol::Composer
//! [`CompositionBuilder<P>`]: eez_protocol::CompositionBuilder
//! [`EvmProtocol`]: eez_evm::EvmProtocol

pub mod inspector;
pub mod overlay;
pub mod post_batch_submitter;

pub use inspector::{
    OverlayChannel, OverlayChannelHandle, SessionInspector, SessionInspectorFactory,
    new_overlay_channel,
};
pub use overlay::{OverlayError, apply_overlay_diff, clone_state};

// Re-export chain-agnostic types so callers can `use eez_evm_inspector::*`
// without reaching into the protocol crate for everyday composition needs.
pub use eez_protocol::{ComposerError, ComposerResult, DEFAULT_CCM_GAS_LIMIT, ProxyLookupConfig};

use eez_evm::EvmProtocol;

/// EVM-bound composer. Alias for `eez_protocol::Composer<EvmProtocol>`.
///
/// Renamed from `Composer` to `EvmComposer` during the port: the
/// `eez-composer` umbrella crate (orchestration role) owns the
/// `Composer` name. The two are distinct concerns — this alias names
/// the per-tx protocol orchestrator; the umbrella names the run-loop
/// + per-rollup state. Same naming clash that motivated
/// `crosschain-evm-composer` → `eez-evm-inspector` for the crate
/// name, applied consistently to the type alias.
pub type EvmComposer = eez_protocol::Composer<EvmProtocol>;

/// EVM-bound per-target config. Alias for `eez_protocol::TargetConfig<EvmProtocol>`.
pub type EvmTargetConfig = eez_protocol::TargetConfig<EvmProtocol>;
