//! EVM-inspector composer glue.
//!
//! The composition orchestration ([`Composer`], [`CompositionBuilder`],
//! configs, errors) lives in [`eez_protocol`]. This crate contributes
//! the one revm-coupled piece — [`SessionInspector`], a
//! `revm::Inspector` impl.
//!
//! ```text
//! eez-protocol
//!   types, ABI, entry building, Composer, CompositionBuilder
//!         ↑
//! eez-evm-inspector   ← you are here (inspector)
//!         ↑
//! eez-composer               (reth integration, wiring)
//! ```
//!
//! Revm is confined to this crate so downstream readers of EVM protocol
//! types (verifiers, external tools) can depend on `eez-protocol`
//! without pulling in the full EVM execution stack.
//!
//! [`SessionInspector`]: crate::SessionInspector
//! [`Composer`]: eez_protocol::Composer
//! [`CompositionBuilder`]: eez_protocol::CompositionBuilder

pub mod inspector;
pub mod overlay;
pub mod post_batch_submitter;

pub use inspector::{
    OverlayChannel, OverlayChannelHandle, SessionInspector, SessionInspectorFactory,
    new_overlay_channel,
};
pub use overlay::{OverlayError, apply_overlay_diff, clone_state};

// Re-export protocol types so callers can `use eez_evm_inspector::*`
// without reaching into the protocol crate for everyday composition needs.
pub use eez_protocol::{ComposerError, ComposerResult, DEFAULT_CCM_GAS_LIMIT, ProxyLookupConfig};
