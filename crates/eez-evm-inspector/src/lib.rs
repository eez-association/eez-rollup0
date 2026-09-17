//! EVM-inspector composer glue.
//!
//! Composition building, configuration, and errors live in [`eez_protocol`].
//! This crate contributes the revm-coupled piece — [`SessionInspector`], a
//! `revm::Inspector` implementation.
//!
//! ```text
//! eez-protocol
//!   types, ABI, entry building, CompositionBuilder
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
//! [`CompositionBuilder`]: eez_protocol::CompositionBuilder

pub mod inspector;
pub mod overlay;

pub use inspector::{SessionInspector, SessionInspectorFactory};
pub use overlay::{
    OverlayChannel, OverlayChannelHandle, OverlayError, apply_overlay_diff, new_overlay_channel,
};
