//! Reth-specific infrastructure for cross-chain composition.
//!
//! Composition orchestration (the per-tx `EvmComposer`, the revm
//! Inspector, the proof-plan resolver) lives in `eez-evm-inspector`.
//! This module provides the reth-backed implementation of the
//! protocol traits the orchestrator drives:
//!
//! - [`LocalChainClient`] — unified chain client impl (entry or follower)
//! - [`LocalExecutionSession`] — stateful per-source-tx target session
//! - [`provider::ChainProvider`] / [`provider::HeaderReader`] — reth
//!   state access abstractions

pub(crate) mod client;
pub(crate) mod provider;
pub(crate) mod session;

#[doc(inline)]
pub use client::LocalChainClient;
#[doc(inline)]
pub use eez_evm_inspector::EvmComposer;
// `Role` + `LocalExecutionSession` are implementation details of
// `local.rs` and `session.rs`; not re-exported beyond the crate.
#[allow(unused_imports, reason = "future test/public consumers")]
pub(crate) use client::Role;
#[allow(unused_imports, reason = "future test/public consumers")]
pub(crate) use session::LocalExecutionSession;
