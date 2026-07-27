//! Reth-specific infrastructure for cross-chain composition.
//!
//! Composition orchestration (the per-tx `eez_protocol::Composer`)
//! lives in `eez-protocol`; the revm Inspector in `eez-evm-inspector`.
//! This module provides the reth-backed implementation of the
//! protocol traits the orchestrator drives:
//!
//! - [`LocalChainClient`] — unified chain client impl (entry or follower)
//! - [`LocalExecutionSession`] — stateful per-source-tx target session
//! - [`provider::ChainProvider`] / [`provider::HeaderSource`] — reth
//!   state access abstractions

pub(crate) mod build;
pub(crate) mod client;
pub mod gnosis_adapter;
pub(crate) mod provider;
pub(crate) mod session;

#[doc(inline)]
pub use build::{BuildError, BuiltSyncBlock, build_sync_block, sync_block_pair_roots};
#[doc(inline)]
pub use client::LocalChainClient;
#[doc(inline)]
pub use gnosis_adapter::GnosisL1Adapter;
#[doc(inline)]
pub use provider::HeaderSource;
// `Role` + `LocalExecutionSession` are implementation details of
// `local.rs` and `session.rs`; not re-exported beyond the crate.
#[allow(unused_imports, reason = "future test/public consumers")]
pub(crate) use client::Role;
#[allow(unused_imports, reason = "future test/public consumers")]
pub(crate) use session::LocalExecutionSession;
