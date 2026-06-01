//! Composer — reth-specific infrastructure for cross-chain composition.
//!
//! Composition orchestration (Composer, inspector, resolver) lives in
//! `crosschain-evm-composer`. This module provides the reth-backed
//! implementation of the protocol traits:
//!
//! - [`LocalChainClient`] — unified chain client impl (entry or follower)
//! - [`LocalExecutionSession`] — stateful per-source-tx target session
//! - [`provider::ChainProvider`] / [`provider::HeaderReader`] — reth
//!   state access abstractions

pub(crate) mod local;
pub(crate) mod provider;
pub(crate) mod session;

#[doc(inline)]
pub use crosschain_evm_composer::Composer;
#[doc(inline)]
pub use local::LocalChainClient;
// `Role` + `LocalExecutionSession` are implementation details of
// `local.rs` and `session.rs`; not re-exported beyond the crate.
#[allow(unused_imports, reason = "future test/public consumers")]
pub(crate) use local::Role;
#[allow(unused_imports, reason = "future test/public consumers")]
pub(crate) use session::LocalExecutionSession;
