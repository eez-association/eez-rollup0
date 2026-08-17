//! Reth-specific infrastructure for cross-chain composition.
//!
//! Per-transaction composition building lives in `eez-protocol`; the revm
//! inspector lives in `eez-evm-inspector`.
//! This module provides the reth-backed implementation of the
//! protocol traits the orchestrator drives:
//!
//! - [`LocalChainClient`] — unified chain client impl (entry or follower)
//! - `LocalExecutionSession` — stateful per-source-tx target session
//! - `slot` — slot-scoped execution contexts driving the real contract paths
//! - `ChainProvider` / `HeaderReader` — reth
//!   state access abstractions

use alloy_primitives::Address;

pub(crate) mod build;
pub(crate) mod client;
pub mod gnosis_adapter;
pub(crate) mod provider;
pub(crate) mod session;
pub(crate) mod slot;

#[doc(inline)]
pub use build::{BuildError, BuiltSyncBlock, build_sync_block, sync_block_pair_roots};
#[doc(inline)]
pub use client::LocalChainClient;
#[doc(inline)]
pub use gnosis_adapter::GnosisL1Adapter;
#[doc(inline)]
pub use slot::LocalSlotHandles;
// Slot execution contexts are driven only by `composer.rs`.
pub(crate) use slot::{L1ManagerExec, L1World, L2BlockProbeExec};
// `Role` + `LocalExecutionSession` are implementation details of
// `local.rs` and `session.rs`; not re-exported beyond the crate.
#[allow(unused_imports, reason = "future test/public consumers")]
pub(crate) use client::Role;
#[allow(unused_imports, reason = "future test/public consumers")]
pub(crate) use session::LocalExecutionSession;

/// Restore `addr`'s nonce in `changes` to its pre-frame value.
///
/// Simulated frames are sent by contract / synthetic accounts: the chain's
/// manager, the `Address::ZERO` proxy-creation caller, or a CREATE2 proxy slot
/// nobody has deployed yet. revm bumps a transaction sender's nonce; on a
/// contract account that nonce only governs CREATE, and every proxy is CREATE2,
/// so leaving the bump in drifts the fork from the chain — and on an undeployed
/// proxy slot it fails EIP-684's "code or nonce non-zero" check when the
/// protocol later deploys the real `CrossChainProxy` there.
pub(crate) fn restore_synthetic_caller_nonce(
    changes: &mut revm::primitives::map::AddressHashMap<revm::state::Account>,
    addr: Address,
) {
    if let Some(account) = changes.get_mut(&addr) {
        account.info.nonce = account.original_info.nonce;
    }
}
