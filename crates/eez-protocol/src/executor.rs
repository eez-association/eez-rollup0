//! Chain-client and execution-session interfaces.
//!
//! The composer talks to every registered rollup through one trait,
//! [`ChainClient`]. Role-specific operations (`simulate_source_tx` for
//! the entry rollup, `stored_target_state_root` for the committed-root
//! host) have default implementations that refuse with
//! [`ExecutorErrorKind::Unavailable`], so a misregistered client fails
//! loudly at the first role-specific call. Nested cross-chain
//! dispatch goes through the borrowed
//! [`CompositionBuilder`].
//!
//! Both traits are synchronous — every in-tree impl does in-process
//! reth/EVM work with no I/O to await. The composer stores clients as
//! trait objects so transports (local reth, test fake) can swap
//! without upstream changes.

use alloy_primitives::{Address, Bytes, U256};

#[allow(
    unused_imports,
    reason = "ExecutorError / its Kind enum used in rustdoc intra-doc links"
)]
use crate::error::{ExecutorError, ExecutorErrorKind};
use crate::rollup_id::RollupId;

/// Request for a single cross-chain execution on the target chain.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    /// Contract the target-chain call lands on. Spec: `Action.targetAddress`.
    pub target_address: Address,
    /// Encoded calldata for the target-chain call. Spec: `Action.data`.
    pub data: Bytes,
    /// Native value sent with the call. Spec: `Action.value`.
    pub value: U256,
    /// Original caller on the source chain — becomes `msg.sender` in the
    /// target invocation. Spec: `Action.sourceAddress`.
    pub source_address: Address,
    /// Rollup ID of the source chain; used for routing and action-hash
    /// derivation. Spec: `Action.sourceRollupId`.
    pub source_rollup_id: RollupId,
}
