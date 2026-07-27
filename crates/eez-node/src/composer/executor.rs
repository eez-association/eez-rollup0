//! Cross-chain execution request shape.
//!
//! The composer talks to every registered rollup through the concrete
//! [`LocalChainClient`](crate::composer::LocalChainClient); per-source-tx
//! target execution runs on the concrete
//! [`LocalExecutionSession`](crate::composer::local::LocalExecutionSession).
//! This module carries the request type both hand around.

use alloy_primitives::{Address, Bytes, U256};

use eez_protocol::rollup_id::RollupId;

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
