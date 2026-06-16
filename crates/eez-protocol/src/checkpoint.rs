//! Execution checkpoint — two-layer design for continuation + proving.
//!
//! [`ExecutionCheckpoint<O, W>`] is generic over the **overlay** `O`
//! (the mutations a call made — what changed) and the **witness** `W`
//! (the state a call read — what it needed to see). Each chain
//! implementation provides its own types:
//!
//! - EVM: `EvmOverlay` + `EvmWitness` (wrapping alloy's `ExecutionWitness`)
//! - Non-EVM: custom types implementing [`Serialize`] + [`DeserializeOwned`](serde::de::DeserializeOwned)
//!
//! # Why two layers
//!
//! A gRPC continuation wants the **overlay** — enough to reconstruct
//! post-call state from the pre-call base — but not the witness,
//! which is strictly larger.
//!
//! A zisk proof wants the **witness** — every storage slot, code
//! byte, and account the call touched — so the prover can replay the
//! call without chain state access. Phase 2 integration populates
//! `witness` via `WitnessRecordingDB`; today it's `None` outside
//! that build.
//!
//! Keeping them separate means a gRPC-only deployment never pays for
//! witness recording, and a proving build never ships witness data
//! over the wire as overlay.

use serde::{Deserialize, Serialize};

/// Two-layer execution checkpoint.
///
/// - `overlay`: accumulated state changes (always present). Used for gRPC
///   continuation — the server reconstructs state from overlay + base block.
/// - `witness`: state access data for proving (optional). Required by the
///   zisk prover to replay execution without chain state access.
///   Populated in Phase 2 via `WitnessRecordingDB`; today it's always
///   `None` outside that build.
///
/// Constraint: targets 32-byte-root protocols. Chains with different hash
/// sizes would need a different checkpoint type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCheckpoint<O, W> {
    /// Schema version for forward compatibility.
    pub version: u32,
    /// Chain ID of the target chain.
    pub chain_id: u64,
    /// Block the base state was opened from.
    pub base_block_number: u64,
    /// Hash of the base block (fork safety — prevents wrong-fork reconstruction).
    pub base_block_hash: [u8; 32],
    /// State root of the base block (before any calls).
    pub base_state_root: [u8; 32],
    /// State root after all accumulated calls. Reconstruction MUST match this.
    pub current_root: [u8; 32],
    /// Accumulated state changes (chain-specific format).
    pub overlay: O,
    /// State access witness for proving (chain-specific). None for gRPC-only use.
    pub witness: Option<W>,
}
