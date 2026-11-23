//! Settlement transaction-framing helpers shared by the composer and proof
//! signer.

/// Native system transactions are identified by their reserved envelope type.
/// Sender recovery alone cannot grant this classification to an Ethereum tx.
#[must_use]
pub fn is_system_tx(tx: &eez_primitives::EezTxEnvelope) -> bool {
    matches!(tx, eez_primitives::EezTxEnvelope::System(_))
}

/// Native effect boundaries follow the same state machine as block execution.
pub use eez_primitives::execution_group_end_positions as pair_end_positions;
