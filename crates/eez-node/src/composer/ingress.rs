//! Cross-chain direction — which axis a held tx travels relative to THIS rollup.
//!
//! Cross-chain txs arrive on dedicated per-source-chain RPC fronts (`eez-node`'s
//! `run_cross_chain_front`): the L1 front holds L1→L2 (`Inbound`), the L2 front
//! holds L2→L1 (`Outbound`). Direction is fixed by endpoint, not by `to`/chain-id;
//! a misdirected pure tx poison-evicts at compose time. Pure txs use the node's
//! normal mempool RPC.
//!
//! `Direction` drives nonce isolation in the [`HeldPool`](crate::composer::HeldPool): an
//! inbound tx carries the originating chain's nonce, an outbound tx carries this
//! L2's nonce, so the two chains of the same EOA advance independently.

/// The axis of a cross-chain message relative to THIS rollup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// L1→L2 (or peer-L2→L2): a call this rollup RECEIVES and re-executes.
    Inbound,
    /// L2→L1 (or this-L2→peer-L2): a call this rollup ORIGINATES and settles.
    Outbound,
}
