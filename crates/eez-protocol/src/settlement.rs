//! Settlement transaction-framing helpers shared by the composer and proof
//! signer.

use alloy_primitives::Address;

use crate::SYSTEM_ADDRESS;

/// A sync-block tx is a SYSTEM tx iff signed by [`SYSTEM_ADDRESS`] and targeting
/// the CCM-L2 predeploy. The proof signer derives per-tx flags this way (from the
/// block RLP); the composer must match it EXACTLY so pair-end positions — and
/// thus the per-effect settlement roots — agree on both sides.
#[must_use]
pub fn is_system_tx(signer: Address, to: Option<Address>, ccm_l2_address: Address) -> bool {
    signer == SYSTEM_ADDRESS && to == Some(ccm_l2_address)
}

/// Pair-end tx positions in a sync block — one per settled cross-chain effect,
/// in tx order. A position ends a pair iff it is a USER tx (the `user` half of
/// an outbound `[load | user]` pair) OR a SYSTEM tx followed by a system tx / the
/// block end (a standalone inbound system tx). Single source of truth shared by
/// the composer (per-effect `StateUpdate.newState` stitch) and proof signer
/// (effect/checkpoint binding), so the settled roots line up entry-for-entry.
#[must_use]
pub fn pair_end_positions(is_system: &[bool]) -> Vec<usize> {
    (0..is_system.len())
        .filter(|&i| !is_system[i] || is_system.get(i + 1).copied().unwrap_or(true))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::pair_end_positions;

    /// A pair ends at every user (non-system) tx, at a system tx followed by a
    /// system tx, and at the last tx regardless. A system tx followed by a user
    /// tx is NOT a pair end — it pairs with that user tx.
    #[test]
    fn pair_end_positions_cases() {
        assert_eq!(pair_end_positions(&[]), Vec::<usize>::new());
        // all user txs → each ends its own pair
        assert_eq!(pair_end_positions(&[false, false, false]), vec![0, 1, 2]);
        // system → user: only the user (index 1) ends the pair
        assert_eq!(pair_end_positions(&[true, false]), vec![1]);
        // system → system: the first system is a standalone pair end; the last
        // tx is always a pair end
        assert_eq!(pair_end_positions(&[true, true]), vec![0, 1]);
        // user then trailing system (last) → both are pair ends
        assert_eq!(pair_end_positions(&[false, true]), vec![0, 1]);
        // two outbound [load|user] pairs → ends at each user
        assert_eq!(pair_end_positions(&[true, false, true, false]), vec![1, 3]);
        // a lone system tx (also the last) → pair end
        assert_eq!(pair_end_positions(&[true]), vec![0]);
    }
}
