//! Settlement transaction-framing helpers shared by the composer and proof
//! signer.

use alloy_primitives::Address;

/// A sync-block transaction is a system transaction iff its recovered signer
/// matches the deployment's system address and it targets `EEZL2`.
/// The proof signer and composer must use the same deployment values so their
/// pair-end positions, and therefore per-effect settlement roots, agree.
#[must_use]
pub fn is_system_tx(
    signer: Address,
    to: Option<Address>,
    expected_system_address: Address,
    eezl2_address: Address,
) -> bool {
    signer == expected_system_address && to == Some(eezl2_address)
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
    use alloy_primitives::address;

    use super::{is_system_tx, pair_end_positions};

    #[test]
    fn system_transaction_identity_uses_deployment_addresses() {
        let system_address = address!("1111111111111111111111111111111111111111");
        let eezl2_address = address!("2222222222222222222222222222222222222222");

        assert!(is_system_tx(
            system_address,
            Some(eezl2_address),
            system_address,
            eezl2_address,
        ));
        assert!(!is_system_tx(
            address!("3333333333333333333333333333333333333333"),
            Some(eezl2_address),
            system_address,
            eezl2_address,
        ));
        assert!(!is_system_tx(
            system_address,
            Some(address!("4444444444444444444444444444444444444444")),
            system_address,
            eezl2_address,
        ));
    }

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
