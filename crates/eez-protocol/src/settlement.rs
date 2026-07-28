//! Settlement-chain helpers shared by the composer (builds the chained per-entry
//! `StateDelta`s `R0 → P_1 → … → R_N`) and the prover (gates them).

use alloy_primitives::{Address, B256, keccak256};

use crate::SYSTEM_ADDRESS;

/// Domain separator for INTERIM interior placeholder roots (Step 1, pre-P1).
/// When per-pair roots are unavailable (pull failure, intra-tx boundary where
/// no real root exists), the composer fills the interior chain boundaries
/// `P_1..P_{N-1}` with a domain-separated value that (a) telescopes (both
/// adjacent entries use the same `P_k`) and (b) can NEVER collide with a real
/// state root (a trie root is `keccak` of trie nodes; this is `keccak` of a
/// tagged, structured preimage), so a replayed middle entry can never
/// accidentally match the registry root.
const INTERIM_ROOT_TAG: &[u8] = b"EEZ_INTERIM_PLACEHOLDER_ROOT_V1";

/// The interim interior boundary `P_k = keccak(tag, r0, k)` for `1 <= k <= N-1`.
pub fn interim_interior_root(r0: B256, k: usize) -> B256 {
    let mut preimage = Vec::with_capacity(INTERIM_ROOT_TAG.len() + 32 + 8);
    preimage.extend_from_slice(INTERIM_ROOT_TAG);
    preimage.extend_from_slice(r0.as_slice());
    preimage.extend_from_slice(&(k as u64).to_be_bytes());
    keccak256(preimage)
}

/// A sync-block tx is a SYSTEM tx iff signed by [`SYSTEM_ADDRESS`] and targeting
/// the CCM-L2 predeploy. The prover derives per-tx flags this way (from the
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
/// the composer (per-effect `newState` stitch) and the prover
/// (`verify_effect_prefix_roots`), so the settled roots line up entry-for-entry.
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
