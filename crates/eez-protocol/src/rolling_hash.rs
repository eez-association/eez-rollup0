//! Rolling-hash accumulators for cross-chain execution entries.
//!
//! The entry accumulator is seeded differently on L1 and L2, then folds the
//! same tagged execution events on both chains. All layouts mirror Solidity's
//! `abi.encodePacked` encoding:
//!
//! ```text
//! L1 state fold  : keccak256(prev[32] || rollupId[8] || currentState[32])
//! L1 entry seed  : keccak256(statesHash[32] || proxyEntryHash[32])
//! L2 entry seed  : keccak256(bytes32(0) || proxyEntryHash[32])
//! CALL_BEGIN     : keccak256(prev[32] || 0x01 || callHash[32])
//! CALL_END       : keccak256(prev[32] || 0x02 || success[1] || returnData[raw])
//! NESTED_BEGIN   : keccak256(prev[32] || 0x03 || callHash[32])
//! NESTED_END     : keccak256(prev[32] || 0x04)
//! CALL_NOT_FOUND : keccak256(prev[32] || 0x05 || callHash[32])
//! Static result  : keccak256(prev[32] || success[1] || returnData[raw])
//! ```

use alloy_primitives::B256;
use sha3::{Digest, Keccak256};

/// Tag for the beginning of a top-level cross-chain call.
pub const CALL_BEGIN: u8 = 1;

/// Tag for the result of a top-level cross-chain call.
pub const CALL_END: u8 = 2;

/// Tag for the beginning of a nested cross-chain call.
pub const NESTED_BEGIN: u8 = 3;

/// Tag for the end of a nested cross-chain call.
pub const NESTED_END: u8 = 4;

/// Tag for an expected cross-chain call that was not made.
pub const CALL_NOT_FOUND: u8 = 5;

/// Tagged rolling-hash accumulator for one execution entry.
///
/// Construction requires choosing the L1 or L2 seed explicitly because the
/// two chains commit different entry context before execution events are
/// folded.
#[derive(Debug, Clone)]
pub struct EntryRollingHash {
    state: B256,
}

impl EntryRollingHash {
    /// Seed an L1 entry from its ordered `(rollup_id, current_state)` updates
    /// and proxy entry hash.
    ///
    /// `rollup_id` is encoded as an 8-byte big-endian `uint64`, matching
    /// Solidity's packed encoding. Update order is commitment-significant.
    #[must_use]
    pub fn for_l1(
        state_updates: impl IntoIterator<Item = (u64, B256)>,
        proxy_entry_hash: B256,
    ) -> Self {
        let states_hash =
            state_updates
                .into_iter()
                .fold(B256::ZERO, |previous, (rollup_id, current_state)| {
                    hash_parts(&[
                        previous.as_slice(),
                        &rollup_id.to_be_bytes(),
                        current_state.as_slice(),
                    ])
                });

        Self {
            state: hash_parts(&[states_hash.as_slice(), proxy_entry_hash.as_slice()]),
        }
    }

    /// Seed an L2 entry from the proxy entry hash.
    #[must_use]
    pub fn for_l2(proxy_entry_hash: B256) -> Self {
        Self {
            state: hash_parts(&[B256::ZERO.as_slice(), proxy_entry_hash.as_slice()]),
        }
    }

    /// Return the current rolling hash.
    #[must_use]
    pub const fn current(&self) -> B256 {
        self.state
    }

    /// Fold the beginning of a call identified by its cross-chain call hash.
    pub fn call_begin(&mut self, call_hash: B256) {
        self.state = hash_parts(&[self.state.as_slice(), &[CALL_BEGIN], call_hash.as_slice()]);
    }

    /// Fold a call's final success flag and raw return data.
    pub fn call_end(&mut self, success: bool, return_data: &[u8]) {
        self.state = hash_parts(&[
            self.state.as_slice(),
            &[CALL_END],
            &[u8::from(success)],
            return_data,
        ]);
    }

    /// Fold the beginning of a nested call identified by its call hash.
    pub fn nested_begin(&mut self, call_hash: B256) {
        self.state = hash_parts(&[self.state.as_slice(), &[NESTED_BEGIN], call_hash.as_slice()]);
    }

    /// Fold the end of the current nested call.
    pub fn nested_end(&mut self) {
        self.state = hash_parts(&[self.state.as_slice(), &[NESTED_END]]);
    }

    /// Fold an expected call that was not made.
    pub fn call_not_found(&mut self, call_hash: B256) {
        self.state = hash_parts(&[
            self.state.as_slice(),
            &[CALL_NOT_FOUND],
            call_hash.as_slice(),
        ]);
    }
}

/// Untagged rolling-hash accumulator for static-call results.
///
/// Static results use a separate, zero-seeded layout with neither event tags
/// nor call identities.
#[derive(Debug, Clone)]
pub struct StaticCallRollingHash {
    state: B256,
}

impl Default for StaticCallRollingHash {
    fn default() -> Self {
        Self::new()
    }
}

impl StaticCallRollingHash {
    /// Construct a zero-seeded static-result accumulator.
    #[must_use]
    pub const fn new() -> Self {
        Self { state: B256::ZERO }
    }

    /// Return the current rolling hash.
    #[must_use]
    pub const fn current(&self) -> B256 {
        self.state
    }

    /// Fold a static call's success flag and raw return data.
    pub fn append(&mut self, success: bool, return_data: &[u8]) {
        self.state = hash_parts(&[self.state.as_slice(), &[u8::from(success)], return_data]);
    }
}

fn hash_parts(parts: &[&[u8]]) -> B256 {
    let mut hasher = Keccak256::new();
    for part in parts {
        hasher.update(part);
    }
    B256::from_slice(&hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l1_update_order_is_commitment_significant() {
        let proxy_hash = B256::with_last_byte(1);
        let first = (1, B256::with_last_byte(2));
        let second = (2, B256::with_last_byte(3));

        let forward = EntryRollingHash::for_l1([first, second], proxy_hash);
        let reversed = EntryRollingHash::for_l1([second, first], proxy_hash);

        assert_ne!(forward.current(), reversed.current());
    }

    #[test]
    fn l1_and_l2_seeds_are_distinct() {
        let proxy_hash = B256::with_last_byte(1);
        let l1 = EntryRollingHash::for_l1([(1, B256::with_last_byte(2))], proxy_hash);
        let l2 = EntryRollingHash::for_l2(proxy_hash);

        assert_ne!(l1.current(), l2.current());
    }

    #[test]
    fn static_accumulator_is_zero_seeded() {
        assert_eq!(StaticCallRollingHash::new().current(), B256::ZERO);
    }
}
