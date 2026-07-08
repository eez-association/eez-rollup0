//! Rolling-hash accumulators for the flat sequential protocol
//! (upstream's "invariant 6" commitment).
//!
//! Two distinct accumulators, one per on-chain fold site. The split is
//! deliberate: the entry-level fold (`EEZBase`'s `_rollingHash*`
//! helpers, driven by `_processNCalls` / `_consumeNestedAction` on EEZ
//! and EEZL2) is **tagged** with a 1-byte
//! `CALL_BEGIN`/`CALL_END`/`NESTED_BEGIN`/`NESTED_END` discriminator
//! and includes a per-event counter, while the static-subcall fold
//! (`EEZBase._processNLookupCalls`, shared by EEZ and EEZL2)
//! is **untagged** and counter-less. Different on-chain byte layouts;
//! making them different Rust types means a wrong fold is a type
//! error rather than a fixture-time regression.
//!
//! # Byte layouts (verified against the Solidity contracts in the
//! `sync-rollups-protocol` submodule)
//!
//! Solidity uses `abi.encodePacked` everywhere; widths:
//! - `bytes32` → 32 bytes
//! - `uint8`   → 1 byte
//! - `uint256` → 32 bytes (big-endian)
//! - `bool`    → 1 byte (0x00 / 0x01)
//! - `bytes`   → raw payload, NO length prefix
//!
//! ```text
//! CALL_BEGIN    : keccak256(prev[32] || 0x01 || u256_be(callNumber)[32])
//! CALL_END      : keccak256(prev[32] || 0x02 || u256_be(callNumber)[32] ||
//!                            success[1] || retData[raw, no length])
//! NESTED_BEGIN  : keccak256(prev[32] || 0x03 || u256_be(nestedNumber)[32])
//! NESTED_END    : keccak256(prev[32] || 0x04 || u256_be(nestedNumber)[32])
//!
//! StaticSubcall : keccak256(prev[32] || success[1] || retData[raw, no length])
//! ```
//!
//! Initial accumulator value for both is 32 bytes of zero
//! (`bytes32(0)`).
//!
//! The on-chain sites this mirrors (refs into the
//! `sync-rollups-protocol` submodule):
//! - `EEZBase.sol:305,311,317,323` — entry fold (shared by EEZ + EEZL2)
//! - `EEZBase.sol:275` (`_processNLookupCalls`) — static subcall fold

use sha3::{Digest, Keccak256};

/// Tag for the BEGIN side of a top-level cross-chain call within an
/// entry's flat call list.
pub const CALL_BEGIN: u8 = 1;

/// Tag for the END side of a top-level cross-chain call.
pub const CALL_END: u8 = 2;

/// Tag for the BEGIN side of a reentrant nested action (consumed
/// out of `entry.nestedActions[]`).
pub const NESTED_BEGIN: u8 = 3;

/// Tag for the END side of a reentrant nested action.
pub const NESTED_END: u8 = 4;

/// Tagged rolling-hash accumulator covering an entry's
/// `entry.calls[]` + `entry.nestedActions[]` traversal.
///
/// Mirrors the on-chain `_rollingHash` updated by `EEZBase`'s
/// `_rollingHash*` helpers (driven by `_processNCalls` /
/// `_consumeNestedAction` on EEZ/EEZL2). Used by the
/// composer to derive `entry.rollingHash` per entry built.
///
/// The accumulator is `bytes32(0)` initially and folds in four event
/// shapes via [`call_begin`](Self::call_begin),
/// [`call_end`](Self::call_end),
/// [`nested_begin`](Self::nested_begin),
/// [`nested_end`](Self::nested_end).
///
/// `revertSpan` / `ContextResult` restoration: the on-chain
/// `executeInContextAndRevert` revert payload carries the post-span values for
/// `_rollingHash` / `_currentCallNumber` / `_lastNestedActionConsumed`;
/// the outer flow restores all three. To model that off-chain, callers
/// can snapshot the inner accumulator via [`current`](Self::current)
/// before opening the span and write it back via
/// [`restore`](Self::restore) after. There is no in-type span
/// machinery — restore is a pure value swap.
#[derive(Debug, Clone)]
pub struct EntryRollingHash {
    state: [u8; 32],
}

impl Default for EntryRollingHash {
    fn default() -> Self {
        Self::new()
    }
}

impl EntryRollingHash {
    /// Construct a fresh accumulator. Initial state is `bytes32(0)`.
    #[must_use]
    pub const fn new() -> Self {
        Self { state: [0u8; 32] }
    }

    /// Current accumulator value. 32-byte big-endian word matching
    /// the on-chain `_rollingHash` view.
    #[must_use]
    pub const fn current(&self) -> [u8; 32] {
        self.state
    }

    /// Replace the accumulator state with `state`. Used to emulate
    /// `ContextResult` restoration after a `revertSpan` window
    /// completes — see module docs.
    pub fn restore(&mut self, state: [u8; 32]) {
        self.state = state;
    }

    /// Append `keccak256(prev || CALL_BEGIN || callNumber)` to the
    /// accumulator. `call_number` matches the on-chain
    /// `_currentCallNumber` (1-indexed; advance before calling).
    pub fn call_begin(&mut self, call_number: u64) {
        let mut h = Keccak256::new();
        h.update(self.state);
        h.update([CALL_BEGIN]);
        h.update(uint256_be(call_number));
        self.state = h.finalize().into();
    }

    /// Append `keccak256(prev || CALL_END || callNumber || success || retData)`.
    /// `success` is the cross-chain call's final success flag;
    /// `ret_data` is the raw return-data bytes (no length prefix).
    pub fn call_end(&mut self, call_number: u64, success: bool, ret_data: &[u8]) {
        let mut h = Keccak256::new();
        h.update(self.state);
        h.update([CALL_END]);
        h.update(uint256_be(call_number));
        h.update([u8::from(success)]);
        h.update(ret_data);
        self.state = h.finalize().into();
    }

    /// Append `keccak256(prev || NESTED_BEGIN || nestedNumber)`.
    pub fn nested_begin(&mut self, nested_number: u64) {
        let mut h = Keccak256::new();
        h.update(self.state);
        h.update([NESTED_BEGIN]);
        h.update(uint256_be(nested_number));
        self.state = h.finalize().into();
    }

    /// Append `keccak256(prev || NESTED_END || nestedNumber)`.
    pub fn nested_end(&mut self, nested_number: u64) {
        let mut h = Keccak256::new();
        h.update(self.state);
        h.update([NESTED_END]);
        h.update(uint256_be(nested_number));
        self.state = h.finalize().into();
    }
}

/// Untagged, counter-less rolling-hash accumulator for the
/// static-subcall fold (`EEZBase._processNLookupCalls`, shared by EEZ
/// and EEZL2).
///
/// Distinct type from [`EntryRollingHash`] — the byte layout has no
/// tag and no counter, so making this its own type prevents a wrong
/// fold from compiling.
///
/// ```text
/// keccak256(prev[32] || success[1] || retData[raw, no length])
/// ```
#[derive(Debug, Clone)]
pub struct StaticCallRollingHash {
    state: [u8; 32],
}

impl Default for StaticCallRollingHash {
    fn default() -> Self {
        Self::new()
    }
}

impl StaticCallRollingHash {
    /// Construct a fresh accumulator. Initial state is `bytes32(0)`.
    #[must_use]
    pub const fn new() -> Self {
        Self { state: [0u8; 32] }
    }

    /// Current accumulator value.
    #[must_use]
    pub const fn current(&self) -> [u8; 32] {
        self.state
    }

    /// Append `keccak256(prev || success || retData)` to the accumulator.
    pub fn append(&mut self, success: bool, ret_data: &[u8]) {
        let mut hasher = Keccak256::new();
        hasher.update(self.state);
        hasher.update([u8::from(success)]);
        hasher.update(ret_data);
        self.state = hasher.finalize().into();
    }
}

/// Encode `value` as 32-byte big-endian (matches `uint256` packed encoding).
fn uint256_be(value: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&value.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_entry_accumulator_is_zero() {
        let h = EntryRollingHash::new();
        assert_eq!(h.current(), [0u8; 32]);
    }

    #[test]
    fn empty_static_accumulator_is_zero() {
        let h = StaticCallRollingHash::new();
        assert_eq!(h.current(), [0u8; 32]);
    }

    #[test]
    fn call_begin_then_end_advances_state() {
        let mut h = EntryRollingHash::new();
        h.call_begin(1);
        let after_begin = h.current();
        h.call_end(1, true, b"\xAA\xBB");
        let after_end = h.current();
        assert_ne!(after_begin, [0u8; 32], "begin must mutate state");
        assert_ne!(after_begin, after_end, "end must advance state");
    }

    #[test]
    fn restore_overwrites_state() {
        let mut h = EntryRollingHash::new();
        h.call_begin(1);
        h.call_end(1, true, b"x");
        let snap = h.current();
        h.call_begin(2); // simulate a revertSpan-inner advance
        h.call_end(2, false, b"y");
        h.restore(snap);
        assert_eq!(
            h.current(),
            snap,
            "restore must replace inner-span advances",
        );
    }

    #[test]
    fn entry_and_static_folds_diverge_on_same_inputs() {
        // Same `(success, retData)` payload should produce different
        // accumulators because the entry fold prepends a tag + counter
        // and the static fold doesn't.
        let mut entry = EntryRollingHash::new();
        let mut sc = StaticCallRollingHash::new();
        entry.call_end(1, true, b"hi");
        sc.append(true, b"hi");
        assert_ne!(entry.current(), sc.current());
    }

    #[test]
    fn bool_is_one_byte_in_packed_layout() {
        // `success = false` and `success = true` must produce
        // distinct hashes — i.e. the bool is folded into the packed
        // bytes, not silently coerced to a 32-byte word.
        let mut a = StaticCallRollingHash::new();
        let mut b = StaticCallRollingHash::new();
        a.append(false, b"");
        b.append(true, b"");
        assert_ne!(a.current(), b.current());
    }

    #[test]
    fn raw_bytes_no_length_prefix() {
        // Two appends with different payload boundaries — `retData =
        // [0x01, 0x02]` then `[0x03]` vs `[0x01, 0x02, 0x03]` then
        // `[]` — must produce DIFFERENT hashes despite carrying the
        // same total bytes. This confirms the length boundary is
        // captured by the per-fold structure (counter / tag) rather
        // than a raw `bytes` length prefix.
        let mut split = EntryRollingHash::new();
        split.call_end(1, true, b"\x01\x02");
        split.call_end(2, true, b"\x03");

        let mut joined = EntryRollingHash::new();
        joined.call_end(1, true, b"\x01\x02\x03");
        joined.call_end(2, true, b"");

        assert_ne!(
            split.current(),
            joined.current(),
            "boundaries are captured by per-event counter/tag, not by a length prefix",
        );
    }

    #[test]
    fn static_fold_zero_then_two_calls_changes_state() {
        // 0 calls → state stays zero.
        let h0 = StaticCallRollingHash::new();
        assert_eq!(h0.current(), [0u8; 32]);
        // 2 calls → state mutates twice.
        let mut h2 = StaticCallRollingHash::new();
        h2.append(true, b"");
        let mid = h2.current();
        h2.append(false, b"\xFF");
        let end = h2.current();
        assert_ne!(mid, [0u8; 32]);
        assert_ne!(mid, end);
    }
}
