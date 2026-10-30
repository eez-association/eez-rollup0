//! Live Sync-block execution identities shared by production and replay.
//!
//! Identity is not authorization or a global deduplication key. Callers must
//! independently authenticate the live target, complete original settlement
//! mapping, exact native bytes, effects and execution checkpoints.

use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::SolValue as _;

/// One complete native group's position in the full Live Sync block. Derive
/// these before selecting a consumed suffix. A load and its signed user count
/// once, including an included reverted user; lookups without an L2 tx do not.
/// Deployment, timestamp, accepted-root and proof authorization remain bound by
/// the full settlement proof; they are not alternate source-hash domains.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceEntry {
    chain_id: u64,
    parent_hash: B256,
    entry_index: u64,
}

impl SourceEntry {
    /// Construct from the intended live L1 parent, never the eventual log block
    /// hash or the currently accepted L2 root. Construction does not authenticate it.
    pub fn new(chain_id: u64, parent_hash: B256, entry_index: u64) -> Result<Self, String> {
        if chain_id == 0 || parent_hash.is_zero() {
            return Err("invalid native live source context".into());
        }
        Ok(Self {
            chain_id,
            parent_hash,
            entry_index,
        })
    }

    /// Bind a zero-based complete-group ordinal in the full Live Sync block.
    /// A later posting or a consumed-prefix selection must not reset this index.
    #[must_use]
    pub const fn with_index(self, entry_index: u64) -> Self {
        Self {
            entry_index,
            ..self
        }
    }

    /// Zero-based whole-group position, before consumption filtering.
    pub const fn index(self) -> u64 {
        self.entry_index
    }

    /// Advance within the original full candidate, never a filtered suffix.
    pub fn offset(self, count: u64) -> Result<Self, String> {
        let index = self
            .entry_index
            .checked_add(count)
            .ok_or_else(|| "native source group index overflow".to_owned())?;
        Ok(self.with_index(index))
    }

    /// Hash the execution position using the literal dynamic ABI string,
    /// uint256 EVM chain ID, bytes32 L1 parent and uint64 group index.
    /// Exact prefix/deployment authorization remains a separate proof obligation.
    pub fn hash(self) -> B256 {
        keccak256(
            (
                "SOURCE_ENTRY_V1",
                U256::from(self.chain_id),
                self.parent_hash,
                self.entry_index,
            )
                .abi_encode_params(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_identity_matches_independent_abi_vector_and_preserves_original_ordinal() {
        // Oracle: cast keccak $(cast abi-encode 'f(string,uint256,bytes32,uint64)'
        // SOURCE_ENTRY_V1 1 0x9999...9999 7). This uses dynamic ABI string encoding.
        let source = SourceEntry::new(1, B256::repeat_byte(0x99), 7).unwrap();
        assert_eq!(
            source.hash(),
            alloy_primitives::b256!(
                "67fb5da1784078579c726c542563b4577c50fb90652c54566d49b9f170784d37"
            )
        );
        assert_eq!(source.with_index(0).offset(7).unwrap(), source);
        for other in [
            source.with_index(8),
            SourceEntry::new(2, B256::repeat_byte(0x99), 7).unwrap(),
            SourceEntry::new(1, B256::repeat_byte(0x98), 7).unwrap(),
        ] {
            assert_ne!(source.hash(), other.hash());
        }
        assert!(source.with_index(u64::MAX).offset(1).is_err());
        assert!(SourceEntry::new(0, B256::repeat_byte(1), 0).is_err());
        assert!(SourceEntry::new(1, B256::ZERO, 0).is_err());
    }
}
