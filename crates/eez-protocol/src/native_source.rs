//! Live Sync-block execution identities shared by production and replay.
//!
//! Identity is not authorization or a global deduplication key. Callers must
//! independently authenticate the live target, complete original settlement
//! mapping, exact native bytes, effects and execution checkpoints.

use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::SolValue as _;

/// The identity inputs fixed before any group in a live candidate executes.
/// Deployment, timestamp, accepted-root and proof authorization remain bound by
/// the full settlement proof; they are not alternate source-hash domains.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveSourceContext {
    chain_id: u64,
    parent_hash: B256,
}

impl LiveSourceContext {
    /// Construct from the intended live L1 parent, never the eventual log block
    /// hash or the currently accepted L2 root. Construction does not authenticate it.
    pub fn new(chain_id: u64, parent_hash: B256) -> Result<Self, String> {
        if chain_id == 0 || parent_hash.is_zero() {
            return Err("invalid native live source context".into());
        }
        Ok(Self {
            chain_id,
            parent_hash,
        })
    }

    /// Bind a zero-based complete-group ordinal in the full Live Sync block.
    /// A later posting or a consumed-prefix selection must not reset this index.
    pub const fn entry(self, entry_index: u64) -> SourceEntry {
        SourceEntry {
            context: self,
            entry_index,
        }
    }
}

/// One complete native group's position in the full Live Sync block. Derive
/// these before selecting a consumed suffix. A load and its signed user count
/// once, including an included reverted user; lookups without an L2 tx do not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceEntry {
    context: LiveSourceContext,
    entry_index: u64,
}

impl SourceEntry {
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
        Ok(self.context.entry(index))
    }

    /// Hash the execution position using the literal dynamic ABI string,
    /// uint256 EVM chain ID, bytes32 L1 parent and uint64 group index.
    /// Exact prefix/deployment authorization remains a separate proof obligation.
    pub fn hash(self) -> B256 {
        keccak256(
            (
                "SOURCE_ENTRY_V1",
                U256::from(self.context.chain_id),
                self.context.parent_hash,
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
        let context = LiveSourceContext::new(1, B256::repeat_byte(0x99)).unwrap();
        assert_eq!(
            context.entry(7).hash(),
            alloy_primitives::b256!(
                "67fb5da1784078579c726c542563b4577c50fb90652c54566d49b9f170784d37"
            )
        );
        assert_eq!(context.entry(0).offset(7).unwrap(), context.entry(7));
        for other in [
            context.entry(8),
            LiveSourceContext::new(2, B256::repeat_byte(0x99))
                .unwrap()
                .entry(7),
            LiveSourceContext::new(1, B256::repeat_byte(0x98))
                .unwrap()
                .entry(7),
        ] {
            assert_ne!(context.entry(7).hash(), other.hash());
        }
        assert!(context.entry(u64::MAX).offset(1).is_err());
        assert!(LiveSourceContext::new(0, B256::repeat_byte(1)).is_err());
        assert!(LiveSourceContext::new(1, B256::ZERO).is_err());
    }
}
