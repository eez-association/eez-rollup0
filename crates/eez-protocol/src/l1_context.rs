//! Live L1 inclusion context for conditional settlement proofs.

use alloy_primitives::{B256, U256};
use alloy_sol_types::SolValue as _;

/// Intended live inclusion context. The reference L1 rollup authenticates these
/// fields through `getCustomData(uint64::MAX)` when the proof is consumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveSettlementContext {
    /// Intended inclusion block's parent hash, not the eventual log block hash.
    pub parent_hash: B256,
    /// Intended inclusion time, equal to the terminal Sync block's timestamp.
    pub timestamp: u64,
}

impl LiveSettlementContext {
    /// Reject absent context before constructing a public-input hash.
    pub fn validate(self) -> Result<Self, String> {
        if self.parent_hash.is_zero() || self.timestamp == 0 {
            return Err("live settlement requires a nonzero L1 parent and timestamp".into());
        }
        Ok(self)
    }

    /// Exact reference-manager custom data: `abi.encode(uint256(time), bytes32(parent))`.
    pub fn custom_data(self) -> Result<alloy_primitives::Bytes, String> {
        self.validate()?;
        Ok((U256::from(self.timestamp), self.parent_hash)
            .abi_encode_params()
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn live_custom_data_matches_the_reference_contract_layout() {
        let context = LiveSettlementContext {
            timestamp: 42,
            parent_hash: B256::repeat_byte(0x12),
        };
        let mut expected = vec![0u8; 64];
        expected[31] = 42;
        expected[32..].fill(0x12);
        assert_eq!(context.custom_data().unwrap().as_ref(), expected);
        assert!(
            LiveSettlementContext {
                timestamp: 0,
                ..context
            }
            .custom_data()
            .is_err()
        );
        assert!(
            LiveSettlementContext {
                parent_hash: B256::ZERO,
                ..context
            }
            .custom_data()
            .is_err()
        );
    }
}
