//! Canonical Sync reconstruction from public chain and rollup identity.
use crate::EEZL2_ADDRESS;
use alloy_primitives::Bytes;
use eez_protocol::abi::ExecutionEntrySol;
use std::{fmt, num::NonZeroU64};
const SYSTEM_TRANSACTION_GAS_PRICE: u128 = 1_000_000_000;
const SYSTEM_TRANSACTION_GAS_LIMIT: u64 = 2_000_000;

/// Configuration for reconstructing and byte-checking omitted Sync-block
/// system transactions.
///
/// The transaction target is fixed to [`EEZL2_ADDRESS`]. Code identity at that
/// address is a deployment invariant and is not checked here.
pub(crate) struct SystemTransactionReconstructor {
    /// Startup-checked operator and fixed protocol reconstruction parameters.
    context: eez_protocol::system_tx::SystemTxContext,
}

impl SystemTransactionReconstructor {
    pub(crate) fn new(l2_chain_id: u64, expected_rollup_id: NonZeroU64) -> Self {
        Self {
            context: eez_protocol::system_tx::SystemTxContext {
                eezl2_address: EEZL2_ADDRESS,
                l2_chain_id,
                l2_gas_price: SYSTEM_TRANSACTION_GAS_PRICE,
                l2_gas_limit: SYSTEM_TRANSACTION_GAS_LIMIT,
                this_rollup_id: expected_rollup_id.get(),
            },
        }
    }

    /// Rebuild the canonical unsigned Sync pairs from authorized effect projections.
    pub(super) fn reconstruct_sync_pairs(
        &self,
        outbound: &[(ExecutionEntrySol, Bytes)],
        inbound: &[ExecutionEntrySol],
        starting_nonce: u64,
    ) -> Result<Vec<eez_protocol::system_tx::SyncPair>, String> {
        eez_protocol::system_tx::build_cross_chain_sync_pairs(
            outbound,
            inbound,
            &self.context,
            starting_nonce,
        )
    }
}

impl fmt::Debug for SystemTransactionReconstructor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemTransactionReconstructor")
            .field("system_address", &eez_primitives::SYSTEM_ADDRESS)
            .field("eezl2_address", &self.context.eezl2_address)
            .field("l2_chain_id", &self.context.l2_chain_id)
            .field("l2_gas_price", &self.context.l2_gas_price)
            .field("l2_gas_limit", &self.context.l2_gas_limit)
            .field("rollup_id", &self.context.this_rollup_id)
            .finish()
    }
}
