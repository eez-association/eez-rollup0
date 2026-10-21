//! Blob-specific L1 block context and settlement commitment verification.

use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use eez_protocol::Rollup0Deployment;
use eez_protocol::abi::EvmBatch;
use eez_protocol::public_inputs::batch_shared_public_input_with;

use crate::error::{L1Error, L1Result};

/// Deployment and block context bound by the manager's `getCustomData`.
#[derive(Debug, Clone, Copy)]
pub(super) struct CandidateBlock<'a> {
    pub deployment: &'a Rollup0Deployment,
    pub timestamp: u64,
    pub parent_hash: B256,
}

impl<'a> CandidateBlock<'a> {
    /// Fetch the block whose timestamp and parent the manager bound, pinned by hash.
    pub(super) async fn fetch(
        provider: &impl Provider,
        block: u64,
        block_hash: B256,
        deployment: &'a Rollup0Deployment,
    ) -> L1Result<Self> {
        let header = provider
            .get_block_by_hash(block_hash)
            .await
            .map_err(|e| L1Error::Provider(format!("get_block({block_hash}): {e}")))?
            .ok_or_else(|| L1Error::SourceIncomplete {
                block,
                detail: format!("block {block_hash} is not served; retry"),
            })?
            .header;
        Ok(Self {
            deployment,
            timestamp: header.timestamp,
            parent_hash: header.parent_hash,
        })
    }

    /// Accept only the wrapper's blob-only carrier, selecting every blob in order.
    pub(super) fn commitment(
        &self,
        batch: &EvmBatch,
        blob_hashes: &[B256],
        sender: Address,
        here: &str,
    ) -> L1Result<B256> {
        let selected = (0..blob_hashes.len()).map(U256::from);
        if batch.blockNumber != u64::MAX
            || blob_hashes.is_empty()
            || !batch.blobIndices.iter().copied().eq(selected)
            || !batch.callData.is_empty()
        {
            return Err(L1Error::Decode(format!(
                "{here}: not a canonical candidate-V1 carrier"
            )));
        }
        // The committed caller identifies the historical wrapper, even after rotation.
        let deployment = Rollup0Deployment {
            settlement_wrapper: sender,
            ..*self.deployment
        };
        let custom_data = deployment.custom_data(self.timestamp, self.parent_hash);
        batch_shared_public_input_with(
            batch,
            sender,
            blob_hashes,
            std::slice::from_ref(&custom_data),
        )
        .map_err(|e| L1Error::Decode(format!("{here}: {e}")))
    }
}
