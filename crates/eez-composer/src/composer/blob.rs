//! Composer blob posting: capacity, fee limits, and sidecar preparation.

use alloy_primitives::B256;

/// Fork schedule, slot duration, and fee cap for blob posting.
#[derive(Clone, Debug)]
pub struct BlobPostingConfig {
    pub chain_spec: std::sync::Arc<reth_chainspec::ChainSpec>,
    pub slot_seconds: u64,
    pub max_fee_per_blob_gas: u128,
}

/// The signed postBatch's tx hash. A type-3 post's pooled bytes also carry
/// the sidecar, so keccak of those bytes is a different hash.
pub(super) fn post_batch_tx_hash(raw: &[u8]) -> Result<B256, String> {
    if raw.first() == Some(&3) {
        return eez_blob::pooled_blob_transaction_hash(raw)
            .map_err(|error| format!("blob postBatch hash: {error}"));
    }
    Ok(alloy_primitives::keccak256(raw))
}

/// Type-3 carrier of a blob post: its sidecar and blob-gas fee cap.
pub(super) struct BlobCarrier {
    pub(super) sidecar: eez_blob::PreparedBlobSidecar,
    pub(super) max_fee_per_blob_gas: u128,
}

/// Fee headroom over the observed blob base fee, bounded by the operator's cap.
const BLOB_FEE_HEADROOM: u128 = 2;

/// The blob fee a post offers now; an error while the base fee is over the
/// operator's cap, since no post could land.
pub(super) async fn blob_fee(
    l1_provider: &alloy_provider::RootProvider,
    config: &BlobPostingConfig,
) -> Result<u128, String> {
    use alloy_provider::Provider as _;
    let base_fee = l1_provider
        .get_blob_base_fee()
        .await
        .map_err(|e| format!("eth_blobBaseFee: {e}"))?;
    offered_blob_fee(base_fee, config.max_fee_per_blob_gas).ok_or_else(|| {
        format!(
            "blob base fee {base_fee} exceeds EEZ_L1_MAX_FEE_PER_BLOB_GAS {}; deferring the post",
            config.max_fee_per_blob_gas,
        )
    })
}

/// Headroom over `base_fee`, bounded by `cap`; `None` when `base_fee` is over `cap`.
fn offered_blob_fee(base_fee: u128, cap: u128) -> Option<u128> {
    (base_fee <= cap).then(|| base_fee.saturating_mul(BLOB_FEE_HEADROOM).clamp(1, cap))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blob_post_offers_headroom_under_the_cap() {
        assert_eq!(offered_blob_fee(10, 50), Some(20));
        assert_eq!(offered_blob_fee(40, 50), Some(50));
        assert_eq!(offered_blob_fee(0, 50), Some(1));
        assert_eq!(offered_blob_fee(50, 50), Some(50));
        assert_eq!(offered_blob_fee(51, 50), None);
    }
}
