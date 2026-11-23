//! Composer blob posting: capacity, fee limits, and sidecar preparation.

use alloy_primitives::Bytes;
use reth_chainspec::{ChainSpec, EthChainSpec as _, EthereumHardforks as _};

/// Fork schedule and fee cap for blob posting.
#[derive(Clone, Debug)]
pub struct BlobPostingConfig {
    pub chain_spec: std::sync::Arc<ChainSpec>,
    pub max_fee_per_blob_gas: u128,
}

/// Type-3 carrier of a blob post: its sidecar and blob-gas fee cap.
pub(super) struct BlobCarrier {
    pub(super) sidecar: alloy_eips::eip7594::BlobTransactionSidecarVariant,
    pub(super) max_fee_per_blob_gas: u128,
}

/// Blobs of an encoded blob post, with the posting config and the target
/// block's blob limit.
pub(super) struct PhysicalBlobs<'a> {
    pub(super) config: &'a BlobPostingConfig,
    pub(super) blobs: Vec<Vec<u8>>,
    pub(super) limit: ActiveBlobLimit,
}

/// A blob sidecar building on the blocking pool, with the blob fee it offers.
pub(super) struct PendingSidecar {
    task: tokio::task::JoinHandle<
        Result<alloy_eips::eip7594::BlobTransactionSidecarVariant, eez_blob::TransportError>,
    >,
    max_fee_per_blob_gas: u128,
}

/// Read the blob fee and start building the sidecar; returns the blobs the
/// attesters prove over and the pending sidecar. No blobs → calldata post.
pub(super) async fn start_blob_sidecar(
    l1_provider: &alloy_provider::RootProvider,
    physical_blobs: Option<PhysicalBlobs<'_>>,
) -> Result<(Vec<Bytes>, Option<PendingSidecar>), String> {
    // The fee is re-read since it can rise after the slot's gate. Cell
    // proofs are CPU-heavy, so the sidecar builds on the blocking pool
    // while the witnesses build and the attesters prove.
    match physical_blobs {
        Some(PhysicalBlobs {
            config,
            blobs,
            limit,
        }) => {
            let max_fee_per_blob_gas = blob_fee(l1_provider, config).await?;
            let blobs: Vec<Bytes> = blobs.into_iter().map(Bytes::from).collect();
            let sidecar_blobs = blobs.clone();
            let task = tokio::task::spawn_blocking(move || {
                eez_blob::prepare_blob_sidecar(&sidecar_blobs, limit.sidecar_encoding)
            });
            Ok((
                blobs,
                Some(PendingSidecar {
                    task,
                    max_fee_per_blob_gas,
                }),
            ))
        }
        None => Ok((Vec::new(), None)),
    }
}

/// Wait for the pending sidecar and wrap it as the post's blob carrier.
pub(super) async fn finish_blob_carrier(
    pending: Option<PendingSidecar>,
) -> Result<Option<BlobCarrier>, String> {
    match pending {
        Some(PendingSidecar {
            task,
            max_fee_per_blob_gas,
        }) => Ok(Some(BlobCarrier {
            sidecar: task
                .await
                .map_err(|e| format!("blob sidecar task: {e}"))?
                .map_err(|e| format!("prepare blob sidecar: {e}"))?,
            max_fee_per_blob_gas,
        })),
        None => Ok(None),
    }
}

/// The blob limit at the L1 block a batch targets: its pinned timestamp, else
/// one slot past the current L1 head.
pub(super) async fn target_blob_limit(
    l1_provider: &alloy_provider::RootProvider,
    config: &BlobPostingConfig,
    l1_block_time: std::time::Duration,
    target: eez_l1::BundleTarget,
) -> Result<ActiveBlobLimit, String> {
    use alloy_provider::Provider as _;
    let timestamp = match target {
        eez_l1::BundleTarget::Exact { timestamp, .. } => timestamp,
        _ => l1_provider
            .get_block_by_number(alloy_eips::BlockNumberOrTag::Latest)
            .await
            .map_err(|e| format!("get blob L1 head: {e}"))?
            .ok_or("L1 head unavailable")?
            .header
            .timestamp
            .checked_add(l1_block_time.as_secs())
            .ok_or("blob target timestamp overflow")?,
    };
    blob_limit_at(&config.chain_spec, timestamp)
        .ok_or_else(|| "no blob fork is active at the blob target".to_owned())
}

/// How many blobs one settlement transaction may carry at a timestamp, and
/// which sidecar wrapper that fork uses.
#[derive(Debug, Clone, Copy)]
pub(super) struct ActiveBlobLimit {
    pub(super) max_blobs: usize,
    pub(super) sidecar_encoding: eez_blob::BlobSidecarEncoding,
}

/// `None` when no blob fork is active, or either cap is zero.
fn blob_limit_at(spec: &ChainSpec, timestamp: u64) -> Option<ActiveBlobLimit> {
    let params = spec.blob_params_at_timestamp(timestamp)?;
    let max = params.max_blobs_per_tx.min(params.max_blob_count);
    let max_blobs = usize::try_from(max).ok().filter(|count| *count > 0)?;
    let sidecar_encoding = if spec.is_osaka_active_at_timestamp(timestamp) {
        eez_blob::BlobSidecarEncoding::Eip7594
    } else {
        eez_blob::BlobSidecarEncoding::Eip4844
    };
    Some(ActiveBlobLimit {
        max_blobs,
        sidecar_encoding,
    })
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

    #[test]
    fn cancun_and_osaka_limits_come_from_the_spec() {
        use eez_blob::BlobSidecarEncoding;
        use reth_chainspec::ChainSpecBuilder;
        let cancun = ChainSpecBuilder::mainnet().cancun_activated().build();
        let osaka = ChainSpecBuilder::mainnet().osaka_activated().build();
        let cancun_limit = blob_limit_at(&cancun, 1).unwrap();
        let osaka_limit = blob_limit_at(&osaka, 1).unwrap();
        assert!(cancun_limit.max_blobs > 0);
        assert_eq!(cancun_limit.sidecar_encoding, BlobSidecarEncoding::Eip4844);
        assert_eq!(osaka_limit.sidecar_encoding, BlobSidecarEncoding::Eip7594);
        assert!(blob_limit_at(&ChainSpecBuilder::mainnet().build(), 0).is_none());
    }

    #[test]
    fn the_fork_maxima_bound_capacity_instead_of_the_fee_target() {
        let mut spec = reth_chainspec::ChainSpecBuilder::mainnet()
            .cancun_activated()
            .build();
        spec.blob_params.cancun.target_blob_count = 1;
        spec.blob_params.cancun.max_blob_count = 4;
        spec.blob_params.cancun.max_blobs_per_tx = 6;
        assert_eq!(blob_limit_at(&spec, 1).unwrap().max_blobs, 4);
        spec.blob_params.cancun.max_blobs_per_tx = 2;
        assert_eq!(blob_limit_at(&spec, 1).unwrap().max_blobs, 2);
        spec.blob_params.cancun.max_blob_count = 0;
        assert!(blob_limit_at(&spec, 1).is_none());
    }
}
