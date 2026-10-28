//! Composer blob posting: capacity, fee limits, and sidecar preparation.

use alloy_primitives::B256;

/// Reserve the stream envelope, close marker, and worst-case block metadata
/// for a complete Sync interval. Transaction bytes are charged separately,
/// including the maximum five-byte length prefix for each transaction.
pub(super) fn ordinary_interval_budget(k: u64, blobs: usize) -> Result<usize, String> {
    let capacity = eez_payload_codec::logical_blob_capacity(blobs);
    let k = usize::try_from(k)
        .ok()
        .filter(|k| *k != 0 && *k <= capacity)
        .ok_or("blob allowance cannot carry an empty Sync interval")?;
    // Alternate metadata so every block starts a new run in the columnar codec.
    let blocks: Vec<_> = (0..k)
        .map(|index| eez_payload_codec::SpanBlock {
            beneficiary: [(index % 2) as u8; 20],
            extra_data: vec![(index % 2) as u8; eez_payload_codec::MAX_EXTRA_DATA],
            transactions: Vec::new(),
        })
        .collect();
    let overhead = eez_payload_codec::encode_container(1, &blocks, &[])
        .map_err(|error| error.to_string())?
        .len();
    // Reserve four extra bytes for each block's transaction count and the
    // operations-length prefix, which can grow from one to five bytes.
    let growth = k
        .checked_add(1)
        .and_then(|fields| fields.checked_mul(4))
        .ok_or("sequencing size overflow")?;
    capacity
        .checked_sub(overhead)
        .and_then(|bytes| bytes.checked_sub(1)) // CloseBlobStream
        .and_then(|bytes| bytes.checked_sub(growth))
        .ok_or_else(|| "blob allowance cannot carry an empty Sync interval".into())
}

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

/// Prepare the type-3 transaction sidecar and blob fee.
pub(super) async fn prepare_blob_post(
    l1_provider: &alloy_provider::RootProvider,
    config: &BlobPostingConfig,
    blobs: &[Vec<u8>],
    encoding: eez_blob::BlobSidecarEncoding,
) -> Result<BlobCarrier, String> {
    use alloy_provider::Provider as _;
    // A cap below the current blob base fee cannot land; refuse before proving.
    let blob_base_fee = l1_provider
        .get_blob_base_fee()
        .await
        .map_err(|e| format!("eth_blobBaseFee: {e}"))?;
    if blob_base_fee > config.max_fee_per_blob_gas {
        return Err(format!(
            "blob base fee {blob_base_fee} exceeds EEZ_L1_MAX_FEE_PER_BLOB_GAS {}; deferring the post",
            config.max_fee_per_blob_gas,
        ));
    }
    let max_fee_per_blob_gas = blob_base_fee
        .saturating_mul(BLOB_FEE_HEADROOM)
        .clamp(1, config.max_fee_per_blob_gas);
    let sidecar = eez_blob::prepare_blob_sidecar(blobs, encoding)
        .map_err(|e| format!("prepare blob sidecar: {e}"))?;
    Ok(BlobCarrier {
        sidecar,
        max_fee_per_blob_gas,
    })
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;

    use super::*;

    #[test]
    fn interval_budget_reserves_all_blocks_and_envelope() {
        for k in [2, 5, 6, 128] {
            for blobs in [1, 2, 6] {
                let budget = ordinary_interval_budget(k, blobs).unwrap();
                let mut blocks = (0..k)
                    .map(|index| eez_payload_codec::SpanBlock {
                        beneficiary: [index as u8; 20],
                        extra_data: vec![index as u8; 32],
                        transactions: Vec::new(),
                    })
                    .collect::<Vec<_>>();
                blocks[0].transactions.push(vec![0xaa; budget - 5]);
                assert!(
                    eez_payload_codec::encode_blob_container(1, &blocks, &[])
                        .unwrap()
                        .len()
                        <= blobs
                );
                blocks[0].transactions.clear();
                for index in 0..budget / 6 {
                    blocks[index % k as usize].transactions.push(vec![0xaa]);
                }
                assert!(
                    eez_payload_codec::encode_blob_container(1, &blocks, &[])
                        .unwrap()
                        .len()
                        <= blobs
                );
            }
        }
        assert!(ordinary_interval_budget(6, 0).is_err());
        assert!(ordinary_interval_budget(0, 2).is_err());
        assert!(ordinary_interval_budget(u64::MAX, 2).is_err());
    }

    fn config() -> BlobPostingConfig {
        BlobPostingConfig {
            chain_spec: std::sync::Arc::new(
                reth_chainspec::ChainSpecBuilder::mainnet()
                    .cancun_activated()
                    .build(),
            ),
            slot_seconds: 12,
            max_fee_per_blob_gas: 50,
        }
    }

    fn mocked() -> (Asserter, alloy_provider::RootProvider) {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::default().connect_mocked_client(asserter.clone());
        (asserter, provider)
    }

    #[tokio::test]
    async fn a_blob_post_uses_a_landable_blob_fee() {
        let blobs = vec![vec![0_u8; eez_blob::BYTES_PER_BLOB]];
        let encoding = eez_blob::BlobSidecarEncoding::Eip4844;
        let (asserter, provider) = mocked();
        asserter.push_success(&U256::from(10));
        let carrier = prepare_blob_post(&provider, &config(), &blobs, encoding)
            .await
            .unwrap();
        assert_eq!(
            carrier.max_fee_per_blob_gas, 20,
            "2x headroom under the cap"
        );
        assert_eq!(carrier.sidecar.versioned_hashes().len(), 1);

        // Twice the base fee would pass the cap, so the cap is offered.
        let (asserter, provider) = mocked();
        asserter.push_success(&U256::from(40));
        let carrier = prepare_blob_post(&provider, &config(), &blobs, encoding)
            .await
            .unwrap();
        assert_eq!(carrier.max_fee_per_blob_gas, 50);

        // Above the cap the post cannot land, so it is refused before proving.
        let (asserter, provider) = mocked();
        asserter.push_success(&U256::from(51));
        let refused = prepare_blob_post(&provider, &config(), &blobs, encoding).await;
        assert!(refused.is_err_and(|e| e.contains("deferring")));
    }
}
