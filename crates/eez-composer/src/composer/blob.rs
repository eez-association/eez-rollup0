//! Composer blob posting: slot targeting, fee limits, and sidecar preparation.

use alloy_primitives::{B256, Bytes};
use eez_l1::BundleTarget;

/// Deployment, fork schedule, slot duration, and fee cap for blob posting.
#[derive(Clone, Debug)]
pub struct BlobPostingConfig {
    pub deployment: eez_protocol::Rollup0Deployment,
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

/// Pin unresolved targets to the slot after the current L1 head. The blob
/// signature binds that slot and its parent; a missed slot requires a new proof.
pub(super) async fn pin_candidate_targets(
    l1_provider: &alloy_provider::RootProvider,
    config: &BlobPostingConfig,
    targets: [BundleTarget; 2],
) -> Result<(BundleTarget, BundleTarget), String> {
    if targets
        .iter()
        .all(|target| matches!(target, BundleTarget::Exact { .. }))
    {
        let [bundle, historical] = targets;
        return Ok((bundle, historical));
    }
    let next = next_candidate_target(l1_provider, config, 0).await?;
    let [bundle, historical] = targets.map(|target| match target {
        BundleTarget::Exact { .. } => target,
        _ => next,
    });
    Ok((bundle, historical))
}

/// Retain a live target or select a later slot on the observed canonical parent.
pub(super) async fn refresh_candidate_target(
    l1_provider: &alloy_provider::RootProvider,
    config: &BlobPostingConfig,
    timing: eez_driver::RollupTiming,
    target: BundleTarget,
    now_ms: u64,
) -> Result<BundleTarget, String> {
    let reserve_ms = u64::try_from(
        timing
            .proof_time()
            .saturating_add(timing.submission_slack())
            .as_millis(),
    )
    .map_err(|_| "candidate-V1 timing overflow")?;
    let earliest_ms = now_ms
        .checked_add(reserve_ms)
        .ok_or("candidate-V1 timing overflow")?;
    if matches!(target, BundleTarget::Exact { timestamp, .. }
        if timestamp.saturating_mul(1_000) > earliest_ms)
    {
        return Ok(target);
    }
    next_candidate_target(l1_provider, config, earliest_ms).await
}

/// Select a slot after the preparation deadline, keeping a known L1 parent.
async fn next_candidate_target(
    l1_provider: &alloy_provider::RootProvider,
    config: &BlobPostingConfig,
    earliest_ms: u64,
) -> Result<BundleTarget, String> {
    use alloy_eips::BlockNumberOrTag;
    use alloy_provider::Provider as _;

    let head = l1_provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await
        .map_err(|error| format!("get candidate-V1 L1 head: {error}"))?
        .ok_or("L1 head unavailable")?;
    let slot_ms = config
        .slot_seconds
        .checked_mul(1_000)
        .filter(|slot| *slot != 0)
        .ok_or("invalid L1 slot duration")?;
    let slots = earliest_ms
        .saturating_sub(head.header.timestamp.saturating_mul(1_000))
        .checked_div(slot_ms)
        .and_then(|slots| slots.checked_add(1))
        .ok_or("candidate-V1 slot overflow")?;
    Ok(BundleTarget::Exact {
        block: head
            .header
            .number
            .checked_add(1)
            .ok_or("L1 block number overflow")?,
        timestamp: config
            .slot_seconds
            .checked_mul(slots)
            .and_then(|offset| head.header.timestamp.checked_add(offset))
            .ok_or("candidate-V1 target timestamp overflow")?,
    })
}

/// Type-3 carrier of a candidate-V1 post: its sidecar and blob-gas fee cap.
pub(super) struct BlobCarrier {
    pub(super) sidecar: eez_blob::PreparedBlobSidecar,
    pub(super) max_fee_per_blob_gas: u128,
}

/// Fee headroom over the observed blob base fee, bounded by the operator's cap.
const BLOB_FEE_HEADROOM: u128 = 2;

/// Prepare the slot-bound proving context and the type-3 transaction sidecar.
pub(super) async fn prepare_candidate_v1(
    l1_provider: &alloy_provider::RootProvider,
    config: &BlobPostingConfig,
    bundle_target: BundleTarget,
    blobs: Vec<Vec<u8>>,
    encoding: eez_blob::BlobSidecarEncoding,
    now: u64,
) -> Result<(eez_prover::BlobProvingContext, BlobCarrier), String> {
    use alloy_eips::BlockNumberOrTag;
    use alloy_provider::Provider as _;

    let BundleTarget::Exact {
        block: target_block,
        timestamp: target_timestamp,
    } = bundle_target
    else {
        return Err("candidate V1 needs an exact L1 target".into());
    };
    let parent_number = target_block
        .checked_sub(1)
        .ok_or("candidate V1 cannot target L1 genesis")?;
    let parent = l1_provider
        .get_block_by_number(BlockNumberOrTag::Number(parent_number))
        .await
        .map_err(|e| format!("get candidate-V1 L1 parent {parent_number}: {e}"))?
        .ok_or_else(|| format!("candidate-V1 L1 parent {parent_number} is unavailable"))?;
    if !eez_blob::on_later_slot(
        parent.header.timestamp,
        target_timestamp,
        config.slot_seconds,
    ) {
        return Err(format!(
            "candidate-V1 target {target_timestamp} is not a later L1 slot after parent {parent_number}"
        ));
    }
    // A lagging L1 RPC or slow composition can pin a slot that has already begun.
    if target_timestamp <= now {
        return Err(format!(
            "candidate-V1 target slot {target_timestamp} has already begun (now {now})"
        ));
    }
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
    let sidecar = eez_blob::prepare_blob_sidecar(&blobs, encoding)
        .map_err(|e| format!("prepare candidate-V1 sidecar: {e}"))?;
    let blob = eez_prover::BlobProvingContext {
        blobs: blobs.into_iter().map(Bytes::from).collect(),
        posting_context: eez_prover::L1PostingContext {
            chain_id: config.deployment.l1_chain_id,
            target_timestamp,
            parent_number,
            parent_hash: parent.header.hash,
        },
        expected_custom_data: config
            .deployment
            .custom_data(target_timestamp, parent.header.hash),
    };
    Ok((
        blob,
        BlobCarrier {
            sidecar,
            max_fee_per_blob_gas,
        },
    ))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256};
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;

    use super::*;

    fn config() -> BlobPostingConfig {
        BlobPostingConfig {
            deployment: eez_protocol::Rollup0Deployment {
                l1_chain_id: 1,
                eez: Address::repeat_byte(0xEE),
                manager: Address::repeat_byte(0x3A),
                settlement_wrapper: Address::repeat_byte(0x3B),
                eez_rollup_id: 1,
                l2_chain_id: 10,
            },
            chain_spec: std::sync::Arc::new(
                reth_chainspec::ChainSpecBuilder::mainnet()
                    .cancun_activated()
                    .build(),
            ),
            slot_seconds: 12,
            max_fee_per_blob_gas: 50,
        }
    }

    fn l1_block(number: u64, timestamp: u64, hash: B256) -> alloy_rpc_types_eth::Block {
        let mut block: alloy_rpc_types_eth::Block = Default::default();
        block.header.hash = hash;
        block.header.inner.number = number;
        block.header.inner.timestamp = timestamp;
        block
    }

    fn mocked() -> (Asserter, alloy_provider::RootProvider) {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::default().connect_mocked_client(asserter.clone());
        (asserter, provider)
    }

    #[tokio::test]
    async fn unpinned_targets_become_the_slot_after_the_l1_head() {
        let exact = BundleTarget::Exact {
            block: 9,
            timestamp: 196,
        };
        // Already exact: no L1 read at all.
        let (_, provider) = mocked();
        let pinned = pin_candidate_targets(&provider, &config(), [exact, exact]).await;
        assert_eq!(pinned.unwrap(), (exact, exact));

        let (asserter, provider) = mocked();
        asserter.push_success(&l1_block(41, 112, B256::repeat_byte(1)));
        let targets = [
            BundleTarget::NextBlock,
            BundleTarget::ExactBlock { block: 42 },
        ];
        let next = BundleTarget::Exact {
            block: 42,
            timestamp: 124,
        };
        let pinned = pin_candidate_targets(&provider, &config(), targets).await;
        assert_eq!(pinned.unwrap(), (next, next));
    }

    #[tokio::test]
    async fn expired_preparation_target_moves_to_a_live_slot_on_the_known_parent() {
        let timing = eez_driver::RollupTiming::new(12_000, 2_000, 4_000, 1_500);
        let target = BundleTarget::Exact {
            block: 42,
            timestamp: 124,
        };
        let (_, provider) = mocked();
        assert_eq!(
            refresh_candidate_target(&provider, &config(), timing, target, 118_499)
                .await
                .unwrap(),
            target,
        );
        let (asserter, provider) = mocked();
        asserter.push_success(&l1_block(41, 112, B256::repeat_byte(1)));
        assert_eq!(
            refresh_candidate_target(&provider, &config(), timing, target, 130_500)
                .await
                .unwrap(),
            BundleTarget::Exact {
                block: 42,
                timestamp: 148,
            },
        );
    }

    #[tokio::test]
    async fn a_candidate_binds_its_slot_domain_and_a_landable_blob_fee() {
        let parent_hash = B256::repeat_byte(0x99);
        let target = BundleTarget::Exact {
            block: 42,
            timestamp: 124,
        };
        let blobs = || vec![vec![0_u8; eez_blob::BYTES_PER_BLOB]];
        let encoding = eez_blob::BlobSidecarEncoding::Eip4844;

        let (asserter, provider) = mocked();
        asserter.push_success(&l1_block(41, 112, parent_hash));
        asserter.push_success(&U256::from(10));
        let (blob, carrier) =
            prepare_candidate_v1(&provider, &config(), target, blobs(), encoding, 0)
                .await
                .unwrap();
        assert_eq!(blob.posting_context.parent_hash, parent_hash);
        assert_eq!(blob.posting_context.parent_number, 41);
        assert_eq!(blob.posting_context.target_timestamp, 124);
        assert_eq!(
            blob.expected_custom_data,
            config().deployment.custom_data(124, parent_hash),
        );
        assert_eq!(
            carrier.max_fee_per_blob_gas, 20,
            "2x headroom under the cap"
        );
        assert_eq!(carrier.sidecar.versioned_hashes().len(), 1);

        // Twice the base fee would pass the cap, so the cap is offered.
        let (asserter, provider) = mocked();
        asserter.push_success(&l1_block(41, 112, parent_hash));
        asserter.push_success(&U256::from(40));
        let (_, carrier) = prepare_candidate_v1(&provider, &config(), target, blobs(), encoding, 0)
            .await
            .unwrap();
        assert_eq!(carrier.max_fee_per_blob_gas, 50);

        // Above the cap the post cannot land, so it is refused before proving.
        let (asserter, provider) = mocked();
        asserter.push_success(&l1_block(41, 112, parent_hash));
        asserter.push_success(&U256::from(51));
        let refused =
            prepare_candidate_v1(&provider, &config(), target, blobs(), encoding, 0).await;
        assert!(refused.is_err_and(|e| e.contains("deferring")));

        // The target must be later than its parent.
        let (asserter, provider) = mocked();
        asserter.push_success(&l1_block(41, 124, parent_hash));
        let stale = prepare_candidate_v1(&provider, &config(), target, blobs(), encoding, 0).await;
        assert!(stale.is_err());
        let unpinned = BundleTarget::NextBlock;
        let (_, provider) = mocked();
        let unpinned =
            prepare_candidate_v1(&provider, &config(), unpinned, blobs(), encoding, 0).await;
        assert!(unpinned.is_err());

        // A slot that has already begun cannot be hit by a bundle signed now.
        let (asserter, provider) = mocked();
        asserter.push_success(&l1_block(41, 112, parent_hash));
        let late = prepare_candidate_v1(&provider, &config(), target, blobs(), encoding, 124).await;
        assert!(late.is_err_and(|e| e.contains("already begun")));
    }
}
