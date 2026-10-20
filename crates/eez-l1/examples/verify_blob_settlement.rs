//! Verify canonical settlement posts against beacon DA and the L2 block bodies.

use std::{env, error::Error};

use alloy_eips::{BlockId, BlockNumberOrTag, Encodable2718};
use alloy_provider::{Provider, ProviderBuilder, ext::DebugApi};
use eez_l1::{L1Reader, L1ReaderConfig};
use eez_protocol::Rollup0Deployment;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let l2 = ProviderBuilder::new().connect_http(env::var("EEZ_L2_RPC_URL")?.parse()?);
    let config = L1ReaderConfig::from_env()?;
    let reader = L1Reader::new(config.clone());
    let deployment =
        Rollup0Deployment::from_env(reader.chain_id().await?, l2.get_chain_id().await?)?
            .ok_or("blob settlement is not configured")?;
    let from = config.deploy_block;
    let reader = L1Reader::new(config.with_candidate(Some(deployment))?);
    let mut chunks = reader.batch_log_chunks(from).await?;
    let mut verified = Vec::new();
    let mut settled_through = 0;
    while let Some(posts) = reader.next_batch_log_chunk(&mut chunks).await? {
        for post in posts
            .into_iter()
            .filter(|post| !post.blob_hashes.is_empty() && !post.settlement.is_empty())
        {
            let blobs = reader.post_blobs(&post).await?;
            let payload = eez_payload_codec::decode_blob_container(&blobs)?;
            if payload.rollup_id != deployment.eez_rollup_id {
                return Err("blob payload names another rollup".into());
            }
            let endpoint = post
                .settlement
                .final_state
                .ok_or("settled post has no endpoint")?;
            let mut hash = endpoint;
            let span = &payload.span;
            let mut end = span.transactions.len();
            for index in (0..span.block_count()).rev() {
                let raw = l2.debug_get_raw_block(BlockId::hash(hash)).await?;
                let block = alloy_rlp::decode_exact::<eez_primitives::Block>(&raw)?;
                settled_through = settled_through.max(block.header.number);
                if block.header.hash_slow() != hash {
                    return Err("L2 returned another block".into());
                }
                let count = span.block_tx_counts[index] as usize;
                let start = end
                    .checked_sub(count)
                    .ok_or("payload transaction count overflow")?;
                let expected = &span.transactions[start..end];
                // Candidate V1 reconstructs inbound protocol transactions after
                // the terminal block's pure prefix; earlier blocks are exact.
                let actual: Vec<_> = block
                    .body
                    .transactions
                    .iter()
                    .map(Encodable2718::encoded_2718)
                    .collect();
                if block.header.beneficiary.into_array() != span.beneficiaries[index]
                    || block.header.extra_data.as_ref() != span.extra_data[index].as_slice()
                    || actual.get(..count) != Some(expected)
                    || (index + 1 != span.block_count() && actual.len() != count)
                    || block
                        .body
                        .transactions
                        .iter()
                        .skip(count)
                        .any(|tx| !matches!(tx, eez_primitives::EezTxEnvelope::System(_)))
                {
                    return Err(format!(
                        "blob payload differs from L2 block {}",
                        block.header.number
                    )
                    .into());
                }
                end = start;
                hash = block.header.parent_hash;
            }
            if end != 0 {
                return Err("blob payload has unassigned transactions".into());
            }
            let parent = post
                .batch
                .entries
                .first()
                .and_then(|entry| {
                    entry
                        .rollupUpdates
                        .iter()
                        .find(|update| update.rollupId == deployment.eez_rollup_id)
                })
                .ok_or("blob post has no Rollup0 anchor")?
                .currentRoot;
            if hash != parent {
                return Err("blob span does not extend its settled parent".into());
            }
            verified.push(json!({
                "transaction_hash": post.tx_hash,
                "l1_block_hash": post.id.l1_block_hash,
                "endpoint": endpoint,
                "blobs": post.blob_hashes.len(),
                "blocks": span.block_count(),
                "actions": payload.actions.len(),
                "l2_transaction_hashes": span.transactions.iter().map(alloy_primitives::keccak256).collect::<Vec<_>>(),
                "parent": hash,
            }));
        }
    }
    if verified.is_empty() {
        return Err("no canonical blob settlement was verified".into());
    }
    let safe = l2
        .get_block_by_number(BlockNumberOrTag::Safe)
        .await?
        .filter(|block| block.header.number >= settled_through)
        .ok_or("L2 safe head has not reached the verified settlement")?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "posts": verified, "safe_l2_block": safe.header.number, "safe_l2_hash": safe.header.hash,
        }))?
    );
    Ok(())
}
