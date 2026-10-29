//! Live blob evidence checks, selected only by the blob Kurtosis runner.
use std::{collections::HashSet, env, fs, time::Duration};

use alloy_primitives::{B256, U256};
use alloy_sol_types::SolEvent as _;
use anyhow::{Context, Result, ensure};
use eez_l1::{EntryRole, L1Reader, L1ReaderConfig};
use eez_protocol::{RollupId, abi::ExecutionConsumed, entries::manifest};
use serde_json::{Value, json};

async fn rpc(client: &reqwest::Client, url: &str, method: &str, params: Value) -> Result<Value> {
    let response: Value = client
        .post(url)
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(response.get("error").is_none(), "{method}: {response}");
    response
        .get("result")
        .cloned()
        .context("missing RPC result")
}

fn verify_actions(
    batch: &eez_protocol::abi::EvmBatch,
    actions: &[eez_payload_codec::Action],
    rollup_id: u64,
) -> Result<()> {
    let mut actions = actions.iter();
    let mut rebuilt_batch = batch.clone();
    for (index, role) in eez_protocol::entries::entry_roles(&batch.entries, rollup_id)
        .into_iter()
        .enumerate()
    {
        if matches!(role, None | Some(EntryRole::Anchor)) {
            continue;
        }
        let action = actions.next().context("entry has no DA action")?;
        let rebuilt = manifest::entry_from_action(action, RollupId(rollup_id))?;
        let entry = &batch.entries[index];
        ensure!(
            entry.success == action.success,
            "action outcome differs from on-chain entry"
        );
        if matches!(role, Some(EntryRole::Inbound { .. })) {
            ensure!(
                entry.returnData.as_ref() == action.return_data.as_slice(),
                "inbound return data differs from on-chain entry"
            );
            ensure!(
                entry.proxyEntryHash == rebuilt.proxyEntryHash,
                "inbound action hash mismatch"
            );
        } else {
            ensure!(
                entry.returnData.is_empty(),
                "immediate outbound entry must have empty on-chain return data"
            );
            // Immediate return data is committed by the rolling hash.
            rebuilt_batch.entries[index].returnData = action.return_data.clone().into();
            ensure!(
                manifest::action_from_entry(&rebuilt_batch.entries[index], RollupId(rollup_id),)?
                    == *action,
                "outbound action differs from on-chain call"
            );
        }
    }
    ensure!(actions.next().is_none(), "unbound DA actions");
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut rebuilt_batch)?;
    for (rebuilt, entry) in rebuilt_batch.entries.iter().zip(&batch.entries) {
        ensure!(
            rebuilt.rollingHash == entry.rollingHash,
            "rebuilt rolling hash differs from on-chain entry"
        );
    }
    Ok(())
}

#[tokio::test]
async fn blob_settlement_evidence() -> Result<()> {
    ensure!(
        env::var("EEZ_SETTLEMENT_PROFILE").as_deref() == Ok("blob"),
        "blob_settlement requires EEZ_SETTLEMENT_PROFILE=blob"
    );
    let config = L1ReaderConfig::from_env()?;
    let rollup_id = config.rollup_id;
    let eez = config.eez;
    let rpc_url = config.rpc_url.to_string();
    let reader = L1Reader::new(config);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let scenarios: Value = serde_json::from_str(&fs::read_to_string(
        env::var("EEZ_SETTLEMENT_SCENARIOS")
            .context("set EEZ_SETTLEMENT_SCENARIOS to the runner's manifest")?,
    )?)?;
    let mut evidence = Vec::new();
    for scenario in scenarios
        .as_array()
        .context("scenario manifest must be an array")?
    {
        let mode = scenario["mode"].as_str().context("missing scenario mode")?;
        let first = scenario["from"]
            .as_u64()
            .context("missing first L1 block")?;
        let last = scenario["to"].as_u64().context("missing last L1 block")?;
        let mut chunks = reader.batch_log_chunks(first).await?;
        let (mut inbound, mut outbound) = (0, 0);
        let mut published_transactions = HashSet::new();
        let mut inbound_calls = HashSet::new();
        while let Some(posts) = reader.next_batch_log_chunk(&mut chunks).await? {
            for post in posts.into_iter().filter(|post| post.id.l1_block <= last) {
                ensure!(
                    post.batch.blockNumber == 0,
                    "nonzero blob batch block number"
                );
                ensure!(
                    !post.blob_hashes.is_empty(),
                    "calldata fallback in blob lane"
                );
                ensure!(post.batch.callData.is_empty(), "nonempty batch calldata");
                ensure!(
                    post.batch
                        .blobIndices
                        .iter()
                        .copied()
                        .eq((0..post.blob_hashes.len()).map(U256::from)),
                    "blob indices must select every blob in order"
                );
                let tx = rpc(
                    &client,
                    &rpc_url,
                    "eth_getTransactionByHash",
                    json!([post.tx_hash]),
                )
                .await?;
                ensure!(
                    tx["type"] == "0x3",
                    "settlement must be a type-3 transaction"
                );
                ensure!(
                    tx["to"]
                        .as_str()
                        .context("missing tx destination")?
                        .parse::<alloy_primitives::Address>()?
                        == eez,
                    "settlement must call EEZ directly"
                );
                let hashes: Vec<B256> = serde_json::from_value(tx["blobVersionedHashes"].clone())?;
                ensure!(
                    hashes == post.blob_hashes,
                    "transaction blob hashes differ from batch carrier"
                );
                let receipt = rpc(
                    &client,
                    &rpc_url,
                    "eth_getTransactionReceipt",
                    json!([post.tx_hash]),
                )
                .await?;
                ensure!(receipt["status"] == "0x1", "settlement reverted");
                ensure!(
                    receipt["blockHash"] == json!(post.id.l1_block_hash),
                    "receipt moved to another block"
                );

                // The production reader pins the beacon slot to this execution block
                // and recomputes KZG commitments before selecting transaction blobs.
                let blobs = reader.post_blobs(&post).await?;
                let container = eez_payload_codec::decode_blob_container(&blobs)?;
                ensure!(container.rollup_id == rollup_id, "foreign blob container");
                published_transactions.extend(
                    container
                        .span
                        .transactions
                        .iter()
                        .map(alloy_primitives::keccak256),
                );
                verify_actions(&post.batch, &container.actions, rollup_id)?;
                for (index, role) in
                    eez_protocol::entries::entry_roles(&post.batch.entries, rollup_id)
                        .into_iter()
                        .enumerate()
                {
                    if matches!(role, None | Some(EntryRole::Anchor)) {
                        continue;
                    }
                    if post
                        .settlement
                        .applied()
                        .iter()
                        .any(|entry| entry.entry_index == index)
                    {
                        match role {
                            Some(EntryRole::Inbound { .. }) => {
                                inbound += 1;
                                inbound_calls.insert((
                                    post.id.l1_block_hash,
                                    post.batch.entries[index].proxyEntryHash,
                                ));
                            }
                            Some(EntryRole::Outbound { .. }) => outbound += 1,
                            _ => unreachable!(),
                        }
                    }
                }
                ensure!(
                    reader.canonical_l1_hash(post.id.l1_block).await?
                        == Some(post.id.l1_block_hash),
                    "settlement reorged during evidence collection"
                );
                evidence.push(json!({"mode": mode, "tx_hash": post.tx_hash,
                    "block_hash": post.id.l1_block_hash, "blob_hashes": hashes,
                    "actions": container.actions.len()}));
            }
        }
        let transactions = fs::read_to_string(
            scenario["transactions"]
                .as_str()
                .context("missing wave transaction manifest")?,
        )?;
        ensure!(
            !transactions.trim().is_empty(),
            "{mode}: no submitted transactions"
        );
        for line in transactions.lines() {
            let (hash, side) = line.split_once('|').context("malformed wave transaction")?;
            let hash: B256 = hash.parse()?;
            if side == "out" {
                ensure!(
                    published_transactions.contains(&hash),
                    "{mode}: outbound transaction {hash} absent from blob DA"
                );
            } else {
                ensure!(side == "in", "unknown wave direction");
                let receipt = rpc(
                    &client,
                    &rpc_url,
                    "eth_getTransactionReceipt",
                    json!([hash]),
                )
                .await?;
                ensure!(
                    receipt["status"] == "0x1",
                    "{mode}: inbound transaction {hash} did not succeed"
                );
                let block: B256 = serde_json::from_value(receipt["blockHash"].clone())?;
                let logs: Vec<alloy_rpc_types_eth::Log> =
                    serde_json::from_value(receipt["logs"].clone())?;
                let calls: Vec<_> = logs
                    .iter()
                    .filter(|log| log.address() == eez)
                    .filter_map(|log| ExecutionConsumed::decode_log(&log.inner).ok())
                    .filter(|log| log.rollupId == rollup_id)
                    .collect();
                ensure!(
                    !calls.is_empty(),
                    "{mode}: inbound transaction {hash} consumed no rollup delivery"
                );
                for call in calls {
                    ensure!(
                        inbound_calls.contains(&(block, call.crossChainCallHash)),
                        "{mode}: inbound transaction {hash} consumed a call absent from applied blob entries"
                    );
                }
            }
        }
        if mode == "inbound" || mode == "mixed" || mode == "mixed-pure" {
            ensure!(inbound > 0, "{mode}: no inbound blob settlement observed");
        }
        if mode == "outbound" || mode == "mixed" || mode == "mixed-pure" {
            ensure!(outbound > 0, "{mode}: no outbound blob settlement observed");
        }
    }
    ensure!(!evidence.is_empty(), "no blob evidence collected");
    fs::write(
        env::var("EEZ_BLOB_EVIDENCE")?,
        serde_json::to_vec_pretty(&evidence)?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eez_payload_codec::{Action, SpanBlock};
    use eez_protocol::abi::{EvmBatch, RollupUpdateSol};
    use eez_protocol::entries::finalize_l1_rolling_hashes;

    const ROLLUP: u64 = 7;

    fn action(outbound: bool) -> Action {
        Action {
            source_rollup_id: if outbound { ROLLUP } else { 0 },
            target_rollup_id: if outbound { 0 } else { ROLLUP },
            source_address: [0x11; 20],
            target_address: [0x22; 20],
            value: [0; 32],
            gas: 0,
            data: vec![1, 2, 3, 4],
            success: true,
            return_data: vec![0x42; 32],
        }
    }

    fn settled_batch(actions: &[Action]) -> EvmBatch {
        let entries = actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                let mut entry = manifest::entry_from_action(action, RollupId(ROLLUP)).unwrap();
                entry.rollupUpdates = vec![RollupUpdateSol {
                    rollupId: ROLLUP,
                    currentRoot: B256::repeat_byte(index as u8),
                    newRoot: B256::repeat_byte((index + 1) as u8),
                    etherDelta: alloy_primitives::aliases::I192::ZERO,
                }];
                if action.target_rollup_id == ROLLUP {
                    entry.l2ToL1Calls.clear();
                }
                entry
            })
            .collect();
        let mut batch = EvmBatch {
            entries,
            immediateEntryCount: U256::from(
                actions
                    .iter()
                    .take_while(|a| a.source_rollup_id == ROLLUP)
                    .count(),
            ),
            ..Default::default()
        };
        finalize_l1_rolling_hashes(&mut batch).unwrap();
        batch
    }

    #[test]
    fn accepts_mixed_entries_with_folded_outbound_return_data() {
        let actions = [action(true), action(false)];
        let batch = settled_batch(&actions);
        assert!(batch.entries[0].returnData.is_empty());
        assert!(!batch.entries[1].returnData.is_empty());
        verify_actions(&batch, &actions, ROLLUP).unwrap();
    }

    #[test]
    fn rejects_tampered_outbound_return_data() {
        let mut actions = [action(true)];
        let batch = settled_batch(&actions);
        actions[0].return_data[0] ^= 1;
        let error = verify_actions(&batch, &actions, ROLLUP).unwrap_err();
        assert!(error.to_string().contains("rolling hash"));
    }

    #[test]
    fn rejects_uncleared_on_chain_outbound_return_data() {
        let actions = [action(true)];
        let mut batch = settled_batch(&actions);
        batch.entries[0].returnData = actions[0].return_data.clone().into();
        let error = verify_actions(&batch, &actions, ROLLUP).unwrap_err();
        assert!(error.to_string().contains("empty on-chain return data"));
    }

    #[test]
    fn rejects_tampered_inbound_call_and_return_data() {
        let actions = [action(false)];
        let batch = settled_batch(&actions);
        let mut changed = actions.clone();
        changed[0].data[0] ^= 1;
        let error = verify_actions(&batch, &changed, ROLLUP).unwrap_err();
        assert!(error.to_string().contains("inbound action hash mismatch"));
        changed = actions.clone();
        changed[0].return_data[0] ^= 1;
        let error = verify_actions(&batch, &changed, ROLLUP).unwrap_err();
        assert!(error.to_string().contains("inbound return data"));
    }

    #[test]
    fn rejects_missing_extra_and_reordered_actions() {
        let actions = [action(true), action(false)];
        let batch = settled_batch(&actions);
        assert!(verify_actions(&batch, &actions[..1], ROLLUP).is_err());
        let extra = [actions[0].clone(), actions[1].clone(), action(false)];
        assert!(verify_actions(&batch, &extra, ROLLUP).is_err());
        let reordered = [actions[1].clone(), actions[0].clone()];
        assert!(verify_actions(&batch, &reordered, ROLLUP).is_err());
    }

    #[test]
    fn rejects_tampered_outbound_call_and_outcome() {
        let actions = [action(true)];
        let batch = settled_batch(&actions);
        let mut changed = actions.clone();
        changed[0].data[0] ^= 1;
        assert!(verify_actions(&batch, &changed, ROLLUP).is_err());
        changed = actions.clone();
        changed[0].success = false;
        assert!(verify_actions(&batch, &changed, ROLLUP).is_err());
    }

    #[test]
    fn verifies_anchor_hashes_without_consuming_actions() {
        let mut batch = settled_batch(&[action(true)]);
        batch.entries[0].l2ToL1Calls.clear();
        finalize_l1_rolling_hashes(&mut batch).unwrap();
        verify_actions(&batch, &[], ROLLUP).unwrap();
        batch.entries[0].rollingHash = B256::ZERO;
        assert!(verify_actions(&batch, &[], ROLLUP).is_err());
    }

    #[test]
    fn decodes_actions_after_a_failed_return_but_rejects_settlement() {
        let mut failed = action(true);
        failed.success = false;
        let actions = [failed, action(false)];
        let blocks = [SpanBlock::default()];
        let blobs = eez_payload_codec::encode_blob_container(ROLLUP, &blocks, &actions).unwrap();
        let decoded = eez_payload_codec::decode_blob_container(&blobs).unwrap();
        assert_eq!(decoded.actions, actions);
        // Wire decoding is permissive; a failed action cannot describe the
        // successful entries admitted by the current settlement profile.
        let batch = settled_batch(&[action(true), action(false)]);
        let error = verify_actions(&batch, &decoded.actions, ROLLUP).unwrap_err();
        assert!(error.to_string().contains("action outcome"));
    }

    #[test]
    fn verifies_blob_inbound_action_from_another_rollup() {
        let mut inbound = action(false);
        inbound.source_rollup_id = ROLLUP + 1;
        let actions = [inbound];
        let batch = settled_batch(&actions);
        let blobs =
            eez_payload_codec::encode_blob_container(ROLLUP, &[SpanBlock::default()], &actions)
                .unwrap();
        let decoded = eez_payload_codec::decode_blob_container(&blobs).unwrap();
        assert_eq!(decoded.actions, actions);
        verify_actions(&batch, &decoded.actions, ROLLUP).unwrap();
        let mut changed = decoded.actions;
        changed[0].source_rollup_id = 0;
        assert!(verify_actions(&batch, &changed, ROLLUP).is_err());
    }

    #[test]
    fn verifies_mixed_actions_after_two_blob_decode() {
        let actions = [action(true), action(false)];
        let batch = settled_batch(&actions);
        let transaction = vec![0x42; eez_payload_codec::logical_blob_capacity(1)];
        let blocks = [SpanBlock {
            transactions: vec![transaction.clone()],
            ..Default::default()
        }];
        let blobs = eez_payload_codec::encode_blob_container(ROLLUP, &blocks, &actions).unwrap();
        assert_eq!(blobs.len(), 2);
        let decoded = eez_payload_codec::decode_blob_container(&blobs).unwrap();
        assert_eq!(decoded.rollup_id, ROLLUP);
        assert_eq!(decoded.span.transactions, vec![transaction]);
        verify_actions(&batch, &decoded.actions, ROLLUP).unwrap();
        assert!(eez_payload_codec::decode_blob_container(&blobs[..1]).is_err());
        let mut reversed = blobs.clone();
        reversed.reverse();
        assert!(eez_payload_codec::decode_blob_container(&reversed).is_err());
    }
}
