//! Live blob evidence checks, selected only by the blob Kurtosis runner.
use std::{collections::HashSet, env, fs, time::Duration};

use alloy_primitives::{B256, U256};
use alloy_sol_types::SolEvent as _;
use anyhow::{Context, Result, ensure};
use eez_l1::{EntryRole, L1Reader, L1ReaderConfig};
use eez_protocol::{Rollup0Deployment, RollupId, abi::ExecutionConsumed, entries::manifest};
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

#[tokio::test]
async fn blob_settlement_evidence() -> Result<()> {
    let deployment = Rollup0Deployment::from_process_env(6290)?
        .context("blob_settlement requires EEZ_SETTLEMENT_PROFILE=blob")?;
    let config = L1ReaderConfig::from_env()?.with_candidate(Some(deployment))?;
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
                    post.sender == deployment.settlement_wrapper,
                    "unexpected settlement wrapper"
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
                        == deployment.settlement_wrapper,
                    "settlement must call the wrapper directly"
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
                ensure!(
                    container.rollup_id == deployment.eez_rollup_id,
                    "foreign blob container"
                );
                published_transactions.extend(
                    container
                        .span
                        .transactions
                        .iter()
                        .map(alloy_primitives::keccak256),
                );
                let mut actions = container.actions.iter();
                for (index, role) in eez_protocol::entries::entry_roles(
                    &post.batch.entries,
                    deployment.eez_rollup_id,
                )
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
                    let action = actions.next().context("entry has no DA action")?;
                    let rebuilt =
                        manifest::entry_from_action(action, RollupId(deployment.eez_rollup_id))?;
                    let entry = &post.batch.entries[index];
                    ensure!(
                        entry.success == action.success
                            && entry.returnData.as_ref() == action.return_data.as_slice(),
                        "action outcome differs from on-chain entry"
                    );
                    if matches!(role, Some(EntryRole::Inbound { .. })) {
                        ensure!(
                            entry.proxyEntryHash == rebuilt.proxyEntryHash,
                            "inbound action hash mismatch"
                        );
                    } else {
                        ensure!(
                            manifest::action_from_entry(entry, RollupId(deployment.eez_rollup_id))?
                                == *action,
                            "outbound action differs from on-chain call"
                        );
                    }
                }
                ensure!(actions.next().is_none(), "unbound DA actions");
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
                    .filter(|log| log.address() == deployment.eez)
                    .filter_map(|log| ExecutionConsumed::decode_log(&log.inner).ok())
                    .filter(|log| log.rollupId == deployment.eez_rollup_id)
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
