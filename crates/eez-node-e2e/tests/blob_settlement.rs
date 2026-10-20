//! Blob settlement through the real signer, L1 execution, and independent replay.

#[path = "blob_settlement/beacon.rs"]
mod beacon;

use std::{collections::HashSet, time::Duration};

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::SolEvent as _;
use anyhow::Result;
use eez_testkit::{
    ANVIL_KEY_1, BuilderStubMode, Harness, IEEZ, L1_BLOCK_TIME_SECS, NodeBinary, NodeConfig,
    NodeHandle, override_env, sign_and_send, signals, wait_for, wait_for_latest_height,
    wait_for_safe_chain_contains, wait_for_safe_prefix_convergence,
};

const TIMEOUT: Duration = Duration::from_mins(3);

async fn fixture() -> Result<(beacon::Beacon, Harness)> {
    let beacon = beacon::Beacon::spawn().await?;
    let harness = Harness::blob(&beacon.url).await?;
    beacon.connect(&harness.anvil.rpc_url);
    Ok((beacon, harness))
}

async fn composer(harness: &Harness, env: &[(&'static str, String)]) -> Result<NodeHandle> {
    NodeHandle::start(
        "blob-composer",
        &NodeConfig {
            genesis_path: Some(harness.l2_genesis_path()),
            ..Default::default()
        },
        env,
    )
    .await
}

async fn follower(harness: &Harness) -> Result<NodeHandle> {
    let env = harness.follower_env(None).await?;
    assert!(
        !env.iter()
            .any(|(key, _)| matches!(*key, "EEZ_SEQUENCER_RPC" | "EEZ_PROVER_URL"))
    );
    NodeHandle::start(
        "blob-independent-follower",
        &NodeConfig {
            binary: NodeBinary::Follower,
            genesis_path: Some(harness.l2_genesis_path()),
        },
        &env,
    )
    .await
}

async fn checkpoint(harness: &Harness, node: &NodeHandle, minimum: u64) -> Result<(u64, B256)> {
    let provider = ProviderBuilder::new().connect_http(node.l2_rpc_url().parse()?);
    wait_for(TIMEOUT, || async {
        let root = harness.chain().commitment().await?;
        let Some(block) = provider.get_block_by_hash(root).await? else {
            return Ok(None);
        };
        Ok((block.header.number >= minimum).then_some((block.header.number, root)))
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fee_refusal_precedes_signing_then_multiblob_catchup_replays_independently() -> Result<()> {
    let (beacon, harness) = fixture().await?;
    beacon.quote_fee(100);
    let env = override_env(harness.env().await?, "EEZ_L1_MAX_FEE_PER_BLOB_GAS", "1");
    let env = override_env(env, "EEZ_L1_RPC_URL", &format!("{}/rpc", beacon.url));
    let node = composer(&harness, &env).await?;
    wait_for_latest_height(&node, 6, TIMEOUT).await?;
    wait_for(TIMEOUT, || async {
        Ok((node.log_count_matching(&["exceeds EEZ_L1_MAX_FEE_PER_BLOB_GAS"])? > 0).then_some(()))
    })
    .await?;
    assert_eq!(harness.successful_attestations()?, 0);
    assert_eq!(
        harness.chain().commitment().await?,
        harness.dep.initial_commitment
    );
    assert!(beacon.canonical().await?.is_empty());

    let rpc = node.l2_rpc_url();
    let provider = ProviderBuilder::new().connect_http(rpc.parse()?);
    let chain_id = provider.get_chain_id().await?;
    let recipient = Address::repeat_byte(0x95);
    let mut hashes = Vec::new();
    let mut minimum = 0;
    for nonce in 0..3 {
        let hash = sign_and_send(
            &rpc,
            ANVIL_KEY_1,
            chain_id,
            nonce,
            Some(recipient),
            U256::from(1),
            vec![0xff; 60_000],
            4_000_000,
        )
        .await?;
        let receipt = wait_for(TIMEOUT, || async {
            Ok(provider.get_transaction_receipt(hash).await?)
        })
        .await?;
        assert!(receipt.status());
        minimum = minimum.max(receipt.block_number.unwrap());
        hashes.push(hash);
    }
    assert_eq!(harness.successful_attestations()?, 0);
    beacon.quote_fee(0);
    let posted = wait_for(TIMEOUT, || async {
        Ok(beacon
            .canonical()
            .await?
            .into_iter()
            .find(|capture| capture.blobs.len() > 1))
    })
    .await?;
    let payload = eez_payload_codec::decode_blob_container(&posted.blobs)?;
    let committed: HashSet<_> = payload.span.transactions.iter().map(keccak256).collect();
    assert!(hashes.iter().all(|hash| committed.contains(hash)));
    assert!(harness.successful_attestations()? > 0);

    harness.set_builder_mode(BuilderStubMode::Drop).await?;
    let (height, root) = checkpoint(&harness, &node, minimum).await?;
    let independent = follower(&harness).await?;
    wait_for_safe_chain_contains(&independent, height, root, TIMEOUT).await?;
    let replay = ProviderBuilder::new().connect_http(independent.l2_rpc_url().parse()?);
    assert_eq!(
        replay
            .get_balance(recipient)
            .block_id(BlockNumberOrTag::Safe.into())
            .await?,
        U256::from(3)
    );
    node.assert_no_divergence_failure_logs();
    independent.assert_no_divergence_failure_logs();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missed_l1_slot_discards_stale_context_and_settles_again() -> Result<()> {
    let (_beacon, harness) = fixture().await?;
    let node = composer(&harness, &harness.env().await?).await?;
    let chain = harness.chain();
    let before = chain.wait_for_batches(2, TIMEOUT).await?;
    harness.set_builder_mode(BuilderStubMode::Drop).await?;
    chain.set_interval_mining(0).await?;
    let l1 = ProviderBuilder::new().connect_http(harness.anvil.rpc_url.parse()?);
    let parent = l1
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await?
        .unwrap();
    let latest = ProviderBuilder::new()
        .connect_http(node.l2_rpc_url().parse()?)
        .get_block_number()
        .await?;
    wait_for_latest_height(&node, latest + 4, TIMEOUT).await?;
    let timestamp = parent.header.timestamp + 3 * L1_BLOCK_TIME_SECS;
    let _: serde_json::Value = l1
        .client()
        .request("evm_setNextBlockTimestamp", (timestamp,))
        .await?;
    chain.mine().await?;
    let skipped = l1
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await?
        .unwrap();
    assert_eq!(skipped.header.number, parent.header.number + 1);
    assert_eq!(skipped.header.timestamp, timestamp);
    chain.set_interval_mining(L1_BLOCK_TIME_SECS).await?;
    harness.set_builder_mode(BuilderStubMode::Forward).await?;
    chain
        .wait_for_batches_or_node_failure(before + 2, &[&node], TIMEOUT)
        .await?;
    wait_for_safe_prefix_convergence(&[&node], latest + 1, TIMEOUT).await?;
    node.assert_no_divergence_failure_logs();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reorg_removes_orphaned_blob_posts_and_both_nodes_resume() -> Result<()> {
    let (beacon, harness) = fixture().await?;
    let node = composer(&harness, &harness.env().await?).await?;
    let chain = harness.chain();
    chain.wait_for_batches(3, TIMEOUT).await?;
    let independent = follower(&harness).await?;
    harness.set_builder_mode(BuilderStubMode::Drop).await?;
    chain.set_interval_mining(0).await?;
    let (height, root) = checkpoint(&harness, &node, 2).await?;
    wait_for_safe_chain_contains(&independent, height, root, TIMEOUT).await?;
    let before = chain.batches_posted().await?;
    let l1 = ProviderBuilder::new().connect_http(harness.anvil.rpc_url.parse()?);
    let posts = l1
        .get_logs(
            &Filter::new()
                .address(harness.dep.eez_address)
                .event_signature(IEEZ::BatchPosted::SIGNATURE_HASH)
                .from_block(harness.dep.deploy_block),
        )
        .await?;
    let post = posts.last().unwrap();
    let orphan_hash = post.block_hash.unwrap();
    assert!(
        beacon
            .canonical()
            .await?
            .iter()
            .any(|capture| capture.block_hash == orphan_hash)
    );
    let depth = l1.get_block_number().await? - post.block_number.unwrap() + 1;
    let cursors = [node.signal_cursor()?, independent.signal_cursor()?];
    harness.anvil.reorg(depth).await?;
    assert!(
        !beacon
            .canonical()
            .await?
            .iter()
            .any(|capture| capture.block_hash == orphan_hash)
    );
    for (peer, cursor) in [&node, &independent].into_iter().zip(cursors) {
        wait_for(TIMEOUT, || async {
            Ok(peer.signals_since(cursor)?.into_iter().find(|signal| {
                signal.name == signals::DERIVER_REORG_RETREATED
                    && signal.u64("dropped_batches").is_ok_and(|count| count > 0)
            }))
        })
        .await?;
    }
    assert_ne!(chain.commitment().await?, root);
    chain.set_interval_mining(L1_BLOCK_TIME_SECS).await?;
    harness.set_builder_mode(BuilderStubMode::Forward).await?;
    chain
        .wait_for_batches_or_node_failure(before + 1, &[&node, &independent], TIMEOUT)
        .await?;
    wait_for_safe_prefix_convergence(&[&node, &independent], height + 1, TIMEOUT).await?;
    node.assert_no_divergence_failure_logs();
    independent.assert_no_divergence_failure_logs();
    Ok(())
}
