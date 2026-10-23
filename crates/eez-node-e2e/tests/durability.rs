//! Restart/durability regressions whose oracle is the L1-confirmed L2 cursor.
//!
//! These deliberately use a persistent datadir and the real node process. A
//! unit test of checkpoint parsing cannot detect an execution database whose
//! canonical head is ahead of the history reconstructible from L1.

use std::time::Duration;

use alloy_primitives::U256;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::BlockNumberOrTag;
use eez_testkit::{
    ANVIL_ADDR_3, ANVIL_KEY_1, Harness, NodeBinary, NodeConfig, NodeHandle, STAGING_USER_KEY,
    block_number_and_hash_at, send_l2_value_transfer_confirmed, signals, wait_for,
    wait_for_latest_height, wait_for_new_attested_safe_block, wait_for_safe_chain_contains,
};

const TIMEOUT: Duration = Duration::from_mins(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_with_datadir_ahead_of_l1_retreats_before_production() {
    // G-10 / P-03 / P-05: stage a crash boundary after L2 commit but before L1.
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let datadir = tempfile::tempdir().unwrap();
    // reth persists only past 50 in-memory blocks by default; at 0 the kill leaves the suffix on disk.
    let cfg = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        extra_args: vec!["--engine.persistence-threshold".into(), "0".into()],
        ..Default::default()
    };
    let env = harness.env_for(STAGING_USER_KEY, true).await.unwrap();

    let staged_tx;
    {
        let node = NodeHandle::start_with_datadir("ahead-of-l1-stage", datadir.path(), &cfg, &env)
            .await
            .unwrap();
        staged_tx = send_l2_value_transfer_confirmed(
            &node.l2_rpc_url(),
            ANVIL_KEY_1,
            ANVIL_ADDR_3,
            U256::from(23u64),
            TIMEOUT,
        )
        .await
        .expect("staged transaction did not land");
        wait_for_latest_height(&node, 4, TIMEOUT)
            .await
            .expect("failed to stage a multi-block unconfirmed suffix");
        // Persistence trails the head; one L1 block lets it flush.
        chain
            .wait_for_l1_blocks(1, Duration::from_secs(20))
            .await
            .expect("L1 did not advance while the staged suffix persisted");
        assert_eq!(
            chain.batches_posted().await.unwrap(),
            0,
            "the precondition requires every staged L2 block to be absent from L1"
        );
    }

    let restarted =
        NodeHandle::start_with_datadir("ahead-of-l1-restart", datadir.path(), &cfg, &env)
            .await
            .unwrap();
    let dropped = wait_for(Duration::from_mins(1), || async {
        Ok(restarted
            .signals_since(0)?
            .into_iter()
            .find(|s| s.name == signals::COMPOSER_UNSETTLED_TAIL_DROPPED))
    })
    .await
    .expect("the restarted composer never dropped its unsettled tail");
    assert_eq!(
        dropped.u64("from").unwrap(),
        1,
        "the drop must start right above the L1-confirmed cursor, block 0"
    );
    assert!(
        dropped.u64("to").unwrap() >= 4,
        "the drop missed part of the persisted staged suffix: {:?}",
        dropped.fields
    );
    assert!(
        dropped.fields["tx_hashes"]
            .as_str()
            .is_some_and(|hashes| hashes.contains(&format!("{staged_tx:#x}"))),
        "the dropped staged transaction is not reported: {:?}",
        dropped.fields
    );
    // The signal saw the transfer on the restarted chain; reth losing its receipt shows the unwind.
    let l2 = ProviderBuilder::new().connect_http(restarted.l2_rpc_url().parse().unwrap());
    wait_for(Duration::from_mins(1), || async {
        Ok(l2
            .get_transaction_receipt(staged_tx)
            .await?
            .is_none()
            .then_some(()))
    })
    .await
    .expect("reth still serves the dropped staged transfer");
    // Catchup ignores the speculative cap, so the restarted chain's height says nothing;
    // the order does: the tail goes before the first Sync slot.
    let order: Vec<_> = restarted
        .signals_since(0)
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .filter(|name| {
            name == signals::COMPOSER_UNSETTLED_TAIL_DROPPED
                || name == signals::COMPOSER_SYNC_SLOT_INVOKED
        })
        .collect();
    assert_eq!(
        order.first().map(String::as_str),
        Some(signals::COMPOSER_UNSETTLED_TAIL_DROPPED),
        "the restarted composer produced before dropping its unsettled tail",
    );
    assert_eq!(
        chain.batches_posted().await.unwrap(),
        0,
        "the unfunded poster must not accidentally make the staged suffix L1-canonical"
    );
    restarted.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settled_safe_prefix_survives_restart_and_fresh_follower_replay() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let datadir = tempfile::tempdir().unwrap();
    let cfg = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        ..Default::default()
    };
    let env = harness.env().await.unwrap();
    let states_before = chain.executed_states().await.unwrap();

    let settled_before_restart;
    {
        let node =
            NodeHandle::start_with_datadir("durability-before-restart", datadir.path(), &cfg, &env)
                .await
                .unwrap();
        send_l2_value_transfer_confirmed(
            &node.l2_rpc_url(),
            ANVIL_KEY_1,
            ANVIL_ADDR_3,
            U256::from(17u64),
            TIMEOUT,
        )
        .await
        .expect("pre-restart transaction did not land");
        settled_before_restart =
            wait_for_new_attested_safe_block(&node, &chain, &states_before, TIMEOUT)
                .await
                .expect("pre-restart transaction never became safe");
        wait_for_safe_chain_contains(
            &node,
            settled_before_restart.0,
            settled_before_restart.1,
            TIMEOUT,
        )
        .await
        .expect("composer did not retain its newly settled safe block");
    }

    let restarted =
        NodeHandle::start_with_datadir("durability-after-restart", datadir.path(), &cfg, &env)
            .await
            .unwrap();
    wait_for_safe_chain_contains(
        &restarted,
        settled_before_restart.0,
        settled_before_restart.1,
        TIMEOUT,
    )
    .await
    .expect("restarted composer did not restore the exact pre-crash safe prefix");
    restarted.assert_no_process_death();

    let states_before_fresh_tx = chain.executed_states().await.unwrap();
    send_l2_value_transfer_confirmed(
        &restarted.l2_rpc_url(),
        ANVIL_KEY_1,
        ANVIL_ADDR_3,
        U256::from(19u64),
        TIMEOUT,
    )
    .await
    .expect("post-restart transaction did not land");
    let settled_after_restart =
        wait_for_new_attested_safe_block(&restarted, &chain, &states_before_fresh_tx, TIMEOUT)
            .await
            .expect("post-restart transaction never became safe");

    let follower_env = harness.follower_env(None).await.unwrap();
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(harness.l2_genesis_path()),
        ..Default::default()
    };
    let follower = NodeHandle::start("durability-fresh-follower", &follower_cfg, &follower_env)
        .await
        .unwrap();
    for (height, hash) in [settled_before_restart, settled_after_restart] {
        wait_for_safe_chain_contains(&follower, height, hash, TIMEOUT)
            .await
            .unwrap_or_else(|err| {
                panic!("fresh follower did not replay exact safe block {height} {hash}: {err:#}")
            });
    }

    restarted.assert_no_divergence_failure_logs();
    restarted.assert_no_process_death();
    follower.assert_no_divergence_failure_logs();
    follower.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l1_only_restart_keeps_finalized() {
    // L1 finalizes 16 blocks behind its head, so after the restart its finality
    // stays below the boot checkpoint for about a minute.
    let harness = Harness::fresh_with_slots_in_an_epoch(8).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let cfg = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        extra_args: vec!["--engine.persistence-threshold".into(), "0".into()],
        ..Default::default()
    };
    // As in production, the boot checkpoint lives in the datadir; the testkit
    // default disables it.
    let mut env = harness.env().await.unwrap();
    env.push((
        "EEZ_L2_DATADIR",
        datadir.path().to_string_lossy().into_owned(),
    ));

    let (finalized, safe) = {
        let node =
            NodeHandle::start_with_datadir("finalized-before-restart", datadir.path(), &cfg, &env)
                .await
                .unwrap();
        let rpc = node.l2_rpc_url();
        let finalized = wait_for(TIMEOUT, || async {
            Ok(tag_number(&rpc, BlockNumberOrTag::Finalized)
                .await?
                .filter(|number| *number > 0))
        })
        .await
        .expect("composer never finalized an L1-settled batch");
        // reth writes the finalized number to disk only with the next block it saves.
        let head = wait_for_latest_height(&node, 0, TIMEOUT).await.unwrap();
        wait_for_latest_height(&node, head.number + 1, TIMEOUT)
            .await
            .expect("composer never saved a block after finalizing");
        let safe = tag_number(&rpc, BlockNumberOrTag::Safe)
            .await
            .unwrap()
            .unwrap();
        node.assert_no_process_death();
        (finalized, safe)
    };

    let restarted =
        NodeHandle::start_with_datadir("finalized-after-restart", datadir.path(), &cfg, &env)
            .await
            .unwrap();
    let rpc = restarted.l2_rpc_url();
    // A safe block past the old one comes from a Deriver FCU after the boot.
    wait_for(TIMEOUT, || async {
        Ok(tag_number(&rpc, BlockNumberOrTag::Safe)
            .await?
            .filter(|number| *number > safe))
    })
    .await
    .expect("restarted composer never settled a new batch");
    let after = tag_number(&rpc, BlockNumberOrTag::Finalized)
        .await
        .unwrap()
        .unwrap_or(0);
    assert!(
        after >= finalized,
        "finalized fell from #{finalized} to #{after} after the restart"
    );
    restarted.assert_no_process_death();
}

async fn tag_number(rpc: &str, tag: BlockNumberOrTag) -> anyhow::Result<Option<u64>> {
    Ok(block_number_and_hash_at(rpc, tag)
        .await?
        .map(|(number, _)| number))
}
