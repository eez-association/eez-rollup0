//! Restart/durability regressions whose oracle is the L1-confirmed L2 cursor.
//!
//! These deliberately use a persistent datadir and the real node process. A
//! unit test of checkpoint parsing cannot detect an execution database whose
//! canonical head is ahead of the history reconstructible from L1.

use std::time::Duration;

use alloy_rpc_types_eth::BlockNumberOrTag;
use eez_testkit::{
    Harness, L2_SYSTEM_KEY, NodeConfig, NodeHandle, block_number_and_hash_at,
    wait_for_latest_height,
};

const TIMEOUT: Duration = Duration::from_mins(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "known restart defect: empty L1 derivation currently leaves an unconfirmed local suffix canonical"]
async fn restart_with_datadir_ahead_of_l1_retreats_before_production() {
    // G-10 / P-03 / P-05: stage a crash boundary after L2 commit but before L1.
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let datadir = tempfile::tempdir().unwrap();
    let cfg = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        ..Default::default()
    };
    let stage_env = harness.env_for(L2_SYSTEM_KEY, true).await.unwrap();

    {
        let node =
            NodeHandle::start_with_datadir("ahead-of-l1-stage", datadir.path(), &cfg, &stage_env)
                .await
                .unwrap();
        wait_for_latest_height(&node, 4, TIMEOUT)
            .await
            .expect("failed to stage a multi-block unconfirmed suffix");
        assert_eq!(
            chain.batches_posted().await.unwrap(),
            0,
            "the precondition requires every staged L2 block to be absent from L1"
        );
    }

    // Append because this key is absent from the standard environment.
    let mut restart_env = stage_env;
    restart_env.push(("EEZ_MAX_SPECULATIVE_DEPTH", "1".to_string()));
    let restarted =
        NodeHandle::start_with_datadir("ahead-of-l1-restart", datadir.path(), &cfg, &restart_env)
            .await
            .unwrap();

    // Give startup reconciliation and one production opportunity time to run.
    chain
        .wait_for_l1_blocks(2, Duration::from_secs(20))
        .await
        .expect("L1 did not advance while observing restart reconciliation");
    let latest = block_number_and_hash_at(&restarted.l2_rpc_url(), BlockNumberOrTag::Latest)
        .await
        .unwrap()
        .expect("restarted node has a latest head");
    assert!(
        latest.number <= 1,
        "unconfirmed pre-crash suffix survived boot: latest L2 is {}, L1-confirmed cursor is 0 and speculative limit is 1",
        latest.number
    );
    assert_eq!(
        chain.batches_posted().await.unwrap(),
        0,
        "the unfunded poster must not accidentally make the staged suffix L1-canonical"
    );
    restarted.assert_no_process_death();
}
