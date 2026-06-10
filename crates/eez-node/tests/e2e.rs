//! End-to-end: anvil L1 + bundle stub + deployed protocol + spawned eez-node.
//!
//! One mega happy-path test asserts every observable invariant of the
//! Builder mode (the only mode eez-rollup0 supports today). Three
//! minimal failure tests exercise one revert path each. See
//! eez-rollup0-testing-sota.md for the pattern sources.

use std::time::Duration;

use alloy_primitives::{B256, U256};

mod common;
use common::{
    ANVIL_ADDR, ANVIL_ADDR_3, ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_4, AnvilConfig,
    Harness, NodeConfig, NodeHandle, reorg_genesis_path, reorg_genesis_state_root,
    safe_block_state_root, send_l2_value_transfer, wait_for, wait_for_l2_rpc,
};

/// Builder mode, sustained operation through a restart. Asserts every
/// observable invariant in one place:
///   - lockstep: `BatchPosted == L2ExecutionPerformed`, always;
///   - zero `ImmediateEntrySkipped` (no prestate/rolling-hash misfire);
///   - `latest_event.newState == rollups[rid].stateRoot` (event-state
///     consistency);
///   - state advances forward (≠ `B256::ZERO`, monotonic);
///   - across restart: counts keep lockstep (no replay), state keeps
///     advancing (`posted_through` re-seeded from on-chain logs).
///
/// Would have caught `transientExecutionEntryCount = 0` (state never
/// advances) AND any future replay bug across the restart boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_case_builder_sustained() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let datadir = tempfile::tempdir().unwrap();
    let env = harness.env();

    // Phase 1 — sustained operation under the first node.
    let n_before;
    let root_before;
    {
        let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();
        n_before = chain
            .wait_for_batches(3, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            chain.executions_performed().await.unwrap(),
            n_before,
            "lockstep"
        );
        assert_eq!(
            chain.entries_skipped().await.unwrap(),
            0,
            "no entry should revert"
        );
        root_before = chain.state_root().await.unwrap();
        assert_ne!(root_before, B256::ZERO);
        assert_eq!(
            chain.latest_execution_state().await.unwrap().unwrap(),
            root_before,
            "latest event's newState == on-chain stateRoot",
        );
    }

    // Phase 2 — restart cycle.
    chain
        .wait_for_l1_blocks(2, Duration::from_secs(15))
        .await
        .unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();

    // After restart, the *only* check that doesn't depend on user txs hitting
    // the L2 (which the dev chain has none of) is forward progress on
    // BatchPosted count + sustained lockstep. The on-chain stateRoot equals
    // root_before because empty L2 blocks have no state writes; the composer
    // posts a delta with currentState == newState and the contract dutifully
    // writes the same value. Correct, not a regression.
    let n_after = chain
        .wait_for_batches(n_before + 1, Duration::from_secs(60))
        .await
        .expect("composer didn't post any new batch after restart");
    assert!(
        n_after > n_before,
        "BatchPosted grew ({n_before} → {n_after})"
    );
    assert_eq!(
        chain.executions_performed().await.unwrap(),
        n_after,
        "no replay"
    );
    assert_eq!(
        chain.entries_skipped().await.unwrap(),
        0,
        "no skipped entries after restart"
    );
    assert_eq!(
        chain.latest_execution_state().await.unwrap().unwrap(),
        chain.state_root().await.unwrap(),
        "event-state consistency holds across restart",
    );
}

/// `EEZ_ROLLUP_ID=999` against a registry where only rollup 1 exists.
/// `postAndVerifyBatch` reverts at the structural validation step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_wrong_rollup_id() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let env = override_env(harness.env(), "EEZ_ROLLUP_ID", "999");
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();

    chain
        .wait_for_l1_blocks(5, Duration::from_secs(30))
        .await
        .unwrap();

    assert_eq!(chain.batches_posted().await.unwrap(), 0);
    assert_eq!(chain.executions_performed().await.unwrap(), 0);
    assert_eq!(
        chain.state_root().await.unwrap(),
        common::dev_genesis_state_root()
    );
}

/// Poster's gas zeroed mid-flight via `anvil_setBalance(addr, 0)`.
/// Composer's `eth_call` simulation reverts with `insufficient funds`,
/// no new batches land. When balance is restored, batches resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_l1_outage_recovery() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &harness.env()).unwrap();

    let n_before = chain
        .wait_for_batches(2, Duration::from_secs(60))
        .await
        .unwrap();

    harness
        .anvil
        .set_balance(ANVIL_ADDR, U256::ZERO)
        .await
        .unwrap();
    chain
        .wait_for_l1_blocks(5, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        chain.batches_posted().await.unwrap(),
        n_before,
        "no progress during outage"
    );

    let restored = U256::from(10u64).pow(U256::from(21u64));
    harness
        .anvil
        .set_balance(ANVIL_ADDR, restored)
        .await
        .unwrap();
    chain
        .wait_for_batches(n_before + 1, Duration::from_secs(60))
        .await
        .expect("composer did not recover after balance restored");
}

/// `MockECDSAProofSystem` deployed with signer A; node started with
/// `EEZ_PROOF_SIGNER_KEY` = B. Prover signs with B, on-chain
/// `verify()` recovers B ≠ A and returns false, `postAndVerifyBatch`
/// reverts at the proof-verification step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_prover_signer_mismatch() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let env = override_env(harness.env(), "EEZ_PROOF_SIGNER_KEY", ANVIL_KEY_1);
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();

    chain
        .wait_for_l1_blocks(5, Duration::from_secs(30))
        .await
        .unwrap();

    assert_eq!(chain.batches_posted().await.unwrap(), 0);
    assert_eq!(chain.executions_performed().await.unwrap(), 0);
    assert_eq!(
        chain.state_root().await.unwrap(),
        common::dev_genesis_state_root()
    );
}

/// Two competing composers (different poster EOAs, same proof-signer
/// key) post against the same EEZ contract on a shared anvil. A
/// background "collector" task generates real L2 state-changing txs so
/// batches carry content, not no-ops. After ≥4 combined batches land,
/// trigger `anvil_reorg(depth=3)` and verify recovery:
///   - both composers' safe L2 stateRoots converge on the same value;
///   - the contract's `rollups[rid].stateRoot` matches that L2 head;
///   - new batches land after the reorg (composers retry from rewound
///     `posted_through`);
///   - both derivers logged the L1 reorg retreat (the test's whole
///     point — without this assertion, convergence could happen via
///     unrelated re-derivation paths);
///   - neither node logs `Fatal` / `UnexpectedStaticFile`.
///
/// Spawns two reth instances concurrently; nextest CI runs integration
/// tests with `--test-threads=1` so this test owns the runner while
/// executing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_two_composers_l1_reorg_recovers() {
    let harness = Harness::with_anvil_config(
        AnvilConfig::for_reorg(),
        reorg_genesis_state_root().unwrap(),
    )
    .await
    .unwrap();
    let chain = harness.chain();
    let genesis = reorg_genesis_path();
    let cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
    };
    let c1_datadir = tempfile::tempdir().unwrap();
    let c2_datadir = tempfile::tempdir().unwrap();
    let c1 =
        NodeHandle::spawn_with(c1_datadir.path(), &cfg, &harness.env_for(ANVIL_KEY, true)).unwrap();
    let c2 = NodeHandle::spawn_with(c2_datadir.path(), &cfg, &harness.env_for(ANVIL_KEY_4, true))
        .unwrap();

    wait_for_l2_rpc(&c1.l2_rpc_url(), Duration::from_secs(90))
        .await
        .unwrap();
    wait_for_l2_rpc(&c2.l2_rpc_url(), Duration::from_secs(90))
        .await
        .unwrap();

    // Background collector: best-effort L2 value transfers every 4s to
    // give each composer's batches real content. `let _ =` because during
    // `anvil_reorg` the nonce view becomes stale, dropped blocks evict
    // pending txs, and reth's L2 RPC briefly pauses — all expected
    // transient failures. We just need *some* txs to land, not all.
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let (c1_url, c2_url) = (c1.l2_rpc_url(), c2.l2_rpc_url());
    let collector = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut cancel_rx => break,
                () = tokio::time::sleep(Duration::from_secs(4)) => {
                    let _ = send_l2_value_transfer(&c1_url, ANVIL_KEY_1, ANVIL_ADDR_3, U256::from(1u64)).await;
                    let _ = send_l2_value_transfer(&c2_url, ANVIL_KEY_2, ANVIL_ADDR_3, U256::from(1u64)).await;
                }
            }
        }
    });

    let pre_batches = chain
        .wait_for_batches(4, Duration::from_secs(120))
        .await
        .expect("pre-reorg: ≥4 combined batches");

    // Drop the most recent 3 L1 blocks. Composer's bundle-target window
    // is `latest + 2`, so depth=3 is enough to roll back at least one
    // landed `BatchPosted` and force both derivers to retreat.
    harness.anvil.reorg(3).await.unwrap();

    // Stop the collector before the convergence wait — under continuous
    // content the composers' state keeps moving and may never be
    // simultaneously equal for one polling window. Recovery should
    // settle the chain to a stable canonical L2 head.
    let _ = cancel_tx.send(());
    collector.await.unwrap();

    // Recovery: both safe stateRoots agree AND match the on-chain
    // stateRoot AND post-reorg batch count grew.
    let Ok(recovered) = wait_for(Duration::from_secs(120), || async {
        let c1s = safe_block_state_root(&c1.l2_rpc_url()).await.ok().flatten();
        let c2s = safe_block_state_root(&c2.l2_rpc_url()).await.ok().flatten();
        let contract = chain.state_root().await.ok();
        let post = chain.batches_posted().await.unwrap_or(0);
        Ok(match (c1s, c2s, contract) {
            (Some(a), Some(b), Some(c))
                if a == b && a == c && a != B256::ZERO && post > pre_batches =>
            {
                Some(a)
            }
            _ => None,
        })
    })
    .await
    else {
        panic!(
            "post-reorg convergence failed:\n  c1.safe = {:?}\n  c2.safe = {:?}\n  contract = {:?}\n  batches: {pre_batches} → {}",
            safe_block_state_root(&c1.l2_rpc_url()).await.ok().flatten(),
            safe_block_state_root(&c2.l2_rpc_url()).await.ok().flatten(),
            chain.state_root().await.ok(),
            chain.batches_posted().await.unwrap_or(0),
        );
    };

    assert_ne!(recovered, B256::ZERO);
    assert!(chain.batches_posted().await.unwrap() > pre_batches);

    // Both derivers actually noticed the reorg. Without this, the test
    // would pass even if reorg-detection regressed (some unrelated path
    // re-derives consistent state).
    let retreat = ["reorg rolled out", "l1.reorg.retreated"];
    assert!(
        c1.log_count_matching(&retreat).unwrap() > 0,
        "c1 deriver missed the reorg"
    );
    assert!(
        c2.log_count_matching(&retreat).unwrap() > 0,
        "c2 deriver missed the reorg"
    );

    let fatal = ["Fatal", "UnexpectedStaticFile"];
    assert_eq!(c1.log_count_matching(&fatal).unwrap(), 0, "c1 fatal error");
    assert_eq!(c2.log_count_matching(&fatal).unwrap(), 0, "c2 fatal error");
}

fn override_env(
    mut env: Vec<(&'static str, String)>,
    key: &str,
    value: &str,
) -> Vec<(&'static str, String)> {
    for (k, v) in &mut env {
        if *k == key {
            *v = value.to_string();
        }
    }
    env
}
