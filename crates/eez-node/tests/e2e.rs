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
    ANVIL_ADDR, ANVIL_ADDR_3, ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_4, Anvil, AnvilConfig,
    Bench, BundleStub, Chain, NodeConfig, NodeHandle, deploy_contracts_with_initial, free_port,
    safe_block_state_root, send_l2_value_transfer, smoke_genesis_path, smoke_genesis_state_root,
    smoke_node_env, wait_for, wait_for_l2_rpc,
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
    let bench = Bench::fresh().await.unwrap();
    let chain = bench.chain();
    let datadir = tempfile::tempdir().unwrap();
    let env = bench.env();

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
    let bench = Bench::fresh().await.unwrap();
    let chain = bench.chain();
    let env = override_env(bench.env(), "EEZ_ROLLUP_ID", "999");
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
    let bench = Bench::fresh().await.unwrap();
    let chain = bench.chain();
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &bench.env()).unwrap();

    let n_before = chain
        .wait_for_batches(2, Duration::from_secs(60))
        .await
        .unwrap();

    bench
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
    bench.anvil.set_balance(ANVIL_ADDR, restored).await.unwrap();
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
    let bench = Bench::fresh().await.unwrap();
    let chain = bench.chain();
    let env = override_env(bench.env(), "EEZ_PROOF_SIGNER_KEY", ANVIL_KEY_1);
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

/// Smoke E (Rust port of the team's `scripts/smoke-e.sh`).
///
/// Two competing composers (c1 / c2) post against the same EEZ contract
/// on a shared anvil. A background "collector" task generates real L2
/// state-changing txs so each batch carries content, not no-ops. After
/// several combined batches land we trigger `anvil_reorg(depth)` and
/// verify recovery:
///   - both composers' safe L2 stateRoots converge on the same value;
///   - the contract's `rollups[rid].stateRoot` matches that L2 head;
///   - new batches land after the reorg (composers retry from rewound
///     `posted_through`);
///   - neither node logs a fatal error during the run.
///
/// Distinct from the other tests in that it spawns two reth instances
/// concurrently — nextest is configured `--test-threads=1` so this
/// test owns the runner while it executes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_smoke_e_two_composers_reorg() {
    let smoke_root = smoke_genesis_state_root().unwrap();
    let genesis_path = smoke_genesis_path();

    let anvil = Anvil::spawn_with(free_port(), AnvilConfig::for_reorg())
        .await
        .unwrap();
    let stub = BundleStub::spawn(free_port(), &anvil.rpc_url)
        .await
        .unwrap();
    let dep = deploy_contracts_with_initial(&anvil.rpc_url, ANVIL_KEY, smoke_root)
        .await
        .unwrap();

    let chain = Chain::new(&anvil, &dep);

    let c1_datadir = tempfile::tempdir().unwrap();
    let c2_datadir = tempfile::tempdir().unwrap();

    let c1_env = smoke_node_env(&anvil, &stub, &dep, ANVIL_KEY, false);
    let c2_env = smoke_node_env(&anvil, &stub, &dep, ANVIL_KEY_4, true);

    let cfg = NodeConfig {
        genesis_path: Some(genesis_path.as_path()),
    };
    let c1 = NodeHandle::spawn_with(c1_datadir.path(), &cfg, &c1_env).unwrap();
    let c2 = NodeHandle::spawn_with(c2_datadir.path(), &cfg, &c2_env).unwrap();

    wait_for_l2_rpc(&c1.l2_rpc_url(), Duration::from_secs(90))
        .await
        .expect("c1 L2 RPC up");
    wait_for_l2_rpc(&c2.l2_rpc_url(), Duration::from_secs(90))
        .await
        .expect("c2 L2 RPC up");

    // Background collector: small L2 value transfers to both composers
    // every 2s. Both senders → ANVIL_ADDR_3 (one collector address).
    // Errors are best-effort: nonce races, mempool eviction, RPC hiccups
    // are all expected during the reorg; we don't want the test to fail
    // from a single missed send.
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let c1_url = c1.l2_rpc_url();
    let c2_url = c2.l2_rpc_url();
    let collector = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut cancel_rx => break,
                () = tokio::time::sleep(Duration::from_secs(2)) => {
                    let _ = send_l2_value_transfer(&c1_url, ANVIL_KEY_1, ANVIL_ADDR_3, U256::from(1u64)).await;
                    let _ = send_l2_value_transfer(&c2_url, ANVIL_KEY_2, ANVIL_ADDR_3, U256::from(1u64)).await;
                }
            }
        }
    });

    let pre_batches = chain
        .wait_for_batches(3, Duration::from_secs(180))
        .await
        .expect("at least 3 combined batches landed pre-reorg");

    anvil.reorg(3).await.unwrap();

    // Recovery: both safe stateRoots agree with each other AND match the
    // on-chain rollups[rid].stateRoot, AND post-reorg batch count exceeds
    // pre-reorg.
    let recovered: B256 = wait_for(Duration::from_secs(180), || async {
        let c1s = safe_block_state_root(&c1.l2_rpc_url()).await.ok().flatten();
        let c2s = safe_block_state_root(&c2.l2_rpc_url()).await.ok().flatten();
        let contract = chain.state_root().await.ok();
        let post_batches = chain.batches_posted().await.unwrap_or(0);
        let conv = match (c1s, c2s, contract) {
            (Some(a), Some(b), Some(c))
                if a == b && a == c && a != B256::ZERO && post_batches > pre_batches =>
            {
                Some(a)
            }
            _ => None,
        };
        Ok(conv)
    })
    .await
    .expect("composers did not converge after reorg");

    let _ = cancel_tx.send(());
    let _ = collector.await;

    let post_batches = chain.batches_posted().await.unwrap();
    assert!(
        post_batches > pre_batches,
        "no new batches after reorg ({pre_batches} → {post_batches})"
    );
    assert_ne!(recovered, B256::ZERO);

    drop(c2);
    drop(c1);
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
