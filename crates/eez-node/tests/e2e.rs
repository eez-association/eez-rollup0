//! End-to-end: anvil L1 + deployed protocol + spawned eez-node.
//!
//! One mega happy-path test asserts every observable invariant of the
//! Builder mode (the only mode eez-rollup0 supports today). Three
//! minimal failure tests exercise one revert path each. See
//! eez-rollup0-testing-sota.md for the pattern sources.

use std::time::Duration;

use alloy_primitives::{B256, U256};

mod common;
use common::{
    ANVIL_ADDR, ANVIL_KEY, ANVIL_KEY_1, Anvil, Chain, Deployment, NodeHandle, deploy_contracts,
    free_port,
};

/// Builder mode, sustained operation through a restart. Asserts every
/// observable invariant in one place rather than spreading them across
/// multiple thin happy-path tests:
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
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let chain = Chain::new(&anvil, &dep);
    let datadir = tempfile::tempdir().unwrap();
    let env = node_env(&anvil.rpc_url, &dep);

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

    // Phase 2 — restart cycle. Drop node, let any in-flight tx settle, respawn.
    chain
        .wait_for_l1_blocks(2, Duration::from_secs(15))
        .await
        .unwrap();

    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();
    let root_after = chain
        .wait_for_state_change(root_before, Duration::from_secs(60))
        .await
        .unwrap();

    let n_after = chain.batches_posted().await.unwrap();
    assert!(
        n_after > n_before,
        "BatchPosted grew (n_before={n_before}, n_after={n_after})"
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
        root_after,
        "event-state consistency holds across restart",
    );
}

/// `EEZ_ROLLUP_ID=999` against a registry where only rollup 1 exists.
/// `postAndVerifyBatch` reverts at the structural validation step; no
/// `BatchPosted` ever fires; rollup 1's state is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_wrong_rollup_id() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let chain = Chain::new(&anvil, &dep);
    let mut env = node_env(&anvil.rpc_url, &dep);
    for (k, v) in &mut env {
        if *k == "EEZ_ROLLUP_ID" {
            *v = "999".to_string();
        }
    }
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();

    chain
        .wait_for_l1_blocks(5, Duration::from_secs(30))
        .await
        .unwrap();

    assert_eq!(chain.batches_posted().await.unwrap(), 0);
    assert_eq!(chain.executions_performed().await.unwrap(), 0);
    assert_eq!(chain.state_root().await.unwrap(), B256::ZERO);
}

/// Poster's gas zeroed mid-flight via `anvil_setBalance(addr, 0)`.
/// Composer's `eth_call` simulation reverts with `insufficient funds`,
/// no new batches land. When balance is restored, batches resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_l1_outage_recovery() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let chain = Chain::new(&anvil, &dep);
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &node_env(&anvil.rpc_url, &dep)).unwrap();

    let n_before = chain
        .wait_for_batches(2, Duration::from_secs(60))
        .await
        .unwrap();

    anvil.set_balance(ANVIL_ADDR, U256::ZERO).await.unwrap();
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
    anvil.set_balance(ANVIL_ADDR, restored).await.unwrap();
    chain
        .wait_for_batches(n_before + 1, Duration::from_secs(60))
        .await
        .expect("composer did not recover after balance restored");
}

/// `MockECDSAProofSystem` deployed with signer A; node started with
/// `EEZ_PROOF_SIGNER_KEY` = B. Prover signs with B, on-chain
/// `verify()` recovers B ≠ A and returns false, `postAndVerifyBatch`
/// reverts at the proof-verification step. No `BatchPosted` fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_prover_signer_mismatch() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    // Deploy with default signer (= ANVIL_ADDR derived from ANVIL_KEY).
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let chain = Chain::new(&anvil, &dep);

    // Override `EEZ_PROOF_SIGNER_KEY` to the *other* anvil account.
    let mut env = node_env(&anvil.rpc_url, &dep);
    for (k, v) in &mut env {
        if *k == "EEZ_PROOF_SIGNER_KEY" {
            *v = ANVIL_KEY_1.to_string();
        }
    }
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();

    chain
        .wait_for_l1_blocks(5, Duration::from_secs(30))
        .await
        .unwrap();

    assert_eq!(chain.batches_posted().await.unwrap(), 0);
    assert_eq!(chain.executions_performed().await.unwrap(), 0);
    assert_eq!(chain.state_root().await.unwrap(), B256::ZERO);
}

fn node_env(rpc_url: &str, dep: &Deployment) -> Vec<(&'static str, String)> {
    vec![
        ("EEZ_L1_RPC_URL", rpc_url.to_string()),
        // PR #6 (eth_sendBundle) added this. Point at the same anvil — it
        // accepts standard txs even when the submitter targets a bundle endpoint.
        ("EEZ_L1_BUILDER_RPC_URL", rpc_url.to_string()),
        ("EEZ_L1_POSTER_KEY", ANVIL_KEY.to_string()),
        ("EEZ_PROOF_SIGNER_KEY", ANVIL_KEY.to_string()),
        ("EEZ_REGISTRY_ADDRESS", format!("{:#x}", dep.eez_address)),
        ("EEZ_REGISTRY_DEPLOY_BLOCK", dep.deploy_block.to_string()),
        (
            "EEZ_MOCK_PROOF_SYSTEM_ADDRESS",
            format!("{:#x}", dep.mock_ps_address),
        ),
        (
            "EEZ_ROLLUP_MANAGER_ADDRESS",
            format!("{:#x}", dep.rollup_manager_address),
        ),
        ("EEZ_ROLLUP_ID", dep.rollup_id.to_string()),
        ("EEZ_COMPOSER_INTERVAL_SECS", "1".to_string()),
        (
            "EEZ_L2_DATADIR",
            "/tmp/unused-overridden-by-flag".to_string(),
        ),
        (
            "RUST_LOG",
            std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string()),
        ),
    ]
}
