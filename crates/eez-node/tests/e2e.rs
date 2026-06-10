//! End-to-end: anvil L1 + deployed protocol + spawned eez-node.
//!
//! Each test cites the rollup system it's modeled after (see
//! eez-rollup0-testing-sota.md). All exercise the full Composer →
//! Submitter → EEZ.sol path.

use std::time::Duration;

use alloy_primitives::B256;
use alloy_sol_types::SolEvent;

mod common;
use common::{
    ANVIL_KEY, Anvil, Deployment, IEEZ, NodeHandle, count_events, deploy_contracts, free_port,
    state_root, wait_for,
};

/// State root must move off bytes32(0) after one composer cycle.
///
/// Would have caught `transientExecutionEntryCount = 0` — proofs verified
/// but the entry never ran, so `rollups[1].stateRoot` stayed at genesis
/// regardless of how many batches landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_root_advances_after_first_batch() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &node_env(&anvil.rpc_url, &dep)).unwrap();

    let advanced = wait_for(Duration::from_secs(120), || async {
        let root = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id).await?;
        Ok((root != B256::ZERO).then_some(root))
    })
    .await
    .expect(
        "state root did not advance — composer likely posting batches whose deltas don't apply",
    );

    assert_ne!(advanced, B256::ZERO);
}

/// N batches posted on L1 emit N `BatchPosted` events AND N
/// `L2ExecutionPerformed` events. `AggLayer`
/// `send_multiple_certificates` pattern: assert exact event count via
/// topic filter rather than fuzzy "state advanced".
///
/// If counts diverge — e.g., `BatchPosted` fired but
/// `L2ExecutionPerformed` did not — the state delta wasn't applied
/// (the `transientExecutionEntryCount=0` failure mode).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn n_batches_emit_n_events() {
    const N: usize = 3;
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &node_env(&anvil.rpc_url, &dep)).unwrap();

    let batches = wait_for(Duration::from_secs(180), || async {
        let n = count_events(
            &anvil.rpc_url,
            dep.eez_address,
            IEEZ::BatchPosted::SIGNATURE_HASH,
            dep.deploy_block,
        )
        .await?;
        Ok((n >= N).then_some(n))
    })
    .await
    .expect("did not observe N batches posted in time");

    let executions = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::L2ExecutionPerformed::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();

    assert_eq!(
        batches, executions,
        "{batches} BatchPosted but {executions} L2ExecutionPerformed — state delta not applying"
    );
    assert_ne!(
        state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id)
            .await
            .unwrap(),
        B256::ZERO,
    );
}

/// Node started with `EEZ_ROLLUP_ID=999` against a registry where only
/// rollup 1 exists: composer's batches target an unregistered rollup,
/// `postAndVerifyBatch` reverts before emitting `BatchPosted`, so the
/// event count stays at zero. Scroll-style negative assertion (count
/// must be exactly zero, not "stays small").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_rollup_id_no_batches_post() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let mut env = node_env(&anvil.rpc_url, &dep);
    for (k, v) in &mut env {
        if *k == "EEZ_ROLLUP_ID" {
            *v = "999".to_string();
        }
    }
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();

    tokio::time::sleep(Duration::from_secs(30)).await;

    let posted = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::BatchPosted::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();
    assert_eq!(
        posted, 0,
        "expected 0 BatchPosted events for unregistered rollup 999, got {posted}"
    );
}

/// Anvil paused with SIGSTOP for 15s mid-flight; composer's L1 calls
/// fail during the outage. After SIGCONT, composer recovers and state
/// root advances again. Mirrors `AggLayer`'s L1-outage recovery test
/// (`l1_settlement/failure.rs`), but using process signals instead of
/// failpoint injection to keep `eez-l1` production code untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l1_outage_recovers() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &node_env(&anvil.rpc_url, &dep)).unwrap();

    let first = wait_for(Duration::from_secs(120), || async {
        let root = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id).await?;
        Ok((root != B256::ZERO).then_some(root))
    })
    .await
    .expect("first advance");

    anvil.pause().unwrap();
    tokio::time::sleep(Duration::from_secs(15)).await;
    anvil.resume().unwrap();

    let second = wait_for(Duration::from_secs(120), || async {
        let root = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id).await?;
        Ok((root != first).then_some(root))
    })
    .await
    .expect("did not recover after L1 outage — composer likely poisoned by transient errors");

    assert_ne!(second, first);
}

/// Kill the node after one batch lands; restart against the same
/// datadir; observe forward progress. Validates the composer's startup
/// re-seed of `posted_through` from on-chain `BatchPosted` log scan.
/// Mirrors OP Stack's `op-e2e/system/...RunSystem` lifecycle tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_catch_up() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let env = node_env(&anvil.rpc_url, &dep);

    let first = {
        let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();
        wait_for(Duration::from_secs(120), || async {
            let root = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id).await?;
            Ok((root != B256::ZERO).then_some(root))
        })
        .await
        .expect("first advance pre-restart")
    };

    tokio::time::sleep(Duration::from_secs(5)).await;

    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();
    let second = wait_for(Duration::from_secs(120), || async {
        let root = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id).await?;
        Ok((root != first).then_some(root))
    })
    .await
    .expect("did not advance after restart — composer likely failed to re-seed posted_through");

    assert_ne!(second, first);
}

fn node_env(rpc_url: &str, dep: &Deployment) -> Vec<(&'static str, String)> {
    vec![
        ("EEZ_L1_RPC_URL", rpc_url.to_string()),
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
        ("EEZ_COMPOSER_INTERVAL_SECS", "5".to_string()),
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
