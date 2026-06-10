//! End-to-end: anvil L1 + deployed protocol + spawned eez-node.
//!
//! Each test cites the rollup system it's modeled after (see
//! eez-rollup0-testing-sota.md). All exercise the full Composer →
//! Submitter → EEZ.sol path.

use std::time::Duration;

use alloy_primitives::{B256, U256};
use alloy_sol_types::SolEvent;

mod common;
use common::{
    ANVIL_ADDR, ANVIL_KEY, Anvil, Deployment, IEEZ, NodeHandle, count_events, deploy_contracts,
    free_port, latest_l2_execution_state, state_root, wait_for, wait_for_l1_blocks,
};

/// State root must move off `B256::ZERO` after one composer cycle, AND
/// the `BatchPosted` / `L2ExecutionPerformed` / `ImmediateEntrySkipped`
/// event counts must agree (at least 1 of each successful kind, 0 skipped).
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

    let advanced = wait_for(Duration::from_secs(60), || async {
        let root = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id).await?;
        Ok((root != B256::ZERO).then_some(root))
    })
    .await
    .expect(
        "state root did not advance — composer likely posting batches whose deltas don't apply",
    );

    let batches = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::BatchPosted::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();
    let executions = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::L2ExecutionPerformed::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();
    let skipped = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::ImmediateEntrySkipped::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();
    assert!(batches >= 1, "expected >=1 BatchPosted, got {batches}");
    assert_eq!(
        executions, batches,
        "BatchPosted/L2ExecutionPerformed mismatch"
    );
    assert_eq!(
        skipped, 0,
        "{skipped} ImmediateEntrySkipped events — prestate or rolling-hash mismatch"
    );
    assert_ne!(advanced, B256::ZERO);
}

/// N batches posted on L1 emit N `BatchPosted` + N `L2ExecutionPerformed`
/// + 0 `ImmediateEntrySkipped`. The last `L2ExecutionPerformed.newState`
/// must equal the on-chain `rollups[rid].stateRoot`. `AggLayer`
/// `send_multiple_certificates` pattern, tightened with event-content
/// validation (theirs only asserts `events.len() == 5`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn n_batches_emit_n_events() {
    const N: usize = 3;
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &node_env(&anvil.rpc_url, &dep)).unwrap();

    let batches = wait_for(Duration::from_secs(60), || async {
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
    let skipped = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::ImmediateEntrySkipped::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();

    assert_eq!(
        batches, executions,
        "{batches} BatchPosted but {executions} L2ExecutionPerformed — state delta not applying"
    );
    assert_eq!(
        skipped, 0,
        "{skipped} ImmediateEntrySkipped — entries reverted via the try/catch"
    );

    let on_chain = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id)
        .await
        .unwrap();
    let event_state = latest_l2_execution_state(
        &anvil.rpc_url,
        dep.eez_address,
        dep.rollup_id,
        dep.deploy_block,
    )
    .await
    .unwrap()
    .expect("L2ExecutionPerformed event missing despite count > 0");
    assert_eq!(
        on_chain, event_state,
        "latest event's newState != on-chain stateRoot"
    );
    assert_ne!(on_chain, B256::ZERO);
}

/// Node started with `EEZ_ROLLUP_ID=999` against a registry where only
/// rollup 1 exists: composer's batches target an unregistered rollup,
/// `postAndVerifyBatch` reverts before emitting either `BatchPosted` or
/// `L2ExecutionPerformed`. Both counts must be exactly zero, AND
/// `rollups[1].stateRoot` must stay at `B256::ZERO` (no collateral
/// effects on the real rollup).
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

    // Give the composer ≥5 attempt windows (1s composer interval × 5 blocks
    // at anvil's 1s block time = 5 real seconds of composer activity).
    wait_for_l1_blocks(
        &anvil.rpc_url,
        dep.deploy_block + 5,
        Duration::from_secs(30),
    )
    .await
    .unwrap();

    let posted = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::BatchPosted::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();
    let executions = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::L2ExecutionPerformed::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();
    let root_of_real_rollup = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id)
        .await
        .unwrap();

    assert_eq!(
        posted, 0,
        "expected 0 BatchPosted for unregistered rollup 999, got {posted}"
    );
    assert_eq!(
        executions, 0,
        "expected 0 L2ExecutionPerformed, got {executions}"
    );
    assert_eq!(
        root_of_real_rollup,
        B256::ZERO,
        "rollup 1's state shouldn't move when 999 is misconfigured"
    );
}

/// Three windows: (a) baseline batches land, (b) poster's gas is zeroed
/// via `anvil_setBalance` — composer's `eth_call` simulation reverts with
/// `insufficient funds`, no new batches; (c) balance restored, batches
/// resume. `AggLayer`'s L1-outage failure-injection pattern adapted to
/// the subprocess model — production-realistic (mirrors a depleted
/// poster account) without process signals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l1_outage_recovers() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let _node = NodeHandle::spawn(datadir.path(), &node_env(&anvil.rpc_url, &dep)).unwrap();

    let n_before = wait_for(Duration::from_secs(60), || async {
        let n = count_events(
            &anvil.rpc_url,
            dep.eez_address,
            IEEZ::BatchPosted::SIGNATURE_HASH,
            dep.deploy_block,
        )
        .await?;
        Ok((n >= 2).then_some(n))
    })
    .await
    .expect("baseline: composer didn't post 2 batches before outage");

    let outage_started_at = {
        let provider =
            alloy_provider::ProviderBuilder::new().connect_http(anvil.rpc_url.parse().unwrap());
        anvil.set_balance(ANVIL_ADDR, U256::ZERO).await.unwrap();
        alloy_provider::Provider::get_block_number(&provider)
            .await
            .unwrap()
    };
    // Wait for 5 L1 blocks — ≥5 composer attempts at 1s interval.
    wait_for_l1_blocks(
        &anvil.rpc_url,
        outage_started_at + 5,
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let n_during = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::BatchPosted::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();
    assert_eq!(
        n_during, n_before,
        "expected no new batches during outage (n_before={n_before}, n_during={n_during})"
    );

    let one_thousand_eth = U256::from(10u64).pow(U256::from(21u64));
    anvil
        .set_balance(ANVIL_ADDR, one_thousand_eth)
        .await
        .unwrap();
    let n_after = wait_for(Duration::from_secs(60), || async {
        let n = count_events(
            &anvil.rpc_url,
            dep.eez_address,
            IEEZ::BatchPosted::SIGNATURE_HASH,
            dep.deploy_block,
        )
        .await?;
        Ok((n > n_before).then_some(n))
    })
    .await
    .expect(
        "did not recover after balance restored — composer likely poisoned by transient errors",
    );
    assert!(
        n_after > n_before,
        "n_after={n_after} should exceed n_before={n_before}"
    );
}

/// Kill the node after at least one batch lands; restart against the
/// same datadir; observe forward progress with no replay. Reads
/// `BatchPosted` event count before+after restart and asserts the
/// `L2ExecutionPerformed` count keeps lockstep — catches "composer
/// re-posted batch [1,N] after restart" replay bugs (extra Execution
/// events without matching state advance). Mirrors OP Stack's
/// `op-e2e/system/...RunSystem` lifecycle tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_catch_up() {
    let anvil = Anvil::spawn(free_port()).await.unwrap();
    let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let env = node_env(&anvil.rpc_url, &dep);

    let (root_pre, n_pre) = {
        let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();
        wait_for(Duration::from_secs(60), || async {
            let root = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id).await?;
            if root == B256::ZERO {
                return Ok(None);
            }
            let n = count_events(
                &anvil.rpc_url,
                dep.eez_address,
                IEEZ::BatchPosted::SIGNATURE_HASH,
                dep.deploy_block,
            )
            .await?;
            Ok(Some((root, n)))
        })
        .await
        .expect("first advance pre-restart")
    };

    // Wait 2 L1 blocks for any in-flight tx from the killed node to settle.
    let killed_at = {
        let provider =
            alloy_provider::ProviderBuilder::new().connect_http(anvil.rpc_url.parse().unwrap());
        alloy_provider::Provider::get_block_number(&provider)
            .await
            .unwrap()
    };
    wait_for_l1_blocks(&anvil.rpc_url, killed_at + 2, Duration::from_secs(15))
        .await
        .unwrap();

    let _node = NodeHandle::spawn(datadir.path(), &env).unwrap();
    let (root_post, n_post) = wait_for(Duration::from_secs(60), || async {
        let root = state_root(&anvil.rpc_url, dep.eez_address, dep.rollup_id).await?;
        if root == root_pre {
            return Ok(None);
        }
        let n = count_events(
            &anvil.rpc_url,
            dep.eez_address,
            IEEZ::BatchPosted::SIGNATURE_HASH,
            dep.deploy_block,
        )
        .await?;
        Ok(Some((root, n)))
    })
    .await
    .expect("did not advance after restart — composer likely failed to re-seed posted_through");

    assert_ne!(root_post, root_pre);
    assert!(
        n_post > n_pre,
        "BatchPosted count must grow (n_pre={n_pre}, n_post={n_post})"
    );

    let executions = count_events(
        &anvil.rpc_url,
        dep.eez_address,
        IEEZ::L2ExecutionPerformed::SIGNATURE_HASH,
        dep.deploy_block,
    )
    .await
    .unwrap();
    assert_eq!(
        executions, n_post,
        "L2ExecutionPerformed count must lockstep with BatchPosted — replay bug otherwise"
    );
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
