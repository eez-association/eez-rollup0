//! End-to-end composer, follower, restart, outage, and reorg scenarios.

use std::time::Duration;

use alloy_primitives::{B256, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::BlockNumberOrTag;

mod common;
use common::{
    ANVIL_ADDR, ANVIL_ADDR_3, ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_3, ANVIL_KEY_4,
    Harness, NodeBinary, NodeConfig, NodeHandle, block_number_and_hash_at, override_env,
    reorg_genesis_state_root, send_l2_value_transfer, send_l2_value_transfer_confirmed, wait_for,
    wait_for_latest_height, wait_for_new_attested_safe_block, wait_for_safe_chain_contains,
    wait_for_safe_prefix_convergence, wait_for_safe_state,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_mins(5);

/// A follower replaces the full divergent suffix of an intra-batch fork.
/// Replaying only the transaction block is insufficient because later blocks descend from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_sequencer_intra_batch_suffix_replay_converges() {
    let harness = Harness::for_reorg().await.unwrap();
    let chain = harness.chain();
    let genesis = harness.l2_genesis_path();
    let stage_cfg = NodeConfig {
        binary: NodeBinary::Dev,
        genesis_path: Some(genesis),
    };
    let composer_cfg = NodeConfig {
        binary: NodeBinary::Composer,
        genesis_path: Some(genesis),
    };
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(genesis),
    };

    let primary_dir = tempfile::tempdir().unwrap();
    let mirror_dir = tempfile::tempdir().unwrap();
    let standalone_env = harness.standalone_env();

    let seq_a = NodeHandle::start_with_datadir(
        "intra-seq-a-stage",
        primary_dir.path(),
        &stage_cfg,
        &standalone_env,
    )
    .await
    .unwrap();
    let seq_b = NodeHandle::start_with_datadir(
        "intra-seq-b-stage",
        mirror_dir.path(),
        &stage_cfg,
        &standalone_env,
    )
    .await
    .unwrap();

    let seq_a_rpc = seq_a.l2_rpc_url();
    let seq_a_provider = ProviderBuilder::new().connect_http(seq_a_rpc.parse().unwrap());
    let tx_hash = send_l2_value_transfer(&seq_a_rpc, ANVIL_KEY_1, ANVIL_ADDR, U256::from(1u64))
        .await
        .expect("submit L2 tx to sequencer A");
    let receipt = wait_for(DEFAULT_TIMEOUT, || async {
        Ok(seq_a_provider.get_transaction_receipt(tx_hash).await?)
    })
    .await
    .unwrap_or_else(|err| panic!("wait for L2 tx {tx_hash} inclusion: {err:#}"));
    assert!(receipt.status(), "L2 tx {tx_hash} reverted");
    let included_block = receipt
        .block_number
        .unwrap_or_else(|| panic!("included L2 tx {tx_hash} missing block_number"));
    assert!(
        included_block > 0,
        "L2 tx {tx_hash} must not be included in genesis"
    );
    let target = included_block + 3;
    wait_for_latest_height(&seq_a, target, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer A did not stage enough local blocks");
    wait_for_latest_height(&seq_b, target, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer B did not stage enough local blocks");

    drop(seq_a);
    drop(seq_b);
    let follower_env = override_env(
        harness.follower_env(None).await.unwrap(),
        "RUST_LOG",
        "warn,eez_deriver=info,eez_l1=info",
    );
    let seq_b = NodeHandle::start_with_datadir(
        "intra-seq-b-follow",
        mirror_dir.path(),
        &follower_cfg,
        &follower_env,
    )
    .await
    .unwrap();
    let composer_env = override_env(
        harness.env_for(ANVIL_KEY, true).await.unwrap(),
        "RUST_LOG",
        "warn,eez_composer=info,eez_deriver=info,eez_l1=info,eez_prover_client=info",
    );
    let seq_a = NodeHandle::start_with_datadir(
        "intra-seq-a-compose",
        primary_dir.path(),
        &composer_cfg,
        &composer_env,
    )
    .await
    .unwrap();

    chain
        .wait_for_batches(1, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer A did not post staged multi-block batch");
    wait_for_safe_prefix_convergence(&[&seq_a, &seq_b], target, DEFAULT_TIMEOUT)
        .await
        .expect("sequencers did not converge after intra-batch suffix replay");

    seq_a.assert_no_divergence_failure_logs();
    seq_b.assert_no_divergence_failure_logs();
}

/// Builder mode, sustained operation through a restart. Asserts every
/// observable invariant in one place:
///   - lockstep: `BatchPosted == L2ExecutionPerformed`, always;
///   - zero `L2TxSkipped` (no prestate/rolling-hash misfire);
///   - `latest_event.newState == rollups[rid].stateRoot` (event-state
///     consistency);
///   - state advances forward (≠ `B256::ZERO`, monotonic);
///   - across restart: counts keep lockstep (no replay), state keeps
///     advancing (`posted_through` re-seeded from on-chain logs).
///
/// Would have caught `immediateEntryCount = 0` (state never
/// advances) AND any future replay bug across the restart boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_case_composer_sustained() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let datadir = tempfile::tempdir().unwrap();
    let env = harness.env().await.unwrap();

    let n_before;
    let pre_restart_latest;
    {
        let node_before_restart = NodeHandle::start_with_datadir(
            "composer-before-restart",
            datadir.path(),
            &NodeConfig::default(),
            &env,
        )
        .await
        .unwrap();
        send_l2_value_transfer_confirmed(
            &node_before_restart.l2_rpc_url(),
            ANVIL_KEY_1,
            ANVIL_ADDR_3,
            U256::from(1u64),
            DEFAULT_TIMEOUT,
        )
        .await
        .expect("pre-restart L2 transfer did not land");
        wait_for_safe_state(
            &node_before_restart,
            &chain,
            common::dev_genesis_state_root(),
            DEFAULT_TIMEOUT,
        )
        .await
        .expect("pre-restart state transition was not attested");
        n_before = chain.wait_for_batches(3, DEFAULT_TIMEOUT).await.unwrap();
        pre_restart_latest = wait_for_latest_height(&node_before_restart, 1, DEFAULT_TIMEOUT)
            .await
            .expect("pre-restart node did not produce L2 blocks");
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
        let root_before = chain.state_root().await.unwrap();
        assert_ne!(root_before, common::dev_genesis_state_root());
        assert_eq!(
            chain.latest_execution_state().await.unwrap().unwrap(),
            root_before,
            "latest event's newState == on-chain stateRoot",
        );
    }

    chain
        .wait_for_l1_blocks(2, Duration::from_secs(15))
        .await
        .unwrap();
    let node = NodeHandle::start_with_datadir(
        "composer-after-restart",
        datadir.path(),
        &NodeConfig::default(),
        &env,
    )
    .await
    .unwrap();

    let n_after = chain
        .wait_for_batches(n_before + 1, DEFAULT_TIMEOUT)
        .await
        .expect("composer didn't post any new batch after restart");
    assert!(
        n_after > n_before,
        "BatchPosted grew ({n_before} → {n_after})"
    );
    let post_restart_target_height = pre_restart_latest.number + 1;
    wait_for_latest_height(&node, post_restart_target_height, DEFAULT_TIMEOUT)
        .await
        .expect("restarted node did not advance L2 height after restart");
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

    let follower_env = harness.follower_env(None).await.unwrap();
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        ..Default::default()
    };
    let follower = NodeHandle::start("follower", &follower_cfg, &follower_env)
        .await
        .unwrap();
    wait_for_safe_state(&follower, &chain, B256::ZERO, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");
    wait_for_safe_prefix_convergence(
        &[&node, &follower],
        post_restart_target_height,
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("restarted node and replay follower did not converge on safe block hashes");
    follower.assert_no_process_death();
    node.assert_no_process_death();
}

/// Proofs for an unregistered rollup ID are signed but never posted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_wrong_rollup_id() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let env = harness.env_with_rollup_id(999).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let node = NodeHandle::start_with_datadir(
        "wrong-rollup",
        datadir.path(),
        &NodeConfig::default(),
        &env,
    )
    .await
    .unwrap();

    wait_for(Duration::from_mins(1), || async {
        Ok((harness.successful_attestations()? > 0).then_some(()))
    })
    .await
    .expect("wrong-rollup scenario never reached proof attestation");

    chain
        .wait_for_l1_blocks(5, Duration::from_secs(30))
        .await
        .unwrap();

    chain
        .assert_failed_post_and_verify_batch(999, common::INVALID_PROOF_SYSTEM_CONFIG_SELECTOR)
        .await
        .expect("wrong-rollup batch did not reach L1 structural validation");

    assert_eq!(chain.batches_posted().await.unwrap(), 0);
    assert_eq!(chain.executions_performed().await.unwrap(), 0);
    assert_eq!(
        chain.state_root().await.unwrap(),
        common::dev_genesis_state_root()
    );
    node.assert_no_process_death();
}

/// Posting resumes after an unfunded poster account is restored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_poster_funds_recovery() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let datadir = tempfile::tempdir().unwrap();
    let node = NodeHandle::start_with_datadir(
        "poster-funds",
        datadir.path(),
        &NodeConfig::default(),
        &harness.env().await.unwrap(),
    )
    .await
    .unwrap();

    let n_before = chain
        .wait_for_batches(2, Duration::from_mins(1))
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
        .wait_for_batches(n_before + 1, Duration::from_mins(1))
        .await
        .expect("composer did not recover after balance restored");
    node.assert_no_process_death();
}

/// A proof accepted by the composer but signed by an attester unauthorized
/// by the deployed L1 proof system reaches L1 and reverts with InvalidProof.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_prover_signer_mismatch() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let env = harness.env_with_proof_signer(ANVIL_KEY_1).await.unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let node = NodeHandle::start_with_datadir(
        "signer-mismatch",
        datadir.path(),
        &NodeConfig::default(),
        &env,
    )
    .await
    .unwrap();

    wait_for(Duration::from_mins(1), || async {
        Ok((harness.successful_attestations()? > 0).then_some(()))
    })
    .await
    .expect("signer-mismatch scenario never reached proof attestation");

    chain
        .wait_for_l1_blocks(5, Duration::from_secs(30))
        .await
        .unwrap();

    chain
        .assert_failed_post_and_verify_batch(harness.dep.rollup_id, common::INVALID_PROOF_SELECTOR)
        .await
        .expect("unauthorized-attester batch did not reach L1 proof verification");

    assert_eq!(chain.batches_posted().await.unwrap(), 0);
    assert_eq!(chain.executions_performed().await.unwrap(), 0);
    assert_eq!(
        chain.state_root().await.unwrap(),
        common::dev_genesis_state_root()
    );
    node.assert_no_process_death();
}

/// Competing composers retreat across an L1 reorg and reconverge.
/// They must produce newly attested work before old-prefix agreement can pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_two_composers_l1_reorg_recovers() {
    let harness = Harness::for_reorg().await.unwrap();
    let chain = harness.chain();
    let genesis = harness.l2_genesis_path();
    let cfg = NodeConfig {
        genesis_path: Some(genesis),
        ..Default::default()
    };
    let env1 = harness.env_for(ANVIL_KEY, true).await.unwrap();
    let env2 = harness.env_for(ANVIL_KEY_4, true).await.unwrap();
    let (c1, c2) = tokio::try_join!(
        NodeHandle::start("c1", &cfg, &env1),
        NodeHandle::start("c2", &cfg, &env2),
    )
    .unwrap();

    c1.run_tx_spammer(ANVIL_KEY_1);
    c2.run_tx_spammer(ANVIL_KEY_2);

    let pre_batches = chain
        .wait_for_batches(4, DEFAULT_TIMEOUT)
        .await
        .expect("pre-reorg: ≥4 combined batches");
    let (c1_pre_reorg_latest, c2_pre_reorg_latest) = tokio::try_join!(
        wait_for_latest_height(&c1, 1, DEFAULT_TIMEOUT),
        wait_for_latest_height(&c2, 1, DEFAULT_TIMEOUT),
    )
    .expect("pre-reorg: both composers should have produced L2 blocks");
    let post_reorg_target_height = c1_pre_reorg_latest.number.min(c2_pre_reorg_latest.number) + 1;
    let pre_reorg_states = chain.executed_states().await.unwrap();

    // Depth three crosses the bundle target and retreats at least one posted batch.
    harness.anvil.reorg(3).await.unwrap();
    send_l2_value_transfer_confirmed(
        &c1.l2_rpc_url(),
        ANVIL_KEY_3,
        ANVIL_ADDR_3,
        U256::from(1u64),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("post-reorg L2 tx did not land on c1");

    // Require post-reorg progress before accepting convergence.
    chain
        .wait_for_batches(pre_batches + 1, DEFAULT_TIMEOUT)
        .await
        .expect("no batches landed after reorg");
    tokio::try_join!(
        wait_for_latest_height(&c1, post_reorg_target_height, DEFAULT_TIMEOUT),
        wait_for_latest_height(&c2, post_reorg_target_height, DEFAULT_TIMEOUT),
    )
    .expect("composers did not advance L2 height after reorg");

    let c1_safe = wait_for_new_attested_safe_block(&c1, &chain, &pre_reorg_states, DEFAULT_TIMEOUT);
    let c2_safe = wait_for_new_attested_safe_block(&c2, &chain, &pre_reorg_states, DEFAULT_TIMEOUT);
    tokio::pin!(c1_safe);
    tokio::pin!(c2_safe);
    let (source_name, peer_name, peer, post_reorg_safe_number, post_reorg_safe_hash) = tokio::select! {
        c1_result = &mut c1_safe => match c1_result {
            Ok((number, hash)) => ("c1", "c2", &c2, number, hash),
            Err(c1_err) => {
                let (number, hash) = c2_safe.await.unwrap_or_else(|c2_err| {
                    panic!(
                        "neither composer imported a newly attested post-reorg safe block; \
                         c1: {c1_err:#}; c2: {c2_err:#}"
                    );
                });
                ("c2", "c1", &c1, number, hash)
            }
        },
        c2_result = &mut c2_safe => match c2_result {
            Ok((number, hash)) => ("c2", "c1", &c1, number, hash),
            Err(c2_err) => {
                let (number, hash) = c1_safe.await.unwrap_or_else(|c1_err| {
                    panic!(
                        "neither composer imported a newly attested post-reorg safe block; \
                         c1: {c1_err:#}; c2: {c2_err:#}"
                    );
                });
                ("c1", "c2", &c2, number, hash)
            }
        },
    };
    wait_for_safe_chain_contains(
        peer,
        post_reorg_safe_number,
        post_reorg_safe_hash,
        DEFAULT_TIMEOUT,
    )
    .await
    .unwrap_or_else(|err| {
        panic!("{peer_name} did not import {source_name}'s post-reorg safe block: {err:#}");
    });

    wait_for_safe_prefix_convergence(&[&c1, &c2], post_reorg_target_height, DEFAULT_TIMEOUT)
        .await
        .expect("composers did not converge on post-reorg safe block hashes");

    c1.wait_for_reorg_seen(DEFAULT_TIMEOUT).await.unwrap();
    c2.wait_for_reorg_seen(DEFAULT_TIMEOUT).await.unwrap();

    c1.assert_no_process_death();
    c2.assert_no_process_death();
}

async fn spawn_follower(
    name: &str,
    harness: &Harness,
    seq_rpc: Option<&str>,
) -> anyhow::Result<NodeHandle> {
    let env = harness.follower_env(seq_rpc).await?;
    let cfg = NodeConfig {
        binary: NodeBinary::Follower,
        ..Default::default()
    };
    NodeHandle::start(name, &cfg, &env).await
}

/// An L1-only follower reconstructs an attested safe state without a sequencer RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_l1_derived() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env().await.unwrap())
        .await
        .unwrap();
    chain
        .wait_for_batches(2, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer landed batches");

    let follower = spawn_follower("follower", &harness, None).await.unwrap();
    wait_for_safe_state(&follower, &chain, B256::ZERO, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");
    wait_for_safe_prefix_convergence(&[&seq, &follower], 1, DEFAULT_TIMEOUT)
        .await
        .expect("follower safe chain did not converge with the sequencer");

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// Sequencer RPC advances unsafe state while L1 remains safe-authoritative.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_sequencer_rpc() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env().await.unwrap())
        .await
        .unwrap();

    let seq_rpc = seq.l2_rpc_url();
    let follower_env = override_env(
        harness.follower_env(Some(&seq_rpc)).await.unwrap(),
        "RUST_LOG",
        "warn,eez_node::follower=info",
    );
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        ..Default::default()
    };
    let follower = NodeHandle::start("follower", &follower_cfg, &follower_env)
        .await
        .unwrap();

    chain
        .wait_for_batches(2, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer landed batches");
    wait_for_safe_state(&follower, &chain, B256::ZERO, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");

    wait_for_safe_prefix_convergence(&[&seq, &follower], 1, DEFAULT_TIMEOUT)
        .await
        .expect("follower safe block never matched the sequencer chain");

    let unsafe_head_patterns = [
        "follower advanced unsafe head to sequencer block",
        "reth accepted sequencer head as a sync target",
    ];
    let unsafe_head_events_before = follower.log_count_matching(&unsafe_head_patterns).unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);
    common::wait_for(DEFAULT_TIMEOUT, || {
        std::future::ready(
            follower
                .log_count_matching(&unsafe_head_patterns)
                .map(|n| (n > unsafe_head_events_before).then_some(())),
        )
    })
    .await
    .expect("follower never reported a sequencer-RPC unsafe-head FCU outcome");

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// An L1-only follower retreats and imports a state newly attested after an L1 reorg.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_l1_reorg_recovers() {
    let harness = Harness::for_reorg().await.unwrap();
    let chain = harness.chain();
    let genesis = harness.l2_genesis_path();
    let seq_cfg = NodeConfig {
        genesis_path: Some(genesis),
        ..Default::default()
    };
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(genesis),
    };
    let seq_env = harness.env().await.unwrap();
    let follower_env = harness.follower_env(None).await.unwrap();
    let (seq, follower) = tokio::try_join!(
        NodeHandle::start("seq", &seq_cfg, &seq_env),
        NodeHandle::start("follower", &follower_cfg, &follower_env),
    )
    .unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);

    let pre_batches = chain
        .wait_for_batches(2, DEFAULT_TIMEOUT)
        .await
        .expect("pre-reorg batches");
    let pre_reorg_states = chain.executed_states().await.unwrap();
    harness.anvil.reorg(3).await.unwrap();
    send_l2_value_transfer_confirmed(
        &seq.l2_rpc_url(),
        ANVIL_KEY_3,
        ANVIL_ADDR_3,
        U256::from(1u64),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("post-reorg L2 tx did not land on sequencer");
    chain
        .wait_for_batches(pre_batches + 1, DEFAULT_TIMEOUT)
        .await
        .expect("no batches landed after reorg");
    let (post_reorg_safe_number, post_reorg_safe_hash) =
        wait_for_new_attested_safe_block(&seq, &chain, &pre_reorg_states, DEFAULT_TIMEOUT)
            .await
            .expect("sequencer did not import the post-reorg safe block");
    wait_for_safe_chain_contains(
        &follower,
        post_reorg_safe_number,
        post_reorg_safe_hash,
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("follower did not import the sequencer's post-reorg safe block");

    follower.wait_for_reorg_seen(DEFAULT_TIMEOUT).await.unwrap();
    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// Followers with different unsafe sources converge on the same safe prefix.
/// This proves L1 derivation, rather than the unsafe source, controls safe-head selection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_cross_safe_parity() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env().await.unwrap())
        .await
        .unwrap();
    chain
        .wait_for_batches(2, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer landed batches");

    let seq_rpc = seq.l2_rpc_url();
    let (f_l1, f_seq) = tokio::try_join!(
        spawn_follower("f_l1", &harness, None),
        spawn_follower("f_seq", &harness, Some(&seq_rpc)),
    )
    .unwrap();
    wait_for_safe_state(&f_l1, &chain, B256::ZERO, DEFAULT_TIMEOUT)
        .await
        .expect("f_l1 did not catch up");
    wait_for_safe_state(&f_seq, &chain, B256::ZERO, DEFAULT_TIMEOUT)
        .await
        .expect("f_seq did not catch up");

    wait_for_safe_prefix_convergence(&[&seq, &f_l1, &f_seq], 1, DEFAULT_TIMEOUT)
        .await
        .expect("followers never shared a sequencer safe block");

    f_l1.assert_no_process_death();
    f_seq.assert_no_process_death();
    seq.assert_no_process_death();
}

/// A rogue unsafe source cannot move the follower's L1-derived safe head.
/// A log assertion separately proves the follower actually polled the rogue source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_rogue_sequencer_safe_head_holds() {
    let harness = Harness::for_reorg().await.unwrap();
    let chain = harness.chain();
    let genesis = harness.l2_genesis_path();

    let seq_cfg = NodeConfig {
        genesis_path: Some(genesis),
        ..Default::default()
    };
    let seq = NodeHandle::start("seq", &seq_cfg, &harness.env().await.unwrap())
        .await
        .unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);

    let rogue_env = vec![(
        "RUST_LOG",
        std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string()),
    )];
    let rogue_cfg = NodeConfig {
        binary: NodeBinary::Dev,
        ..Default::default()
    };
    let rogue = NodeHandle::start("rogue", &rogue_cfg, &rogue_env)
        .await
        .unwrap();

    let follower_env = override_env(
        harness
            .follower_env(Some(&rogue.l2_rpc_url()))
            .await
            .unwrap(),
        "RUST_LOG",
        "warn,eez_node::follower=info",
    );
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(genesis),
    };
    let follower = NodeHandle::start("follower", &follower_cfg, &follower_env)
        .await
        .unwrap();

    wait_for_safe_state(
        &follower,
        &chain,
        reorg_genesis_state_root().unwrap(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("follower safe head did not reach a non-genesis attested stateRoot while on the rogue");

    // Prove the rogue unsafe source was actually polled.
    assert!(
        follower
            .log_count_matching(&["reth accepted sequencer head as a sync target"])
            .unwrap()
            > 0,
        "follower never processed a rogue unsafe head",
    );

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// A late follower backfills the complete pre-existing batch history.
/// Matching a block captured before startup rules out a partial catch-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_deep_backfill_late_join() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();

    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env().await.unwrap())
        .await
        .unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);

    chain
        .wait_for_batches(4, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer did not build a deep backlog");

    let (backlog_depth, backlog_hash) =
        block_number_and_hash_at(&seq.l2_rpc_url(), BlockNumberOrTag::Safe)
            .await
            .unwrap()
            .expect("sequencer has a safe block");

    let follower = spawn_follower("follower", &harness, None).await.unwrap();

    wait_for_safe_state(&follower, &chain, B256::ZERO, DEFAULT_TIMEOUT)
        .await
        .expect("late-joining follower did not backfill into an attested stateRoot");

    wait_for_safe_chain_contains(&follower, backlog_depth, backlog_hash, DEFAULT_TIMEOUT)
        .await
        .unwrap_or_else(|_| {
            panic!("follower did not replay full backlog to block {backlog_depth}")
        });

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}
