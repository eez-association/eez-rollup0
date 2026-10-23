//! End-to-end composer, follower, restart, outage, and reorg scenarios.

use std::time::Duration;

use alloy_primitives::{B256, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::BlockNumberOrTag;

use eez_testkit::signals;
use eez_testkit::{
    ANVIL_ADDR, ANVIL_ADDR_3, ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_3, ANVIL_KEY_4,
    ANVIL_KEY_6, BuilderStubMode, Harness, INVALID_PROOF_SELECTOR,
    INVALID_PROOF_SYSTEM_CONFIG_SELECTOR, NodeBinary, NodeConfig, NodeHandle, SEQUENCER_ADDR,
    STAGING_USER_KEY, block_number_and_hash_at, enode, override_env, peers_mode, rollup_root_at,
    safe_block_hash, send_l2_value_transfer, send_l2_value_transfer_confirmed, unused_tcp_port,
    wait_for, wait_for_latest_height, wait_for_new_attested_safe_block,
    wait_for_safe_chain_contains, wait_for_safe_prefix_convergence, wait_for_safe_state,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_mins(5);
/// Fixed so a seed node's enode is known before it starts.
const SEED_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

// A follower replaces the full divergent suffix of an intra-batch fork.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_composer_intra_batch_suffix_replay_converges() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let genesis = harness.l2_genesis_path();
    let composer_cfg = NodeConfig {
        genesis_path: Some(genesis),
        ..Default::default()
    };
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(genesis),
        ..Default::default()
    };

    let primary_dir = tempfile::tempdir().unwrap();
    let mirror_dir = tempfile::tempdir().unwrap();
    // The staging user key is not funded on Anvil. Both composers can
    // therefore sequence and prove, but neither can land a batch while the two
    // divergent local histories are being staged.
    let primary_stage_env = harness.env_for(STAGING_USER_KEY, true).await.unwrap();
    let mirror_stage_env = harness.env_for(STAGING_USER_KEY, true).await.unwrap();
    let (primary, mirror) = tokio::try_join!(
        NodeHandle::start_with_datadir(
            "intra-primary-stage",
            primary_dir.path(),
            &composer_cfg,
            &primary_stage_env,
        ),
        NodeHandle::start_with_datadir(
            "intra-mirror-stage",
            mirror_dir.path(),
            &composer_cfg,
            &mirror_stage_env,
        ),
    )
    .unwrap();

    let primary_rpc = primary.l2_rpc_url();
    let primary_provider = ProviderBuilder::new().connect_http(primary_rpc.parse().unwrap());
    let tx_hash = send_l2_value_transfer(&primary_rpc, ANVIL_KEY_1, ANVIL_ADDR, U256::from(1u64))
        .await
        .expect("submit divergent L2 transaction to primary composer");
    let receipt = wait_for(DEFAULT_TIMEOUT, || async {
        Ok(primary_provider.get_transaction_receipt(tx_hash).await?)
    })
    .await
    .unwrap_or_else(|err| panic!("wait for L2 tx {tx_hash} inclusion: {err:#}"));
    assert!(receipt.status(), "L2 tx {tx_hash} reverted");
    let included_block = receipt
        .block_number
        .unwrap_or_else(|| panic!("included L2 tx {tx_hash} missing block_number"));
    assert!(included_block > 0, "transaction must not land in genesis");

    let target = included_block + 3;
    let (primary_target, mirror_target) = tokio::try_join!(
        wait_for_latest_height(&primary, target, DEFAULT_TIMEOUT),
        wait_for_latest_height(&mirror, target, DEFAULT_TIMEOUT),
    )
    .expect("composers did not stage a multi-block suffix");
    assert_ne!(
        primary_target.hash, mirror_target.hash,
        "the staged suffix must actually diverge",
    );
    assert_eq!(
        chain.batches_posted().await.unwrap(),
        0,
        "unfunded staging composers must not settle either history",
    );

    drop(primary);
    drop(mirror);

    let follower_env = override_env(
        harness.follower_env(None).await.unwrap(),
        "RUST_LOG",
        "warn,eez_deriver=info,eez_l1=info",
    );
    let mirror = NodeHandle::start_with_datadir(
        "intra-mirror-follow",
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
    let primary = NodeHandle::start_with_datadir(
        "intra-primary-compose",
        primary_dir.path(),
        &composer_cfg,
        &composer_env,
    )
    .await
    .unwrap();
    let mirror_resyncs_before = mirror
        .count_signal(signals::DERIVER_RESYNC_RECOVERED)
        .unwrap_or_default();

    chain
        .wait_for_batches(1, DEFAULT_TIMEOUT)
        .await
        .expect("primary composer did not post its staged multi-block batch");
    wait_for_safe_prefix_convergence(&[&primary, &mirror], target, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not replace the complete divergent suffix");

    primary.assert_no_divergence_failure_logs();
    mirror.assert_no_divergence_failure_logs();
    assert_eq!(
        mirror
            .count_signal(signals::DERIVER_RESYNC_RECOVERED)
            .unwrap_or_default(),
        mirror_resyncs_before,
        "follower convergence was repaired by a full deriver resync instead of suffix replay",
    );
}

// Two composers build incompatible candidates for one settlement window. The
// loser must converge on the winning chain whether the relay drops its bundle
// or L1 includes it and rejects its stale state claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_composers_one_winner_loser_resyncs() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let cfg = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        ..Default::default()
    };
    let env_a = harness.env_for(ANVIL_KEY, true).await.unwrap();
    let env_b = harness.env_for(ANVIL_KEY_4, true).await.unwrap();
    chain.set_interval_mining(0).await.unwrap();
    let (composer_a, composer_b) = tokio::try_join!(
        NodeHandle::start("race-a", &cfg, &env_a),
        NodeHandle::start("race-b", &cfg, &env_b),
    )
    .unwrap();

    // Different transactions force different local candidate block hashes for the same
    // open settlement window instead of allowing two identical candidates.
    let composer_a_rpc = composer_a.l2_rpc_url();
    let composer_b_rpc = composer_b.l2_rpc_url();
    let (tx_a, tx_b) = tokio::try_join!(
        send_l2_value_transfer_confirmed(
            &composer_a_rpc,
            ANVIL_KEY_1,
            ANVIL_ADDR,
            U256::from(1u64),
            DEFAULT_TIMEOUT,
        ),
        send_l2_value_transfer_confirmed(
            &composer_b_rpc,
            ANVIL_KEY_2,
            ANVIL_ADDR_3,
            U256::from(2u64),
            DEFAULT_TIMEOUT,
        ),
    )
    .expect("failed to stage distinct Composer candidates");
    assert_ne!(
        tx_a, tx_b,
        "the competing candidates must carry distinct txs"
    );

    // Convergence must advance through the divergent transactions, rather than
    // passing vacuously because both nodes still agree on an older safe prefix.
    let provider_a = ProviderBuilder::new().connect_http(composer_a_rpc.parse().unwrap());
    let provider_b = ProviderBuilder::new().connect_http(composer_b_rpc.parse().unwrap());
    let (receipt_a, receipt_b) = tokio::try_join!(
        provider_a.get_transaction_receipt(tx_a),
        provider_b.get_transaction_receipt(tx_b),
    )
    .expect("failed to read staged transaction receipts");
    let candidate_a_height = receipt_a
        .and_then(|receipt| receipt.block_number)
        .expect("Composer A staged transaction has no block number");
    let candidate_b_height = receipt_b
        .and_then(|receipt| receipt.block_number)
        .expect("Composer B staged transaction has no block number");
    let convergence_height = candidate_a_height.max(candidate_b_height);
    tokio::try_join!(
        wait_for_latest_height(&composer_a, convergence_height, DEFAULT_TIMEOUT),
        wait_for_latest_height(&composer_b, convergence_height, DEFAULT_TIMEOUT),
    )
    .expect("composers did not reach a common height with their staged transactions");
    let (candidate_a, candidate_b) = tokio::try_join!(
        block_number_and_hash_at(
            &composer_a_rpc,
            BlockNumberOrTag::Number(convergence_height),
        ),
        block_number_and_hash_at(
            &composer_b_rpc,
            BlockNumberOrTag::Number(convergence_height),
        ),
    )
    .expect("failed to read competing candidates at their common height");
    let candidate_a = candidate_a.expect("Composer A is missing the common-height candidate");
    let candidate_b = candidate_b.expect("Composer B is missing the common-height candidate");
    assert_ne!(
        candidate_a.1, candidate_b.1,
        "the test must stage incompatible candidates at the same L2 height"
    );

    chain
        .set_interval_mining(eez_testkit::Chain::block_time_secs())
        .await
        .unwrap();

    chain
        .wait_for_batches_or_node_failure(2, &[&composer_a, &composer_b], DEFAULT_TIMEOUT)
        .await
        .expect("neither Composer established a canonical winner");
    wait_for_safe_prefix_convergence(
        &[&composer_a, &composer_b],
        convergence_height,
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("the losing Composer did not replace its fork with the L1 winner");
    let canonical = block_number_and_hash_at(
        &composer_a_rpc,
        BlockNumberOrTag::Number(convergence_height),
    )
    .await
    .expect("failed to read the canonical candidate after convergence")
    .expect("canonical chain is missing the contested height");
    assert!(
        canonical.1 == candidate_a.1 || canonical.1 == candidate_b.1,
        "the canonical history must be one of the two candidates selected by L1"
    );
    composer_a.assert_no_process_death();
    composer_b.assert_no_process_death();
}

// Two independent composer databases alternate as the active poster. Each
// takeover must resume from L1 and extend one continuous L2 history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_composers_alternate_without_safe_chain_gaps() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let genesis = harness.l2_genesis_path();
    let cfg = NodeConfig {
        genesis_path: Some(genesis),
        ..Default::default()
    };
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let env_a = harness.env_for(ANVIL_KEY, true).await.unwrap();
    let env_b = harness.env_for(ANVIL_KEY_4, true).await.unwrap();

    let composer_a = NodeHandle::start_with_datadir("alternate-a1", dir_a.path(), &cfg, &env_a)
        .await
        .unwrap();
    chain
        .wait_for_batches_or_node_failure(2, &[&composer_a], DEFAULT_TIMEOUT)
        .await
        .expect("Composer A did not establish the first canonical segment");
    composer_a.assert_no_process_death();
    drop(composer_a);

    let after_a = chain.batches_posted().await.unwrap();
    let composer_b = NodeHandle::start_with_datadir("alternate-b", dir_b.path(), &cfg, &env_b)
        .await
        .unwrap();
    chain
        .wait_for_batches_or_node_failure(after_a + 2, &[&composer_b], DEFAULT_TIMEOUT)
        .await
        .expect("Composer B did not extend Composer A's canonical segment");
    composer_b.assert_no_process_death();
    drop(composer_b);

    let after_b = chain.batches_posted().await.unwrap();
    let composer_a = NodeHandle::start_with_datadir("alternate-a2", dir_a.path(), &cfg, &env_a)
        .await
        .unwrap();
    chain
        .wait_for_batches_or_node_failure(after_b + 2, &[&composer_a], DEFAULT_TIMEOUT)
        .await
        .expect("Composer A did not resume after Composer B's segment");
    wait_for_safe_state(
        &composer_a,
        &chain,
        chain.initial_commitment(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("the returning Composer did not catch up to the L1-derived state");

    let safe = block_number_and_hash_at(&composer_a.l2_rpc_url(), BlockNumberOrTag::Safe)
        .await
        .unwrap()
        .expect("returning Composer has no safe head");
    let provider = ProviderBuilder::new().connect_http(composer_a.l2_rpc_url().parse().unwrap());
    for height in 0..=safe.0 {
        assert!(
            provider
                .get_block_by_number(BlockNumberOrTag::Number(height))
                .await
                .unwrap()
                .is_some(),
            "safe chain has a missing block at height {height}"
        );
    }
    composer_a.assert_no_process_death();
}

/// Builder mode, sustained operation through a restart. Asserts every
/// observable invariant in one place:
///   - lockstep: `BatchPosted == L2ExecutionPerformed`, always;
///   - zero `L2TxSkipped` (no prestate/rolling-hash misfire);
///   - `latest_event.newRoot == rollups[rid].root` (event-state
///     consistency);
///   - state advances beyond genesis and remains monotonic;
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
            chain.initial_commitment(),
            DEFAULT_TIMEOUT,
        )
        .await
        .expect("pre-restart state transition was not attested");
        chain.wait_for_batches(3, DEFAULT_TIMEOUT).await.unwrap();
        pre_restart_latest = wait_for_latest_height(&node_before_restart, 1, DEFAULT_TIMEOUT)
            .await
            .expect("pre-restart node did not produce L2 blocks");
        let before = chain.snapshot().await.unwrap();
        n_before = before.batches_posted;
        assert_eq!(before.executions_performed, n_before, "lockstep");
        assert_eq!(before.entries_skipped, 0, "no entry should revert");
        // Against the genesis BLOCK HASH: comparing a commitment with the
        // genesis state root can never match, so it would pass vacuously.
        assert_ne!(
            before.rollup_commitment,
            chain.initial_commitment(),
            "the commitment must have moved off genesis",
        );
        assert_eq!(
            before.latest_execution_root.unwrap(),
            before.rollup_commitment,
            "latest event's newRoot == on-chain root",
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

    chain
        .wait_for_batches(n_before + 1, DEFAULT_TIMEOUT)
        .await
        .expect("composer didn't post any new batch after restart");
    let after = chain.snapshot().await.unwrap();
    let n_after = after.batches_posted;
    assert!(
        n_after > n_before,
        "BatchPosted grew ({n_before} → {n_after})"
    );
    let post_restart_target_height = pre_restart_latest.number + 1;
    wait_for_latest_height(&node, post_restart_target_height, DEFAULT_TIMEOUT)
        .await
        .expect("restarted node did not advance L2 height after restart");
    assert_eq!(after.executions_performed, n_after, "no replay");
    assert_eq!(after.entries_skipped, 0, "no skipped entries after restart");
    assert_eq!(
        after.latest_execution_root.unwrap(),
        after.rollup_commitment,
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
    wait_for_safe_state(
        &follower,
        &chain,
        chain.initial_commitment(),
        DEFAULT_TIMEOUT,
    )
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

// A relay may acknowledge bundles while silently omitting them from its target
// blocks. The composer must preserve the local candidate, retry after the relay
// recovers, and continue settling fresh L2 traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_silent_drop_preserves_safety_and_recovers_progress() {
    let harness = Harness::fresh().await.unwrap();
    harness
        .set_builder_mode(BuilderStubMode::Drop)
        .await
        .expect("configure silent builder drops");
    let chain = harness.chain();
    let node = NodeHandle::start(
        "builder-silent-drop",
        &NodeConfig::default(),
        &harness.env().await.unwrap(),
    )
    .await
    .unwrap();

    // Wait for observed misses rather than sleeping. This proves the composer
    // reached the relay and the relay accepted but omitted multiple bundles.
    wait_for(DEFAULT_TIMEOUT, || async {
        Ok((node.count_signal(signals::BUNDLE_DROPPED)? >= 2).then_some(()))
    })
    .await
    .expect("composer did not observe two silently dropped bundles");
    assert_eq!(
        chain.batches_posted().await.unwrap(),
        0,
        "an acknowledged but omitted bundle must not advance settlement",
    );
    node.assert_no_process_death();

    harness
        .set_builder_mode(BuilderStubMode::Forward)
        .await
        .expect("restore builder forwarding");
    chain
        .wait_for_batches_or_node_failure(1, &[&node], DEFAULT_TIMEOUT)
        .await
        .expect("composer did not recover after silent bundle drops");

    // Recovery is incomplete if only the old candidate settles and the
    // composer remains wedged. Require a fresh transaction and its exact block
    // to enter the safe chain after forwarding resumes.
    let l2_rpc = node.l2_rpc_url();
    let fresh = send_l2_value_transfer_confirmed(
        &l2_rpc,
        ANVIL_KEY_1,
        ANVIL_ADDR,
        U256::from(7u64),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("fresh post-recovery L2 transaction did not execute");
    let provider = ProviderBuilder::new().connect_http(l2_rpc.parse().unwrap());
    let receipt = provider
        .get_transaction_receipt(fresh)
        .await
        .unwrap()
        .expect("fresh transaction receipt disappeared");
    let block_number = receipt
        .block_number
        .expect("fresh receipt has no block number");
    let block_hash = receipt.block_hash.expect("fresh receipt has no block hash");
    wait_for_safe_chain_contains(&node, block_number, block_hash, DEFAULT_TIMEOUT)
        .await
        .expect("fresh post-recovery transaction never entered the safe chain");
    node.assert_no_divergence_failure_logs();
}

// Proofs for an unregistered rollup ID are signed but never posted.
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
        .assert_failed_post_and_verify_batch(999, INVALID_PROOF_SYSTEM_CONFIG_SELECTOR)
        .await
        .expect("wrong-rollup batch did not reach L1 structural validation");

    let snapshot = chain.snapshot().await.unwrap();
    assert_eq!(snapshot.batches_posted, 0);
    assert_eq!(snapshot.executions_performed, 0);
    assert_eq!(snapshot.rollup_commitment, chain.initial_commitment());
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

    chain
        .wait_for_batches(2, Duration::from_mins(1))
        .await
        .unwrap();

    harness
        .anvil
        .set_balance(ANVIL_ADDR, U256::ZERO)
        .await
        .unwrap();
    // Let work submitted before the balance change either land or fail before
    // measuring the outage interval.
    chain
        .wait_for_l1_blocks(1, Duration::from_secs(15))
        .await
        .unwrap();
    let outage_baseline = chain.batches_posted().await.unwrap();
    chain
        .wait_for_l1_blocks(5, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        chain.batches_posted().await.unwrap(),
        outage_baseline,
        "no progress during outage"
    );

    let restored = U256::from(10u64).pow(U256::from(21u64));
    harness
        .anvil
        .set_balance(ANVIL_ADDR, restored)
        .await
        .unwrap();
    chain
        .wait_for_batches(outage_baseline + 1, Duration::from_mins(1))
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
        .assert_failed_post_and_verify_batch(harness.dep.rollup_id, INVALID_PROOF_SELECTOR)
        .await
        .expect("unauthorized-attester batch did not reach L1 proof verification");

    let snapshot = chain.snapshot().await.unwrap();
    assert_eq!(snapshot.batches_posted, 0);
    assert_eq!(snapshot.executions_performed, 0);
    assert_eq!(snapshot.rollup_commitment, chain.initial_commitment());
    node.assert_no_process_death();
}

/// Competing composers retreat across an L1 reorg and reconverge.
/// They must produce newly attested work before old-prefix agreement can pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_two_composers_l1_reorg_recovers() {
    let harness = Harness::fresh().await.unwrap();
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
        .wait_for_batches_or_node_failure(pre_batches + 1, &[&c1, &c2], DEFAULT_TIMEOUT)
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

// Three composers race on one L1 without a reorg or injected fault. Each
// composer must remain converged with L1 instead of wedging on redundant work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_composer_steady_state_stays_converged_with_l1() {
    const COMPOSERS: usize = 3;
    // Enough to catch a composer that converges once and then drifts.
    const SETTLEMENTS: usize = 24;

    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let cfg = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        ..Default::default()
    };

    // Distinct posters: a shared key serialises them on nonces, so no race.
    let poster_keys = [ANVIL_KEY, ANVIL_KEY_4, ANVIL_KEY_6];
    let spam_keys = [ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_3];
    let mut nodes = Vec::with_capacity(COMPOSERS);
    for (i, poster) in poster_keys.iter().take(COMPOSERS).enumerate() {
        // `expect_external_batches`: peer batches are normal here, not an error.
        let env = harness.env_for(poster, true).await.unwrap();
        let name: &'static str = ["mc1", "mc2", "mc3"][i];
        nodes.push((name, NodeHandle::start(name, &cfg, &env).await.unwrap()));
    }
    for ((_, node), key) in nodes.iter().zip(spam_keys) {
        node.run_tx_spammer(key);
    }

    // Comparing against L1's CURRENT root would race, since L1 advances while
    // we read. So: the safe root must be one L1 recorded (soundness), and the
    // safe height must keep rising (liveness — a wedged deriver stops).
    let mut last_safe = vec![0u64; nodes.len()];
    let mut advanced = 0usize;
    let mut compared = 0usize;
    for settled in 1..=SETTLEMENTS {
        chain
            .wait_for_batches(settled, DEFAULT_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("stalled at settlement {settled} of {SETTLEMENTS}: {e:#}"));

        // Every root L1 recorded: a lagging composer is fine, an invented
        // one is not.
        let recorded = chain.executed_states().await.unwrap();
        for (i, (name, node)) in nodes.iter().enumerate() {
            node.assert_no_process_death();
            let Some((number, _hash)) =
                block_number_and_hash_at(&node.l2_rpc_url(), BlockNumberOrTag::Safe)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("{name}: safe block unreadable at {settled}: {e:#}")
                    })
            else {
                continue; // no attested block yet; nothing to judge
            };
            if number == 0 {
                continue;
            }
            let safe_hash = safe_block_hash(&node.l2_rpc_url())
                .await
                .unwrap()
                .unwrap_or_default();
            compared += 1;
            assert!(
                recorded.contains(&safe_hash),
                "{name} safe root {safe_hash} at L2 height {number} was never recorded by L1 \
                 (settlement {settled}); the composer derived a chain L1 did not ratify",
            );
            if number > last_safe[i] {
                advanced += 1;
                last_safe[i] = number;
            }
        }
    }

    // Surfaced so a pass is auditable — a run that compared nothing would
    // otherwise look identical to a real one.
    eprintln!(
        "multi-composer: root comparisons={compared} l1_recorded_roots={} final_safe_heights={last_safe:?}",
        chain.executed_states().await.unwrap().len(),
    );

    // Caught up AND still moving: a wedged deriver attests a few then stops.
    for (i, (name, _)) in nodes.iter().enumerate() {
        assert!(
            last_safe[i] > 0,
            "{name} never attested a safe block — it never caught up to L1 at all",
        );
    }
    assert!(
        advanced >= SETTLEMENTS,
        "safe heads advanced only {advanced} times across {SETTLEMENTS} settlements and \
         {COMPOSERS} composers — at least one composer stopped tracking L1",
    );
    // Allow a startup window: early settlements can precede a composer's
    // first attested safe block.
    let floor = (SETTLEMENTS - 3) * COMPOSERS;
    assert!(
        compared >= floor,
        "only {compared} root comparisons (floor {floor}) across {SETTLEMENTS} settlements × \
         {COMPOSERS} composers — the soundness check was mostly skipped, so this proves little",
    );
}

// A composer in based mode classifies a peer's batch as expected external work.
// The observer is unfunded so only the peer can land a batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn based_mode_logs_peer_batch_as_expected_external() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let cfg = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        ..Default::default()
    };
    let observer_env = harness.env_for(STAGING_USER_KEY, true).await.unwrap();
    let peer_env = harness.env_for(ANVIL_KEY, true).await.unwrap();
    let (observer, peer) = tokio::try_join!(
        NodeHandle::start("based-observer", &cfg, &observer_env),
        NodeHandle::start("based-peer", &cfg, &peer_env),
    )
    .unwrap();

    chain
        .wait_for_batches_or_node_failure(2, &[&observer, &peer], DEFAULT_TIMEOUT)
        .await
        .expect("the funded peer did not land a batch");
    wait_for(DEFAULT_TIMEOUT, || {
        std::future::ready(
            observer
                .log_count_matching(&["external batch landed (based mode)"])
                .map(|count| (count > 0).then_some(())),
        )
    })
    .await
    .expect("based-mode observer never classified the peer batch as expected external traffic");

    assert_eq!(
        observer
            .log_count_matching(&["external batch landed in sequenced-mode rollup"])
            .unwrap(),
        0,
        "based-mode peer traffic must not be logged as a sequenced-mode violation",
    );
    observer.assert_no_process_death();
    peer.assert_no_process_death();
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
    wait_for_safe_state(
        &follower,
        &chain,
        chain.initial_commitment(),
        DEFAULT_TIMEOUT,
    )
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
        "warn,eez_follower::unsafe_head=info",
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
    wait_for_safe_state(
        &follower,
        &chain,
        chain.initial_commitment(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("follower did not catch up via L1 replay");

    wait_for_safe_prefix_convergence(&[&seq, &follower], 1, DEFAULT_TIMEOUT)
        .await
        .expect("follower safe block never matched the sequencer chain");

    let from = follower.signal_cursor().unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);
    // Only the signed-head path records `head.advanced`, after verifying and
    // importing the head; the sequencer must hold that exact block.
    wait_for(DEFAULT_TIMEOUT, || async {
        for s in follower.signals_since(from)? {
            if s.name != signals::FOLLOWER_HEAD_ADVANCED {
                continue;
            }
            let n = s.u64("block.number")?;
            let at_seq = block_number_and_hash_at(&seq_rpc, BlockNumberOrTag::Number(n)).await?;
            if at_seq == Some((n, s.b256("block.hash")?)) {
                return Ok(Some(()));
            }
        }
        Ok(None)
    })
    .await
    .expect("follower never adopted a signed sequencer head");

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// Follower B, with no sequencer RPC, finds the network from the seed's bootnode,
/// receives heads over gossip, and fetches their ancestors from devp2p peers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_follows_over_p2p() {
    let harness = Harness::fresh().await.unwrap();
    let p2p = |binary, args: &[&str]| NodeConfig {
        binary,
        discovery: true,
        extra_args: args.iter().map(ToString::to_string).collect(),
        ..Default::default()
    };
    let overlay = || {
        let port = unused_tcp_port();
        ("EEZ_P2P_LISTEN", format!("/ip4/127.0.0.1/tcp/{port}"))
    };

    // The bootnodes guard needs an entry. The seed's own is a no-op, so the
    // ports, leased only at spawn, don't matter.
    let seq_cfg = p2p(
        NodeBinary::Composer,
        &[
            "--p2p-secret-key-hex",
            SEED_KEY,
            "--bootnodes",
            &enode(SEED_KEY, 0, 0),
        ],
    );
    let mut seq_env = harness.env().await.unwrap();
    seq_env.push(overlay());
    let seq = NodeHandle::start("seq", &seq_cfg, &seq_env).await.unwrap();

    let follower_cfg = p2p(
        NodeBinary::Follower,
        &[
            "--bootnodes",
            &enode(SEED_KEY, seq.p2p_port, seq.discv5_port),
        ],
    );
    let mut follower_env = harness.follower_env(None).await.unwrap();
    follower_env.push(("EEZ_SEQUENCER_ADDRESS", format!("{SEQUENCER_ADDR:#x}")));
    let mut a_env = follower_env.clone();
    a_env.push(overlay());
    let a = NodeHandle::start("follower-a", &follower_cfg, &a_env)
        .await
        .unwrap();

    wait_for_latest_height(&seq, 10, DEFAULT_TIMEOUT)
        .await
        .expect("composer did not reach block 10");
    let mut b_env = override_env(
        follower_env,
        "RUST_LOG",
        "warn,eez_follower::unsafe_head=info",
    );
    b_env.push(overlay());
    let b = NodeHandle::start("follower-b", &follower_cfg, &b_env)
        .await
        .unwrap();

    // With no RPC, `imported >= 2` means the ancestors came from devp2p peers.
    let seq_rpc = seq.l2_rpc_url();
    wait_for(DEFAULT_TIMEOUT, || async {
        for s in b.signals_since(0)? {
            if s.name != signals::FOLLOWER_HEAD_ADVANCED || s.u64("imported")? < 2 {
                continue;
            }
            let n = s.u64("block.number")?;
            let at_seq = block_number_and_hash_at(&seq_rpc, BlockNumberOrTag::Number(n)).await?;
            if at_seq == Some((n, s.b256("block.hash")?)) {
                return Ok(Some(()));
            }
        }
        Ok(None)
    })
    .await
    .expect("follower B never adopted a gossiped head with peer-fetched ancestors");

    for node in [&seq, &a, &b] {
        node.assert_no_process_death();
    }
}

/// An L1-only follower retreats, replays the replacement suffix, and converges
/// with the composer after an L1 reorg.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_l1_reorg_recovers() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let genesis = harness.l2_genesis_path();
    let seq_cfg = NodeConfig {
        genesis_path: Some(genesis),
        ..Default::default()
    };
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(genesis),
        ..Default::default()
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
        .wait_for_batches_or_node_failure(pre_batches + 1, &[&seq, &follower], DEFAULT_TIMEOUT)
        .await
        .expect("no batches landed after reorg");
    let (post_reorg_safe_number, _) =
        wait_for_new_attested_safe_block(&seq, &chain, &pre_reorg_states, DEFAULT_TIMEOUT)
            .await
            .expect("sequencer did not import the post-reorg safe block");
    wait_for_safe_prefix_convergence(&[&seq, &follower], post_reorg_safe_number, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not converge on the composer's replacement safe suffix");

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
    wait_for_safe_state(&f_l1, &chain, chain.initial_commitment(), DEFAULT_TIMEOUT)
        .await
        .expect("f_l1 did not catch up");
    wait_for_safe_state(&f_seq, &chain, chain.initial_commitment(), DEFAULT_TIMEOUT)
        .await
        .expect("f_seq did not catch up");

    wait_for_safe_prefix_convergence(&[&seq, &f_l1, &f_seq], 1, DEFAULT_TIMEOUT)
        .await
        .expect("followers never shared a sequencer safe block");

    f_l1.assert_no_process_death();
    f_seq.assert_no_process_death();
    seq.assert_no_process_death();
}

/// Heads signed by a key other than the configured sequencer's are rejected:
/// the follower's head never takes them and its L1-derived safe head holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_rogue_sequencer_safe_head_holds() {
    let harness = Harness::fresh().await.unwrap();
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

    // The real sequencer, trusted under another address: every head it signs is rogue.
    let follower_env = override_env(
        harness.follower_env(Some(&seq.l2_rpc_url())).await.unwrap(),
        "EEZ_SEQUENCER_ADDRESS",
        &format!("{ANVIL_ADDR:#x}"),
    );
    let follower_env = override_env(
        follower_env,
        "RUST_LOG",
        "warn,eez_follower::unsafe_head=info",
    );
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(genesis),
        ..Default::default()
    };
    let follower = NodeHandle::start("follower", &follower_cfg, &follower_env)
        .await
        .unwrap();

    wait_for_safe_state(
        &follower,
        &chain,
        chain.initial_commitment(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("follower safe head did not reach a non-genesis attested root while on the rogue");

    // Stop settlement but keep the rogue signing, and let any already-submitted
    // batch land before fixing the safe anchor used by this assertion.
    harness
        .set_builder_mode(BuilderStubMode::Drop)
        .await
        .expect("stop settlement");
    chain
        .wait_for_l1_blocks(1, Duration::from_secs(15))
        .await
        .expect("L1 did not flush after stopping settlement");
    let final_state = chain
        .snapshot()
        .await
        .unwrap()
        .latest_execution_root
        .expect("composer settled a batch before settlement stopped");
    // An arbitrary historical attestation is insufficient here: a final batch
    // may have landed while settlement was stopping.
    wait_for(DEFAULT_TIMEOUT, || async {
        Ok((safe_block_hash(&follower.l2_rpc_url()).await? == Some(final_state)).then_some(()))
    })
    .await
    .expect("follower did not derive the composer's final landed batch");
    let safe_before = block_number_and_hash_at(&follower.l2_rpc_url(), BlockNumberOrTag::Safe)
        .await
        .unwrap()
        .expect("follower has a safe head");

    let rejected_before = follower
        .count_signal(signals::FOLLOWER_HEAD_REJECTED)
        .unwrap();
    chain
        .wait_for_l1_blocks(3, Duration::from_secs(30))
        .await
        .expect("L1 did not advance three blocks");
    assert!(
        follower
            .count_signal(signals::FOLLOWER_HEAD_REJECTED)
            .unwrap()
            > rejected_before,
        "follower must reject the heads the rogue keeps signing",
    );
    assert_eq!(
        follower
            .count_signal(signals::FOLLOWER_HEAD_ADVANCED)
            .unwrap(),
        0,
        "a rogue head must never move the follower's head",
    );
    let safe_after = block_number_and_hash_at(&follower.l2_rpc_url(), BlockNumberOrTag::Safe)
        .await
        .unwrap();
    assert_eq!(
        safe_after,
        Some(safe_before),
        "rogue heads must not move the L1-derived safe head",
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

    wait_for_safe_state(
        &follower,
        &chain,
        chain.initial_commitment(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("late-joining follower did not backfill into an attested root");

    wait_for_safe_chain_contains(&follower, backlog_depth, backlog_hash, DEFAULT_TIMEOUT)
        .await
        .unwrap_or_else(|_| {
            panic!("follower did not replay full backlog to block {backlog_depth}")
        });

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// A fresh peers-mode follower syncs the history below L1-finalized from its
/// trusted peer through reth's pipeline, and scans L1 only from there; one
/// without peers waits at genesis and never derives from L1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_follower_syncs_history_from_peers() {
    let harness = Harness::fresh_with_fast_finality().await.unwrap();
    let composer = peers_composer(&harness).await;

    // reth backfills with its pipeline only past DEFAULT_BACKFILL_RUN_THRESHOLD = EPOCH_SLOTS = 32
    // (engine/primitives/src/config.rs:28); a shorter gap is downloaded block by block.
    let (finalized_l1, height, root) = wait_for(DEFAULT_TIMEOUT, || async {
        Ok(finalized_root(&harness, &composer)
            .await?
            .filter(|(_, height, _)| *height > 32))
    })
    .await
    .expect("L1 never finalized a root above L2 block 32");

    let mut follower_env = harness.follower_env(None).await.unwrap();
    peers_mode(&mut follower_env);
    let follower_cfg = |extra_args| NodeConfig {
        binary: NodeBinary::Follower,
        extra_args,
        ..Default::default()
    };
    let lonely = NodeHandle::start("follower-no-peers", &follower_cfg(vec![]), &follower_env)
        .await
        .unwrap();
    let waiting = wait_for(Duration::from_secs(40), || async {
        Ok(lonely
            .signals_since(0)?
            .into_iter()
            .find(|s| s.name == signals::NODE_SYNC_WAITING_FOR_PEERS))
    })
    .await
    .expect("a follower without peers never reported waiting for them");
    assert_eq!(
        waiting.u64("peers").unwrap(),
        0,
        "the lonely follower has a peer"
    );
    // Checked again at the end, after it has waited through the other follower's sync.
    let still_waiting = || async {
        let latest = block_number_and_hash_at(&lonely.l2_rpc_url(), BlockNumberOrTag::Latest)
            .await
            .unwrap();
        assert_eq!(
            latest.map(|(n, _)| n),
            Some(0),
            "follower without peers left genesis"
        );
        assert_eq!(
            lonely
                .count_signal(signals::DERIVER_CATCH_UP_START)
                .unwrap(),
            0,
            "follower without peers derived from L1",
        );
    };
    still_waiting().await;

    let trusted = vec![
        "--trusted-peers".into(),
        enode(SEED_KEY, composer.p2p_port, composer.discv5_port),
    ];
    let synced = NodeHandle::start("follower-synced", &follower_cfg(trusted), &follower_env)
        .await
        .unwrap();
    wait_for_safe_chain_contains(&synced, height, root, DEFAULT_TIMEOUT)
        .await
        .expect("follower never synced to the L1-finalized root");
    // reth reports the synced root as safe before boot reaches the L1 scan.
    let first_scan = wait_for(DEFAULT_TIMEOUT, || async {
        Ok(synced
            .signals_since(0)?
            .into_iter()
            .find(|s| s.name == signals::DERIVER_CATCH_UP_START))
    })
    .await
    .expect("synced follower never scanned L1");
    // The seeded scan starts at the follower's own F, never below the one read here.
    assert!(
        first_scan.u64("from_l1_block").unwrap() >= finalized_l1,
        "synced follower scanned L1 from below L1-finalized",
    );
    // No head source moves safe now, so passing it takes a batch the Deriver reconciled from L1.
    let (seeded_safe, _) = block_number_and_hash_at(&synced.l2_rpc_url(), BlockNumberOrTag::Safe)
        .await
        .unwrap()
        .expect("synced follower has a safe block");
    wait_for_safe_prefix_convergence(&[&composer, &synced], seeded_safe + 1, DEFAULT_TIMEOUT)
        .await
        .expect("synced follower never reconciled an L1 batch past its seed");

    still_waiting().await;
    for node in [&composer, &lonely, &synced] {
        node.assert_no_process_death();
    }
}

/// A peers-mode follower restarted more than 32 blocks below L1-finalized
/// rewinds to its own finalized block, syncs the rest from its peer through
/// reth's pipeline, and scans L1 only from F.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_restart_below_finalized_resyncs_from_peers() {
    let harness = Harness::fresh_with_fast_finality().await.unwrap();
    let composer = peers_composer(&harness).await;
    let mut env = harness.follower_env(None).await.unwrap();
    peers_mode(&mut env);
    // At threshold 0 reth persists every block, so the kill keeps a finalized block above genesis.
    let cfg = NodeConfig {
        binary: NodeBinary::Follower,
        extra_args: vec![
            "--trusted-peers".into(),
            enode(SEED_KEY, composer.p2p_port, composer.discv5_port),
            "--engine.persistence-threshold".into(),
            "0".into(),
        ],
        ..Default::default()
    };
    let datadir = tempfile::tempdir().unwrap();
    let last_head = {
        let first = NodeHandle::start_with_datadir("follower-first", datadir.path(), &cfg, &env)
            .await
            .unwrap();
        let rpc = first.l2_rpc_url();
        wait_for(DEFAULT_TIMEOUT, || async {
            let finalized = block_number_and_hash_at(&rpc, BlockNumberOrTag::Finalized).await?;
            Ok(finalized.filter(|(number, _)| *number > 0))
        })
        .await
        .expect("follower never finalized an L1-settled batch");
        // reth writes a finalized FCU to disk only with the next block it saves.
        let head = wait_for_latest_height(&first, 0, DEFAULT_TIMEOUT).await;
        let later = wait_for_latest_height(&first, head.unwrap().number + 1, DEFAULT_TIMEOUT)
            .await
            .expect("follower never saved a block after finalizing");
        // Persistence trails the head; one L1 block lets it flush.
        harness
            .chain()
            .wait_for_l1_blocks(1, Duration::from_secs(20))
            .await
            .unwrap();
        first.assert_no_process_death();
        later.number
    };

    let (finalized_l1, height, root) = wait_for(DEFAULT_TIMEOUT, || async {
        Ok(finalized_root(&harness, &composer)
            .await?
            .filter(|(_, height, _)| *height > last_head + 32))
    })
    .await
    .expect("L1 never finalized a root 32 blocks past the stopped follower");
    let restarted =
        NodeHandle::start_with_datadir("follower-restarted", datadir.path(), &cfg, &env)
            .await
            .unwrap();
    let first_signal = |name: &'static str| {
        let node = &restarted;
        wait_for(DEFAULT_TIMEOUT, move || async move {
            Ok(node.signals_since(0)?.into_iter().find(|s| s.name == name))
        })
    };
    // Waiting for peers means the Sync path, not Seed; a best block above 0 means it rewound to f > 0.
    let syncing = first_signal(signals::NODE_SYNC_WAITING_FOR_PEERS)
        .await
        .expect("restarted follower never synced from peers");
    assert!(syncing.u64("local_best").unwrap() > 0, "rewound to genesis");
    wait_for_safe_chain_contains(&restarted, height, root, DEFAULT_TIMEOUT)
        .await
        .expect("restarted follower never reached the L1-finalized root");
    let first_scan = first_signal(signals::DERIVER_CATCH_UP_START)
        .await
        .expect("restarted follower never scanned L1");
    assert!(
        first_scan.u64("from_l1_block").unwrap() >= finalized_l1,
        "restarted follower scanned L1 from below L1-finalized",
    );
    wait_for_safe_prefix_convergence(&[&composer, &restarted], height + 1, DEFAULT_TIMEOUT)
        .await
        .expect("restarted follower never reconciled an L1 batch past the root");
    restarted.assert_no_process_death();
    composer.assert_no_process_death();
}

/// A peers-mode composer under [`SEED_KEY`], with traffic.
async fn peers_composer(harness: &Harness) -> NodeHandle {
    let mut env = harness.env().await.unwrap();
    peers_mode(&mut env);
    let cfg = NodeConfig {
        extra_args: vec!["--p2p-secret-key-hex".into(), SEED_KEY.into()],
        ..Default::default()
    };
    let composer = NodeHandle::start("composer", &cfg, &env).await.unwrap();
    composer.run_tx_spammer(ANVIL_KEY_1);
    composer
}

/// L1's finalized block F, and the height and hash of the root L1 stores at F
/// once `node` holds it.
async fn finalized_root(
    harness: &Harness,
    node: &NodeHandle,
) -> anyhow::Result<Option<(u64, u64, B256)>> {
    let l1 = ProviderBuilder::new().connect_http(harness.anvil.rpc_url.parse()?);
    let Some(finalized) = l1.get_block_by_number(BlockNumberOrTag::Finalized).await? else {
        return Ok(None);
    };
    let f = finalized.header.number;
    let (eez, rollup_id) = (harness.dep.eez_address, harness.dep.rollup_id);
    let root = rollup_root_at(&harness.anvil.rpc_url, eez, rollup_id, Some(f)).await?;
    let l2 = ProviderBuilder::new().connect_http(node.l2_rpc_url().parse()?);
    let block = l2.get_block_by_hash(root).await?;
    Ok(block.map(|block| (f, block.header.number, root)))
}
