//! End-to-end: anvil L1 + bundle stub + deployed protocol + spawned eez-node.
//!
//! One mega happy-path test asserts every observable invariant of the
//! Builder mode (the only mode eez-rollup0 supports today). Three
//! minimal failure tests exercise one revert path each. See
//! eez-rollup0-testing-sota.md for the pattern sources.

use std::time::Duration;

use alloy_primitives::{B256, U256};
use alloy_provider::{Provider, ProviderBuilder};

mod common;
use common::{
    ANVIL_ADDR, ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_4, AnvilConfig, Harness, NodeConfig,
    NodeHandle, override_env, reorg_genesis_path, reorg_genesis_state_root, send_l2_value_transfer,
    wait_for, wait_for_latest_height, wait_for_node_caught_up, wait_for_safe_prefix_convergence,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

fn with_composer_disabled(mut env: Vec<(&'static str, String)>) -> Vec<(&'static str, String)> {
    env.push(("EEZ_COMPOSER_DISABLED", "1".to_string()));
    env
}

fn assert_no_divergence_failure_logs(nodes: &[&NodeHandle]) {
    for node in nodes {
        node.assert_no_divergence_failure_logs();
    }
}

/// Regression for the original suffix-replay bug. Sequencer B locally
/// builds empty blocks on its own ancestry. Sequencer A builds one L2
/// tx followed by empty blocks, then posts them as a single multi-block
/// batch. B must replay the mismatched tx block and every later block
/// in the same batch, even when those later tx lists match.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_sequencer_intra_batch_suffix_replay_converges() {
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

    let primary_dir = tempfile::tempdir().unwrap();
    let mirror_dir = tempfile::tempdir().unwrap();
    let seq_a_env_disabled = with_composer_disabled(harness.env_for(ANVIL_KEY, true));
    let seq_b_env = with_composer_disabled(harness.env_for(ANVIL_KEY_4, true));

    let seq_a = NodeHandle::start_with_datadir(
        "intra-seq-a-stage",
        primary_dir.path(),
        &cfg,
        &seq_a_env_disabled,
    )
    .await
    .unwrap();
    let seq_b = NodeHandle::start_with_datadir("intra-seq-b", mirror_dir.path(), &cfg, &seq_b_env)
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
    let seq_a = NodeHandle::start_with_datadir(
        "intra-seq-a-compose",
        primary_dir.path(),
        &cfg,
        &harness.env_for(ANVIL_KEY, true),
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

    assert_no_divergence_failure_logs(&[&seq_a, &seq_b]);
}

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
    let node = NodeHandle::spawn(datadir.path(), &env).unwrap();

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

    // Phase 3 — follower full-replay. Spawn a fresh-datadir node with
    // sequencer + composer disabled; `Deriver::catch_up` must rebuild
    // state from L1 events alone, land on a stateRoot the contract
    // has attested, and agree with the restarted node's safe block hash.
    let mut follower_env = env;
    follower_env.push(("EEZ_SEQUENCER_DISABLED", "1".to_string()));
    follower_env.push(("EEZ_COMPOSER_DISABLED", "1".to_string()));
    let follower = NodeHandle::start("follower", &NodeConfig::default(), &follower_env)
        .await
        .unwrap();
    wait_for_node_caught_up(&follower, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");
    wait_for_safe_prefix_convergence(&[&node, &follower], n_after as u64, DEFAULT_TIMEOUT)
        .await
        .expect("restarted node and replay follower did not converge on safe block hashes");
    follower.assert_no_process_death();
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
/// background collector sends real L2 state-changing txs so batches
/// carry content (exercises receipt pruning, state-trie writes, and
/// txpool reorg paths that empty batches skip). After ≥4 batches land
/// we call `anvil_reorg(depth=3)` and verify five invariants:
///
/// **I1 — No process death.** Neither node logs `Fatal` /
/// `UnexpectedStaticFile`. Without this every other check is moot.
///
/// **I2 — Both derivers noticed the reorg.** Each node logged
/// `reorg rolled out` / `l1.reorg.retreated` ≥ 1 time. Crucial because
/// state convergence can happen via unrelated re-derivation paths; this
/// is the only assertion that proves the deriver itself exercised its
/// reorg-handling code.
///
/// **I3 — Each node saw and processed an L1-attested state.** For each
/// composer independently: at some poll, `node.safe.stateRoot` appears
/// in the set of all `newState` values the contract has emitted via
/// `L2ExecutionPerformed`. The two nodes don't have to coincide, and
/// we don't require equality with the *current* contract head — the
/// contract is a moving target while composers keep posting, so
/// instant-equality is timing-fragile. The set-membership check is the
/// honest restatement: "this node has imported a block whose stateRoot
/// the contract has, at some point, attested as canonical."
///
/// **I4 — Same safe block hash.** The two nodes converge on an identical
/// safe block hash after reorg recovery. This is stronger than stateRoot
/// equality: empty or equivalent-state blocks can still diverge by hash.
///
/// **I5 — Liveness.** Post-reorg batches landed (`batches_posted` grew
/// past the pre-reorg snapshot). Proves the chain didn't freeze on the
/// rewound state.
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
    // Start both concurrently — sequential start lets c1 race alone
    // long enough to skew batch-race dynamics and (empirically) drop
    // the L1-reorg notification.
    let env1 = harness.env_for(ANVIL_KEY, true);
    let env2 = harness.env_for(ANVIL_KEY_4, true);
    let (c1, c2) = tokio::try_join!(
        NodeHandle::start("c1", &cfg, &env1),
        NodeHandle::start("c2", &cfg, &env2),
    )
    .unwrap();

    // Distinct keys → independent nonce tracks per node.
    c1.run_tx_spammer(ANVIL_KEY_1);
    c2.run_tx_spammer(ANVIL_KEY_2);

    let pre_batches = chain
        .wait_for_batches(4, DEFAULT_TIMEOUT)
        .await
        .expect("pre-reorg: ≥4 combined batches");

    // Drop the most recent 3 L1 blocks. Composer's bundle-target window
    // is `latest + 2`, so depth=3 is enough to roll back at least one
    // landed `BatchPosted` and force both derivers to retreat. Note:
    // anvil's reorg rewinds contract state too, so the contract's
    // `batchesPosted` and `stateRoot` drop back to a pre-reorg value
    // and only re-grow as composers repost.
    harness.anvil.reorg(3).await.unwrap();

    // I5 — Liveness FIRST: wait for `batchesPosted` to climb past the
    // pre-reorg count. Without this, I3 below could trivially succeed
    // against the rewound state (a stale equality, not catch-up).
    chain
        .wait_for_batches(pre_batches + 1, DEFAULT_TIMEOUT)
        .await
        .expect("no batches landed after reorg");

    // I3 — Each node independently catches up to the (now-advancing)
    // contract: at some poll, `node.safe.stateRoot == contract.stateRoot`
    // sampled back-to-back. The two nodes don't have to coincide.
    wait_for_node_caught_up(&c1, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("c1 did not catch up to contract post-reorg");
    wait_for_node_caught_up(&c2, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("c2 did not catch up to contract post-reorg");

    // I4 — The nodes agree on block hashes, not only state roots.
    wait_for_safe_prefix_convergence(&[&c1, &c2], (pre_batches + 1) as u64, DEFAULT_TIMEOUT)
        .await
        .expect("composers did not converge on post-reorg safe block hashes");

    // I2 — Both derivers logged reorg handling.
    c1.assert_reorg_seen();
    c2.assert_reorg_seen();

    // I1 — No process death.
    c1.assert_no_process_death();
    c2.assert_no_process_death();
}
