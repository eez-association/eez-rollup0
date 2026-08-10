//! End-to-end: anvil L1 + bundle stub + deployed protocol + spawned eez-node.
//!
//! One mega happy-path test asserts every observable invariant of the
//! Builder mode (the only mode eez-rollup0 supports today). Three
//! minimal failure tests exercise one revert path each. See
//! eez-rollup0-testing-sota.md for the pattern sources.

use std::time::Duration;

use alloy_primitives::{B256, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::BlockNumberOrTag;

mod common;
use common::{
    ANVIL_ADDR, ANVIL_ADDR_3, ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_3, ANVIL_KEY_4,
    AnvilConfig, Harness, NodeConfig, NodeHandle, block_number_and_hash_at, override_env,
    reorg_genesis_path, reorg_genesis_state_root, send_l2_value_transfer,
    send_l2_value_transfer_confirmed, wait_for, wait_for_latest_height,
    wait_for_new_attested_safe_block, wait_for_safe_chain_contains,
    wait_for_safe_prefix_convergence, wait_for_safe_state,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_mins(3);

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
async fn happy_case_builder_sustained() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let datadir = tempfile::tempdir().unwrap();
    let env = harness.env();

    // Phase 1 — sustained operation under the first node.
    let n_before;
    let root_before;
    let pre_restart_latest;
    {
        let node_before_restart = NodeHandle::spawn(datadir.path(), &env).unwrap();
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

    // Phase 3 — follower full-replay. Spawn a fresh-datadir node in
    // follower mode by keeping L1 env and omitting the proof signer;
    // `Deriver::catch_up` must rebuild state from L1 events alone and
    // land on a stateRoot the contract has attested, and agree with the
    // restarted node's safe block hash.
    let follower_env = harness.follower_env(None);
    let follower = NodeHandle::start("follower", &NodeConfig::default(), &follower_env)
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
}

/// `rollup.id=999` against a registry where only rollup 1 exists.
/// `postAndVerifyBatch` reverts at the structural validation step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_wrong_rollup_id() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let env = harness.env_with_rollup_id(999);
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
}

/// `MockECDSAProofSystem` deployed with signer A; node started with
/// `keys.proof_signer_key` = B. Prover signs with B, on-chain
/// `verify()` recovers B ≠ A and returns false, `postAndVerifyBatch`
/// reverts at the proof-verification step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_prover_signer_mismatch() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let env = harness.env_with_proof_signer(ANVIL_KEY_1);
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
/// **I2 — Both nodes noticed the reorg.** Each node logged either the
/// L1 watcher rewind or deriver retreat marker ≥ 1 time. Crucial because
/// state convergence can happen via unrelated re-derivation paths; this
/// is the assertion that proves the reorg-handling path ran.
///
/// **I3 — Both nodes imported the same post-reorg safe block.** Once
/// one node reaches a safe block whose state root is newly attested after
/// the reorg, the other node's safe chain must contain that exact
/// `(number, hash)`.
///
/// **I4 — Same safe block hash.** The two nodes converge on an identical
/// safe block hash after reorg recovery. This is stronger than stateRoot
/// equality: empty or equivalent-state blocks can still diverge by hash.
///
/// **I5 — Liveness.** Post-reorg batches landed (`batches_posted` grew
/// past the pre-reorg snapshot). Proves the chain didn't freeze on the
/// rewound state.
// Known-failing pending a fix: after an L1 reorg re-grows the chain, defer-on-lateness
// holds the pool every slot so the composer never reposts (fail-closed, reorg-path only).
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
    let (c1_pre_reorg_latest, c2_pre_reorg_latest) = tokio::try_join!(
        wait_for_latest_height(&c1, 1, DEFAULT_TIMEOUT),
        wait_for_latest_height(&c2, 1, DEFAULT_TIMEOUT),
    )
    .expect("pre-reorg: both composers should have produced L2 blocks");
    let post_reorg_target_height = c1_pre_reorg_latest.number.min(c2_pre_reorg_latest.number) + 1;
    let pre_reorg_states = chain.executed_states().await.unwrap();

    // Drop the most recent 3 L1 blocks. Composer's bundle-target window
    // is `latest + 2`, so depth=3 is enough to roll back at least one
    // landed `BatchPosted` and force both derivers to retreat. Note:
    // anvil's reorg rewinds contract state too, so the contract's
    // `batchesPosted` and `stateRoot` drop back to a pre-reorg value
    // and only re-grow as composers repost.
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

    // I5 — Liveness FIRST: wait for `batchesPosted` to climb past the
    // pre-reorg count. Without this, I3 below could trivially succeed
    // against the rewound state (a stale equality, not catch-up).
    chain
        .wait_for_batches(pre_batches + 1, DEFAULT_TIMEOUT)
        .await
        .expect("no batches landed after reorg");
    tokio::try_join!(
        wait_for_latest_height(&c1, post_reorg_target_height, DEFAULT_TIMEOUT),
        wait_for_latest_height(&c2, post_reorg_target_height, DEFAULT_TIMEOUT),
    )
    .expect("composers did not advance L2 height after reorg");

    // I3 — Both nodes import the same post-reorg safe block. Either
    // composer may import the newly attested block first after the L1 reorg.
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

    // I4 — The nodes agree on block hashes, not only state roots.
    wait_for_safe_prefix_convergence(&[&c1, &c2], post_reorg_target_height, DEFAULT_TIMEOUT)
        .await
        .expect("composers did not converge on post-reorg safe block hashes");

    // I2 — Both nodes observed the reorg.
    c1.wait_for_reorg_seen(DEFAULT_TIMEOUT).await.unwrap();
    c2.wait_for_reorg_seen(DEFAULT_TIMEOUT).await.unwrap();

    // I1 — No process death.
    c1.assert_no_process_death();
    c2.assert_no_process_death();
}

/// Helper: spawn the unified `eez-node` binary in follower mode with a
/// fresh datadir. `seq_rpc = Some(_)` sets `EEZ_SEQUENCER_RPC` for
/// unsafe-head following; `None` runs L1-derived-only mode.
async fn spawn_follower(
    name: &str,
    harness: &Harness,
    seq_rpc: Option<&str>,
) -> anyhow::Result<NodeHandle> {
    let env = harness.follower_env(seq_rpc);
    let cfg = NodeConfig::default();
    NodeHandle::start(name, &cfg, &env).await
}

/// Unified `eez-node` in follower mode, L1-derived only (no
/// `EEZ_SEQUENCER_RPC`). The sequencer posts batches; the follower's
/// Deriver alone must rebuild state and land on a contract-attested
/// stateRoot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_l1_derived() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env())
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

    let (safe_number, safe_hash) =
        block_number_and_hash_at(&follower.l2_rpc_url(), BlockNumberOrTag::Safe)
            .await
            .unwrap()
            .expect("follower has a safe block");
    assert!(safe_number > 0, "follower safe is genesis");
    let (_, seq_hash) =
        block_number_and_hash_at(&seq.l2_rpc_url(), BlockNumberOrTag::Number(safe_number))
            .await
            .unwrap()
            .expect("sequencer has the follower safe block");
    assert_eq!(
        safe_hash, seq_hash,
        "follower safe block hash must match sequencer at block {safe_number}",
    );

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// Unified `eez-node` follower with `EEZ_SEQUENCER_RPC` pointing at the sequencer.
/// Asserts BOTH paths:
///   - safe head: still reaches a contract-attested stateRoot (the L1
///     deriver is authoritative) and matches the sequencer chain.
///   - unsafe head: sequencer-RPC polling submits a fresh FCU outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_sequencer_rpc() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env())
        .await
        .unwrap();

    let seq_rpc = seq.l2_rpc_url();
    let follower_env = override_env(
        harness.follower_env(Some(&seq_rpc)),
        "RUST_LOG",
        "warn,eez_node::follower=info",
    );
    let follower = NodeHandle::start("follower", &NodeConfig::default(), &follower_env)
        .await
        .unwrap();

    chain
        .wait_for_batches(2, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer landed batches");
    wait_for_safe_state(&follower, &chain, B256::ZERO, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");

    // The follower's safe head must be a real sequencer block.
    common::wait_for(DEFAULT_TIMEOUT, || {
        let seq_rpc = seq_rpc.clone();
        let follower_rpc = follower.l2_rpc_url();
        async move {
            let Some((safe_number, safe_hash)) =
                block_number_and_hash_at(&follower_rpc, BlockNumberOrTag::Safe).await?
            else {
                return Ok(None);
            };
            if safe_number == 0 {
                return Ok(None);
            }
            let Some((_, seq_safe_hash)) =
                block_number_and_hash_at(&seq_rpc, BlockNumberOrTag::Number(safe_number)).await?
            else {
                return Ok(None);
            };

            Ok((safe_hash == seq_safe_hash).then_some(()))
        }
    })
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

/// Unified `eez-node` follower in L1-derived-only mode through an `anvil_reorg`:
/// the follower's deriver must retreat just like the sequencer's, and
/// its safe head must catch up to a post-reorg attestation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_l1_reorg_recovers() {
    let harness = Harness::with_anvil_config(
        AnvilConfig::for_reorg(),
        reorg_genesis_state_root().unwrap(),
    )
    .await
    .unwrap();
    let chain = harness.chain();
    let genesis = reorg_genesis_path();
    let seq_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
    };
    let follower_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
    };
    let seq_env = harness.env();
    let follower_env = harness.follower_env(None);
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

/// Architectural claim: two followers watching *different* sources
/// (one L1-derived-only, one tracking sequencer RPC) still agree on
/// the safe head — the L1 deriver overrides the unsafe-head source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_cross_safe_parity() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env())
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

    // Compare a block inside both followers' safe boundaries, then verify
    // that shared safe block is on the sequencer's chain.
    common::wait_for(DEFAULT_TIMEOUT, || {
        let f_l1_rpc = f_l1.l2_rpc_url();
        let f_seq_rpc = f_seq.l2_rpc_url();
        let seq_rpc = seq_rpc.clone();
        async move {
            let Some((l1_safe_number, _)) =
                block_number_and_hash_at(&f_l1_rpc, BlockNumberOrTag::Safe).await?
            else {
                return Ok(None);
            };
            let Some((seq_follower_safe_number, _)) =
                block_number_and_hash_at(&f_seq_rpc, BlockNumberOrTag::Safe).await?
            else {
                return Ok(None);
            };
            let common_safe_number = l1_safe_number.min(seq_follower_safe_number);
            if common_safe_number == 0 {
                return Ok(None);
            }
            let Some((_, l1_hash)) =
                block_number_and_hash_at(&f_l1_rpc, BlockNumberOrTag::Number(common_safe_number))
                    .await?
            else {
                return Ok(None);
            };
            let Some((_, seq_follower_hash)) =
                block_number_and_hash_at(&f_seq_rpc, BlockNumberOrTag::Number(common_safe_number))
                    .await?
            else {
                return Ok(None);
            };
            let Some((_, sequencer_hash)) =
                block_number_and_hash_at(&seq_rpc, BlockNumberOrTag::Number(common_safe_number))
                    .await?
            else {
                return Ok(None);
            };
            Ok((l1_hash == seq_follower_hash && l1_hash == sequencer_hash).then_some(()))
        }
    })
    .await
    .expect("followers never shared a sequencer safe block");

    f_l1.assert_no_process_death();
    f_seq.assert_no_process_death();
    seq.assert_no_process_death();
}

/// Unified `eez-node` follower with `EEZ_SEQUENCER_RPC` pointing at a
/// *rogue* source: a separate `eez-node` on `--chain dev` (block production
/// on, composer off) serving a different chain as its `latest`, while the
/// honest sequencer posts the real batches to L1. The only follower test
/// where the unsafe source disagrees with L1 — so the only one that proves
/// the deriver is authoritative rather than merely agreeing with an honest
/// source.
/// Asserts:
///   - safe head reaches a non-genesis contract-attested stateRoot
///     (membership excludes the rogue's chain; non-genesis excludes a
///     trivial stuck-at-genesis pass).
///   - the unsafe poll actually processed a rogue head: the follower logs
///     that reth accepted the (body-less, cross-genesis) head as a sync
///     target, so broken wiring can't silently downgrade this to
///     L1-derived-only.
///   - no process death.
///
/// Different-genesis (not a same-chain fork) is deliberate: the follower
/// never fetches the rogue's bodies (no peers — discovery is disabled), so
/// reth sees an unknown head and answers `SYNCING`, which the committer
/// accepts — the deriver advances safe regardless.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_rogue_sequencer_safe_head_holds() {
    let harness = Harness::with_anvil_config(
        AnvilConfig::for_reorg(),
        reorg_genesis_state_root().unwrap(),
    )
    .await
    .unwrap();
    let chain = harness.chain();
    let genesis = reorg_genesis_path();

    // Honest sequencer: real genesis, composer on, real txs so attested
    // states move past genesis.
    let seq_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
    };
    let seq = NodeHandle::start("seq", &seq_cfg, &harness.env())
        .await
        .unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);

    // Rogue source: a standalone *different* chain (`--chain dev`) with no L1
    // env, so it cannot post batches and never converges on the real chain.
    // It only feeds the follower divergent unsafe heads.
    let rogue_env = vec![(
        "RUST_LOG",
        std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string()),
    )];
    let rogue = NodeHandle::start("rogue", &NodeConfig::default(), &rogue_env)
        .await
        .unwrap();

    // Follower: real genesis + L1 deriver, unsafe head pointed at the
    // rogue. `eez_node::follower=info` surfaces the per-head outcome events.
    let follower_env = override_env(
        harness.follower_env(Some(&rogue.l2_rpc_url())),
        "RUST_LOG",
        "warn,eez_node::follower=info",
    );
    let follower_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
    };
    let follower = NodeHandle::start("follower", &follower_cfg, &follower_env)
        .await
        .unwrap();

    // Safe head reaches a real (non-genesis) attested stateRoot despite the rogue.
    wait_for_safe_state(
        &follower,
        &chain,
        reorg_genesis_state_root().unwrap(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("follower safe head did not reach a non-genesis attested stateRoot while on the rogue");

    // Proof the rogue path actually ran (not silently downgraded to L1-derived).
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

/// Unified `eez-node` follower joining late against a deep backlog: the
/// sequencer posts 4 batches *before* the follower exists, so its boot
/// `catch_up` must replay the whole history in one pass (`scan_batches`).
/// Every other test starts the follower after ~2 batches; this is the only
/// one that exercises catch-up at non-trivial depth (the "spin up a new RPC
/// node long after genesis" path). Asserts the follower's safe head reaches a
/// non-genesis contract-attested stateRoot and includes the exact sequencer
/// block hash at the backlog depth captured at join time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_deep_backfill_late_join() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();

    // Honest sequencer with real txs, building a deep backlog on L1.
    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env())
        .await
        .unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);

    // Pile up history BEFORE the follower exists.
    chain
        .wait_for_batches(4, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer did not build a deep backlog");

    // Snapshot how deep the backlog is at join time: the sequencer's
    // L1-derived safe height is exactly the history the follower must
    // replay.
    let (backlog_depth, backlog_hash) =
        block_number_and_hash_at(&seq.l2_rpc_url(), BlockNumberOrTag::Safe)
            .await
            .unwrap()
            .expect("sequencer has a safe block");

    // Fresh follower joins late; its boot catch-up must replay everything.
    let follower = spawn_follower("follower", &harness, None).await.unwrap();

    wait_for_safe_state(&follower, &chain, B256::ZERO, DEFAULT_TIMEOUT)
        .await
        .expect("late-joining follower did not backfill into an attested stateRoot");

    // Prove catch-up replayed the *entire* pre-existing backlog, not just
    // the first batch: the follower's safe chain must include the sequencer's
    // exact block at the depth the chain already had when it joined.
    wait_for_safe_chain_contains(&follower, backlog_depth, backlog_hash, DEFAULT_TIMEOUT)
        .await
        .unwrap_or_else(|_| {
            panic!("follower did not replay full backlog to block {backlog_depth}")
        });

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}
