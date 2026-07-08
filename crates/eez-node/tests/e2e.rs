//! End-to-end: anvil L1 + bundle stub + deployed protocol + spawned eez-node.
//!
//! One mega happy-path test asserts every observable invariant of the
//! Builder mode (the only mode eez-rollup0 supports today). Three
//! minimal failure tests exercise one revert path each. See
//! eez-rollup0-testing-sota.md for the pattern sources.

use std::time::Duration;

use alloy_primitives::{B256, U256};
use alloy_rpc_types_eth::BlockNumberOrTag;

mod common;
use common::{
    ANVIL_ADDR, ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_4, AnvilConfig, Harness, NodeConfig,
    NodeHandle, block_number_and_hash_at, reorg_genesis_path, reorg_genesis_state_root,
    safe_block_state_root, wait_for_node_caught_up,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

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
        n_before = chain.wait_for_batches(3, DEFAULT_TIMEOUT).await.unwrap();
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
        .wait_for_batches(n_before + 1, DEFAULT_TIMEOUT)
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

    // Phase 3 — follower full-replay. Spawn a fresh-datadir node in
    // follower mode by keeping L1 env and omitting the proof signer;
    // `Deriver::catch_up` must rebuild state from L1 events alone and
    // land on a stateRoot the contract has attested.
    let follower_env = harness.follower_env(None);
    let follower = NodeHandle::start("follower", &NodeConfig::default(), &follower_env)
        .await
        .unwrap();
    wait_for_node_caught_up(&follower, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");
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
/// we call `anvil_reorg(depth=3)` and verify four invariants:
///
/// **I1 — No process death.** Neither node logs `Fatal` /
/// `UnexpectedStaticFile`. Without this every other check is moot.
///
/// **I2 — Both nodes noticed the reorg.** Each node logged either the
/// L1 watcher rewind or deriver retreat marker ≥ 1 time. Crucial because
/// state convergence can happen via unrelated re-derivation paths; this
/// is the assertion that proves the reorg-handling path ran.
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
/// **I4 — Liveness.** Post-reorg batches landed (`batches_posted` grew
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
        ..NodeConfig::default()
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

    // I4 — Liveness FIRST: wait for `batchesPosted` to climb past the
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
    wait_for_node_caught_up(&follower, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// Unified `eez-node` follower with `EEZ_SEQUENCER_RPC` pointing at the sequencer.
/// Asserts BOTH paths:
///   - safe head: still reaches a contract-attested stateRoot (the L1
///     deriver is authoritative).
///   - latest head: the follower's public head stays on the sequencer's
///     chain while the safe head remains contract-attested.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_sequencer_rpc() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let seq = NodeHandle::start("seq", &NodeConfig::default(), &harness.env())
        .await
        .unwrap();

    let seq_rpc = seq.l2_rpc_url();
    let follower = spawn_follower("follower", &harness, Some(&seq_rpc))
        .await
        .unwrap();

    chain
        .wait_for_batches(2, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer landed batches");
    wait_for_node_caught_up(&follower, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");

    let follower_safe = safe_block_state_root(&follower.l2_rpc_url())
        .await
        .unwrap()
        .expect("follower has a safe block");
    assert_ne!(follower_safe, B256::ZERO, "follower safe is genesis");

    // Assert the follower's public head directly over JSON-RPC instead
    // of relying on process logs: it must be a real sequencer block,
    // and the safe head must not outrun it.
    common::wait_for(DEFAULT_TIMEOUT, || {
        let seq_rpc = seq_rpc.clone();
        let follower_rpc = follower.l2_rpc_url();
        async move {
            let Some((latest_number, latest_hash)) =
                block_number_and_hash_at(&follower_rpc, BlockNumberOrTag::Latest).await?
            else {
                return Ok(None);
            };
            let Some((safe_number, _)) =
                block_number_and_hash_at(&follower_rpc, BlockNumberOrTag::Safe).await?
            else {
                return Ok(None);
            };
            let Some((_, seq_hash)) =
                block_number_and_hash_at(&seq_rpc, BlockNumberOrTag::Number(latest_number)).await?
            else {
                return Ok(None);
            };

            Ok(
                (latest_number > 0 && latest_number >= safe_number && latest_hash == seq_hash)
                    .then_some(()),
            )
        }
    })
    .await
    .expect("follower latest block never matched the sequencer chain");

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
        ..NodeConfig::default()
    };
    let follower_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
        ..NodeConfig::default()
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
    harness.anvil.reorg(3).await.unwrap();
    chain
        .wait_for_batches(pre_batches + 1, DEFAULT_TIMEOUT)
        .await
        .expect("no batches landed after reorg");
    wait_for_node_caught_up(&follower, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up post-reorg");

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
    wait_for_node_caught_up(&f_l1, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("f_l1 did not catch up");
    wait_for_node_caught_up(&f_seq, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("f_seq did not catch up");

    // The actual parity claim: poll both followers' safe.stateRoot
    // until they agree at the same instant. The contract keeps
    // advancing, so we can't compare at arbitrary moments — we wait
    // for a tick where both followers' safe heads coincide.
    common::wait_for(DEFAULT_TIMEOUT, || async {
        let a = safe_block_state_root(&f_l1.l2_rpc_url())
            .await
            .ok()
            .flatten();
        let b = safe_block_state_root(&f_seq.l2_rpc_url())
            .await
            .ok()
            .flatten();
        Ok(match (a, b) {
            (Some(x), Some(y)) if x == y && x != B256::ZERO => Some(()),
            _ => None,
        })
    })
    .await
    .expect("followers never agreed on safe head");

    f_l1.assert_no_process_death();
    f_seq.assert_no_process_death();
    seq.assert_no_process_death();
}
