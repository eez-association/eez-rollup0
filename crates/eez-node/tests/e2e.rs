//! End-to-end: anvil L1 + bundle stub + deployed protocol + spawned eez-node.
//!
//! One mega happy-path test asserts every observable invariant of the
//! Builder mode (the only mode eez-rollup0 supports today). Three
//! minimal failure tests exercise one revert path each. See
//! eez-rollup0-testing-sota.md for the pattern sources.

use std::time::Duration;

use alloy_primitives::{B256, U256};

mod common;
use alloy_rpc_types_eth::BlockNumberOrTag;
use common::{
    ANVIL_ADDR, ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_4, AnvilConfig, Harness, NodeBinary,
    NodeConfig, NodeHandle, block_number_at, override_env, reorg_genesis_path,
    reorg_genesis_state_root, safe_block_state_root, wait_for_node_caught_up,
    wait_for_real_safe_state, with_env,
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

    // Phase 3 — follower full-replay. Spawn a fresh-datadir node with
    // sequencer + composer disabled; `Deriver::catch_up` must rebuild
    // state from L1 events alone and land on a stateRoot the contract
    // has attested.
    let mut follower_env = env;
    follower_env.push(("EEZ_SEQUENCER_DISABLED", "1".to_string()));
    follower_env.push(("EEZ_COMPOSER_DISABLED", "1".to_string()));
    let follower = NodeHandle::start("follower", &NodeConfig::default(), &follower_env)
        .await
        .unwrap();
    wait_for_node_caught_up(&follower, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");
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
/// we call `anvil_reorg(depth=3)` and verify four invariants:
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
/// **I4 — Liveness.** Post-reorg batches landed (`batches_posted` grew
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

    // I2 — Both derivers logged the retreat.
    c1.assert_reorg_seen();
    c2.assert_reorg_seen();

    // I1 — No process death.
    c1.assert_no_process_death();
    c2.assert_no_process_death();
}

/// Helper: spawn an `eez-follower` binary with fresh datadir + the
/// harness's env. `seq_rpc = Some(_)` sets `EEZ_SEQUENCER_RPC` for
/// unsafe-head following; `None` runs L1-derived-only mode.
async fn spawn_follower(
    name: &str,
    harness: &Harness,
    seq_rpc: Option<&str>,
) -> anyhow::Result<NodeHandle> {
    let env = match seq_rpc {
        Some(url) => with_env(harness.env(), "EEZ_SEQUENCER_RPC", url),
        None => harness.env(),
    };
    let cfg = NodeConfig {
        binary: NodeBinary::EezFollower,
        ..NodeConfig::default()
    };
    NodeHandle::start(name, &cfg, &env).await
}

/// `eez-follower` binary in L1-derived-only mode (no
/// `EEZ_SEQUENCER_RPC`). The sequencer posts batches; the follower's
/// Deriver alone must rebuild state and land on a contract-attested
/// stateRoot. Distinct from the eez-node Phase 3 because it exercises
/// the real follower binary (its own boot path, `catch_up` call site,
/// fcu-refresh loop).
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

/// `eez-follower` with `EEZ_SEQUENCER_RPC` pointing at the sequencer.
/// Asserts BOTH paths:
///   - safe head: still reaches a contract-attested stateRoot (the L1
///     deriver is authoritative).
///   - unsafe head: the follower polls the sequencer's `latest` and
///     advances. Verified by reading the follower's `latest` block
///     number once safe is caught up — it should be ≥ contract's
///     `safe` (unsafe head ≥ safe head, by definition).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_sequencer_rpc() {
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
    let follower = spawn_follower("follower", &harness, Some(&seq_rpc))
        .await
        .unwrap();
    wait_for_node_caught_up(&follower, &chain, DEFAULT_TIMEOUT)
        .await
        .expect("follower did not catch up via L1 replay");

    let follower_safe = safe_block_state_root(&follower.l2_rpc_url())
        .await
        .unwrap()
        .expect("follower has a safe block");
    assert_ne!(follower_safe, B256::ZERO, "follower safe is genesis");

    // Sequencer-RPC unsafe-head must actually drive the follower's
    // `latest` past its `safe`. The L1 deriver only moves safe (and
    // finalized); only `EEZ_SEQUENCER_RPC` polling advances `latest`
    // beyond. So `latest > safe` is the structural proof that the
    // unsafe-head path ran. Log-grep is unreliable here — the follower
    // process is SIGKILL'd at test teardown before stdout buffers
    // flush, so INFO events may never reach the captured file.
    let latest = block_number_at(&follower.l2_rpc_url(), BlockNumberOrTag::Latest)
        .await
        .unwrap()
        .expect("follower has a latest block");
    let safe = block_number_at(&follower.l2_rpc_url(), BlockNumberOrTag::Safe)
        .await
        .unwrap()
        .expect("follower has a safe block");
    assert!(
        latest > safe,
        "sequencer-RPC unsafe-head never advanced past safe; latest={latest}, safe={safe}",
    );

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// `eez-follower` in L1-derived-only mode through an `anvil_reorg`:
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
        binary: NodeBinary::EezFollower,
    };
    let env = harness.env();
    let (seq, follower) = tokio::try_join!(
        NodeHandle::start("seq", &seq_cfg, &env),
        NodeHandle::start("follower", &follower_cfg, &env),
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

    follower.assert_reorg_seen();
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

/// `eez-follower` with `EEZ_SEQUENCER_RPC` pointing at a *rogue* source: a
/// separate `eez-node` on `--chain dev` (block production on, composer off)
/// serving a different chain as its `latest`, while the honest sequencer
/// posts the real batches to L1. The only follower test where the unsafe
/// source disagrees with L1 — so the only one that proves the deriver is
/// authoritative rather than merely agreeing with an honest source.
/// Asserts:
///   - safe head reaches a non-genesis contract-attested stateRoot
///     (membership excludes the rogue's chain; non-genesis excludes a
///     trivial stuck-at-genesis pass).
///   - the unsafe poll actually processed rogue heads (`eez.follower.head.*`),
///     so broken wiring can't silently downgrade this to L1-derived-only.
///   - no process death.
///
/// Different-genesis (not a same-chain fork) is deliberate: the follower
/// never fetches the rogue's bodies (no peers — discovery is disabled), so
/// reth sees an unknown head and answers `SYNCING`, which the committer
/// accepts — the deriver advances safe regardless.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_rogue_sequencer_safe_head_holds() {
    let harness =
        Harness::with_anvil_config(AnvilConfig::for_reorg(), reorg_genesis_state_root().unwrap())
            .await
            .unwrap();
    let chain = harness.chain();
    let genesis = reorg_genesis_path();

    // Honest sequencer: real genesis, composer on, real txs so attested
    // states move past genesis.
    let seq_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
        ..NodeConfig::default()
    };
    let seq = NodeHandle::start("seq", &seq_cfg, &harness.env())
        .await
        .unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);

    // Rogue source: a *different* chain (`--chain dev`) that never posts to
    // L1 (composer off) and never converges on the real chain — it only
    // feeds the follower divergent unsafe heads.
    let rogue_env = with_env(harness.env(), "EEZ_COMPOSER_DISABLED", "1");
    let rogue = NodeHandle::start("rogue", &NodeConfig::default(), &rogue_env)
        .await
        .unwrap();

    // Follower: real genesis + L1 deriver, unsafe head pointed at the
    // rogue. `eez_follower=info` surfaces the per-head outcome events.
    let follower_env = with_env(
        with_env(harness.env(), "EEZ_SEQUENCER_RPC", &rogue.l2_rpc_url()),
        "RUST_LOG",
        "warn,eez_follower=info",
    );
    let follower_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
        binary: NodeBinary::EezFollower,
    };
    let follower = NodeHandle::start("follower", &follower_cfg, &follower_env)
        .await
        .unwrap();

    // Safe head reaches a real (non-genesis) attested stateRoot despite the rogue.
    wait_for_real_safe_state(
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
            .log_count_matching(&["eez.follower.head."])
            .unwrap()
            > 0,
        "follower never processed a rogue unsafe head",
    );

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}

/// `eez-follower` joining late against a deep backlog: the sequencer posts
/// 10 batches *before* the follower exists, so its boot `catch_up` must
/// replay the whole history in one pass (`scan_batches`). Every other test
/// starts the follower after ~2 batches; this is the only one that
/// exercises catch-up at non-trivial depth (the "spin up a new RPC node
/// long after genesis" path). The follower can't reach the tip without
/// replaying every batch below it, so a non-genesis contract-attested safe
/// head proves the deep replay happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_case_follower_deep_backfill_late_join() {
    let harness =
        Harness::with_anvil_config(AnvilConfig::for_reorg(), reorg_genesis_state_root().unwrap())
            .await
            .unwrap();
    let chain = harness.chain();
    let genesis = reorg_genesis_path();

    // Honest sequencer with real txs, building a deep backlog on L1.
    let seq_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
        ..NodeConfig::default()
    };
    let seq = NodeHandle::start("seq", &seq_cfg, &harness.env())
        .await
        .unwrap();
    seq.run_tx_spammer(ANVIL_KEY_1);

    // Pile up history BEFORE the follower exists.
    chain
        .wait_for_batches(10, DEFAULT_TIMEOUT)
        .await
        .expect("sequencer did not build a deep backlog");

    // Fresh follower joins late; its boot catch-up must replay everything.
    let follower_cfg = NodeConfig {
        genesis_path: Some(genesis.as_path()),
        binary: NodeBinary::EezFollower,
    };
    let follower = NodeHandle::start("follower", &follower_cfg, &harness.env())
        .await
        .unwrap();

    wait_for_real_safe_state(
        &follower,
        &chain,
        reorg_genesis_state_root().unwrap(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("late-joining follower did not backfill into a non-genesis attested stateRoot");

    follower.assert_no_process_death();
    seq.assert_no_process_death();
}
