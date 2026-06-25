//! VALUE-BEARING inbound (L1→L2) cross-chain E2E — the value-carrying
//! variant of `e2e_inbound.rs`. The user attaches `msg.value = V` on L1; V
//! must (a) be escrowed on L1 (the rollup's `etherBalance += V`) and (b) be
//! delivered to the L2 target.
//!
//! Identical bring-up + flow as `e2e_inbound.rs`, with three deltas:
//!   - target = `ValuePayable` (the plain `Value` reverts on incoming ETH);
//!   - the inbound user tx carries `value = DEPOSIT_WEI` (was 0);
//!   - a new acceptance (a2): the L2 target's ETH balance rose by exactly V.
//!
//! The value path is the crux this guards. On L1 the bundled user tx's
//! `executeCrossChainCall` consume enforces EEZ's per-entry invariant
//! `totalEtherDelta == _entryEtherIn - etherOut` (= `+V == V - 0`), so the
//! composer MUST book `etherDelta = +V` on the lean inbound entry's
//! settlement delta — if it left it 0 (the value-free path), the consume
//! reverts (EtherDeltaMismatch) and the inbound never settles. On L2 the
//! delivery system tx attaches V (sourced from the pre-funded SYSTEM_ADDRESS,
//! amount from the sidecar's `l2ToL1Calls[0].value`) and transfers it to the
//! target. The fresh follower re-deriving the settled root from L1 alone
//! proves the value delivery is byte-identical on both sides.
//!
//! Bring-up (Phase A placeholder → Phase B restart) + follower demotion are
//! byte-for-byte the same as `e2e_inbound.rs` / `e2e_outbound.rs`; see those
//! files for the deploy-ordering / persistence-window / Mode::Follower
//! rationale (not re-explained here).

use std::time::Duration;

use alloy_primitives::{Address, U256, address};

mod common;
use common::{
    ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_3, EmbeddedL1, L1Chain, NodeConfig, NodeHandle,
    PLACEHOLDER_ADDRESS, block_state_root_at, create_l2_cross_chain_proxy, cross_chain_env,
    deploy_contracts_with_initial, deploy_value_payable, eth_get_balance,
    fresh_cross_chain_genesis, l1_block_number, proxy_original_address, read_chain_id, read_value,
    read_value_at_block, remove_env, safe_block_number_and_root, safe_block_state_root,
    send_inbound_set_value, state_root, wait_for, wait_for_l1_blocks, wait_for_l2_rpc,
    wait_for_rpc_down, with_inbound_source_chain_ids,
};

/// Embedded reth bring-up + a fresh genesis materialization can take a
/// while; be generous (design: 120–180s).
const L1_BOOT_TIMEOUT: Duration = Duration::from_secs(180);
const BATCH_TIMEOUT: Duration = Duration::from_secs(180);

/// Inbound settle round-trip (ingress hold → drain → loadTable +
/// `executeIncomingCrossChainCall` in the Sync block → postBatch on L1 →
/// L2 root settles) takes a few slots behind the scheduler; be generous.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(150);

/// A fresh-datadir follower must boot reth, subscribe to the embedded L1,
/// catch_up from the deploy block (re-deriving every settled batch incl.
/// the inbound one), and land its safe head on the attested settled root.
const FOLLOWER_TIMEOUT: Duration = Duration::from_secs(180);

/// The INBOUND user tx sets the L2 `ValuePayable` to this.
const INBOUND_VALUE: u64 = 42;

/// ETH (wei) the INBOUND user ATTACHES on L1 (`msg.value`) — the deposit.
/// The L1 proxy forwards it into `executeCrossChainCall` (`_entryEtherIn`),
/// the composer books `etherDelta = +V` on the lean entry (crediting the
/// rollup's L1 `etherBalance`), and the L2 delivery transfers V to the
/// `ValuePayable` target (sourced from the pre-funded SYSTEM_ADDRESS). A
/// distinctive non-round value so the balance assertions are unambiguous.
/// 0.5 ETH + 7 wei.
const DEPOSIT_WEI: u128 = 500_000_000_000_000_007;

/// The L2 fixture genesis (`reorg_genesis_path`) pins `chainId = 1`.
/// Sanity-asserted at runtime so the inbound L1-chain-id signal is
/// genuinely distinct from the L2 chain (== the ingress mismatch the
/// classifier keys on).
const L2_CHAIN_ID: u64 = 1;

/// `EEZL2.createCrossChainProxy(L2Value, L2_rollupId)` creates an INBOUND
/// (L1→L2) proxy on the **L1** EEZ whose `originalRollupId` is the L2
/// rollup id (the destination of the inbound call). The deployed rollup
/// is registered as id 1.
const L2_ROLLUP_ID: u64 = 1;

/// anvil#2 (`0x3C44…`) — deploys `Value(0)` on L2 AND creates the L1
/// proxy. Distinct from the L2 SYSTEM_ADDRESS (anvil#0) so neither tx
/// interleaves the composer's `loadExecutionTable` system-tx nonce
/// stream, and distinct from the inbound sender (anvil#3) so the L1
/// nonce streams (proxy creation vs the user tx) never collide.
const PROXY_CREATOR_ADDR: Address = address!("0x3C44Cdddb6a900fa2b585dD299E03D12FA4293bC");

/// anvil#3 (`0x90F7…`) — the INBOUND user (signs the L1-chain-id
/// `setValue(42)` tx). A funded L1 EOA (the embedded reth `--dev` genesis
/// funds the full hardhat set). Its L1 nonce is touched by no other phase
/// of this test, so it can't collide. Distinct from the proxy creator
/// (anvil#2).
const INBOUND_SENDER_ADDR: Address = address!("0x90F79bf6EB2c4f870365E785982E1f101E93b906");

/// S2+S3+S4 — the full INBOUND (L1→L2) acceptance + follower re-derivation.
///
/// **S2** (Phase-A window): deploy `Value(0)` on the node's **L2**, create
/// the **L1** cross-chain proxy `P_L1 =
/// EEZ.createCrossChainProxy(L2Value, L2_rollupId)`, restart the node
/// (Phase B) with `EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS=<embedded L1 chain
/// id>` so the ingress classifier tags the L1-signed tx INBOUND. Assert
/// `P_L1` is non-zero and `authorizedProxies(P_L1).originalAddress ==
/// L2Value`.
///
/// **S3**: send the INBOUND user tx (L1-chain-id, `to = P_L1`,
/// `setValue(42)`, from a funded L1 EOA) to the L2 ingress; assert
/// (a) `Value.value() == 42` on **L2** (the inbound delivery executed via
/// `executeIncomingCrossChainCall`) and (b) `rollups[1].stateRoot` on L1
/// EEZ == the L2 safe-block state root (the delivery-inclusive settled
/// root).
///
/// **S4** (follower re-derivation): with the composer still ALIVE, spawn a
/// fresh-datadir FOLLOWER (proof-signer removed → `Mode::Follower`, L1 RPC
/// → the composer's embedded L1) and assert it re-derives the inbound
/// settled L2 root from L1 ALONE — the byte-equality proof that the
/// deriver's `build_inbound_system_txs` reconstruction is identical to the
/// composer's emit. Belt-and-suspenders: scrape the deriver log for the
/// inbound reconstruction markers (`reconcile_batch_blocks entered` +
/// `cross_chain=true` / `fetching postBatch entries` / `built outbound load
/// + inbound delivery system txs` with `inbound>0`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn value_inbound_deposit_settles_on_l2_and_follower_rederives() {
    let l2_genesis = fresh_cross_chain_genesis().expect("fresh cross-chain genesis");
    let cfg = NodeConfig {
        genesis_path: Some(l2_genesis.path.as_path()),
        ..Default::default()
    };

    let l1 = EmbeddedL1::alloc().expect("alloc embedded L1");
    let l2_datadir = tempfile::tempdir().expect("l2 datadir");

    let rollup_id = 1u64;

    // ── Phase A — placeholder registry (see e2e_outbound.rs S1). ──
    let phase_a_env = cross_chain_env(
        &l1,
        ANVIL_KEY_1, // poster = anvil#1 (≠ deploy/proof-signer anvil#0)
        PLACEHOLDER_ADDRESS,
        PLACEHOLDER_ADDRESS,
        0,
        rollup_id,
        None, // no proxy env (inbound uses the source-chain-id signal, added in Phase B)
    );
    let node_a = NodeHandle::spawn_with("composer-a", l2_datadir.path(), &cfg, &phase_a_env)
        .expect("spawn phase-A node");

    // Embedded L1 RPC up.
    wait_for_l2_rpc(&l1.rpc_url, L1_BOOT_TIMEOUT)
        .await
        .expect("embedded L1 RPC did not come up");

    // ── Deploy the protocol onto the embedded L1 (anvil#0 = proof
    // signer; see e2e_outbound.rs). `initialState` = cross-chain genesis
    // state root.
    let dep = deploy_contracts_with_initial(&l1.rpc_url, ANVIL_KEY, l2_genesis.state_root)
        .await
        .expect("deploy protocol onto embedded L1");
    assert_eq!(
        dep.rollup_id, L2_ROLLUP_ID,
        "registered rollup id changed; the L1 proxy's originalRollupId must equal it",
    );

    // The embedded L1's chain id — the INBOUND classifier signal AND the
    // chain id the inbound user tx is signed for. `l1_env` pins
    // `EEZ_L1_CHAIN_ID=1337`; assert the live value matches so the
    // classifier var and the tx signature agree with reality.
    let l1_chain_id = read_chain_id(&l1.rpc_url)
        .await
        .expect("eth_chainId on the embedded L1");
    assert_ne!(
        l1_chain_id, L2_CHAIN_ID,
        "embedded L1 chain id must differ from the L2 chain id — the mismatch IS the inbound signal",
    );
    eprintln!("[S2] embedded L1 chain id = {l1_chain_id} (≠ L2 chain id {L2_CHAIN_ID})");

    // ── S2.a — deploy `Value(0)` on the node's L2 (the inbound
    // settlement target). The L2 RPC serves even under a placeholder L1
    // registry (only the composer's postBatch path is idle). Deployer =
    // anvil#2 (funded in the L2 genesis fixture; ≠ SYSTEM_ADDRESS anvil#0,
    // ≠ inbound sender anvil#3).
    let l2_rpc = node_a.l2_rpc_url();
    wait_for_l2_rpc(&l2_rpc, L1_BOOT_TIMEOUT)
        .await
        .expect("Phase A L2 RPC did not come up");
    // Sanity: the L2 RPC really is the chainId-1 fixture (so the inbound
    // L1-chain-id tx is genuinely cross-chain).
    let l2_chain_id = read_chain_id(&l2_rpc).await.expect("eth_chainId on L2");
    assert_eq!(
        l2_chain_id, L2_CHAIN_ID,
        "L2 fixture chainId changed; the inbound tx's L1 chain id must differ from it",
    );

    // PAYABLE Value variant — the plain `Value` reverts on incoming ETH, so a
    // value-bearing inbound delivery would fail. `ValuePayable` accepts the
    // deposited V via a `payable setValue` + `receive()`.
    let l2_value_addr = deploy_value_payable(&l2_rpc, ANVIL_KEY_2, 0)
        .await
        .expect("deploy ValuePayable(0) on L2");
    assert_ne!(
        l2_value_addr,
        Address::ZERO,
        "ValuePayable deploy returned the zero address"
    );
    let v0 = read_value(&l2_rpc, l2_value_addr)
        .await
        .expect("read L2 ValuePayable.value() post-deploy");
    assert_eq!(
        v0,
        U256::ZERO,
        "fresh ValuePayable(0) should read 0, got {v0}"
    );
    let bal0 = eth_get_balance(&l2_rpc, l2_value_addr)
        .await
        .expect("read L2 ValuePayable ETH balance post-deploy");
    assert_eq!(
        bal0,
        U256::ZERO,
        "fresh ValuePayable should hold 0 ETH, got {bal0}"
    );
    eprintln!("[S2] L2 ValuePayable deployed @ {l2_value_addr:#x}  value()={v0}  balance={bal0}");

    // ── S2.b — create the INBOUND cross-chain proxy on the L1 EEZ:
    // `EEZ.createCrossChainProxy(L2Value, L2_rollupId)`. `create_l2_cross_-
    // chain_proxy` is contract-generic (EEZ and EEZL2 both inherit the
    // EEZBase cross-chain surface); here it targets the L1 EEZ. Creator =
    // anvil#2 (its L1 nonce is clean — only the Value deploy used it, and
    // that was on L2). The proxy's `originalRollupId` = the L2 rollup id
    // (the destination of the inbound call).
    let p_l1 = create_l2_cross_chain_proxy(
        &l1.rpc_url,
        ANVIL_KEY_2,
        dep.eez_address,
        l2_value_addr,
        L2_ROLLUP_ID,
    )
    .await
    .expect("createCrossChainProxy on L1 EEZ");
    assert_ne!(p_l1, Address::ZERO, "L1 proxy P_L1 is the zero address");
    eprintln!("[S2] L1 cross-chain proxy P_L1 = {p_l1:#x}");

    // ── S2 assertion — the L1 proxy is registered against the L2 Value.
    let registered = proxy_original_address(&l1.rpc_url, dep.eez_address, p_l1)
        .await
        .expect("authorizedProxies(P_L1).originalAddress");
    assert_eq!(
        registered, l2_value_addr,
        "authorizedProxies({p_l1:#x}).originalAddress = {registered:#x}, expected the L2 Value {l2_value_addr:#x}",
    );
    eprintln!("[S2] L1 authorizedProxies(P_L1).originalAddress == L2 Value ✓");

    // ── Bury the L1 deploys + proxy creation deep enough to flush before
    // the Phase-A kill (reth 2-block persistence window; see e2e_outbound.rs).
    let after_deploy = l1_block_number(&l1.rpc_url).await;
    wait_for_l1_blocks(&l1.rpc_url, after_deploy + 6, Duration::from_secs(120))
        .await
        .expect("embedded L1 did not advance to persist the deploy");

    // ── Phase B — restart with the REAL addresses AND the INBOUND
    // source-chain-id env so the classifier tags L1-chain-id txs INBOUND.
    drop(node_a);
    wait_for_rpc_down(&l1.rpc_url, Duration::from_secs(60))
        .await
        .expect("Phase A embedded L1 did not shut down");

    let phase_b_env = with_inbound_source_chain_ids(
        cross_chain_env(
            &l1,
            ANVIL_KEY_1,
            dep.eez_address,
            dep.mock_ps_address,
            dep.deploy_block,
            dep.rollup_id,
            None, // INBOUND: no proxy-address signal; uses source-chain-ids below
        ),
        &[l1_chain_id], // EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS = the embedded L1 chain id
    );
    // Sanity: the inbound signal is actually present.
    assert!(
        phase_b_env
            .iter()
            .any(|(k, v)| *k == "EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS" && v == &l1_chain_id.to_string()),
        "Phase B env missing EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS={l1_chain_id}",
    );
    let node_b = NodeHandle::spawn_with("composer-b", l2_datadir.path(), &cfg, &phase_b_env)
        .expect("spawn phase-B node");
    let l2_rpc_b = node_b.l2_rpc_url();
    wait_for_l2_rpc(&l2_rpc_b, L1_BOOT_TIMEOUT)
        .await
        .expect("Phase B L2 RPC did not come up");
    wait_for_l2_rpc(&l1.rpc_url, L1_BOOT_TIMEOUT)
        .await
        .expect("Phase B embedded L1 RPC did not come up");

    // The L1 proxy survived the restart (same embedded L1 datadir): re-assert.
    let registered_b = proxy_original_address(&l1.rpc_url, dep.eez_address, p_l1)
        .await
        .expect("Phase B authorizedProxies(P_L1)");
    assert_eq!(
        registered_b, l2_value_addr,
        "L1 proxy registration lost across the Phase-A→B restart",
    );

    // ── Wait for the Phase-B composer to start settling (≥1 anchor batch)
    // before firing the inbound tx — proves the embedded L1 + EEZ are live
    // and the composer's postBatch stream is flowing.
    let l1_chain = L1Chain::new(&l1.rpc_url, &dep);
    let batches_before = l1_chain
        .wait_for_batches(1, BATCH_TIMEOUT)
        .await
        .expect("Phase B composer did not settle any anchor batch");
    eprintln!("[S3] Phase B settling; batches so far = {batches_before}");

    // ── S3 — the INBOUND user tx. L1-chain-id (embedded reth `--dev`),
    // to = P_L1, data = setValue(42), from anvil#3 (a funded L1 EOA, ≠
    // proxy creator anvil#2). The nonce is read from the embedded L1 (the
    // ingress gate validates the L1 nonce, not the L2 nonce); the tx is
    // submitted to the L2 ingress.
    assert_ne!(
        PROXY_CREATOR_ADDR, INBOUND_SENDER_ADDR,
        "proxy creator and inbound sender must be distinct EOAs",
    );
    let tx_hash = send_inbound_set_value(
        &l2_rpc_b,
        &l1.rpc_url, // L1 nonce source (the ingress admission gate keys on it)
        ANVIL_KEY_3,
        l1_chain_id,
        p_l1,
        INBOUND_VALUE,
        U256::from(DEPOSIT_WEI), // value-bearing inbound: deposit V on L1
    )
    .await
    .expect("submit value-bearing inbound setValue(42) + deposit to L2 ingress");
    eprintln!(
        "[S3] inbound user tx submitted: chain_id={l1_chain_id} (L1) to={p_l1:#x} \
         data=setValue({INBOUND_VALUE}) value={DEPOSIT_WEI} wei hash={tx_hash:#x}"
    );

    // ── S3 acceptance (a) — Value.value() == 42 on the L2. Poll: the
    // round-trip (hold → drain → executeIncomingCrossChainCall in the Sync
    // block → settle) takes a few slots.
    let settled = wait_for(SETTLE_TIMEOUT, || async {
        let v = read_value(&l2_rpc_b, l2_value_addr).await?;
        Ok((v == U256::from(INBOUND_VALUE)).then_some(v))
    })
    .await;
    let final_value = read_value(&l2_rpc_b, l2_value_addr)
        .await
        .expect("read final L2 Value.value()");

    assert!(
        settled.is_ok(),
        "INBOUND setValue(42) did not settle on L2: Value.value() = {final_value} \
         (expected {INBOUND_VALUE}). The inbound hold/drain or \
         executeIncomingCrossChainCall delivery did not execute the cross-chain call. \
         Inspect the composer log for 'cross-chain tx held' and \
         eez.composer.cc_compose.tx events.",
    );
    eprintln!("[S3] ACCEPTANCE (a): L2 ValuePayable.value() == {final_value} ✓");

    // ── S3 acceptance (a2) — VALUE MOVED: the L2 target's ETH balance rose
    // by exactly the deposited V. The `value()==42` poll already confirmed
    // the delivery executed; the balance proves the ETH the system delivery
    // tx attached (sourced from the pre-funded SYSTEM_ADDRESS, amount taken
    // from the sidecar's l2ToL1Calls[0].value) actually landed at the target.
    // If the lean entry's `etherDelta` were wrong, the bundled L1 user tx's
    // `executeCrossChainCall` consume would have reverted (EtherDeltaMismatch)
    // and the inbound would never have settled — so reaching here AND a
    // matching balance is the end-to-end value proof.
    let l2_bal = eth_get_balance(&l2_rpc_b, l2_value_addr)
        .await
        .expect("read final L2 ValuePayable ETH balance");
    assert_eq!(
        l2_bal,
        U256::from(DEPOSIT_WEI),
        "VALUE-BEARING inbound did not deliver the ETH: L2 ValuePayable balance = {l2_bal} \
         (expected {DEPOSIT_WEI}). The setValue landed (acceptance a) but the deposited V did \
         not reach the target — the system delivery tx's value or the lean entry's etherDelta \
         is wrong.",
    );
    eprintln!("[S3] ACCEPTANCE (a2): L2 ValuePayable ETH balance == {l2_bal} (== deposit V) ✓");

    // ── S3 acceptance (b) — L1 rollups[1].stateRoot == L2 safe-block
    // state root (the delivery-inclusive settled root). Both advance
    // asynchronously; poll for convergence. PIN the (number, root) for the
    // S4 follower height comparison.
    let reconciled = wait_for(SETTLE_TIMEOUT, || async {
        let l1_root = state_root(&l1.rpc_url, dep.eez_address, dep.rollup_id).await?;
        let l2 = safe_block_number_and_root(&l2_rpc_b).await?;
        Ok(match l2 {
            Some((num, root)) if root != alloy_primitives::B256::ZERO && root == l1_root => {
                // DELIVERY-INCLUSIVE guard: the SAFE head lags the live
                // delivery block, so an L1↔L2 match can land on a PRE-delivery
                // anchor root (Value still 0). Pinning that root makes the S4
                // follower re-derive it WITHOUT the inbound reconstruction
                // (inbound=0 → the marker scrape fails). Require Value==42 AT
                // this safe height so we pin the delivery-inclusive settled root
                // — the one whose follower re-derivation MUST walk
                // build_inbound_system_txs.
                match read_value_at_block(&l2_rpc_b, l2_value_addr, num).await {
                    Ok(v) if v == U256::from(INBOUND_VALUE) => Some((l1_root, num, root)),
                    _ => None,
                }
            }
            _ => None,
        })
    })
    .await;
    let (settled_block_number, settled_root) = match reconciled {
        Ok((l1_root, num, l2_root)) => {
            assert_eq!(l1_root, l2_root, "reconciled roots must be equal");
            eprintln!(
                "[S3] ACCEPTANCE (b): L1 rollups[{}].stateRoot == L2 safe root == {l1_root:#x} \
                 (L2 block #{num}) ✓",
                dep.rollup_id
            );
            (num, l2_root)
        }
        Err(e) => {
            let l1_root = state_root(&l1.rpc_url, dep.eez_address, dep.rollup_id)
                .await
                .unwrap_or_default();
            let l2_root = safe_block_state_root(&l2_rpc_b)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            panic!(
                "L1↔L2 root reconciliation failed ({e}): \
                 L1 rollups[{}].stateRoot = {l1_root:#x}, L2 safe root = {l2_root:#x}. \
                 setValue(42) executed on L2 (acceptance a passed) but the tracked \
                 root never matched the L2 safe head — a settled-root mismatch.",
                dep.rollup_id,
            );
        }
    };
    assert_ne!(
        settled_root,
        alloy_primitives::B256::ZERO,
        "S3 settled root is zero — nothing for the follower to re-derive",
    );
    eprintln!(
        "[S4] S3 settled root (the follower's target) = {settled_root:#x} \
         @ L2 block #{settled_block_number}"
    );

    // ─── S4 — FRESH FOLLOWER RE-DERIVES THE INBOUND-SETTLED ROOT ───────
    // Build the follower env from the composer's Phase-B env, then demote
    // it to a follower (see e2e_outbound.rs S4 for the full rationale):
    //   - REMOVE `EEZ_PROOF_SIGNER_KEY` → Mode::Follower.
    //   - Pin `EEZ_L1_EMBEDDED=0`; the L1 URLs already point at the
    //     composer's embedded L1 (the live one we read postBatch from).
    //   - KEEP the cross-chain system-tx context + the INBOUND source-
    //     chain-id env so the deriver's reconcile path runs (cross_chain =
    //     system_tx_cfg.is_some()), and force the deriver target to `info`
    //     so the reconcile markers are emitted.
    let follower_env = {
        let mut e = with_inbound_source_chain_ids(
            cross_chain_env(
                &l1,
                ANVIL_KEY_1,
                dep.eez_address,
                dep.mock_ps_address,
                dep.deploy_block,
                dep.rollup_id,
                None,
            ),
            &[l1_chain_id],
        );
        e = remove_env(e, "EEZ_PROOF_SIGNER_KEY"); // → Mode::Follower
        e = common::override_env(e, "EEZ_L1_EMBEDDED", "0");
        let base_log = std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string());
        e = common::override_env(e, "RUST_LOG", &format!("{base_log},eez_deriver=info"));
        e
    };
    assert!(
        !follower_env
            .iter()
            .any(|(k, _)| *k == "EEZ_PROOF_SIGNER_KEY"),
        "follower env still carries EEZ_PROOF_SIGNER_KEY → would boot as composer",
    );
    let follower_l1_rpc = follower_env
        .iter()
        .find(|(k, _)| *k == "EEZ_L1_RPC_URL")
        .map(|(_, v)| v.clone())
        .expect("follower env missing EEZ_L1_RPC_URL");
    assert_eq!(
        follower_l1_rpc, l1.rpc_url,
        "follower EEZ_L1_RPC_URL must point at the composer's embedded L1",
    );
    eprintln!(
        "[S4] follower env: Mode=Follower (EEZ_PROOF_SIGNER_KEY removed), \
         EEZ_L1_RPC_URL={follower_l1_rpc} (composer's embedded L1), EEZ_L1_EMBEDDED=0"
    );

    let follower_cfg = NodeConfig {
        genesis_path: cfg.genesis_path,
        clean_cwd: true, // .env-free cwd so dotenvy can't re-add the proof signer
    };
    let follower_datadir = tempfile::tempdir().expect("follower datadir");
    let follower = NodeHandle::spawn_with(
        "follower",
        follower_datadir.path(),
        &follower_cfg,
        &follower_env,
    )
    .expect("spawn fresh follower");
    wait_for_l2_rpc(&follower.l2_rpc_url(), L1_BOOT_TIMEOUT)
        .await
        .expect("follower L2 RPC did not come up");

    // The follower re-derives from L1 alone. Two complementary signals
    // (composer + embedded L1 STILL ALIVE → follower's safe head keeps
    // advancing past the inbound block):
    //   (A) HEIGHT-PINNED: canonical block at `settled_block_number` has
    //       state root == `settled_root`.
    //   (B) SAFE-HEAD MATCH: the follower's `safe` head root == settled_root.
    // Either proves byte-identical re-derivation. A height-pinned block
    // PRESENT but with a DIFFERENT root is a HARD failure (real divergence).
    let follower_outcome = wait_for(FOLLOWER_TIMEOUT, || async {
        if let Ok(Some(r)) = block_state_root_at(&follower.l2_rpc_url(), settled_block_number).await
        {
            if r == settled_root {
                return Ok(Some(format!(
                    "height-pinned: block #{settled_block_number} root == settled root"
                )));
            }
            if r != alloy_primitives::B256::ZERO {
                return Err(anyhow::anyhow!(
                    "follower's re-derived block #{settled_block_number} root {r:#x} != \
                     composer's settled root {settled_root:#x}: the deriver's INBOUND \
                     reconstruction (build_inbound_system_txs / delivery system-tx replay) is \
                     NOT byte-identical to the composer's. This is a real inbound derivation gap."
                ));
            }
        }
        if let Ok(Some(root)) = safe_block_state_root(&follower.l2_rpc_url()).await {
            if root == settled_root {
                return Ok(Some(
                    "safe-head: follower safe root == settled root".to_string(),
                ));
            }
        }
        Ok(None)
    })
    .await
    .unwrap_or_else(|e| {
        panic!(
            "FOLLOWER did not re-derive the INBOUND-settled root {settled_root:#x} \
             (inbound height #{settled_block_number}) within {FOLLOWER_TIMEOUT:?}: {e}. \
             A follower that cannot reconstruct an INBOUND batch from L1 alone is a real \
             deriver bug. Inspect the follower log for the deriver reconcile messages \
             ('reconcile_batch_blocks entered' / 'fetching postBatch entries' / \
             'built outbound load + inbound delivery system txs' with inbound≥1).",
        )
    });

    follower.assert_no_process_death();

    eprintln!(
        "[S4] ACCEPTANCE: fresh FOLLOWER re-derived the INBOUND-settled root {settled_root:#x} \
         from L1 ALONE ({follower_outcome}) ✓"
    );

    // Belt-and-suspenders: prove the follower's deriver actually walked the
    // INBOUND reconstruction path (not some unrelated re-convergence). The
    // `event!(name: …)` events render as their MESSAGE strings; scrape the
    // messages emitted by `Deriver::reconcile_batch_blocks`:
    //   - "reconcile_batch_blocks entered" + "cross_chain=true"
    //   - "fetching postBatch entries via L1 RPC" (codec-v1 fallback)
    //   - "built outbound load + inbound delivery system txs" with
    //     "inbound=" ≥ 1 (the INBOUND deferred entries were reconstructed).
    // reth's tracing layer writes ANSI-colored output even to a file, so
    // strip CSI escapes first so `key<ESC…>=<ESC…>value` collapses to a
    // bare `key=value` the marker scrape can substring-match.
    let raw_log = std::fs::read_to_string(&follower.log_path).unwrap_or_default();
    let log = strip_ansi_and_normalize(&raw_log);
    let reconcile_ran = log
        .lines()
        .any(|l| l.contains("reconcile_batch_blocks entered") && l.contains("cross_chain=true"));
    let fetched_entries = log.contains("fetching postBatch entries via L1 RPC");
    let built_inbound = log.lines().any(|l| {
        l.contains("built outbound load + inbound delivery system txs")
            && l.contains("inbound=")
            && !l.contains("inbound=0 ")
            && !l.contains("inbound=0,")
    });
    eprintln!(
        "[S4] follower deriver markers: reconcile(cross_chain)={reconcile_ran} \
         fetch_postBatch_entries={fetched_entries} system_txs_built(inbound>0)={built_inbound}"
    );
    assert!(
        reconcile_ran && fetched_entries && built_inbound,
        "follower's deriver log lacks the INBOUND reconstruction markers \
         (reconcile_batch_blocks entered+cross_chain / fetching postBatch entries / \
         built outbound load … inbound>0). The matching root may have re-converged via \
         an unintended path. Set EEZ_TEST_LOG_DIR + EEZ_TEST_LOG=info and inspect {}.",
        follower.log_path.display(),
    );
}

/// Strip ANSI CSI escape sequences (`ESC [ … <final-byte>`) from a log
/// dump so structured-log field renders like
/// `key<ESC[0m><ESC[2m>=<ESC[0m>value` collapse to a bare `key=value`
/// the marker scrape can substring-match. reth's tracing layer emits
/// color codes even when writing to a file. Pure std (no extra dep).
fn strip_ansi_and_normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for cc in chars.by_ref() {
                    if cc.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}
