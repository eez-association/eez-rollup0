//! Mixed-batch cross-chain E2E — ONE inbound (L1→L2) **and** one outbound
//! (L2→L1) cross-chain user tx settled in the **SAME** Sync slot (A2b).
//!
//! This is the union of `e2e_outbound.rs` and `e2e_inbound.rs`: it stands
//! up the SAME embedded-L1 cross-chain composer, then deploys BOTH targets
//! and BOTH proxies —
//!
//! | | OUTBOUND leg | INBOUND leg |
//! |---|---|---|
//! | `Value` target | **L1** | **L2** |
//! | CrossChainProxy | L2 (`createCrossChainProxy(L1Value, MAINNET=0)`) | L1 (`createCrossChainProxy(L2Value, L2_rollupId)`) |
//! | user tx chain-id | L2 (=1) | L1 (embedded reth `--dev`) |
//! | classifier signal | `EEZ_CROSS_CHAIN_PROXY_ADDRESSES` | `EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS` |
//! | sender | anvil#3 | anvil#4 (distinct EOA) |
//! | settlement effect | L1 `Value.value() == 42` | L2 `Value.value() == 43` |
//!
//! Phase B carries BOTH classifier signals; the two user txs are submitted
//! CONCURRENTLY so the composer's per-slot drain (`pop_n(MAX_USER_TXS_PER_-
//! BUNDLE=3)`) bundles them into ONE Sync slot — the mixed batch. Distinct
//! values (42 vs 43) prove the legs don't cross-wire.
//!
//! **What it guards (A2b).** A mixed slot is the case where the composer's
//! and the deriver's Sync-block construction MUST agree byte-for-byte under
//! the harder constraints: the SYSTEM_ADDRESS nonce stream spans BOTH
//! directions (two-phase: outbound loads, then inbound deliveries) and the
//! Sync-block tx order MUST be the canonical interleave `[load,user,…,
//! deliveries]` (a `loadExecutionTable` self-clean makes any system-first
//! ordering L2-invalid). Both sides now build the slot through the SINGLE
//! shared `eez_evm::system_tx::build_cross_chain_sync_pairs` +
//! `interleave_sync_block_txs`, so equality is structural. The follower
//! re-deriving the mixed root from L1 ALONE — plus a deriver log line that
//! reconstructed BOTH directions in ONE batch (`outbound≥1` AND
//! `inbound≥1`) — is the end-to-end proof.
//!
//! Bring-up (Phase A placeholder → Phase B restart) and the follower
//! demotion are byte-for-byte the same as `e2e_outbound.rs` / `e2e_inbound.rs`;
//! see those files for the deploy-ordering / persistence-window /
//! Mode::Follower rationale (not re-explained here).

use std::time::Duration;

use alloy_primitives::{Address, U256, address};

mod common;
use common::{
    ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_3, ANVIL_KEY_4, CCM_L2_ADDRESS, EmbeddedL1,
    L1Chain, NodeConfig, NodeHandle, PLACEHOLDER_ADDRESS, block_state_root_at,
    create_l2_cross_chain_proxy, cross_chain_env, deploy_contracts_with_initial, deploy_value,
    fresh_cross_chain_genesis, l1_block_number, proxy_original_address, read_chain_id, read_value,
    read_value_at_block, remove_env, safe_block_number_and_root, safe_block_state_root,
    send_inbound_set_value, send_outbound_set_value, state_root, wait_for, wait_for_l1_blocks,
    wait_for_l2_rpc, wait_for_l2_tx_receipt, wait_for_rpc_down, with_inbound_source_chain_ids,
};

const L1_BOOT_TIMEOUT: Duration = Duration::from_secs(180);
const BATCH_TIMEOUT: Duration = Duration::from_secs(180);
/// Mixed round-trip closes both legs in one slot but the L1 settlement +
/// L2 root convergence still lag the scheduler by a few slots; be generous.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);
const FOLLOWER_TIMEOUT: Duration = Duration::from_secs(180);

/// The OUTBOUND user tx sets the **L1** `Value`. Distinct from
/// `INBOUND_VALUE` so a leg crossing wires is caught.
const OUTBOUND_VALUE: u64 = 42;
/// The INBOUND user tx sets the **L2** `Value`.
const INBOUND_VALUE: u64 = 43;

/// L2 fixture chainId (`fresh_cross_chain_genesis`). The inbound L1-chain-id
/// signal must differ from it (== the ingress mismatch the classifier keys on).
const L2_CHAIN_ID: u64 = 1;
/// The deployed rollup is registered as id 1; the L1 inbound proxy's
/// `originalRollupId` (the inbound call destination) must equal it.
const L2_ROLLUP_ID: u64 = 1;
/// The L2 outbound proxy's `originalRollupId` (the settlement destination =
/// mainnet/L1).
const MAINNET_ROLLUP_ID: u64 = 0;

/// anvil#2 (`0x3C44…`) — deploys `Value(0)` on L2 + creates BOTH proxies
/// (the L2 outbound proxy and the L1 inbound proxy, on distinct chains so
/// the nonce streams never collide). Distinct from both user senders
/// (anvil#3 / anvil#4) and the L2 SYSTEM_ADDRESS (anvil#0).
const PROXY_CREATOR_ADDR: Address = address!("0x3C44Cdddb6a900fa2b585dD299E03D12FA4293bC");
/// anvil#3 (`0x90F7…`) — the OUTBOUND user (L2-chain-id `setValue(42)`).
const OUTBOUND_SENDER_ADDR: Address = address!("0x90F79bf6EB2c4f870365E785982E1f101E93b906");
/// anvil#4 (`0x15d3…`) — the INBOUND user (L1-chain-id `setValue(43)`).
/// A DISTINCT EOA from the outbound sender so the two legs share no nonce
/// stream on any chain; both are funded (embedded reth `--dev` on L1, the
/// L2 genesis fixture alloc on L2).
const INBOUND_SENDER_ADDR: Address = address!("0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65");

/// Full MIXED (inbound L1→L2 + outbound L2→L1 in ONE slot) acceptance +
/// follower re-derivation. See the module header for the A2b rationale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_inbound_outbound_same_slot_settles_and_follower_rederives() {
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
        None, // signals added in Phase B
    );
    let node_a = NodeHandle::spawn_with("composer-a", l2_datadir.path(), &cfg, &phase_a_env)
        .expect("spawn phase-A node");

    wait_for_l2_rpc(&l1.rpc_url, L1_BOOT_TIMEOUT)
        .await
        .expect("embedded L1 RPC did not come up");

    // ── Deploy the protocol onto the embedded L1 (anvil#0 = proof signer).
    let dep = deploy_contracts_with_initial(&l1.rpc_url, ANVIL_KEY, l2_genesis.state_root)
        .await
        .expect("deploy protocol onto embedded L1");
    assert_eq!(
        dep.rollup_id, L2_ROLLUP_ID,
        "registered rollup id changed; the L1 inbound proxy's originalRollupId must equal it",
    );

    // The embedded L1 chain id — the INBOUND classifier signal + the chain
    // id the inbound user tx is signed for.
    let l1_chain_id = read_chain_id(&l1.rpc_url)
        .await
        .expect("eth_chainId on the embedded L1");
    assert_ne!(
        l1_chain_id, L2_CHAIN_ID,
        "embedded L1 chain id must differ from the L2 chain id — the mismatch IS the inbound signal",
    );
    eprintln!("[S2] embedded L1 chain id = {l1_chain_id} (≠ L2 chain id {L2_CHAIN_ID})");

    // Distinct EOAs (defensive: the two legs must share no nonce stream).
    assert_ne!(
        OUTBOUND_SENDER_ADDR, INBOUND_SENDER_ADDR,
        "outbound and inbound senders must be distinct EOAs",
    );
    assert_ne!(
        PROXY_CREATOR_ADDR, OUTBOUND_SENDER_ADDR,
        "proxy creator and outbound sender must be distinct EOAs",
    );
    assert_ne!(
        PROXY_CREATOR_ADDR, INBOUND_SENDER_ADDR,
        "proxy creator and inbound sender must be distinct EOAs",
    );

    // ── OUTBOUND target: deploy `Value(0)` on the embedded L1 (anvil#0,
    // sequentially after the protocol deploy → no nonce race).
    let l1_value_addr = deploy_value(&l1.rpc_url, ANVIL_KEY, 0)
        .await
        .expect("deploy Value(0) on embedded L1");
    assert_ne!(
        l1_value_addr,
        Address::ZERO,
        "L1 Value deploy returned zero"
    );
    let l1_v0 = read_value(&l1.rpc_url, l1_value_addr)
        .await
        .expect("read L1 Value.value() post-deploy");
    assert_eq!(
        l1_v0,
        U256::ZERO,
        "fresh L1 Value(0) should read 0, got {l1_v0}"
    );
    eprintln!("[S2] OUTBOUND target: L1 Value @ {l1_value_addr:#x} value()={l1_v0}");

    // ── INBOUND target: deploy `Value(0)` on the node's L2 (anvil#2).
    let l2_rpc = node_a.l2_rpc_url();
    wait_for_l2_rpc(&l2_rpc, L1_BOOT_TIMEOUT)
        .await
        .expect("Phase A L2 RPC did not come up");
    let l2_chain_id = read_chain_id(&l2_rpc).await.expect("eth_chainId on L2");
    assert_eq!(
        l2_chain_id, L2_CHAIN_ID,
        "L2 fixture chainId changed; the inbound tx's L1 chain id must differ from it",
    );
    let l2_value_addr = deploy_value(&l2_rpc, ANVIL_KEY_2, 0)
        .await
        .expect("deploy Value(0) on L2");
    assert_ne!(
        l2_value_addr,
        Address::ZERO,
        "L2 Value deploy returned zero"
    );
    let l2_v0 = read_value(&l2_rpc, l2_value_addr)
        .await
        .expect("read L2 Value.value() post-deploy");
    assert_eq!(
        l2_v0,
        U256::ZERO,
        "fresh L2 Value(0) should read 0, got {l2_v0}"
    );
    eprintln!("[S2] INBOUND target: L2 Value @ {l2_value_addr:#x} value()={l2_v0}");

    // ── OUTBOUND proxy on EEZL2 (0x42..07): createCrossChainProxy(L1Value,
    // MAINNET=0). Creator = anvil#2 (L2 nonce 1, after the L2 Value deploy).
    let proxy_out = create_l2_cross_chain_proxy(
        &l2_rpc,
        ANVIL_KEY_2,
        CCM_L2_ADDRESS,
        l1_value_addr,
        MAINNET_ROLLUP_ID,
    )
    .await
    .expect("createCrossChainProxy on L2 (outbound)");
    assert_ne!(proxy_out, Address::ZERO, "outbound proxy is zero");
    let reg_out = proxy_original_address(&l2_rpc, CCM_L2_ADDRESS, proxy_out)
        .await
        .expect("authorizedProxies(proxy_out).originalAddress");
    assert_eq!(
        reg_out, l1_value_addr,
        "outbound proxy not registered against the L1 Value",
    );
    eprintln!("[S2] OUTBOUND proxy (L2) = {proxy_out:#x} → L1 Value ✓");

    // ── INBOUND proxy on the L1 EEZ: createCrossChainProxy(L2Value,
    // L2_rollupId). Creator = anvil#2 (L1 nonce 0).
    let proxy_in = create_l2_cross_chain_proxy(
        &l1.rpc_url,
        ANVIL_KEY_2,
        dep.eez_address,
        l2_value_addr,
        L2_ROLLUP_ID,
    )
    .await
    .expect("createCrossChainProxy on L1 EEZ (inbound)");
    assert_ne!(proxy_in, Address::ZERO, "inbound proxy is zero");
    let reg_in = proxy_original_address(&l1.rpc_url, dep.eez_address, proxy_in)
        .await
        .expect("authorizedProxies(proxy_in).originalAddress");
    assert_eq!(
        reg_in, l2_value_addr,
        "inbound proxy not registered against the L2 Value",
    );
    eprintln!("[S2] INBOUND proxy (L1) = {proxy_in:#x} → L2 Value ✓");

    // ── Bury the deploys + proxy creations deep enough to flush before the
    // Phase-A kill (reth 2-block persistence window).
    let after_deploy = l1_block_number(&l1.rpc_url).await;
    wait_for_l1_blocks(&l1.rpc_url, after_deploy + 6, Duration::from_secs(120))
        .await
        .expect("embedded L1 did not advance to persist the deploy");

    // ── Phase B — restart with the REAL addresses AND BOTH classifier
    // signals: the outbound proxy address AND the inbound source chain id.
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
            Some(&[proxy_out]), // OUTBOUND signal: EEZ_CROSS_CHAIN_PROXY_ADDRESSES
        ),
        &[l1_chain_id], // INBOUND signal: EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS
    );
    assert!(
        phase_b_env
            .iter()
            .any(|(k, v)| *k == "EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS" && v == &l1_chain_id.to_string()),
        "Phase B env missing the inbound signal EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS={l1_chain_id}",
    );
    assert!(
        phase_b_env
            .iter()
            .any(|(k, _)| *k == "EEZ_CROSS_CHAIN_PROXY_ADDRESSES"),
        "Phase B env missing the outbound signal EEZ_CROSS_CHAIN_PROXY_ADDRESSES",
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

    // Both proxies survived the restart (same datadirs): re-assert.
    let reg_out_b = proxy_original_address(&l2_rpc_b, CCM_L2_ADDRESS, proxy_out)
        .await
        .expect("Phase B authorizedProxies(proxy_out)");
    assert_eq!(
        reg_out_b, l1_value_addr,
        "outbound proxy registration lost across restart"
    );
    let reg_in_b = proxy_original_address(&l1.rpc_url, dep.eez_address, proxy_in)
        .await
        .expect("Phase B authorizedProxies(proxy_in)");
    assert_eq!(
        reg_in_b, l2_value_addr,
        "inbound proxy registration lost across restart"
    );

    // ── Wait for the Phase-B composer to settle ≥1 anchor batch.
    let l1_chain = L1Chain::new(&l1.rpc_url, &dep);
    let batches_before = l1_chain
        .wait_for_batches(1, BATCH_TIMEOUT)
        .await
        .expect("Phase B composer did not settle any anchor batch");
    eprintln!("[S3] Phase B settling; batches so far = {batches_before}");

    // ── S3 — submit BOTH user txs CONCURRENTLY so the composer holds both
    // before the next per-slot drain (`pop_n(3)`) → ONE mixed Sync slot.
    //   OUTBOUND: L2-chain-id, to = proxy_out, setValue(42), from anvil#3.
    //   INBOUND : L1-chain-id, to = proxy_in,  setValue(43), from anvil#4.
    let (out_res, in_res) = tokio::join!(
        send_outbound_set_value(
            &l2_rpc_b,
            ANVIL_KEY_3,
            proxy_out,
            OUTBOUND_VALUE,
            U256::ZERO
        ),
        send_inbound_set_value(
            &l2_rpc_b,
            &l1.rpc_url, // L1 nonce source for the inbound (ingress gates on it)
            ANVIL_KEY_4,
            l1_chain_id,
            proxy_in,
            INBOUND_VALUE,
            U256::ZERO, // value-free inbound
        ),
    );
    let out_hash = out_res.expect("submit outbound setValue(42) to L2 ingress");
    let in_hash = in_res.expect("submit inbound setValue(43) to L2 ingress");
    eprintln!(
        "[S3] submitted CONCURRENTLY — outbound to={proxy_out:#x} hash={out_hash:#x} | \
         inbound to={proxy_in:#x} hash={in_hash:#x}"
    );

    // ── S3 acceptance (a) — BOTH legs settle: L1 Value == 42 (outbound) AND
    // L2 Value == 43 (inbound). Poll until both hold.
    let both_settled = wait_for(SETTLE_TIMEOUT, || async {
        let l1v = read_value(&l1.rpc_url, l1_value_addr).await?;
        let l2v = read_value(&l2_rpc_b, l2_value_addr).await?;
        Ok(
            (l1v == U256::from(OUTBOUND_VALUE) && l2v == U256::from(INBOUND_VALUE))
                .then_some((l1v, l2v)),
        )
    })
    .await;
    let l1_final = read_value(&l1.rpc_url, l1_value_addr)
        .await
        .unwrap_or_default();
    let l2_final = read_value(&l2_rpc_b, l2_value_addr)
        .await
        .unwrap_or_default();
    assert!(
        both_settled.is_ok(),
        "MIXED slot did not settle BOTH legs: L1 Value = {l1_final} (expected {OUTBOUND_VALUE}), \
         L2 Value = {l2_final} (expected {INBOUND_VALUE}). One leg's drain/delivery did not \
         execute — inspect the composer log for the outbound `loadExecutionTable` and the \
         inbound `executeIncomingCrossChainCall` in the SAME Sync block.",
    );
    eprintln!("[S3] ACCEPTANCE (a): L1 Value == {l1_final}, L2 Value == {l2_final} ✓");

    // ── S3 acceptance (c) — the OUTBOUND user tx's own L2 receipt is SUCCESS
    // (regression pin for the RollingHashMismatch fix). The inbound leg lands
    // via a system tx (always status=1), so the outbound user tx is the only
    // one that could revert. Before the lean build_l2_outbound_entry shape its
    // consume re-delivered to a codeless L1 target on L2 → RollingHashMismatch
    // (EEZL2.sol:422), status=0 — while the L1 effect (a) still settled.
    let out_status = wait_for_l2_tx_receipt(&l2_rpc_b, out_hash, SETTLE_TIMEOUT)
        .await
        .expect("outbound user tx never got an L2 receipt");
    assert!(
        out_status,
        "MIXED outbound user tx {out_hash:#x} reverted on L2 (status=0) — expected SUCCESS \
         (RollingHashMismatch from a populated outbound entry; the lean shape fixes it).",
    );
    eprintln!("[S3] ACCEPTANCE (c): outbound user tx L2 receipt status == success ✓");

    // ── S3 acceptance (b) — L1 rollups[1].stateRoot == L2 safe root, with
    // the inbound delivery present at that height (L2 Value == 43). The
    // mixed Sync block carries the outbound user tx + the inbound delivery
    // TOGETHER, so this height is settlement-inclusive for BOTH legs. Pin it
    // for the S4 follower height comparison.
    let reconciled = wait_for(SETTLE_TIMEOUT, || async {
        let l1_root = state_root(&l1.rpc_url, dep.eez_address, dep.rollup_id).await?;
        let l2 = safe_block_number_and_root(&l2_rpc_b).await?;
        Ok(match l2 {
            Some((num, root)) if root != alloy_primitives::B256::ZERO && root == l1_root => {
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
                 (L2 block #{num}, mixed-settlement-inclusive) ✓",
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
                "L1↔L2 root reconciliation failed ({e}): L1 rollups[{}].stateRoot = {l1_root:#x}, \
                 L2 safe root = {l2_root:#x}. Both legs executed (acceptance a passed) but the \
                 tracked root never matched a mixed-settlement-inclusive L2 safe head.",
                dep.rollup_id,
            );
        }
    };
    assert_ne!(
        settled_root,
        alloy_primitives::B256::ZERO,
        "settled root is zero — nothing for the follower to re-derive",
    );
    eprintln!(
        "[S4] mixed settled root (follower target) = {settled_root:#x} @ L2 block #{settled_block_number}"
    );

    // ─── S4 — FRESH FOLLOWER RE-DERIVES THE MIXED-SETTLED ROOT ────────────
    // Follower env = Phase-B env demoted to Mode::Follower (proof signer
    // removed), keeping BOTH cross-chain signals so the deriver's reconcile
    // path runs over the mixed batch. See e2e_inbound.rs S4 for the full
    // rationale.
    let follower_env = {
        let mut e = with_inbound_source_chain_ids(
            cross_chain_env(
                &l1,
                ANVIL_KEY_1,
                dep.eez_address,
                dep.mock_ps_address,
                dep.deploy_block,
                dep.rollup_id,
                Some(&[proxy_out]),
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

    let follower_cfg = NodeConfig {
        genesis_path: cfg.genesis_path,
        clean_cwd: true,
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
                     composer's mixed settled root {settled_root:#x}: the deriver's MIXED-batch \
                     reconstruction (build_cross_chain_sync_pairs → interleave_sync_block_txs) is \
                     NOT byte-identical to the composer's. Real mixed-batch derivation fork."
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
            "FOLLOWER did not re-derive the MIXED-settled root {settled_root:#x} \
             (height #{settled_block_number}) within {FOLLOWER_TIMEOUT:?}: {e}. A follower that \
             cannot reconstruct a MIXED batch from L1 alone is a real deriver bug.",
        )
    });

    follower.assert_no_process_death();
    eprintln!(
        "[S4] ACCEPTANCE: fresh FOLLOWER re-derived the MIXED-settled root {settled_root:#x} \
         from L1 ALONE ({follower_outcome}) ✓"
    );

    // Belt-and-suspenders: prove the follower's deriver reconstructed BOTH
    // directions in ONE batch — a single
    // `built outbound load + inbound delivery system txs` line with
    // `outbound≥1` AND `inbound≥1`. That line is emitted once per
    // `reconcile_batch_blocks`; both counts non-zero on one line ⇒ a genuine
    // mixed batch was re-derived through the shared canonical builder.
    let raw_log = std::fs::read_to_string(&follower.log_path).unwrap_or_default();
    let log = strip_ansi_and_normalize(&raw_log);
    let reconcile_ran = log
        .lines()
        .any(|l| l.contains("reconcile_batch_blocks entered") && l.contains("cross_chain=true"));
    let fetched_entries = log.contains("fetching postBatch entries via L1 RPC");
    let built_mixed = log.lines().any(|l| {
        l.contains("built outbound load + inbound delivery system txs")
            && nonzero_field(l, "outbound=")
            && nonzero_field(l, "inbound=")
    });
    eprintln!(
        "[S4] follower deriver markers: reconcile(cross_chain)={reconcile_ran} \
         fetch_postBatch_entries={fetched_entries} mixed_batch(outbound>0 && inbound>0)={built_mixed}"
    );
    assert!(
        reconcile_ran && fetched_entries && built_mixed,
        "follower's deriver log lacks the MIXED reconstruction markers (reconcile+cross_chain / \
         fetching postBatch entries / a single 'built outbound load + inbound delivery system txs' \
         line with BOTH outbound>0 and inbound>0). Either the two legs did not co-locate in one \
         Sync slot, or the deriver did not reconstruct both. Set EEZ_TEST_LOG_DIR + \
         EEZ_TEST_LOG=info and inspect {}.",
        follower.log_path.display(),
    );
}

/// True iff `line` contains `field` immediately followed by a non-zero
/// integer (e.g. `outbound=1`). Guards against matching `outbound=0`.
fn nonzero_field(line: &str, field: &str) -> bool {
    line.split(field).skip(1).any(|rest| {
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        !digits.is_empty() && digits.chars().any(|c| c != '0')
    })
}

/// Strip ANSI CSI escape sequences from a log dump so structured-log field
/// renders collapse to bare `key=value` the marker scrape can substring-match
/// (reth's tracing layer emits color codes even when writing to a file).
fn strip_ansi_and_normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                chars.next();
                while let Some(&n) = chars.peek() {
                    chars.next();
                    if n.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}
