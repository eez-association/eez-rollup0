//! VALUE-BEARING outbound (L2→L1) WITHDRAWAL cross-chain E2E — the mirror of
//! `e2e_value_inbound.rs`. A user on L2 attaches `msg.value = M`; M is burned
//! on L2 (to SYSTEM_ADDRESS) and PAID OUT on L1 from the rollup's escrowed
//! `etherBalance` to the L1 target.
//!
//! The catch: the rollup's L1 `etherBalance` starts at 0 and there is NO
//! deposit primitive on the contract — it is credited ONLY by a value-bearing
//! INBOUND deposit's `+V` settlement delta. So a withdrawal REQUIRES a prior
//! deposit to fund the reserve. This test runs the full canonical sequence on
//! ONE stack (both targets + both proxies, like `e2e_mixed.rs`):
//!
//!   1. DEPOSIT  (inbound, value = FUND_WEI): credits `etherBalance += FUND_WEI`
//!      and lands FUND_WEI on the L2 deposit target. Wait until settled.
//!   2. WITHDRAW (outbound, value = WITHDRAW_WEI ≤ FUND_WEI): debits
//!      `etherBalance -= WITHDRAW_WEI` and pays WITHDRAW_WEI to the L1 target.
//!
//! Acceptances: the L1 withdraw target's ETH balance rises by WITHDRAW_WEI,
//! the rollup's `etherBalance` ends at `FUND_WEI - WITHDRAW_WEI`, and a fresh
//! follower re-derives the final settled root from L1 alone.
//!
//! The value path this guards (composer.rs outbound splice): an immediate
//! (outbound) entry has `_entryEtherIn == 0`, so EEZ's per-entry invariant
//! `totalEtherDelta == _entryEtherIn - etherOut` forces `etherDelta = -M`. The
//! composer MUST book that debit (via `outbound_ether_out`) — if it left it 0
//! (the value-free path), `etherOut = M` but `totalEtherDelta = 0` and the
//! settlement reverts EtherDeltaMismatch (or, worse, the rollup pays out M it
//! never debited). Targets are `ValuePayable` (the plain `Value` reverts on
//! incoming ETH).
//!
//! Bring-up (Phase A placeholder → Phase B restart) + follower demotion are
//! byte-for-byte the same as `e2e_mixed.rs` / `e2e_inbound.rs`; see those files
//! for the deploy-ordering / persistence-window / Mode::Follower rationale.

use std::time::Duration;

use alloy_primitives::{Address, U256, address};

mod common;
use common::{
    ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_3, ANVIL_KEY_4, CCM_L2_ADDRESS, EmbeddedL1,
    L1Chain, NodeConfig, NodeHandle, PLACEHOLDER_ADDRESS, block_state_root_at,
    create_l2_cross_chain_proxy, cross_chain_env, deploy_contracts_with_initial,
    deploy_value_payable, eth_get_balance, fresh_cross_chain_genesis, l1_block_number,
    proxy_original_address, read_chain_id, read_value, remove_env, rollup_ether_balance,
    safe_block_number_and_root, safe_block_state_root, send_inbound_set_value,
    send_outbound_set_value, state_root, wait_for, wait_for_l1_blocks, wait_for_l2_rpc,
    wait_for_l2_tx_receipt, wait_for_rpc_down, with_inbound_source_chain_ids,
};

const L1_BOOT_TIMEOUT: Duration = Duration::from_secs(180);
const BATCH_TIMEOUT: Duration = Duration::from_secs(180);
/// Mixed round-trip closes both legs in one slot but the L1 settlement +
/// L2 root convergence still lag the scheduler by a few slots; be generous.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);
const FOLLOWER_TIMEOUT: Duration = Duration::from_secs(180);

/// The WITHDRAW (outbound) user tx's `setValue` arg on the **L1** target.
const OUTBOUND_VALUE: u64 = 42;
/// The DEPOSIT (inbound) user tx's `setValue` arg on the **L2** target.
const INBOUND_VALUE: u64 = 43;

/// ETH (wei) the DEPOSIT funds the rollup's L1 `etherBalance` with — the
/// reserve the later withdrawal draws from. Must be ≥ `WITHDRAW_WEI`. 1 ETH.
const FUND_WEI: u128 = 1_000_000_000_000_000_000;
/// ETH (wei) the WITHDRAW pays out from the reserve to the L1 target. A
/// distinctive non-round value < `FUND_WEI` so the balance + reserve-debit
/// assertions are unambiguous (and prove a partial draw). 0.4 ETH + 9 wei.
const WITHDRAW_WEI: u128 = 400_000_000_000_000_009;

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

/// Full VALUE-BEARING outbound withdrawal (deposit-to-fund → withdraw)
/// acceptance + follower re-derivation. See the module header for the flow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn value_outbound_withdrawal_settles_on_l1_and_follower_rederives() {
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

    // ── WITHDRAW target: deploy `ValuePayable(0)` on the embedded L1 (anvil#0,
    // sequentially after the protocol deploy → no nonce race). PAYABLE so it
    // can receive the withdrawn ETH (the plain `Value` reverts on incoming ETH).
    let l1_value_addr = deploy_value_payable(&l1.rpc_url, ANVIL_KEY, 0)
        .await
        .expect("deploy ValuePayable(0) on embedded L1");
    assert_ne!(
        l1_value_addr,
        Address::ZERO,
        "L1 ValuePayable deploy returned zero"
    );
    let l1_v0 = read_value(&l1.rpc_url, l1_value_addr)
        .await
        .expect("read L1 ValuePayable.value() post-deploy");
    assert_eq!(
        l1_v0,
        U256::ZERO,
        "fresh L1 ValuePayable(0) should read 0, got {l1_v0}"
    );
    let l1_bal0 = eth_get_balance(&l1.rpc_url, l1_value_addr)
        .await
        .expect("read L1 ValuePayable ETH balance post-deploy");
    assert_eq!(
        l1_bal0,
        U256::ZERO,
        "fresh L1 ValuePayable should hold 0 ETH, got {l1_bal0}"
    );
    eprintln!(
        "[S2] WITHDRAW target: L1 ValuePayable @ {l1_value_addr:#x} value()={l1_v0} balance={l1_bal0}"
    );

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
    // PAYABLE so the deposit's ETH can land on it.
    let l2_value_addr = deploy_value_payable(&l2_rpc, ANVIL_KEY_2, 0)
        .await
        .expect("deploy ValuePayable(0) on L2");
    assert_ne!(
        l2_value_addr,
        Address::ZERO,
        "L2 ValuePayable deploy returned zero"
    );
    let l2_v0 = read_value(&l2_rpc, l2_value_addr)
        .await
        .expect("read L2 ValuePayable.value() post-deploy");
    assert_eq!(
        l2_v0,
        U256::ZERO,
        "fresh L2 ValuePayable(0) should read 0, got {l2_v0}"
    );
    eprintln!("[S2] DEPOSIT target: L2 ValuePayable @ {l2_value_addr:#x} value()={l2_v0}");

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

    // ── S3a — DEPOSIT (value-bearing inbound) to FUND the reserve. anvil#4
    // attaches FUND_WEI on L1 to the inbound proxy; the bundled consume credits
    // the rollup's etherBalance += V and the L2 delivery lands V on the L2
    // deposit target. WITHDRAW can't settle until this funds the reserve, so we
    // run them SEQUENTIALLY (not in one slot).
    let dep_hash = send_inbound_set_value(
        &l2_rpc_b,
        &l1.rpc_url, // L1 nonce source (ingress gates on it)
        ANVIL_KEY_4,
        l1_chain_id,
        proxy_in,
        INBOUND_VALUE,
        U256::from(FUND_WEI),
    )
    .await
    .expect("submit DEPOSIT (value-bearing inbound) to L2 ingress");
    eprintln!(
        "[S3a] DEPOSIT submitted: value={FUND_WEI} wei to proxy_in={proxy_in:#x} hash={dep_hash:#x}"
    );

    // Wait until the reserve is funded AND the L2 deposit target holds V — the
    // deposit fully settled on both chains.
    let deposited = wait_for(SETTLE_TIMEOUT, || async {
        let reserve = rollup_ether_balance(&l1.rpc_url, dep.eez_address, dep.rollup_id).await?;
        let l2_bal = eth_get_balance(&l2_rpc_b, l2_value_addr).await?;
        Ok(
            (reserve >= U256::from(WITHDRAW_WEI) && l2_bal == U256::from(FUND_WEI))
                .then_some((reserve, l2_bal)),
        )
    })
    .await;
    let reserve_funded = rollup_ether_balance(&l1.rpc_url, dep.eez_address, dep.rollup_id)
        .await
        .unwrap_or_default();
    let l2_dep_bal = eth_get_balance(&l2_rpc_b, l2_value_addr)
        .await
        .unwrap_or_default();
    assert!(
        deposited.is_ok(),
        "DEPOSIT did not fund the reserve / land on L2: rollup etherBalance = {reserve_funded} \
         (need >= {WITHDRAW_WEI}), L2 deposit-target balance = {l2_dep_bal} (expected {FUND_WEI}).",
    );
    assert_eq!(
        reserve_funded,
        U256::from(FUND_WEI),
        "reserve should equal the deposited V"
    );
    eprintln!(
        "[S3a] DEPOSIT settled: rollup etherBalance == {reserve_funded}, L2 target balance == {l2_dep_bal} ✓"
    );

    // ── S3b — WITHDRAW (value-bearing outbound). anvil#3 attaches WITHDRAW_WEI
    // on L2 to the outbound proxy; EEZL2 burns M to SYSTEM_ADDRESS, and on L1
    // the rollup pays M from its reserve to the L1 target (etherDelta=-M).
    let wd_hash = send_outbound_set_value(
        &l2_rpc_b,
        ANVIL_KEY_3,
        proxy_out,
        OUTBOUND_VALUE,
        U256::from(WITHDRAW_WEI),
    )
    .await
    .expect("submit WITHDRAW (value-bearing outbound) to L2 ingress");
    eprintln!(
        "[S3b] WITHDRAW submitted: value={WITHDRAW_WEI} wei to proxy_out={proxy_out:#x} hash={wd_hash:#x}"
    );

    // ── S3b acceptance (a) — the L1 target received M, the L1 setValue landed,
    // AND the reserve was debited by exactly M.
    let withdrawn = wait_for(SETTLE_TIMEOUT, || async {
        let l1_bal = eth_get_balance(&l1.rpc_url, l1_value_addr).await?;
        let l1_val = read_value(&l1.rpc_url, l1_value_addr).await?;
        Ok(
            (l1_bal == U256::from(WITHDRAW_WEI) && l1_val == U256::from(OUTBOUND_VALUE))
                .then_some(l1_bal),
        )
    })
    .await;
    let l1_final_bal = eth_get_balance(&l1.rpc_url, l1_value_addr)
        .await
        .unwrap_or_default();
    let l1_final_val = read_value(&l1.rpc_url, l1_value_addr)
        .await
        .unwrap_or_default();
    let reserve_after = rollup_ether_balance(&l1.rpc_url, dep.eez_address, dep.rollup_id)
        .await
        .unwrap_or_default();
    assert!(
        withdrawn.is_ok(),
        "WITHDRAW did not pay out: L1 target ETH balance = {l1_final_bal} (expected {WITHDRAW_WEI}), \
         L1 target value = {l1_final_val} (expected {OUTBOUND_VALUE}). The outbound settlement's \
         etherDelta=-M (composer outbound splice) or the L2 burn did not execute.",
    );
    assert_eq!(
        reserve_after,
        U256::from(FUND_WEI - WITHDRAW_WEI),
        "reserve must be debited by exactly M ({FUND_WEI} - {WITHDRAW_WEI}), got {reserve_after}",
    );
    eprintln!(
        "[S3b] ACCEPTANCE (a): L1 target balance == {l1_final_bal} (== M), value == {l1_final_val}, \
         rollup etherBalance == {reserve_after} (== V - M) ✓"
    );

    // ── S3b acceptance (c) — the WITHDRAW user tx's own L2 receipt is SUCCESS.
    // For a value-bearing outbound this is the BURN pin: the tx burns M to
    // SYSTEM_ADDRESS (EEZL2.sol:192-195) BEFORE `_consumeAndExecute`, so a
    // status=1 receipt proves the burn committed. Before the lean
    // build_l2_outbound_entry shape the consume reverted `RollingHashMismatch`
    // (EEZL2.sol:422), rolling back the burn — so the L1 reserve paid M out
    // while the L2 sender KEPT M (silent inflation that acceptance (a)/(b),
    // both L1-side, never caught). The lean entry makes the consume a no-op so
    // the tx succeeds and the burn stands.
    let wd_status = wait_for_l2_tx_receipt(&l2_rpc_b, wd_hash, SETTLE_TIMEOUT)
        .await
        .expect("WITHDRAW user tx never got an L2 receipt");
    assert!(
        wd_status,
        "WITHDRAW user tx {wd_hash:#x} reverted on L2 (status=0) — expected SUCCESS. The L2 burn \
         of M to SYSTEM_ADDRESS did not commit, so the rollup paid M out of its L1 reserve while \
         the L2 sender kept M (inflation). The lean outbound entry must make the consume succeed.",
    );
    eprintln!(
        "[S3b] ACCEPTANCE (c): WITHDRAW user tx L2 receipt status == success (burn committed) ✓"
    );

    // ── S3b acceptance (b) — L1 rollups[1].stateRoot == L2 safe root at a
    // withdraw-inclusive height. Pin it for the S4 follower comparison.
    let reconciled = wait_for(SETTLE_TIMEOUT, || async {
        let l1_root = state_root(&l1.rpc_url, dep.eez_address, dep.rollup_id).await?;
        let l2 = safe_block_number_and_root(&l2_rpc_b).await?;
        // Gate on the WITHDRAW having paid out (L1 target balance == M) so we
        // pin a post-withdraw root, not an earlier deposit/anchor root.
        let l1_bal = eth_get_balance(&l1.rpc_url, l1_value_addr).await?;
        Ok(match l2 {
            Some((num, root))
                if root != alloy_primitives::B256::ZERO
                    && root == l1_root
                    && l1_bal == U256::from(WITHDRAW_WEI) =>
            {
                Some((l1_root, num, root))
            }
            _ => None,
        })
    })
    .await;
    let (settled_block_number, settled_root) = match reconciled {
        Ok((l1_root, num, l2_root)) => {
            assert_eq!(l1_root, l2_root, "reconciled roots must be equal");
            eprintln!(
                "[S3b] ACCEPTANCE (b): L1 rollups[{}].stateRoot == L2 safe root == {l1_root:#x} \
                 (L2 block #{num}, withdraw-settlement-inclusive) ✓",
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
                "L1<->L2 root reconciliation failed ({e}): L1 rollups[{}].stateRoot = {l1_root:#x}, \
                 L2 safe root = {l2_root:#x}. The withdraw paid out (acceptance a passed) but the \
                 tracked root never matched a withdraw-inclusive L2 safe head.",
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
        "[S4] withdraw settled root (follower target) = {settled_root:#x} @ L2 block #{settled_block_number}"
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
        if let Ok(Some(r)) = block_state_root_at(&follower.l2_rpc_url(), settled_block_number).await {
            if r == settled_root {
                return Ok(Some(format!(
                    "height-pinned: block #{settled_block_number} root == settled root"
                )));
            }
            if r != alloy_primitives::B256::ZERO {
                return Err(anyhow::anyhow!(
                    "follower's re-derived block #{settled_block_number} root {r:#x} != \
                     composer's withdraw settled root {settled_root:#x}: the deriver's value-bearing \
                     reconstruction (deposit etherDelta / withdraw etherDelta=-M) is NOT \
                     byte-identical to the composer's. Real value-bearing derivation fork."
                ));
            }
        }
        if let Ok(Some(root)) = safe_block_state_root(&follower.l2_rpc_url()).await {
            if root == settled_root {
                return Ok(Some("safe-head: follower safe root == settled root".to_string()));
            }
        }
        Ok(None)
    })
    .await
    .unwrap_or_else(|e| {
        panic!(
            "FOLLOWER did not re-derive the WITHDRAW-settled root {settled_root:#x} \
             (height #{settled_block_number}) within {FOLLOWER_TIMEOUT:?}: {e}. A follower that \
             cannot reconstruct a value-bearing deposit+withdraw from L1 alone is a real deriver bug.",
        )
    });

    follower.assert_no_process_death();
    eprintln!(
        "[S4] ACCEPTANCE: fresh FOLLOWER re-derived the WITHDRAW-settled root {settled_root:#x} \
         from L1 ALONE ({follower_outcome}) ✓"
    );

    // Belt-and-suspenders: prove the follower's deriver reconstructed BOTH
    // legs. The deposit and the withdraw settle in SEPARATE batches (run
    // sequentially), so the deriver emits TWO distinct
    // `built outbound load + inbound delivery system txs` lines: one with
    // inbound>0 (the deposit batch) and one with outbound>0 (the withdraw
    // batch). Both non-empty proves the value deposit AND the value withdrawal
    // were re-derived from L1 alone.
    let raw_log = std::fs::read_to_string(&follower.log_path).unwrap_or_default();
    let log = strip_ansi_and_normalize(&raw_log);
    let reconcile_ran = log
        .lines()
        .any(|l| l.contains("reconcile_batch_blocks entered") && l.contains("cross_chain=true"));
    let fetched_entries = log.contains("fetching postBatch entries via L1 RPC");
    let built_inbound = log.lines().any(|l| {
        l.contains("built outbound load + inbound delivery system txs")
            && nonzero_field(l, "inbound=")
    });
    let built_outbound = log.lines().any(|l| {
        l.contains("built outbound load + inbound delivery system txs")
            && nonzero_field(l, "outbound=")
    });
    eprintln!(
        "[S4] follower deriver markers: reconcile(cross_chain)={reconcile_ran} \
         fetch_postBatch_entries={fetched_entries} deposit(inbound>0)={built_inbound} \
         withdraw(outbound>0)={built_outbound}"
    );
    assert!(
        reconcile_ran && fetched_entries && built_inbound && built_outbound,
        "follower's deriver log lacks the deposit+withdraw reconstruction markers \
         (reconcile+cross_chain / fetching postBatch entries / a 'built ...' line with inbound>0 \
         for the DEPOSIT and one with outbound>0 for the WITHDRAW). Set EEZ_TEST_LOG_DIR + \
         EEZ_TEST_LOG=info and inspect {}.",
        follower.log_path.display(),
    );

    // A4 — the deriver's outbound FOLLOWER gate (now HARD) ACCEPTED this legit
    // withdrawal: no "REJECTED" error in the follower's log. This confirms the
    // tx-based gate END-TO-END on a REAL outbound (the deriver pairs each outbound
    // settlement entry with its SIGNED Sync-block user tx from DA and the
    // signer/value/data/proxy-target binds all hold) — the bit the synthetic unit
    // test can't cover (the DA pairing). A false reject would also abort
    // re-derivation (the gate returns local_diverged before committing), so the
    // follower-converges assertions above would already have failed; this is the
    // explicit, named check.
    assert!(
        !log.contains("A4 outbound gate REJECTED"),
        "A4 outbound follower gate FALSE-REJECTED a legit withdrawal — the DA \
         pairing (outbound entry <-> signed Sync-block user tx) or a bind \
         (signer/value/data/proxy-target) is wrong. Inspect {}.",
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
