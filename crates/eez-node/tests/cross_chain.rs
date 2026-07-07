//! Cross-chain L1→L2 smoke — Rust port of `scripts/devnet-test.sh`.
//!
//! Uses the embedded dev L1 as the anchor (not a separate anvil): source-tx
//! simulation reads the embedded L1's in-process state, so proxies created
//! on a separate anvil would be invisible to it. As a side effect,
//! `EEZ_L1_BUILDER_RPC_URL` defaults to the embedded L1 and bundles flow
//! through `bundle_rpc.rs` — which is what this test exercises.

use std::time::Duration;

use alloy_primitives::{Address, U256, address};
use alloy_sol_types::SolCall;
use anyhow::bail;

mod common;
use common::{
    ANVIL_KEY_2, ANVIL_KEY_3, DEV_CHAIN_ID, DevnetCfg, IValue, NodeHandle, batches_posted,
    create_cross_chain_proxy, deploy_protocol_dev, deploy_value_l2, l2_balance, l2_value,
    pending_nonce, receipt_ok, sign_send_raw, signer_address, state_root, wait_for,
    wait_for_l2_rpc,
};

const SETUP_TIMEOUT: Duration = Duration::from_secs(90);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);

/// One setter + one deposit per wave; last setter value wins.
const WAVE_SETTERS: &[u64] = &[7, 11, 17];
const WAVE_DEPOSITS: &[u128] = &[
    1_000_000_000_000_000,
    2_000_000_000_000_000,
    3_000_000_000_000_000,
];

/// Cross-chain setter + deposit over `eth_sendBundle` on the embedded dev L1.
///
/// Fires three waves of setter+deposit ops, then verifies: all L1 receipts
/// succeed, semantic effects converge, L1 stateRoot matches L2 safe stateRoot,
/// ≥N BatchPosted events, and zero divergence events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_chain_setter_deposit_over_bundle() {
    let cfg = DevnetCfg::new().unwrap();
    let l1_rpc = cfg.l1_rpc_url();
    let l1_xchain = cfg.l1_xchain_url();

    // Value (setter target) is CREATE(deployer, 0); recipient (deposit target)
    // is a fixed EOA. The proxies are created after boot — the classifier routes
    // our ops by source chain id, so it needn't know the proxy addresses upfront.
    let value_deployer = ANVIL_KEY_3;
    let value_addr = signer_address(value_deployer).unwrap().create(0);
    let recipient: Address = address!("0x2222222222222222222222222222222222222222");

    let datadir = tempfile::tempdir().unwrap();
    let env = cfg.env();
    let node = NodeHandle::spawn(datadir.path(), &env).unwrap();

    wait_for_l2_rpc(&l1_rpc, SETUP_TIMEOUT).await.unwrap();
    let dep = deploy_protocol_dev(&l1_rpc, cfg.deployer_key, cfg.initial_state)
        .await
        .expect("deploy protocol onto embedded L1");
    assert_eq!(
        dep.eez_address, cfg.eez_address,
        "EEZ address deterministic"
    );
    assert_eq!(dep.rollup_id, cfg.rollup_id, "first rollup id");

    wait_for_l2_rpc(&node.l2_rpc_url(), SETUP_TIMEOUT)
        .await
        .unwrap();
    let value = deploy_value_l2(&node.l2_rpc_url(), value_deployer, U256::from(5u64))
        .await
        .expect("deploy Value on L2");
    assert_eq!(value, value_addr, "Value address deterministic");

    let setter_proxy = create_cross_chain_proxy(
        &l1_rpc,
        cfg.deployer_key,
        cfg.eez_address,
        value_addr,
        cfg.rollup_id,
    )
    .await
    .expect("create setter proxy");
    let deposit_proxy = create_cross_chain_proxy(
        &l1_rpc,
        cfg.deployer_key,
        cfg.eez_address,
        recipient,
        cfg.rollup_id,
    )
    .await
    .expect("create deposit proxy");

    let user = ANVIL_KEY_2;
    let recipient_before = l2_balance(&node.l2_rpc_url(), recipient).await.unwrap();
    let deposit_sum: u128 = WAVE_DEPOSITS.iter().sum();

    // Three waves: each wave submits one setter + one deposit op to the L1
    // cross-chain front (L1-signed; the front is fixed `Inbound` and holds
    // the tx for the next Sync slot instead of forwarding it to the L1 mempool).
    let mut l1_nonce = pending_nonce(&l1_rpc, user).await.unwrap();
    let mut hashes = Vec::new();

    for (set_v, dep_v) in WAVE_SETTERS.iter().zip(WAVE_DEPOSITS.iter()) {
        let set_call = IValue::setValueCall {
            v: U256::from(*set_v),
        }
        .abi_encode();
        hashes.push(
            sign_send_raw(
                &l1_xchain,
                user,
                DEV_CHAIN_ID,
                l1_nonce,
                Some(setter_proxy),
                U256::ZERO,
                set_call,
                600_000,
            )
            .await
            .unwrap(),
        );
        l1_nonce += 1;
        hashes.push(
            sign_send_raw(
                &l1_xchain,
                user,
                DEV_CHAIN_ID,
                l1_nonce,
                Some(deposit_proxy),
                U256::from(*dep_v),
                Vec::new(),
                600_000,
            )
            .await
            .unwrap(),
        );
        l1_nonce += 1;
    }

    // Each cross-chain op must land on L1 and succeed (a revert means the
    // composer's bundle settled but the source tx itself failed).
    for h in &hashes {
        let h = *h;
        let l1 = l1_rpc.clone();
        wait_for(SETTLE_TIMEOUT, move || {
            let l1 = l1.clone();
            async move {
                match receipt_ok(&l1, h).await? {
                    Some(true) => Ok(Some(())),
                    Some(false) => bail!("cross-chain user tx {h} reverted on L1"),
                    None => Ok(None),
                }
            }
        })
        .await
        .expect("cross-chain user tx did not land on L1");
    }

    let final_value = l2_value(&node.l2_rpc_url(), value_addr).await.unwrap();
    assert_eq!(
        final_value,
        U256::from(*WAVE_SETTERS.last().unwrap()),
        "setter converged",
    );

    let recipient_after = l2_balance(&node.l2_rpc_url(), recipient).await.unwrap();
    assert_eq!(
        recipient_after,
        recipient_before + U256::from(deposit_sum),
        "deposits converged",
    );

    // Polled because the composer keeps posting batches; the two roots can
    // differ momentarily between posts.
    let l2_rpc = node.l2_rpc_url();
    let (eez, rollup_id) = (cfg.eez_address, cfg.rollup_id);
    wait_for(SETTLE_TIMEOUT, || {
        let (l1_rpc, l2_rpc) = (l1_rpc.clone(), l2_rpc.clone());
        async move {
            let l1_root = state_root(&l1_rpc, eez, rollup_id).await?;
            let l2_root = common::safe_block_state_root(&l2_rpc).await?;
            Ok(l2_root.filter(|r| *r == l1_root).map(|_| ()))
        }
    })
    .await
    .expect("L1 stored stateRoot never matched L2 safe stateRoot");

    // At least one postBatch per wave must have landed on L1.
    let pb = batches_posted(&l1_rpc, cfg.eez_address, dep.deploy_block)
        .await
        .unwrap();
    assert!(
        pb >= WAVE_SETTERS.len(),
        "expected ≥{} BatchPosted events, got {pb}",
        WAVE_SETTERS.len(),
    );

    // A divergence event means the deriver found a mismatch between local L2
    // state and an L1-confirmed batch — must never happen on a healthy node.
    assert_eq!(
        node.log_count_matching(&["local L2 state root"]).unwrap(),
        0,
        "zero state-root divergence events",
    );

    // Match on the event message text, not the `name:` metadata — the default
    // tracing fmt layer prints the message but not the event name.
    assert!(
        node.log_count_matching(&["eth_sendBundle: forwarded txs to pool in order"])
            .unwrap()
            > 0,
        "embedded dev L1 eth_sendBundle was exercised",
    );
    assert_eq!(
        node.log_count_matching(&["relay has no eth_sendBundle; submitting txs via mempool"])
            .unwrap(),
        0,
        "composer must not fall back to eth_sendRawTransaction",
    );
    node.assert_no_process_death();
}
