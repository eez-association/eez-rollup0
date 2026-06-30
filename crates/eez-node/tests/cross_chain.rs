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

mod common;
use common::{
    ANVIL_KEY_2, ANVIL_KEY_3, DEV_CHAIN_ID, DevnetCfg, IValue, NodeHandle, create_cross_chain_proxy,
    deploy_protocol_dev, deploy_value_l2, l2_balance, l2_value, pending_nonce, receipt_ok,
    signer_address, state_root, submit_to_l2_ingress, wait_for, wait_for_l2_rpc,
};

const SETUP_TIMEOUT: Duration = Duration::from_secs(90);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);

/// Cross-chain setter + deposit over `eth_sendBundle` on the embedded dev L1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_chain_setter_deposit_over_bundle() {
    let cfg = DevnetCfg::new().unwrap();
    let l1_rpc = cfg.l1_rpc_url();

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
    assert_eq!(dep.eez_address, cfg.eez_address, "EEZ address deterministic");
    assert_eq!(dep.rollup_id, cfg.rollup_id, "first rollup id");

    wait_for_l2_rpc(&node.l2_rpc_url(), SETUP_TIMEOUT).await.unwrap();
    let value = deploy_value_l2(&node.l2_rpc_url(), value_deployer, U256::from(5u64))
        .await
        .expect("deploy Value on L2");
    assert_eq!(value, value_addr, "Value address deterministic");

    let setter_proxy = create_cross_chain_proxy(&l1_rpc, cfg.deployer_key, cfg.eez_address, value_addr, cfg.rollup_id)
        .await
        .expect("create setter proxy");
    let deposit_proxy = create_cross_chain_proxy(&l1_rpc, cfg.deployer_key, cfg.eez_address, recipient, cfg.rollup_id)
        .await
        .expect("create deposit proxy");

    // L1-signed txs targeting proxies, POSTed to the L2 ingress.
    let user = ANVIL_KEY_2;
    let recipient_before = l2_balance(&node.l2_rpc_url(), recipient).await.unwrap();
    let setters: [u64; 2] = [7, 11]; // last one wins
    let deposits: [u128; 2] = [1_000_000_000_000_000, 2_000_000_000_000_000];
    let deposit_sum: u128 = deposits.iter().sum();

    let mut nonce = pending_nonce(&l1_rpc, user).await.unwrap();
    let mut hashes = Vec::new();
    for i in 0..2 {
        let set_call = IValue::setValueCall { v: U256::from(setters[i]) }.abi_encode();
        hashes.push(
            submit_to_l2_ingress(&node.l2_rpc_url(), user, DEV_CHAIN_ID, nonce, setter_proxy, U256::ZERO, set_call, 600_000)
                .await
                .unwrap(),
        );
        nonce += 1;
        hashes.push(
            submit_to_l2_ingress(&node.l2_rpc_url(), user, DEV_CHAIN_ID, nonce, deposit_proxy, U256::from(deposits[i]), Vec::new(), 600_000)
                .await
                .unwrap(),
        );
        nonce += 1;
    }

    for h in &hashes {
        let h = *h;
        let l1 = l1_rpc.clone();
        wait_for(SETTLE_TIMEOUT, move || {
            let l1 = l1.clone();
            async move { Ok(receipt_ok(&l1, h).await?.map(|ok| assert_ok(ok))) }
        })
        .await
        .expect("cross-chain user tx did not land on L1");
    }

    let final_value = l2_value(&node.l2_rpc_url(), value_addr).await.unwrap();
    assert_eq!(final_value, U256::from(*setters.last().unwrap()), "setter converged");

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

fn assert_ok(ok: bool) {
    assert!(ok, "cross-chain user tx reverted on L1");
}
