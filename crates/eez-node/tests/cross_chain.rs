//! Bidirectional cross-chain wave test over the embedded dev L1.

use std::time::Duration;

use alloy_primitives::{Address, U256, address};
use alloy_provider::Provider;
use alloy_sol_types::SolCall;

mod common;
use common::{
    ANVIL_KEY_2, ANVIL_KEY_3, ANVIL_KEY_4, DEV_CHAIN_ID, DevnetCfg, ISetterWrapper, IValue,
    IValueNoRet, NodeHandle, batches_posted, create_cross_chain_proxy, create_l2_cross_chain_proxy,
    deploy_protocol_dev, deploy_setter_wrapper, deploy_value_l1, deploy_value_l2,
    deploy_value_no_ret, l2_balance, l2_value, pending_nonce, receipt_ok, sign_and_send,
    signer_address, state_root, value_no_ret, wait_for, wait_for_l2_rpc,
};

const SETUP_TIMEOUT: Duration = Duration::from_secs(90);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);

const WAVE_SETTERS: &[u64] = &[7, 11, 17];
const WAVE_DEPOSITS: &[u128] = &[
    1_000_000_000_000_000,
    2_000_000_000_000_000,
    3_000_000_000_000_000,
];

/// Runs mixed waves with direct, no-return, wrapper, and value-transfer calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_cross_chain_wave_matrix_over_bundle() {
    let cfg = DevnetCfg::new().unwrap();
    let l1_rpc = cfg.l1_rpc_url();
    let l1_xchain = cfg.l1_xchain_url();
    let l2_xchain = cfg.l2_xchain_url();

    let value_deployer = ANVIL_KEY_3;
    let value_addr = signer_address(value_deployer).unwrap().create(0);
    let recipient: Address = address!("0x2222222222222222222222222222222222222222");
    let withdrawal_recipient: Address = address!("0x3333333333333333333333333333333333333333");

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
    let l2_chain_id = alloy_provider::ProviderBuilder::new()
        .connect_http(node.l2_rpc_url().parse().unwrap())
        .get_chain_id()
        .await
        .unwrap();
    let inbound_no_ret = deploy_value_no_ret(
        &node.l2_rpc_url(),
        value_deployer,
        l2_chain_id,
        U256::from(5u64),
    )
    .await
    .expect("deploy inbound ValueNoRet on L2");
    let outbound_value = deploy_value_l1(&l1_rpc, ANVIL_KEY_3, U256::from(5u64))
        .await
        .expect("deploy outbound Value on L1");
    let outbound_no_ret = deploy_value_no_ret(&l1_rpc, ANVIL_KEY_3, DEV_CHAIN_ID, U256::from(5u64))
        .await
        .expect("deploy outbound ValueNoRet on L1");

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
    let inbound_no_ret_proxy = create_cross_chain_proxy(
        &l1_rpc,
        cfg.deployer_key,
        cfg.eez_address,
        inbound_no_ret,
        cfg.rollup_id,
    )
    .await
    .expect("create inbound no-return proxy");
    let inbound_wrapper = deploy_setter_wrapper(&l1_rpc, ANVIL_KEY_3, DEV_CHAIN_ID, setter_proxy)
        .await
        .expect("deploy inbound wrapper on L1");
    let outbound_proxy =
        create_l2_cross_chain_proxy(&node.l2_rpc_url(), value_deployer, outbound_value, 0)
            .await
            .expect("create outbound setter proxy on L2");
    let outbound_no_ret_proxy =
        create_l2_cross_chain_proxy(&node.l2_rpc_url(), value_deployer, outbound_no_ret, 0)
            .await
            .expect("create outbound no-return proxy on L2");
    let withdrawal_proxy =
        create_l2_cross_chain_proxy(&node.l2_rpc_url(), value_deployer, withdrawal_recipient, 0)
            .await
            .expect("create withdrawal proxy on L2");
    let outbound_wrapper = deploy_setter_wrapper(
        &node.l2_rpc_url(),
        value_deployer,
        l2_chain_id,
        outbound_proxy,
    )
    .await
    .expect("deploy outbound wrapper on L2");

    let inbound_user = ANVIL_KEY_2;
    let outbound_user = ANVIL_KEY_4;
    let recipient_before = l2_balance(&node.l2_rpc_url(), recipient).await.unwrap();
    let withdrawal_before = l2_balance(&l1_rpc, withdrawal_recipient).await.unwrap();
    let deposit_sum: u128 = WAVE_DEPOSITS.iter().sum();

    let mut l1_nonce = pending_nonce(&l1_rpc, inbound_user).await.unwrap();
    let mut l2_nonce = pending_nonce(&node.l2_rpc_url(), outbound_user)
        .await
        .unwrap();
    let mut inbound_hashes = Vec::new();
    let mut outbound_hashes = Vec::new();

    // L1-signed calls enter through the inbound front; L2-signed calls use the
    // outbound front. Each front holds the transaction for cross-chain composition.
    for (set_v, dep_v) in WAVE_SETTERS.iter().zip(WAVE_DEPOSITS.iter()) {
        let set_call = IValue::setValueCall {
            v: U256::from(*set_v),
        }
        .abi_encode();
        inbound_hashes.push(
            sign_and_send(
                &l1_xchain,
                inbound_user,
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
        inbound_hashes.push(
            sign_and_send(
                &l1_xchain,
                inbound_user,
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
        for (to, input, value) in [
            (
                inbound_no_ret_proxy,
                IValueNoRet::setValueCall {
                    v: U256::from(*set_v + 200),
                }
                .abi_encode(),
                U256::ZERO,
            ),
            (
                inbound_wrapper,
                ISetterWrapper::setViaProxyCall {
                    v: U256::from(*set_v + 100),
                }
                .abi_encode(),
                U256::ZERO,
            ),
        ] {
            inbound_hashes.push(
                sign_and_send(
                    &l1_xchain,
                    inbound_user,
                    DEV_CHAIN_ID,
                    l1_nonce,
                    Some(to),
                    value,
                    input,
                    900_000,
                )
                .await
                .unwrap(),
            );
            l1_nonce += 1;
        }

        for (to, input, value) in [
            (
                outbound_proxy,
                IValue::setValueCall {
                    v: U256::from(*set_v),
                }
                .abi_encode(),
                U256::ZERO,
            ),
            (
                outbound_no_ret_proxy,
                IValueNoRet::setValueCall {
                    v: U256::from(*set_v + 200),
                }
                .abi_encode(),
                U256::ZERO,
            ),
            (withdrawal_proxy, Vec::new(), U256::from(*dep_v)),
            (
                outbound_wrapper,
                ISetterWrapper::setViaProxyCall {
                    v: U256::from(*set_v + 100),
                }
                .abi_encode(),
                U256::ZERO,
            ),
        ] {
            outbound_hashes.push(
                sign_and_send(
                    &l2_xchain,
                    outbound_user,
                    l2_chain_id,
                    l2_nonce,
                    Some(to),
                    value,
                    input,
                    900_000,
                )
                .await
                .unwrap(),
            );
            l2_nonce += 1;
        }
    }

    for h in &inbound_hashes {
        let h = *h;
        let l1 = l1_rpc.clone();
        let outcome = wait_for(SETTLE_TIMEOUT, move || {
            let l1 = l1.clone();
            async move {
                match receipt_ok(&l1, h).await? {
                    Some(true) => Ok(Some(true)),
                    Some(false) => Ok(Some(false)),
                    None => Ok(None),
                }
            }
        })
        .await
        .expect("cross-chain user tx did not land on L1");
        assert!(outcome, "inbound source tx {h} reverted on L1");
    }
    for h in &outbound_hashes {
        let h = *h;
        wait_for(SETTLE_TIMEOUT, || {
            let l2_rpc = node.l2_rpc_url();
            async move { Ok(receipt_ok(&l2_rpc, h).await?.filter(|ok| *ok)) }
        })
        .await
        .expect("outbound source tx did not settle successfully on L2");
    }

    let final_value = l2_value(&node.l2_rpc_url(), value_addr).await.unwrap();
    assert_eq!(
        final_value,
        U256::from(*WAVE_SETTERS.last().unwrap() + 100),
        "inbound wrapper setter converged",
    );
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            Ok((l2_value(&l1_rpc, outbound_value).await?
                == U256::from(*WAVE_SETTERS.last().unwrap() + 100))
            .then_some(()))
        }
    })
    .await
    .expect("outbound setter did not converge on L1");
    assert_eq!(
        value_no_ret(&node.l2_rpc_url(), inbound_no_ret)
            .await
            .unwrap(),
        U256::from(*WAVE_SETTERS.last().unwrap() + 200),
        "inbound no-return setter converged",
    );
    assert_eq!(
        value_no_ret(&l1_rpc, outbound_no_ret).await.unwrap(),
        U256::from(*WAVE_SETTERS.last().unwrap() + 200),
        "outbound no-return setter converged",
    );

    let recipient_after = l2_balance(&node.l2_rpc_url(), recipient).await.unwrap();
    assert_eq!(
        recipient_after,
        recipient_before + U256::from(deposit_sum),
        "deposits converged",
    );
    assert_eq!(
        l2_balance(&l1_rpc, withdrawal_recipient).await.unwrap(),
        withdrawal_before + U256::from(deposit_sum),
        "withdrawals converged on L1",
    );

    // Roots can differ between settlement and safe-head advancement.
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
    assert_eq!(
        node.log_count_matching(&["user_tx evicted after"]).unwrap()
            + node
                .log_count_matching(&["same-sender", "evicted"])
                .unwrap(),
        0,
        "all non-poison wave transactions must settle without eviction",
    );

    // The default tracing layer prints the message but not the event name.
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
