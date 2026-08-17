//! Cross-chain integration and nonce-gap regression tests.

use alloy_primitives::{TxHash, U256};
use alloy_sol_types::SolCall;

mod common;
use common::{
    DEV_CHAIN_ID, INBOUND_USER, ISetterWrapper, IValue, IValueNoRet, OUTBOUND_USER, SETTLE_TIMEOUT,
    batches_posted, l2_balance, l2_value, pending_nonce, receipt_ok, setup_cross_chain,
    sign_and_send, state_root, value_no_ret, wait_for,
};

const WAVE_SETTERS: &[u64] = &[7, 11, 17];
const WAVE_DEPOSITS: &[u128] = &[
    1_000_000_000_000_000,
    2_000_000_000_000_000,
    3_000_000_000_000_000,
];

/// The embedded bundle path preserves order but is not atomic like rbuilder,
/// so every submitted valid transaction must be verified independently.
async fn assert_all_transactions_succeeded(rpc_url: &str, hashes: &[TxHash], label: &str) {
    assert!(!hashes.is_empty(), "no {label} transactions were submitted");
    for &hash in hashes {
        let rpc_url = rpc_url.to_owned();
        let status = wait_for(SETTLE_TIMEOUT, move || {
            let rpc_url = rpc_url.clone();
            async move { receipt_ok(&rpc_url, hash).await }
        })
        .await
        .unwrap_or_else(|err| panic!("{label} transaction {hash} did not land: {err:#}"));
        assert!(status, "{label} transaction {hash} reverted");
    }
}

/// Runs one transaction in each direction through the full node pipeline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minimal_bidirectional_cross_chain_smoke() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();

    let inbound = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(41u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .expect("inbound smoke transaction must be admitted");
    let outbound = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(43u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .expect("outbound smoke transaction must be admitted");

    assert_all_transactions_succeeded(&l1_rpc, &[inbound], "inbound smoke").await;
    assert_all_transactions_succeeded(&l2_rpc, &[outbound], "outbound smoke").await;

    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move { Ok((l2_value(&l2_rpc, w.value_l2).await? == U256::from(41u64)).then_some(())) }
    })
    .await
    .expect("inbound smoke effect did not reach L2");
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            Ok((l2_value(&l1_rpc, w.outbound_value).await? == U256::from(43u64)).then_some(()))
        }
    })
    .await
    .expect("outbound smoke effect did not reach L1");

    let (eez, rollup_id) = (w.cfg.eez_address, w.cfg.rollup_id);
    wait_for(SETTLE_TIMEOUT, || {
        let (l1_rpc, l2_rpc) = (l1_rpc.clone(), l2_rpc.clone());
        async move {
            let l1_root = state_root(&l1_rpc, eez, rollup_id).await?;
            let l2_root = common::safe_block_state_root(&l2_rpc).await?;
            Ok(l2_root.filter(|root| *root == l1_root).map(|_| ()))
        }
    })
    .await
    .expect("minimal smoke never reconciled L1 and L2 safe state roots");

    assert!(
        batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block)
            .await
            .unwrap()
            >= 1,
        "minimal smoke must post at least one batch",
    );
    assert_eq!(
        w.node.log_count_matching(&["local L2 state root"]).unwrap(),
        0,
        "minimal smoke must not diverge",
    );
    assert_eq!(
        w.node.log_count_matching(&["evicting"]).unwrap(),
        0,
        "valid smoke transactions must not be evicted",
    );
    w.node.assert_no_process_death();
}

/// Runs mixed waves with direct, no-return, wrapper, and value-transfer calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_cross_chain_wave_matrix_over_bundle() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l1_xchain = w.l1_xchain();
    let l2_xchain = w.l2_xchain();
    let l2_rpc = w.l2_rpc();

    let recipient_before = l2_balance(&l2_rpc, w.recipient).await.unwrap();
    let withdrawal_before = l2_balance(&l1_rpc, w.withdrawal_recipient).await.unwrap();
    let deposit_sum: u128 = WAVE_DEPOSITS.iter().sum();

    let mut inbound_hashes = Vec::new();
    let mut outbound_hashes = Vec::new();

    for (set_v, dep_v) in WAVE_SETTERS.iter().zip(WAVE_DEPOSITS.iter()) {
        // Let each wave settle before deriving the next source nonces. A compose
        // tick removes transactions from the held pool before they land on the
        // source chain, so incrementing one cached nonce across multiple waves
        // races the ingress gate's `on_chain + held` validation.
        let mut l1_nonce = pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
        let mut l2_nonce = pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
        let inbound_wave_start = inbound_hashes.len();
        let outbound_wave_start = outbound_hashes.len();

        let set_input = IValue::setValueCall {
            v: U256::from(*set_v),
        }
        .abi_encode();
        let no_ret_input = IValueNoRet::setValueCall {
            v: U256::from(*set_v + 200),
        }
        .abi_encode();
        let wrapper_input = ISetterWrapper::setViaProxyCall {
            v: U256::from(*set_v + 100),
        }
        .abi_encode();
        inbound_hashes.push(
            sign_and_send(
                &l1_xchain,
                INBOUND_USER,
                DEV_CHAIN_ID,
                l1_nonce,
                Some(w.setter_proxy),
                U256::ZERO,
                set_input.clone(),
                600_000,
            )
            .await
            .unwrap(),
        );
        l1_nonce += 1;
        inbound_hashes.push(
            sign_and_send(
                &l1_xchain,
                INBOUND_USER,
                DEV_CHAIN_ID,
                l1_nonce,
                Some(w.deposit_proxy),
                U256::from(*dep_v),
                Vec::new(),
                600_000,
            )
            .await
            .unwrap(),
        );
        l1_nonce += 1;
        for (to, input, value) in [
            (w.inbound_no_ret_proxy, no_ret_input.clone(), U256::ZERO),
            (w.inbound_wrapper, wrapper_input.clone(), U256::ZERO),
        ] {
            inbound_hashes.push(
                sign_and_send(
                    &l1_xchain,
                    INBOUND_USER,
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
            (w.outbound_proxy, set_input, U256::ZERO),
            (w.outbound_no_ret_proxy, no_ret_input, U256::ZERO),
            (w.withdrawal_proxy, Vec::new(), U256::from(*dep_v)),
            (w.outbound_wrapper, wrapper_input, U256::ZERO),
        ] {
            outbound_hashes.push(
                sign_and_send(
                    &l2_xchain,
                    OUTBOUND_USER,
                    w.l2_chain_id,
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

        assert_all_transactions_succeeded(
            &l1_rpc,
            &inbound_hashes[inbound_wave_start..],
            "inbound wave",
        )
        .await;
        assert_all_transactions_succeeded(
            &l2_rpc,
            &outbound_hashes[outbound_wave_start..],
            "outbound wave",
        )
        .await;
    }

    let expected_per_direction = WAVE_SETTERS.len() * 4;
    assert_eq!(
        inbound_hashes.len(),
        expected_per_direction,
        "every inbound wave operation must be submitted",
    );
    assert_eq!(
        outbound_hashes.len(),
        expected_per_direction,
        "every outbound wave operation must be submitted",
    );

    let final_value = l2_value(&l2_rpc, w.value_l2).await.unwrap();
    assert_eq!(
        final_value,
        U256::from(*WAVE_SETTERS.last().unwrap() + 100),
        "inbound wrapper setter converged",
    );
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            Ok((l2_value(&l1_rpc, w.outbound_value).await?
                == U256::from(*WAVE_SETTERS.last().unwrap() + 100))
            .then_some(()))
        }
    })
    .await
    .expect("outbound setter did not converge on L1");
    assert_eq!(
        value_no_ret(&l2_rpc, w.inbound_no_ret).await.unwrap(),
        U256::from(*WAVE_SETTERS.last().unwrap() + 200),
        "inbound no-return setter converged",
    );
    assert_eq!(
        value_no_ret(&l1_rpc, w.outbound_no_ret).await.unwrap(),
        U256::from(*WAVE_SETTERS.last().unwrap() + 200),
        "outbound no-return setter converged",
    );

    let recipient_after = l2_balance(&l2_rpc, w.recipient).await.unwrap();
    assert_eq!(
        recipient_after,
        recipient_before + U256::from(deposit_sum),
        "deposits converged",
    );
    assert_eq!(
        l2_balance(&l1_rpc, w.withdrawal_recipient).await.unwrap(),
        withdrawal_before + U256::from(deposit_sum),
        "withdrawals converged on L1",
    );

    let (eez, rollup_id) = (w.cfg.eez_address, w.cfg.rollup_id);
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

    let pb = batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block)
        .await
        .unwrap();
    assert!(
        pb >= WAVE_SETTERS.len(),
        "expected ≥{} BatchPosted events, got {pb}",
        WAVE_SETTERS.len(),
    );

    assert_eq!(
        w.node.log_count_matching(&["local L2 state root"]).unwrap(),
        0,
        "zero state-root divergence events",
    );
    assert_eq!(
        w.node
            .log_count_matching(&["user_tx evicted after"])
            .unwrap()
            + w.node
                .log_count_matching(&[
                    "same-sender tx above an evicted nonce",
                    "same-sender pooled tx above an evicted nonce",
                ])
                .unwrap(),
        0,
        "all non-poison wave transactions must settle without eviction",
    );

    assert!(
        w.node
            .log_count_matching(&["eth_sendBundle: forwarded txs to pool in order"])
            .unwrap()
            > 0,
        "embedded dev L1 eth_sendBundle was exercised",
    );
    assert_eq!(
        w.node
            .log_count_matching(&["relay has no eth_sendBundle; submitting txs via mempool"])
            .unwrap(),
        0,
        "composer must not fall back to eth_sendRawTransaction",
    );
    w.node.assert_no_process_death();
}

/// Stateful proxy calls in one bundle must observe accumulated target state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_bundle_uses_accumulated_target_state() {
    use alloy_provider::{Provider, ProviderBuilder};
    use alloy_rpc_types_eth::Filter;
    use alloy_sol_types::SolEvent;

    let w = common::setup_cross_chain_with_env(&[("EEZ_MAX_USER_TXS_PER_BUNDLE", "3".to_string())])
        .await
        .unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let nonce = pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap();

    let mut hashes = Vec::with_capacity(3);
    for offset in 0..3 {
        hashes.push(
            sign_and_send(
                &w.l1_xchain(),
                INBOUND_USER,
                DEV_CHAIN_ID,
                nonce + offset,
                Some(w.inbound_wrapper),
                U256::ZERO,
                ISetterWrapper::setViaProxyCall {
                    v: U256::from(1u64),
                }
                .abi_encode(),
                900_000,
            )
            .await
            .expect("state-dependent call must be admitted"),
        );
    }
    assert_all_transactions_succeeded(&l1_rpc, &hashes, "sequential bundle").await;

    let l1_provider = ProviderBuilder::new().connect_http(l1_rpc.parse().unwrap());
    let wrapped = wait_for(SETTLE_TIMEOUT, || {
        let l1_provider = l1_provider.clone();
        async move {
            let logs = l1_provider
                .get_logs(
                    &Filter::new()
                        .address(w.inbound_wrapper)
                        .event_signature(ISetterWrapper::Wrapped::SIGNATURE_HASH),
                )
                .await?;
            Ok((logs.len() == 3).then_some(logs))
        }
    })
    .await
    .expect("all three source calls must emit a Wrapped result");

    let changed: Vec<bool> = wrapped
        .iter()
        .map(|log| {
            let event = ISetterWrapper::Wrapped::decode_log(&log.inner)?;
            Ok(event.changed)
        })
        .collect::<anyhow::Result<_>>()
        .unwrap();
    assert_eq!(
        changed,
        [true, false, false],
        "each call must observe state accumulated by the preceding call",
    );

    let l2_provider = ProviderBuilder::new().connect_http(l2_rpc.parse().unwrap());
    let target_logs = wait_for(SETTLE_TIMEOUT, || {
        let l2_provider = l2_provider.clone();
        async move {
            let logs = l2_provider
                .get_logs(
                    &Filter::new()
                        .address(w.value_l2)
                        .event_signature(IValue::ValueSet::SIGNATURE_HASH),
                )
                .await?;
            Ok((logs.len() == 3).then_some(logs))
        }
    })
    .await
    .expect("all three target calls must execute");

    let target_blocks: std::collections::HashSet<_> = target_logs
        .iter()
        .filter_map(|log| log.block_number)
        .collect();
    assert_eq!(
        target_blocks.len(),
        1,
        "the three state-dependent calls must execute in one Sync block",
    );
    assert_eq!(
        l2_value(&l2_rpc, w.value_l2).await.unwrap(),
        U256::from(1u64)
    );
    assert_eq!(
        w.node.log_count_matching(&["evicting"]).unwrap(),
        0,
        "valid state-dependent calls must not be evicted",
    );

    // Outbound exercises the same carried-state invariant against an L1 target.
    let outbound_nonce = pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
    let mut outbound_hashes = Vec::with_capacity(3);
    for offset in 0..3 {
        outbound_hashes.push(
            sign_and_send(
                &w.l2_xchain(),
                OUTBOUND_USER,
                w.l2_chain_id,
                outbound_nonce + offset,
                Some(w.outbound_wrapper),
                U256::ZERO,
                ISetterWrapper::setViaProxyCall {
                    v: U256::from(1u64),
                }
                .abi_encode(),
                900_000,
            )
            .await
            .expect("outbound state-dependent call must be admitted"),
        );
    }
    assert_all_transactions_succeeded(&l2_rpc, &outbound_hashes, "outbound sequential bundle")
        .await;

    let outbound_wrapped = wait_for(SETTLE_TIMEOUT, || {
        let l2_provider = l2_provider.clone();
        async move {
            let logs = l2_provider
                .get_logs(
                    &Filter::new()
                        .address(w.outbound_wrapper)
                        .event_signature(ISetterWrapper::Wrapped::SIGNATURE_HASH),
                )
                .await?;
            Ok((logs.len() == 3).then_some(logs))
        }
    })
    .await
    .expect("all three outbound source calls must emit a Wrapped result");
    let outbound_changed: Vec<bool> = outbound_wrapped
        .iter()
        .map(|log| {
            let event = ISetterWrapper::Wrapped::decode_log(&log.inner)?;
            Ok(event.changed)
        })
        .collect::<anyhow::Result<_>>()
        .unwrap();
    assert_eq!(
        outbound_changed,
        [true, false, false],
        "outbound calls must observe state accumulated by the preceding call",
    );
    let outbound_source_blocks: std::collections::HashSet<_> = outbound_wrapped
        .iter()
        .filter_map(|log| log.block_number)
        .collect();
    assert_eq!(
        outbound_source_blocks.len(),
        1,
        "the three outbound state-dependent calls must execute in one Sync block",
    );

    let outbound_target_logs = wait_for(SETTLE_TIMEOUT, || {
        let l1_provider = l1_provider.clone();
        async move {
            let logs = l1_provider
                .get_logs(
                    &Filter::new()
                        .address(w.outbound_value)
                        .event_signature(IValue::ValueSet::SIGNATURE_HASH),
                )
                .await?;
            Ok((logs.len() == 3).then_some(logs))
        }
    })
    .await
    .expect("all three outbound target calls must execute");
    assert_eq!(outbound_target_logs.len(), 3);
    assert_eq!(
        l2_value(&l1_rpc, w.outbound_value).await.unwrap(),
        U256::from(1u64)
    );
    w.node.assert_no_process_death();
}
