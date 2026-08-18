//! Cross-chain integration and nonce-gap regression tests.

use alloy_primitives::{Address, Bytes, TxHash, U256, keccak256};
use alloy_sol_types::{SolCall, SolEvent, SolValue};

use common::{
    DEV_CHAIN_ID, IEEZL2Direct, IEmptyCall, INBOUND_USER, INestedSetterOuter, IReturnData,
    IReturnDataWrapper, ISetterWrapper, IValue, IValueNoRet, OUTBOUND_USER, SETTLE_TIMEOUT,
    account_code, batches_posted, completed_proxy_calls, count_events, deploy_nested_setter_inner,
    deploy_nested_setter_outer, empty_call_state, l2_balance, l2_value, last_proxy_result,
    nested_proxy_calls, pending_nonce, receipt_ok, return_data_hash, return_data_length,
    setup_cross_chain, setup_cross_chain_codeless, setup_cross_chain_empty_call,
    setup_cross_chain_nested_setter, setup_cross_chain_outbound_return_data,
    setup_cross_chain_return_data, sign_and_send, state_root, value_no_ret, wait_for,
};
use eez_protocol::EEZL2_ADDRESS;
use eez_testkit as common;

const WAVE_SETTERS: &[u64] = &[7, 11, 17];
const WAVE_DEPOSITS: &[u128] = &[
    1_000_000_000_000_000,
    2_000_000_000_000_000,
    3_000_000_000_000_000,
];

// The embedded bundle path preserves ordering but not all-or-nothing inclusion,
// so a wave must check every source receipt independently.
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

// Reject-policy scenarios must be mined failures, not ingress rejections.
async fn assert_transaction_reverted(rpc_url: &str, hash: TxHash, label: &str) {
    let status = wait_for(SETTLE_TIMEOUT, || {
        let rpc_url = rpc_url.to_owned();
        async move { receipt_ok(&rpc_url, hash).await }
    })
    .await
    .unwrap_or_else(|err| panic!("{label} transaction {hash} did not land: {err:#}"));
    assert!(!status, "{label} transaction {hash} unexpectedly succeeded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_directions_zero_value_direct_proxy_success_single_call() {
    let w = setup_cross_chain().await.unwrap();
    common::run_scenarios(
        &w,
        [
            common::Scenario::new("A-01 bidirectional zero-value direct proxy")
                .inbound(common::setter_call(w.setter_proxy, 41u64))
                .outbound(common::setter_call(w.outbound_proxy, 43u64))
                .expect_l2_state(common::value_read(w.value_l2), 41u64)
                .expect_l1_state(common::value_read(w.outbound_value), 43u64)
                .expect_settled_fully(),
        ],
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_empty_calldata_and_zero_value_is_not_skipped() {
    // Zero calldata and zero value are still a materialized entry.
    let w = setup_cross_chain_empty_call().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();

    let tx = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.empty_call_proxy),
        U256::ZERO,
        Vec::new(),
        600_000,
    )
    .await
    .expect("empty-calldata inbound transaction must be admitted");
    assert_all_transactions_succeeded(&l1_rpc, &[tx], "empty-calldata inbound").await;

    // `calls` proves materialization; `received` and `lastValue` prove the
    // all-zero call was not rewritten while crossing the boundary.
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move {
            Ok((empty_call_state(&l2_rpc, w.empty_call_l2).await?
                == (U256::from(1u64), U256::ZERO, U256::ZERO))
                .then_some(()))
        }
    })
    .await
    .expect("empty-calldata cross-chain entry was skipped");
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_deposit_to_payable_contract_runs_fallback() {
    // `EmptyCall` has no `receive`, so empty calldata exercises fallback.
    let w = setup_cross_chain_empty_call().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let value = U256::from(123_456u64);

    let tx = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.empty_call_proxy),
        value,
        Vec::new(),
        600_000,
    )
    .await
    .expect("payable inbound deposit must be admitted");
    assert_all_transactions_succeeded(&l1_rpc, &[tx], "payable inbound deposit").await;

    // The fallback increments `calls` and accumulates only `msg.value`.
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move {
            Ok((empty_call_state(&l2_rpc, w.empty_call_l2).await?
                == (U256::from(1u64), value, U256::ZERO))
                .then_some(()))
        }
    })
    .await
    .expect("payable destination fallback did not receive the deposit");
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_value_and_calldata_apply_atomically() {
    // Assert the same destination frame sees both value and calldata.
    let w = setup_cross_chain_empty_call().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let value = U256::from(456_789u64);
    let next = U256::from(91u64);

    let tx = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.empty_call_proxy),
        value,
        IEmptyCall::setValueCall { next }.abi_encode(),
        600_000,
    )
    .await
    .expect("value-and-calldata inbound transaction must be admitted");
    assert_all_transactions_succeeded(&l1_rpc, &[tx], "value-and-calldata inbound").await;

    // One observation binds the call count, transferred value, and decoded arg.
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move {
            Ok((empty_call_state(&l2_rpc, w.empty_call_l2).await?
                == (U256::from(1u64), value, next))
                .then_some(()))
        }
    })
    .await
    .expect("destination did not receive value and calldata atomically");
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_ccm_l2_outbound_call_is_rejected() {
    // Direct CCM-L2 access is mined, then rejected during execution.
    let w = setup_cross_chain().await.unwrap();
    let l2_rpc = w.l2_rpc();
    let caller: Address = common::signer_address(OUTBOUND_USER).unwrap();
    let tx = sign_and_send(
        &l2_rpc,
        OUTBOUND_USER,
        w.l2_chain_id,
        pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(EEZL2_ADDRESS),
        U256::ZERO,
        IEEZL2Direct::executeCrossChainCallCall {
            sourceAddress: caller,
            callData: Bytes::new(),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .expect("direct CCM-L2 transaction must be accepted for execution");

    // Admission is expected; only execution must fail for an unauthorized caller.
    assert_transaction_reverted(&l2_rpc, tx, "direct CCM-L2 outbound").await;
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_directions_return_value_and_wrapper_success_repeated_waves() {
    // Sustained matrix: direct, no-return, wrapper, and value-transfer calls
    // in both directions. Per-wave receipt checks avoid ingress nonce races.
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
        // tick removes held transactions before they land on the source chain,
        // so caching nonces across waves races the ingress gate's
        // `on_chain + held` validation.
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
    assert_eq!(
        last_proxy_result(&l1_rpc, w.inbound_wrapper).await.unwrap(),
        (true, U256::from(*WAVE_SETTERS.last().unwrap() + 100)),
        "inbound wrapper must receive the exact destination return",
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
        last_proxy_result(&l2_rpc, w.outbound_wrapper)
            .await
            .unwrap(),
        (true, U256::from(*WAVE_SETTERS.last().unwrap() + 100)),
        "outbound wrapper must receive the exact destination return",
    );
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
        w.node
            .count_signals(&[
                common::signals::DERIVER_STATE_DIVERGED_PRE,
                common::signals::DERIVER_STATE_DIVERGED_POST,
            ])
            .unwrap(),
        0
    );
    assert_eq!(
        w.node
            .count_signals(&[
                common::signals::TX_POISON_EVICTED,
                common::signals::TX_NONCE_CHAIN_EVICTED,
            ])
            .unwrap(),
        0
    );

    assert!(
        w.node
            .count_signal(common::signals::BUNDLE_ACCEPTED)
            .unwrap()
            > 0,
        "embedded dev L1 eth_sendBundle was exercised",
    );
    assert_eq!(
        w.node
            .count_signal(common::signals::BUNDLE_MEMPOOL_FALLBACK)
            .unwrap(),
        0,
        "composer must not fall back to eth_sendRawTransaction",
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_destination_events_are_retained_in_the_l2_block() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();

    let tx = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(61u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .expect("event-emitting inbound transaction must be admitted");
    assert_all_transactions_succeeded(&l1_rpc, &[tx], "event-emitting inbound").await;

    // The destination event must survive execution of the derived L2 block.
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move {
            Ok(
                (count_events(&l2_rpc, w.value_l2, IValue::ValueSet::SIGNATURE_HASH, 0).await?
                    >= 1)
                    .then_some(()),
            )
        }
    })
    .await
    .expect("destination event was not retained in the L2 block");
    assert_eq!(
        l2_value(&l2_rpc, w.value_l2).await.unwrap(),
        U256::from(61u64),
        "the event must correspond to the requested destination write",
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codeless_registered_targets_complete_deterministically_in_both_directions() {
    let w = setup_cross_chain_codeless().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let data = IReturnDataWrapper::callAndRecordCall { data: Bytes::new() }.abi_encode();

    assert!(
        account_code(&l2_rpc, w.recipient).await.unwrap().is_empty(),
        "inbound target must be codeless",
    );
    assert!(
        account_code(&l1_rpc, w.withdrawal_recipient)
            .await
            .unwrap()
            .is_empty(),
        "outbound target must be codeless",
    );

    let inbound = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.inbound_wrapper),
        U256::ZERO,
        data.clone(),
        900_000,
    )
    .await
    .expect("codeless inbound transaction must be admitted");
    let outbound = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(w.outbound_wrapper),
        U256::ZERO,
        data,
        900_000,
    )
    .await
    .expect("codeless outbound transaction must be admitted");
    assert_all_transactions_succeeded(&l1_rpc, &[inbound], "codeless inbound").await;
    assert_all_transactions_succeeded(&l2_rpc, &[outbound], "codeless outbound").await;

    let empty_hash = keccak256(Bytes::new());
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            Ok(
                (return_data_length(&l1_rpc, w.inbound_wrapper).await? == U256::ZERO
                    && return_data_hash(&l1_rpc, w.inbound_wrapper).await? == empty_hash)
                    .then_some(()),
            )
        }
    })
    .await
    .expect("codeless L2 target did not return the deterministic empty result");
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move {
            Ok(
                (return_data_length(&l2_rpc, w.outbound_wrapper).await? == U256::ZERO
                    && return_data_hash(&l2_rpc, w.outbound_wrapper).await? == empty_hash)
                    .then_some(()),
            )
        }
    })
    .await
    .expect("codeless L1 target did not return the deterministic empty result");

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
    .expect("codeless calls did not reconcile the committed state roots");
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_dynamic_and_empty_bytes_returns_are_preserved_distinctly() {
    let w = setup_cross_chain_outbound_return_data().await.unwrap();
    let l2_rpc = w.l2_rpc();
    let payload = Bytes::from(vec![0x5a; 96]);
    let dynamic_return_hash = keccak256(payload.abi_encode());
    let dynamic_tx = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(w.return_data_wrapper),
        U256::ZERO,
        IReturnDataWrapper::callAndRecordCall {
            data: IReturnData::echoCall { value: payload }.abi_encode().into(),
        }
        .abi_encode(),
        1_200_000,
    )
    .await
    .expect("outbound dynamic-return transaction must be admitted");
    assert_all_transactions_succeeded(&l2_rpc, &[dynamic_tx], "outbound dynamic return").await;
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move {
            Ok(
                (return_data_length(&l2_rpc, w.return_data_wrapper).await? == U256::from(160u64)
                    && return_data_hash(&l2_rpc, w.return_data_wrapper).await?
                        == dynamic_return_hash)
                    .then_some(()),
            )
        }
    })
    .await
    .expect("outbound dynamic return data was truncated or altered");

    let empty_tx = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(w.return_data_wrapper),
        U256::ZERO,
        IReturnDataWrapper::callAndRecordCall {
            data: IReturnData::emptyBytesCall {}.abi_encode().into(),
        }
        .abi_encode(),
        1_200_000,
    )
    .await
    .expect("outbound empty-bytes-return transaction must be admitted");
    assert_all_transactions_succeeded(&l2_rpc, &[empty_tx], "outbound empty bytes return").await;
    let empty_return_hash = keccak256(Bytes::new().abi_encode());
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move {
            Ok(
                (return_data_length(&l2_rpc, w.return_data_wrapper).await? == U256::from(64u64)
                    && return_data_hash(&l2_rpc, w.return_data_wrapper).await?
                        == empty_return_hash)
                    .then_some(()),
            )
        }
    })
    .await
    .expect("outbound ABI-encoded empty bytes were not preserved");
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_identical_wrapper_proxy_calls_settle_as_ordered_entries() {
    // Identical calls share a semantic hash; entry order must keep both alive.
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let value = U256::from(73u64);

    let tx = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.inbound_wrapper),
        U256::ZERO,
        ISetterWrapper::setSameValueTwiceCall { v: value }.abi_encode(),
        1_200_000,
    )
    .await
    .expect("duplicate proxy-call transaction must be admitted");

    assert_all_transactions_succeeded(&l1_rpc, &[tx], "duplicate inbound proxy calls").await;
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move { Ok((l2_value(&l2_rpc, w.value_l2).await? == value).then_some(())) }
    })
    .await
    .expect("duplicate calls did not reach L2");

    // The source wrapper advances only after decoding each independent return.
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            Ok(
                (completed_proxy_calls(&l1_rpc, w.inbound_wrapper).await? == U256::from(2u64))
                    .then_some(()),
            )
        }
    })
    .await
    .expect("both ordered proxy calls did not return to the wrapper");
    assert_eq!(
        last_proxy_result(&l1_rpc, w.inbound_wrapper).await.unwrap(),
        (false, value),
        "the final ordered call must deliver its exact decoded return",
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_identical_wrapper_proxy_calls_settle_as_ordered_entries() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let value = U256::from(79u64);

    let tx = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(w.outbound_wrapper),
        U256::ZERO,
        ISetterWrapper::setSameValueTwiceCall { v: value }.abi_encode(),
        1_200_000,
    )
    .await
    .expect("duplicate outbound proxy-call transaction must be admitted");
    assert_all_transactions_succeeded(&l2_rpc, &[tx], "duplicate outbound proxy calls").await;

    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move { Ok((l2_value(&l1_rpc, w.outbound_value).await? == value).then_some(())) }
    })
    .await
    .expect("duplicate calls did not reach L1");
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move {
            Ok(
                (completed_proxy_calls(&l2_rpc, w.outbound_wrapper).await? == U256::from(2u64))
                    .then_some(()),
            )
        }
    })
    .await
    .expect("both ordered outbound proxy calls did not return to the wrapper");
    assert_eq!(
        last_proxy_result(&l2_rpc, w.outbound_wrapper)
            .await
            .unwrap(),
        (false, value),
        "the final ordered call must deliver its exact decoded return",
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_destination_events_are_retained_in_the_l1_block() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();

    let tx = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(67u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .expect("event-emitting outbound transaction must be admitted");
    assert_all_transactions_succeeded(&l2_rpc, &[tx], "event-emitting outbound").await;

    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            Ok((count_events(
                &l1_rpc,
                w.outbound_value,
                IValue::ValueSet::SIGNATURE_HASH,
                0,
            )
            .await?
                >= 1)
                .then_some(()))
        }
    })
    .await
    .expect("destination event was not retained in the L1 block");
    assert_eq!(
        l2_value(&l1_rpc, w.outbound_value).await.unwrap(),
        U256::from(67u64),
        "the event must correspond to the requested destination write",
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_dynamic_and_empty_bytes_returns_are_preserved_distinctly() {
    // Length detects truncation; hash verifies raw ABI bytes, not shape.
    let w = setup_cross_chain_return_data().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let payload = Bytes::from(vec![0xa5; 96]);
    let dynamic_call = IReturnData::echoCall {
        value: payload.clone(),
    }
    .abi_encode();
    let dynamic_return_hash = keccak256(payload.abi_encode());
    let dynamic_tx = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.return_data_wrapper),
        U256::ZERO,
        IReturnDataWrapper::callAndRecordCall {
            data: dynamic_call.into(),
        }
        .abi_encode(),
        1_200_000,
    )
    .await
    .expect("dynamic-return transaction must be admitted");
    assert_all_transactions_succeeded(&l1_rpc, &[dynamic_tx], "dynamic return").await;

    // Check both expected ABI size and exact raw return bytes.
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            let len = return_data_length(&l1_rpc, w.return_data_wrapper).await?;
            let hash = return_data_hash(&l1_rpc, w.return_data_wrapper).await?;
            Ok((len == U256::from(160u64) && hash == dynamic_return_hash).then_some(()))
        }
    })
    .await
    .expect("dynamic return data was truncated or not recorded");

    let empty_tx = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.return_data_wrapper),
        U256::ZERO,
        IReturnDataWrapper::callAndRecordCall {
            data: IReturnData::emptyBytesCall {}.abi_encode().into(),
        }
        .abi_encode(),
        1_200_000,
    )
    .await
    .expect("empty-bytes-return transaction must be admitted");
    assert_all_transactions_succeeded(&l1_rpc, &[empty_tx], "empty bytes return").await;
    let empty_return_hash = keccak256(Bytes::new().abi_encode());

    // ABI-encoded `bytes(\"\")` is two words, unlike an empty return buffer.
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            let len = return_data_length(&l1_rpc, w.return_data_wrapper).await?;
            let hash = return_data_hash(&l1_rpc, w.return_data_wrapper).await?;
            Ok((len == U256::from(64u64) && hash == empty_return_hash).then_some(()))
        }
    })
    .await
    .expect("ABI-encoded empty bytes must not be treated as no return data");
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_nested_contract_to_contract_to_proxy_preserves_source_attribution() {
    // Only the inner frame invokes the proxy and receives its return.
    let w = setup_cross_chain_nested_setter().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let value_l2 = w.world.value_l2;
    let value = U256::from(89u64);
    let tx = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(w.nested_setter_outer),
        U256::ZERO,
        INestedSetterOuter::setViaInnerCall { v: value }.abi_encode(),
        1_200_000,
    )
    .await
    .expect("nested wrapper transaction must be admitted");
    assert_all_transactions_succeeded(&l1_rpc, &[tx], "nested inbound proxy call").await;
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move { Ok((l2_value(&l2_rpc, value_l2).await? == value).then_some(())) }
    })
    .await
    .expect("nested proxy call did not reach L2");

    // The return must target the inner caller, not the outer transaction entry.
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            Ok(
                (nested_proxy_calls(&l1_rpc, w.nested_setter_inner).await? == U256::from(1u64))
                    .then_some(()),
            )
        }
    })
    .await
    .expect("inner wrapper did not receive the cross-chain return");
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_nested_contract_to_contract_to_proxy_preserves_source_attribution() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let inner = deploy_nested_setter_inner(
        &l2_rpc,
        common::TARGET_DEPLOYER,
        w.l2_chain_id,
        w.outbound_proxy,
    )
    .await
    .unwrap();
    let outer = deploy_nested_setter_outer(&l2_rpc, common::TARGET_DEPLOYER, w.l2_chain_id, inner)
        .await
        .unwrap();
    let value = U256::from(97u64);

    let tx = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(outer),
        U256::ZERO,
        INestedSetterOuter::setViaInnerCall { v: value }.abi_encode(),
        1_200_000,
    )
    .await
    .expect("nested outbound wrapper transaction must be admitted");
    assert_all_transactions_succeeded(&l2_rpc, &[tx], "nested outbound proxy call").await;
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move { Ok((l2_value(&l1_rpc, w.outbound_value).await? == value).then_some(())) }
    })
    .await
    .expect("nested proxy call did not reach L1");
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = l2_rpc.clone();
        async move { Ok((nested_proxy_calls(&l2_rpc, inner).await? == U256::from(1u64)).then_some(())) }
    })
    .await
    .expect("inner wrapper did not receive the cross-chain return");
    w.node.assert_no_process_death();
}
