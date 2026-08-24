//! Cross-chain integration and nonce-gap regression tests.

use alloy_primitives::{Address, Bytes, TxHash, U256, keccak256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::{SolCall, SolError, SolEvent, SolValue};

use eez_protocol::EEZL2_ADDRESS;
use eez_testkit::signals;
use eez_testkit::{
    ANVIL_KEY_1, DEV_CHAIN_ID, IEEZL2Direct, IEmptyCall, INBOUND_USER, INestedSetterInner,
    INestedSetterOuter, IReturnData, IReturnDataWrapper, IRevertBubbleWrapper, IRevertingTarget,
    ISetterWrapper, IValue, IValueNoRet, L1_ROLLUP_ID, OUTBOUND_USER, ProverMutation,
    SETTLE_TIMEOUT, Scenario, ScenarioCall, StateRead, TARGET_DEPLOYER, account_code,
    assert_latest_batch_signature, batches_posted, call_read, call_revert_data,
    completed_proxy_calls, count_events, cross_chain_source_proxy, deploy_nested_setter_inner,
    deploy_nested_setter_outer, events_since, l2_balance, l2_value, last_proxy_result,
    onchain_nonce, read_state_word, receipt_ok, run_scenarios, safe_block_state_root, setter_call,
    setup_cross_chain, setup_cross_chain_codeless, setup_cross_chain_empty_call,
    setup_cross_chain_nested_setter, setup_cross_chain_outbound_return_data,
    setup_cross_chain_proxied, setup_cross_chain_return_data, setup_cross_chain_reverting,
    sign_and_send, signer_address, state_root, value_no_ret, value_read, wait_for,
};

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
        let landed = wait_for(
            SETTLE_TIMEOUT,
            || async move { receipt_ok(rpc_url, hash).await },
        )
        .await;
        let status = match landed {
            Ok(status) => status,
            Err(err) => {
                panic!(
                    "{label} transaction {hash} did not land: {err:#}{}",
                    w.settlement_diagnostics()
                );
            }
        };
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

fn calls_read(target: Address) -> StateRead {
    call_read(target, "calls()", IEmptyCall::callsCall {}.abi_encode())
}

fn received_read(target: Address) -> StateRead {
    call_read(
        target,
        "received()",
        IEmptyCall::receivedCall {}.abi_encode(),
    )
}

fn last_value_read(target: Address) -> StateRead {
    call_read(
        target,
        "lastValue()",
        IEmptyCall::lastValueCall {}.abi_encode(),
    )
}

fn completed_calls_read(wrapper: Address) -> StateRead {
    call_read(
        wrapper,
        "completedProxyCalls()",
        ISetterWrapper::completedProxyCallsCall {}.abi_encode(),
    )
}

fn return_length_read(wrapper: Address) -> StateRead {
    call_read(
        wrapper,
        "lastReturnDataLength()",
        IReturnDataWrapper::lastReturnDataLengthCall {}.abi_encode(),
    )
}

fn return_hash_read(wrapper: Address) -> StateRead {
    call_read(
        wrapper,
        "lastReturnDataHash()",
        IReturnDataWrapper::lastReturnDataHashCall {}.abi_encode(),
    )
}

// Derive the source address observed on the destination chain.
async fn attributed_sender(
    destination_rpc: &str,
    manager: Address,
    source: Address,
    source_rollup_id: u64,
) -> Address {
    cross_chain_source_proxy(destination_rpc, manager, source, source_rollup_id)
        .await
        .expect("derive destination-side source proxy")
}

// Assert the number and sender of newly emitted `ValueSet` events.
async fn assert_value_set_attribution(
    rpc: &str,
    target: Address,
    before: usize,
    expected_new: usize,
    expected_sender: Address,
    label: &str,
) {
    let logs = wait_for(SETTLE_TIMEOUT, || {
        let rpc = rpc.to_owned();
        async move {
            let logs = events_since(&rpc, target, IValue::ValueSet::SIGNATURE_HASH, 0).await?;
            Ok((logs.len() >= before + expected_new).then_some(logs))
        }
    })
    .await
    .unwrap_or_else(|err| panic!("{label}: destination events were not retained: {err:#}"));
    assert_eq!(
        logs.len(),
        before + expected_new,
        "{label} must emit exactly {expected_new} new ValueSet event(s)",
    );
    for log in &logs[before..] {
        let event = IValue::ValueSet::decode_log(&log.inner).expect("decode ValueSet");
        assert_eq!(
            event.by, expected_sender,
            "{label} must attribute the destination write to the originating source proxy",
        );
    }
}

/// Exercise one signed and derived transaction in each direction, including
/// verification of the posted proof's recomputed public-input hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minimal_bidirectional_cross_chain_smoke() {
    let attester = signer_address(ANVIL_KEY_1).unwrap();
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();

    let inbound = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
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
        onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
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

    let (eez, rollup_id) = (w.cfg.eez_address, w.cfg.rollup_id);
    let converged = wait_for(SETTLE_TIMEOUT, || {
        let (l1_rpc, l2_rpc) = (l1_rpc.clone(), l2_rpc.clone());
        async move {
            let inbound_applied = l2_value(&l2_rpc, w.value_l2).await? == U256::from(41u64);
            let outbound_applied = l2_value(&l1_rpc, w.outbound_value).await? == U256::from(43u64);
            let l1_root = state_root(&l1_rpc, eez, rollup_id).await?;
            let l2_root = safe_block_state_root(&l2_rpc).await?;
            Ok((inbound_applied && outbound_applied && l2_root == Some(l1_root)).then_some(()))
        }
    })
    .await;
    if let Err(err) = converged {
        panic!(
            "minimal smoke did not settle both directions: {err:#}{}",
            w.settlement_diagnostics(),
        );
    }

    assert!(
        batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block)
            .await
            .unwrap()
            >= 1,
        "minimal smoke must post at least one batch",
    );
    assert_latest_batch_signature(&l1_rpc, &w.dep, attester)
        .await
        .expect("posted proof must recover over the recomputed public-input hash");
    assert_eq!(
        w.node
            .count_signals(&[
                signals::COMPOSER_INBOUND_POISON_EVICTED,
                signals::COMPOSER_OUTBOUND_POISON_EVICTED,
                signals::TX_POISON_EVICTED,
                signals::TX_NONCE_CHAIN_EVICTED,
            ])
            .unwrap(),
        0,
        "valid smoke transactions must not be evicted",
    );
    w.node.assert_no_divergence_failure_logs();
}

async fn assert_real_signer_rejects(mutation: ProverMutation, tampered_input: &str) {
    let attester = signer_address(ANVIL_KEY_1).unwrap();
    let w = setup_cross_chain_proxied(mutation, attester).await.unwrap();
    let l1_rpc = w.l1_rpc();
    let proxy = w.prover_proxy.as_ref().expect("real signer proxy");

    let rejected = wait_for(SETTLE_TIMEOUT, || async {
        Ok((proxy.rejections() >= 1).then_some(()))
    })
    .await;
    if rejected.is_err() {
        w.proof_signer.assert_alive();
        assert_ne!(
            proxy.attempts(),
            0,
            "composer never proved a window, so tampered {tampered_input} was never exercised",
        );
        panic!(
            "signer accepted tampered {tampered_input}: {} attempts, {} attested",
            proxy.attempts(),
            proxy.successes(),
        );
    }
    let batches_after_rejection = batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block)
        .await
        .unwrap();
    let root_after_rejection = state_root(&l1_rpc, w.cfg.eez_address, w.cfg.rollup_id)
        .await
        .unwrap();
    wait_for(SETTLE_TIMEOUT, || async {
        Ok((proxy.rejections() >= 2).then_some(()))
    })
    .await
    .expect("composer did not retry the rejected proof window");
    assert_eq!(
        batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block)
            .await
            .unwrap(),
        batches_after_rejection,
        "batch count advanced while the signer repeatedly rejected tampered {tampered_input}",
    );
    assert_eq!(
        state_root(&l1_rpc, w.cfg.eez_address, w.cfg.rollup_id)
            .await
            .unwrap(),
        root_after_rejection,
        "state root advanced while the signer repeatedly rejected tampered {tampered_input}",
    );
    w.proof_signer.assert_alive();
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_signer_rejects_tampered_post_batch_calldata() {
    assert_real_signer_rejects(ProverMutation::PostBatch, "PostBatch calldata").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_signer_rejects_tampered_witness() {
    assert_real_signer_rejects(ProverMutation::Witness, "witness").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_signer_attester_mismatch_never_submits_a_batch() {
    let wrong_attester = Address::repeat_byte(0x44);
    let w = setup_cross_chain_proxied(ProverMutation::None, wrong_attester)
        .await
        .unwrap();
    let proxy = w.prover_proxy.as_ref().expect("real signer proxy");

    let attested = wait_for(SETTLE_TIMEOUT, || async {
        Ok((proxy.successes() >= 2).then_some(()))
    })
    .await;
    if let Err(err) = attested {
        w.proof_signer.assert_alive();
        panic!("signer never returned the mismatched attestations: {err:#}");
    }
    assert_eq!(
        proxy.rejections(),
        0,
        "the signer must have attested; only the composer may refuse here",
    );
    assert_eq!(
        batches_posted(&w.l1_rpc(), w.cfg.eez_address, w.dep.deploy_block)
            .await
            .unwrap(),
        0,
        "an attestation from an unexpected signer must never reach L1",
    );
    assert_eq!(
        state_root(&w.l1_rpc(), w.cfg.eez_address, w.cfg.rollup_id)
            .await
            .unwrap(),
        w.cfg.initial_state,
        "signer mismatch must leave the registered state root unchanged",
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_directions_zero_value_direct_proxy_success_single_call() {
    let w = setup_cross_chain().await.unwrap();
    run_scenarios(
        &w,
        [Scenario::new("Bidirectional zero-value direct proxy")
            .inbound(setter_call(w.setter_proxy, 41u64))
            .outbound(setter_call(w.outbound_proxy, 43u64))
            .expect_l2_state(value_read(w.value_l2), 41u64)
            .expect_l1_state(value_read(w.outbound_value), 43u64)
            .expect_settled_fully()],
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_empty_calldata_and_zero_value_is_not_skipped() {
    // Verify that an all-zero call is materialized without being rewritten.
    let w = setup_cross_chain_empty_call().await.unwrap();
    Scenario::new("empty-calldata zero-value inbound")
        .inbound(ScenarioCall::new(w.empty_call_proxy, Vec::new()).with_gas_limit(600_000))
        .expect_l2_state(calls_read(w.empty_call_l2), 1u64)
        .expect_l2_state(received_read(w.empty_call_l2), 0u64)
        .expect_l2_state(last_value_read(w.empty_call_l2), 0u64)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_deposit_to_payable_contract_runs_fallback() {
    // `EmptyCall` has no `receive`, so empty calldata exercises its fallback.
    let w = setup_cross_chain_empty_call().await.unwrap();
    let value = U256::from(123_456u64);
    Scenario::new("payable inbound deposit")
        .inbound(
            ScenarioCall::new(w.empty_call_proxy, Vec::new())
                .with_value(value)
                .with_gas_limit(600_000),
        )
        .expect_l2_state(calls_read(w.empty_call_l2), 1u64)
        .expect_l2_state(received_read(w.empty_call_l2), value)
        .expect_l2_state(last_value_read(w.empty_call_l2), 0u64)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_value_and_calldata_apply_atomically() {
    // Bind the transferred value and decoded argument to one destination call.
    let w = setup_cross_chain_empty_call().await.unwrap();
    let value = U256::from(456_789u64);
    let next = U256::from(91u64);
    Scenario::new("value-and-calldata inbound")
        .inbound(
            ScenarioCall::new(
                w.empty_call_proxy,
                IEmptyCall::setValueCall { next }.abi_encode(),
            )
            .with_value(value)
            .with_gas_limit(600_000),
        )
        .expect_l2_state(calls_read(w.empty_call_l2), 1u64)
        .expect_l2_state(received_read(w.empty_call_l2), value)
        .expect_l2_state(last_value_read(w.empty_call_l2), next)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_ccm_l2_outbound_call_is_rejected() {
    // Direct CCM-L2 access is mined, then rejected during execution.
    let w = setup_cross_chain().await.unwrap();
    let l2_rpc = w.l2_rpc();
    let caller: Address = signer_address(OUTBOUND_USER).unwrap();
    let tx = sign_and_send(
        &l2_rpc,
        OUTBOUND_USER,
        w.l2_chain_id,
        onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
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

    // Pin the failure to `EEZL2`'s authorization check.
    let revert_data = call_revert_data(
        &l2_rpc,
        caller,
        EEZL2_ADDRESS,
        IEEZL2Direct::executeCrossChainCallCall {
            sourceAddress: caller,
            callData: Bytes::new(),
        }
        .abi_encode(),
    )
    .await
    .expect("direct CCM-L2 call must revert with data");
    assert_eq!(
        revert_data.as_ref(),
        IEEZL2Direct::UnauthorizedProxy::SELECTOR.as_slice(),
        "direct CCM-L2 access must be rejected as an unauthorized proxy",
    );
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
    // Deltas, not totals: batches and bundles counted from deployment include
    // the background minimal batches the composer posts on an idle chain, so an
    // absolute threshold can be met without this wave contributing anything.
    let batches_before = batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block)
        .await
        .unwrap();
    let bundles_before = w.node.count_signal(signals::BUNDLE_ACCEPTED).unwrap();

    let mut inbound_hashes = Vec::new();
    let mut outbound_hashes = Vec::new();

    for (set_v, dep_v) in WAVE_SETTERS.iter().zip(WAVE_DEPOSITS.iter()) {
        // Let each wave settle before deriving the next source nonces. A compose
        // tick removes held transactions before they land on the source chain,
        // so caching nonces across waves races the ingress gate's
        // `on_chain + held` validation.
        let mut l1_nonce = onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
        let mut l2_nonce = onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
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
            &w,
            &l1_rpc,
            &inbound_hashes[inbound_wave_start..],
            "inbound wave",
        )
        .await;
        assert_all_transactions_succeeded(
            &w,
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
            let converged = l2_value(&l2_rpc, w.value_l2).await? == setter_final
                && l2_value(&l1_rpc, w.outbound_value).await? == setter_final
                && value_no_ret(&l2_rpc, w.inbound_no_ret).await? == no_ret_final
                && value_no_ret(&l1_rpc, w.outbound_no_ret).await? == no_ret_final
                && l2_balance(&l2_rpc, w.recipient).await? == recipient_final
                && l2_balance(&l1_rpc, w.withdrawal_recipient).await? == withdrawal_final;
            Ok(converged.then_some(()))
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
            let l2_root = safe_block_state_root(&l2_rpc).await?;
            Ok(l2_root.filter(|r| *r == l1_root).map(|_| ()))
        }
    })
    .await
    .expect("L1 stored stateRoot never matched L2 safe stateRoot");

    let batches = batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block)
        .await
        .unwrap();
    assert!(
        batches >= batches_before + WAVE_SETTERS.len(),
        "expected ≥{} new BatchPosted events, got {}",
        WAVE_SETTERS.len(),
        batches - batches_before,
    );
    assert_eq!(
        w.node
            .count_signals(&[
                signals::DERIVER_STATE_DIVERGED_PRE,
                signals::DERIVER_STATE_DIVERGED_POST,
            ])
            .unwrap(),
        0,
        "zero state-root divergence events",
    );
    assert_eq!(
        w.node
            .count_signals(&[
                signals::COMPOSER_INBOUND_POISON_EVICTED,
                signals::COMPOSER_OUTBOUND_POISON_EVICTED,
                signals::TX_POISON_EVICTED,
                signals::TX_NONCE_CHAIN_EVICTED,
            ])
            .unwrap(),
        0,
        "all non-poison wave transactions must settle without eviction",
    );

    assert!(
        w.node.count_signal(signals::BUNDLE_ACCEPTED).unwrap() > bundles_before,
        "embedded dev L1 eth_sendBundle was exercised by this wave",
    );
    assert_eq!(
        w.node
            .count_signal(signals::BUNDLE_MEMPOOL_FALLBACK)
            .unwrap(),
        0,
        "composer must not fall back to eth_sendRawTransaction",
    );
    w.node.assert_no_divergence_failure_logs();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_destination_events_are_retained_in_the_l2_block() {
    let w = setup_cross_chain().await.unwrap();
    let l2_rpc = w.l2_rpc();
    // Capture the event count before submitting the scenario.
    let before = count_events(&l2_rpc, w.value_l2, IValue::ValueSet::SIGNATURE_HASH, 0)
        .await
        .unwrap();
    // The destination must see the L1 sender's source proxy.
    let expected_sender = attributed_sender(
        &l2_rpc,
        EEZL2_ADDRESS,
        signer_address(INBOUND_USER).unwrap(),
        L1_ROLLUP_ID,
    )
    .await;

    Scenario::new("event-emitting inbound")
        .inbound(setter_call(w.setter_proxy, 61u64).with_gas_limit(600_000))
        .expect_l2_state(value_read(w.value_l2), 61u64)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();

    assert_value_set_attribution(
        &l2_rpc,
        w.value_l2,
        before,
        1,
        expected_sender,
        "event-emitting inbound",
    )
    .await;
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

    // The nonzero empty-buffer hash proves the wrapper ran.
    let empty_hash = keccak256(Bytes::new());
    Scenario::new("codeless registered targets")
        .inbound(ScenarioCall::new(w.inbound_wrapper, data.clone()))
        .outbound(ScenarioCall::new(w.outbound_wrapper, data))
        .expect_l1_state(return_length_read(w.inbound_wrapper), 0u64)
        .expect_l1_state(return_hash_read(w.inbound_wrapper), empty_hash)
        .expect_l2_state(return_length_read(w.outbound_wrapper), 0u64)
        .expect_l2_state(return_hash_read(w.outbound_wrapper), empty_hash)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_dynamic_and_empty_bytes_returns_are_preserved_distinctly() {
    // Length and hash distinguish dynamic bytes from ABI-encoded empty bytes.
    let w = setup_cross_chain_outbound_return_data().await.unwrap();
    let payload = Bytes::from(vec![0x5a; 96]);
    let wrapper = w.return_data_wrapper;
    run_scenarios(
        &w,
        [
            Scenario::new("outbound dynamic return")
                .outbound(
                    ScenarioCall::new(
                        wrapper,
                        IReturnDataWrapper::callAndRecordCall {
                            data: IReturnData::echoCall {
                                value: payload.clone(),
                            }
                            .abi_encode()
                            .into(),
                        }
                        .abi_encode(),
                    )
                    .with_gas_limit(1_200_000),
                )
                .expect_l2_state(return_length_read(wrapper), 160u64)
                .expect_l2_state(return_hash_read(wrapper), keccak256(payload.abi_encode()))
                .expect_settled_fully(),
            Scenario::new("outbound empty-bytes return")
                .outbound(
                    ScenarioCall::new(
                        wrapper,
                        IReturnDataWrapper::callAndRecordCall {
                            data: IReturnData::emptyBytesCall {}.abi_encode().into(),
                        }
                        .abi_encode(),
                    )
                    .with_gas_limit(1_200_000),
                )
                .expect_l2_state(return_length_read(wrapper), 64u64)
                .expect_l2_state(
                    return_hash_read(wrapper),
                    keccak256(Bytes::new().abi_encode()),
                )
                .expect_settled_fully(),
        ],
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_identical_wrapper_proxy_calls_settle_as_ordered_entries() {
    // Identical calls share a semantic hash; entry order must keep both alive.
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let value = U256::from(73u64);
    let wrapped_before = events_since(
        &l1_rpc,
        w.inbound_wrapper,
        ISetterWrapper::Wrapped::SIGNATURE_HASH,
        0,
    )
    .await
    .unwrap()
    .len();
    assert_ne!(
        l2_value(&w.l2_rpc(), w.value_l2).await.unwrap(),
        value,
        "the first call must change destination state for the ordered-return assertion",
    );

    Scenario::new("duplicate inbound proxy calls")
        .inbound(
            ScenarioCall::new(
                w.inbound_wrapper,
                ISetterWrapper::setSameValueTwiceCall { v: value }.abi_encode(),
            )
            .with_gas_limit(1_200_000),
        )
        .expect_l2_state(value_read(w.value_l2), value)
        // Require both independent returns.
        .expect_l1_state(completed_calls_read(w.inbound_wrapper), 2u64)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();

    // Verify that the second return observes the first destination write.
    let wrapped = events_since(
        &l1_rpc,
        w.inbound_wrapper,
        ISetterWrapper::Wrapped::SIGNATURE_HASH,
        0,
    )
    .await
    .unwrap();
    let results = wrapped[wrapped_before..]
        .iter()
        .map(|log| {
            let event = ISetterWrapper::Wrapped::decode_log(&log.inner).unwrap();
            (event.input, event.ok, event.changed, event.newValue)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results,
        vec![(value, true, true, value), (value, true, false, value)],
        "the second ordered call must observe the first call's destination write",
    );
    assert_eq!(
        last_proxy_result(&l1_rpc, w.inbound_wrapper).await.unwrap(),
        (false, value),
        "the final ordered call must deliver its exact decoded return",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_multiple_proxy_calls_in_one_transaction_are_evicted() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let value = U256::from(79u64);
    let source_nonce = onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
    let destination_before = l2_value(&l1_rpc, w.outbound_value).await.unwrap();
    let signal_cursor = w.node.signal_cursor().unwrap();

    let tx = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        source_nonce,
        Some(w.outbound_wrapper),
        U256::ZERO,
        ISetterWrapper::setSameValueTwiceCall { v: value }.abi_encode(),
        1_200_000,
    )
    .await
    .expect("duplicate outbound proxy-call transaction must be admitted");

    let tx_hash = tx.to_string();
    wait_for(SETTLE_TIMEOUT, || async {
        let rejected = w
            .node
            .signals_since(signal_cursor)?
            .into_iter()
            .any(|signal| {
                signal.name == signals::COMPOSER_OUTBOUND_MULTICALL_UNSUPPORTED
                    && signal
                        .fields
                        .get("tx_hash")
                        .and_then(serde_json::Value::as_str)
                        == Some(tx_hash.as_str())
                    && signal.u64("rollup_id").ok() == Some(w.cfg.rollup_id)
                    && signal.u64("entries").ok() == Some(2)
            });
        Ok(rejected.then_some(()))
    })
    .await
    .expect("composer did not report the unsupported outbound multicall");

    // The signal fires before poison-root handling and pool eviction finish, so
    // an immediate absent-receipt check races and proves nothing about later
    // blocks. Reusing the released nonce is the conclusive test: a replacement
    // can only land if the rejected transaction is really gone from the pool,
    // and once the replacement has mined the rejected hash can never appear.
    let replacement = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        source_nonce,
        Some(w.outbound_wrapper),
        U256::ZERO,
        ISetterWrapper::setViaProxyCall { v: value }.abi_encode(),
        1_200_000,
    )
    .await
    .expect("a replacement at the released nonce must be admitted");
    assert_all_transactions_succeeded(&l2_rpc, &[replacement], "eviction replacement").await;

    assert_eq!(
        receipt_ok(&l2_rpc, tx).await.unwrap(),
        None,
        "an evicted outbound multicall must never land on its source chain",
    );
    assert_eq!(
        completed_proxy_calls(&l2_rpc, w.outbound_wrapper)
            .await
            .unwrap(),
        U256::from(1u64),
        "only the replacement, never the rejected multicall, may execute the wrapper",
    );
    // The replacement writes the same value the rejected multicall would have,
    // so a changed destination here means the replacement did it — and the
    // wrapper counter above bounds it to exactly one call.
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move { Ok((l2_value(&l1_rpc, w.outbound_value).await? == value).then_some(())) }
    })
    .await
    .expect("the replacement outbound call did not reach its destination");
    assert_ne!(
        destination_before, value,
        "the replacement must change destination state for that assertion to bind",
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_identical_calls_in_separate_transactions_settle_as_ordered_entries() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let value = U256::from(79u64);
    let nonce = onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
    let call = ISetterWrapper::setViaProxyCall { v: value }.abi_encode();
    assert_ne!(
        l2_value(&l1_rpc, w.outbound_value).await.unwrap(),
        value,
        "the first call must change destination state for the ordered-return assertion",
    );

    let provider = ProviderBuilder::new().connect_http(l2_rpc.parse().unwrap());
    let wrapped_filter = Filter::new()
        .address(w.outbound_wrapper)
        .event_signature(ISetterWrapper::Wrapped::SIGNATURE_HASH)
        .from_block(0u64);
    let wrapped_before = provider.get_logs(&wrapped_filter).await.unwrap().len();

    // Submit immediately after a batch so both transactions are available to
    // the next drain. The same-block assertion below fails if this window is
    // missed instead of silently weakening the state-dependency scenario.
    let batches_before = batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block)
        .await
        .unwrap();
    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move {
            let batches = batches_posted(&l1_rpc, w.cfg.eez_address, w.dep.deploy_block).await?;
            Ok((batches > batches_before).then_some(()))
        }
    })
    .await
    .expect("composer did not open a drain window");

    let first = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        nonce,
        Some(w.outbound_wrapper),
        U256::ZERO,
        call.clone(),
        1_200_000,
    )
    .await
    .expect("first duplicate outbound proxy-call transaction must be admitted");
    let second = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        nonce + 1,
        Some(w.outbound_wrapper),
        U256::ZERO,
        call,
        1_200_000,
    )
    .await
    .expect("second duplicate outbound proxy-call transaction must be admitted");
    assert_all_transactions_succeeded(&l2_rpc, &[first, second], "duplicate outbound proxy calls")
        .await;

    wait_for(SETTLE_TIMEOUT, || {
        let provider = provider.clone();
        let wrapped_filter = wrapped_filter.clone();
        async move {
            let count = provider.get_logs(&wrapped_filter).await?.len();
            Ok((count == wrapped_before + 2).then_some(()))
        }
    })
    .await
    .expect("both ordered outbound calls did not return to the source wrapper");

    let wrapped = provider.get_logs(&wrapped_filter).await.unwrap();
    let wrapped = &wrapped[wrapped_before..];
    let sync_block = wrapped[0]
        .block_number
        .expect("first wrapper result must belong to a mined Sync block");
    assert_eq!(
        wrapped[1].block_number,
        Some(sync_block),
        "the state dependency must be exercised within one Sync block",
    );
    assert_eq!(
        [wrapped[0].transaction_hash, wrapped[1].transaction_hash],
        [Some(first), Some(second)],
        "wrapper results must retain source transaction order",
    );
    let results = wrapped
        .iter()
        .map(|log| {
            let event = ISetterWrapper::Wrapped::decode_log(&log.inner).unwrap();
            (event.input, event.ok, event.changed, event.newValue)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results,
        vec![(value, true, true, value), (value, true, false, value),],
        "the second call must observe the first call's destination-state change",
    );

    wait_for(SETTLE_TIMEOUT, || {
        let l1_rpc = l1_rpc.clone();
        async move { Ok((l2_value(&l1_rpc, w.outbound_value).await? == value).then_some(())) }
    })
    .await
    .expect("ordered outbound calls did not settle on L1");
    assert_eq!(
        l2_value(&l1_rpc, w.outbound_value).await.unwrap(),
        value,
        "the ordered calls must leave the destination at the requested value",
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_destination_events_are_retained_in_the_l1_block() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let before = count_events(
        &l1_rpc,
        w.outbound_value,
        IValue::ValueSet::SIGNATURE_HASH,
        0,
    )
    .await
    .unwrap();
    let expected_sender = attributed_sender(
        &l1_rpc,
        w.cfg.eez_address,
        signer_address(OUTBOUND_USER).unwrap(),
        w.cfg.rollup_id,
    )
    .await;

    Scenario::new("event-emitting outbound")
        .outbound(setter_call(w.outbound_proxy, 67u64))
        .expect_l1_state(value_read(w.outbound_value), 67u64)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();

    assert_value_set_attribution(
        &l1_rpc,
        w.outbound_value,
        before,
        1,
        expected_sender,
        "event-emitting outbound",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_dynamic_and_empty_bytes_returns_are_preserved_distinctly() {
    // Mirror the outbound return-data assertions.
    let w = setup_cross_chain_return_data().await.unwrap();
    let payload = Bytes::from(vec![0xa5; 96]);
    let wrapper = w.return_data_wrapper;
    run_scenarios(
        &w,
        [
            Scenario::new("inbound dynamic return")
                .inbound(
                    ScenarioCall::new(
                        wrapper,
                        IReturnDataWrapper::callAndRecordCall {
                            data: IReturnData::echoCall {
                                value: payload.clone(),
                            }
                            .abi_encode()
                            .into(),
                        }
                        .abi_encode(),
                    )
                    .with_gas_limit(1_200_000),
                )
                .expect_l1_state(return_length_read(wrapper), 160u64)
                .expect_l1_state(return_hash_read(wrapper), keccak256(payload.abi_encode()))
                .expect_settled_fully(),
            Scenario::new("inbound empty-bytes return")
                .inbound(
                    ScenarioCall::new(
                        wrapper,
                        IReturnDataWrapper::callAndRecordCall {
                            data: IReturnData::emptyBytesCall {}.abi_encode().into(),
                        }
                        .abi_encode(),
                    )
                    .with_gas_limit(1_200_000),
                )
                .expect_l1_state(return_length_read(wrapper), 64u64)
                .expect_l1_state(
                    return_hash_read(wrapper),
                    keccak256(Bytes::new().abi_encode()),
                )
                .expect_settled_fully(),
        ],
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_nested_contract_to_contract_to_proxy_preserves_source_attribution() {
    // Only the inner frame invokes the proxy, so it must own the source identity.
    let w = setup_cross_chain_nested_setter().await.unwrap();
    let l2_rpc = w.l2_rpc();
    let value_l2 = w.world.value_l2;
    let value = U256::from(89u64);
    let before = count_events(&l2_rpc, value_l2, IValue::ValueSet::SIGNATURE_HASH, 0)
        .await
        .unwrap();
    let inner_sender =
        attributed_sender(&l2_rpc, EEZL2_ADDRESS, w.nested_setter_inner, L1_ROLLUP_ID).await;
    let outer_sender =
        attributed_sender(&l2_rpc, EEZL2_ADDRESS, w.nested_setter_outer, L1_ROLLUP_ID).await;
    let eoa_sender = attributed_sender(
        &l2_rpc,
        EEZL2_ADDRESS,
        signer_address(INBOUND_USER).unwrap(),
        L1_ROLLUP_ID,
    )
    .await;
    assert!(
        inner_sender != outer_sender && inner_sender != eoa_sender,
        "the attribution candidates must be distinguishable for this assertion to mean anything",
    );

    Scenario::new("nested inbound proxy call")
        .inbound(
            ScenarioCall::new(
                w.nested_setter_outer,
                INestedSetterOuter::setViaInnerCall { v: value }.abi_encode(),
            )
            .with_gas_limit(1_200_000),
        )
        .expect_l2_state(value_read(value_l2), value)
        // The return must reach the inner caller.
        .expect_l1_state(
            call_read(
                w.nested_setter_inner,
                "completedProxyCalls()",
                INestedSetterInner::completedProxyCallsCall {}.abi_encode(),
            ),
            1u64,
        )
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();

    assert_value_set_attribution(
        &l2_rpc,
        value_l2,
        before,
        1,
        inner_sender,
        "nested inbound proxy call",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_nested_contract_to_contract_to_proxy_preserves_source_attribution() {
    // The L1 destination must see the inner contract's source proxy.
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let inner =
        deploy_nested_setter_inner(&l2_rpc, TARGET_DEPLOYER, w.l2_chain_id, w.outbound_proxy)
            .await
            .unwrap();
    let outer = deploy_nested_setter_outer(&l2_rpc, TARGET_DEPLOYER, w.l2_chain_id, inner)
        .await
        .unwrap();
    let value = U256::from(97u64);
    let before = count_events(
        &l1_rpc,
        w.outbound_value,
        IValue::ValueSet::SIGNATURE_HASH,
        0,
    )
    .await
    .unwrap();
    let inner_sender = attributed_sender(&l1_rpc, w.cfg.eez_address, inner, w.cfg.rollup_id).await;
    let outer_sender = attributed_sender(&l1_rpc, w.cfg.eez_address, outer, w.cfg.rollup_id).await;
    let eoa_sender = attributed_sender(
        &l1_rpc,
        w.cfg.eez_address,
        signer_address(OUTBOUND_USER).unwrap(),
        w.cfg.rollup_id,
    )
    .await;
    assert!(
        inner_sender != outer_sender && inner_sender != eoa_sender,
        "the attribution candidates must be distinguishable for this assertion to mean anything",
    );

    Scenario::new("nested outbound proxy call")
        .outbound(
            ScenarioCall::new(
                outer,
                INestedSetterOuter::setViaInnerCall { v: value }.abi_encode(),
            )
            .with_gas_limit(1_200_000),
        )
        .expect_l1_state(value_read(w.outbound_value), value)
        .expect_l2_state(
            call_read(
                inner,
                "completedProxyCalls()",
                INestedSetterInner::completedProxyCallsCall {}.abi_encode(),
            ),
            1u64,
        )
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();

    assert_value_set_attribution(
        &l1_rpc,
        w.outbound_value,
        before,
        1,
        inner_sender,
        "nested outbound proxy call",
    )
    .await;
}

fn failures_read(wrapper: Address) -> StateRead {
    call_read(
        wrapper,
        "failures()",
        IRevertBubbleWrapper::failuresCall {}.abi_encode(),
    )
}

fn successes_read(wrapper: Address) -> StateRead {
    call_read(
        wrapper,
        "successes()",
        IRevertBubbleWrapper::successesCall {}.abi_encode(),
    )
}

fn target_calls_read(target: Address) -> StateRead {
    call_read(
        target,
        "calls()",
        IRevertingTarget::callsCall {}.abi_encode(),
    )
}

fn target_last_value_read(target: Address) -> StateRead {
    call_read(
        target,
        "lastValue()",
        IRevertingTarget::lastValueCall {}.abi_encode(),
    )
}

fn record_call(wrapper: Address, inner: Vec<u8>) -> ScenarioCall {
    ScenarioCall::new(
        wrapper,
        IRevertBubbleWrapper::callAndRecordCall { data: inner.into() }.abi_encode(),
    )
    .with_gas_limit(1_200_000)
}

fn revert_cases(value: U256) -> [(&'static str, Vec<u8>); 3] {
    [
        (
            "custom error",
            IRevertingTarget::revertCustomCall { v: value }.abi_encode(),
        ),
        (
            "string reason",
            IRevertingTarget::revertStringCall { v: value }.abi_encode(),
        ),
        (
            // Writes two slots before reverting, so a surviving write is visible.
            "write then revert",
            IRevertingTarget::writeThenRevertCall { v: value }.abi_encode(),
        ),
    ]
}

async fn assert_destination_untouched(rpc: &str, target: Address, label: &str) {
    for read in [target_calls_read(target), target_last_value_read(target)] {
        let observed = read_state_word(rpc, &read).await.unwrap();
        assert_eq!(
            observed,
            U256::ZERO,
            "{label}: reverted destination must not persist state",
        );
    }
}

async fn wait_for_poison_eviction(
    w: &eez_testkit::CrossChainWorld,
    cursor: usize,
    hash: TxHash,
    signal: &str,
    label: &str,
) {
    let tx_hash = hash.to_string();
    wait_for(SETTLE_TIMEOUT, || async {
        let evicted = w.node.signals_since(cursor)?.into_iter().any(|record| {
            record.name == signal
                && record.fields.get("tx_hash").and_then(|v| v.as_str()) == Some(tx_hash.as_str())
        });
        Ok(evicted.then_some(()))
    })
    .await
    .unwrap_or_else(|err| panic!("{label} was not poison-evicted: {err:#}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_reverting_destination_is_evicted_without_state_changes() {
    let w = setup_cross_chain_reverting().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let wrapper = w.inbound_wrapper;
    let target = w.reverting_target_l2;
    let value = U256::from(31u64);

    for (label, inner) in revert_cases(value) {
        let nonce = onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
        let call = record_call(wrapper, inner);
        let cursor = w.node.signal_cursor().unwrap();
        let hash = sign_and_send(
            &w.l1_xchain(),
            INBOUND_USER,
            DEV_CHAIN_ID,
            nonce,
            Some(call.to),
            call.value,
            call.data,
            call.gas_limit,
        )
        .await
        .unwrap_or_else(|err| panic!("submit inbound {label}: {err:#}"));
        wait_for_poison_eviction(
            &w,
            cursor,
            hash,
            signals::COMPOSER_INBOUND_POISON_EVICTED,
            label,
        )
        .await;
        assert_eq!(
            receipt_ok(&l1_rpc, hash).await.unwrap(),
            None,
            "inbound {label} must not settle on its source chain",
        );
        assert_eq!(
            read_state_word(&l1_rpc, &failures_read(wrapper))
                .await
                .unwrap(),
            U256::ZERO
        );
        assert_eq!(
            read_state_word(&l1_rpc, &successes_read(wrapper))
                .await
                .unwrap(),
            U256::ZERO
        );
        assert_destination_untouched(&l2_rpc, target, label).await;
    }

    // Reusing the released nonce proves the poison entries left no stuck gap.
    Scenario::new("inbound valid call after reverts")
        .inbound(record_call(
            wrapper,
            IRevertingTarget::succeedCall { v: value }.abi_encode(),
        ))
        .expect_l2_state(target_calls_read(target), 1u64)
        .expect_l2_state(target_last_value_read(target), value)
        .expect_l1_state(successes_read(wrapper), 1u64)
        .expect_l1_state(failures_read(wrapper), 0u64)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_reverting_destination_is_evicted_without_state_changes() {
    let w = setup_cross_chain_reverting().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let wrapper = w.outbound_wrapper;
    let target = w.reverting_target_l1;
    let value = U256::from(37u64);

    for (label, inner) in revert_cases(value) {
        let nonce = onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
        let call = record_call(wrapper, inner);
        let cursor = w.node.signal_cursor().unwrap();
        let hash = sign_and_send(
            &w.l2_xchain(),
            OUTBOUND_USER,
            w.l2_chain_id,
            nonce,
            Some(call.to),
            call.value,
            call.data,
            call.gas_limit,
        )
        .await
        .unwrap_or_else(|err| panic!("submit outbound {label}: {err:#}"));
        wait_for_poison_eviction(
            &w,
            cursor,
            hash,
            signals::COMPOSER_OUTBOUND_POISON_EVICTED,
            label,
        )
        .await;
        assert_eq!(
            receipt_ok(&l2_rpc, hash).await.unwrap(),
            None,
            "outbound {label} must not settle on its source chain",
        );
        assert_eq!(
            read_state_word(&l2_rpc, &failures_read(wrapper))
                .await
                .unwrap(),
            U256::ZERO
        );
        assert_eq!(
            read_state_word(&l2_rpc, &successes_read(wrapper))
                .await
                .unwrap(),
            U256::ZERO
        );
        assert_destination_untouched(&l1_rpc, target, label).await;
    }

    Scenario::new("outbound valid call after reverts")
        .outbound(record_call(
            wrapper,
            IRevertingTarget::succeedCall { v: value }.abi_encode(),
        ))
        .expect_l1_state(target_calls_read(target), 1u64)
        .expect_l1_state(target_last_value_read(target), value)
        .expect_l2_state(successes_read(wrapper), 1u64)
        .expect_l2_state(failures_read(wrapper), 0u64)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();
}
