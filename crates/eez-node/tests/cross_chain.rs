//! Cross-chain integration and nonce-gap regression tests.

use alloy_primitives::{Address, TxHash, U256};
use alloy_sol_types::SolCall;

mod common;
use common::{
    ANVIL_KEY_1, DEV_CHAIN_ID, INBOUND_USER, ISetterWrapper, IValue, IValueNoRet, OUTBOUND_USER,
    ProverMutation, SETTLE_TIMEOUT, assert_latest_batch_signature, batches_posted, l2_balance,
    l2_value, pending_nonce, receipt_ok, setup_cross_chain, setup_cross_chain_proxied,
    sign_and_send, signer_address, state_root, value_no_ret, wait_for,
};

const WAVE_SETTERS: &[u64] = &[7, 11, 17];
const WAVE_DEPOSITS: &[u128] = &[
    1_000_000_000_000_000,
    2_000_000_000_000_000,
    3_000_000_000_000_000,
];

async fn assert_all_transactions_succeeded(
    w: &common::CrossChainWorld,
    rpc_url: &str,
    hashes: &[TxHash],
    label: &str,
) {
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

/// Exercises one real signed and derived transaction in each direction.
/// The final signature check covers the recomputed public-input hash from L1.
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

    assert_all_transactions_succeeded(&w, &l1_rpc, &[inbound], "inbound smoke").await;
    assert_all_transactions_succeeded(&w, &l2_rpc, &[outbound], "outbound smoke").await;

    let (eez, rollup_id) = (w.cfg.eez_address, w.cfg.rollup_id);
    let converged = wait_for(SETTLE_TIMEOUT, || {
        let (l1_rpc, l2_rpc) = (l1_rpc.clone(), l2_rpc.clone());
        async move {
            let inbound_applied = l2_value(&l2_rpc, w.value_l2).await? == U256::from(41u64);
            let outbound_applied = l2_value(&l1_rpc, w.outbound_value).await? == U256::from(43u64);
            let l1_root = state_root(&l1_rpc, eez, rollup_id).await?;
            let l2_root = common::safe_block_state_root(&l2_rpc).await?;
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
        w.node.log_count_matching(&["evicting"]).unwrap(),
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

    // A signer rejection distinguishes validation from a stalled proof path.
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

/// `ResourceExhausted` is opaque to the Composer (including the real signer's
/// checkpoint-quota rejection from #120). A valid transaction may be sacrificed,
/// but it must leave the retry loop after the bounded number of proof episodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opaque_prover_rejection_eventually_evicts_the_candidate() {
    let attester = signer_address(ANVIL_KEY_1).unwrap();
    let w = setup_cross_chain_proxied(ProverMutation::ResourceExhausted, attester)
        .await
        .unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let starting_nonce = pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
    let starting_value = l2_value(&l2_rpc, w.value_l2).await.unwrap();
    let tx_hash = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        starting_nonce,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall { v: U256::from(76) }.abi_encode(),
        600_000,
    )
    .await
    .expect("valid transaction must be admitted before the prover rejects its batch");

    let evicted = wait_for(SETTLE_TIMEOUT, || async {
        let logs = w
            .node
            .log_lines_matching(&["potentially valid user_tx evicted"], 20);
        Ok(logs
            .lines()
            .any(|line| line.contains("ERROR") && line.contains(&tx_hash.to_string()))
            .then_some(()))
    })
    .await;
    if let Err(error) = evicted {
        panic!(
            "opaque prover rejection did not reach bounded eviction: {error:#}{}",
            w.settlement_diagnostics(),
        );
    }

    let proxy = w.prover_proxy.as_ref().expect("rejection proxy");
    assert!(
        proxy.rejections() >= 3,
        "eviction must require three distinct rejected proof episodes"
    );
    assert_eq!(
        receipt_ok(&l1_rpc, tx_hash).await.unwrap(),
        None,
        "a sacrificed valid transaction must not appear to have landed"
    );
    assert_eq!(
        pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        starting_nonce,
        "the rejected transaction must not burn its source nonce"
    );
    assert_eq!(
        l2_value(&l2_rpc, w.value_l2).await.unwrap(),
        starting_value,
        "the rejected cross-chain call must not change its target value"
    );
    assert!(
        proxy.successes() > 0,
        "anchor-only fallback proofs must remain healthy so recovery can make progress"
    );
    w.node.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_signer_attester_mismatch_never_submits_a_batch() {
    let wrong_attester = Address::repeat_byte(0x44);
    let w = setup_cross_chain_proxied(ProverMutation::None, wrong_attester)
        .await
        .unwrap();
    let proxy = w.prover_proxy.as_ref().expect("real signer proxy");
    // Successful responses prove that rejection occurs in the composer.
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

/// Exercises mixed bidirectional calls and transfers over repeated waves.
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

    // Source receipts prove admission; destination effects arrive after settlement.
    let setter_final = U256::from(*WAVE_SETTERS.last().unwrap() + 100);
    let no_ret_final = U256::from(*WAVE_SETTERS.last().unwrap() + 200);
    let recipient_final = recipient_before + U256::from(deposit_sum);
    let withdrawal_final = withdrawal_before + U256::from(deposit_sum);
    let effects_converged = wait_for(SETTLE_TIMEOUT, || {
        let (l1_rpc, l2_rpc) = (l1_rpc.clone(), l2_rpc.clone());
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
    .await;
    if let Err(err) = effects_converged {
        panic!(
            "cross-chain wave effects did not converge: {err:#}{}",
            w.settlement_diagnostics(),
        );
    }

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
    w.node.assert_no_divergence_failure_logs();
}
