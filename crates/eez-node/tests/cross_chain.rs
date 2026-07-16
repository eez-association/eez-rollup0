//! Cross-chain integration and nonce-gap regression tests.

use alloy_primitives::{TxHash, U256, address};
use alloy_sol_types::SolCall;

mod common;
use common::{
    ANVIL_KEY_2, DEV_CHAIN_ID, INBOUND_USER, ISetterWrapper, IValue, IValueNoRet, OUTBOUND_USER,
    SETTLE_TIMEOUT, UNFUNDED_KEY, batches_posted, l2_balance, l2_value, pending_nonce, receipt_ok,
    setup_cross_chain, setup_cross_chain_with_env, sign_and_send, state_root, value_no_ret,
    wait_for,
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

    let mut l1_nonce = pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
    let mut l2_nonce = pending_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
    let mut inbound_hashes = Vec::new();
    let mut outbound_hashes = Vec::new();

    for (set_v, dep_v) in WAVE_SETTERS.iter().zip(WAVE_DEPOSITS.iter()) {
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

    assert_all_transactions_succeeded(&l1_rpc, &inbound_hashes, "inbound wave").await;
    assert_all_transactions_succeeded(&l2_rpc, &outbound_hashes, "outbound wave").await;

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

/// Positive outbound value is poison while the fresh rollup escrow is empty.
const POISON_WITHDRAWAL_WEI: u128 = 1_000_000_000_000_000; // 0.001 ETH

/// Evicts same-drain dependants without blocking unrelated traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until the same-drain dependent-poison cascade fix lands"]
async fn same_drain_poison_does_not_orphan_higher_nonce() {
    let w = setup_cross_chain().await.unwrap();

    let mut out_nonce = pending_nonce(&w.l2_rpc(), OUTBOUND_USER).await.unwrap();
    let poison_hash = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        out_nonce,
        Some(w.withdrawal_proxy),
        U256::from(POISON_WITHDRAWAL_WEI),
        Vec::new(),
        900_000,
    )
    .await
    .unwrap();
    out_nonce += 1;
    let orphan_hash = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        out_nonce,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(42u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .unwrap();

    let in_nonce = pending_nonce(&w.l1_rpc(), INBOUND_USER).await.unwrap();
    let unrelated_hash = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        in_nonce,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(77u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .unwrap();

    wait_for(SETTLE_TIMEOUT, || {
        let l1 = w.l1_rpc();
        async move { Ok(receipt_ok(&l1, unrelated_hash).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("unrelated inbound tx never settled — composer stalled on the gapped chain");
    wait_for(SETTLE_TIMEOUT, || {
        let (l2, value) = (w.l2_rpc(), w.value_l2);
        async move { Ok((l2_value(&l2, value).await? == U256::from(77u64)).then_some(())) }
    })
    .await
    .expect("unrelated inbound setter effect never landed on L2");

    // Require explicit eviction; a missing receipt alone can also mean stalled.
    let orphan_hash_text = orphan_hash.to_string();
    wait_for(SETTLE_TIMEOUT, || async {
        Ok((w
            .node
            .log_count_matching_all(&[orphan_hash_text.as_str(), "gapped chain can't land"])?
            > 0)
        .then_some(()))
    })
    .await
    .expect("nonce N+1 was never explicitly evicted as dependent poison");

    assert!(
        receipt_ok(&w.l2_rpc(), poison_hash)
            .await
            .unwrap()
            .is_none(),
        "poison withdrawal must never settle",
    );
    assert!(
        receipt_ok(&w.l2_rpc(), orphan_hash)
            .await
            .unwrap()
            .is_none(),
        "orphaned nonce N+1 must be cascade-evicted, never settle",
    );

    // The sender can reuse the evicted nonces after cleanup.
    let replacement_n = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        out_nonce - 1,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(101u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .expect("corrected replacement at nonce N must be admitted after cascade cleanup");
    let replacement_n1 = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        out_nonce,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(102u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .expect("corrected replacement at nonce N+1 must be admitted contiguously");
    for (hash, label) in [
        (replacement_n, "replacement nonce N"),
        (replacement_n1, "replacement nonce N+1"),
    ] {
        wait_for(SETTLE_TIMEOUT, || {
            let l2 = w.l2_rpc();
            async move { Ok(receipt_ok(&l2, hash).await?.filter(|ok| *ok)) }
        })
        .await
        .unwrap_or_else(|_| panic!("{label} did not settle after poison-chain cleanup"));
    }

    w.node.assert_no_process_death();
}

/// Keeps inbound and outbound nonce chains independent for one sender.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until the same-drain dependent-poison cascade fix lands"]
async fn poison_cascade_is_direction_scoped() {
    let w = setup_cross_chain().await.unwrap();
    let user = OUTBOUND_USER;

    let mut out_nonce = pending_nonce(&w.l2_rpc(), user).await.unwrap();
    let poison_hash = sign_and_send(
        &w.l2_xchain(),
        user,
        w.l2_chain_id,
        out_nonce,
        Some(w.withdrawal_proxy),
        U256::from(POISON_WITHDRAWAL_WEI),
        Vec::new(),
        900_000,
    )
    .await
    .unwrap();
    out_nonce += 1;
    let orphan_hash = sign_and_send(
        &w.l2_xchain(),
        user,
        w.l2_chain_id,
        out_nonce,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(9u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .unwrap();

    let in_nonce = pending_nonce(&w.l1_rpc(), user).await.unwrap();
    let inbound_hash = sign_and_send(
        &w.l1_xchain(),
        user,
        DEV_CHAIN_ID,
        in_nonce,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(123u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .unwrap();

    wait_for(SETTLE_TIMEOUT, || {
        let l1 = w.l1_rpc();
        async move { Ok(receipt_ok(&l1, inbound_hash).await?.filter(|ok| *ok)) }
    })
    .await
    .expect(
        "same-EOA inbound tx never settled — outbound poison wrongly cascaded across directions",
    );
    wait_for(SETTLE_TIMEOUT, || {
        let (l2, value) = (w.l2_rpc(), w.value_l2);
        async move { Ok((l2_value(&l2, value).await? == U256::from(123u64)).then_some(())) }
    })
    .await
    .expect("inbound setter effect never landed on L2");

    assert!(
        receipt_ok(&w.l2_rpc(), poison_hash)
            .await
            .unwrap()
            .is_none(),
        "poison withdrawal must never settle",
    );
    assert!(
        receipt_ok(&w.l2_rpc(), orphan_hash)
            .await
            .unwrap()
            .is_none(),
        "orphaned outbound nonce N+1 must be cascade-evicted",
    );
    w.node.assert_no_process_death();
}

/// Cascades across the local drain and the remaining shared pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until the same-drain dependent-poison cascade fix lands"]
async fn poison_cascade_spans_drain_cap() {
    let w = setup_cross_chain_with_env(&[("EEZ_MAX_USER_TXS_PER_BUNDLE", "2".to_string())])
        .await
        .unwrap();
    let user = OUTBOUND_USER;
    let mut nonce = pending_nonce(&w.l2_rpc(), user).await.unwrap();
    let poison = sign_and_send(
        &w.l2_xchain(),
        user,
        w.l2_chain_id,
        nonce,
        Some(w.withdrawal_proxy),
        U256::from(POISON_WITHDRAWAL_WEI),
        Vec::new(),
        900_000,
    )
    .await
    .unwrap();
    nonce += 1;
    let local_orphan = sign_and_send(
        &w.l2_xchain(),
        user,
        w.l2_chain_id,
        nonce,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(1u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .unwrap();
    nonce += 1;
    let pool_orphan = sign_and_send(
        &w.l2_xchain(),
        user,
        w.l2_chain_id,
        nonce,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(2u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .unwrap();

    let in_nonce = pending_nonce(&w.l1_rpc(), INBOUND_USER).await.unwrap();
    let unrelated = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        in_nonce,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(55u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .unwrap();

    wait_for(SETTLE_TIMEOUT, || {
        let l1 = w.l1_rpc();
        async move { Ok(receipt_ok(&l1, unrelated).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("unrelated tx never settled — cascade failed to clear local and/or pool orphans");
    for (h, label) in [
        (poison, "poison N"),
        (local_orphan, "local orphan N+1"),
        (pool_orphan, "pool orphan N+2"),
    ] {
        assert!(
            receipt_ok(&w.l2_rpc(), h).await.unwrap().is_none(),
            "{label} must be evicted, never settle",
        );
    }
    w.node.assert_no_process_death();
}

/// Preserves unrelated sender traffic in an interleaved drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until the same-drain dependent-poison cascade fix lands"]
async fn interleaved_senders_poison_isolation() {
    let w = setup_cross_chain().await.unwrap();
    let a = OUTBOUND_USER; // poison chain
    let b = ANVIL_KEY_2; // clean, must survive the same drain

    let mut an = pending_nonce(&w.l2_rpc(), a).await.unwrap();
    let a_poison = sign_and_send(
        &w.l2_xchain(),
        a,
        w.l2_chain_id,
        an,
        Some(w.withdrawal_proxy),
        U256::from(POISON_WITHDRAWAL_WEI),
        Vec::new(),
        900_000,
    )
    .await
    .unwrap();
    an += 1;
    let bn = pending_nonce(&w.l2_rpc(), b).await.unwrap();
    let b_hash = sign_and_send(
        &w.l2_xchain(),
        b,
        w.l2_chain_id,
        bn,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(33u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .unwrap();
    let a_orphan = sign_and_send(
        &w.l2_xchain(),
        a,
        w.l2_chain_id,
        an,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(44u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .unwrap();

    wait_for(SETTLE_TIMEOUT, || {
        let l2 = w.l2_rpc();
        async move { Ok(receipt_ok(&l2, b_hash).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("unrelated sender B's tx was dropped with A's broken chain");
    assert!(
        receipt_ok(&w.l2_rpc(), a_poison).await.unwrap().is_none(),
        "A poison must be evicted",
    );
    assert!(
        receipt_ok(&w.l2_rpc(), a_orphan).await.unwrap().is_none(),
        "A orphan must be evicted",
    );
    w.node.assert_no_process_death();
}

/// Recovers when a plain L1 transaction consumes a held inbound nonce.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing-dependent until the compose step has a deterministic test seam"]
async fn inbound_dos_same_nonce_before_postbatch() {
    let w = setup_cross_chain().await.unwrap();
    let attacker = INBOUND_USER;

    let n = pending_nonce(&w.l1_rpc(), attacker).await.unwrap();
    let _held = sign_and_send(
        &w.l1_xchain(),
        attacker,
        DEV_CHAIN_ID,
        n,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(5u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .unwrap();
    let self_addr = common::signer_address(attacker).unwrap();
    let plain = sign_and_send(
        &w.l1_rpc(),
        attacker,
        DEV_CHAIN_ID,
        n,
        Some(self_addr),
        U256::ZERO,
        Vec::new(),
        21_000,
    )
    .await
    .expect("plain L1 transaction must win nonce N for this test to be valid");
    wait_for(SETTLE_TIMEOUT, || {
        let l1 = w.l1_rpc();
        async move { Ok(receipt_ok(&l1, plain).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("plain L1 transaction did not consume nonce N");

    let out_nonce = pending_nonce(&w.l2_rpc(), OUTBOUND_USER).await.unwrap();
    let unrelated = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        out_nonce,
        Some(w.outbound_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(88u64),
        }
        .abi_encode(),
        900_000,
    )
    .await
    .unwrap();
    wait_for(SETTLE_TIMEOUT, || {
        let l2 = w.l2_rpc();
        async move { Ok(receipt_ok(&l2, unrelated).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("unrelated tx never settled — composer stalled on the stranded held tx");
    w.node.assert_no_process_death();
}

/// Manual reproducer for adjacent-nonce admission races.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual race reproducer only; replace with a deterministic pre-insert barrier before enabling in CI"]
async fn concurrent_adjacent_nonce_admission_no_gap() {
    let w = setup_cross_chain().await.unwrap();
    let n = pending_nonce(&w.l2_rpc(), OUTBOUND_USER).await.unwrap();
    let mk = |nonce: u64, v: u64| {
        let (front, proxy, cid) = (w.l2_xchain(), w.outbound_proxy, w.l2_chain_id);
        async move {
            sign_and_send(
                &front,
                OUTBOUND_USER,
                cid,
                nonce,
                Some(proxy),
                U256::ZERO,
                IValue::setValueCall { v: U256::from(v) }.abi_encode(),
                900_000,
            )
            .await
        }
    };
    let first = tokio::spawn(mk(n, 1));
    tokio::task::yield_now().await;
    let second = mk(n + 1, 2).await;
    let first = first.await.expect("nonce N submission task panicked");
    assert!(
        first.is_ok() && second.is_ok(),
        "adjacent-nonce admission raced with insertion (first={:?}, second={:?})",
        first.err(),
        second.err(),
    );
    w.node.assert_no_process_death();
}

/// Applies escrow limits cumulatively within one drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-process edge test; enable in the dedicated cross-chain lane after the fixture is slimmed"]
async fn cumulative_escrow_evicts_second_withdrawal() {
    let w = setup_cross_chain().await.unwrap();
    let deposit = 3_000_000_000_000_000u128; // 0.003 ETH
    let in_nonce = pending_nonce(&w.l1_rpc(), INBOUND_USER).await.unwrap();
    let dep_hash = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        in_nonce,
        Some(w.deposit_proxy),
        U256::from(deposit),
        Vec::new(),
        600_000,
    )
    .await
    .unwrap();
    wait_for(SETTLE_TIMEOUT, || {
        let l1 = w.l1_rpc();
        async move { Ok(receipt_ok(&l1, dep_hash).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("deposit did not settle");

    let w1 = 2_000_000_000_000_000u128;
    let w2 = 2_000_000_000_000_000u128;
    let recip_before = l2_balance(&w.l1_rpc(), w.withdrawal_recipient)
        .await
        .unwrap();
    let mut on = pending_nonce(&w.l2_rpc(), OUTBOUND_USER).await.unwrap();
    let h1 = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        on,
        Some(w.withdrawal_proxy),
        U256::from(w1),
        Vec::new(),
        900_000,
    )
    .await
    .unwrap();
    on += 1;
    let h2 = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        on,
        Some(w.withdrawal_proxy),
        U256::from(w2),
        Vec::new(),
        900_000,
    )
    .await
    .unwrap();

    wait_for(SETTLE_TIMEOUT, || {
        let l2 = w.l2_rpc();
        async move { Ok(receipt_ok(&l2, h1).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("first withdrawal did not settle");
    wait_for(SETTLE_TIMEOUT, || {
        let (l1, before) = (w.l1_rpc(), recip_before);
        async move {
            Ok(
                (l2_balance(&l1, w.withdrawal_recipient).await? == before + U256::from(w1))
                    .then_some(()),
            )
        }
    })
    .await
    .expect("recipient did not gain exactly the first withdrawal");
    // Confirm classification before relying on receipt absence.
    wait_for(SETTLE_TIMEOUT, || async {
        Ok((w
            .node
            .log_count_matching(&["outbound withdrawal exceeds L1 rollup escrow; evicting"])?
            > 0)
        .then_some(()))
    })
    .await
    .expect("second withdrawal was never classified as over-escrow poison");
    assert!(
        receipt_ok(&w.l2_rpc(), h2).await.unwrap().is_none(),
        "second withdrawal exceeded remaining escrow and must be evicted",
    );
    w.node.assert_no_process_death();
}

/// Evicts a pure transaction misdirected to a cross-chain front.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-process edge test; enable in the dedicated cross-chain lane after the fixture is slimmed"]
async fn misdirected_pure_tx_is_evicted_not_stalled() {
    let w = setup_cross_chain().await.unwrap();
    let stray_to = address!("0x4444444444444444444444444444444444444444");
    let on = pending_nonce(&w.l2_rpc(), OUTBOUND_USER).await.unwrap();
    let stray = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        on,
        Some(stray_to),
        U256::from(1u64),
        Vec::new(),
        100_000,
    )
    .await
    .unwrap();
    let in_nonce = pending_nonce(&w.l1_rpc(), INBOUND_USER).await.unwrap();
    let legit = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        in_nonce,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(66u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .unwrap();
    wait_for(SETTLE_TIMEOUT, || {
        let l1 = w.l1_rpc();
        async move { Ok(receipt_ok(&l1, legit).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("legit cross-chain tx never settled after a misdirected pure tx");
    wait_for(SETTLE_TIMEOUT, || {
        let (l2, target) = (w.l2_rpc(), w.value_l2);
        async move { Ok((l2_value(&l2, target).await? == U256::from(66u64)).then_some(())) }
    })
    .await
    .expect("legit cross-chain effect did not land after a misdirected pure tx");
    wait_for(SETTLE_TIMEOUT, || async {
        Ok((w.node.log_count_matching(&[
            "outbound tx produced no L1 settlement entry; evicting",
            "outbound tx fails simulation deterministically; evicting",
        ])? > 0)
            .then_some(()))
    })
    .await
    .expect("misdirected pure transaction was never classified as poison");
    assert!(
        receipt_ok(&w.l2_rpc(), stray).await.unwrap().is_none(),
        "misdirected pure transaction must be evicted, not mined",
    );
    w.node.assert_no_process_death();
}

/// Replaces a held transaction when the same sender submits the same nonce
/// again before the pool drains. The most recently admitted transaction wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until same-nonce held transaction replacement is supported"]
async fn same_nonce_latest_transaction_replaces_held_transaction() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();

    // Start immediately after a completed drain to leave a full Sync interval
    // for both same-nonce submissions to reach the held pool.
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
    .expect("no completed bundle observed before replacement test");

    let nonce = pending_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
    let first = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        nonce,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(111u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .expect("initial nonce-N transaction must be admitted");
    let replacement = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        nonce,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(222u64),
        }
        .abi_encode(),
        600_000,
    )
    .await
    .expect("latest nonce-N transaction must replace the held transaction");
    assert_ne!(first, replacement, "replacement must have a distinct hash");

    assert_all_transactions_succeeded(&l1_rpc, &[replacement], "replacement").await;
    wait_for(SETTLE_TIMEOUT, || {
        let l2_rpc = w.l2_rpc();
        async move {
            Ok((l2_value(&l2_rpc, w.value_l2).await? == U256::from(222u64)).then_some(()))
        }
    })
    .await
    .expect("replacement effect did not reach L2");
    assert!(
        receipt_ok(&l1_rpc, first).await.unwrap().is_none(),
        "superseded nonce-N transaction must not land",
    );
    w.node.assert_no_process_death();
}

/// Rejects consumed, gapped, and underfunded transactions at ingress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-process edge test; enable in the dedicated cross-chain lane after the fixture is slimmed"]
async fn ingress_front_admission_guards() {
    let w = setup_cross_chain().await.unwrap();

    // Already-consumed nonce.
    let n = pending_nonce(&w.l1_rpc(), INBOUND_USER).await.unwrap();
    let self_addr = common::signer_address(INBOUND_USER).unwrap();
    let plain = sign_and_send(
        &w.l1_rpc(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        n,
        Some(self_addr),
        U256::ZERO,
        Vec::new(),
        21_000,
    )
    .await
    .unwrap();
    wait_for(SETTLE_TIMEOUT, || {
        let l1 = w.l1_rpc();
        async move { Ok(receipt_ok(&l1, plain).await?.filter(|ok| *ok)) }
    })
    .await
    .expect("plain L1 tx did not confirm");
    let consumed = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        n,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(1u64),
        }
        .abi_encode(),
        600_000,
    )
    .await;
    assert!(
        consumed.is_err(),
        "front admitted a cross-chain tx at an already-consumed nonce",
    );

    // Gapped nonce.
    let cur = pending_nonce(&w.l1_rpc(), INBOUND_USER).await.unwrap();
    let gapped = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        cur + 2,
        Some(w.setter_proxy),
        U256::ZERO,
        IValue::setValueCall {
            v: U256::from(2u64),
        }
        .abi_encode(),
        600_000,
    )
    .await;
    assert!(
        gapped.is_err(),
        "front admitted a gapped nonce (on_chain + 2 with nothing held)",
    );

    // Insufficient balance.
    let broke = sign_and_send(
        &w.l2_xchain(),
        UNFUNDED_KEY,
        w.l2_chain_id,
        0,
        Some(w.withdrawal_proxy),
        U256::from(1_000_000_000_000_000u128),
        Vec::new(),
        900_000,
    )
    .await;
    assert!(
        broke.is_err(),
        "front admitted an outbound tx from a zero-balance sender",
    );

    w.node.assert_no_process_death();
}
