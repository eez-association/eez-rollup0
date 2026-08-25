//! Chained-interstate slot composition (issue #88, `docs/CHAINED-INTERSTATE-DESIGN.md` §9).
//!
//! Every test here composes order-dependent cross-chain calls into a single
//! drain. The composer must simulate them in canonical order so the claims it
//! records (`returnData` → rolling hash) match sequential on-chain execution.
//! Isolated simulation over one pre-slot state produces claims the chain
//! contradicts: delivery reverts with `RollingHashMismatch`, the proof signer
//! rejects the window, and the drain re-queues the same set forever.

use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, B256, Bytes, TxHash, U256, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::{SolCall, SolEvent, sol};
use anyhow::{Context, Result};
use eez_protocol::{EEZL2_ADDRESS, EvmBatch, entries::decode_postbatch};
use eez_testkit::signals;
use eez_testkit::{
    ANVIL_KEY_6, CrossChainWorld, DEV_CHAIN_ID, ICounter, IEEZ, INBOUND_USER, ISetterWrapper,
    IValue, OUTBOUND_USER, SETTLE_TIMEOUT, Scenario, ScenarioCall, StateRead, TARGET_DEPLOYER,
    call_read, counter_count, create_cross_chain_proxy, create_l2_cross_chain_proxy,
    deploy_counter, events_since, l2_value, last_proxy_result, onchain_nonce, receipt_ok,
    safe_block_state_root, setup_cross_chain, setup_cross_chain_with_env, sign_and_send,
    state_root, value_read, wait_for,
};

sol! {
    /// Emitted per executed cross-chain call by both managers (`EEZ` on L1,
    /// `EEZL2` on L2) — the ACTUAL result folded into the rolling hash. The two
    /// share a signature; the emitting address tells them apart.
    event CallResult(uint256 indexed entryIndex, uint256 indexed callNumber, bool success, bytes returnData);
}

/// Bundle cap for these tests. Pinned rather than inherited so a default
/// change cannot silently split a co-bundled set across drains.
const MAX_USER_TXS: (&str, &str) = ("EEZ_MAX_USER_TXS_PER_BUNDLE", "3");

fn cap_env() -> Vec<(&'static str, String)> {
    vec![(MAX_USER_TXS.0, MAX_USER_TXS.1.to_string())]
}

fn as_u256(data: &[u8]) -> Option<U256> {
    (data.len() == 32).then(|| U256::from_be_slice(data))
}

/// Every `postAndVerifyBatch` the composer has landed, in L1 order.
async fn posted_batches(l1_rpc: &str, eez: Address, from_block: u64) -> Result<Vec<EvmBatch>> {
    let provider = ProviderBuilder::new().connect_http(l1_rpc.parse()?);
    let filter = Filter::new()
        .address(eez)
        .event_signature(IEEZ::BatchPosted::SIGNATURE_HASH)
        .from_block(from_block);
    let mut batches = Vec::new();
    for log in provider.get_logs(&filter).await? {
        let Some(hash) = log.transaction_hash else {
            continue;
        };
        let tx = provider
            .get_transaction_by_hash(hash)
            .await?
            .with_context(|| format!("postBatch tx {hash} not found"))?;
        batches.push(decode_postbatch(tx.input()).context("decode postAndVerifyBatch calldata")?);
    }
    Ok(batches)
}

/// The inbound (L1→L2) claims one batch carries, in entry order.
///
/// A deferred entry (`proxyEntryHash != 0`) is what an L1 user tx consumes:
/// its `returnData` is what EEZ hands back to the proxy caller — the
/// composer's claim about the L2 execution, byte for byte. The on-chain entry
/// is lean (it binds the call only through `proxyEntryHash`; the call shape
/// rides the DA sidecar), so attribution is by direction. The only cross-chain
/// traffic in these fixtures is the test's own.
fn inbound_claims(batch: &EvmBatch) -> Vec<U256> {
    batch
        .entries
        .iter()
        .filter(|e| e.proxyEntryHash != B256::ZERO)
        .map(|e| {
            as_u256(&e.returnData).unwrap_or_else(|| {
                panic!(
                    "inbound claim is not a uint256: 0x{}",
                    hex::encode(&e.returnData)
                )
            })
        })
        .collect()
}

/// The outbound (L2→L1) calldata one batch executes against `l1_target`, in
/// execution order. Outbound calls ride immediate entries (`proxyEntryHash ==
/// 0`) and run inside `postAndVerifyBatch`, ahead of the bundle's user txs.
fn outbound_calls(batch: &EvmBatch, l1_target: Address) -> Vec<Bytes> {
    batch
        .entries
        .iter()
        .filter(|e| e.proxyEntryHash == B256::ZERO)
        .flat_map(|e| e.l2ToL1Calls.iter())
        .filter(|c| c.targetAddress == l1_target)
        .map(|c| c.data.clone())
        .collect()
}

struct CallOutcome {
    block: u64,
    tx: TxHash,
    tx_index: u64,
    success: bool,
    return_data: Bytes,
}

/// Cross-chain call outcomes as the manager at `contract` actually executed
/// them. On L2 these come from the delivery system txs; a delivery that
/// diverged from its claim reverts, taking its logs with it.
async fn call_results(rpc_url: &str, contract: Address) -> Result<Vec<CallOutcome>> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let filter = Filter::new()
        .address(contract)
        .event_signature(CallResult::SIGNATURE_HASH)
        .from_block(0u64);
    provider
        .get_logs(&filter)
        .await?
        .into_iter()
        .map(|log| {
            let decoded = CallResult::decode_log(&log.inner)?;
            Ok(CallOutcome {
                block: log.block_number.unwrap_or_default(),
                tx: log.transaction_hash.unwrap_or_default(),
                tx_index: log.transaction_index.unwrap_or_default(),
                success: decoded.success,
                return_data: decoded.returnData.clone(),
            })
        })
        .collect()
}

async fn assert_receipt_ok(rpc_url: &str, hash: TxHash, label: &str) {
    let url = rpc_url.to_owned();
    let status = wait_for(SETTLE_TIMEOUT, move || {
        let url = url.clone();
        async move { receipt_ok(&url, hash).await }
    })
    .await
    .unwrap_or_else(|err| panic!("{label} ({hash}) never landed: {err:#}"));
    assert!(status, "{label} ({hash}) reverted");
}

async fn wait_for_count(rpc_url: &str, counter: Address, expected: u64, label: &str) {
    let url = rpc_url.to_owned();
    wait_for(SETTLE_TIMEOUT, move || {
        let url = url.clone();
        async move { Ok((counter_count(&url, counter).await? == U256::from(expected)).then_some(())) }
    })
    .await
    .unwrap_or_else(|err| panic!("{label} never reached count={expected}: {err:#}"));
}

/// L1's stored `rollups[rid].stateRoot` must equal the L2 safe block's root.
async fn assert_reconciled(w: &CrossChainWorld) {
    let (eez, rollup_id) = (w.cfg.eez_address, w.cfg.rollup_id);
    let (l1_rpc, l2_rpc) = (w.l1_rpc(), w.l2_rpc());
    wait_for(SETTLE_TIMEOUT, || {
        let (l1_rpc, l2_rpc) = (l1_rpc.clone(), l2_rpc.clone());
        async move {
            let l1_root = state_root(&l1_rpc, eez, rollup_id).await?;
            let l2_root = safe_block_state_root(&l2_rpc).await?;
            Ok(l2_root.filter(|root| *root == l1_root).map(|_| ()))
        }
    })
    .await
    .expect("L1 stored stateRoot never matched the L2 safe stateRoot");
}

/// Return just after the composer drains its pool. Transactions submitted next
/// receive a complete collection interval before the following drain.
async fn open_drain_window(w: &CrossChainWorld) {
    let cursor = w.node.signal_cursor().unwrap();
    wait_for(SETTLE_TIMEOUT, || async {
        let drained = w
            .node
            .signals_since(cursor)?
            .iter()
            .any(|signal| signal.name == signals::COMPOSER_SYNC_SLOT_DRAIN);
        Ok(drained.then_some(()))
    })
    .await
    .expect("composer did not report a fresh sync-slot drain boundary");
}

async fn batches(w: &CrossChainWorld) -> Vec<EvmBatch> {
    posted_batches(&w.l1_rpc(), w.cfg.eez_address, w.dep.deploy_block)
        .await
        .expect("read posted batches")
}

/// Match the eviction MESSAGES, not the word. Every drain reports its counts as
/// `evicted_poison=N evicted_stale=N`, so "evicted" hits those field names even
/// at zero; "evicting" is only ever the verb, and every eviction path logs one
/// of these per transaction.
fn assert_no_evictions(w: &CrossChainWorld) {
    assert_eq!(
        w.node
            .log_count_matching(&["evicting", "evicted instead"])
            .unwrap(),
        0,
        "composable transactions must not be evicted",
    );
}

fn completed_calls_read(wrapper: Address) -> StateRead {
    call_read(
        wrapper,
        "completedProxyCalls()",
        ISetterWrapper::completedProxyCallsCall {}.abi_encode(),
    )
}

/// Two identical inbound calls from one L1 transaction. The composer must keep
/// both ordered entries: the first changes destination state and the second
/// observes it, returning `changed = false` instead of being deduplicated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_inbound_calls_in_one_source_transaction_chain_state() {
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
        "the first call must change destination state",
    );

    Scenario::new("repeated inbound calls in one source transaction")
        .inbound(
            ScenarioCall::new(
                w.inbound_wrapper,
                ISetterWrapper::setSameValueTwiceCall { v: value }.abi_encode(),
            )
            .with_gas_limit(1_200_000),
        )
        .expect_l2_state(value_read(w.value_l2), value)
        .expect_l1_state(completed_calls_read(w.inbound_wrapper), 2u64)
        .expect_settled_fully()
        .run(&w)
        .await
        .unwrap();

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
        "the repeated call must observe the first destination write",
    );
    assert_eq!(
        last_proxy_result(&l1_rpc, w.inbound_wrapper).await.unwrap(),
        (false, value),
        "the final ordered call must return the post-write state",
    );

    assert_reconciled(&w).await;
    assert_no_evictions(&w);
    w.node.assert_no_process_death();
}

/// Two outbound transactions with identical calls must remain distinct ordered
/// entries. The second execution observes the first write and returns
/// `changed = false`; semantic-hash deduplication would drop that result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_outbound_calls_in_separate_transactions_chain_state() {
    let w = setup_cross_chain_with_env(&cap_env()).await.unwrap();
    let (l1_rpc, l2_rpc) = (w.l1_rpc(), w.l2_rpc());
    let value = U256::from(79u64);
    let wrapper_call = ISetterWrapper::setViaProxyCall { v: value }.abi_encode();
    let destination_call = IValue::setValueCall { v: value }.abi_encode();
    let provider = ProviderBuilder::new().connect_http(l2_rpc.parse().unwrap());
    let wrapped_filter = Filter::new()
        .address(w.outbound_wrapper)
        .event_signature(ISetterWrapper::Wrapped::SIGNATURE_HASH)
        .from_block(0u64);
    let wrapped_before = provider.get_logs(&wrapped_filter).await.unwrap().len();
    assert_ne!(
        l2_value(&l1_rpc, w.outbound_value).await.unwrap(),
        value,
        "the first call must change destination state",
    );

    open_drain_window(&w).await;
    let nonce = onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
    let first = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        nonce,
        Some(w.outbound_wrapper),
        U256::ZERO,
        wrapper_call.clone(),
        1_200_000,
    )
    .await
    .expect("first identical outbound call must be admitted");
    let second = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        nonce + 1,
        Some(w.outbound_wrapper),
        U256::ZERO,
        wrapper_call,
        1_200_000,
    )
    .await
    .expect("second identical outbound call must be admitted");

    assert_receipt_ok(&l2_rpc, first, "first identical outbound call").await;
    assert_receipt_ok(&l2_rpc, second, "second identical outbound call").await;
    wait_for(SETTLE_TIMEOUT, || {
        let provider = provider.clone();
        let wrapped_filter = wrapped_filter.clone();
        async move {
            let count = provider.get_logs(&wrapped_filter).await?.len();
            Ok((count == wrapped_before + 2).then_some(()))
        }
    })
    .await
    .expect("both identical outbound calls did not return to the source wrapper");

    let carried: Vec<Vec<Bytes>> = wait_for(SETTLE_TIMEOUT, || async {
        let carried: Vec<Vec<Bytes>> = batches(&w)
            .await
            .iter()
            .map(|batch| outbound_calls(batch, w.outbound_value))
            .filter(|calls| !calls.is_empty())
            .collect();
        Ok((!carried.is_empty()).then_some(carried))
    })
    .await
    .expect("identical outbound calls never reached L1");
    assert_eq!(
        carried,
        vec![vec![
            Bytes::from(destination_call.clone()),
            Bytes::from(destination_call),
        ]],
        "both identical calls must ride one postBatch in source order",
    );

    let wrapped = provider.get_logs(&wrapped_filter).await.unwrap();
    let wrapped = &wrapped[wrapped_before..];
    let sync_block = wrapped[0]
        .block_number
        .expect("first wrapper result must belong to a mined Sync block");
    assert_eq!(
        wrapped[1].block_number,
        Some(sync_block),
        "both calls must execute in one Sync block",
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
        vec![(value, true, true, value), (value, true, false, value)],
        "the second call must observe the first destination write",
    );

    assert_reconciled(&w).await;
    assert_no_evictions(&w);
    w.node.assert_no_process_death();
}

/// THE issue #88 repro. Three L1→L2 `increment()` calls against one stateful
/// L2 target, co-bundled into a single drain: the claims must chain 1, 2, 3.
///
/// Without the redesign each call is simulated against the same pre-slot state
/// and all three claim `returnData = 1`. The second delivery re-executes for
/// real, folds `2`, and reverts `RollingHashMismatch`; the signer rejects the
/// window and the set re-queues forever — count stays 0 and nothing settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_order_dependent_inbound_calls_in_one_bundle() {
    let w = setup_cross_chain_with_env(&cap_env()).await.unwrap();
    let (l1_rpc, l2_rpc) = (w.l1_rpc(), w.l2_rpc());

    let counter = deploy_counter(&l2_rpc, TARGET_DEPLOYER, w.l2_chain_id)
        .await
        .unwrap();
    let proxy = create_cross_chain_proxy(
        &l1_rpc,
        w.cfg.deployer_key,
        w.cfg.eez_address,
        counter,
        w.cfg.rollup_id,
    )
    .await
    .unwrap();

    open_drain_window(&w).await;
    let mut nonce = onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
    let mut hashes = Vec::new();
    for _ in 0..3 {
        hashes.push(
            sign_and_send(
                &w.l1_xchain(),
                INBOUND_USER,
                DEV_CHAIN_ID,
                nonce,
                Some(proxy),
                U256::ZERO,
                ICounter::incrementCall {}.abi_encode(),
                600_000,
            )
            .await
            .expect("inbound increment must be admitted"),
        );
        nonce += 1;
    }

    for (i, hash) in hashes.iter().enumerate() {
        assert_receipt_ok(&l1_rpc, *hash, &format!("inbound increment {i}")).await;
    }
    wait_for_count(&l2_rpc, counter, 3, "L2 counter").await;

    // The claim chain, read off the entries L1 accepted.
    let claimed: Vec<Vec<U256>> = batches(&w)
        .await
        .iter()
        .map(inbound_claims)
        .filter(|claims| !claims.is_empty())
        .collect();
    assert_eq!(
        claimed.len(),
        1,
        "all three calls must ride ONE postBatch (got {claimed:?}); split batches mean the drain \
         window was missed, not that the invariant broke",
    );
    assert_eq!(
        claimed[0],
        vec![U256::from(1u64), U256::from(2u64), U256::from(3u64)],
        "co-bundled claims must chain",
    );

    // The deliveries that re-ran those claims on L2, in one Sync block.
    let delivered = call_results(&l2_rpc, EEZL2_ADDRESS).await.unwrap();
    assert_eq!(delivered.len(), 3, "one delivery per inbound call");
    assert!(
        delivered.iter().all(|o| o.success),
        "every delivered cross-chain call must succeed",
    );
    assert!(
        delivered.iter().all(|o| o.block == delivered[0].block),
        "the three deliveries must share one Sync block",
    );
    assert_eq!(
        delivered
            .iter()
            .filter_map(|o| as_u256(&o.return_data))
            .collect::<Vec<_>>(),
        vec![U256::from(1u64), U256::from(2u64), U256::from(3u64)],
        "on-chain results must equal the claims",
    );
    for outcome in &delivered {
        assert_receipt_ok(&l2_rpc, outcome.tx, "delivery system tx").await;
    }

    assert_reconciled(&w).await;
    assert_no_evictions(&w);
    assert_eq!(
        w.node.log_count_matching(&["local L2 state root"]).unwrap(),
        0,
        "no state-root divergence",
    );
    w.node.assert_no_process_death();
}

/// Both directions in one slot. Pins the canonical block order — outbound
/// `[load, user]` pairs first, then inbound deliveries — and the L1 state
/// advancing by real frames: the outbound call executes inside
/// `postAndVerifyBatch`, ahead of the inbound user tx in the same bundle.
///
/// Without the redesign the two directions are simulated against unrelated
/// snapshots and the composed Sync block need not match what the bundle does
/// on L1, so nothing here is guaranteed to settle together.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_direction_state_chain_in_one_slot() {
    let w = setup_cross_chain_with_env(&cap_env()).await.unwrap();
    let (l1_rpc, l2_rpc) = (w.l1_rpc(), w.l2_rpc());

    let l2_counter = deploy_counter(&l2_rpc, TARGET_DEPLOYER, w.l2_chain_id)
        .await
        .unwrap();
    let l1_counter = deploy_counter(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID)
        .await
        .unwrap();
    let inbound_proxy = create_cross_chain_proxy(
        &l1_rpc,
        w.cfg.deployer_key,
        w.cfg.eez_address,
        l2_counter,
        w.cfg.rollup_id,
    )
    .await
    .unwrap();
    let outbound_proxy = create_l2_cross_chain_proxy(&l2_rpc, TARGET_DEPLOYER, l1_counter, 0)
        .await
        .unwrap();

    let add_five = ICounter::addCall {
        x: U256::from(5u64),
    }
    .abi_encode();

    open_drain_window(&w).await;
    let inbound = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(inbound_proxy),
        U256::ZERO,
        ICounter::incrementCall {}.abi_encode(),
        600_000,
    )
    .await
    .expect("inbound increment must be admitted");
    let outbound = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap(),
        Some(outbound_proxy),
        U256::ZERO,
        add_five.clone(),
        900_000,
    )
    .await
    .expect("outbound add must be admitted");

    assert_receipt_ok(&l1_rpc, inbound, "inbound increment").await;
    assert_receipt_ok(&l2_rpc, outbound, "outbound add").await;
    wait_for_count(&l2_rpc, l2_counter, 1, "L2 counter").await;
    wait_for_count(&l1_rpc, l1_counter, 5, "L1 counter").await;

    let carried: Vec<(Vec<U256>, Vec<Bytes>)> = batches(&w)
        .await
        .iter()
        .map(|b| (inbound_claims(b), outbound_calls(b, l1_counter)))
        .filter(|(inb, outb)| !inb.is_empty() || !outb.is_empty())
        .collect();
    assert_eq!(
        carried.len(),
        1,
        "both directions must ride ONE postBatch (got {carried:?})",
    );
    assert_eq!(carried[0].0, vec![U256::from(1u64)], "inbound claim");
    assert_eq!(
        carried[0].1,
        vec![Bytes::from(add_five)],
        "outbound call carried as an immediate entry",
    );

    // Canonical Sync-block order: the outbound user tx precedes the inbound
    // delivery, matching `build_cross_chain_sync_pairs`.
    let provider = ProviderBuilder::new().connect_http(l2_rpc.parse().unwrap());
    let outbound_receipt = provider
        .get_transaction_receipt(outbound)
        .await
        .unwrap()
        .expect("outbound receipt");
    let delivered = call_results(&l2_rpc, EEZL2_ADDRESS).await.unwrap();
    assert_eq!(delivered.len(), 1, "one inbound delivery");
    assert!(delivered[0].success, "delivery must succeed");
    assert_eq!(
        outbound_receipt.block_number.unwrap(),
        delivered[0].block,
        "outbound user tx and inbound delivery share the Sync block",
    );
    assert!(
        outbound_receipt.transaction_index.unwrap() < delivered[0].tx_index,
        "outbound pair must precede the inbound delivery",
    );

    // L1 executed the outbound call for real, inside the postBatch.
    let l1_results = call_results(&l1_rpc, w.cfg.eez_address).await.unwrap();
    assert_eq!(
        l1_results
            .iter()
            .filter(|o| o.success)
            .filter_map(|o| as_u256(&o.return_data))
            .collect::<Vec<_>>(),
        vec![U256::from(5u64)],
        "L1 target's actual result",
    );

    assert_reconciled(&w).await;
    assert_no_evictions(&w);
    w.node.assert_no_process_death();
}

/// A poison transaction between two order-dependent survivors. The survivors
/// must settle with claims 1 and 2 — the chain closes over the evicted tx
/// rather than reserving it a slot (claims 1 and 3) — and composition must
/// keep running instead of freezing the window.
///
/// Poison here is the harness's established form (`scripts/xchain-test.sh`): a
/// cross-chain submission whose `to` is not a proxy, so the source simulation
/// records no cross-chain call and the tx can never compose. Its sender is
/// distinct from the survivors' because eviction cascades along a sender's
/// nonce chain.
///
/// Without the redesign the poison degrades the whole slot and the survivors'
/// claims come from isolated sims (both `1`), so nothing settles at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poison_mid_bundle_leaves_survivors_correct() {
    let w = setup_cross_chain_with_env(&cap_env()).await.unwrap();
    let (l1_rpc, l2_rpc) = (w.l1_rpc(), w.l2_rpc());

    let counter = deploy_counter(&l2_rpc, TARGET_DEPLOYER, w.l2_chain_id)
        .await
        .unwrap();
    let proxy = create_cross_chain_proxy(
        &l1_rpc,
        w.cfg.deployer_key,
        w.cfg.eez_address,
        counter,
        w.cfg.rollup_id,
    )
    .await
    .unwrap();

    open_drain_window(&w).await;
    let mut nonce = onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
    let first = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        nonce,
        Some(proxy),
        U256::ZERO,
        ICounter::incrementCall {}.abi_encode(),
        600_000,
    )
    .await
    .expect("first increment must be admitted");
    nonce += 1;
    let poison = sign_and_send(
        &w.l1_xchain(),
        ANVIL_KEY_6,
        DEV_CHAIN_ID,
        onchain_nonce(&l1_rpc, ANVIL_KEY_6).await.unwrap(),
        Some(w.recipient), // plain address: never a cross-chain proxy on L1
        U256::ZERO,
        ICounter::incrementCall {}.abi_encode(),
        600_000,
    )
    .await
    .expect("poison must be admitted (it fails at composition, not ingress)");
    let second = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        nonce,
        Some(proxy),
        U256::ZERO,
        ICounter::incrementCall {}.abi_encode(),
        600_000,
    )
    .await
    .expect("second increment must be admitted");

    assert_receipt_ok(&l1_rpc, first, "first increment").await;
    assert_receipt_ok(&l1_rpc, second, "second increment").await;
    wait_for_count(&l2_rpc, counter, 2, "L2 counter").await;

    let claimed: Vec<Vec<U256>> = batches(&w)
        .await
        .iter()
        .map(inbound_claims)
        .filter(|claims| !claims.is_empty())
        .collect();
    assert_eq!(
        claimed.len(),
        1,
        "both survivors must ride ONE postBatch (got {claimed:?})",
    );
    assert_eq!(
        claimed[0],
        vec![U256::from(1u64), U256::from(2u64)],
        "survivor claims must close over the evicted tx",
    );

    assert_eq!(
        receipt_ok(&l1_rpc, poison).await.unwrap(),
        None,
        "the poison tx must be dropped, not bundled",
    );
    assert!(
        w.node.log_count_matching(&["evicting", "evicted"]).unwrap() > 0,
        "the poison tx must be evicted loudly",
    );

    // The window is not frozen: a later slot still settles.
    open_drain_window(&w).await;
    let third = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap(),
        Some(proxy),
        U256::ZERO,
        ICounter::incrementCall {}.abi_encode(),
        600_000,
    )
    .await
    .expect("post-poison increment must be admitted");
    assert_receipt_ok(&l1_rpc, third, "post-poison increment").await;
    wait_for_count(&l2_rpc, counter, 3, "L2 counter after poison").await;

    assert_reconciled(&w).await;
    w.node.assert_no_process_death();
}

/// Two outbound calls from ONE L2 sender (nonces n, n+1) in one slot against a
/// stateful L1 target. The L1 state must advance by real frames between the
/// two source simulations: `increment()` then `add(5)` leaves count 6, and the
/// second call's claim is 6, not 5.
///
/// Without the redesign both simulate over the same L1 snapshot and claim
/// against count 0 (`1` and `5`); the postBatch re-executes them sequentially,
/// folds `1` and `6`, and reverts — the bundle drops and both txs re-queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_sender_outbound_chain() {
    let w = setup_cross_chain_with_env(&cap_env()).await.unwrap();
    let (l1_rpc, l2_rpc) = (w.l1_rpc(), w.l2_rpc());

    let l1_counter = deploy_counter(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID)
        .await
        .unwrap();
    let outbound_proxy = create_l2_cross_chain_proxy(&l2_rpc, TARGET_DEPLOYER, l1_counter, 0)
        .await
        .unwrap();

    let increment = ICounter::incrementCall {}.abi_encode();
    let add_five = ICounter::addCall {
        x: U256::from(5u64),
    }
    .abi_encode();

    open_drain_window(&w).await;
    let mut nonce = onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
    let mut hashes = Vec::new();
    for input in [increment.clone(), add_five.clone()] {
        hashes.push(
            sign_and_send(
                &w.l2_xchain(),
                OUTBOUND_USER,
                w.l2_chain_id,
                nonce,
                Some(outbound_proxy),
                U256::ZERO,
                input,
                900_000,
            )
            .await
            .expect("outbound call must be admitted"),
        );
        nonce += 1;
    }

    for (i, hash) in hashes.iter().enumerate() {
        assert_receipt_ok(&l2_rpc, *hash, &format!("outbound call {i}")).await;
    }
    wait_for_count(&l1_rpc, l1_counter, 6, "L1 counter").await;

    let carried: Vec<Vec<Bytes>> = batches(&w)
        .await
        .iter()
        .map(|b| outbound_calls(b, l1_counter))
        .filter(|calls| !calls.is_empty())
        .collect();
    assert_eq!(
        carried.len(),
        1,
        "both outbound calls must ride ONE postBatch (got {carried:?})",
    );
    assert_eq!(
        carried[0],
        vec![Bytes::from(increment), Bytes::from(add_five)],
        "sender nonce order must survive the drain",
    );

    let l1_results = call_results(&l1_rpc, w.cfg.eez_address).await.unwrap();
    assert_eq!(
        l1_results
            .iter()
            .filter(|o| o.success)
            .filter_map(|o| as_u256(&o.return_data))
            .collect::<Vec<_>>(),
        vec![U256::from(1u64), U256::from(6u64)],
        "L1 executed the chain in order",
    );

    assert_reconciled(&w).await;
    assert_no_evictions(&w);
    w.node.assert_no_process_death();
}
