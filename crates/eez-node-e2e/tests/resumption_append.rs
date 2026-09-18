//! The resumed-batch REPLAY arm: a resumed batch carrying L2 content to append.
//!
//! `resumption.rs` reaches the resumed branch, but its crafted suffix is
//! state-only, so `new_content` is empty and the deriver skips. This drives the
//! other arm — a peer settles a strict PREFIX of a real cross-chain batch, so
//! when that batch lands its leading entries are already applied and only the
//! suffix runs. That suffix carries system transactions the deriver must append
//! to the Sync block the prefix built.
//!
//! The prefix cannot be synthesised from nothing. Its `newState` has to be a
//! candidate block hash the composer actually computed for that transaction
//! prefix; any other value names a block the deriver cannot build, so it would
//! diverge instead of appending. So the prefix is cut from a real batch while
//! that batch is still pending, and raced in front of it with a higher tip.

use std::time::Duration;

use alloy_consensus::Transaction as _;
use alloy_primitives::{B256, Bytes, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::SolCall;
use eez_protocol::abi::{EvmBatch, postAndVerifyBatchCall};
use eez_protocol::signer::EcdsaProofSigner;
use eez_testkit::{
    IValue, OUTBOUND_USER, onchain_nonce, setup_cross_chain, sign_and_send, signals,
};

const TIMEOUT: Duration = Duration::from_mins(6);

/// The first postBatch sitting in the pending pool with enough entries to cut a
/// strict prefix from: an anchor plus at least two producing entries.
async fn pending_batch_with_producing_entries(
    l1_rpc: &str,
    eez: alloy_primitives::Address,
) -> Option<(EvmBatch, B256)> {
    let provider = ProviderBuilder::new().connect_http(l1_rpc.parse().ok()?);
    let pending = provider
        .get_block_by_number(alloy_eips::BlockNumberOrTag::Pending)
        .full()
        .await
        .ok()??;
    for tx in pending.transactions.txns() {
        if tx.to() != Some(eez) || !tx.input().starts_with(&postAndVerifyBatchCall::SELECTOR) {
            continue;
        }
        let call = postAndVerifyBatchCall::abi_decode(tx.input()).ok()?;
        // Cutting the prefix must leave at least one immediate entry that can
        // still apply, or every immediate is stale and the post unwinds with
        // AllImmediateL2TxsFailed instead of resuming.
        if call.batch.immediateEntryCount >= U256::from(3) {
            return Some((call.batch, *tx.inner.hash()));
        }
    }
    None
}

/// Re-attest `batch` and submit it with a tip high enough to order ahead of the
/// composer's own pending postBatch in the same block.
async fn post_ahead(
    l1_rpc: &str,
    eez: alloy_primitives::Address,
    sender_key: &str,
    attester_key: &str,
    mut batch: EvmBatch,
) -> eyre::Result<B256> {
    let attester =
        EcdsaProofSigner::from_private_key(attester_key.trim_start_matches("0x").parse::<B256>()?)?;
    let vkey = B256::left_padding_from(attester.address().as_slice());
    let hashes = eez_protocol::public_inputs::public_inputs_hashes(&batch, vkey)?;
    batch.proofs = hashes
        .iter()
        .map(|h| attester.sign_prehash(*h))
        .collect::<Result<Vec<Bytes>, _>>()?;

    let signer: alloy_signer_local::PrivateKeySigner =
        sender_key.trim_start_matches("0x").parse()?;
    let provider = ProviderBuilder::new()
        .wallet(alloy_network::EthereumWallet::from(signer))
        .connect_http(l1_rpc.parse()?);
    let pending = provider
        .send_transaction(
            alloy_rpc_types_eth::TransactionRequest::default()
                .to(eez)
                .input(postAndVerifyBatchCall { batch }.abi_encode().into())
                // Ordering within the block is by effective tip, and the
                // composer posts at its configured priority fee.
                .max_priority_fee_per_gas(500_000_000_000u128)
                .max_fee_per_gas(1_000_000_000_000u128)
                .gas_limit(8_000_000),
        )
        .await?;
    Ok(*pending.tx_hash())
}

/// Cut `batch` down to its anchor plus one outbound entry, both drained inline.
///
/// Only the leading `proxyEntryHash == 0` run is taken: those settle on their
/// own, whereas a deferred entry waits on a bundled user transaction this
/// synthetic peer does not have.
fn strict_prefix(batch: &EvmBatch) -> eyre::Result<EvmBatch> {
    let mut prefix = batch.clone();
    prefix.entries.truncate(2);
    prefix.immediateEntryCount = U256::from(prefix.entries.len());
    prefix.proofs = Vec::new();
    // The begin hash binds the starting state and identity, so it is never zero;
    // without recomputing it `_executeEntry` refuses the entry for a rolling
    // hash mismatch and the post unwinds with AllImmediateL2TxsFailed.
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut prefix)?;
    Ok(prefix)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_batch_appends_the_l2_content_of_the_entries_it_settled() {
    let w = setup_cross_chain().await.unwrap();
    let l1 = w.l1_rpc();
    let l2 = w.l2_rpc();
    let eez = w.cfg.eez_address;

    let deadline = std::time::Instant::now() + TIMEOUT;
    let mut raced = 0usize;
    let mut nonce = onchain_nonce(&l2, OUTBOUND_USER).await.unwrap();

    // Keep outbound traffic flowing so batches carry several immediate entries,
    // and try to cut a prefix out of each one before it lands.
    while std::time::Instant::now() < deadline {
        // Outbound calls, because they become IMMEDIATE entries: the batch then
        // carries `[anchor, outbound, outbound, …]` and a cut prefix still
        // leaves one that applies. Two per attempt so `immediateEntryCount`
        // reaches three.
        for bump in 0..2u64 {
            if sign_and_send(
                &w.l2_xchain(),
                OUTBOUND_USER,
                w.l2_chain_id,
                nonce,
                Some(w.outbound_proxy),
                U256::ZERO,
                IValue::setValueCall {
                    v: U256::from(40u64 + raced as u64 * 2 + bump),
                }
                .abi_encode(),
                900_000,
            )
            .await
            .is_ok()
            {
                nonce += 1;
            }
        }

        // Poll tightly: the batch is only pending for part of one L1 slot.
        for _ in 0..80 {
            if let Some((batch, _)) = pending_batch_with_producing_entries(&l1, eez).await {
                if let Ok(prefix) = strict_prefix(&batch)
                    && post_ahead(&l1, eez, w.cfg.deployer_key, w.cfg.attester_key, prefix)
                        .await
                        .is_ok()
                {
                    raced += 1;
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Did any race land the deriver in the replay arm?
        let replayed = w
            .node
            .count_signal(signals::DERIVER_RESUMED_APPENDED)
            .unwrap_or_default();
        if replayed > 0 {
            break;
        }

        // Let the composer recover before racing again. Front-running it back
        // to back starves the held pool, and the batches that follow carry only
        // an anchor — a resumed batch with nothing to append, which is the skip
        // arm, not the one under test.
        let settled_before = w
            .node
            .log_count_matching(&["\"settled\":true"])
            .unwrap_or_default();
        let recovered = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < recovered {
            let now = w
                .node
                .log_count_matching(&["\"settled\":true"])
                .unwrap_or_default();
            if now > settled_before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    let resumed = w
        .node
        .count_signal(signals::DERIVER_RESUMED_PLACEMENT)
        .unwrap_or_default();
    let replayed = w
        .node
        .count_signal(signals::DERIVER_RESUMED_APPENDED)
        .unwrap_or_default();

    assert!(
        resumed > 0,
        "no resumed batch was produced in {TIMEOUT:?} after {raced} prefix races\n{}",
        w.settlement_diagnostics()
    );
    assert!(
        replayed > 0,
        "resumed batches occurred ({resumed}) but none took the replay arm: the \
         settled suffix carried no L2 content to append after {raced} races\n{}",
        w.settlement_diagnostics()
    );

    // Appending must rebuild the Sync block, not corrupt it.
    w.node.assert_no_divergence_failure_logs();
    w.node.assert_no_process_death();

    // And the pipeline must keep settling afterwards.
    let head = ProviderBuilder::new()
        .connect_http(l2.parse().unwrap())
        .get_block_number()
        .await
        .unwrap();
    eez_testkit::wait_for(TIMEOUT, || async {
        let now = ProviderBuilder::new()
            .connect_http(l2.parse().unwrap())
            .get_block_number()
            .await?;
        Ok((now > head).then_some(now))
    })
    .await
    .expect("L2 stopped advancing after a resumed batch appended content");
    w.node.assert_no_divergence_failure_logs();
}
