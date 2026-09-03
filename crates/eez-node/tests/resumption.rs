//! Resumed-batch settlement (`settlement.start > 0`): L1 skips leading entries
//! a competitor already applied and settles only the new suffix.
//!
//! No other suite reaches this path, since a composer produces one only when a
//! peer settles a strict prefix of the same chain. So the batch is built here —
//! but from a REAL one read back off L1, with entries appended and the
//! attestation recomputed, so every root is one the composer chose.

use std::time::Duration;

use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::SolCall;
use eez_protocol::abi::{
    ExecutionEntrySol, ProofSystemBatchPerVerificationEntriesSol as EvmBatch, StateUpdateSol,
    postAndVerifyBatchCall,
};
use eez_protocol::signer::EcdsaProofSigner;

mod common;
use common::{
    ANVIL_ATTESTER_KEY, ANVIL_KEY_1, Harness, NodeConfig, NodeHandle, wait_for,
    wait_for_latest_height,
};

const TIMEOUT: Duration = Duration::from_mins(5);

/// The composer's most recent `postAndVerifyBatch` calldata, decoded.
async fn last_posted_batch(l1_rpc: &str, eez: Address, from_block: u64) -> Option<(EvmBatch, u64)> {
    let provider = ProviderBuilder::new().connect_http(l1_rpc.parse().ok()?);
    let tip = provider.get_block_number().await.ok()?;
    // Batches are sparse, so a bounded backwards scan beats a log filter.
    for n in (from_block..=tip).rev() {
        let block = provider.get_block_by_number(n.into()).full().await.ok()??;
        for tx in block.transactions.txns() {
            let input = tx.input();
            if tx.to() == Some(eez) && input.starts_with(&postAndVerifyBatchCall::SELECTOR) {
                let call = postAndVerifyBatchCall::abi_decode(input).ok()?;
                return Some((call.batch, n));
            }
        }
    }
    None
}

/// Re-attest `batch` for `vkey` and submit it from `signer_key`.
async fn post_batch(
    l1_rpc: &str,
    eez: Address,
    sender_key: &str,
    attester_key: &str,
    mut batch: EvmBatch,
) -> eyre::Result<B256> {
    // The proof system is ECDSA over the public-inputs digest, so only the
    // attester's signature is needed; the signer service is not in this path.
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
    let calldata = postAndVerifyBatchCall { batch }.abi_encode();
    let pending = provider
        .send_transaction(
            alloy_rpc_types_eth::TransactionRequest::default()
                .to(eez)
                .input(calldata.into()),
        )
        .await?;
    Ok(*pending.tx_hash())
}

/// An immediate entry moving `rollup_id` from `from` to `to`, no calls, no
/// value. EEZ.sol accepts it whenever `from` is the live stored root.
fn transition_immediate(rollup_id: u64, from: B256, to: B256) -> ExecutionEntrySol {
    ExecutionEntrySol {
        stateUpdates: vec![StateUpdateSol {
            rollupId: rollup_id,
            currentState: from,
            newState: to,
            etherDelta: alloy_primitives::I256::ZERO,
        }],
        proxyEntryHash: B256::ZERO,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        rollingHash: B256::ZERO,
        destinationRollupId: rollup_id,
        success: true,
        returnData: Bytes::new(),
    }
}

/// Re-posting a landed batch with extra immediates appended must settle only
/// that suffix: the original entries are stale now, so L1 skips them.
///
/// Asserts the prefix is skipped, the batch does not unwind
/// (`AllImmediateL2TxsFailed`), and the deriver keeps settling afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_batch_settles_only_its_new_suffix() {
    let harness = Harness::fresh().await.unwrap();
    let chain = harness.chain();
    let env = harness.env().await.unwrap();
    let node = NodeHandle::start("resume-composer", &NodeConfig::default(), &env)
        .await
        .unwrap();

    // Let the composer establish a real chain and land batches of its own.
    chain.wait_for_batches(3, TIMEOUT).await.unwrap();
    wait_for_latest_height(&node, 1, TIMEOUT).await.unwrap();

    let l1 = chain.rpc_url();
    let (original, at_block) = wait_for(TIMEOUT, || async {
        Ok(last_posted_batch(l1, chain.eez_address(), chain.deploy_block()).await)
    })
    .await
    .expect("composer posted a batch we can read back");

    // Pause mining so no composer batch moves the stored root between the read
    // below and our tx landing, which would make the appended entry stale.
    chain.set_interval_mining(0).await.unwrap();

    let entries_before = original.entries.len();
    let skipped_before = chain.entries_skipped().await.unwrap();
    let executions_before = chain.executions_performed().await.unwrap();
    let live_root = chain.state_root().await.unwrap();

    // A round trip `live -> probe -> live`: both apply and the root ends where
    // it started. A single no-op would not do — its `newState` equals the
    // composer's own final root, so the claimed chain reads `[live, live]` and
    // the positional match lands on index 0, never entering the resumed branch.
    // `immediateEntryCount` must cover the whole leading zero-hash run.
    let probe = B256::repeat_byte(0xA7);
    let mut resumed = original.clone();
    resumed
        .entries
        .push(transition_immediate(chain.rollup_id(), live_root, probe));
    resumed
        .entries
        .push(transition_immediate(chain.rollup_id(), probe, live_root));
    resumed.immediateEntryCount = U256::from(resumed.entries.len());
    // `_executeEntry` checks `_rollingHash != entry.rollingHash`, and the
    // entry-begin hash binds the starting state and identity — so it is never
    // zero. Without this the appended entry is skipped for RollingHashMismatch
    // and the post unwinds with AllImmediateL2TxsFailed.
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut resumed)
        .expect("rolling hashes for the crafted batch");

    let tx = post_batch(
        l1,
        chain.eez_address(),
        ANVIL_KEY_1,
        ANVIL_ATTESTER_KEY,
        resumed,
    )
    .await
    .expect("crafted resumed batch submitted");
    // Mine exactly one block, then hand pacing back to anvil.
    chain.mine().await.unwrap();
    chain
        .set_interval_mining(common::Chain::block_time_secs())
        .await
        .unwrap();

    // It must land, not revert: a reverted post would roll back its own
    // `L2TxSkipped` events and prove nothing.
    let provider = ProviderBuilder::new().connect_http(l1.parse().unwrap());
    let receipt = wait_for(TIMEOUT, || async {
        Ok(provider.get_transaction_receipt(tx).await?)
    })
    .await
    .unwrap_or_else(|e| panic!("resumed batch receipt: {e:#}"));
    assert!(
        receipt.status(),
        "resumed batch reverted — the appended immediate should have kept \
         `anyExecuted` true and avoided AllImmediateL2TxsFailed"
    );

    // L1 skipped the stale prefix …
    let skipped_after = chain.entries_skipped().await.unwrap();
    assert_eq!(
        skipped_after - skipped_before,
        entries_before,
        "expected every one of the {entries_before} already-applied entries to be \
         skipped (posted at L1 block {at_block})",
    );
    // … and applied exactly the appended suffix.
    let executions_after = chain.executions_performed().await.unwrap();
    assert_eq!(
        executions_after - executions_before,
        2,
        "exactly the two appended immediates should have applied",
    );
    // A no-op leaves the root where it was.
    assert_eq!(
        chain.state_root().await.unwrap(),
        live_root,
        "a no-op suffix must not move the stored root",
    );

    // The deriver has now seen a resumed batch. It must neither diverge nor
    // wedge: settlement continues afterwards.
    node.assert_no_divergence_failure_logs();
    let batches_now = chain.batches_posted().await.unwrap();
    chain
        .wait_for_batches(batches_now + 2, TIMEOUT)
        .await
        .expect("composer stopped settling after observing a resumed batch");
    node.assert_no_divergence_failure_logs();
    node.assert_no_process_death();
}
