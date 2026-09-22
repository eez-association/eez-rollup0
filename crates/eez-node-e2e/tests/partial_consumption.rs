//! A user transaction that reverts AT INCLUSION leaves L1 settled at a PREFIX:
//! its entry goes unconsumed, and every entry after it fails `currentState`.
//!
//! `ParityGate` reverts on ODD L1 blocks, so a call composed at an even anchor
//! passes simulation and reverts when the batch lands a block later.
//!
//! Direct is queued first, so the surviving prefix is non-empty.
use std::time::Duration;

use alloy_primitives::U256;
use alloy_sol_types::SolCall;
use eez_testkit::{
    DEV_CHAIN_ID, INBOUND_USER, IValue, TARGET_DEPLOYER, batches_posted, deploy_parity_gate,
    onchain_nonce, setup_cross_chain, sign_and_send, signals, wait_for,
};

const TIMEOUT: Duration = Duration::from_mins(6);
/// Each round costs one Sync slot and hits a revert only on an odd anchor, so
/// several rounds are needed before one lands.
const ROUNDS: usize = 12;

/// IGNORED: `builder-stub.py` refuses multi-tx bundles, so the submitter falls
/// back to the mempool and `revertingTxHashes` never reaches a relay.
///
/// The apparatus itself is sound: both calls drain into one slot and become
/// entries, and nothing is evicted at compose time.
///
/// The prefix path is covered on the kurtosis rig, whose rbuilder honours the
/// whitelist — see `testing/kurtosis/scripts/parity-gate-host.sh`.
#[ignore = "builder-stub refuses multi-tx bundles, so revertingTxHashes never \
            reaches a relay; the kurtosis rig covers this path"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revert_at_inclusion_settles_a_prefix_and_burns_only_that_nonce() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();

    // Forwards to the very proxy the direct call uses, so the two differ only
    // in whether the gate is in the path.
    let gate = deploy_parity_gate(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, w.setter_proxy)
        .await
        .expect("ParityGate deploys on L1");

    let mut observed_prefix = false;
    for round in 0..ROUNDS {
        let nonce = onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
        let v = u64::try_from(round).unwrap();

        // Direct first: it survives every parity, so the prefix it forms is
        // what distinguishes a PREFIX settlement from an anchor-only one.
        let direct = sign_and_send(
            &w.l1_xchain(),
            INBOUND_USER,
            DEV_CHAIN_ID,
            nonce,
            Some(w.setter_proxy),
            U256::ZERO,
            IValue::setValueCall {
                v: U256::from(100 + v),
            }
            .abi_encode(),
            600_000,
        )
        .await;
        // Through the gate: reverts iff the batch lands on an odd L1 block.
        let gated = sign_and_send(
            &w.l1_xchain(),
            INBOUND_USER,
            DEV_CHAIN_ID,
            nonce + 1,
            Some(gate),
            U256::ZERO,
            IValue::setValueCall {
                v: U256::from(200 + v),
            }
            .abi_encode(),
            600_000,
        )
        .await;
        // Ingress may refuse while a front is still starting; that round is
        // simply skipped rather than failing the test.
        if direct.is_err() || gated.is_err() {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        // Give the pair a slot to drain, settle and be observed.
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let prefix = w
                .node
                .count_signals(&[
                    signals::COMPOSER_SETTLED_SHORT,
                    signals::DERIVER_PARTIAL_CONSUMPTION,
                ])
                .unwrap_or(0);
            if prefix > 0 {
                observed_prefix = true;
                break;
            }
        }
        if observed_prefix {
            break;
        }
    }

    assert!(
        observed_prefix,
        "no prefix settlement in {ROUNDS} rounds; the gate never reverted at \
         inclusion, so this test exercised nothing it exists for",
    );

    // A prefix is a settlement, not a fault: the entries that ran stay, and the
    // chain keeps going without the deriver disagreeing with L1.
    w.node.assert_no_divergence_failure_logs();
    w.node.assert_no_process_death();

    // The reverting tx has an L1 receipt, so its nonce is burned and it is NOT
    // re-queued — the user resubmits.
    assert!(
        w.node
            .count_signal(signals::COMPOSER_NONCE_BURNED)
            .unwrap_or(0)
            > 0,
        "a reverted user_tx must be released rather than re-queued",
    );

    // And settlement continues afterwards: a prefix must not wedge the cursor.
    let before = batches_posted(&l1_rpc, w.dep.eez_address, w.dep.deploy_block)
        .await
        .unwrap();
    wait_for(TIMEOUT, || async {
        let now = batches_posted(&l1_rpc, w.dep.eez_address, w.dep.deploy_block).await?;
        Ok((now >= before + 2).then_some(now))
    })
    .await
    .expect("the composer keeps settling after a prefix");
    w.node.assert_no_divergence_failure_logs();
}
