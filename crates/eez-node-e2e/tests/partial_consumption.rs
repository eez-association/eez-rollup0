//! Partial and anchor-only settlement coverage with inclusion-time reverts.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use alloy_primitives::{B256, U256};
use alloy_rpc_types_eth::BlockNumberOrTag;
use alloy_sol_types::SolCall;
use eez_testkit::{
    DEV_CHAIN_ID, INBOUND_USER, IValue, NodeBinary, NodeConfig, NodeHandle, OUTBOUND_USER,
    TARGET_DEPLOYER, batches_posted, block_number_and_hash_at, deploy_parity_gate, l2_value,
    onchain_nonce, rollup_commitment, safe_block_hash, setup_cross_chain, sign_and_send, signals,
    wait_for, wait_for_safe_chain_contains,
};

const TIMEOUT: Duration = Duration::from_mins(6);
const ROUNDS: usize = 12;
fn assert_settlements_use_dispatched_sync_heights(
    records: &[eez_testkit::NodeSignal],
) -> anyhow::Result<()> {
    let mut dispatched = HashMap::new();
    let mut settled = Vec::new();
    for record in records {
        match record.name.as_str() {
            signals::COMPOSER_BUNDLE_DISPATCHED | signals::COMPOSER_PHASE1_BUNDLE_DISPATCHED => {
                dispatched.insert(record.b256("post_batch_hash")?, record.u64("sync_height")?);
            }
            signals::COMPOSER_HISTORICAL_CHUNK => {
                dispatched.insert(record.b256("post_batch_hash")?, record.u64("boundary")?);
            }
            signals::DERIVER_SAFE_ADVANCED => {
                settled.push((record.b256("tx_hash")?, record.u64("to_block")?));
            }
            _ => {}
        }
    }
    anyhow::ensure!(
        !settled.is_empty(),
        "no included batch advanced the safe head"
    );
    for (tx_hash, height) in settled {
        let dispatched_height = dispatched.get(&tx_hash).ok_or_else(|| {
            anyhow::anyhow!("included batch {tx_hash} has no correlated dispatch")
        })?;
        anyhow::ensure!(
            *dispatched_height == height,
            "included batch {tx_hash} settled at {height}, not its dispatched Sync height {dispatched_height}"
        );
    }
    Ok(())
}

#[test]
fn settlement_height_oracle_rejects_a_non_sync_height() {
    let tx_hash = B256::repeat_byte(0x11);
    let record = |name: &str, fields: &[(&str, serde_json::Value)]| eez_testkit::NodeSignal {
        name: name.to_owned(),
        fields: serde_json::Map::from_iter(
            fields
                .iter()
                .map(|(field, value)| ((*field).to_owned(), value.clone())),
        ),
    };
    let records = [
        record(
            signals::COMPOSER_BUNDLE_DISPATCHED,
            &[
                ("sync_height", 10.into()),
                ("post_batch_hash", tx_hash.to_string().into()),
            ],
        ),
        record(
            signals::DERIVER_SAFE_ADVANCED,
            &[
                ("tx_hash", tx_hash.to_string().into()),
                ("to_block", 9.into()),
            ],
        ),
    ];
    assert!(assert_settlements_use_dispatched_sync_heights(&records).is_err());
}

#[test]
fn settlement_height_oracle_accepts_a_minimal_bundle_sync_height() {
    let tx_hash = B256::repeat_byte(0x22);
    let record = |name: &str, fields: &[(&str, serde_json::Value)]| eez_testkit::NodeSignal {
        name: name.to_owned(),
        fields: serde_json::Map::from_iter(
            fields
                .iter()
                .map(|(field, value)| ((*field).to_owned(), value.clone())),
        ),
    };
    let records = [
        record(
            signals::COMPOSER_PHASE1_BUNDLE_DISPATCHED,
            &[
                ("sync_height", 10.into()),
                ("post_batch_hash", tx_hash.to_string().into()),
            ],
        ),
        record(
            signals::DERIVER_SAFE_ADVANCED,
            &[
                ("tx_hash", tx_hash.to_string().into()),
                ("to_block", 10.into()),
            ],
        ),
    ];
    assert!(assert_settlements_use_dispatched_sync_heights(&records).is_ok());
}

#[test]
fn settlement_height_oracle_uses_a_historical_chunk_boundary() {
    let tx_hash = B256::repeat_byte(0x23);
    let record = |name: &str, fields: &[(&str, serde_json::Value)]| eez_testkit::NodeSignal {
        name: name.to_owned(),
        fields: serde_json::Map::from_iter(
            fields
                .iter()
                .map(|(field, value)| ((*field).to_owned(), value.clone())),
        ),
    };
    let records = [
        record(
            signals::COMPOSER_HISTORICAL_CHUNK,
            &[
                ("sync_height", 20.into()),
                ("boundary", 12.into()),
                ("post_batch_hash", tx_hash.to_string().into()),
            ],
        ),
        record(
            signals::DERIVER_SAFE_ADVANCED,
            &[
                ("tx_hash", tx_hash.to_string().into()),
                ("to_block", 12.into()),
            ],
        ),
    ];
    assert!(assert_settlements_use_dispatched_sync_heights(&records).is_ok());
}

#[test]
fn settlement_height_oracle_rejects_swapped_batch_heights() {
    let first = B256::repeat_byte(0x33);
    let second = B256::repeat_byte(0x44);
    let record = |name: &str, fields: &[(&str, serde_json::Value)]| eez_testkit::NodeSignal {
        name: name.to_owned(),
        fields: serde_json::Map::from_iter(
            fields
                .iter()
                .map(|(field, value)| ((*field).to_owned(), value.clone())),
        ),
    };
    let records = [
        record(
            signals::COMPOSER_BUNDLE_DISPATCHED,
            &[
                ("sync_height", 10.into()),
                ("post_batch_hash", first.to_string().into()),
            ],
        ),
        record(
            signals::COMPOSER_BUNDLE_DISPATCHED,
            &[
                ("sync_height", 12.into()),
                ("post_batch_hash", second.to_string().into()),
            ],
        ),
        record(
            signals::DERIVER_SAFE_ADVANCED,
            &[
                ("tx_hash", first.to_string().into()),
                ("to_block", 12.into()),
            ],
        ),
        record(
            signals::DERIVER_SAFE_ADVANCED,
            &[
                ("tx_hash", second.to_string().into()),
                ("to_block", 10.into()),
            ],
        ),
    ];
    assert!(assert_settlements_use_dispatched_sync_heights(&records).is_err());
}

async fn assert_follower_reaches(w: &eez_testkit::CrossChainWorld, expected: (u64, B256)) {
    let follower_cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(w.cfg.l2_genesis.0.as_path()),
    };
    let follower = NodeHandle::start(
        "partial-settlement-follower",
        &follower_cfg,
        &w.follower_env(),
    )
    .await
    .unwrap();
    wait_for_safe_chain_contains(&follower, expected.0, expected.1, TIMEOUT)
        .await
        .expect("follower did not reproduce the settled safe block");
    follower.assert_no_process_death();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inclusion_revert_settles_a_mixed_prefix_and_follower_converges() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let gate = deploy_parity_gate(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, w.setter_proxy)
        .await
        .expect("ParityGate deploys on L1");
    let signal_cursor = w.node.signal_cursor().unwrap();

    let mut partial_sync_height = None;
    let mut gated_hashes = HashSet::new();
    for round in 0..ROUNDS {
        let inbound_nonce = onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
        let outbound_nonce = onchain_nonce(&l2_rpc, OUTBOUND_USER).await.unwrap();
        let value = u64::try_from(round).unwrap();

        let direct = sign_and_send(
            &w.l1_xchain(),
            INBOUND_USER,
            DEV_CHAIN_ID,
            inbound_nonce,
            Some(w.setter_proxy),
            U256::ZERO,
            IValue::setValueCall {
                v: U256::from(100 + value),
            }
            .abi_encode(),
            600_000,
        )
        .await;
        let outbound = sign_and_send(
            &w.l2_xchain(),
            OUTBOUND_USER,
            w.l2_chain_id,
            outbound_nonce,
            Some(w.outbound_proxy),
            U256::ZERO,
            IValue::setValueCall {
                v: U256::from(150 + value),
            }
            .abi_encode(),
            600_000,
        )
        .await;
        let gated = sign_and_send(
            &w.l1_xchain(),
            INBOUND_USER,
            DEV_CHAIN_ID,
            inbound_nonce + 1,
            Some(gate),
            U256::ZERO,
            IValue::setValueCall {
                v: U256::from(200 + value),
            }
            .abi_encode(),
            600_000,
        )
        .await;
        let (Ok(_), Ok(_), Ok(gated_hash)) = (direct, outbound, gated) else {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        };
        gated_hashes.insert(gated_hash);

        partial_sync_height = wait_for(Duration::from_secs(40), || async {
            let records = w.node.signals_since(signal_cursor)?;
            for record in records
                .iter()
                .filter(|record| record.name == signals::DERIVER_PARTIAL_CONSUMPTION)
            {
                let outbound = record.u64("outbound")?;
                let inbound = record.u64("inbound")?;
                let outbound_applied = record.u64("outbound_applied")?;
                let inbound_applied = record.u64("inbound_applied")?;
                if outbound_applied > 0
                    && inbound_applied > 0
                    && (outbound_applied < outbound || inbound_applied < inbound)
                {
                    let tx_hash = record.b256("tx_hash")?;
                    if let Some(advanced) = records.iter().find(|candidate| {
                        candidate.name == signals::DERIVER_SAFE_ADVANCED
                            && candidate.b256("tx_hash").ok() == Some(tx_hash)
                    }) {
                        return Ok(Some(advanced.u64("to_block")?));
                    }
                }
            }
            Ok(None)
        })
        .await
        .ok();
        if partial_sync_height.is_some() {
            break;
        }
    }

    let partial_sync_height =
        partial_sync_height.expect("no inclusion-time revert produced a prefix settlement");
    wait_for(Duration::from_secs(40), || async {
        let records = w.node.signals_since(signal_cursor)?;
        let classified = records.iter().any(|record| {
            record.name == signals::COMPOSER_SETTLED_PARTIAL
                && record.u64("sync_height").ok() == Some(partial_sync_height)
        });
        let nonce_burned = records.iter().any(|record| {
            record.name == signals::COMPOSER_NONCE_BURNED
                && record
                    .b256("tx_hash")
                    .is_ok_and(|tx_hash| gated_hashes.contains(&tx_hash))
        });
        Ok((classified && nonce_burned).then_some(()))
    })
    .await
    .expect("composer did not finish recovering the partial settlement");

    wait_for(TIMEOUT, || async {
        let commitment = rollup_commitment(&l1_rpc, w.dep.eez_address, w.dep.rollup_id).await?;
        let safe = safe_block_hash(&l2_rpc).await?;
        Ok((safe == Some(commitment)).then_some(()))
    })
    .await
    .expect("L1 commitment and L2 safe hash did not converge");
    assert!(
        l2_value(&l1_rpc, w.outbound_value).await.unwrap() >= U256::from(150),
        "the outbound entry in the surviving prefix was not applied",
    );
    w.node.assert_no_divergence_failure_logs();
    w.node.assert_no_process_death();
    let records = w.node.signals_since(0).unwrap();
    assert_settlements_use_dispatched_sync_heights(&records).unwrap();
    let settled = block_number_and_hash_at(&l2_rpc, BlockNumberOrTag::Safe)
        .await
        .unwrap()
        .expect("partial settlement must produce a safe block");
    assert_follower_reaches(&w, settled).await;

    let before = batches_posted(&l1_rpc, w.dep.eez_address, w.dep.deploy_block)
        .await
        .unwrap();
    wait_for(TIMEOUT, || async {
        let now = batches_posted(&l1_rpc, w.dep.eez_address, w.dep.deploy_block).await?;
        Ok((now > before).then_some(()))
    })
    .await
    .expect("settlement did not continue after partial consumption");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inclusion_revert_can_settle_only_the_anchor() {
    let w = setup_cross_chain().await.unwrap();
    let l1_rpc = w.l1_rpc();
    let l2_rpc = w.l2_rpc();
    let gate = deploy_parity_gate(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, w.setter_proxy)
        .await
        .expect("ParityGate deploys on L1");
    let signal_cursor = w.node.signal_cursor().unwrap();

    let mut anchor_only = None;
    for round in 0..ROUNDS {
        let nonce = onchain_nonce(&l1_rpc, INBOUND_USER).await.unwrap();
        if sign_and_send(
            &w.l1_xchain(),
            INBOUND_USER,
            DEV_CHAIN_ID,
            nonce,
            Some(gate),
            U256::ZERO,
            IValue::setValueCall {
                v: U256::from(300 + u64::try_from(round).unwrap()),
            }
            .abi_encode(),
            600_000,
        )
        .await
        .is_err()
        {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        anchor_only = wait_for(Duration::from_secs(40), || async {
            let records = w.node.signals_since(signal_cursor)?;
            for partial in records
                .iter()
                .filter(|record| record.name == signals::DERIVER_PARTIAL_CONSUMPTION)
            {
                let total = partial.u64("outbound")? + partial.u64("inbound")?;
                if total == 0
                    || partial.u64("outbound_applied")? != 0
                    || partial.u64("inbound_applied")? != 0
                {
                    continue;
                }
                let tx_hash = partial.b256("tx_hash")?;
                if let Some(advanced) = records.iter().find(|record| {
                    record.name == signals::DERIVER_SAFE_ADVANCED
                        && record.u64("applied_entries").ok() == Some(1)
                        && record.b256("tx_hash").ok() == Some(tx_hash)
                }) {
                    return Ok(Some((
                        advanced.u64("to_block")?,
                        advanced.b256("new_safe_hash")?,
                        advanced.b256("l1_settled_commitment")?,
                    )));
                }
            }
            Ok(None)
        })
        .await
        .ok();
        if anchor_only.is_some() {
            break;
        }
    }

    let settled = anchor_only.expect("no inclusion-time revert produced an anchor-only settlement");
    assert_eq!(settled.1, settled.2);
    let canonical = block_number_and_hash_at(&l2_rpc, BlockNumberOrTag::Number(settled.0))
        .await
        .unwrap()
        .expect("anchor-only settlement height must be canonical");
    assert_eq!(canonical.1, settled.1);
    let records = w.node.signals_since(0).unwrap();
    assert_settlements_use_dispatched_sync_heights(&records).unwrap();
    assert_follower_reaches(&w, (settled.0, settled.1)).await;
    w.node.assert_no_divergence_failure_logs();
    w.node.assert_no_process_death();
}
