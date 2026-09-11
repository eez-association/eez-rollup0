//! Projection between an execution entry and the DA action that describes it.
//!
//! The DA publishes actions, not entries. Every field an entry carries is
//! either one of the action's own fields or derived from them: the proxy-entry
//! hash is the cross-chain call hash over exactly those fields, and the rolling
//! hash folds that hash with the outcome. Publishing the entry instead would
//! ship hashes a follower has to recompute anyway — and, for an inbound call,
//! the on-chain entry is lean (both call vectors empty), so the DA is the only
//! place the call itself appears.
//!
//! The projection must be lossless for every entry we emit: derivation rebuilds
//! the entry from the action and feeds it to the same system-transaction
//! builder the composer used, so a dropped field is a divergence. The
//! round-trip test below is what enforces that.

use alloy_primitives::{Address, Bytes, U256};
use eez_payload_codec::Action;

use crate::abi::{ExecutionEntrySol, L2ToL1CallSol};
use crate::action::{CallHashInput, CallMode, common_cross_chain_call_hash};
use crate::rolling_hash::EntryRollingHash;
use crate::{ProtocolResult, RollupId};

/// Describe `entry` as the action it was built from.
///
/// # Errors
///
/// Returns an error when the entry carries no call to describe, or carries
/// more than one — neither shape is produced by the entry builders.
pub fn action_from_entry(entry: &ExecutionEntrySol, local: RollupId) -> ProtocolResult<Action> {
    let [call] = entry.l2ToL1Calls.as_slice() else {
        return Err(crate::ProtocolErrorKind::InvalidEncoding(
            "a DA action needs exactly one call on its entry".to_string(),
        )
        .into());
    };
    // An entry whose call originates here is our outbound settlement, so its
    // target is the settlement L1; anything else is an inbound delivery to us.
    let source_rollup_id = RollupId(call.sourceRollupId);
    let target_rollup_id = if source_rollup_id == local {
        RollupId::MAINNET
    } else {
        RollupId(entry.destinationRollupId)
    };
    Ok(Action {
        source_rollup_id: source_rollup_id.0,
        target_rollup_id: target_rollup_id.0,
        source_address: call.sourceAddress.into(),
        target_address: call.targetAddress.into(),
        value: call.value.to_be_bytes(),
        gas: call.gas,
        data: call.data.to_vec(),
        success: entry.success,
        return_data: entry.returnData.to_vec(),
    })
}

/// Rebuild the execution entry `action` describes.
///
/// # Errors
///
/// Returns an error when the action names neither us as the target nor us as
/// the source, so it belongs to no rollup we derive.
pub fn entry_from_action(action: &Action, local: RollupId) -> ProtocolResult<ExecutionEntrySol> {
    let source_rollup_id = RollupId(action.source_rollup_id);
    let target_rollup_id = RollupId(action.target_rollup_id);
    let outbound = source_rollup_id == local && target_rollup_id.is_mainnet();
    let inbound = target_rollup_id == local;
    if !outbound && !inbound {
        return Err(crate::ProtocolErrorKind::InvalidEncoding(
            "a DA action must have this rollup as its source or its target".to_string(),
        )
        .into());
    }

    let value = U256::from_be_bytes(action.value);
    let data = Bytes::copy_from_slice(&action.data);
    let return_data = Bytes::copy_from_slice(&action.return_data);
    let call = L2ToL1CallSol {
        revertNextNCalls: 0,
        isStatic: false,
        gas: action.gas,
        sourceAddress: Address::from(action.source_address),
        sourceRollupId: source_rollup_id.0,
        targetAddress: Address::from(action.target_address),
        value,
        data: data.clone(),
    };

    // Outbound entries lead the batch as immediates: a zero proxy hash marks
    // them, and their rolling hash stays unfinalized until the Composer
    // attaches the ordered state updates.
    let (proxy_entry_hash, rolling_hash) = if outbound {
        (alloy_primitives::B256::ZERO, alloy_primitives::B256::ZERO)
    } else {
        let call_hash = common_cross_chain_call_hash(CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: call.sourceAddress,
            source_rollup_id,
            target_address: call.targetAddress,
            target_rollup_id,
            value,
            data: &data,
        });
        let mut rolling = EntryRollingHash::seed_for_l2(call_hash);
        rolling.call_begin(call_hash);
        rolling.call_end(action.success, &return_data);
        (call_hash, rolling.current())
    };

    Ok(ExecutionEntrySol {
        stateUpdates: Vec::new(),
        proxyEntryHash: proxy_entry_hash,
        l2ToL1Calls: vec![call],
        expectedL1ToL2Calls: Vec::new(),
        rollingHash: rolling_hash,
        destinationRollupId: local.0,
        success: action.success,
        returnData: return_data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entries::{build_inbound_target_entries, build_l1_postbatch};
    use crate::types::{ExecutedAction, ExecutionOutcome};
    use alloy_primitives::address;
    use alloy_sol_types::SolValue as _;

    const LOCAL: RollupId = RollupId(1);

    fn call(source: RollupId, target: RollupId, value: U256, data: &[u8]) -> ExecutedAction {
        ExecutedAction {
            call_mode: CallMode::Mutable,
            target_address: address!("00000000000000000000000000000000000000bb"),
            target_rollup_id: target,
            source_rollup_id: source,
            source_address: address!("00000000000000000000000000000000000000cc"),
            data: Bytes::copy_from_slice(data),
            value,
            outcome: ExecutionOutcome::Resolved {
                return_data: vec![0xab, 0xcd],
                gas_used: 0,
                success: true,
            },
            revert_span: None,
        }
    }

    /// The projection must be lossless for every entry the builders emit —
    /// derivation rebuilds entries from actions and feeds them to the same
    /// system-transaction builder, so any dropped field is a divergence.
    #[test]
    fn every_emitted_entry_round_trips_through_its_action() {
        let inbound = build_inbound_target_entries(
            &[
                call(RollupId::MAINNET, LOCAL, U256::ZERO, &[0x11, 0x22]),
                // Value-bearing: the deposit shape, whose value exists on L1
                // only inside the proxy-entry hash preimage.
                call(RollupId::MAINNET, LOCAL, U256::from(42), &[]),
            ],
            LOCAL,
        )
        .expect("inbound sidecar builds");

        let outbound = build_l1_postbatch(
            &[call(LOCAL, RollupId::MAINNET, U256::from(7), &[0x33])],
            LOCAL,
        )
        .expect("outbound settlement builds");

        for (kind, batch) in [("inbound", inbound), ("outbound", outbound)] {
            for (i, entry) in batch.entries.iter().enumerate() {
                let action = action_from_entry(entry, LOCAL)
                    .unwrap_or_else(|e| panic!("{kind}[{i}] projects: {e}"));
                let rebuilt = entry_from_action(&action, LOCAL)
                    .unwrap_or_else(|e| panic!("{kind}[{i}] rebuilds: {e}"));
                // Byte equality, which is what derivation actually needs.
                assert_eq!(
                    rebuilt.abi_encode(),
                    entry.abi_encode(),
                    "{kind}[{i}] must round-trip exactly",
                );
            }
        }
    }

    /// The derived hashes are the point: they are recomputed from the action's
    /// own fields, never carried, so a tampered field cannot keep its hash.
    #[test]
    fn a_changed_field_changes_the_derived_hashes() {
        let batch = build_inbound_target_entries(
            &[call(RollupId::MAINNET, LOCAL, U256::ZERO, &[0x11])],
            LOCAL,
        )
        .unwrap();
        let action = action_from_entry(&batch.entries[0], LOCAL).unwrap();

        let mut tampered = action.clone();
        tampered.value = U256::from(1).to_be_bytes();
        let rebuilt = entry_from_action(&tampered, LOCAL).unwrap();
        assert_ne!(rebuilt.proxyEntryHash, batch.entries[0].proxyEntryHash);
        assert_ne!(rebuilt.rollingHash, batch.entries[0].rollingHash);
    }

    #[test]
    fn an_action_for_another_rollup_is_rejected() {
        let action = Action {
            source_rollup_id: 7,
            target_rollup_id: 9,
            ..Default::default()
        };
        assert!(entry_from_action(&action, LOCAL).is_err());
    }
}
