//! Entry construction and ABI encoding for the pinned protocol.
//!
//! The materializer accepts mutable, successful, top-level calls with no
//! revert span. It rejects other shapes before emitting entries.

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::SolCall;
use tracing::{debug, trace};

use crate::abi::{
    CrossChainCallSol, EvmBatch, ExecutionEntrySol, L2ExecutionEntrySol, L2ToL1CallSol,
    postAndVerifyBatchCall,
};
use crate::action::{CallHashInput, CallMode, common_cross_chain_call_hash, l2_outbound_call_hash};
use crate::{ExecutedAction, ProtocolResult, RollupId, rolling_hash::EntryRollingHash};

const UNSUCCESSFUL_CALL: &str = "unsuccessful cross-chain calls are not supported";
const STATIC_CALL: &str = "static cross-chain calls are not supported";
const REVERT_SPAN: &str = "cross-chain call revert spans are not supported";
const NESTED_CALL: &str = "nested cross-chain calls are not supported";

/// Reject calls that the entry profile cannot represent exactly.
pub(crate) fn ensure_materializable_calls(calls: &[ExecutedAction]) -> ProtocolResult<()> {
    for call in calls {
        supported_return_data(call)?;
    }
    Ok(())
}

/// Reject calls that did not originate on the batch's source rollup.
pub(crate) fn ensure_source_side_calls<'a>(
    calls: impl IntoIterator<Item = &'a ExecutedAction>,
    source_rollup_id: RollupId,
) -> ProtocolResult<()> {
    if calls
        .into_iter()
        .any(|call| call.source_rollup_id != source_rollup_id)
    {
        return Err(crate::ProtocolErrorKind::Unsupported(NESTED_CALL).into());
    }
    Ok(())
}

/// Build the source-chain table for top-level calls originating on
/// `source_rollup_id`.
///
/// L2 source entries are seed-only: the user transaction performs the outer
/// call, so the table stores its precomputed result without re-executing it.
/// L1 entries remain unfinalized until their `StateUpdate`s are attached and
/// [`finalize_l1_rolling_hashes`] is called.
///
/// # Errors
///
/// Returns an error for pending, static, unsuccessful, reverted-span, or
/// nested calls.
#[tracing::instrument(level = "debug", name = "build_batch", skip_all, fields(source = %source_rollup_id), err)]
pub fn build_batch(
    recorded: &[ExecutedAction],
    source_rollup_id: RollupId,
) -> ProtocolResult<EvmBatch> {
    ensure_materializable_calls(recorded)?;
    let group = recorded
        .iter()
        .filter(|call| {
            call.source_rollup_id == source_rollup_id || call.target_rollup_id == source_rollup_id
        })
        .collect::<Vec<_>>();

    ensure_source_side_calls(group.iter().copied(), source_rollup_id)?;

    let entries = group
        .into_iter()
        .map(|call| {
            let return_data = Bytes::copy_from_slice(supported_return_data(call)?);
            let proxy_entry_hash = source_side_call_hash(call);
            let rolling_hash = if source_rollup_id.is_mainnet() {
                // The L1 seed depends on StateUpdates that are attached later.
                B256::ZERO
            } else {
                EntryRollingHash::seed_for_l2(proxy_entry_hash).current()
            };

            Ok(ExecutionEntrySol {
                stateUpdates: Vec::new(),
                proxyEntryHash: proxy_entry_hash,
                l2ToL1Calls: Vec::new(),
                expectedL1ToL2Calls: Vec::new(),
                rollingHash: rolling_hash,
                destinationRollupId: call.target_rollup_id.0,
                success: true,
                returnData: return_data,
            })
        })
        .collect::<ProtocolResult<Vec<_>>>()?;

    debug!(
        target: "eez::entries",
        %source_rollup_id,
        recorded = recorded.len(),
        entries = entries.len(),
        "source execution table built",
    );

    Ok(batch_with_entries(entries, 0))
}

/// Build immediate L1 entries for successful L2-to-L1 calls.
///
/// The entries intentionally carry an unfinished zero `rollingHash`: their L1
/// seed cannot be computed until the Composer attaches the final ordered
/// `StateUpdate`s and calls [`finalize_l1_rolling_hashes`].
pub(crate) fn build_l1_postbatch(
    calls: &[ExecutedAction],
    source_rollup_id: RollupId,
) -> ProtocolResult<EvmBatch> {
    let mut entries = Vec::with_capacity(calls.len());
    for call in calls {
        let return_data = supported_top_level_return_data(call, source_rollup_id)?;
        if !call.target_rollup_id.is_mainnet() {
            return Err(crate::ProtocolErrorKind::Unsupported(
                "L1 post-batch entries only support L2-to-L1 calls",
            )
            .into());
        }

        entries.push(ExecutionEntrySol {
            stateUpdates: Vec::new(),
            proxyEntryHash: B256::ZERO,
            l2ToL1Calls: vec![l1_call_from_action(call)],
            expectedL1ToL2Calls: Vec::new(),
            // Finalized only after StateUpdates are stitched by the Composer.
            rollingHash: B256::ZERO,
            destinationRollupId: source_rollup_id.0,
            success: true,
            returnData: Bytes::copy_from_slice(return_data),
        });
    }

    trace!(
        target: "eez::entries",
        %source_rollup_id,
        entries = entries.len(),
        "unfinalized immediate L1 entries built",
    );
    let immediate_count = entries.len();
    Ok(batch_with_entries(entries, immediate_count))
}

/// Finalize every mutable L1 entry after the Composer has attached its ordered
/// state updates.
///
/// Every entry must be successful, have at least one `StateUpdate`, contain no
/// reentrant expected calls, and contain at most one flat mutable call with no
/// gas limit or revert span.
///
/// # Errors
///
/// Returns an error when any entry is incomplete or outside that profile.
pub fn finalize_l1_rolling_hashes(batch: &mut EvmBatch) -> ProtocolResult<()> {
    if !batch.staticEntries.is_empty() {
        return Err(crate::ProtocolErrorKind::Unsupported(STATIC_CALL).into());
    }

    for (entry_index, entry) in batch.entries.iter_mut().enumerate() {
        if entry.stateUpdates.is_empty() {
            return Err(crate::ProtocolErrorKind::InvalidEncoding(format!(
                "L1 entry {entry_index} has no StateUpdates"
            ))
            .into());
        }
        if !entry.success {
            return Err(crate::ProtocolErrorKind::Unsupported(UNSUCCESSFUL_CALL).into());
        }
        if !entry.expectedL1ToL2Calls.is_empty() {
            return Err(crate::ProtocolErrorKind::Unsupported(NESTED_CALL).into());
        }
        if entry.l2ToL1Calls.len() > 1 {
            return Err(crate::ProtocolErrorKind::Unsupported(
                "multiple flat calls in one L1 entry are not supported",
            )
            .into());
        }

        let mut rolling_hash = EntryRollingHash::seed_for_l1(
            entry
                .stateUpdates
                .iter()
                .map(|update| (update.rollupId, update.currentState)),
            entry.proxyEntryHash,
        );

        if let Some(call) = entry.l2ToL1Calls.first() {
            ensure_supported_flat_call(call)?;
            let call_hash = common_cross_chain_call_hash(CallHashInput {
                call_mode: CallMode::Mutable,
                source_address: call.sourceAddress,
                source_rollup_id: RollupId(call.sourceRollupId),
                target_address: call.targetAddress,
                target_rollup_id: RollupId::MAINNET,
                value: call.value,
                data: &call.data,
            });
            rolling_hash.call_begin(call_hash);
            rolling_hash.call_end(entry.success, &entry.returnData);
        }

        entry.rollingHash = rolling_hash.current();
    }

    Ok(())
}

/// Return the ETH released by the supported flat L1 entry shape.
///
/// Empty entries release zero. A single successful call releases its value; a
/// single unsuccessful call releases zero. More than one flat call or any
/// nested expected call returns `None` so callers cannot undercount effects.
#[must_use]
pub fn outbound_ether_out(entry: &ExecutionEntrySol) -> Option<U256> {
    if !entry.expectedL1ToL2Calls.is_empty() {
        return None;
    }
    match entry.l2ToL1Calls.as_slice() {
        [] => Some(U256::ZERO),
        [call] => Some(if entry.success {
            call.value
        } else {
            U256::ZERO
        }),
        _ => None,
    }
}

/// Inputs for one incoming L2 execution entry.
#[derive(Clone, Debug)]
pub struct IncomingEntry {
    /// Destination contract on the L2.
    pub target: Address,
    /// Original caller on the source chain.
    pub source: Address,
    /// Native value forwarded with the call.
    pub value: U256,
    /// Calldata forwarded to the destination.
    pub data: Bytes,
    /// Rollup containing the original caller.
    pub source_rollup_id: RollupId,
    /// Destination L2 rollup ID.
    pub l2_rollup_id: RollupId,
    /// Precomputed return data.
    pub return_data: Bytes,
    /// Precomputed execution outcome; unsuccessful outcomes are rejected.
    pub success: bool,
}

/// Build one exact L2 entry for an incoming mutable call.
///
/// # Errors
///
/// Returns an error for an unsuccessful outcome.
pub fn build_l2_incoming_entry(entry: IncomingEntry) -> ProtocolResult<L2ExecutionEntrySol> {
    let IncomingEntry {
        target,
        source,
        value,
        data,
        source_rollup_id,
        l2_rollup_id,
        return_data,
        success,
    } = entry;
    if !success {
        return Err(crate::ProtocolErrorKind::Unsupported(UNSUCCESSFUL_CALL).into());
    }

    let call_hash = common_cross_chain_call_hash(CallHashInput {
        call_mode: CallMode::Mutable,
        source_address: source,
        source_rollup_id,
        target_address: target,
        target_rollup_id: l2_rollup_id,
        value,
        data: &data,
    });
    let mut rolling_hash = EntryRollingHash::seed_for_l2(call_hash);
    rolling_hash.call_begin(call_hash);
    rolling_hash.call_end(true, &return_data);

    Ok(L2ExecutionEntrySol {
        proxyEntryHash: call_hash,
        incomingCalls: vec![CrossChainCallSol {
            revertNextNCalls: 0,
            isStatic: false,
            gas: 0,
            sourceAddress: source,
            sourceRollupId: source_rollup_id.0,
            targetAddress: target,
            value,
            data,
        }],
        expectedOutgoingCalls: Vec::new(),
        rollingHash: rolling_hash.current(),
        success: true,
        returnData: return_data,
    })
}

/// Inputs for one source-L2 outbound entry.
#[derive(Clone, Debug)]
pub struct OutboundEntry {
    /// Destination contract on L1.
    pub target: Address,
    /// Original caller on the L2.
    pub source: Address,
    /// Native value carried by the call.
    pub value: U256,
    /// Call data sent to L1.
    pub data: Bytes,
    /// Source L2 rollup ID.
    pub l2_rollup_id: RollupId,
    /// Precomputed L1 return data.
    pub return_data: Bytes,
    /// Precomputed execution outcome; unsuccessful outcomes are rejected.
    pub success: bool,
}

/// Build the seed-only source-L2 entry for an outbound mutable call.
///
/// The gas-aware proxy key identifies the user call. No incoming call is
/// emitted because the source L2 must not re-execute the L1 target.
///
/// # Errors
///
/// Returns an error for an unsuccessful outcome.
pub fn build_l2_outbound_entry(entry: OutboundEntry) -> ProtocolResult<L2ExecutionEntrySol> {
    let OutboundEntry {
        target,
        source,
        value,
        data,
        l2_rollup_id,
        return_data,
        success,
    } = entry;
    if !success {
        return Err(crate::ProtocolErrorKind::Unsupported(UNSUCCESSFUL_CALL).into());
    }

    let proxy_entry_hash = l2_outbound_call_hash(
        CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: source,
            source_rollup_id: l2_rollup_id,
            target_address: target,
            target_rollup_id: RollupId::MAINNET,
            value,
            data: &data,
        },
        0,
    );

    Ok(L2ExecutionEntrySol {
        proxyEntryHash: proxy_entry_hash,
        incomingCalls: Vec::new(),
        expectedOutgoingCalls: Vec::new(),
        rollingHash: EntryRollingHash::seed_for_l2(proxy_entry_hash).current(),
        success: true,
        returnData: return_data,
    })
}

/// Build one deferred L1 entry representing a successful L1-to-L2 call.
///
/// Its rolling hash remains unfinished until settlement attaches at least one
/// state update and calls [`finalize_l1_rolling_hashes`].
#[must_use]
pub fn build_l1_inbound_entry(
    target: Address,
    value: U256,
    data: Bytes,
    source: Address,
    destination_rollup_id: RollupId,
    return_data: Bytes,
) -> EvmBatch {
    let proxy_entry_hash = common_cross_chain_call_hash(CallHashInput {
        call_mode: CallMode::Mutable,
        source_address: source,
        source_rollup_id: RollupId::MAINNET,
        target_address: target,
        target_rollup_id: destination_rollup_id,
        value,
        data: &data,
    });

    batch_with_entries(
        vec![ExecutionEntrySol {
            stateUpdates: Vec::new(),
            proxyEntryHash: proxy_entry_hash,
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: B256::ZERO,
            destinationRollupId: destination_rollup_id.0,
            success: true,
            returnData: return_data,
        }],
        0,
    )
}

/// Build the target-L2 sidecar used to reconstruct incoming system calls.
///
/// Although represented by the shared batch type, each entry carries the L2
/// proxy hash, incoming-call descriptor, and rolling hash needed to reconstruct
/// the canonical L2 system transaction.
pub(crate) fn build_l1_inbound_sidecar(
    calls: &[ExecutedAction],
    target_rollup_id: RollupId,
) -> ProtocolResult<EvmBatch> {
    let mut entries = Vec::with_capacity(calls.len());
    for call in calls {
        let return_data = supported_return_data(call)?;
        if call.target_rollup_id != target_rollup_id || call.source_rollup_id == target_rollup_id {
            return Err(crate::ProtocolErrorKind::Unsupported(
                "inbound sidecars only support top-level calls from another rollup",
            )
            .into());
        }

        let call_hash = common_cross_chain_call_hash(CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: call.source_address,
            source_rollup_id: call.source_rollup_id,
            target_address: call.target_address,
            target_rollup_id,
            value: call.value,
            data: &call.data,
        });
        let mut rolling_hash = EntryRollingHash::seed_for_l2(call_hash);
        rolling_hash.call_begin(call_hash);
        rolling_hash.call_end(true, return_data);

        entries.push(ExecutionEntrySol {
            stateUpdates: Vec::new(),
            proxyEntryHash: call_hash,
            l2ToL1Calls: vec![l1_call_from_action(call)],
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: rolling_hash.current(),
            destinationRollupId: target_rollup_id.0,
            success: true,
            returnData: Bytes::copy_from_slice(return_data),
        });
    }

    Ok(batch_with_entries(entries, 0))
}

/// Build one immediate, call-free L1 settlement entry.
///
/// Its rolling hash is finalized after the settlement state update is
/// attached.
#[must_use]
pub fn build_l1_settlement_only(rollup_id: RollupId) -> EvmBatch {
    batch_with_entries(
        vec![ExecutionEntrySol {
            stateUpdates: Vec::new(),
            proxyEntryHash: B256::ZERO,
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: B256::ZERO,
            destinationRollupId: rollup_id.0,
            success: true,
            returnData: Bytes::new(),
        }],
        1,
    )
}

/// Encode one L2 incoming execution call.
#[must_use]
pub fn encode_execute_incoming(
    destination: Address,
    value: U256,
    data: Bytes,
    source: Address,
    source_rollup_id: RollupId,
    entry: L2ExecutionEntrySol,
) -> Vec<u8> {
    crate::abi::executeIncomingCrossChainCallCall {
        destination,
        value,
        data,
        sourceAddress: source,
        sourceRollup: source_rollup_id.0,
        _entries: vec![entry],
        _staticEntries: Vec::new(),
    }
    .abi_encode()
}

/// Selector for `executeIncomingCrossChainCall`.
pub const EXECUTE_INCOMING_SELECTOR: [u8; 4] =
    crate::abi::executeIncomingCrossChainCallCall::SELECTOR;

/// Decode the supported incoming call's return data.
#[must_use]
pub fn decode_inbound_return_data(calldata: &[u8]) -> Option<Bytes> {
    decode_inbound(calldata).map(|decoded| decoded.return_data)
}

/// Incoming call and outcome observed in sealed L2 calldata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedInbound {
    /// Destination contract on the L2.
    pub target: Address,
    /// Native value forwarded with the call.
    pub value: U256,
    /// Calldata forwarded to the destination.
    pub data: Bytes,
    /// Original source-chain caller.
    pub source: Address,
    /// Precomputed return data committed by the entry.
    pub return_data: Bytes,
    /// Validated entry outcome. Decoding rejects unsuccessful entries, so this
    /// is always `true`.
    pub success: bool,
}

/// Decode and validate the supported single-call incoming entry shape.
///
/// The decoder reads `success` from the target ABI and includes it when
/// verifying the rolling hash.
#[must_use]
pub fn decode_inbound(calldata: &[u8]) -> Option<DecodedInbound> {
    let call = crate::abi::executeIncomingCrossChainCallCall::abi_decode(calldata).ok()?;
    if call._entries.len() != 1 || !call._staticEntries.is_empty() {
        return None;
    }
    let entry = call._entries.into_iter().next()?;
    if !entry.success
        || entry.proxyEntryHash == B256::ZERO
        || entry.incomingCalls.len() != 1
        || !entry.expectedOutgoingCalls.is_empty()
    {
        return None;
    }
    let incoming = entry.incomingCalls.first()?;
    if incoming.revertNextNCalls != 0
        || incoming.isStatic
        || incoming.gas != 0
        || incoming.sourceAddress != call.sourceAddress
        || incoming.sourceRollupId != call.sourceRollup
        || incoming.targetAddress != call.destination
        || incoming.value != call.value
        || incoming.data != call.data
    {
        return None;
    }

    let mut rolling_hash = EntryRollingHash::seed_for_l2(entry.proxyEntryHash);
    rolling_hash.call_begin(entry.proxyEntryHash);
    rolling_hash.call_end(entry.success, &entry.returnData);
    if rolling_hash.current() != entry.rollingHash {
        return None;
    }

    Some(DecodedInbound {
        target: call.destination,
        value: call.value,
        data: call.data,
        source: call.sourceAddress,
        return_data: entry.returnData,
        success: entry.success,
    })
}

/// Encode a batch as `EEZ.postAndVerifyBatch` calldata.
#[must_use]
pub fn encode_postbatch(batch: &EvmBatch) -> Vec<u8> {
    postAndVerifyBatchCall {
        batch: batch.clone(),
    }
    .abi_encode()
}

/// Decode `EEZ.postAndVerifyBatch` calldata.
pub fn decode_postbatch(calldata: &[u8]) -> alloy_sol_types::Result<EvmBatch> {
    Ok(postAndVerifyBatchCall::abi_decode(calldata)?.batch)
}

fn supported_return_data(call: &ExecutedAction) -> ProtocolResult<&[u8]> {
    if call.outcome.is_pending() {
        return Err(crate::ProtocolErrorKind::InvalidEncoding(
            "recorded cross-chain call still has a pending outcome".to_owned(),
        )
        .into());
    }
    if call.call_mode == CallMode::Static {
        return Err(crate::ProtocolErrorKind::Unsupported(STATIC_CALL).into());
    }
    if call.revert_span.is_some() {
        return Err(crate::ProtocolErrorKind::Unsupported(REVERT_SPAN).into());
    }
    match &call.outcome {
        crate::ExecutionOutcome::Resolved {
            return_data,
            success: true,
            ..
        } => Ok(return_data),
        crate::ExecutionOutcome::Resolved { success: false, .. } => {
            Err(crate::ProtocolErrorKind::Unsupported(UNSUCCESSFUL_CALL).into())
        }
        crate::ExecutionOutcome::Pending => unreachable!("pending outcome rejected above"),
    }
}

fn supported_top_level_return_data(
    call: &ExecutedAction,
    source_rollup_id: RollupId,
) -> ProtocolResult<&[u8]> {
    let return_data = supported_return_data(call)?;
    if call.source_rollup_id != source_rollup_id {
        return Err(crate::ProtocolErrorKind::Unsupported(NESTED_CALL).into());
    }
    Ok(return_data)
}

fn ensure_supported_flat_call(call: &L2ToL1CallSol) -> ProtocolResult<()> {
    if call.revertNextNCalls != 0 {
        return Err(crate::ProtocolErrorKind::Unsupported(REVERT_SPAN).into());
    }
    if call.isStatic {
        return Err(crate::ProtocolErrorKind::Unsupported(STATIC_CALL).into());
    }
    if call.gas != 0 {
        return Err(crate::ProtocolErrorKind::Unsupported(
            "explicit cross-chain call gas limits are not supported",
        )
        .into());
    }
    Ok(())
}

fn l1_call_from_action(call: &ExecutedAction) -> L2ToL1CallSol {
    L2ToL1CallSol {
        revertNextNCalls: 0,
        isStatic: false,
        gas: 0,
        sourceAddress: call.source_address,
        sourceRollupId: call.source_rollup_id.0,
        targetAddress: call.target_address,
        value: call.value,
        data: call.data.clone(),
    }
}

fn source_side_call_hash(call: &ExecutedAction) -> B256 {
    common_cross_chain_call_hash(CallHashInput {
        call_mode: call.call_mode,
        source_address: call.source_address,
        source_rollup_id: call.source_rollup_id,
        target_address: call.target_address,
        target_rollup_id: call.target_rollup_id,
        value: call.value,
        data: &call.data,
    })
}

fn batch_with_entries(entries: Vec<ExecutionEntrySol>, immediate_count: usize) -> EvmBatch {
    EvmBatch {
        entries,
        immediateEntryCount: U256::from(immediate_count),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{I256, address};

    use super::*;
    use crate::ExecutionOutcome;
    use crate::abi::StateUpdateSol;

    fn record(target: RollupId, source: RollupId) -> ExecutedAction {
        ExecutedAction {
            call_mode: CallMode::Mutable,
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: target,
            source_rollup_id: source,
            source_address: address!("00000000000000000000000000000000000000bb"),
            data: Bytes::from_static(&[0x12, 0x34]),
            value: U256::ZERO,
            outcome: ExecutionOutcome::Resolved {
                return_data: vec![0xab],
                gas_used: 21_000,
                success: true,
            },
            revert_span: None,
        }
    }

    fn state_update(rollup_id: u64, current_state: B256) -> StateUpdateSol {
        StateUpdateSol {
            rollupId: rollup_id,
            currentState: current_state,
            newState: B256::with_last_byte(0xff),
            etherDelta: I256::ZERO,
        }
    }

    #[test]
    fn unsupported_recorded_shapes_fail_closed() {
        let mut pending = record(RollupId(1), RollupId::MAINNET);
        pending.outcome = ExecutionOutcome::Pending;
        assert!(ensure_materializable_calls(&[pending]).is_err());

        let mut static_call = record(RollupId(1), RollupId::MAINNET);
        static_call.call_mode = CallMode::Static;
        assert!(ensure_materializable_calls(&[static_call]).is_err());

        let mut failed = record(RollupId(1), RollupId::MAINNET);
        let ExecutionOutcome::Resolved { success, .. } = &mut failed.outcome else {
            unreachable!()
        };
        *success = false;
        assert!(ensure_materializable_calls(&[failed]).is_err());

        let mut reverted_span = record(RollupId(1), RollupId::MAINNET);
        reverted_span.revert_span = Some(0);
        assert!(ensure_materializable_calls(&[reverted_span]).is_err());
    }

    #[test]
    fn build_batch_rejects_nested_calls() {
        let calls = [
            record(RollupId(1), RollupId::MAINNET),
            record(RollupId::MAINNET, RollupId(1)),
        ];

        assert!(build_batch(&calls, RollupId::MAINNET).is_err());
    }

    #[test]
    fn source_l2_entry_is_seed_only_and_uses_gas_aware_key() {
        let action = record(RollupId::MAINNET, RollupId(7));
        let batch = build_batch(std::slice::from_ref(&action), RollupId(7)).unwrap();
        let entry = &batch.entries[0];
        let expected_key = l2_outbound_call_hash(
            CallHashInput {
                call_mode: CallMode::Mutable,
                source_address: action.source_address,
                source_rollup_id: action.source_rollup_id,
                target_address: action.target_address,
                target_rollup_id: action.target_rollup_id,
                value: action.value,
                data: &action.data,
            },
            0,
        );

        assert_eq!(entry.proxyEntryHash, expected_key);
        assert!(entry.l2ToL1Calls.is_empty());
        assert_eq!(
            entry.rollingHash,
            EntryRollingHash::seed_for_l2(expected_key).current()
        );
        assert!(entry.success);
    }

    #[test]
    fn l1_postbatch_is_unfinished_until_state_updates_are_attached() {
        let action = record(RollupId::MAINNET, RollupId(7));
        let batch = build_l1_postbatch(&[action], RollupId(7)).unwrap();
        let entry = &batch.entries[0];

        assert_eq!(batch.immediateEntryCount, U256::from(1));
        assert!(entry.stateUpdates.is_empty());
        assert_eq!(entry.rollingHash, B256::ZERO);
        assert_eq!(entry.destinationRollupId, 7);
        assert!(entry.success);
        let call = &entry.l2ToL1Calls[0];
        assert_eq!(call.revertNextNCalls, 0);
        assert!(!call.isStatic);
        assert_eq!(call.gas, 0);
        assert_eq!(call.sourceRollupId, 7);
    }

    #[test]
    fn l1_finalizer_folds_state_seed_and_flat_call() {
        let action = record(RollupId::MAINNET, RollupId(7));
        let mut batch = build_l1_postbatch(&[action], RollupId(7)).unwrap();
        batch.entries[0]
            .stateUpdates
            .push(state_update(7, B256::with_last_byte(0x11)));

        finalize_l1_rolling_hashes(&mut batch).unwrap();

        let entry = &batch.entries[0];
        let call = &entry.l2ToL1Calls[0];
        let call_hash = common_cross_chain_call_hash(CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: call.sourceAddress,
            source_rollup_id: RollupId(call.sourceRollupId),
            target_address: call.targetAddress,
            target_rollup_id: RollupId::MAINNET,
            value: call.value,
            data: &call.data,
        });
        let mut expected =
            EntryRollingHash::seed_for_l1([(7, B256::with_last_byte(0x11))], B256::ZERO);
        expected.call_begin(call_hash);
        expected.call_end(true, &entry.returnData);
        assert_eq!(entry.rollingHash, expected.current());
    }

    #[test]
    fn l1_finalizer_rejects_incomplete_or_complex_entries() {
        let action = record(RollupId::MAINNET, RollupId(7));
        let mut missing_updates =
            build_l1_postbatch(std::slice::from_ref(&action), RollupId(7)).unwrap();
        assert!(finalize_l1_rolling_hashes(&mut missing_updates).is_err());

        let mut multi_call = build_l1_postbatch(&[action], RollupId(7)).unwrap();
        multi_call.entries[0]
            .stateUpdates
            .push(state_update(7, B256::ZERO));
        let extra_call = multi_call.entries[0].l2ToL1Calls[0].clone();
        multi_call.entries[0].l2ToL1Calls.push(extra_call);
        assert!(finalize_l1_rolling_hashes(&mut multi_call).is_err());
    }

    #[test]
    fn outbound_ether_uses_explicit_success_and_rejects_multicall() {
        let call = L2ToL1CallSol {
            revertNextNCalls: 0,
            isStatic: false,
            gas: 0,
            sourceAddress: Address::ZERO,
            sourceRollupId: 7,
            targetAddress: Address::ZERO,
            value: U256::from(9),
            data: Bytes::new(),
        };
        let mut entry = ExecutionEntrySol {
            l2ToL1Calls: vec![call.clone()],
            success: true,
            ..Default::default()
        };
        assert_eq!(outbound_ether_out(&entry), Some(U256::from(9)));

        entry.success = false;
        assert_eq!(outbound_ether_out(&entry), Some(U256::ZERO));

        entry.l2ToL1Calls.push(call);
        assert_eq!(outbound_ether_out(&entry), None);
    }

    #[test]
    fn l2_incoming_entry_has_exact_seed_and_flat_fold() {
        let target = address!("00000000000000000000000000000000000000aa");
        let source = address!("00000000000000000000000000000000000000bb");
        let data = Bytes::from_static(&[1, 2, 3]);
        let return_data = Bytes::from_static(&[4, 5]);
        let entry = build_l2_incoming_entry(IncomingEntry {
            target,
            source,
            value: U256::from(6),
            data: data.clone(),
            source_rollup_id: RollupId::MAINNET,
            l2_rollup_id: RollupId(7),
            return_data: return_data.clone(),
            success: true,
        })
        .unwrap();

        let mut expected = EntryRollingHash::seed_for_l2(entry.proxyEntryHash);
        expected.call_begin(entry.proxyEntryHash);
        expected.call_end(true, &return_data);
        assert_eq!(entry.rollingHash, expected.current());
        assert!(entry.success);
        assert_eq!(entry.incomingCalls.len(), 1);
        let call = &entry.incomingCalls[0];
        assert_eq!(call.revertNextNCalls, 0);
        assert!(!call.isStatic);
        assert_eq!(call.gas, 0);
        assert_eq!(call.sourceRollupId, 0);
    }

    #[test]
    fn l2_builders_reject_unsuccessful_outcomes() {
        let incoming = build_l2_incoming_entry(IncomingEntry {
            target: Address::ZERO,
            source: Address::ZERO,
            value: U256::ZERO,
            data: Bytes::new(),
            source_rollup_id: RollupId::MAINNET,
            l2_rollup_id: RollupId(1),
            return_data: Bytes::new(),
            success: false,
        });
        assert!(incoming.is_err());

        let outbound = build_l2_outbound_entry(OutboundEntry {
            target: Address::ZERO,
            source: Address::ZERO,
            value: U256::ZERO,
            data: Bytes::new(),
            l2_rollup_id: RollupId(1),
            return_data: Bytes::new(),
            success: false,
        });
        assert!(outbound.is_err());
    }

    #[test]
    fn source_l2_outbound_entry_is_seed_only() {
        let entry = build_l2_outbound_entry(OutboundEntry {
            target: address!("00000000000000000000000000000000000000aa"),
            source: address!("00000000000000000000000000000000000000bb"),
            value: U256::from(1),
            data: Bytes::from_static(&[2]),
            l2_rollup_id: RollupId(7),
            return_data: Bytes::from_static(&[3]),
            success: true,
        })
        .unwrap();

        assert!(entry.incomingCalls.is_empty());
        assert_eq!(
            entry.rollingHash,
            EntryRollingHash::seed_for_l2(entry.proxyEntryHash).current()
        );
        assert!(entry.success);
    }

    #[test]
    fn incoming_calldata_round_trips_and_uses_explicit_success() {
        let target = address!("00000000000000000000000000000000000000aa");
        let source = address!("00000000000000000000000000000000000000bb");
        let data = Bytes::from_static(&[1, 2]);
        let entry = build_l2_incoming_entry(IncomingEntry {
            target,
            source,
            value: U256::ZERO,
            data: data.clone(),
            source_rollup_id: RollupId::MAINNET,
            l2_rollup_id: RollupId(7),
            return_data: Bytes::from_static(&[3]),
            success: true,
        })
        .unwrap();
        let calldata = encode_execute_incoming(
            target,
            U256::ZERO,
            data.clone(),
            source,
            RollupId::MAINNET,
            entry.clone(),
        );

        assert_eq!(
            decode_inbound(&calldata),
            Some(DecodedInbound {
                target,
                value: U256::ZERO,
                data,
                source,
                return_data: Bytes::from_static(&[3]),
                success: true,
            })
        );

        let mut failed = entry;
        failed.success = false;
        let failed_calldata = encode_execute_incoming(
            target,
            U256::ZERO,
            Bytes::from_static(&[1, 2]),
            source,
            RollupId::MAINNET,
            failed,
        );
        assert_eq!(decode_inbound(&failed_calldata), None);
    }

    #[test]
    fn postbatch_round_trip_uses_pinned_selector() {
        let batch = EvmBatch::default();
        let encoded = encode_postbatch(&batch);
        assert_eq!(&encoded[..4], &postAndVerifyBatchCall::SELECTOR);
        assert_eq!(decode_postbatch(&encoded).unwrap().entries.len(), 0);
    }
}
