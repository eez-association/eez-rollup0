//! [`SessionInspector`] detects cross-chain proxy calls during EVM execution
//! and dispatches each one through a borrowed [`CompositionBuilder`].
//!
//! When the inspector sees a CALL:
//! 1. `lookup_authorized_proxy_live` reads `authorizedProxies[addr]` from the
//!    **live EVM state**, so proxies registered by earlier calls in the same
//!    transaction, or by earlier transactions in the same in-progress block,
//!    are visible.
//! 2. If the address is a registered proxy,
//!    [`CompositionBuilder::dispatch_call`] forwards the call to the target
//!    session and returns the execution response.
//! 3. The inspector answers the call with that response so the source
//!    transaction's control flow continues.
//!
//! Errors surfaced by dispatch are stashed locally so the caller can consult
//! them after the EVM pass without unwinding the EVM.
//!
//! # Why live EVM state
//!
//! The lookup must see writes made earlier in the same transaction (e.g.
//! `Bridge.bridgeEther` deploys a CREATE2 proxy and calls it inside one tx)
//! and earlier in the same in-progress block (multi-tx composition). Reading
//! through the transaction overlay captures both; a pre-tx storage snapshot
//! would not.

use alloy_primitives::{Address, Bytes};
use eez_evm::EezEvmTypes;
use evm2::{
    DatabaseError, Inspector, InspectorConfig,
    evm::{Cache, DbResult, State},
    interpreter::{
        GasTracker, InstrStop, Interpreter, Message, MessageKind, MessageResult, MessageResultExt,
        Word,
    },
};

use crate::overlay::OverlayChannelHandle;
use eez_protocol::{CallMode, CompositionBuilder, ExecutorError, ProxyLookupConfig, RollupId};
use eez_protocol::{ProxyInfo, decode_proxy_value, proxy_mapping_key};

/// Look up `authorizedProxies[addr]` in the live EVM state.
///
/// The read is untracked: it sees the transaction's writes but neither loads
/// nor warms the slot, so the inspected transaction's gas is unchanged.
/// Returns `None` for unregistered proxies and for DB errors; the caller
/// treats both as "not a proxy."
fn lookup_authorized_proxy_live(
    state: &mut State<'_>,
    lookup: &ProxyLookupConfig,
    addr: Address,
) -> Option<ProxyInfo> {
    let key = proxy_mapping_key(addr, lookup.authorized_proxies_slot);
    match state.storage_slot_untracked(&lookup.contract_address, &key.into()) {
        Ok(value) => decode_proxy_value(value),
        Err(error) => {
            tracing::warn!(
                contract = %lookup.contract_address,
                proxy = %addr,
                %error,
                "proxy lookup: DB error reading authorizedProxies slot; treating as not-a-proxy",
            );
            None
        }
    }
}

/// This rollup's live state as a cache: the accepted state plus the current
/// transaction's loaded accounts and slots at their in-flight values.
fn live_snapshot(state: &mut State<'_>) -> DbResult<Cache> {
    let mut cache = state.overlay_db().cache.clone();
    // Collect first: the reads below borrow `state` mutably.
    let loaded: Vec<(Address, Vec<Word>)> = state
        .loaded_accounts()
        .map(|(address, slots)| (*address, slots.copied().collect()))
        .collect();
    for (address, slots) in loaded {
        let info = state.account_info_untracked(&address)?;
        if let Some(info) = &info
            && !cache.contracts.contains_key(&info.code_hash)
        {
            let code = match &info.code {
                Some(code) => code.clone(),
                None => state.code_untracked(&address)?,
            };
            cache.contracts.insert(info.code_hash, code);
        }
        cache.accounts.insert(address, info);
        let storage = cache.storage.entry(address).or_default();
        for key in slots {
            storage
                .slots
                .insert(key, state.storage_slot_untracked(&address, &key)?);
        }
    }
    Ok(cache)
}

/// EVM inspector that detects proxy calls and dispatches them through
/// the borrowed [`CompositionBuilder`].
///
/// After the EVM pass, the caller consults `take_error()` to surface
/// any dispatch failure; the dispatcher already holds the recorded calls.
///
/// # Frame bracketing
///
/// The inspector brackets every EVM CALL frame with a snapshot of the
/// dispatcher's recorded-call count. On `call_end`, the popped value
/// pairs with the current count to compute `(start, span)` for any
/// reverted frame, and the bracket is forwarded to
/// [`CompositionBuilder::annotate_revert_span`](eez_protocol::CompositionBuilder::annotate_revert_span).
/// A frame whose answered call the EVM discards is treated the same way.
/// Calls are recorded in preorder because each slot is opened before its
/// session executes. Therefore, `span = end - start` covers the reverted
/// frame's recorded calls. Batch materialization rejects such spans.
pub struct SessionInspector<'a> {
    /// Combined proxy-lookup configuration: the contract address to
    /// read and which slot to read from.
    proxy_lookup: ProxyLookupConfig,
    /// Composition builder that routes and records detected calls.
    dispatcher: &'a mut CompositionBuilder,
    /// Rollup ID of the chain this inspector is running on. Written to each
    /// [`eez_protocol::ExecutionRequest`] as the source identity used for
    /// target execution, recording, and call hashing.
    caller_rollup_id: RollupId,
    /// First target execution error, if any.
    error: Option<ExecutorError>,
    /// Per-rollup cache channel for nested re-entry.
    overlay_channel: OverlayChannelHandle,
    /// One entry per open EVM CALL frame, pushed in `Inspector::call` and
    /// popped in `Inspector::call_end`.
    frames: Vec<Frame>,
}

/// An open CALL frame.
#[derive(Debug, Clone, Copy)]
struct Frame {
    /// The dispatcher's recorded-call count when the frame opened. With the
    /// count at `call_end` it brackets the calls dispatched inside the frame.
    start: usize,
    /// The stop the inspector answered the frame with, if it dispatched it.
    answered: Option<InstrStop>,
}

impl Frame {
    /// Whether the frame's dispatched calls never happen: it reverted, or the
    /// EVM discarded the inspector's answer. The latter is a top-level frame
    /// that runs out of gas before running.
    fn voided(self, stop: InstrStop) -> bool {
        stop == InstrStop::Revert || self.answered.is_some_and(|answered| answered != stop)
    }
}

impl std::fmt::Debug for SessionInspector<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionInspector")
            .field("proxy_lookup", &self.proxy_lookup)
            .field("caller_rollup_id", &self.caller_rollup_id)
            .field("has_error", &self.error.is_some())
            .finish_non_exhaustive()
    }
}

/// Shared construction surface for [`SessionInspector`] instances.
///
/// Holds the per-chain proxy lookup, caller rollup id, and overlay channel
/// shared by source and target-session inspectors.
///
/// Each call site produces an inspector via [`build`](Self::build).
#[derive(Debug, Clone)]
pub struct SessionInspectorFactory {
    /// Contract and storage slot for this chain's `authorizedProxies` map.
    proxy_lookup: ProxyLookupConfig,
    /// Source rollup ID written to every request dispatched by inspectors
    /// built from this factory.
    caller_rollup_id: RollupId,
    /// Per-rollup channel for propagating state through recursive re-entry.
    /// Source and target-session inspectors use the channel belonging to the
    /// rollup whose EVM state they inspect.
    overlay_channel: OverlayChannelHandle,
}

impl SessionInspectorFactory {
    /// Create a factory pinned to one chain's configuration.
    #[must_use]
    pub fn new(
        proxy_lookup: ProxyLookupConfig,
        caller_rollup_id: RollupId,
        overlay_channel: OverlayChannelHandle,
    ) -> Self {
        Self {
            proxy_lookup,
            caller_rollup_id,
            overlay_channel,
        }
    }

    /// Build an inspector for source simulation or a target session.
    ///
    /// Calls are opened before target execution, so nested dispatches are
    /// recorded as preorder children of their outer call.
    pub fn build<'a>(&self, dispatcher: &'a mut CompositionBuilder) -> SessionInspector<'a> {
        SessionInspector {
            proxy_lookup: self.proxy_lookup.clone(),
            dispatcher,
            caller_rollup_id: self.caller_rollup_id,
            error: None,
            overlay_channel: self.overlay_channel.clone(),
            frames: Vec::new(),
        }
    }
}

impl SessionInspector<'_> {
    /// Take the recorded error, if any.
    pub fn take_error(&mut self) -> Option<ExecutorError> {
        self.error.take()
    }

    fn has_error(&self) -> bool {
        self.error.is_some()
    }

    fn record_error(&mut self, error: ExecutorError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    /// Answer the open frame's call with `output`, returning all of its gas to
    /// the caller.
    fn answer(
        &mut self,
        message: &Message<EezEvmTypes>,
        stop: InstrStop,
        output: Bytes,
    ) -> MessageResult<EezEvmTypes> {
        if let Some(frame) = self.frames.last_mut() {
            frame.answered = Some(stop);
        }
        MessageResultExt {
            stop,
            gas: GasTracker::new_with_execution_gas_and_reservoir(
                message.gas_limit,
                message.reservoir,
            ),
            output,
            ..Default::default()
        }
    }

    /// Dispatch a proxy call. A downstream session that re-enters this rollup
    /// opens over the live snapshot pushed here and publishes its writes, which
    /// are replayed on `state` before the source frame continues.
    fn dispatch(
        &mut self,
        state: &mut State<'_>,
        target_rollup: RollupId,
        req: eez_protocol::ExecutionRequest,
    ) -> Result<eez_protocol::ExecutionOutcome, ExecutorError> {
        let snapshot = live_snapshot(state).map_err(provider_error)?;
        let mark = self.overlay_channel.writes_len();
        self.overlay_channel.push_pre_snapshot(snapshot);
        let outcome = self.dispatcher.dispatch_call(target_rollup, req);
        self.overlay_channel.pop_pre_snapshot();
        if let Some(writes) = self.overlay_channel.take_writes_since(mark) {
            crate::overlay::apply_overlay_writes(state, &writes).map_err(|e| {
                ExecutorError::Evm(format!("overlay write-apply failed: {e}").into())
            })?;
        }
        outcome
    }
}

fn provider_error(error: DatabaseError) -> ExecutorError {
    ExecutorError::Provider(Box::new(error))
}

impl Inspector<EezEvmTypes> for SessionInspector<'_> {
    fn inspector_config(&self) -> InspectorConfig {
        InspectorConfig::without_steps()
    }

    fn call(
        &mut self,
        interp: &mut Interpreter<'_, '_, EezEvmTypes>,
        message: &mut Message<EezEvmTypes>,
    ) -> Option<MessageResult<EezEvmTypes>> {
        // Bracket every CALL frame with a recorded-count snapshot so
        // `call_end` can detect "frame reverted AFTER dispatching one
        // or more cross-chain calls" (revert-continue patterns —
        // see `Frame`).
        self.frames.push(Frame {
            start: self.dispatcher.recorded_count(),
            answered: None,
        });

        // Only mutable CALLs can represent cross-chain proxy calls.
        // Leave all other call shapes to the EVM.
        if self.has_error() || message.is_static() || message.kind != MessageKind::Call {
            return None;
        }

        let calldata = message.input.to_bytes(interp.memory());
        let calldata_len = calldata.len();
        let state = interp.host().state_mut();

        tracing::trace!(
            depth = message.depth,
            target_addr = %message.destination,
            caller = %message.caller,
            calldata_len,
            "inspector: CALL"
        );

        let Some(info) =
            lookup_authorized_proxy_live(state, &self.proxy_lookup, message.destination)
        else {
            tracing::trace!(addr = %message.destination, "inspector: not a proxy");
            return None;
        };

        let req = eez_protocol::ExecutionRequest {
            call_mode: CallMode::Mutable,
            target_address: info.original_address,
            data: calldata,
            value: message.value,
            source_address: message.caller,
            source_rollup_id: self.caller_rollup_id,
        };
        let sim = match self.dispatch(state, info.original_rollup_id, req) {
            Ok(response) => response,
            Err(e) => {
                // Expected outcome; `record_error` still carries it up.
                tracing::warn!(error = %e, "target execution failed");
                self.record_error(e);
                return Some(self.answer(message, InstrStop::Revert, Bytes::new()));
            }
        };

        let success = sim.is_success();
        tracing::info!(
            dest = %info.original_address,
            rollup_id = %info.original_rollup_id,
            caller = %message.caller,
            proxy = %message.destination,
            depth = message.depth,
            calldata_len,
            value = %message.value,
            target_result = if success { "ok" } else { "REVERT" },
            target_gas = sim.gas_used().unwrap_or(0),
            "cross-chain proxy call detected"
        );

        let return_data: Bytes = sim
            .return_data()
            .map(<[u8]>::to_vec)
            .unwrap_or_default()
            .into();

        // Surface a failed cross-chain sim as `Revert` to the surrounding
        // EVM — the outer Solidity frame should see the call as reverted
        // so try/catch + reverted-frame accounting bracket the right range.
        let stop = if success {
            InstrStop::Return
        } else {
            InstrStop::Revert
        };
        Some(self.answer(message, stop, return_data))
    }

    fn call_end(
        &mut self,
        _interp: &mut Interpreter<'_, '_, EezEvmTypes>,
        _message: &Message<EezEvmTypes>,
        result: &mut MessageResult<EezEvmTypes>,
    ) {
        // Pair with the count captured in `call`. A reverted frame with a
        // non-empty dispatch range records the span and queues the affected
        // session checkpoints for rollback before the next dispatch.
        //
        // Skip when an executor error already aborts composition, no calls
        // were dispatched in the frame, or the frame was not voided.
        let Some(frame) = self.frames.pop() else {
            return;
        };
        let end = self.dispatcher.recorded_count();
        if !self.has_error() && frame.start < end && frame.voided(result.stop) {
            let span = u32::try_from(end - frame.start).unwrap_or(u32::MAX);
            self.dispatcher.annotate_revert_span(frame.start, span);
        }
    }
}

#[cfg(test)]
mod tests {
    //! [`lookup_authorized_proxy_live`] reads the live state (transaction
    //! overlay, then the database), not a pre-tx snapshot.

    use super::*;
    use alloy_primitives::{U256, address};
    use eez_protocol::{EEZ_AUTHORIZED_PROXIES_SLOT, EEZL2_AUTHORIZED_PROXIES_SLOT};
    use evm2::evm::InMemoryDB;

    const CONTRACT: Address = address!("0x1111111111111111111111111111111111111111");
    const PROXY_ADDR: Address = address!("0x2222222222222222222222222222222222222222");
    const DESTINATION_ADDR: Address = address!("0x3333333333333333333333333333333333333333");
    const TARGET_ROLLUP: u64 = 42;

    fn lookup(authorized_proxies_slot: u8) -> ProxyLookupConfig {
        ProxyLookupConfig {
            contract_address: CONTRACT,
            authorized_proxies_slot,
        }
    }

    fn packed_proxy_value() -> U256 {
        let mut word = [0u8; 32];
        word[3..11].copy_from_slice(&TARGET_ROLLUP.to_be_bytes());
        word[11..31].copy_from_slice(DESTINATION_ADDR.as_ref());
        word[31] = 1;
        U256::from_be_bytes(word)
    }

    /// A state whose database registers `PROXY_ADDR` under `slot`.
    fn registered_in_db(slot: u8) -> State<'static> {
        let mut db = InMemoryDB::default();
        db.insert_account_storage(
            &CONTRACT,
            &proxy_mapping_key(PROXY_ADDR, slot).into(),
            &packed_proxy_value(),
        );
        State::new(db)
    }

    fn assert_registered(info: Option<ProxyInfo>) {
        let info = info.expect("proxy is registered");
        assert_eq!(info.original_address, DESTINATION_ADDR);
        assert_eq!(info.original_rollup_id, RollupId(TARGET_ROLLUP));
    }

    #[test]
    fn unregistered_address_returns_none() {
        let mut state = State::new(InMemoryDB::default());
        let lookup = lookup(EEZ_AUTHORIZED_PROXIES_SLOT);
        assert!(lookup_authorized_proxy_live(&mut state, &lookup, PROXY_ADDR).is_none());
    }

    #[test]
    fn both_dialect_slots_read_the_database() {
        for slot in [EEZ_AUTHORIZED_PROXIES_SLOT, EEZL2_AUTHORIZED_PROXIES_SLOT] {
            let mut state = registered_in_db(slot);
            assert_registered(lookup_authorized_proxy_live(
                &mut state,
                &lookup(slot),
                PROXY_ADDR,
            ));
        }
    }

    #[test]
    fn in_transaction_write_is_visible() {
        let mut state = State::new(InMemoryDB::default());
        let key = proxy_mapping_key(PROXY_ADDR, EEZ_AUTHORIZED_PROXIES_SLOT);
        state
            .storage_slot(&CONTRACT, key.into())
            .expect("load slot")
            .set(packed_proxy_value());
        let lookup = lookup(EEZ_AUTHORIZED_PROXIES_SLOT);
        assert_registered(lookup_authorized_proxy_live(
            &mut state, &lookup, PROXY_ADDR,
        ));
    }

    #[test]
    fn wrong_slot_returns_none() {
        let wrong_slot = 7;
        assert_ne!(EEZ_AUTHORIZED_PROXIES_SLOT, wrong_slot);
        let mut state = registered_in_db(EEZ_AUTHORIZED_PROXIES_SLOT);
        assert!(
            lookup_authorized_proxy_live(&mut state, &lookup(wrong_slot), PROXY_ADDR).is_none(),
            "reading the wrong slot must miss, not decode garbage"
        );
    }
}
