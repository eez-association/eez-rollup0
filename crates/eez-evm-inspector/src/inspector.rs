//! [`SessionInspector`] detects cross-chain proxy calls during EVM execution
//! and dispatches each one through a borrowed [`CompositionBuilder`].
//!
//! When the inspector sees a CALL opcode:
//! 1. `lookup_authorized_proxy_live` reads `authorizedProxies[addr]`
//!    from the **live EVM state** (revm journal + DB) — so proxies
//!    registered by earlier calls in the same transaction, or by
//!    earlier transactions in the same in-progress block, are visible.
//! 2. If the address is a registered proxy,
//!    [`CompositionBuilder::dispatch_call`] forwards the call to the target
//!    session and returns the execution response.
//! 3. The inspector synthesizes the outcome back into the source EVM so
//!    the source transaction's control flow can continue.
//!
//! Errors surfaced by dispatch are stashed locally so the caller can
//! consult them after the EVM pass without unwinding the revm machine.
//!
//! # Why live EVM state
//!
//! The lookup must see writes made earlier in the same transaction
//! (e.g. `Bridge.bridgeEther` deploys a CREATE2 proxy and calls it
//! inside one tx) and earlier in the same in-progress block
//! (multi-tx composition). Reading through revm's journal captures
//! both; a pre-tx storage snapshot would not.
//!
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, Bytes};
use revm::Inspector;
use revm::context_interface::{ContextTr, Host, JournalTr};
use revm::database::{CacheState, State};
use revm::interpreter::{
    CallInputs, CallOutcome, CallScheme, Gas, InstructionResult, InterpreterResult,
};

use eez_protocol::{CallMode, CompositionBuilder, ExecutorError, ProxyLookupConfig, RollupId};
use eez_protocol::{ProxyInfo, decode_proxy_value, proxy_mapping_key};

/// Cache exchange for nested dispatches that re-enter a suspended rollup.
///
/// # Why it exists
///
/// During nested dispatch, the caller's revm state remains mutably borrowed.
/// A re-entered session therefore cannot borrow that state directly. This
/// channel transfers cache snapshots across the dispatch boundary using two
/// LIFO stacks:
///
/// - `source_cache` stores this rollup's live cache before dispatch. A session
///   that re-enters the rollup peeks at the latest snapshot when it opens.
///
/// - `overlay_cache` stores the cache produced by a re-entered session. The
///   suspended inspector pops it after dispatch and applies the diff through
///   [`apply_overlay_diff`](crate::overlay::apply_overlay_diff).
///
/// Stacks keep recursive re-entry snapshots paired with their call frames.
#[derive(Debug, Default)]
pub struct OverlayChannel {
    /// Stack of pre-dispatch cache snapshots. See [`push_pre_snapshot`]
    /// / [`peek_pre_snapshot`] / [`pop_pre_snapshot`].
    ///
    /// [`push_pre_snapshot`]: OverlayChannel::push_pre_snapshot
    /// [`peek_pre_snapshot`]: OverlayChannel::peek_pre_snapshot
    /// [`pop_pre_snapshot`]: OverlayChannel::pop_pre_snapshot
    source_cache: Mutex<Vec<CacheState>>,
    /// Stack of post-execute cache snapshots. See [`push_post_cache`]
    /// / [`pop_post_cache`].
    ///
    /// [`push_post_cache`]: OverlayChannel::push_post_cache
    /// [`pop_post_cache`]: OverlayChannel::pop_post_cache
    overlay_cache: Mutex<Vec<CacheState>>,
}

impl OverlayChannel {
    /// Push a pre-dispatch cache snapshot for this rollup.
    pub fn push_pre_snapshot(&self, cache: CacheState) {
        if let Ok(mut guard) = self.source_cache.lock() {
            guard.push(cache);
        }
    }

    /// Pop the matching pre-dispatch snapshot at the end of the
    /// inspector frame.
    pub fn pop_pre_snapshot(&self) -> Option<CacheState> {
        self.source_cache.lock().ok().and_then(|mut g| g.pop())
    }

    /// Clone the latest pre-dispatch snapshot for a re-entered session.
    pub fn peek_pre_snapshot(&self) -> Option<CacheState> {
        self.source_cache
            .lock()
            .ok()
            .and_then(|g| g.last().cloned())
    }

    /// Push the cache produced by a re-entered session.
    pub fn push_post_cache(&self, cache: CacheState) {
        if let Ok(mut guard) = self.overlay_cache.lock() {
            guard.push(cache);
        }
    }

    /// Pop the cache produced by the latest re-entered session.
    pub fn pop_post_cache(&self) -> Option<CacheState> {
        self.overlay_cache.lock().ok().and_then(|mut g| g.pop())
    }
}

/// Shared handle to an [`OverlayChannel`].
pub type OverlayChannelHandle = Arc<OverlayChannel>;

/// Build a fresh, empty [`OverlayChannelHandle`].
#[must_use]
pub fn new_overlay_channel() -> OverlayChannelHandle {
    Arc::new(OverlayChannel::default())
}

/// Look up `authorizedProxies[addr]` from the live EVM state at the
/// hook site.
///
/// Goes through revm's journal (so writes by earlier calls or earlier
/// txs in the same in-progress block are visible) and falls back to the
/// underlying DB for cold slots. Returns `None` for unregistered proxies
/// and for DB errors — the caller treats both as "not a proxy."
///
/// # Account warming
///
/// revm's `sload` requires the target account to already be present in
/// the journal; it's the normal precondition when the EVM does an SLOAD
/// while executing a contract. The inspector fires on every CALL —
/// including calls to destinations that have never had their state
/// touched before — so we can't assume the rollups contract is warm.
/// We explicitly load it; a no-op if it was already warm.
fn lookup_authorized_proxy_live<CTX: ContextTr + Host>(
    ctx: &mut CTX,
    rollups_addr: Address,
    proxy_slot: u8,
    addr: Address,
) -> Option<ProxyInfo> {
    // Warm the rollups account before the SLOAD (see fn-level doc).
    ctx.journal_mut().load_account(rollups_addr).ok()?;
    let key = proxy_mapping_key(addr, proxy_slot);
    let load = ctx.sload(rollups_addr, key.into())?;
    decode_proxy_value(load.data)
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
    overlay_channel: Option<OverlayChannelHandle>,
    /// Per-EVM-frame snapshot of the dispatcher's recorded-call count
    /// at the entry of `Inspector::call`. On `Inspector::call_end`,
    /// the popped value pairs with the current count to bracket the
    /// range of [`eez_protocol::ExecutedAction`]s dispatched
    /// inside this frame. If the frame's outcome is
    /// `InstructionResult::Revert` AND the range is non-empty, the
    /// inspector forwards `(start, span)` to
    /// [`CompositionBuilder::annotate_revert_span`](eez_protocol::CompositionBuilder::annotate_revert_span).
    frame_starts: Vec<usize>,
}

impl std::fmt::Debug for SessionInspector<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionInspector")
            .field("proxy_lookup", &self.proxy_lookup)
            .field("caller_rollup_id", &self.caller_rollup_id)
            .field("has_error", &self.error.is_some())
            .field("has_overlay_channel", &self.overlay_channel.is_some())
            .finish_non_exhaustive()
    }
}

/// Shared construction surface for [`SessionInspector`] instances.
///
/// Holds the per-chain proxy lookup, caller rollup id, and optional overlay
/// channel shared by source and target-session inspectors.
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
    overlay_channel: Option<OverlayChannelHandle>,
}

impl SessionInspectorFactory {
    /// Create a factory pinned to one chain's configuration.
    ///
    /// The caller may install an overlay channel with
    /// [`with_overlay_channel`](Self::with_overlay_channel).
    #[must_use]
    pub fn new(proxy_lookup: ProxyLookupConfig, caller_rollup_id: RollupId) -> Self {
        Self {
            proxy_lookup,
            caller_rollup_id,
            overlay_channel: None,
        }
    }

    /// Install the overlay channel used by the shared-source-state
    /// overlay path.
    ///
    /// Source and target-session inspectors use the channel belonging to the
    /// rollup whose EVM state they inspect.
    #[must_use]
    pub fn with_overlay_channel(mut self, channel: OverlayChannelHandle) -> Self {
        self.overlay_channel = Some(channel);
        self
    }

    /// Configuration this factory was built with.
    #[must_use]
    pub fn proxy_lookup(&self) -> &ProxyLookupConfig {
        &self.proxy_lookup
    }

    /// Source rollup ID written into requests by every built inspector.
    #[must_use]
    pub fn caller_rollup_id(&self) -> RollupId {
        self.caller_rollup_id
    }

    /// Build an inspector for source simulation or a target session.
    ///
    /// Calls are opened before target execution, so nested dispatches are
    /// recorded as preorder children of their outer call.
    pub fn build<'a>(&self, dispatcher: &'a mut CompositionBuilder) -> SessionInspector<'a> {
        let mut insp =
            SessionInspector::new(self.proxy_lookup.clone(), dispatcher, self.caller_rollup_id);
        insp.overlay_channel = self.overlay_channel.clone();
        insp
    }
}

impl<'a> SessionInspector<'a> {
    /// Create an inspector that routes detected calls through `dispatcher`.
    ///
    /// - `proxy_lookup`: the (contract, slot) pair used to read
    ///   `authorizedProxies` from the chain-local manager.
    /// - `dispatcher`: dispatch surface for detected calls.
    /// - `caller_rollup_id`: source rollup ID stored in each dispatched request.
    pub fn new(
        proxy_lookup: ProxyLookupConfig,
        dispatcher: &'a mut CompositionBuilder,
        caller_rollup_id: RollupId,
    ) -> Self {
        Self {
            proxy_lookup,
            dispatcher,
            caller_rollup_id,
            error: None,
            overlay_channel: None,
            frame_starts: Vec::new(),
        }
    }

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
}

impl<'db, DB, CTX> Inspector<CTX> for SessionInspector<'_>
where
    CTX: ContextTr<Db = &'db mut State<DB>> + Host,
    DB: 'db,
{
    fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        // Bracket every CALL frame with a recorded-count snapshot so
        // `call_end` can detect "frame reverted AFTER dispatching one
        // or more cross-chain calls" (revert-continue patterns —
        // see field doc).
        self.frame_starts.push(self.dispatcher.recorded_count());

        if self.has_error() {
            return None;
        }

        // Only mutable CALLs can represent cross-chain proxy calls.
        // Leave all other call shapes to the EVM.
        if inputs.is_static || !matches!(inputs.scheme, CallScheme::Call) {
            return None;
        }

        let calldata = inputs.input.bytes(context);

        tracing::trace!(
            depth = context.journal_ref().depth(),
            target_addr = %inputs.target_address,
            caller = %inputs.caller,
            calldata_len = calldata.len(),
            "inspector: CALL"
        );

        // Look up `authorizedProxies[target_address]` on the configured
        // contract via the live EVM state. Sees in-tx / in-block writes.
        let Some(info) = lookup_authorized_proxy_live(
            context,
            self.proxy_lookup.contract_address,
            self.proxy_lookup.authorized_proxies_slot,
            inputs.target_address,
        ) else {
            tracing::trace!(addr = %inputs.target_address, "inspector: not a proxy");
            return None;
        };

        let call_value = inputs.value.get();

        let req = eez_protocol::ExecutionRequest {
            call_mode: CallMode::Mutable,
            target_address: info.original_address,
            data: calldata.clone(),
            value: call_value,
            source_address: inputs.caller,
            source_rollup_id: self.caller_rollup_id,
        };
        // Dispatch is synchronous, so this inspector can exchange cache
        // snapshots around the nested execution.
        // Snapshot this rollup's in-flight cache before dispatch. If the
        // downstream call re-enters this rollup, the new session preloads the
        // snapshot and publishes its updated cache for this frame to apply.
        let overlay_active = self.overlay_channel.is_some();
        let mut before_snapshot: Option<CacheState> = None;
        if overlay_active {
            // revm keeps mid-transaction writes in its journal, while loaded
            // cache entries can still contain their pre-write values. Refresh
            // every loaded account and slot through `Host` before sharing the
            // cache. An `SSTORE` first loads its slot, so the cache already
            // identifies every storage key that needs refreshing.
            let mut cache = context.db_mut().cache.clone();
            // Collect keys before mutating the cloned cache with refreshed
            // values.
            let mut slots_to_refresh: Vec<(Address, alloy_primitives::U256)> = Vec::new();
            let mut addrs_to_refresh: Vec<Address> = Vec::new();
            for (addr, acc) in &cache.accounts {
                addrs_to_refresh.push(*addr);
                if let Some(plain) = acc.account.as_ref() {
                    for slot in plain.storage.keys() {
                        slots_to_refresh.push((*addr, *slot));
                    }
                }
            }
            // Read live storage values through the journal-aware host.
            let mut live_storage: Vec<(Address, alloy_primitives::U256, alloy_primitives::U256)> =
                Vec::with_capacity(slots_to_refresh.len());
            for (addr, slot) in slots_to_refresh {
                if let Some(load) = context.sload(addr, slot) {
                    live_storage.push((addr, slot, load.data));
                }
            }
            // Read live balances and nonces through the host.
            let mut live_account_info: Vec<(Address, alloy_primitives::U256, u64)> =
                Vec::with_capacity(addrs_to_refresh.len());
            for addr in addrs_to_refresh {
                if let Ok(info) = context.load_account_info_skip_cold_load(addr, false, true) {
                    live_account_info.push((addr, info.balance, info.nonce));
                }
            }
            for (addr, slot, value) in live_storage {
                if let Some(acc) = cache.accounts.get_mut(&addr)
                    && let Some(plain) = acc.account.as_mut()
                {
                    plain.storage.insert(slot, value);
                }
            }
            for (addr, balance, nonce) in live_account_info {
                if let Some(acc) = cache.accounts.get_mut(&addr)
                    && let Some(plain) = acc.account.as_mut()
                {
                    plain.info.balance = balance;
                    plain.info.nonce = nonce;
                }
            }
            before_snapshot = Some(cache.clone());
            if let Some(channel) = &self.overlay_channel {
                channel.push_pre_snapshot(cache);
            }
        }
        let sim = self.dispatcher.dispatch_call(info.original_rollup_id, req);
        // A downstream session that re-entered this rollup publishes its
        // post-execution cache on the same channel. Apply that diff before
        // this EVM frame continues.
        if overlay_active {
            let after_opt = self
                .overlay_channel
                .as_ref()
                .and_then(|c| c.pop_post_cache());
            if let (Some(before), Some(after)) = (before_snapshot.as_ref(), after_opt.as_ref())
                && let Err(e) = crate::overlay::apply_overlay_diff(context, before, after)
            {
                self.record_error(ExecutorError::evm(format!(
                    "overlay diff-apply failed: {e}"
                )));
            }
            if let Some(channel) = &self.overlay_channel {
                channel.pop_pre_snapshot();
            }
        }
        let sim = match sim {
            Ok(response) => response,
            Err(e) => {
                tracing::error!(error = %e, "target execution failed");
                self.record_error(e);
                return Some(CallOutcome::new(
                    InterpreterResult::new(
                        InstructionResult::Revert,
                        Bytes::new(),
                        Gas::new(inputs.gas_limit),
                    ),
                    inputs.return_memory_offset.clone(),
                ));
            }
        };

        let success = sim.is_success();
        tracing::info!(
            dest = %info.original_address,
            rollup_id = %info.original_rollup_id,
            caller = %inputs.caller,
            proxy = %inputs.target_address,
            depth = context.journal_ref().depth(),
            calldata_len = calldata.len(),
            value = %call_value,
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
        let result = if success {
            InstructionResult::Return
        } else {
            InstructionResult::Revert
        };

        Some(CallOutcome::new(
            InterpreterResult::new(result, return_data, Gas::new(inputs.gas_limit)),
            inputs.return_memory_offset.clone(),
        ))
    }

    fn call_end(&mut self, _context: &mut CTX, _inputs: &CallInputs, outcome: &mut CallOutcome) {
        // Pair with the count captured in `call`. A reverted frame with a
        // non-empty dispatch range records the span and queues the affected
        // session checkpoints for rollback before the next dispatch.
        //
        // Skip when an executor error already aborts composition, no calls
        // were dispatched in the frame, or the outcome is not an explicit
        // revert.
        let start = self.frame_starts.pop().unwrap_or(0);
        let end = self.dispatcher.recorded_count();
        if !self.has_error() && start < end && outcome.result.result == InstructionResult::Revert {
            let span = u32::try_from(end - start).unwrap_or(u32::MAX);
            self.dispatcher.annotate_revert_span(start, span);
        }
    }
}

#[cfg(test)]
mod tests {
    //! Tests for [`lookup_authorized_proxy_live`] + the two per-variant
    //! slot paths exercised via [`ProxyLookupConfig`].
    //!
    //! These target the core reason this helper exists: reading from
    //! revm's live state (journal + DB) instead of a pre-tx `StateProvider`
    //! snapshot.

    use super::*;
    use alloy_primitives::{U256, address};
    use eez_protocol::{EEZ_AUTHORIZED_PROXIES_SLOT, EEZL2_AUTHORIZED_PROXIES_SLOT};
    use revm::MainContext;
    use revm::context::Context;
    use revm::database::{CacheDB, EmptyDB};

    const EEZ_ADDRESS: Address = address!("0x1111111111111111111111111111111111111111");
    const EEZL2_ADDRESS: Address = address!("0x4444444444444444444444444444444444444444");
    const PROXY_ADDR: Address = address!("0x2222222222222222222222222222222222222222");
    const DESTINATION_ADDR: Address = address!("0x3333333333333333333333333333333333333333");
    const TARGET_ROLLUP: u64 = 42;

    fn packed_proxy_value(destination: Address, rollup_id: u64) -> U256 {
        let mut word = [0u8; 32];
        word[3..11].copy_from_slice(&rollup_id.to_be_bytes());
        word[11..31].copy_from_slice(destination.as_ref());
        word[31] = 1;
        U256::from_be_bytes(word)
    }

    fn fresh_context() -> Context<
        revm::context::BlockEnv,
        revm::context::TxEnv,
        revm::context::CfgEnv,
        CacheDB<EmptyDB>,
        revm::context::Journal<CacheDB<EmptyDB>>,
        (),
    > {
        Context::mainnet().with_db(CacheDB::<EmptyDB>::default())
    }

    #[test]
    fn unregistered_address_returns_none() {
        let mut ctx = fresh_context();
        ctx.journal_mut()
            .load_account(EEZ_ADDRESS)
            .expect("load EEZ account");

        let info = lookup_authorized_proxy_live(
            &mut ctx,
            EEZ_ADDRESS,
            EEZ_AUTHORIZED_PROXIES_SLOT,
            PROXY_ADDR,
        );
        assert!(info.is_none());
    }

    #[test]
    fn registered_in_db_returns_some_cold_read() {
        let key = proxy_mapping_key(PROXY_ADDR, EEZ_AUTHORIZED_PROXIES_SLOT);
        let value = packed_proxy_value(DESTINATION_ADDR, TARGET_ROLLUP);

        let mut cache_db = CacheDB::<EmptyDB>::default();
        cache_db
            .insert_account_storage(EEZ_ADDRESS, key.into(), value)
            .expect("populate storage");

        let mut ctx = Context::mainnet().with_db(cache_db);
        ctx.journal_mut()
            .load_account(EEZ_ADDRESS)
            .expect("load EEZ account");

        let info = lookup_authorized_proxy_live(
            &mut ctx,
            EEZ_ADDRESS,
            EEZ_AUTHORIZED_PROXIES_SLOT,
            PROXY_ADDR,
        )
        .expect("proxy present in DB");

        assert_eq!(info.original_address, DESTINATION_ADDR);
        assert_eq!(info.original_rollup_id, RollupId(TARGET_ROLLUP));
    }

    #[test]
    fn registered_in_journal_returns_some_hot_read() {
        let mut ctx = fresh_context();
        let key = proxy_mapping_key(PROXY_ADDR, EEZ_AUTHORIZED_PROXIES_SLOT);
        let value = packed_proxy_value(DESTINATION_ADDR, TARGET_ROLLUP);

        ctx.journal_mut()
            .load_account(EEZ_ADDRESS)
            .expect("load EEZ account");
        ctx.journal_mut()
            .sstore(EEZ_ADDRESS, key.into(), value)
            .expect("journal sstore");

        let info = lookup_authorized_proxy_live(
            &mut ctx,
            EEZ_ADDRESS,
            EEZ_AUTHORIZED_PROXIES_SLOT,
            PROXY_ADDR,
        )
        .expect("journal must expose in-tx writes to the inspector");

        assert_eq!(info.original_address, DESTINATION_ADDR);
        assert_eq!(info.original_rollup_id, RollupId(TARGET_ROLLUP));
    }

    #[test]
    fn wrong_source_contract_slot_returns_none() {
        // Populate the correct slot; reading an arbitrary other slot
        // must miss, not silently decode garbage.
        let correct_slot = EEZ_AUTHORIZED_PROXIES_SLOT;
        let wrong_slot: u8 = 7;
        assert_ne!(
            correct_slot, wrong_slot,
            "test premise: the two slots must differ"
        );

        let key = proxy_mapping_key(PROXY_ADDR, correct_slot);
        let value = packed_proxy_value(DESTINATION_ADDR, TARGET_ROLLUP);

        let mut cache_db = CacheDB::<EmptyDB>::default();
        cache_db
            .insert_account_storage(EEZ_ADDRESS, key.into(), value)
            .expect("populate storage");

        let mut ctx = Context::mainnet().with_db(cache_db);
        ctx.journal_mut()
            .load_account(EEZ_ADDRESS)
            .expect("load EEZ account");

        let info = lookup_authorized_proxy_live(&mut ctx, EEZ_ADDRESS, wrong_slot, PROXY_ADDR);
        assert!(
            info.is_none(),
            "reading with the wrong slot must miss, not silently decode garbage"
        );
    }

    // ── Cache exchange and proxy lookup ───────────────────────────────

    #[test]
    fn overlay_cache_snapshots_are_lifo() {
        let channel = OverlayChannel::default();
        let first = CacheState::default();
        let mut second = CacheState::default();
        second.insert_not_existing(PROXY_ADDR);

        channel.push_pre_snapshot(first.clone());
        channel.push_pre_snapshot(second.clone());
        assert_eq!(channel.peek_pre_snapshot(), Some(second.clone()));
        assert_eq!(channel.pop_pre_snapshot(), Some(second.clone()));
        assert_eq!(channel.pop_pre_snapshot(), Some(first.clone()));
        assert_eq!(channel.pop_pre_snapshot(), None);

        channel.push_post_cache(first.clone());
        channel.push_post_cache(second.clone());
        assert_eq!(channel.pop_post_cache(), Some(second));
        assert_eq!(channel.pop_post_cache(), Some(first));
        assert_eq!(channel.pop_post_cache(), None);
    }

    #[test]
    fn eez_slot_path_reads_slot_0() {
        // ProxyLookupConfig with source_contract=EEZ routes to slot 0
        // (authorizedProxies declared on EEZBase, first storage slot
        // of every child).
        let config = ProxyLookupConfig {
            contract_address: EEZ_ADDRESS,
            authorized_proxies_slot: EEZ_AUTHORIZED_PROXIES_SLOT,
        };
        assert_eq!(config.authorized_proxies_slot, 0u8);

        let key = proxy_mapping_key(PROXY_ADDR, config.authorized_proxies_slot);
        let value = packed_proxy_value(DESTINATION_ADDR, TARGET_ROLLUP);

        let mut cache_db = CacheDB::<EmptyDB>::default();
        cache_db
            .insert_account_storage(config.contract_address, key.into(), value)
            .expect("populate storage");
        let mut ctx = Context::mainnet().with_db(cache_db);
        ctx.journal_mut()
            .load_account(config.contract_address)
            .expect("load");

        let info = lookup_authorized_proxy_live(
            &mut ctx,
            config.contract_address,
            config.authorized_proxies_slot,
            PROXY_ADDR,
        )
        .expect("EEZ config path must find proxy");
        assert_eq!(info.original_rollup_id, RollupId(TARGET_ROLLUP));
        assert_eq!(info.original_address, DESTINATION_ADDR);
    }

    #[test]
    fn eezl2_slot_path_reads_slot_0() {
        // Keep the EEZL2 constant path distinct so each dialect's configured
        // slot is tested.
        let config = ProxyLookupConfig {
            contract_address: EEZL2_ADDRESS,
            authorized_proxies_slot: EEZL2_AUTHORIZED_PROXIES_SLOT,
        };
        assert_eq!(config.authorized_proxies_slot, 0u8);

        let key = proxy_mapping_key(PROXY_ADDR, config.authorized_proxies_slot);
        let value = packed_proxy_value(DESTINATION_ADDR, TARGET_ROLLUP);

        let mut cache_db = CacheDB::<EmptyDB>::default();
        cache_db
            .insert_account_storage(config.contract_address, key.into(), value)
            .expect("populate storage");
        let mut ctx = Context::mainnet().with_db(cache_db);
        ctx.journal_mut()
            .load_account(config.contract_address)
            .expect("load");

        let info = lookup_authorized_proxy_live(
            &mut ctx,
            config.contract_address,
            config.authorized_proxies_slot,
            PROXY_ADDR,
        )
        .expect("EEZL2 config path must find proxy");
        assert_eq!(info.original_rollup_id, RollupId(TARGET_ROLLUP));
        assert_eq!(info.original_address, DESTINATION_ADDR);
    }
}
