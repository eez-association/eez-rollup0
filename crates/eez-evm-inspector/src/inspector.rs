//! [`SessionInspector`]: detects cross-chain proxy calls during source
//! chain EVM execution and dispatches each one through a borrowed
//! [`Dispatcher`].
//!
//! When the inspector sees a CALL opcode:
//! 1. `lookup_authorized_proxy_live` reads `authorizedProxies[addr]`
//!    from the **live EVM state** (revm journal + DB) — so proxies
//!    registered by earlier calls in the same transaction, or by
//!    earlier transactions in the same in-progress block, are visible.
//! 2. If the address is a registered proxy,
//!    [`Dispatcher::dispatch_call`] forwards the call to the target
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
//! [`Dispatcher`]: eez_protocol::CompositionBuilder
//! [`Dispatcher::dispatch_call`]: eez_protocol::CompositionBuilder::dispatch_call

use alloy_primitives::{Address, Bytes};
use std::sync::{Arc, Mutex};

use revm::Inspector;
use revm::context_interface::{ContextTr, Host, JournalTr};
use revm::database::{CacheState, State};
use revm::interpreter::{
    CallInputs, CallOutcome, CallScheme, Gas, InstructionResult, InterpreterResult,
};

use eez_protocol::{CompositionBuilder, ExecutorError, ProxyLookupConfig, RollupId};
use eez_protocol::{ProxyInfo, decode_proxy_value, proxy_mapping_key};

/// Bidirectional side-channel between source-sim and the overlay
/// session opened on the entry rollup.
///
/// # Why it exists
///
/// At the moment a target-session inspector detects a nested call
/// targeting the entry rollup, the inspector holds `&mut target_ctx`
/// — not `&mut source_ctx`. Source-sim's State is several stack frames
/// up, mutably borrowed by source-sim's `evm.transact`. The overlay
/// session opened on the entry rollup needs source-sim's *current*
/// cache to preload a fresh `State` via
/// `State::builder().with_cached_prestate(...)`; once it executes,
/// source-sim's continued execution needs to observe the overlay's
/// mutations. Rust can't hand out a second `&mut` to source state.
///
/// The channel is the workaround. Two slots:
///
/// - `source_cache` (source-sim → overlay): the source-sim inspector
///   clones source's cache into this slot before dispatching
///   downstream. The entry rollup's `LocalExecutionSession`, opened
///   as the entry rollup's `TargetExecutionSession` by the
///   `CompositionBuilder`, reads from it on construction to preload.
///
/// - `overlay_cache` (overlay → source-sim): the
///   `LocalExecutionSession` (entry rollup) writes its
///   post-execute `state.cache.clone()` into this slot at the end of
///   each `execute`. The source-sim inspector reads it after the
///   downstream dispatch returns and applies the diff onto source's
///   journal via [`apply_overlay_diff`](crate::overlay::apply_overlay_diff).
///   Last write wins — the entry session is reused across multiple
///   overlay executes within one source-sim hook, so the final
///   accumulated cache is what reaches source-sim.
///
/// Single-threaded by design — `block_in_place` keeps source and
/// overlay execution on the same worker. The `Mutex`es are therefore
/// uncontended in steady state; they exist for borrow-checker
/// satisfaction, not concurrent access.
///
/// # Lifecycle
///
/// Both slots are visible only while source-sim's `evm.transact` is
/// inside an `Inspector::call` hook that has dispatched to a target
/// session. Outside that window both slots are `None`. Any read of
/// `source_cache` outside the hook is a programming error.
#[derive(Debug, Default)]
pub struct OverlayChannel {
    /// Monotonic write counter incremented by every `push_post_cache`
    /// or `append_post_root`. Sampled into a session checkpoint so
    /// rollback can truncate the post-cache log + post-root log back
    /// to the snapshot point. The counter itself never decreases —
    /// `truncate_to_epoch` cuts the underlying logs to length
    /// `target_epoch`, leaving the counter ahead so post-rollback
    /// writes still get strictly-greater epoch values.
    epoch: std::sync::atomic::AtomicU64,
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
    /// Append-only per-overlay-execute post-state roots. See
    /// [`append_post_root`] / [`drain_post_roots`].
    ///
    /// [`append_post_root`]: OverlayChannel::append_post_root
    /// [`drain_post_roots`]: OverlayChannel::drain_post_roots
    per_tx_roots: Mutex<Vec<[u8; 32]>>,
}

impl OverlayChannel {
    /// Push a pre-dispatch cache snapshot — called by every inspector
    /// frame before `block_in_place`. Stack-based for recursive
    /// re-entry: `reentrantCrossChainCalls` re-enters L1's overlay
    /// multiple times within one source-sim tx; each frame's
    /// pre-snapshot nests cleanly via push/pop.
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

    /// Peek the top pre-dispatch snapshot without popping. Lazy-opened
    /// sessions for this rollup use this to preload — "the latest live
    /// state of this rollup" at the moment the inner dispatch fires.
    pub fn peek_pre_snapshot(&self) -> Option<CacheState> {
        self.source_cache
            .lock()
            .ok()
            .and_then(|g| g.last().cloned())
    }

    /// Push a post-execute cache snapshot — called by the overlay
    /// session at `commit_and_finish`. The matching inspector pops it
    /// after `block_in_place` returns and applies the diff onto its
    /// own `&mut ctx`. Without the stack, two nested overlay executes'
    /// post-caches would collide.
    pub fn push_post_cache(&self, cache: CacheState) {
        if let Ok(mut guard) = self.overlay_cache.lock() {
            guard.push(cache);
            self.epoch.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    /// Pop the most recent post-execute cache so the inspector can
    /// run `apply_overlay_diff` on it.
    pub fn pop_post_cache(&self) -> Option<CacheState> {
        self.overlay_cache.lock().ok().and_then(|mut g| g.pop())
    }

    /// Append a per-overlay-execute post-state root in chronological
    /// dispatch order. Each entry rollup overlay session execute
    /// appends its `current_root` after `commit_and_finish`.
    pub fn append_post_root(&self, root: [u8; 32]) {
        if let Ok(mut guard) = self.per_tx_roots.lock() {
            guard.push(root);
            self.epoch.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    /// Drain the accumulated per-tx roots. Called by source-sim at
    /// end of `simulate_source_tx`; forwarded to
    /// `CompositionBuilder::set_extra_per_tx_roots(entry_id, roots)` so
    /// `finalize` can populate `per_tx_roots_by_rollup[entry]` —
    /// otherwise nested calls attributed to the entry rollup
    /// (`reentrantCrossChainCalls`-style deep alternation) hit
    /// `InvalidCheckpoint: no per_tx_roots entry for attribution
    /// rollup <entry>` in `build_batch`.
    pub fn drain_post_roots(&self) -> Vec<[u8; 32]> {
        self.per_tx_roots
            .lock()
            .ok()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default()
    }

    /// Snapshot the current write-epoch. Pair with
    /// [`truncate_to_lengths`](Self::truncate_to_lengths) to roll the
    /// channel back to its state at the snapshot point. Counter is
    /// monotonic; truncation only shortens the underlying logs.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Truncate the post-cache log + post-root log to the lengths they
    /// held when `target_epoch` was sampled. The atomic counter is
    /// NOT decremented — its only consumer is to record snapshot
    /// points, and post-rollback writes must produce strictly-greater
    /// epoch values to keep ordering with any retained snapshots.
    ///
    /// Each `push_post_cache` and `append_post_root` since the
    /// snapshot point incremented the epoch by 1, so the difference
    /// `current - target` is exactly the number of writes to undo
    /// across the two logs combined. Because the two logs interleave
    /// in real-time (a single dispatch usually does both), this
    /// primitive truncates each log to its own length-at-snapshot
    /// rather than redistributing the delta.
    ///
    /// Callers stash the per-log lengths alongside the epoch in the
    /// session's snapshot; this method clamps the logs to those
    /// lengths.
    pub fn truncate_to_lengths(&self, post_cache_len: usize, post_roots_len: usize) {
        if let Ok(mut guard) = self.overlay_cache.lock() {
            guard.truncate(post_cache_len);
        }
        if let Ok(mut guard) = self.per_tx_roots.lock() {
            guard.truncate(post_roots_len);
        }
    }

    /// Sample the current per-log lengths. Stash alongside the
    /// session snapshot so
    /// [`truncate_to_lengths`](Self::truncate_to_lengths) can roll
    /// back precisely.
    #[must_use]
    pub fn current_log_lengths(&self) -> (usize, usize) {
        let post_cache = self.overlay_cache.lock().map_or(0, |g| g.len());
        let post_roots = self.per_tx_roots.lock().map_or(0, |g| g.len());
        (post_cache, post_roots)
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
/// [`CompositionBuilder::annotate_revert_span`](eez_protocol::CompositionBuilder::annotate_revert_span). `recorded[..]` is preorder
/// by construction — every call's slot index is fixed at
/// `CompositionBuilder::open_call` time, BEFORE the session recurses — so
/// `span = end - start` is exactly the on-chain `revertSpan` for the
/// bracketed top-level call.
pub struct SessionInspector<'a> {
    /// Combined proxy-lookup configuration: the contract address to
    /// read and which slot to read from.
    proxy_lookup: ProxyLookupConfig,
    /// Dispatch surface — composer or gRPC stub — that routes detected
    /// calls to their target rollups by id.
    dispatcher: &'a mut CompositionBuilder,
    /// Rollup id of the chain this inspector is running on — the
    /// `caller_id` passed to `dispatch_call` so the resulting
    /// [`RecordedCall.caller_rollup_id`](eez_protocol::ExecutedAction::caller_rollup_id)
    /// is correct for nested action-hash emission.
    caller_rollup_id: RollupId,
    /// First target execution error, if any.
    error: Option<ExecutorError>,
    call_depth: usize,
    proxy_lookups: usize,
    /// Tokio runtime handle for bridging the sync `Inspector::call` hook
    /// to the async `CompositionBuilder::dispatch_call`.
    handle: tokio::runtime::Handle,
    /// Bidirectional side-channel between source-sim and the entry
    /// rollup's overlay session.
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
            .field("call_depth", &self.call_depth)
            .field("proxy_lookups", &self.proxy_lookups)
            .field("has_error", &self.error.is_some())
            .field("has_overlay_channel", &self.overlay_channel.is_some())
            .finish_non_exhaustive()
    }
}

/// Shared construction surface for [`SessionInspector`] instances.
///
/// Collapses the per-chain configuration (proxy lookup + caller rollup
/// id) that both the source-sim path and the target-session path need,
/// so each call site produces inspectors the same way and any future
/// field added to `SessionInspector` flows through one site.
///
/// Each call site produces an inspector via [`build`](Self::build).
#[derive(Debug, Clone)]
pub struct SessionInspectorFactory {
    /// `(contract_address, slot)` discriminator for this chain's
    /// `authorizedProxies`. Derived from role at client construction.
    proxy_lookup: ProxyLookupConfig,
    /// Rollup id of the chain this factory belongs to — becomes the
    /// `caller_id` on every call dispatched through inspectors it
    /// builds.
    caller_rollup_id: RollupId,
    /// Overlay channel handle for the shared-source-state overlay
    /// path. `Some(channel)` only on the entry-role factory used at
    /// the source-sim site; follower-role factories used at
    /// target-session sites leave this `None`. Forwarded to the
    /// inspector via [`SessionInspectorFactory::build`] so root-frame
    /// inspectors snapshot source's cache before each downstream
    /// dispatch and apply overlay diffs after.
    overlay_channel: Option<OverlayChannelHandle>,
}

impl SessionInspectorFactory {
    /// Create a factory pinned to one chain's configuration.
    ///
    /// Defaults to no overlay channel — the entry-role factory used
    /// for source-sim should call
    /// [`with_overlay_channel`](Self::with_overlay_channel) to install
    /// one.
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
    /// Caller responsibility: only attach this to the factory the
    /// source-sim inspector is built from. Inspectors built from
    /// factories without a channel do not snapshot or apply diffs,
    /// which is the correct default for target-session frames.
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

    /// Caller rollup id this factory passes to every built inspector.
    #[must_use]
    pub fn caller_rollup_id(&self) -> RollupId {
        self.caller_rollup_id
    }

    /// Build an inspector. Used by both the source-sim path and the
    /// target-session path. The inspector observes proxy CALLs and
    /// dispatches each through the supplied [`CompositionBuilder`]; nested
    /// dispatches (a target-session inspector handing a callback
    /// back to the composer) are recorded as preorder children of
    /// the outer call by virtue of the dispatcher's `open_call`
    /// timing.
    pub fn build<'a>(
        &self,
        dispatcher: &'a mut CompositionBuilder,
        handle: tokio::runtime::Handle,
    ) -> SessionInspector<'a> {
        let mut insp = SessionInspector::new(
            self.proxy_lookup.clone(),
            dispatcher,
            self.caller_rollup_id,
            handle,
        );
        insp.overlay_channel = self.overlay_channel.clone();
        insp
    }
}

impl<'a> SessionInspector<'a> {
    /// Create a new inspector instance.
    ///
    /// Used by the source-simulation path where every detected proxy
    /// call dispatches through the supplied [`CompositionBuilder`] and is
    /// recorded into the composition's preorder `recorded[..]` slice.
    ///
    /// - `proxy_lookup`: the (contract, slot) pair to read
    ///   `authorizedProxies` from on this chain. Derived from role at
    ///   `main.rs` startup (entry → Rollups slot 3; follower → CCM slot 1).
    /// - `dispatcher`: dispatch surface for detected calls.
    /// - `caller_rollup_id`: this chain's rollup id — becomes the
    ///   `caller_id` on each dispatched call.
    /// - `handle`: tokio runtime handle for the sync→async bridge.
    pub fn new(
        proxy_lookup: ProxyLookupConfig,
        dispatcher: &'a mut CompositionBuilder,
        caller_rollup_id: RollupId,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            proxy_lookup,
            dispatcher,
            caller_rollup_id,
            error: None,
            call_depth: 0,
            proxy_lookups: 0,
            handle,
            overlay_channel: None,
            frame_starts: Vec::new(),
        }
    }

    /// Number of proxy lookups performed during this EVM pass.
    #[must_use]
    pub fn proxy_lookups(&self) -> usize {
        self.proxy_lookups
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
        self.call_depth += 1;
        // Bracket every CALL frame with a recorded-count snapshot so
        // `call_end` can detect "frame reverted AFTER dispatching one
        // or more cross-chain calls" (revert-continue patterns —
        // see field doc).
        self.frame_starts.push(self.dispatcher.recorded_count());

        if self.has_error() {
            return None;
        }

        let calldata = inputs.input.bytes(context);

        tracing::trace!(
            depth = self.call_depth,
            target_addr = %inputs.target_address,
            caller = %inputs.caller,
            calldata_len = calldata.len(),
            "inspector: CALL"
        );

        // Look up `authorizedProxies[target_address]` on the configured
        // contract via the live EVM state. Sees in-tx / in-block writes.
        self.proxy_lookups += 1;
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

        let scheme = match inputs.scheme {
            CallScheme::Call => "CALL",
            CallScheme::CallCode => "CALLCODE",
            CallScheme::DelegateCall => "DELEGATECALL",
            CallScheme::StaticCall => "STATICCALL",
        };

        let req = eez_protocol::ExecutionRequest {
            target_address: info.original_address,
            data: calldata.clone(),
            value: call_value,
            source_address: inputs.caller,
            source_rollup_id: self.caller_rollup_id,
        };
        // Bridge sync Inspector::call → async CompositionBuilder::dispatch_call.
        //
        // Uses `tokio::task::block_in_place` to release the worker
        // thread from tokio's runtime context for the duration of the
        // dispatch, then `Handle::block_on` to drive the future. The
        // dispatch runs on the SAME thread that's running source-sim's
        // `evm.transact` — keeping cross-chain dispatch on a single
        // thread and giving the overlay path direct access to
        // source-sim's `&mut State<DB>` via the inspector's `&mut ctx`.
        //
        // Hard requirement: multi-threaded tokio runtime.
        // `block_in_place` panics on single-threaded runtimes; the
        // gRPC server's bidi Execute stream imposes the same
        // constraint, so the workspace runs only on multi-threaded
        // tokio.
        //
        let caller_id = self.caller_rollup_id;
        // Overlay-path producer/consumer: source-sim's root-frame
        // inspector snapshots source's in-flight cache into the channel
        // before dispatching downstream (preload for the entry-rollup
        // overlay session) and applies the channel's overlay-after diff
        // back onto source's journal after dispatch returns. Both halves
        // are skipped on target-session inspectors (whose factory is
        // built without an overlay channel); the runtime check on
        // `is_nested_frame` keeps the invariant local to this file.
        // Stack-based overlay channel for recursive re-entry. When the
        // channel is installed every inspector frame participates in
        // push/pop on its rollup's channel — re-entered sessions
        // inherit their own rollup's mid-execute state and write
        // back accumulated mutations.
        let overlay_active = self.overlay_channel.is_some();
        let mut before_snapshot: Option<CacheState> = None;
        if overlay_active {
            // `state.cache.clone()` holds the values revm LOADED from
            // disk during the source-sim tx, but mid-tx `SSTORE` /
            // balance changes live in the journal until commit; the
            // cached values stay at their pre-write snapshots. flash-
            // loan exposed this: source-sim's `bridge.bridgeTokens`
            // does `safeTransferFrom(executor, BridgeL1, 10000e18)`
            // BEFORE its proxy.call dispatches CC1, then later CC2-
            // nested needs the L1 entry overlay to read the locked
            // balance — the cache alone returns 0.
            //
            // Refresh the cloned cache by re-reading every loaded slot
            // and account through `Host::sload` / `Host::balance` /
            // `Host::load_account_info_skip_cold_load`, which go
            // through the journal and return the LIVE post-write
            // values. We don't need to discover NEW slots — the journal
            // and cache touch the same addresses (any SSTORE is
            // preceded by an SLOAD which adds the slot to the cache),
            // so iterating cache slots is sufficient.
            let mut cache = context.db_mut().cache.clone();
            // Collect (addr, slot) pairs to refresh — must release the
            // `&mut updated` borrow before calling Host::sload.
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
            // Snapshot live storage values via Host::sload (journal-aware).
            let mut live_storage: Vec<(Address, alloy_primitives::U256, alloy_primitives::U256)> =
                Vec::with_capacity(slots_to_refresh.len());
            for (addr, slot) in slots_to_refresh {
                if let Some(load) = context.sload(addr, slot) {
                    live_storage.push((addr, slot, load.data));
                }
            }
            // Snapshot live balance/nonce via Host::load_account_info_skip_cold_load.
            let mut live_account_info: Vec<(Address, alloy_primitives::U256, u64)> =
                Vec::with_capacity(addrs_to_refresh.len());
            for addr in addrs_to_refresh {
                if let Ok(info) = context.load_account_info_skip_cold_load(addr, false, true) {
                    live_account_info.push((addr, info.balance, info.nonce));
                }
            }
            // Apply live values to the cloned cache.
            for (addr, slot, value) in live_storage {
                if let Some(acc) = cache.accounts.get_mut(&addr) {
                    if let Some(plain) = acc.account.as_mut() {
                        plain.storage.insert(slot, value);
                    }
                }
            }
            for (addr, balance, nonce) in live_account_info {
                if let Some(acc) = cache.accounts.get_mut(&addr) {
                    if let Some(plain) = acc.account.as_mut() {
                        plain.info.balance = balance;
                        plain.info.nonce = nonce;
                    }
                }
            }
            before_snapshot = Some(cache.clone());
            if let Some(channel) = &self.overlay_channel {
                channel.push_pre_snapshot(cache);
            }
        }
        let sim = {
            let handle = &self.handle;
            let dispatcher = &mut *self.dispatcher;
            tokio::task::block_in_place(|| {
                handle.block_on(dispatcher.dispatch_call(info.original_rollup_id, caller_id, req))
            })
        };
        // After block_in_place: any inner sessions on THIS rollup have
        // pushed their post-cache to `overlay_cache` (LIFO). Pop the
        // top and apply diff onto our `&mut ctx` so this frame's
        // continued execution sees the inner session's mutations.
        // Errors during apply are surfaced via `record_error` so the
        // caller's `take_error()` consumer sees them (invariant 7).
        if overlay_active {
            let after_opt = self
                .overlay_channel
                .as_ref()
                .and_then(|c| c.pop_post_cache());
            if let (Some(before), Some(after)) = (before_snapshot.as_ref(), after_opt.as_ref()) {
                if let Err(e) = crate::overlay::apply_overlay_diff(context, before, after) {
                    self.record_error(ExecutorError::evm(format!(
                        "overlay diff-apply failed: {e}"
                    )));
                }
            }
            if let Some(channel) = &self.overlay_channel {
                channel.pop_pre_snapshot();
            }
        }
        let sim = match sim {
            Ok(outcome) => outcome,
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
            depth = self.call_depth,
            scheme,
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
        // so try/catch + revertSpan accounting bracket the right range.
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
        // Pair with the snapshot pushed in `call`. If the frame ended
        // with `Revert` AND one or more dispatches happened inside it
        // (range non-empty), notify the dispatcher: those recorded
        // calls' effects on the source state-delta chain are rolled
        // back, and any in-memory target session that captured those
        // writes is evicted so the next dispatch reads pre-revert
        // disk state.
        //
        // Skipped when:
        // - We already recorded an executor error this pass — the
        //   composition will fail; spurious marks would be noise.
        // - The popped range is empty — proxy frames have count++ but
        //   we synthesize `Return` on success; non-proxy frames with
        //   no children leave count unchanged. Either way, nothing
        //   to mark.
        // - The frame's `InstructionResult` is not `Revert` — we
        //   only act on controlled Solidity reverts (the `revert(...)`
        //   opcode), not `OutOfGas` / `InvalidOpcode` / etc. Other
        //   failure modes are not part of any try/catch revert-continue
        //   pattern this protocol supports.
        let start = self.frame_starts.pop().unwrap_or(0);
        let end = self.dispatcher.recorded_count();
        if !self.has_error() && start < end && outcome.result.result == InstructionResult::Revert {
            let span = u32::try_from(end - start).unwrap_or(u32::MAX);
            self.dispatcher.annotate_revert_span(start, span);
        }
        self.call_depth = self.call_depth.saturating_sub(1);
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
    use eez_protocol::{CCM_AUTHORIZED_PROXIES_SLOT, ROLLUPS_AUTHORIZED_PROXIES_SLOT};
    use revm::MainContext;
    use revm::context::Context;
    use revm::database::{CacheDB, EmptyDB};

    const ROLLUPS_ADDR: Address = address!("0x1111111111111111111111111111111111111111");
    const PROXY_ADDR: Address = address!("0x2222222222222222222222222222222222222222");
    const DESTINATION_ADDR: Address = address!("0x3333333333333333333333333333333333333333");
    const TARGET_ROLLUP: u64 = 42;

    fn packed_proxy_value(destination: Address, rollup_id: u64) -> U256 {
        let mut word = [0u8; 32];
        word[4..12].copy_from_slice(&rollup_id.to_be_bytes());
        word[12..32].copy_from_slice(destination.as_ref());
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
            .load_account(ROLLUPS_ADDR)
            .expect("load rollups account");

        let info = lookup_authorized_proxy_live(
            &mut ctx,
            ROLLUPS_ADDR,
            ROLLUPS_AUTHORIZED_PROXIES_SLOT,
            PROXY_ADDR,
        );
        assert!(info.is_none());
    }

    #[test]
    fn registered_in_db_returns_some_cold_read() {
        let key = proxy_mapping_key(PROXY_ADDR, ROLLUPS_AUTHORIZED_PROXIES_SLOT);
        let value = packed_proxy_value(DESTINATION_ADDR, TARGET_ROLLUP);

        let mut cache_db = CacheDB::<EmptyDB>::default();
        cache_db
            .insert_account_storage(ROLLUPS_ADDR, key.into(), value)
            .expect("populate storage");

        let mut ctx = Context::mainnet().with_db(cache_db);
        ctx.journal_mut()
            .load_account(ROLLUPS_ADDR)
            .expect("load rollups account");

        let info = lookup_authorized_proxy_live(
            &mut ctx,
            ROLLUPS_ADDR,
            ROLLUPS_AUTHORIZED_PROXIES_SLOT,
            PROXY_ADDR,
        )
        .expect("proxy present in DB");

        assert_eq!(info.original_address, DESTINATION_ADDR);
        assert_eq!(info.original_rollup_id, RollupId(TARGET_ROLLUP));
    }

    #[test]
    fn registered_in_journal_returns_some_hot_read() {
        let mut ctx = fresh_context();
        let key = proxy_mapping_key(PROXY_ADDR, ROLLUPS_AUTHORIZED_PROXIES_SLOT);
        let value = packed_proxy_value(DESTINATION_ADDR, TARGET_ROLLUP);

        ctx.journal_mut()
            .load_account(ROLLUPS_ADDR)
            .expect("load rollups account");
        ctx.journal_mut()
            .sstore(ROLLUPS_ADDR, key.into(), value)
            .expect("journal sstore");

        let info = lookup_authorized_proxy_live(
            &mut ctx,
            ROLLUPS_ADDR,
            ROLLUPS_AUTHORIZED_PROXIES_SLOT,
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
        let correct_slot = ROLLUPS_AUTHORIZED_PROXIES_SLOT;
        let wrong_slot: u8 = 7;
        assert_ne!(
            correct_slot, wrong_slot,
            "test premise: the two slots must differ"
        );

        let key = proxy_mapping_key(PROXY_ADDR, correct_slot);
        let value = packed_proxy_value(DESTINATION_ADDR, TARGET_ROLLUP);

        let mut cache_db = CacheDB::<EmptyDB>::default();
        cache_db
            .insert_account_storage(ROLLUPS_ADDR, key.into(), value)
            .expect("populate storage");

        let mut ctx = Context::mainnet().with_db(cache_db);
        ctx.journal_mut()
            .load_account(ROLLUPS_ADDR)
            .expect("load rollups account");

        let info = lookup_authorized_proxy_live(&mut ctx, ROLLUPS_ADDR, wrong_slot, PROXY_ADDR);
        assert!(
            info.is_none(),
            "reading with the wrong slot must miss, not silently decode garbage"
        );
    }

    // ── New tests for ProxyLookupConfig variants ────────────────────

    #[test]
    fn rollups_slot_path_reads_slot_0() {
        // ProxyLookupConfig with source_contract=EEZ routes to slot 0
        // (authorizedProxies declared on EEZBase, first storage slot
        // of every child).
        let config = ProxyLookupConfig {
            contract_address: ROLLUPS_ADDR,
            authorized_proxies_slot: ROLLUPS_AUTHORIZED_PROXIES_SLOT,
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
        .expect("Rollups-config path must find proxy");
        assert_eq!(info.original_rollup_id, RollupId(TARGET_ROLLUP));
        assert_eq!(info.original_address, DESTINATION_ADDR);
    }

    #[test]
    fn ccm_slot_path_reads_slot_0() {
        // ProxyLookupConfig with source_contract=EEZL2 routes to slot 0
        // (authorizedProxies on EEZBase, first storage slot). Same
        // value as the L1 path today — kept distinct so the two
        // constants diverging later breaks loudly.
        let config = ProxyLookupConfig {
            contract_address: ROLLUPS_ADDR,
            authorized_proxies_slot: CCM_AUTHORIZED_PROXIES_SLOT,
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
        .expect("CCM-config path must find proxy");
        assert_eq!(info.original_rollup_id, RollupId(TARGET_ROLLUP));
        assert_eq!(info.original_address, DESTINATION_ADDR);
    }
}
