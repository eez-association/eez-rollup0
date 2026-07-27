//! Stateful per-source-tx target-chain execution session.
//!
//! [`LocalExecutionSession`] accumulates target-chain state across
//! calls within one source transaction. It drives a direct call to the
//! destination contract with the proxy address as `msg.sender`
//! (CREATE2-derived); CCM verification happens in a separate batch
//! simulation coordinated by [`crate::composer::local::LocalChainClient`].
//!
//! Also hosts [`simulate_local_transactions`] — the CCM-verify batch
//! simulator on a fresh state — and the reth helpers
//! ([`open_chain_state`], [`disable_checks`], [`compute_state_root`])
//! shared between the two paths.

use alloy_primitives::{Address, B256, Bytes, U256};

use eez_evm_inspector::{OverlayChannelHandle, SessionInspectorFactory};
use reth_evm::{ConfigureEvm, Evm as _};
use reth_evm_ethereum::EthEvmConfig;
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_storage_api::{BlockNumReader, StateProvider, StateProviderFactory};
use reth_trie_common::{HashedPostState, KeccakKeyHasher};
use revm::DatabaseCommit;
use revm::database::CacheState;

use eez_protocol::{
    CompositionBuilder, ExecutionRequest, ExecutionResponse, ExecutorError, ExecutorErrorKind,
    ExecutorResult, RollupId, TargetBatchSimulation, TargetExecutionSession, TargetTransaction,
};

use super::provider::ChainProvider;

/// Gas limit for direct target-chain calls during inspection. Generous
/// enough for worst-case dev-chain paths; too low → silent reverts.
pub(super) const DIRECT_CALL_GAS_LIMIT: u64 = 30_000_000;

/// Stateful target-chain execution session.
///
/// Opened by `LocalChainClient::begin_execution_session` (the
/// `ChainClient` trait method, not an inherent fn); owned by the
/// `CompositionBuilder` through a `Rollup.session` slot for the
/// lifetime of one composition.
///
/// ## Limitation: direct call, not full CCM path
///
/// The session calls the destination contract directly with the proxy
/// address as `msg.sender` (computed via CREATE2). This gives the
/// source simulation synchronous return data, but it does not reproduce
/// the full `executeIncomingCrossChainCall` path. The composer later
/// runs a separate CCM batch simulation (via
/// [`TargetBatchSimulation`]) and patches the terminal source-entry
/// `newState` with that final root.
pub struct LocalExecutionSession {
    evm_config: EthEvmConfig,
    state: State<StateProviderDatabase<reth_storage_api::StateProviderBox>>,
    evm_env: reth_evm::EvmEnvFor<EthEvmConfig>,
    state_provider: reth_storage_api::StateProviderBox,
    current_root: B256,
    chain_id: u64,
    ccm_address: Address,
    /// When `Some`, `execute()` runs each target call under a
    /// [`eez_evm_inspector::SessionInspector`] built from this factory — forwarding
    /// detected proxy CALLs to the dispatcher. `None` means a plain
    /// direct execute with no inspection layer (used for the entry
    /// role's target-session path when the topology has no nested
    /// reentry into the entry chain). The factory holds this chain's
    /// `proxy_lookup` + `rollup_id`, so the inspector's `caller_id`
    /// matches the chain it runs on.
    inspector_factory: Option<SessionInspectorFactory>,
    /// Overlay channel write-back handle.
    /// `Some(channel)` when the entry rollup's session is opened on
    /// behalf of a nested-back-to-entry dispatch — this session writes
    /// its post-execute `state.cache.clone()` into `channel.overlay_cache`
    /// so source-sim's inspector can apply the diff onto source's
    /// journal. `None` for follower sessions and for entry sessions
    /// opened outside the overlay context.
    overlay_channel: Option<OverlayChannelHandle>,
}

impl std::fmt::Debug for LocalExecutionSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExecutionSession")
            .field("current_root", &self.current_root)
            .field("chain_id", &self.chain_id)
            .field("ccm_address", &self.ccm_address)
            .field("has_inspector_factory", &self.inspector_factory.is_some())
            .finish_non_exhaustive()
    }
}

impl LocalExecutionSession {
    /// Create from a `ChainProvider`. Opens latest state.
    ///
    /// `inspector_factory` pins this session's target-side inspection
    /// policy. When supplied, each `execute` call runs under a
    /// [`eez_evm_inspector::SessionInspector`] so proxy CALLs
    /// detected during the target-chain execution dispatch back
    /// through the supplied `dispatcher` and are recorded into the
    /// composition's preorder `recorded[..]` slice. When `None`, the
    /// session executes without an inspector — used for the entry
    /// rollup's session in topologies where no inspector dispatches
    /// back to the entry chain.
    ///
    /// `cache` is the overlay-path preload: when an inner
    /// target-session frame dispatches back to the entry rollup, the
    /// entry session is lazily opened with a clone of source-sim's
    /// in-flight cache (via the
    /// [`eez_evm_inspector::OverlayChannel`] side-channel), so
    /// the nested L1 call observes source-sim's pending writes —
    /// load-bearing for fixtures that mutate L1 state inside a single
    /// source tx (e.g. `flash-loan`). `None` opens a fresh State.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Provider`] if reading the latest
    /// block number or header fails, or if opening the state provider
    /// fails.
    pub fn new(
        provider: &ChainProvider,
        ccm_address: Address,
        inspector_factory: Option<SessionInspectorFactory>,
        cache: Option<CacheState>,
        overlay_channel: Option<OverlayChannelHandle>,
    ) -> ExecutorResult<Self> {
        let num = provider
            .provider
            .best_block_number()
            .map_err(provider_err)?;
        tracing::debug!(block = num, "target session: best block number");

        let header = provider
            .headers
            .header_by_number(num)
            .map_err(provider_err)?
            .ok_or_else(|| ExecutorError::from(ExecutorErrorKind::Missing("target header")))?;
        let current_root = header.state_root;
        tracing::debug!(
            block = num,
            state_root = %current_root,
            "target session: opened header"
        );

        let state_prov = provider.provider.latest().map_err(provider_err)?;
        let root_prov = provider.provider.latest().map_err(provider_err)?;

        let mut evm_env = provider.evm_config.evm_env(&header).map_err(evm_err)?;
        let chain_id = evm_env.cfg_env.chain_id;
        disable_checks(&mut evm_env);

        let db = StateProviderDatabase::new(state_prov);
        let mut builder = State::builder().with_database(db).with_bundle_update();
        if let Some(cache) = cache {
            builder = builder.with_cached_prestate(cache);
        }
        let state = builder.build();

        tracing::debug!(
            block = num,
            chain_id,
            %ccm_address,
            %current_root,
            "target session: ready"
        );

        Ok(Self {
            evm_config: provider.evm_config.clone(),
            state,
            evm_env,
            state_provider: root_prov,
            current_root,
            chain_id,
            ccm_address,
            inspector_factory,
            overlay_channel,
        })
    }

    /// Compute the target-chain proxy address for a given
    /// `(sourceAddress, sourceRollup)` via a static call to
    /// `CCM.computeCrossChainProxyAddress()`.
    fn compute_proxy_address(
        &mut self,
        source_address: Address,
        source_rollup: RollupId,
    ) -> Option<Address> {
        alloy_sol_types::sol! {
            function computeCrossChainProxyAddress(address originalAddress, uint256 originalRollupId) external view returns (address);
        }
        use alloy_sol_types::SolCall;

        let calldata = computeCrossChainProxyAddressCall {
            originalAddress: source_address,
            originalRollupId: U256::from(source_rollup.0),
        }
        .abi_encode();

        let tx_env = revm::context::TxEnv {
            caller: Address::ZERO,
            gas_limit: 1_000_000,
            kind: alloy_primitives::TxKind::Call(self.ccm_address),
            data: calldata.into(),
            chain_id: Some(self.chain_id),
            ..Default::default()
        };

        let result = {
            let mut evm = self
                .evm_config
                .evm_with_env(&mut self.state, self.evm_env.clone());
            evm.transact(tx_env).ok()?
        };

        let output = result.result.output()?;
        (output.len() >= 32).then(|| Address::from_slice(&output[12..32]))
    }

    /// Uninspected direct-call path: a plain `evm.transact` plus
    /// invariant-7 bookkeeping. Used when this session was opened with
    /// `inspector_factory = None` (entry rollup).
    fn execute_internal(
        &mut self,
        destination: &Address,
        calldata: &Bytes,
        value: &U256,
        source_address: &Address,
        source_rollup: RollupId,
    ) -> ExecutorResult<eez_protocol::ExecutionOutcome> {
        let tx_env = self.build_tx_env(destination, calldata, value, source_address, source_rollup);
        let caller = tx_env.caller;
        let pre_root = self.current_root;
        let (return_data, gas_used, success, mut changes) = {
            let mut evm = self
                .evm_config
                .evm_with_env(&mut self.state, self.evm_env.clone());
            let result = evm.transact(tx_env).map_err(evm_err)?;
            (
                result.result.output().cloned().unwrap_or_default(),
                result.result.tx_gas_used(),
                result.result.is_success(),
                result.state,
            )
        };
        restore_caller_nonce(&mut changes, caller);
        self.commit_and_finish(pre_root, return_data, gas_used, success, changes)
    }

    /// Inspected direct-call path. Runs the target-chain tx under the
    /// supplied [`eez_evm_inspector::SessionInspector`] so proxy CALLs detected during
    /// execution dispatch back through the underlying `Dispatcher`.
    ///
    /// The session takes the inspector by value (non-`'static`
    /// borrow) — it can't outlive this function call anyway, and the
    /// reth `evm_with_env_and_inspector` API wants ownership. Dispatch
    /// errors are surfaced via the inspector's `take_error` path on
    /// drop; we promote them into `Err` before returning the outcome.
    fn execute_internal_with_inspector(
        &mut self,
        inspector: eez_evm_inspector::SessionInspector<'_>,
        destination: &Address,
        calldata: &Bytes,
        value: &U256,
        source_address: &Address,
        source_rollup: RollupId,
    ) -> ExecutorResult<eez_protocol::ExecutionOutcome> {
        let tx_env = self.build_tx_env(destination, calldata, value, source_address, source_rollup);
        let caller = tx_env.caller;
        let pre_root = self.current_root;
        let (return_data, gas_used, success, mut changes, inspector_error) = {
            let mut evm = self.evm_config.evm_with_env_and_inspector(
                &mut self.state,
                self.evm_env.clone(),
                inspector,
            );
            let result = evm.transact(tx_env).map_err(evm_err)?;
            let inspector_error = evm.inspector_mut().take_error();
            (
                result.result.output().cloned().unwrap_or_default(),
                result.result.tx_gas_used(),
                result.result.is_success(),
                result.state,
                inspector_error,
            )
        };
        if let Some(err) = inspector_error {
            return Err(err);
        }
        restore_caller_nonce(&mut changes, caller);
        self.commit_and_finish(pre_root, return_data, gas_used, success, changes)
    }

    /// Shared post-commit bookkeeping: commit the revm bundle, compute
    /// the post-state root, advance `current_root`, log, and emit an
    /// [`ExecutionOutcome`](eez_protocol::ExecutionOutcome).
    fn commit_and_finish(
        &mut self,
        pre_root: B256,
        return_data: Bytes,
        gas_used: u64,
        success: bool,
        changes: revm::primitives::map::AddressHashMap<revm::state::Account>,
    ) -> ExecutorResult<eez_protocol::ExecutionOutcome> {
        self.state.commit(changes);

        let post_root = compute_state_root(&mut self.state, self.state_provider.as_ref())?;
        self.current_root = post_root;

        // Overlay write-back: when this session is the entry rollup's
        // overlay session (channel handle installed at construction),
        // publish the post-execute cache for the source-sim inspector
        // to diff-apply onto source's journal after the dispatch
        // returns. The cache here is the cumulative state after every
        // overlay call so far, since multiple overlay executes within
        // one source-sim hook reuse the same session.
        //
        // Also append `post_root` to the channel's per-tx roots —
        // nested entries attributed to the entry rollup pull
        // `newState` from this list during `build_batch`. Order is
        // chronological dispatch order, matching the cursor
        // `build_batch` walks for the entry rollup.
        //
        // Stack-based push: the inspector at the dispatching frame
        // pops the top after the dispatch returns and applies the
        // diff. Stack semantics let nested re-entries chain
        // post-caches through their respective inspector pops without
        // collision.
        if let Some(channel) = &self.overlay_channel {
            channel.push_post_cache(self.state.cache.clone());
            channel.append_post_root(post_root.0);
        }

        tracing::debug!(
            success = success,
            gas = gas_used,
            pre = %pre_root,
            post = %post_root,
            return_len = return_data.len(),
            "target call completed");

        Ok(eez_protocol::ExecutionOutcome::Resolved {
            return_data: return_data.to_vec(),
            pre_state_root: pre_root.0,
            post_state_root: post_root.0,
            gas_used,
            success,
        })
    }

    /// Build the `TxEnv` for a direct call on the target chain. Shared
    /// between inspected and uninspected paths so the revm input shape
    /// is identical regardless of inspection policy.
    fn build_tx_env(
        &mut self,
        destination: &Address,
        calldata: &Bytes,
        value: &U256,
        source_address: &Address,
        source_rollup: RollupId,
    ) -> revm::context::TxEnv {
        let caller = self
            .compute_proxy_address(*source_address, source_rollup)
            .unwrap_or(Address::ZERO);

        tracing::trace!(
            dest = %destination,
            proxy_caller = %caller,
            source_addr = %source_address,
            %source_rollup,
            value = %value,
            calldata_len = calldata.len(),
            "executing direct call on target chain");

        revm::context::TxEnv {
            caller,
            gas_limit: DIRECT_CALL_GAS_LIMIT,
            kind: alloy_primitives::TxKind::Call(*destination),
            data: calldata.clone(),
            value: *value,
            chain_id: Some(self.chain_id),
            ..Default::default()
        }
    }
}

impl TargetExecutionSession for LocalExecutionSession {
    fn execute(
        &mut self,
        req: ExecutionRequest,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionResponse> {
        let outcome = if let Some(factory) = self.inspector_factory.clone() {
            let inspector = factory.build(dispatcher);
            self.execute_internal_with_inspector(
                inspector,
                &req.target_address,
                &req.data,
                &req.value,
                &req.source_address,
                req.source_rollup_id,
            )?
        } else {
            let _ = dispatcher;
            self.execute_internal(
                &req.target_address,
                &req.data,
                &req.value,
                &req.source_address,
                req.source_rollup_id,
            )?
        };

        let pre = outcome.pre_state_root().copied().unwrap_or([0u8; 32]);
        let post = outcome.post_state_root().copied().unwrap_or([0u8; 32]);
        let checkpoint = eez_protocol::ExecutionCheckpoint {
            version: 1,
            chain_id: self.chain_id,
            base_block_number: 0,
            base_block_hash: [0u8; 32],
            base_state_root: pre,
            current_root: post,
            overlay: eez_protocol::EvmOverlay::default(),
            witness: None,
        };

        Ok(ExecutionResponse {
            outcome,
            checkpoint,
        })
    }

    fn checkpoint(&mut self) -> ExecutorResult<eez_protocol::SessionSnapshot> {
        // Opaque snapshot carrying the current root only. The full
        // revm `State<DB>` deep-clone is tracked as known debt — see
        // CLAUDE.md "Known limitations".
        Ok(Box::new(self.current_root.0) as Box<dyn std::any::Any + Send>)
    }

    fn rollback(&mut self, snapshot: eez_protocol::SessionSnapshot) -> ExecutorResult<()> {
        let root: [u8; 32] = *snapshot.downcast::<[u8; 32]>().map_err(|_e| {
            ExecutorError::from(ExecutorErrorKind::Encoding(
                "LocalExecutionSession::rollback: snapshot type mismatch".into(),
            ))
        })?;
        self.current_root = revm::primitives::B256::from(root);
        Ok(())
    }

    /// Placeholder checkpoint until overlay/witness recording is implemented.
    fn take_checkpoint(&mut self) -> Option<eez_protocol::ExecutionCheckpoint> {
        Some(eez_protocol::ExecutionCheckpoint {
            version: 1,
            chain_id: self.chain_id,
            base_block_number: 0,
            base_block_hash: [0u8; 32],
            base_state_root: [0u8; 32],
            current_root: self.current_root.0,
            overlay: eez_protocol::EvmOverlay::default(),
            witness: None,
        })
    }
}

/// Batch-simulate CCM-verify transactions (`loadExecutionTable` +
/// `executeIncomingCrossChainCall`) on a fresh target state.
pub(super) fn simulate_local_transactions(
    target: &ChainProvider,
    txs: &[TargetTransaction],
) -> ExecutorResult<TargetBatchSimulation> {
    tracing::debug!(batch_size = txs.len(), "simulating target tx batch");
    if txs.is_empty() {
        return Err(ExecutorErrorKind::EmptyBatch.into());
    }
    let (_target_header, target_state, mut evm_env) = open_chain_state(target)?;
    disable_checks(&mut evm_env);

    let db = StateProviderDatabase::new(target_state.as_ref() as &dyn StateProvider);
    let mut state = State::builder()
        .with_database(db)
        .with_bundle_update()
        .build();
    let chain_id = evm_env.cfg_env.chain_id;

    let mut per_tx_roots: Vec<[u8; 32]> = Vec::with_capacity(txs.len());

    for (index, tx) in txs.iter().enumerate() {
        let tx_env = revm::context::TxEnv {
            caller: tx.caller,
            gas_limit: tx.gas_limit,
            kind: alloy_primitives::TxKind::Call(tx.destination),
            data: tx.calldata.clone(),
            value: tx.value,
            chain_id: Some(chain_id),
            ..Default::default()
        };

        let mut evm = target.evm_config.evm_with_env(&mut state, evm_env.clone());
        let result = evm.transact(tx_env).map_err(evm_err)?;
        if !result.result.is_success() {
            let return_data = result
                .result
                .output()
                .map(|bytes| bytes.to_vec())
                .unwrap_or_default();
            return Err(ExecutorErrorKind::TargetTransactionReverted { index, return_data }.into());
        }

        tracing::trace!(
            tx = index,
            gas = result.result.tx_gas_used(),
            "target batch tx simulated"
        );
        state.commit(result.state);

        // Capture cumulative post-state root after this tx. revm's
        // `merge_transitions` is idempotent under an empty pending
        // queue (no-op) and folds this commit's changes into the bundle
        // on first call; the resulting hashed post-state covers every
        // tx committed so far. See `states/state.rs::merge_transitions`
        // in revm-database.
        let root = compute_state_root(&mut state, target_state.as_ref() as &dyn StateProvider)?;
        per_tx_roots.push(root.0);
    }

    // `per_tx_roots` is non-empty here: we returned early on `txs.is_empty`
    // and every successful tx pushes exactly one root.
    let final_state_root = *per_tx_roots
        .last()
        .expect("per_tx_roots is non-empty after the non-empty-batch guard above");

    tracing::debug!(
        final_state_root = ?final_state_root,
        txs = txs.len(),
        "target batch simulation complete");

    Ok(TargetBatchSimulation {
        final_state_root,
        per_tx_roots,
    })
}

pub(super) fn open_chain_state(
    chain: &ChainProvider,
) -> ExecutorResult<(
    alloy_consensus::Header,
    reth_storage_api::StateProviderBox,
    reth_evm::EvmEnvFor<EthEvmConfig>,
)> {
    let num = chain.provider.best_block_number().map_err(provider_err)?;
    let header = chain
        .headers
        .header_by_number(num)
        .map_err(provider_err)?
        .ok_or_else(|| ExecutorError::provider("no header"))?;
    let state = chain.provider.latest().map_err(provider_err)?;
    let evm_env = chain.evm_config.evm_env(&header).map_err(evm_err)?;
    Ok((header, state, evm_env))
}

/// Restore the caller's nonce in `changes` to its pre-tx (`original_info`)
/// value.
///
/// The session calls each cross-chain target with
/// `msg.sender = compute_proxy_address(source_address, source_rollup)` —
/// a deterministic CREATE2 slot that the protocol later deploys via
/// `createCrossChainProxy`. revm's tx execution increments the caller's
/// nonce by 1 (real-chain semantics for EOA senders), but in our session
/// the caller is a CONTRACT slot, not an EOA. Bumping its nonce makes
/// the address fail EIP-684's "code or nonce non-zero" collision check
/// when a downstream `Bridge.bridgeTokens` later tries to CREATE2 a real
/// `CrossChainProxy` at the same slot — burning ~28M gas and reverting
/// the whole session with empty data.
///
/// Restoring `info.nonce` to `original_info.nonce` (the disk value
/// before the tx) keeps the slot fresh for the deterministic CREATE2.
/// The real chain's flow handles this via
/// `CCM.{_processCallAtScope,executeIncomingCrossChainCall}` which
/// CREATE2-deploys the proxy BEFORE forwarding the call; our direct-
/// call session shortcut needs the equivalent invariant restored
/// post-hoc.
fn restore_caller_nonce(
    changes: &mut revm::primitives::map::AddressHashMap<revm::state::Account>,
    caller: Address,
) {
    if let Some(account) = changes.get_mut(&caller) {
        account.info.nonce = account.original_info.nonce;
    }
}

pub(super) fn disable_checks(env: &mut reth_evm::EvmEnvFor<EthEvmConfig>) {
    env.cfg_env.disable_base_fee = true;
    env.cfg_env.disable_balance_check = true;
    env.cfg_env.disable_nonce_check = true;
    env.cfg_env.disable_eip3607 = true;
    env.cfg_env.disable_block_gas_limit = true;
    env.cfg_env.tx_gas_limit_cap = Some(u64::MAX);
}

pub(super) fn compute_state_root<DB: StateProvider>(
    state: &mut State<StateProviderDatabase<DB>>,
    provider: &dyn StateProvider,
) -> ExecutorResult<B256> {
    state.merge_transitions(revm::database::states::bundle_state::BundleRetention::Reverts);
    let hashed = HashedPostState::from_bundle_state::<KeccakKeyHasher>(state.bundle_state.state());
    provider.state_root(hashed).map_err(provider_err)
}

pub(super) fn provider_err(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> ExecutorError {
    ExecutorError::provider(e)
}

pub(super) fn evm_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> ExecutorError {
    ExecutorError::evm(e)
}
