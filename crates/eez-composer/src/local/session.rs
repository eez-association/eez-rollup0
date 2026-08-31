//! Stateful per-source-tx target-chain execution session.
//!
//! [`LocalExecutionSession`] accumulates target-chain state across
//! calls within one source transaction. It drives a direct call to the
//! destination contract with the proxy address as `msg.sender`
//! (CREATE2-derived).
//!
//! Also hosts the reth helper [`disable_checks`].

use alloy_primitives::{Address, Bytes, U256};

use eez_evm_inspector::{OverlayChannelHandle, SessionInspectorFactory};
use reth_evm::{ConfigureEvm, Evm as _};
use reth_evm_ethereum::EthEvmConfig;
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_storage_api::{BlockNumReader, StateProviderFactory};
use revm::DatabaseCommit;
use revm::database::CacheState;

use eez_protocol::{
    CallMode, CompositionBuilder, ExecutionOutcome, ExecutionRequest, ExecutorError,
    ExecutorErrorKind, ExecutorResult, RollupId, TargetExecutionSession,
};

use super::provider::ChainProvider;
use super::reset_frame_caller_nonce;

/// Gas cap for simulated direct target calls; exhaustion is returned as an
/// unsuccessful execution outcome.
pub(super) const DIRECT_CALL_GAS_LIMIT: u64 = 30_000_000;

/// Stateful target-chain execution session.
///
/// Opened by `LocalChainClient::begin_execution_session` (the
/// `ChainClient` trait method, not an inherent fn); owned by the
/// `CompositionBuilder` through a `Rollup.session` slot for the
/// lifetime of one composition.
///
/// ## Limitation: direct call, not full manager path
///
/// The session calls the destination contract directly with the proxy
/// address as `msg.sender` (computed via CREATE2). This gives the
/// source simulation synchronous return data, but it does not reproduce
/// the full `executeIncomingCrossChainCall` path.
pub struct LocalExecutionSession {
    evm_config: EthEvmConfig,
    state: State<StateProviderDatabase<reth_storage_api::StateProviderBox>>,
    evm_env: reth_evm::EvmEnvFor<EthEvmConfig>,
    chain_id: u64,
    manager_address: Address,
    /// Optional factory for inspecting nested proxy calls. `None` disables
    /// nested-call detection for this session.
    inspector_factory: Option<SessionInspectorFactory>,
    /// This rollup's cache channel for propagating state through re-entry.
    overlay_channel: OverlayChannelHandle,
}

impl std::fmt::Debug for LocalExecutionSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExecutionSession")
            .field("chain_id", &self.chain_id)
            .field("manager_address", &self.manager_address)
            .field("has_inspector_factory", &self.inspector_factory.is_some())
            .finish_non_exhaustive()
    }
}

impl LocalExecutionSession {
    /// Create from a `ChainProvider`. Opens latest state.
    ///
    /// When supplied, `inspector_factory` detects nested proxy calls and routes
    /// them through the composition builder. `None` executes without nested
    /// call inspection.
    ///
    /// `cache` preloads in-flight state captured before a nested dispatch;
    /// `None` opens a fresh revm state. `overlay_channel` publishes cumulative
    /// state after execution so a suspended inspector can apply the diff.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Provider`] if reading the latest
    /// block number or header fails, or if opening the state provider
    /// fails.
    pub fn new(
        provider: &ChainProvider,
        manager_address: Address,
        inspector_factory: Option<SessionInspectorFactory>,
        cache: Option<CacheState>,
        overlay_channel: OverlayChannelHandle,
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
        tracing::debug!(
            block = num,
            state_root = %header.state_root,
            "target session: opened header"
        );

        let state_prov = provider.provider.latest().map_err(provider_err)?;

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
            %manager_address,
            "target session: ready"
        );

        Ok(Self {
            evm_config: provider.evm_config.clone(),
            state,
            evm_env,
            chain_id,
            manager_address,
            inspector_factory,
            overlay_channel,
        })
    }

    /// Compute the target-chain proxy address for a given
    /// `(sourceAddress, sourceRollup)` via a static call to
    /// the chain-local manager's `computeCrossChainProxyAddress()`.
    fn compute_proxy_address(
        &mut self,
        source_address: Address,
        source_rollup: RollupId,
    ) -> Option<Address> {
        alloy_sol_types::sol! {
            function computeCrossChainProxyAddress(address originalAddress, uint64 originalRollupId) external view returns (address);
        }
        use alloy_sol_types::SolCall;

        let calldata = computeCrossChainProxyAddressCall {
            originalAddress: source_address,
            originalRollupId: source_rollup.0,
        }
        .abi_encode();

        let tx_env = revm::context::TxEnv {
            caller: Address::ZERO,
            gas_limit: 1_000_000,
            kind: alloy_primitives::TxKind::Call(self.manager_address),
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

    /// Uninspected direct-call path. Executes the call and restores the
    /// simulated proxy caller's nonce before committing the result.
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
        reset_frame_caller_nonce(&mut changes, caller);
        Ok(self.commit_and_finish(return_data, gas_used, success, changes))
    }

    /// Inspected direct-call path. Runs the target-chain tx under the
    /// supplied [`eez_evm_inspector::SessionInspector`] so proxy CALLs detected
    /// during execution dispatch through the composition builder.
    ///
    /// The session takes the inspector by value because reth owns it for the
    /// EVM pass. The caller reads `take_error` before returning the outcome.
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
        reset_frame_caller_nonce(&mut changes, caller);
        Ok(self.commit_and_finish(return_data, gas_used, success, changes))
    }

    /// Commit the revm bundle, publish the overlay cache, and return the
    /// execution outcome.
    fn commit_and_finish(
        &mut self,
        return_data: Bytes,
        gas_used: u64,
        success: bool,
        changes: revm::primitives::map::AddressHashMap<revm::state::Account>,
    ) -> eez_protocol::ExecutionOutcome {
        self.state.commit(changes);

        // Publish the cumulative post-execute cache. An inspector waiting on
        // the same channel can pop and apply it after nested dispatch; the
        // stack preserves LIFO order for recursive re-entry.
        self.overlay_channel
            .push_post_cache(self.state.cache.clone());

        tracing::debug!(
            success = success,
            gas = gas_used,
            return_len = return_data.len(),
            "target call completed"
        );

        eez_protocol::ExecutionOutcome::Resolved {
            return_data: return_data.to_vec(),
            gas_used,
            success,
        }
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
    ) -> ExecutorResult<ExecutionOutcome> {
        if req.call_mode == CallMode::Static {
            return Err(ExecutorErrorKind::Unavailable(
                "static target execution is not implemented".to_owned(),
            )
            .into());
        }
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

        Ok(outcome)
    }

    fn checkpoint(&mut self) -> ExecutorResult<eez_protocol::SessionSnapshot> {
        // The snapshot restores no session state: the revm cache and bundle
        // are not captured (full-snapshot rollback is planned separately).
        // Annulled-call safety rests on batch materialization rejecting
        // revert spans, so a composition with rolled-back calls never emits
        // entries.
        Ok(Box::new(()) as Box<dyn std::any::Any + Send>)
    }

    fn rollback(&mut self, snapshot: eez_protocol::SessionSnapshot) -> ExecutorResult<()> {
        snapshot.downcast::<()>().map_err(|_e| {
            ExecutorError::from(ExecutorErrorKind::Encoding(
                "LocalExecutionSession::rollback: snapshot type mismatch".into(),
            ))
        })?;
        Ok(())
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

pub(super) fn provider_err(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> ExecutorError {
    ExecutorError::provider(e)
}

pub(super) fn evm_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> ExecutorError {
    ExecutorError::evm(e)
}
