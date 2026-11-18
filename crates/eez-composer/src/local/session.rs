//! Stateful per-source-tx target-chain execution session.
//!
//! [`LocalExecutionSession`] accumulates target-chain state across
//! calls within one source transaction. It drives a direct call to the
//! destination contract with the proxy address as `msg.sender`
//! (CREATE2-derived).

use alloy_primitives::{Address, U256};
use alloy_sol_types::SolCall;

use crate::{OverlayChannelHandle, SessionInspector, SessionInspectorFactory};
use eez_evm::{EezEvmConfig, EezEvmTypes};
use evm2::{
    Inspector, TxResultWithState,
    evm::{Cache, InMemoryDB},
};
use reth_evm::{ConfigureEvm, EvmEnvFor};

use eez_protocol::{
    CallMode, CompositionBuilder, ExecutionOutcome, ExecutionRequest, ExecutorError,
    ExecutorResult, RollupId, TargetExecutionSession, abi::computeCrossChainProxyAddressCall,
};

use super::provider::ChainProvider;
use super::sim::{SimState, disable_checks, reset_frame_caller_nonce, synthetic_call};
use super::slot::transact_err;

/// Gas cap for simulated direct target calls; exhaustion is returned as an
/// unsuccessful execution outcome.
pub(super) const DIRECT_CALL_GAS_LIMIT: u64 = 30_000_000;

/// Gas cap for the manager's `computeCrossChainProxyAddress` view call.
const PROXY_ADDRESS_GAS_LIMIT: u64 = 1_000_000;

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
    evm_config: EezEvmConfig,
    state: SimState,
    evm_env: EvmEnvFor<EezEvmConfig>,
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
    /// `None` opens the latest state as is. `overlay_channel` receives the
    /// writes of every execution so a suspended inspector can replay them.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Provider`] if reading the latest
    /// block number or header fails, or if opening the state provider
    /// fails.
    pub fn new(
        provider: &ChainProvider,
        manager_address: Address,
        inspector_factory: Option<SessionInspectorFactory>,
        cache: Option<Cache>,
        overlay_channel: OverlayChannelHandle,
    ) -> ExecutorResult<Self> {
        let num = provider.headers.best_block_number().map_err(provider_err)?;
        tracing::debug!(block = num, "target session: best block number");

        let header = provider
            .headers
            .header_by_number(num)
            .map_err(provider_err)?
            .ok_or(ExecutorError::Missing("target header"))?;
        tracing::debug!(
            block = num,
            state_root = %header.state_root,
            "target session: opened header"
        );

        let state_prov = provider.provider.latest().map_err(provider_err)?;

        let mut evm_env = provider.evm_config.evm_env(&header).map_err(evm_err)?;
        let chain_id = evm_env.version.chain_id;
        disable_checks(&mut evm_env);

        let state = SimState::new(state_prov, cache.unwrap_or_default());

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
    ) -> ExecutorResult<Address> {
        let calldata = computeCrossChainProxyAddressCall {
            originalAddress: source_address,
            originalRollupId: source_rollup.0,
        }
        .abi_encode();
        let call = synthetic_call(
            self.chain_id,
            Address::ZERO,
            self.manager_address,
            calldata.into(),
            U256::ZERO,
            PROXY_ADDRESS_GAS_LIMIT,
        );

        let result = self
            .state
            .transact(&self.evm_config, &self.evm_env, &call, None)
            .map_err(transact_err)?
            .result;
        if !result.is_success() {
            return Err(ExecutorError::Unavailable(format!(
                "computeCrossChainProxyAddress reverted on manager {}: {:?} {}",
                self.manager_address, result.stop, result.output
            )));
        }
        computeCrossChainProxyAddressCall::abi_decode_returns(&result.output).map_err(|error| {
            ExecutorError::Unavailable(format!(
                "manager {} returned malformed computeCrossChainProxyAddress output: {error}",
                self.manager_address
            ))
        })
    }

    /// Run the direct call from the source's proxy address, inspected when the
    /// session has an inspector factory, then commit it with the proxy's nonce
    /// restored and publish its writes on the overlay channel.
    ///
    /// The caller reads the inspector's error before returning the outcome.
    fn execute_call(
        &mut self,
        req: &ExecutionRequest,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionOutcome> {
        let caller = self.compute_proxy_address(req.source_address, req.source_rollup_id)?;

        tracing::trace!(
            dest = %req.target_address,
            proxy_caller = %caller,
            source_addr = %req.source_address,
            source_rollup = %req.source_rollup_id,
            value = %req.value,
            calldata_len = req.data.len(),
            "executing direct call on target chain");

        let call = synthetic_call(
            self.chain_id,
            caller,
            req.target_address,
            req.data.clone(),
            req.value,
            DIRECT_CALL_GAS_LIMIT,
        );
        let mut inspector = self
            .inspector_factory
            .as_ref()
            .map(|factory| factory.build(dispatcher));
        let outcome = self.state.transact(
            &self.evm_config,
            &self.evm_env,
            &call,
            inspector
                .as_mut()
                .map(|inspector| inspector as &mut dyn Inspector<EezEvmTypes>),
        );
        let inspector_error = inspector.as_mut().and_then(SessionInspector::take_error);
        let TxResultWithState {
            result,
            mut pending_state,
            ..
        } = outcome.map_err(transact_err)?;
        if let Some(error) = inspector_error {
            return Err(error);
        }
        reset_frame_caller_nonce(&mut pending_state, caller);
        self.state.commit(&pending_state);

        // Publish this execution's writes. An inspector suspended in a dispatch
        // that re-entered this rollup replays them on its own state.
        let mut writes = InMemoryDB::default();
        writes.commit_pending(&pending_state);
        self.overlay_channel.push_writes(writes.cache);

        tracing::debug!(
            success = result.is_success(),
            gas = result.tx_gas_used(),
            return_len = result.output.len(),
            "target call completed"
        );

        Ok(ExecutionOutcome::Resolved {
            return_data: result.output.to_vec(),
            gas_used: result.tx_gas_used(),
            success: result.is_success(),
        })
    }
}

impl TargetExecutionSession for LocalExecutionSession {
    fn execute(
        &mut self,
        req: ExecutionRequest,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionOutcome> {
        if req.call_mode == CallMode::Static {
            return Err(ExecutorError::Unavailable(
                "static target execution is not implemented".to_owned(),
            ));
        }
        self.execute_call(&req, dispatcher)
    }

    fn checkpoint(&mut self) -> ExecutorResult<eez_protocol::SessionSnapshot> {
        // The snapshot restores no session state: the simulation cache is
        // not captured (full-snapshot rollback is planned separately).
        // Annulled-call safety rests on batch materialization rejecting
        // revert spans, so a composition with rolled-back calls never emits
        // entries.
        Ok(Box::new(()) as Box<dyn std::any::Any + Send>)
    }

    fn rollback(&mut self, snapshot: eez_protocol::SessionSnapshot) -> ExecutorResult<()> {
        snapshot.downcast::<()>().map_err(|_e| {
            ExecutorError::Encoding(
                "LocalExecutionSession::rollback: snapshot type mismatch".into(),
            )
        })?;
        Ok(())
    }
}

pub(super) fn provider_err(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> ExecutorError {
    ExecutorError::Provider(e.into())
}

pub(super) fn evm_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> ExecutorError {
    ExecutorError::Evm(e.into())
}
