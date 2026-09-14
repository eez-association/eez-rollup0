//! Payload construction adapted from reth-ethereum-payload-builder at fd59fd22
//! (MIT/Apache-2.0). The pinned upstream builder fixes Ethereum primitives; this
//! adapter uses EEZ types and excludes blob transactions from L2 payloads.

use alloy_consensus::{Transaction, transaction::TxHashRef};
use alloy_primitives::{Bytes, U256};
use alloy_rlp::Encodable;
use alloy_rpc_types_engine::PayloadAttributes as EthPayloadAttributes;
use eez_evm::EezEvmConfig;
use eez_primitives::engine::EezBuiltPayload;
use eez_primitives::{EezPrimitives, EezTxEnvelope};
use reth_basic_payload_builder::{
    BuildArguments, BuildOutcome, MissingPayloadBehaviour, PayloadBuilder, PayloadConfig,
    is_better_payload,
};
use reth_chainspec::{ChainSpecProvider, EthereumHardforks};
use reth_consensus_common::validation::MAX_RLP_BLOCK_SIZE;
use reth_errors::{BlockExecutionError, BlockValidationError, ConsensusError};
use reth_evm::{
    ConfigureEvm, Evm, NextBlockEnvAttributes,
    block::TxResult,
    execute::{BlockBuilder, BlockBuilderOutcome, BlockExecutor},
};
use reth_execution_cache::{CachedStateMetrics, CachedStateMetricsSource, CachedStateProvider};
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_payload_primitives::PayloadAttributes;
use reth_primitives_traits::transaction::error::InvalidTransactionError;
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_storage_api::StateProviderFactory;
use reth_transaction_pool::{
    BestTransactions, BestTransactionsAttributes, PoolTransaction, TransactionPool,
    ValidPoolTransaction, error::InvalidPoolTransactionError,
};
use revm::context_interface::{Block as _, Cfg as _};
use std::sync::Arc;
use tracing::{debug, trace, warn};

use reth_ethereum_payload_builder::EthereumBuilderConfig;

type BestTransactionsIter<Tx> = Box<dyn BestTransactions<Item = Arc<ValidPoolTransaction<Tx>>>>;

/// Live L2 payload builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePayloadBuilder<Pool, Client, EvmConfig = EezEvmConfig> {
    client: Client,
    pool: Pool,
    evm_config: EvmConfig,
    builder_config: EthereumBuilderConfig,
}

impl<Pool, Client, EvmConfig> NativePayloadBuilder<Pool, Client, EvmConfig> {
    pub const fn new(
        client: Client,
        pool: Pool,
        evm_config: EvmConfig,
        builder_config: EthereumBuilderConfig,
    ) -> Self {
        Self {
            client,
            pool,
            evm_config,
            builder_config,
        }
    }
}

impl<Pool, Client, EvmConfig> PayloadBuilder for NativePayloadBuilder<Pool, Client, EvmConfig>
where
    EvmConfig: ConfigureEvm<Primitives = EezPrimitives, NextBlockEnvCtx = NextBlockEnvAttributes>,
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec: EthereumHardforks> + Clone,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = EezTxEnvelope>>,
{
    type Attributes = EthPayloadAttributes;
    type BuiltPayload = EezBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<EthPayloadAttributes, EezBuiltPayload>,
    ) -> Result<BuildOutcome<EezBuiltPayload>, PayloadBuilderError> {
        build_payload(
            self.evm_config.clone(),
            self.client.clone(),
            self.builder_config.clone(),
            args,
            |attributes| self.pool.best_transactions_with_attributes(attributes),
        )
    }

    fn on_missing_payload(
        &self,
        _args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
    ) -> MissingPayloadBehaviour<Self::BuiltPayload> {
        if self.builder_config.await_payload_on_missing {
            MissingPayloadBehaviour::AwaitInProgress
        } else {
            MissingPayloadBehaviour::RaceEmptyPayload
        }
    }

    fn build_empty_payload(
        &self,
        config: PayloadConfig<Self::Attributes>,
    ) -> Result<EezBuiltPayload, PayloadBuilderError> {
        let args = BuildArguments::new(
            Default::default(),
            Default::default(),
            None,
            config,
            Default::default(),
            None,
        );

        build_payload(
            self.evm_config.clone(),
            self.client.clone(),
            self.builder_config.clone(),
            args,
            |_| -> BestTransactionsIter<Pool::Transaction> { Box::new(std::iter::empty()) },
        )?
        .into_payload()
        .ok_or_else(|| PayloadBuilderError::MissingPayload)
    }
}

/// Builds a Live L2 payload from the best non-blob transactions in the pool.
/// Copied from upstream `reth-ethereum-payload-builder` at `fd59fd22`, with
/// EEZ types substituted and blob transaction handling removed.
#[inline]
pub fn build_payload<EvmConfig, Client, Tx, F>(
    evm_config: EvmConfig,
    client: Client,
    builder_config: EthereumBuilderConfig,
    args: BuildArguments<EthPayloadAttributes, EezBuiltPayload>,
    best_txs: F,
) -> Result<BuildOutcome<EezBuiltPayload>, PayloadBuilderError>
where
    EvmConfig: ConfigureEvm<Primitives = EezPrimitives, NextBlockEnvCtx = NextBlockEnvAttributes>,
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec: EthereumHardforks>,
    Tx: PoolTransaction<Consensus = EezTxEnvelope>,
    F: FnOnce(BestTransactionsAttributes) -> BestTransactionsIter<Tx>,
{
    let BuildArguments {
        mut cached_reads,
        execution_cache,
        trie_handle,
        config,
        cancel,
        best_payload,
    } = args;
    let PayloadConfig {
        parent_header,
        attributes,
        payload_id,
    } = config;

    let mut state_provider = client.state_by_block_hash(parent_header.hash())?;
    if let Some(execution_cache) = execution_cache {
        state_provider = Box::new(CachedStateProvider::new(
            state_provider,
            execution_cache.cache().clone(),
            CachedStateMetrics::zeroed(CachedStateMetricsSource::Builder),
        ));
    }
    let state = StateProviderDatabase::new(state_provider.as_ref());
    let chain_spec = client.chain_spec();
    let is_amsterdam = chain_spec.is_amsterdam_active_at_timestamp(attributes.timestamp());
    let mut db = State::builder()
        .with_database(cached_reads.as_db_mut(state))
        .with_bundle_update()
        .with_bal_builder_if(is_amsterdam)
        .build();

    let mut builder = evm_config
        .builder_for_next_block(
            &mut db,
            &parent_header,
            NextBlockEnvAttributes {
                timestamp: attributes.timestamp(),
                suggested_fee_recipient: attributes.suggested_fee_recipient,
                prev_randao: attributes.prev_randao,
                gas_limit: builder_config.gas_limit(parent_header.gas_limit),
                parent_beacon_block_root: attributes.parent_beacon_block_root(),
                withdrawals: attributes.withdrawals.clone().map(Into::into),
                extra_data: builder_config.extra_data,
                slot_number: attributes.slot_number(),
            },
        )
        .map_err(PayloadBuilderError::other)?;

    debug!(target: "payload_builder", id=%payload_id, parent_header = ?parent_header.hash(), parent_number = parent_header.number, "building new payload");
    let mut cumulative_tx_gas_used = 0;
    let mut block_regular_gas_used = 0;
    let mut block_state_gas_used = 0;
    let block_gas_limit: u64 = builder.evm_mut().block().gas_limit();
    let tx_gas_limit_cap = builder.evm_mut().cfg_env().tx_gas_limit_cap();
    let base_fee = builder.evm_mut().block().basefee();

    let mut best_txs = best_txs(BestTransactionsAttributes::new(base_fee, None));
    best_txs.skip_blobs();
    let mut total_fees = U256::ZERO;

    // Stream state diffs so the trie task can compute the root alongside execution.
    if let Some(ref handle) = trie_handle {
        builder
            .executor_mut()
            .set_state_hook(Some(Box::new(handle.state_hook())));
    }

    builder.apply_pre_execution_changes().map_err(|err| {
        warn!(target: "payload_builder", %err, "failed to apply pre-execution changes");
        PayloadBuilderError::Internal(err.into())
    })?;

    let mut block_transactions_rlp_length = 0;

    let is_osaka = chain_spec.is_osaka_active_at_timestamp(attributes.timestamp);

    let withdrawals_rlp_length = attributes
        .withdrawals
        .as_ref()
        .map(alloy_rlp::Encodable::length)
        .unwrap_or(0);

    while let Some(pool_tx) = best_txs.next() {
        let exceeds_gas_limit = if is_amsterdam {
            let regular_available_gas = block_gas_limit.saturating_sub(block_regular_gas_used);
            let state_available_gas = block_gas_limit.saturating_sub(block_state_gas_used);
            let regular_tx_gas_limit = pool_tx.gas_limit().min(tx_gas_limit_cap);

            if regular_tx_gas_limit > regular_available_gas {
                Some((regular_tx_gas_limit, regular_available_gas))
            } else if pool_tx.gas_limit() > state_available_gas {
                Some((pool_tx.gas_limit(), state_available_gas))
            } else {
                None
            }
        } else {
            let block_available_gas = block_gas_limit.saturating_sub(cumulative_tx_gas_used);
            (pool_tx.gas_limit() > block_available_gas)
                .then_some((pool_tx.gas_limit(), block_available_gas))
        };

        if let Some((transaction_gas_limit, block_available_gas)) = exceeds_gas_limit {
            // Skipping this nonce also excludes dependent transactions from the candidate.
            best_txs.mark_invalid(
                &pool_tx,
                InvalidPoolTransactionError::ExceedsGasLimit(
                    transaction_gas_limit,
                    block_available_gas,
                ),
            );
            continue;
        }

        if cancel.is_cancelled() {
            return Ok(BuildOutcome::Cancelled);
        }

        let tx = pool_tx.to_consensus();

        let tx_rlp_len = tx.inner().length();

        let estimated_block_size_with_tx =
            block_transactions_rlp_length + tx_rlp_len + withdrawals_rlp_length + 1024; // 1Kb of overhead for the block header

        if is_osaka && estimated_block_size_with_tx > MAX_RLP_BLOCK_SIZE {
            best_txs.mark_invalid(
                &pool_tx,
                InvalidPoolTransactionError::OversizedData {
                    size: estimated_block_size_with_tx,
                    limit: MAX_RLP_BLOCK_SIZE,
                },
            );
            continue;
        }

        let miner_fee = tx.effective_tip_per_gas(base_fee);
        let tx_hash = *tx.tx_hash();

        let mut tx_regular_gas_used = 0;
        let gas_output = match builder.execute_transaction_with_result_closure(tx, |result| {
            tx_regular_gas_used = result.result().result.gas().block_regular_gas_used();
        }) {
            Ok(gas_output) => gas_output,
            Err(BlockExecutionError::Validation(BlockValidationError::InvalidTx {
                error, ..
            })) => {
                if error.is_nonce_too_low() {
                    trace!(target: "payload_builder", %error, ?tx_hash, "skipping nonce too low transaction");
                } else {
                    trace!(target: "payload_builder", %error, ?tx_hash, "skipping invalid transaction and its descendants");
                    best_txs.mark_invalid(
                        &pool_tx,
                        InvalidPoolTransactionError::Consensus(
                            InvalidTransactionError::TxTypeNotSupported,
                        ),
                    );
                }
                continue;
            }
            // The executor is the source of truth for block gas availability. Keep this
            // non-fatal in case local builder accounting diverges from executor rules.
            Err(BlockExecutionError::Validation(
                BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                    transaction_gas_limit,
                    block_available_gas,
                },
            )) => {
                trace!(target: "payload_builder", %transaction_gas_limit, %block_available_gas, ?tx_hash, "skipping transaction exceeding block gas limit");
                best_txs.mark_invalid(
                    &pool_tx,
                    InvalidPoolTransactionError::ExceedsGasLimit(
                        transaction_gas_limit,
                        block_available_gas,
                    ),
                );
                continue;
            }
            Err(err) => return Err(PayloadBuilderError::evm(err)),
        };

        block_transactions_rlp_length += tx_rlp_len;

        let gas_used = gas_output.tx_gas_used();
        let miner_fee = miner_fee.expect("fee is always valid; execution succeeded");
        total_fees += U256::from(miner_fee) * U256::from(gas_used);
        cumulative_tx_gas_used += gas_used;
        block_regular_gas_used += tx_regular_gas_used;
        block_state_gas_used += gas_output.state_gas_used();
    }

    if !is_better_payload(best_payload.as_ref(), total_fees) {
        drop(builder);
        return Ok(BuildOutcome::Aborted {
            fees: total_fees,
            cached_reads,
        });
    }

    let BlockBuilderOutcome {
        execution_result,
        block,
        block_access_list,
        ..
    } = if let Some(mut handle) = trie_handle {
        // Dropping the hook signals the trie task to finalize before we wait for its root.
        builder.executor_mut().set_state_hook(None);

        match handle.state_root() {
            Ok(outcome) => {
                debug!(target: "payload_builder", id=%payload_id, state_root=?outcome.state_root, "received state root from sparse trie");
                builder.finish(
                    state_provider.as_ref(),
                    Some((
                        outcome.state_root,
                        Arc::unwrap_or_clone(outcome.trie_updates),
                    )),
                )?
            }
            Err(err) => {
                warn!(target: "payload_builder", id=%payload_id, %err, "sparse trie failed, falling back to sync state root");
                builder.finish(state_provider.as_ref(), None)?
            }
        }
    } else {
        builder.finish(state_provider.as_ref(), None)?
    };

    let requests = chain_spec
        .is_prague_active_at_timestamp(attributes.timestamp)
        .then_some(execution_result.requests);

    let sealed_block = Arc::new(block.into_sealed_block());
    debug!(target: "payload_builder", id=%payload_id, sealed_block_header = ?sealed_block.sealed_header(), "sealed built block");

    if is_osaka && sealed_block.rlp_length() > MAX_RLP_BLOCK_SIZE {
        return Err(PayloadBuilderError::other(ConsensusError::BlockTooLarge {
            rlp_length: sealed_block.rlp_length(),
            max_rlp_length: MAX_RLP_BLOCK_SIZE,
        }));
    }

    let block_access_list: Option<Bytes> =
        block_access_list.map(|block_access_list| alloy_rlp::encode(&block_access_list).into());
    let payload = EezBuiltPayload::new(sealed_block, total_fees, requests, block_access_list);

    Ok(BuildOutcome::Better {
        payload,
        cached_reads,
    })
}
