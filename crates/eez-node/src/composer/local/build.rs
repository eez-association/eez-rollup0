//! Build a Sync block from cross-chain system transactions.
//!
//! Manual block-construction path — bypasses reth's payload builder
//! because Sync blocks carry only cross-chain system txs (Rollup-1 §5),
//! never pool txs. Uses the same reth-evm `BlockBuilder` machinery the
//! Deriver uses to replay L1-derived blocks
//! (`crate::deriver::Deriver::execute_block`).

use crate::driver::{BUILDER_EXTRA_DATA, BUILDER_GAS_LIMIT};
use alloy_consensus::{BlockHeader, Header, Transaction};
use alloy_eips::Decodable2718;
use alloy_primitives::{Address, B256, Bytes};
use alloy_rpc_types_engine::ExecutionData;
use reth_chainspec::EthereumHardforks;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_ethereum_primitives::{Block, TransactionSigned};
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes, execute::BlockBuilder};
use reth_evm_ethereum::EthEvmConfig;
use reth_payload_primitives::PayloadTypes;
use reth_primitives_traits::{RecoveredBlock, SealedHeader, SignedTransaction, SignerRecoverable};
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::StateProviderFactory;
use revm::database::State;
use thiserror::Error;

/// Errors raised by [`build_sync_block`].
#[derive(Debug, Error)]
pub enum BuildError {
    /// Provider lookup (state, header) failed.
    #[error("provider: {0}")]
    Provider(String),
    /// EIP-2718 decode failed for the held tx at `idx`.
    #[error("decode tx #{idx}: {msg}")]
    DecodeTx { idx: usize, msg: String },
    /// Signer recovery failed for the held tx at `idx`.
    #[error("recover signer for tx #{idx}")]
    RecoverSigner { idx: usize },
    /// Revm rejected the tx at execution time (gas, balance, …).
    #[error("execute tx #{idx}: {msg}")]
    ExecuteTx { idx: usize, msg: String },
    /// `reth-evm` block-builder primitive surfaced an error during
    /// setup, pre-execution changes, or `finish`.
    #[error("block builder: {0}")]
    Builder(String),
}

/// Built Sync-block artifact ready for [`commit_derived`].
///
/// [`commit_derived`]: crate::driver::BlockCommitterHandle::commit_derived
#[derive(Debug)]
pub struct BuiltSyncBlock {
    /// Engine-API payload — pass to `commit_derived`.
    pub payload: ExecutionData,
    /// Sealed header of the new block (cursor mirror in the committer).
    pub header: SealedHeader<Header>,
    /// The recovered Sync block. Remote-prover mode captures the endpoint witness
    /// from here, since the block isn't committed yet (no store can serve it).
    pub block: RecoveredBlock<Block>,
}

/// Build a Sync block on top of `parent`, executing `system_txs` (raw
/// EIP-2718 bytes from the per-rollup `HeldPool`) in order.
///
/// The returned [`BuiltSyncBlock`] is committed via
/// [`BlockCommitterHandle::commit_derived`] — same engine-API tail
/// the Deriver uses for L1-derived blocks (single source of truth for
/// `newPayload` + head-FCU).
///
/// # Errors
///
/// See [`BuildError`].
///
/// [`BlockCommitterHandle::commit_derived`]: crate::driver::BlockCommitterHandle::commit_derived
pub fn build_sync_block<P>(
    l2_provider: &P,
    evm_config: &EthEvmConfig,
    parent: &SealedHeader<Header>,
    timestamp: u64,
    suggested_fee_recipient: Address,
    system_txs: &[Bytes],
) -> Result<BuiltSyncBlock, BuildError>
where
    P: StateProviderFactory,
{
    let parent_hash = parent.hash();
    let state_provider = l2_provider
        .state_by_block_hash(parent_hash)
        .map_err(|e| BuildError::Provider(format!("state_by_block_hash({parent_hash}): {e}")))?;
    let state_db = StateProviderDatabase::new(state_provider.as_ref());
    let mut db = State::builder()
        .with_database(state_db)
        .with_bundle_update()
        .build();

    // `prev_randao` is L1-derived (protocol §13.14); left zero here to
    // match the Deriver's path. Hardfork-gated fields
    // (parent_beacon_block_root, withdrawals) and `extra_data` /
    // `gas_limit` mirror the Deriver's `execute_block` attrs so all three
    // STF paths (sequencer pool, deriver replay, sync-slot composer)
    // produce identical headers — otherwise `check_claimed_state`
    // diverges where Cancun/Shanghai isn't active at genesis.
    let chain_spec = evm_config.chain_spec();
    let attributes = NextBlockEnvAttributes {
        timestamp,
        suggested_fee_recipient,
        prev_randao: B256::ZERO,
        gas_limit: BUILDER_GAS_LIMIT,
        parent_beacon_block_root: chain_spec
            .is_cancun_active_at_timestamp(timestamp)
            .then_some(B256::ZERO),
        withdrawals: chain_spec
            .is_shanghai_active_at_timestamp(timestamp)
            .then(alloy_eips::eip4895::Withdrawals::default),
        extra_data: Bytes::from_static(BUILDER_EXTRA_DATA),
        slot_number: None,
    };

    let mut builder = evm_config
        .builder_for_next_block(&mut db, parent, attributes)
        .map_err(|e| BuildError::Builder(format!("builder_for_next_block: {e}")))?;

    builder
        .apply_pre_execution_changes()
        .map_err(|e| BuildError::Builder(format!("apply_pre_execution_changes: {e}")))?;

    for (idx, tx_bytes) in system_txs.iter().enumerate() {
        let tx = TransactionSigned::decode_2718(&mut tx_bytes.as_ref()).map_err(|e| {
            BuildError::DecodeTx {
                idx,
                msg: e.to_string(),
            }
        })?;
        let recovered = SignedTransaction::try_into_recovered(tx)
            .map_err(|_| BuildError::RecoverSigner { idx })?;
        builder
            .execute_transaction(recovered)
            .map_err(|e| BuildError::ExecuteTx {
                idx,
                msg: e.to_string(),
            })?;
    }

    let outcome = builder
        .finish(state_provider.as_ref(), None)
        .map_err(|e| BuildError::Builder(format!("finish: {e}")))?;
    let block = outcome.block;
    let sealed_block = block.sealed_block().clone();
    let header = sealed_block.sealed_header().clone();
    let payload = <EthEngineTypes as PayloadTypes>::block_to_payload(sealed_block, None);
    Ok(BuiltSyncBlock {
        payload,
        header,
        block,
    })
}

/// Per-effect intermediate L2 state roots — the root after each cross-chain
/// effect's tx group (its pair-end), in tx order, one per effect.
///
/// The prover requires each settlement entry's `newState` to equal its effect's
/// root (not the final Sync-block root), so the composer fills them with these.
/// Computed by rebuilding the Sync block on each pair-end prefix of `sync_txs`;
/// since our L2 blocks are state no-ops past the effects, a prefix block's root
/// equals the full block's root at that tx. Settlement path only.
///
/// # Errors
///
/// See [`BuildError`]. A sync tx that fails to decode is treated as a non-system
/// tx (pair-end), matching the prover's fail-safe flagging.
pub fn sync_block_pair_roots<P>(
    l2_provider: &P,
    evm_config: &EthEvmConfig,
    parent: &SealedHeader<Header>,
    timestamp: u64,
    suggested_fee_recipient: Address,
    sync_txs: &[Bytes],
    ccm_l2_address: Address,
) -> Result<Vec<B256>, BuildError>
where
    P: StateProviderFactory,
{
    // Per-tx SYSTEM flags — must match the prover's `system_tx_flags_from_rlp`
    // (signer == SYSTEM_ADDRESS && to == CCM-L2) so pair-ends agree on both sides.
    let flags: Vec<bool> = sync_txs
        .iter()
        .map(|raw| {
            let Ok(tx) = TransactionSigned::decode_2718(&mut raw.as_ref()) else {
                return false;
            };
            let to = tx.to();
            tx.recover_signer()
                .is_ok_and(|s| eez_protocol::settlement::is_system_tx(s, to, ccm_l2_address))
        })
        .collect();

    eez_protocol::settlement::pair_end_positions(&flags)
        .into_iter()
        .map(|p| {
            build_sync_block(
                l2_provider,
                evm_config,
                parent,
                timestamp,
                suggested_fee_recipient,
                &sync_txs[..=p],
            )
            .map(|prefix| prefix.header.state_root())
        })
        .collect()
}
