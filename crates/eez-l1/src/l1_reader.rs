//! Read-only canonical L1 access for the Deriver.

use std::sync::Arc;

use alloy_eips::{BlockId, BlockNumberOrTag, RpcBlockHash};
use alloy_primitives::{Address, B256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{Filter, TransactionRequest};
use alloy_sol_types::SolCall;
use eez_protocol::abi::rollupsCall;

use crate::config::L1ReaderConfig;
use crate::error::{L1Error, L1Result};
use crate::scan::{BatchLogChunks, Post, scan_next_batch_log_chunk};

/// Read-only L1 client for historical batch scans and canonicality checks.
/// Cheaply [`Clone`]able.
#[derive(Clone)]
pub struct L1Reader {
    config: Arc<L1ReaderConfig>,
}

/// The rollup's L2 block hash as stored on L1 at its finalized block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedCommitment {
    pub l1_block: u64,
    pub l1_hash: B256,
    /// `None` when the rollup is not registered at `l1_block`.
    pub root: Option<B256>,
}

/// Interprets the registry's `rollups(id)` answer at `l1_block`. A registered
/// rollup with a zero root is a broken registry, not "not registered". Empty
/// registry code at or after its deploy block is a misconfiguration.
fn registered_root(
    registry: Address,
    deploy_block: u64,
    code_is_empty: bool,
    rollup_contract: Address,
    root: B256,
    rollup_id: u64,
    l1_block: u64,
) -> L1Result<Option<B256>> {
    if code_is_empty && l1_block >= deploy_block {
        return Err(L1Error::Config(format!(
            "no EEZ registry code at {registry} at L1 block {l1_block}, at or after its deploy block {deploy_block}; check EEZ_REGISTRY_ADDRESS and EEZ_REGISTRY_DEPLOY_BLOCK"
        )));
    }
    if code_is_empty || rollup_contract.is_zero() {
        return Ok(None);
    }
    if root.is_zero() {
        return Err(L1Error::Config(format!(
            "rollup {rollup_id} is registered at L1 block {l1_block} with root 0"
        )));
    }
    Ok(Some(root))
}

/// What the L1 source can currently serve. See [`L1Reader::readiness`].
#[derive(Debug, Clone, Copy)]
pub struct L1Readiness {
    /// Highest block number the source will serve.
    pub head_block_number: u64,
    /// Advisory: an endpoint that refuses `eth_syncing` reads as not-syncing.
    pub syncing: bool,
}

impl std::fmt::Debug for L1Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L1Reader")
            .field("config", &self.config)
            .finish()
    }
}

impl L1Reader {
    /// Build a read-only L1 client from its config.
    #[must_use]
    pub fn new(config: L1ReaderConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Creates bounded `BatchPosted` log chunks from `from_block` to the
    /// current L1 head. Call [`Self::next_batch_log_chunk`] to consume it.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn batch_log_chunks(&self, from_block: u64) -> L1Result<BatchLogChunks> {
        let provider = self.build_provider();
        let latest = provider
            .get_block_number()
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_number: {e}")))?;
        Ok(BatchLogChunks::new(from_block, latest))
    }

    /// Scans and returns our rollup's `Post`s from the next log chunk, or
    /// `None` when the chunks are exhausted.
    ///
    /// # Errors
    ///
    /// - [`L1Error::Provider`] on RPC failure (log, tx or trace fetch).
    /// - [`L1Error::SourceIncomplete`] when a canonical batch tx is not
    ///   served by the L1 source yet — retryable once the source syncs.
    /// - A terminal error (see [`L1Error::is_terminal`]) when L1 breaks the protocol.
    pub async fn next_batch_log_chunk(
        &self,
        chunks: &mut BatchLogChunks,
    ) -> L1Result<Option<Vec<Post>>> {
        let provider = self.build_provider();
        let trace = self.build_trace_provider();
        scan_next_batch_log_chunk(
            &provider,
            &trace,
            self.config.eez,
            self.config.rollup_id,
            chunks,
        )
        .await
    }

    /// Hash of the canonical L1 block at `number`, or `None` if none. Used by
    /// the Deriver's resync to check whether an indexed batch is still canonical.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn canonical_l1_hash(&self, number: u64) -> L1Result<Option<alloy_primitives::B256>> {
        let provider = self.build_provider();
        Ok(provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_by_number({number}): {e}")))?
            .map(|b| b.header.hash))
    }

    /// Whether the source serves `block`'s header, logs, and tx bodies — the
    /// three things the batch scan reads.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn serves_history(&self, block: u64) -> L1Result<bool> {
        let provider = self.build_provider();
        let header = provider
            .get_block_by_number(BlockNumberOrTag::Number(block))
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_by_number({block}): {e}")))?;
        if header.is_none() {
            return Ok(false);
        }
        let filter = Filter::new()
            .from_block(block)
            .to_block(block)
            .address(self.config.eez);
        // Result discarded: this asks whether eth_getLogs works at all (some
        // endpoints restrict it), not whether the block has events.
        provider
            .get_logs(&filter)
            .await
            .map_err(|e| L1Error::Provider(format!("get_logs probe at {block}: {e}")))?;
        // The scan reads bodies only by (block hash, index), so a source that
        // prunes them passes the probes above and stalls boot later.
        // `None` here just means an empty block.
        let header = header.expect("checked above");
        provider
            .get_transaction_by_block_hash_and_index(header.header.hash, 0)
            .await
            .map_err(|e| L1Error::Provider(format!("tx-by-index probe at {block}: {e}")))?;
        Ok(true)
    }

    /// Chain id the configured L1 RPC serves.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn chain_id(&self) -> L1Result<u64> {
        self.build_provider()
            .get_chain_id()
            .await
            .map_err(|e| L1Error::Provider(format!("eth_chainId: {e}")))
    }

    /// L1 head, and whether the node reports itself syncing.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] when the head is unreadable.
    pub async fn readiness(&self) -> L1Result<L1Readiness> {
        let provider = self.build_provider();
        let head = provider
            .get_block_number()
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_number: {e}")))?;
        let syncing = provider
            .syncing()
            .await
            .is_ok_and(|s| !matches!(s, alloy_rpc_types_eth::SyncStatus::None));
        Ok(L1Readiness {
            head_block_number: head,
            syncing,
        })
    }

    /// The L1 source's finalized block `(number, hash)`, or `None` when the
    /// source reports no finalized block yet (fresh embedded chiado before
    /// the CL's first FCU; dev chains without a CL).
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn finalized_block(&self) -> L1Result<Option<(u64, alloy_primitives::B256)>> {
        let provider = self.build_provider();
        Ok(provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|e| L1Error::Provider(format!("get_block(finalized): {e}")))?
            .map(|b| (b.header.number, b.header.hash)))
    }

    /// The rollup's committed root at L1's finalized block, or `None` while L1
    /// has no finalized block.
    ///
    /// # Errors
    ///
    /// - [`L1Error::Provider`] on RPC failure (retryable).
    /// - [`L1Error::Decode`] when the registry's answer isn't an EEZ `rollups` return.
    /// - [`L1Error::Config`] when the rollup is registered with root zero.
    pub async fn finalized_commitment(&self) -> L1Result<Option<FinalizedCommitment>> {
        let Some((l1_block, l1_hash)) = self.finalized_block().await? else {
            return Ok(None);
        };
        let provider = self.build_provider();
        // Pin both reads to F's hash so a reorg cannot mix two blocks.
        let at = BlockId::Hash(RpcBlockHash {
            block_hash: l1_hash,
            require_canonical: Some(true),
        });
        let eez = self.eez();
        let rollup_id = self.rollup_id();
        let code = provider
            .get_code_at(eez)
            .block_id(at)
            .await
            .map_err(|e| L1Error::Provider(format!("get_code({eez}) at {l1_hash}: {e}")))?;
        let (rollup_contract, root) = if code.is_empty() {
            (Address::ZERO, B256::ZERO)
        } else {
            let req = TransactionRequest::default().to(eez).input(
                rollupsCall {
                    rollupId: rollup_id,
                }
                .abi_encode()
                .into(),
            );
            let out = provider.call(req).block(at).await.map_err(|e| {
                L1Error::Provider(format!("rollups({rollup_id}) at {l1_hash}: {e}"))
            })?;
            // The block is pinned, so an undecodable answer stays wrong: a wrong registry address.
            let r = rollupsCall::abi_decode_returns(&out).map_err(|e| {
                L1Error::Decode(format!(
                    "{eez} answered rollups({rollup_id}) with bytes that don't decode; is it an EEZ registry? {e}"
                ))
            })?;
            (r.rollupContract, r.root)
        };
        let root = registered_root(
            eez,
            self.config.deploy_block,
            code.is_empty(),
            rollup_contract,
            root,
            rollup_id,
            l1_block,
        )?;
        Ok(Some(FinalizedCommitment {
            l1_block,
            l1_hash,
            root,
        }))
    }

    pub(crate) fn rpc_url(&self) -> url::Url {
        self.config.rpc_url.clone()
    }

    pub(crate) fn eez(&self) -> alloy_primitives::Address {
        self.config.eez
    }

    pub(crate) fn rollup_id(&self) -> u64 {
        self.config.rollup_id
    }

    fn build_provider(&self) -> impl Provider + use<> {
        ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(self.config.rpc_url.clone())
    }

    /// Provider for block traces, at [`L1ReaderConfig::trace_rpc_url`].
    pub(crate) fn build_trace_provider(&self) -> impl Provider + use<> {
        ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(self.config.trace_rpc_url.clone())
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256};

    use super::registered_root;

    #[test]
    fn registered_root_cases() {
        let root = B256::repeat_byte(7);
        let rc = Address::repeat_byte(1);
        assert_eq!(registered_root(rc, 10, true, rc, root, 1, 5).unwrap(), None);
        let err = registered_root(rc, 5, true, rc, root, 1, 5).unwrap_err();
        assert!(err.to_string().contains("deploy block 5"));
        assert!(matches!(err, crate::L1Error::Config(_)));
        assert_eq!(
            registered_root(rc, 5, false, Address::ZERO, B256::ZERO, 1, 5).unwrap(),
            None
        );
        let err = registered_root(rc, 5, false, rc, B256::ZERO, 1, 5).unwrap_err();
        assert!(
            err.to_string()
                .contains("registered at L1 block 5 with root 0")
        );
        assert_eq!(
            registered_root(rc, 5, false, rc, root, 1, 5).unwrap(),
            Some(root)
        );
    }
}
