//! Read-only canonical L1 access for the Deriver.

use std::sync::Arc;

use alloy_eips::BlockNumberOrTag;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;

use crate::config::L1ReaderConfig;
use crate::error::{L1Error, L1Result};
use crate::scan::{BatchLogChunks, ScannedBatch, scan_next_batch_log_chunk};

/// Read-only L1 client for historical batch scans and canonicality checks.
/// Cheaply [`Clone`]able.
#[derive(Clone)]
pub struct L1Reader {
    config: Arc<L1ReaderConfig>,
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

    /// Scans and returns the next `BatchPosted` log chunk, or `None` when
    /// the chunks are exhausted.
    ///
    /// # Errors
    ///
    /// - [`L1Error::Provider`] on RPC failure (log fetch, tx fetch).
    /// - [`L1Error::SourceIncomplete`] when a canonical batch tx is not
    ///   served by the L1 source yet — retryable once the source syncs.
    pub async fn next_batch_log_chunk(
        &self,
        chunks: &mut BatchLogChunks,
    ) -> L1Result<Option<Vec<ScannedBatch>>> {
        let provider = self.build_provider();
        scan_next_batch_log_chunk(&provider, self.config.eez, self.config.rollup_id, chunks).await
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
}
