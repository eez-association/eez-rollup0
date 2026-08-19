//! Read-only canonical L1 access for the Deriver.

use std::sync::Arc;

use alloy_eips::BlockNumberOrTag;
use alloy_provider::{Provider, ProviderBuilder};

use crate::config::L1ReaderConfig;
use crate::error::{L1Error, L1Result};
use crate::scan::{BatchLogChunks, ScannedBatch, scan_next_batch_log_chunk};

/// Read-only L1 client for historical batch scans and canonicality checks.
/// Cheaply [`Clone`]able.
#[derive(Clone)]
pub struct L1Reader {
    config: Arc<L1ReaderConfig>,
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
