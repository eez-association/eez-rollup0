//! Header-type shim: chiado L1 provider → [`super::LocalChainClient`]-compatible
//! provider.
//!
//! `LocalChainClient` requires
//! `HeaderProvider<Header = alloy_consensus::Header>` (plus the usual
//! `StateProviderFactory + BlockNumReader + HeaderReader` cluster).
//! The chiado L1 reth instance is launched with
//! `reth_gnosis::GnosisNode`, whose provider exposes
//! `HeaderProvider<Header = gnosis_primitives::header::GnosisHeader>`.
//!
//! `GnosisHeader` is structurally identical to `alloy_consensus::Header`
//! on post-merge blocks (chiado has been post-merge since the chiado
//! merge fork); the only extras are `aura_step` / `aura_seal`, which
//! are `None` post-merge. The adapter wraps the gnosis provider and
//! converts headers on read using
//! [`gnosis_primitives::header::GnosisHeader::to_alloy_header`] (which
//! itself panics on pre-merge inputs — matching invariant 7's
//! no-silent-failures rule). State queries do not touch the header
//! type, so all other reads are direct delegation.
//!
//! `StateProviderFactory`, `BlockNumReader`, `BlockHashReader`, and
//! `BlockIdReader` are implemented as explicit pass-through delegation
//! to the inner chiado provider (a `BlockchainProvider` over
//! `GnosisNode`): only header reads need the `GnosisHeader → alloy`
//! conversion above, so the state-side traits forward unchanged.

use gnosis_primitives::header::GnosisHeader;
use reth_storage_api::{
    BlockHashReader, BlockIdReader, BlockNumReader, HeaderProvider, StateProviderFactory,
};

use super::provider::HeaderReader as LocalHeaderReader;

/// Wraps a chiado L1 provider (`HeaderProvider<Header = GnosisHeader>`)
/// and exposes `HeaderProvider<Header = alloy_consensus::Header>` —
/// the bound `LocalChainClient` requires.
///
/// State queries are not affected by header type, so [`StateProviderFactory`],
/// [`BlockNumReader`], [`BlockHashReader`], and [`BlockIdReader`] are
/// delegated unmodified to the inner provider.
///
/// # Invariants
///
/// - Inner headers MUST be post-merge (chiado has been post-merge since
///   the merge; this is enforced in `GnosisHeader → Header` by the
///   upstream `From` impl which panics if `mix_hash` / `nonce` are
///   unset).
pub struct GnosisL1Adapter<P> {
    inner: P,
}

impl<P> GnosisL1Adapter<P> {
    /// Wrap a chiado L1 provider.
    pub const fn new(inner: P) -> Self {
        Self { inner }
    }
}

impl<P: Clone> Clone for GnosisL1Adapter<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P> std::fmt::Debug for GnosisL1Adapter<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GnosisL1Adapter").finish_non_exhaustive()
    }
}

/// Convert a [`GnosisHeader`] to [`alloy_consensus::Header`].
///
/// Loud-failure (invariant 7): if the source header has any aura field
/// set (i.e. is pre-merge), we panic. Chiado has been fully post-merge
/// since the chiado merge fork; pre-merge headers would only appear if
/// the chiado-db snapshot we mount predates the merge, which the
/// operator's deploy.sh guards against. Quietly dropping aura fields
/// here would mean a malformed alloy header (its hash wouldn't match
/// the source GnosisHeader's hash), breaking every downstream
/// HeaderReader caller.
#[inline]
fn convert_header(gnosis: GnosisHeader) -> alloy_consensus::Header {
    assert!(
        gnosis.aura_step.is_none() && gnosis.aura_seal.is_none(),
        "GnosisL1Adapter: encountered pre-merge GnosisHeader with aura_step/aura_seal set at block {} — chiado must be post-merge for cross-chain composition (invariant 7: no silent fallback)",
        gnosis.number,
    );
    // `From<GnosisHeader> for Header` also panics if mix_hash/nonce are
    // unset — that path catches headers that are neither pre-merge nor
    // post-merge (an impossible-but-defensive case).
    alloy_consensus::Header::from(gnosis)
}

impl<P> HeaderProvider for GnosisL1Adapter<P>
where
    P: HeaderProvider<Header = GnosisHeader> + Send + Sync,
{
    type Header = alloy_consensus::Header;

    fn header(
        &self,
        block_hash: alloy_primitives::BlockHash,
    ) -> reth_storage_api::errors::provider::ProviderResult<Option<Self::Header>> {
        Ok(self.inner.header(block_hash)?.map(convert_header))
    }

    fn header_by_number(
        &self,
        num: u64,
    ) -> reth_storage_api::errors::provider::ProviderResult<Option<Self::Header>> {
        Ok(self.inner.header_by_number(num)?.map(convert_header))
    }

    fn headers_range(
        &self,
        range: impl core::ops::RangeBounds<alloy_primitives::BlockNumber>,
    ) -> reth_storage_api::errors::provider::ProviderResult<Vec<Self::Header>> {
        Ok(self
            .inner
            .headers_range(range)?
            .into_iter()
            .map(convert_header)
            .collect())
    }

    fn sealed_header(
        &self,
        number: alloy_primitives::BlockNumber,
    ) -> reth_storage_api::errors::provider::ProviderResult<
        Option<reth_primitives_traits::SealedHeader<Self::Header>>,
    > {
        let Some(sealed) = self.inner.sealed_header(number)? else {
            return Ok(None);
        };
        let (gnosis_header, hash) = sealed.split();
        Ok(Some(reth_primitives_traits::SealedHeader::new(
            convert_header(gnosis_header),
            hash,
        )))
    }

    fn sealed_headers_while(
        &self,
        range: impl core::ops::RangeBounds<alloy_primitives::BlockNumber>,
        mut predicate: impl FnMut(&reth_primitives_traits::SealedHeader<Self::Header>) -> bool,
    ) -> reth_storage_api::errors::provider::ProviderResult<
        Vec<reth_primitives_traits::SealedHeader<Self::Header>>,
    > {
        // Convert each sealed header before applying the predicate. The
        // inner predicate is over alloy_consensus::Header so the
        // inspector sees the same shape any other caller would.
        let mut out = Vec::new();
        for sealed in self.inner.sealed_headers_range(range)? {
            let (gnosis_header, hash) = sealed.split();
            let converted =
                reth_primitives_traits::SealedHeader::new(convert_header(gnosis_header), hash);
            if !predicate(&converted) {
                break;
            }
            out.push(converted);
        }
        Ok(out)
    }
}

impl<P> BlockHashReader for GnosisL1Adapter<P>
where
    P: BlockHashReader + Send + Sync,
{
    fn block_hash(
        &self,
        number: alloy_primitives::BlockNumber,
    ) -> reth_storage_api::errors::provider::ProviderResult<Option<alloy_primitives::B256>> {
        self.inner.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: alloy_primitives::BlockNumber,
        end: alloy_primitives::BlockNumber,
    ) -> reth_storage_api::errors::provider::ProviderResult<Vec<alloy_primitives::B256>> {
        self.inner.canonical_hashes_range(start, end)
    }
}

impl<P> BlockNumReader for GnosisL1Adapter<P>
where
    P: BlockNumReader + Send + Sync,
{
    fn chain_info(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<reth_chainspec::ChainInfo> {
        self.inner.chain_info()
    }

    fn best_block_number(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<alloy_primitives::BlockNumber> {
        self.inner.best_block_number()
    }

    fn last_block_number(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<alloy_primitives::BlockNumber> {
        self.inner.last_block_number()
    }

    fn block_number(
        &self,
        hash: alloy_primitives::B256,
    ) -> reth_storage_api::errors::provider::ProviderResult<Option<alloy_primitives::BlockNumber>>
    {
        self.inner.block_number(hash)
    }
}

impl<P> BlockIdReader for GnosisL1Adapter<P>
where
    P: BlockIdReader + Send + Sync,
{
    fn pending_block_num_hash(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<Option<alloy_eips::BlockNumHash>> {
        self.inner.pending_block_num_hash()
    }

    fn safe_block_num_hash(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<Option<alloy_eips::BlockNumHash>> {
        self.inner.safe_block_num_hash()
    }

    fn finalized_block_num_hash(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<Option<alloy_eips::BlockNumHash>> {
        self.inner.finalized_block_num_hash()
    }
}

impl<P> StateProviderFactory for GnosisL1Adapter<P>
where
    P: StateProviderFactory + Send + Sync,
{
    fn latest(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<reth_storage_api::StateProviderBox>
    {
        self.inner.latest()
    }

    fn state_by_block_number_or_tag(
        &self,
        number_or_tag: alloy_eips::BlockNumberOrTag,
    ) -> reth_storage_api::errors::provider::ProviderResult<reth_storage_api::StateProviderBox>
    {
        self.inner.state_by_block_number_or_tag(number_or_tag)
    }

    fn history_by_block_number(
        &self,
        block: alloy_primitives::BlockNumber,
    ) -> reth_storage_api::errors::provider::ProviderResult<reth_storage_api::StateProviderBox>
    {
        self.inner.history_by_block_number(block)
    }

    fn history_by_block_hash(
        &self,
        block: alloy_primitives::BlockHash,
    ) -> reth_storage_api::errors::provider::ProviderResult<reth_storage_api::StateProviderBox>
    {
        self.inner.history_by_block_hash(block)
    }

    fn state_by_block_hash(
        &self,
        block: alloy_primitives::BlockHash,
    ) -> reth_storage_api::errors::provider::ProviderResult<reth_storage_api::StateProviderBox>
    {
        self.inner.state_by_block_hash(block)
    }

    fn pending(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<reth_storage_api::StateProviderBox>
    {
        self.inner.pending()
    }

    fn pending_state_by_hash(
        &self,
        block_hash: alloy_primitives::B256,
    ) -> reth_storage_api::errors::provider::ProviderResult<
        Option<reth_storage_api::StateProviderBox>,
    > {
        self.inner.pending_state_by_hash(block_hash)
    }

    fn maybe_pending(
        &self,
    ) -> reth_storage_api::errors::provider::ProviderResult<
        Option<reth_storage_api::StateProviderBox>,
    > {
        self.inner.maybe_pending()
    }
}

// `HeaderReader` (local trait) is auto-impl'd for any
// `HeaderProvider<Header = alloy_consensus::Header> + Send + Sync` via
// the blanket impl in `super::provider`. The adapter's
// `HeaderProvider` impl above already returns
// `alloy_consensus::Header`, so the blanket applies — no explicit impl
// needed. The bound is re-stated here for documentation.
#[allow(dead_code)]
fn _assert_local_header_reader<P>()
where
    P: HeaderProvider<Header = GnosisHeader> + Send + Sync + 'static,
    GnosisL1Adapter<P>: LocalHeaderReader,
{
}
