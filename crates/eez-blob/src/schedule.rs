//! Blob limit for one L1 timestamp, taken from that chain's spec.

use reth_chainspec::{ChainSpec, EthChainSpec as _, EthereumHardforks as _};

use crate::BlobSidecarEncoding;

/// How many blobs one settlement transaction may carry at `timestamp`, and
/// which sidecar wrapper that fork uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveBlobLimit {
    pub max_blobs: usize,
    pub sidecar_encoding: BlobSidecarEncoding,
}

/// `None` when no blob fork is active, or either cap is zero.
#[must_use]
pub fn blob_limit_at(spec: &ChainSpec, timestamp: u64) -> Option<ActiveBlobLimit> {
    let params = spec.blob_params_at_timestamp(timestamp)?;
    let max = params.max_blobs_per_tx.min(params.max_blob_count);
    let max_blobs = usize::try_from(max).ok().filter(|count| *count > 0)?;
    let sidecar_encoding = if spec.is_osaka_active_at_timestamp(timestamp) {
        BlobSidecarEncoding::Eip7594
    } else {
        BlobSidecarEncoding::Eip4844
    };
    Some(ActiveBlobLimit {
        max_blobs,
        sidecar_encoding,
    })
}

#[cfg(test)]
mod tests {
    use reth_chainspec::ChainSpecBuilder;

    use super::*;

    #[test]
    fn cancun_and_osaka_limits_come_from_the_spec() {
        let cancun = ChainSpecBuilder::mainnet().cancun_activated().build();
        let osaka = ChainSpecBuilder::mainnet().osaka_activated().build();
        let cancun_limit = blob_limit_at(&cancun, 1).unwrap();
        let osaka_limit = blob_limit_at(&osaka, 1).unwrap();
        assert!(cancun_limit.max_blobs > 0);
        assert_eq!(cancun_limit.sidecar_encoding, BlobSidecarEncoding::Eip4844);
        assert_eq!(osaka_limit.sidecar_encoding, BlobSidecarEncoding::Eip7594);
        assert!(blob_limit_at(&ChainSpecBuilder::mainnet().build(), 0).is_none());
    }

    #[test]
    fn the_fork_maxima_bound_capacity_instead_of_the_fee_target() {
        let mut spec = ChainSpecBuilder::mainnet().cancun_activated().build();
        spec.blob_params.cancun.target_blob_count = 1;
        spec.blob_params.cancun.max_blob_count = 4;
        spec.blob_params.cancun.max_blobs_per_tx = 6;
        assert_eq!(blob_limit_at(&spec, 1).unwrap().max_blobs, 4);
        spec.blob_params.cancun.max_blobs_per_tx = 2;
        assert_eq!(blob_limit_at(&spec, 1).unwrap().max_blobs, 2);
        spec.blob_params.cancun.max_blob_count = 0;
        assert!(blob_limit_at(&spec, 1).is_none());
    }
}
