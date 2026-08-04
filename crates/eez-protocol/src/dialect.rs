//! Contract dialect used for per-rollup composition and proxy lookup.
//!
//! The variants distinguish the supported dispatch contracts:
//!
//! - [`EvmL2Style`](ChainDialect::EvmL2Style) — `EEZL2`; execution tables
//!   and inbound deliveries are installed by the configured system address.
//! - [`EvmL1Style`](ChainDialect::EvmL1Style) — `EEZ`; batches are posted
//!   and proof-verified through `postAndVerifyBatch`.
//!
//! Composition uses the dialect to select the target-batch shape. Local
//! clients use it to derive contract storage configuration; inspectors receive
//! the resulting [`crate::ProxyLookupConfig`].

use crate::authorized_proxies::{EEZ_AUTHORIZED_PROXIES_SLOT, EEZL2_AUTHORIZED_PROXIES_SLOT};

/// Selects the dispatch-contract layout and target-batch construction path for
/// one rollup.
///
/// Stored on [`crate::TargetConfig`]; composition uses it to distinguish L1
/// proof-posting targets from L2 execution-table targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ChainDialect {
    /// `EEZL2`: system-address-authorized `loadExecutionTable` and
    /// `executeIncomingCrossChainCall`. Default.
    #[default]
    EvmL2Style,
    /// L1 `EEZ`: proof-verified `postAndVerifyBatch` submission.
    EvmL1Style,
}

impl ChainDialect {
    /// Storage slot of the `authorizedProxies` mapping on this chain's
    /// dispatch contract.
    #[must_use]
    pub const fn proxy_lookup_slot(&self) -> u8 {
        match self {
            Self::EvmL2Style => EEZL2_AUTHORIZED_PROXIES_SLOT,
            Self::EvmL1Style => EEZ_AUTHORIZED_PROXIES_SLOT,
        }
    }

    /// Whether this is the L1 `EEZ` dialect whose batches are submitted
    /// through `postAndVerifyBatch`.
    #[must_use]
    pub const fn is_zk_poster(&self) -> bool {
        matches!(self, Self::EvmL1Style)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_lookup_slot_l2_is_eezl2() {
        assert_eq!(
            ChainDialect::EvmL2Style.proxy_lookup_slot(),
            EEZL2_AUTHORIZED_PROXIES_SLOT
        );
    }

    #[test]
    fn proxy_lookup_slot_l1_is_eez() {
        assert_eq!(
            ChainDialect::EvmL1Style.proxy_lookup_slot(),
            EEZ_AUTHORIZED_PROXIES_SLOT
        );
    }
}
