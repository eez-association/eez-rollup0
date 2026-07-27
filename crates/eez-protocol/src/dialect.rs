//! Per-chain dialect for entry encoding and CCM-verify batch construction.
//!
//! Two [`ChainDialect`] variants distinguish the two
//! contract surfaces the protocol exposes:
//!
//! - [`EvmL2Style`](ChainDialect::EvmL2Style) — `EEZL2`,
//!   system-address-loaded `loadExecutionTable`.
//! - [`EvmL1Style`](ChainDialect::EvmL1Style) — `EEZ.sol`,
//!   permissionless `executeCrossChainCall` / `executeL2TX`.
//!
//! Slot, ABI selection, and emission rules flow through
//! `TargetConfig`; the runtime composer (Step 7) and
//! inspectors never see `ChainDialect` directly.

use crate::authorized_proxies::{CCM_AUTHORIZED_PROXIES_SLOT, ROLLUPS_AUTHORIZED_PROXIES_SLOT};

/// Selects the contract ABI and entry-emission rules for one rollup.
///
/// Stored on [`crate::TargetConfig`] and read at composition
/// time to select the correct calldata encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ChainDialect {
    /// L2 follower (`EEZL2`): system-address-loaded
    /// `loadExecutionTable`. Default.
    #[default]
    EvmL2Style,
    /// L1 follower (`EEZ.sol`): permissionless
    /// `executeCrossChainCall` / `executeL2TX`.
    EvmL1Style,
}

impl ChainDialect {
    /// Storage slot of the `authorizedProxies` mapping on this chain's
    /// dispatch contract.
    #[must_use]
    pub const fn proxy_lookup_slot(&self) -> u8 {
        match self {
            Self::EvmL2Style => CCM_AUTHORIZED_PROXIES_SLOT,
            Self::EvmL1Style => ROLLUPS_AUTHORIZED_PROXIES_SLOT,
        }
    }

    /// Whether the CCM execute transaction must be sent from the
    /// registered system address (L2-style) or is permissionless
    /// through the registered proxy (L1-style).
    #[must_use]
    pub const fn system_address_required(&self) -> bool {
        matches!(self, Self::EvmL2Style)
    }

    /// Whether this dialect routes its table-loading payload through
    /// the canonical proof-bundle poster (L1-style →
    /// `EEZ.postAndVerifyBatch`). Drives
    /// [`encode_table_payload`](crate::entries::encode_table_payload)'s
    /// dispatch.
    #[must_use]
    pub const fn is_zk_poster(&self) -> bool {
        matches!(self, Self::EvmL1Style)
    }
}

// (Removed in the 5c51e02 bump: `encode_execute_cross_chain_call` /
// `executeL1ToL2Call` — a stale direct-invocation helper whose selector never
// matched the in-tree contracts; production always flows through the proxy
// fallback. No in-tree consumers remained.)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_lookup_slot_l2_is_ccm() {
        assert_eq!(
            ChainDialect::EvmL2Style.proxy_lookup_slot(),
            CCM_AUTHORIZED_PROXIES_SLOT
        );
    }

    #[test]
    fn proxy_lookup_slot_l1_is_rollups() {
        assert_eq!(
            ChainDialect::EvmL1Style.proxy_lookup_slot(),
            ROLLUPS_AUTHORIZED_PROXIES_SLOT
        );
    }

    #[test]
    fn l2_style_requires_system_address() {
        assert!(ChainDialect::EvmL2Style.system_address_required());
    }

    #[test]
    fn l1_style_does_not_require_system_address() {
        assert!(!ChainDialect::EvmL1Style.system_address_required());
    }
}
