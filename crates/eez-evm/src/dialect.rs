//! Per-chain dialect for entry encoding and CCM-verify batch construction.
//!
//! [`ChainDialect`] is the [`ChainProtocol::Dialect`](crosschain_protocol::ChainProtocol::Dialect)
//! implementation for EVM chains. Two variants distinguish the two
//! contract surfaces the protocol exposes:
//!
//! - [`EvmL2Style`](ChainDialect::EvmL2Style) — `CrossChainManagerL2`,
//!   system-address-loaded `loadExecutionTable`.
//! - [`EvmL1Style`](ChainDialect::EvmL1Style) — `EEZ.sol`,
//!   permissionless `executeL1ToL2Call` / `executeL2TX`.
//!
//! Slot, ABI selection, and emission rules flow through
//! `TargetConfig<EvmProtocol>`; `LocalChainClient` and inspectors never
//! see `ChainDialect` directly.

use alloy_sol_types::SolCall;
use crosschain_protocol::{RecordedCall, RollupId};

use crate::authorized_proxies::{CCM_AUTHORIZED_PROXIES_SLOT, ROLLUPS_AUTHORIZED_PROXIES_SLOT};
use crate::types::executeL1ToL2CallCall;
use crate::EvmProtocol;

/// Selects the contract ABI and entry-emission rules for one rollup.
///
/// Stored on [`crosschain_protocol::TargetConfig`] (via
/// `ChainProtocol::Dialect = ChainDialect`) and read at composition
/// time to select the correct calldata encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ChainDialect {
    /// L2 follower (`CrossChainManagerL2`): system-address-loaded
    /// `loadExecutionTable`. Default.
    #[default]
    EvmL2Style,
    /// L1 follower (`EEZ.sol`): permissionless
    /// `executeL1ToL2Call` / `executeL2TX`.
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

    /// Encode the follower-side trigger calldata for the
    /// `outer_root` cross-chain call.
    ///
    /// In the multi-prover protocol, both L1 and L2 dispatch via
    /// the same `executeL1ToL2Call(sourceAddress, callData)`
    /// entry point on the manager — invoked through the registered
    /// proxy. The composer's CCM-verify simulation forges the
    /// call from the proxy's address; this method returns the
    /// calldata for the outer ROUTING call (the proxy's `fallback`
    /// receives `outer_root.calldata` as-is and forwards to the
    /// manager).
    ///
    /// `raw_tx` and `source_rollup_id` are accepted for trait
    /// compatibility but unused — the proxy fallback only needs
    /// the original calldata.
    #[must_use]
    pub fn encode_follower_trigger(
        &self,
        call: &RecordedCall<EvmProtocol>,
        _source_rollup_id: RollupId,
        _raw_tx: &[u8],
    ) -> Vec<u8> {
        let _ = self;
        // Both dialects route through the proxy's fallback, which
        // forwards the original calldata to the manager. The
        // simulator's TargetTransaction sets `destination` to the
        // proxy address externally; here we just pass through the
        // outer call's calldata bytes.
        call.calldata.to_vec()
    }
}

/// Stand-alone helper: encode `executeL1ToL2Call(sourceAddress,
/// callData)` against a manager. Used for direct manager invocation
/// (currently only by tests / Phase-2 paths); the production flow
/// goes through the proxy fallback.
#[must_use]
pub fn encode_execute_cross_chain_call(
    source_address: alloy_primitives::Address,
    call_data: &[u8],
) -> Vec<u8> {
    executeL1ToL2CallCall {
        sourceAddress: source_address,
        callData: call_data.to_vec().into(),
    }
    .abi_encode()
}

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
