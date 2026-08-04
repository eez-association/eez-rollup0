//! Per-rollup composition configuration.
//!
//! The long-lived orchestration that holds clients and runs one
//! [`CompositionBuilder`](crate::composition::CompositionBuilder)
//! pass per source tx lives in the runtime composer
//! (`eez-composer`'s `CrossChainWiring`); this module carries the
//! static per-rollup configuration it registers.

use alloy_primitives::Address;

use crate::dialect::ChainDialect;

// ── Config ───────────────────────────────────────────────────────

/// Combined proxy-lookup configuration for a registered rollup.
///
/// `contract_address` identifies the chain-local manager and
/// `authorized_proxies_slot` identifies its mapping slot. Both supported
/// manager contracts inherit `authorizedProxies` at slot zero from `EEZBase`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyLookupConfig {
    /// Address of the contract holding `authorizedProxies` on this chain.
    pub contract_address: Address,
    /// Storage slot index where `authorizedProxies` lives on
    /// `contract_address`. The inspector reads
    /// `keccak256(addr ++ slot)` to find a registered proxy.
    pub authorized_proxies_slot: u8,
}

/// Per-rollup static configuration.
///
/// Holds the proxy lookup for one rollup (entry or follower). Passed to the
/// runtime composer's rollup-registration paths alongside the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetConfig {
    /// Proxy-lookup configuration for this rollup.
    pub proxy_lookup: ProxyLookupConfig,
    /// Contract dialect used for proxy lookup and target-batch construction.
    pub dialect: ChainDialect,
}
