//! Per-rollup composition configuration.
//!
//! The long-lived orchestration that holds clients and runs one
//! [`CompositionBuilder`](crate::composer::composition::CompositionBuilder)
//! pass per source tx lives in the runtime composer
//! (`composer::composer`'s `CrossChainWiring`); this module carries the
//! static per-rollup configuration it registers.

use alloy_primitives::Address;

use eez_protocol::dialect::ChainDialect;

// ── Config ───────────────────────────────────────────────────────

/// Combined proxy-lookup configuration for a registered rollup.
///
/// Bundles the storage-contract address and the storage slot index
/// where that contract holds its `authorizedProxies` mapping.
///
/// Constructed at `main.rs` startup from the rollup's role:
/// - L1-style client (entry-as-L1 or follower-as-L1):
///   `contract_address = rollups_address`,
///   `authorized_proxies_slot = 0` (`EEZ.authorizedProxies` —
///   inherited from `EEZBase` at slot 0).
/// - L2-style client:
///   `contract_address = ccm_address`,
///   `authorized_proxies_slot = 0`
///   (`EEZL2.authorizedProxies` — inherited from `EEZBase`
///   at slot 0).
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
/// Holds the proxy lookup and ABI dialect for one rollup (entry or
/// follower). Passed to
/// [`ComposerBuilder::entry`] / [`ComposerBuilder::rollup`] alongside
/// the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetConfig {
    /// Proxy-lookup configuration for this rollup.
    pub proxy_lookup: ProxyLookupConfig,
    /// ABI dialect: selects entry-encoding and
    /// batch shape (L1-style vs L2-style).
    /// Default = `EvmL2Style`
    /// (preserves byte-identity for the existing 12 L1→L2 fixtures).
    pub dialect: ChainDialect,
}
