//! Per-rollup composition configuration.
//!
//! The long-lived orchestration that holds clients and runs one
//! [`CompositionBuilder`](crate::composition::CompositionBuilder)
//! pass per source tx lives in the runtime composer
//! (`eez-composer`'s `CrossChainWiring`); this module carries the
//! static per-rollup configuration it registers.

use std::collections::HashMap;

use alloy_primitives::Address;

use crate::dialect::ChainDialect;
use crate::rollup_id::RollupId;

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

/// Per-rollup attribution inputs for batch construction.
///
/// [`crate::entries::build_batch`]
/// consumes this to chain per-entry `stateDeltas` (upstream's invariant 6).
/// Two sources of truth:
///
/// - `initial_roots[rollup]` — the state root each rollup started at,
///   read from the entry chain once per
///   [`Composer::simulate_and_resolve`](Composer::simulate_and_resolve).
/// - `per_tx_roots_by_rollup[rollup]` — the post-state roots
///   `finalize` attributed per rollup (zk-poster settlement root or
///   inbound delivery root).
///
/// References (no ownership) so the builder materializes each map once
/// per composition and hands borrowed handles to the batch builder.
///
/// This struct is protocol-agnostic by construction: no EVM types named.
/// Builders that need chain-specific bookkeeping (counter folds,
/// classifier passes) walk the preorder `recorded[..]` slice directly —
/// the attribution here is purely about numeric state roots.
#[derive(Debug)]
pub struct SourceAttribution<'a> {
    /// Per-rollup initial state roots, as of the entry chain's current
    /// block when the composition began.
    pub initial_roots: &'a HashMap<RollupId, [u8; 32]>,
    /// Per-rollup cumulative post-state roots for each tx in that
    /// rollup's CCM-verify batch. Keyed by `RollupId`; each `Vec` is
    /// ordered by batch tx index.
    pub per_tx_roots_by_rollup: &'a HashMap<RollupId, Vec<[u8; 32]>>,
}
