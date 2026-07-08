//! Strongly-typed rollup identifier.
//!
//! Treats `rollupId` as a first-class parameter of every component
//! (upstream's "Invariant 8"). [`RollupId`] makes that constraint
//! compiler-enforced: it cannot be confused with `chain_id`, `gas_used`,
//! block numbers, or any of the dozens of other `u64`s that flow through
//! the codebase.
//!
//! Serializes transparently as a bare number — JSON / TOML / proto
//! representations are byte-identical to plain `u64`, so existing
//! configs and RPC responses round-trip unchanged.

use serde::{Deserialize, Serialize};

/// The full identity of one chain in a deployment — the THREE names the
/// same chain answers to, carried together at REGISTRATION time so every
/// component resolves from one place (the D-3 decision in
/// `chain-identity-unification.md`):
///
/// - `wire_key` — the opaque transport key (queue keys, gRPC routing,
///   logs; e.g. `"eez-dev-33333"`). Never part of any hash.
/// - `chain_id` — the EIP-155 chain id (signature/admission validation,
///   the EVM env). VM identity, not protocol identity.
/// - `registry_id` — the id the L1 `EEZ.registerRollup` assigned. THE
///   protocol identity: the message hash `H`, settlement `StateDelta`s,
///   proxy registrations, and (registry-native, D-1) the entire
///   composition space — rollup maps, sessions, dispatches — key on it.
///   Every runtime [`RollupId`] IS a registry id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainIdentity {
    /// Opaque transport key (see type docs).
    pub wire_key: String,
    /// EIP-155 chain id.
    pub chain_id: u64,
    /// L1-registry id — the protocol identity.
    pub registry_id: RollupId,
}

/// Identifier of a rollup within this deployment.
///
/// Wraps a `u64` to distinguish rollup identity from other numeric
/// quantities at the type level. Construct with the tuple syntax
/// (`RollupId(42)`) or `u64::into()`; extract the raw value with `.0`
/// or `u64::from()`.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct RollupId(pub u64);

impl RollupId {
    /// The mainnet / L1 sentinel rollup id (`0`). Used as the `from`/`source`
    /// rollup of an L1-originated cross-chain call (e.g. `EEZ.executeCrossChainCall`
    /// hardcodes it as the source for L1 proxies).
    pub const MAINNET: RollupId = RollupId(0);

    /// Whether this is the [mainnet/L1 sentinel](Self::MAINNET).
    #[must_use]
    pub const fn is_mainnet(self) -> bool {
        self.0 == Self::MAINNET.0
    }
}

impl std::fmt::Display for RollupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<u64> for RollupId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<RollupId> for u64 {
    fn from(id: RollupId) -> Self {
        id.0
    }
}
