//! Strongly-typed rollup identifier.
//!
//! Invariant 8 of the protocol treats `rollupId` as a first-class
//! parameter of every component. [`RollupId`] makes that invariant
//! compiler-enforced: it cannot be confused with `chain_id`, `gas_used`,
//! block numbers, or any of the dozens of other `u64`s that flow through
//! the codebase.
//!
//! Serializes transparently as a bare number — JSON / TOML / proto
//! representations are byte-identical to plain `u64`, so existing
//! configs and RPC responses round-trip unchanged.

use serde::{Deserialize, Serialize};

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
