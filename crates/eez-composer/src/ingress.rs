//! Classification of incoming user transactions for the
//! Sequencer/Composer ingress path (§5.4.5).
//!
//! When `eth_sendRawTransaction` arrives at our reth node, the
//! middleware in `eez-node` decides whether the tx is:
//!
//! - **L2-only**: a vanilla L2 transaction. Routed to the standard
//!   reth pool; reth's payload builder picks it up for the next block.
//! - **Cross-chain**: routed to the per-rollup
//!   [`HeldPool`](crate::HeldPool) and composed at the next Sync slot.
//!   Two directions:
//!     * **Inbound** (L1→L2): an L1-bound raw tx POSTed to L2's RPC,
//!       matched by `tx.chain_id ∈ cross_chain_source_chain_ids` — the
//!       static signal this type still carries.
//!     * **Outbound** (L2→L1): a call to a registered cross-chain proxy
//!       on this rollup. This is NO LONGER a static-set membership test:
//!       the `eez-node` middleware detects it DYNAMICALLY by reading
//!       `authorizedProxies[to]` on the L2 CCM (EEZL2) — the protocol's
//!       own on-chain identity mechanism, mirroring the inbound B0 L1
//!       interceptor's `authorizedProxies` lookup. So [`IngressClassifier`]
//!       only carries the inbound chain-id signal now; outbound is resolved
//!       against live L2 state in the middleware.
//!
//! Why dynamic outbound: a hand-maintained proxy env list silently drifts
//! from the on-chain registry — a proxy created on L2 but missing from the
//! list is mis-routed `L2Only`, mines as a normal tx, REVERTS (no loaded
//! entry), and the L2→L1 effect is lost with NO recovery (only HELD txs
//! reach the authoritative `SessionInspector` at the composer drain). The
//! live `authorizedProxies` read eliminates that footgun.
//!
//! Mis-classification of the inbound chain-id signal remains recoverable
//! per §5.4.5 (the authoritative classifier is `SessionInspector` during
//! `simulate_and_resolve`).

use std::collections::HashSet;

/// Direction of a cross-chain transaction, derived from which ingress
/// signal matched.
///
/// - [`Direction::Inbound`] — an **L1→L2** call: an L1-source intent
///   (the user POSTs an L1-bound raw tx to L2's RPC), matched by
///   `tx.chain_id ∈ cross_chain_source_chain_ids`.
/// - [`Direction::Outbound`] — an **L2→L1** call: a call to a registered
///   proxy on this rollup, matched by a live `authorizedProxies[to]` read
///   on the L2 CCM in the `eez-node` ingress middleware (not here).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// L1→L2 (inbound) — matched by foreign source chain id.
    Inbound,
    /// L2→L1 (outbound) — matched by a live `authorizedProxies` lookup.
    Outbound,
}

/// Tx classification verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Classification {
    /// Vanilla L2 transaction; route to reth pool.
    L2Only,
    /// Cross-chain transaction; route to the per-rollup `HeldPool`.
    /// Carries the [`Direction`] derived from which signal matched.
    CrossChain(Direction),
}

/// Foreign-source-chain-id set used to classify INBOUND (L1→L2) intents at
/// ingress.
///
/// **Inbound** signal: `tx.chain_id ∈ cross_chain_source_chain_ids` — an
/// L1-source intent (user POSTs an L1-bound raw tx to L2's RPC; the chainId
/// mismatch is the signal), processed by the composer on the next Sync slot.
///
/// **Outbound** is NOT classified here: the `eez-node` middleware reads
/// `authorizedProxies[to]` on the L2 CCM (EEZL2) live, so there is no static
/// proxy set to drift from the on-chain registry. The inbound chain-id signal
/// is still a heuristic — the authoritative classifier is `SessionInspector`
/// during `simulate_and_resolve`. Empty set ⇒ no inbound fast-path matches.
///
/// Read-only after construction; classification is a hot-path lookup
/// (every incoming `eth_sendRawTransaction`).
#[derive(Debug, Clone, Default)]
pub struct IngressClassifier {
    cross_chain_source_chain_ids: HashSet<u64>,
}

impl IngressClassifier {
    /// Construct from a set of foreign source chain ids (the inbound
    /// signal). An empty set means no tx matches the inbound fast path;
    /// outbound is resolved dynamically by the middleware regardless.
    #[must_use]
    pub fn new(cross_chain_source_chain_ids: HashSet<u64>) -> Self {
        Self {
            cross_chain_source_chain_ids,
        }
    }

    /// True iff no foreign source chain ids are registered — the inbound
    /// fast path classifies every tx as `L2Only`. NOTE: this is only the
    /// INBOUND signal; the middleware still detects outbound dynamically,
    /// so an empty classifier does NOT mean the ingress middleware is inert.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cross_chain_source_chain_ids.is_empty()
    }

    /// Number of foreign source chain ids registered.
    #[must_use]
    pub fn source_chain_id_count(&self) -> usize {
        self.cross_chain_source_chain_ids.len()
    }

    /// Classify the INBOUND signal by `chain_id`: a foreign source chain id
    /// (an L1-bound raw tx POSTed to L2's RPC) → [`Classification::CrossChain`]
    /// `(`[`Direction::Inbound`]`)`; else [`Classification::L2Only`].
    ///
    /// OUTBOUND (L2→L1) is detected separately + dynamically by the
    /// `eez-node` middleware via a live `authorizedProxies[to]` read — NOT
    /// here. `chain_id = None` (pre-EIP-155 legacy) never matches inbound.
    #[must_use]
    pub fn classify(&self, chain_id: Option<u64>) -> Classification {
        if let Some(cid) = chain_id {
            if self.cross_chain_source_chain_ids.contains(&cid) {
                return Classification::CrossChain(Direction::Inbound);
            }
        }
        Classification::L2Only
    }
}

impl FromIterator<u64> for IngressClassifier {
    fn from_iter<I: IntoIterator<Item = u64>>(ids: I) -> Self {
        Self {
            cross_chain_source_chain_ids: ids.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_classifier_always_l2_only() {
        let c = IngressClassifier::default();
        assert!(c.is_empty());
        assert_eq!(c.classify(Some(1)), Classification::L2Only);
        assert_eq!(c.classify(None), Classification::L2Only);
    }

    #[test]
    fn foreign_chain_id_classified_inbound() {
        // L2 chainId=1; L1 chainId=31337 (anvil). Tx with chainId=31337
        // = an L1-source deposit intent posted to L2's RPC.
        let l1_chain = 31337u64;
        let c = IngressClassifier::new([l1_chain].into_iter().collect());
        assert_eq!(
            c.classify(Some(l1_chain)),
            Classification::CrossChain(Direction::Inbound),
        );
        // L2-chainId tx → L2Only (outbound is resolved by the middleware,
        // not by this classifier).
        assert_eq!(c.classify(Some(1)), Classification::L2Only);
        // pre-EIP-155 legacy (no chain id) never matches inbound.
        assert_eq!(c.classify(None), Classification::L2Only);
    }

    #[test]
    fn from_iter_builds_inbound_set() {
        let c: IngressClassifier = [10200u64, 31337u64].into_iter().collect();
        assert_eq!(c.source_chain_id_count(), 2);
        assert_eq!(
            c.classify(Some(10200)),
            Classification::CrossChain(Direction::Inbound)
        );
        assert_eq!(
            c.classify(Some(31337)),
            Classification::CrossChain(Direction::Inbound)
        );
        assert_eq!(c.classify(Some(999)), Classification::L2Only);
    }
}
