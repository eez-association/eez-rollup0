//! Classification of incoming user transactions for the
//! Sequencer/Composer ingress path (§5.4.5).
//!
//! When `eth_sendRawTransaction` arrives at our reth node, the
//! middleware in `eez-node` consults [`IngressClassifier`] to decide
//! whether the tx is:
//!
//! - **L2-only**: a vanilla L2 transaction. Routed to the standard
//!   reth pool; reth's payload builder picks it up for the next
//!   block.
//! - **Cross-chain**: the tx touches a registered proxy contract on
//!   this chain (the `authorizedProxies` lookup per Rollup-1
//!   §5.4 / spec §10). Routed to the per-rollup
//!   [`HeldPool`](crate::HeldPool); composed at the next Sync slot.
//!
//! The classifier is intentionally **dumb**: it inspects the `to`
//! address of the decoded tx and checks set membership. It does NOT
//! simulate, it does NOT decode calldata. Mis-classifications are
//! recoverable per §5.4.5: an L2-only-misrouted-to-held tx gets
//! composed needlessly then included in a Sync block (cost: one
//! wasted simulation); a held-misrouted-to-L2 tx lands in a normal
//! block with no cross-chain effect (cost: user-visible failure if
//! the tx was actually a cross-chain call — but no protocol-level
//! corruption).
//!
//! Stage-4 status: the proxy-address set is configured by env vars
//! (defaulting to empty — every tx classifies as L2-only until
//! cross-chain content is wired). When `EvmComposer<EvmProtocol>`
//! construction lands at the umbrella (S4.8 follow-up), the
//! classifier picks up the actual proxy addresses from the rollup
//! config.

use std::collections::HashSet;

use alloy_primitives::Address;

/// Tx classification verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Classification {
    /// Vanilla L2 transaction; route to reth pool.
    L2Only,
    /// Cross-chain transaction (touches a registered proxy); route
    /// to the per-rollup `HeldPool`.
    CrossChain,
}

/// Proxy-address set used to classify incoming transactions.
///
/// Empty set ⇒ every tx classifies as [`Classification::L2Only`].
/// Populated from rollup config at startup (S4.8+; for now the set
/// is configured by `eez-node` at umbrella-build time based on the
/// env vars `EEZ_CROSS_CHAIN_PROXY_ADDRESSES` — a comma-separated
/// list of hex addresses).
///
/// The set is read-only after construction; classification is a
/// hot-path lookup (every incoming `eth_sendRawTransaction`).
#[derive(Debug, Clone, Default)]
pub struct IngressClassifier {
    proxy_addresses: HashSet<Address>,
}

impl IngressClassifier {
    /// Construct from a set of proxy contract addresses on this
    /// chain. An empty set classifies every tx as L2-only.
    #[must_use]
    pub fn new(proxy_addresses: HashSet<Address>) -> Self {
        Self { proxy_addresses }
    }

    /// True iff no proxy addresses are registered. Classifier will
    /// return [`Classification::L2Only`] for every tx.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.proxy_addresses.is_empty()
    }

    /// Total number of registered proxy addresses.
    #[must_use]
    pub fn len(&self) -> usize {
        self.proxy_addresses.len()
    }

    /// Classify a tx by its `to` address.
    ///
    /// `to = None` (contract creation) is always
    /// [`Classification::L2Only`] — cross-chain calls always target
    /// a known proxy contract, never a creation.
    #[must_use]
    pub fn classify(&self, to: Option<&Address>) -> Classification {
        match to {
            Some(addr) if self.proxy_addresses.contains(addr) => Classification::CrossChain,
            _ => Classification::L2Only,
        }
    }
}

impl FromIterator<Address> for IngressClassifier {
    fn from_iter<I: IntoIterator<Item = Address>>(addresses: I) -> Self {
        Self {
            proxy_addresses: addresses.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn empty_classifier_always_l2_only() {
        let c = IngressClassifier::default();
        assert!(c.is_empty());
        let addr = address!("1111111111111111111111111111111111111111");
        assert_eq!(c.classify(Some(&addr)), Classification::L2Only);
        assert_eq!(c.classify(None), Classification::L2Only);
    }

    #[test]
    fn proxy_address_classified_cross_chain() {
        let proxy = address!("2222222222222222222222222222222222222222");
        let c: IngressClassifier = [proxy].into_iter().collect();
        assert_eq!(c.classify(Some(&proxy)), Classification::CrossChain);
        let other = address!("3333333333333333333333333333333333333333");
        assert_eq!(c.classify(Some(&other)), Classification::L2Only);
        assert_eq!(c.classify(None), Classification::L2Only);
    }
}
