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

/// Proxy-address set + foreign-chain-id set used to classify incoming
/// transactions.
///
/// Two independent fast-path signals decide
/// [`Classification::CrossChain`]:
/// 1. **By proxy address**: `to ∈ proxy_addresses` — an L2-source call
///    touching a known proxy on this rollup.
/// 2. **By source chainId**: `tx.chain_id ∈ cross_chain_source_chain_ids`
///    — an L1-source intent (user POSTs an L1-bound raw_tx to L2's RPC;
///    the chainId mismatch is the signal), processed by the composer on
///    the next Sync slot.
///
/// Both signals are heuristics — the authoritative classifier is
/// `SessionInspector` during `simulate_and_resolve`. Empty sets ⇒ every
/// tx classifies as [`Classification::L2Only`].
///
/// Read-only after construction; classification is a hot-path lookup
/// (every incoming `eth_sendRawTransaction`).
#[derive(Debug, Clone, Default)]
pub struct IngressClassifier {
    proxy_addresses: HashSet<Address>,
    cross_chain_source_chain_ids: HashSet<u64>,
}

impl IngressClassifier {
    /// Construct from a set of proxy contract addresses on this
    /// chain and a set of foreign source chain ids. An empty
    /// combination classifies every tx as L2-only.
    #[must_use]
    pub fn new(
        proxy_addresses: HashSet<Address>,
        cross_chain_source_chain_ids: HashSet<u64>,
    ) -> Self {
        Self {
            proxy_addresses,
            cross_chain_source_chain_ids,
        }
    }

    /// True iff no proxy addresses AND no source chain ids are
    /// registered — classification reduces to `L2Only` for every tx.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.proxy_addresses.is_empty() && self.cross_chain_source_chain_ids.is_empty()
    }

    /// Total number of registered proxy addresses.
    #[must_use]
    pub fn len(&self) -> usize {
        self.proxy_addresses.len()
    }

    /// Number of foreign source chain ids registered.
    #[must_use]
    pub fn source_chain_id_count(&self) -> usize {
        self.cross_chain_source_chain_ids.len()
    }

    /// Classify a tx by its `to` address and its `chain_id`.
    ///
    /// `to = None` (contract creation) routes by chain_id only —
    /// cross-chain proxy calls always target a known proxy, but
    /// a deposit-intent tx may have any shape.
    /// `chain_id = None` (pre-EIP-155 legacy) routes by `to` only.
    #[must_use]
    pub fn classify(&self, to: Option<&Address>, chain_id: Option<u64>) -> Classification {
        if let Some(addr) = to {
            if self.proxy_addresses.contains(addr) {
                return Classification::CrossChain;
            }
        }
        if let Some(cid) = chain_id {
            if self.cross_chain_source_chain_ids.contains(&cid) {
                return Classification::CrossChain;
            }
        }
        Classification::L2Only
    }
}

impl FromIterator<Address> for IngressClassifier {
    fn from_iter<I: IntoIterator<Item = Address>>(addresses: I) -> Self {
        Self {
            proxy_addresses: addresses.into_iter().collect(),
            cross_chain_source_chain_ids: HashSet::new(),
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
        assert_eq!(c.classify(Some(&addr), Some(1)), Classification::L2Only);
        assert_eq!(c.classify(None, None), Classification::L2Only);
    }

    #[test]
    fn proxy_address_classified_cross_chain() {
        let proxy = address!("2222222222222222222222222222222222222222");
        let c: IngressClassifier = [proxy].into_iter().collect();
        assert_eq!(
            c.classify(Some(&proxy), Some(1)),
            Classification::CrossChain
        );
        let other = address!("3333333333333333333333333333333333333333");
        assert_eq!(c.classify(Some(&other), Some(1)), Classification::L2Only);
        assert_eq!(c.classify(None, Some(1)), Classification::L2Only);
    }

    #[test]
    fn foreign_chain_id_classified_cross_chain() {
        // L2 chainId=1; L1 chainId=31337 (anvil). Tx with chainId=31337
        // = an L1-source deposit intent posted to L2's RPC.
        let l1_chain = 31337u64;
        let c = IngressClassifier::new(HashSet::new(), [l1_chain].into_iter().collect());
        let any_to = address!("4444444444444444444444444444444444444444");
        assert_eq!(
            c.classify(Some(&any_to), Some(l1_chain)),
            Classification::CrossChain,
        );
        // L2-chainId tx with no proxy match → L2Only.
        assert_eq!(c.classify(Some(&any_to), Some(1)), Classification::L2Only);
    }

    #[test]
    fn proxy_match_wins_over_chain_match() {
        // Both signals say cross-chain; result is the same.
        let proxy = address!("5555555555555555555555555555555555555555");
        let c = IngressClassifier::new(
            [proxy].into_iter().collect(),
            [31337u64].into_iter().collect(),
        );
        assert_eq!(
            c.classify(Some(&proxy), Some(31337)),
            Classification::CrossChain
        );
    }
}
