//! What the batch scan reports downstream: one [`Post`] per
//! `postAndVerifyBatch` call that listed our rollup.

use std::collections::HashSet;

use alloy_primitives::{Address, B256};
use eez_protocol::abi::EvmBatch;

pub use eez_protocol::entries::EntryRole;

/// A post's identity: its `BatchPosted` log. The block hash names the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PostId {
    pub l1_block: u64,
    pub l1_block_hash: B256,
    /// Index of the `BatchPosted` log in its block.
    pub log_index: u64,
}

impl PostId {
    /// L1 order on one chain: block, then log.
    #[must_use]
    pub const fn position(&self) -> (u64, u64) {
        (self.l1_block, self.log_index)
    }
}

/// One `postAndVerifyBatch` call that listed our rollup, read from L1.
#[derive(Debug, Clone)]
pub struct Post {
    pub id: PostId,
    pub tx_hash: B256,
    /// EEZ's `msg.sender` for this call.
    pub sender: Address,
    /// Signer of the outer L1 transaction, including wrapper-mediated posts.
    pub tx_sender: Address,
    pub batch: EvmBatch,
    /// Candidate-V1 carrier: the versioned hashes `blobIndices` selected, in
    /// order. Empty for the calldata profile, whose stream is `batch.callData`.
    pub blob_hashes: Vec<B256>,
    /// What L1 applied of it for our rollup.
    pub settlement: Settlement,
}

/// One entry L1 applied, with what it contributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedEntry {
    /// Index into the on-chain `batch.entries`.
    pub entry_index: usize,
    pub role: EntryRole,
}

/// Which of a post's entries L1 applied for our rollup, read from its own
/// consumption events rather than inferred from the roots it emitted.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Settlement {
    /// Application (log) order, which can differ from index order across queues.
    applied: Vec<AppliedEntry>,
    /// Our root after the last applied entry: L1's commitment, the only valid endpoint.
    pub final_state: Option<B256>,
    /// The stored root the applied run started from; a rival's endpoint on a resume.
    pub entry_state: Option<B256>,
    /// The run did not start at the post's first entry for our rollup.
    resumed: bool,
}

impl Settlement {
    /// Nothing of this post applied on L1.
    pub const NONE: Self = Self {
        applied: Vec::new(),
        final_state: None,
        entry_state: None,
        resumed: false,
    };

    /// Build from the applied entries in application order.
    #[must_use]
    pub fn new(
        applied: Vec<AppliedEntry>,
        final_state: Option<B256>,
        entry_state: Option<B256>,
        resumed: bool,
    ) -> Self {
        debug_assert!(
            {
                let mut seen = HashSet::new();
                applied.iter().all(|entry| seen.insert(entry.entry_index))
            },
            "an entry cannot apply twice: {applied:?}",
        );
        Self {
            applied,
            final_state,
            entry_state,
            resumed,
        }
    }

    /// True when L1 ran none of this post's entries: its claims are phantoms.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.applied.is_empty()
    }

    /// The applied entries, in application order.
    #[must_use]
    pub fn applied(&self) -> &[AppliedEntry] {
        &self.applied
    }

    /// Just the applied entry indices, for diagnostics.
    #[must_use]
    pub fn applied_indices(&self) -> Vec<usize> {
        self.applied.iter().map(|entry| entry.entry_index).collect()
    }

    /// The first entry L1 applied, where the applied run starts.
    #[must_use]
    pub fn first_applied(&self) -> Option<usize> {
        self.applied.first().map(|entry| entry.entry_index)
    }

    /// A mid-chain resume: in a multi-rollup batch, entry 0 may not be ours.
    #[must_use]
    pub fn resumed(&self) -> bool {
        self.resumed
    }
}

#[cfg(test)]
mod tests {
    use super::{AppliedEntry, EntryRole, Settlement};

    fn applied(entry_index: usize) -> AppliedEntry {
        AppliedEntry {
            entry_index,
            role: EntryRole::Anchor,
        }
    }

    /// Application order is kept; `resumed` is what the scan recorded.
    #[test]
    fn application_order_is_kept_and_resumed_is_recorded() {
        assert!(Settlement::NONE.is_empty() && !Settlement::NONE.resumed());
        let settlement = Settlement::new(vec![applied(4), applied(3)], None, None, true);
        assert_eq!(settlement.applied_indices(), vec![4, 3]);
        assert_eq!(settlement.first_applied(), Some(4));
        assert!(settlement.resumed());
        // The stored flag, not `!is_empty()`.
        let unresumed = Settlement::new(vec![applied(1)], None, None, false);
        assert!(!unresumed.is_empty() && !unresumed.resumed());
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "cannot apply twice")]
    fn an_entry_applied_twice_is_rejected() {
        let _ = Settlement::new(vec![applied(2), applied(2)], None, None, false);
    }
}
