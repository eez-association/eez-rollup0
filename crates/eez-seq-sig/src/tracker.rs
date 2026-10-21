use std::collections::HashMap;

use alloy_primitives::{Address, B256};

use crate::VerifiedHead;

/// How far ahead of local time a claim's `epoch_ms` may be.
pub const MAX_FUTURE_MS: u64 = 10_000;
/// Bound on tracked sources; the lowest `epoch_ms` is evicted past it.
pub const MAX_SOURCES: usize = 8;

/// One signer process: a signer key plus its restart epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Source {
    pub signer: Address,
    pub epoch_ms: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Observation {
    New,
    Duplicate,
    Stale,
    Future,
    Equivocation { kept: B256 },
}

/// Keeps the latest head of each source.
#[derive(Debug, Default)]
pub struct HeadTracker {
    latest: HashMap<Source, VerifiedHead>,
}

impl HeadTracker {
    pub fn observe(&mut self, head: VerifiedHead, now_ms: u64) -> Observation {
        if head.claim.epoch_ms > now_ms.saturating_add(MAX_FUTURE_MS) {
            return Observation::Future;
        }
        let source = Source {
            signer: head.signer,
            epoch_ms: head.claim.epoch_ms,
        };
        if let Some(cur) = self.latest.get(&source) {
            let (c, h) = (cur.claim.counter, cur.claim.block_hash);
            return match head.claim.counter.cmp(&c) {
                std::cmp::Ordering::Less => Observation::Stale,
                std::cmp::Ordering::Equal if head.claim.block_hash == h => Observation::Duplicate,
                std::cmp::Ordering::Equal => Observation::Equivocation { kept: h },
                std::cmp::Ordering::Greater => {
                    self.latest.insert(source, head);
                    Observation::New
                }
            };
        }
        self.latest.insert(source, head);
        if self.latest.len() > MAX_SOURCES {
            let oldest = *self
                .latest
                .keys()
                .min_by_key(|s| s.epoch_ms)
                .expect("non-empty");
            self.latest.remove(&oldest);
            if oldest == source {
                return Observation::Stale;
            }
        }
        Observation::New
    }

    /// Latest head of every source, highest `Version` first.
    pub fn candidates(&self) -> Vec<&VerifiedHead> {
        let mut heads: Vec<_> = self.latest.values().collect();
        heads.sort_by_key(|h| std::cmp::Reverse((h.claim.version(), h.signer)));
        heads
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeadClaim;

    fn head(epoch_ms: u64, counter: u64, hash: u8) -> VerifiedHead {
        VerifiedHead {
            claim: HeadClaim {
                rollup_id: 1,
                number: counter,
                block_hash: B256::repeat_byte(hash),
                parent_hash: B256::ZERO,
                epoch_ms,
                counter,
            },
            signer: Address::repeat_byte(1),
            block: None,
        }
    }

    #[test]
    fn higher_counter_replaces_within_source() {
        let mut t = HeadTracker::default();
        assert_eq!(t.observe(head(100, 1, 1), 100), Observation::New);
        assert_eq!(t.observe(head(100, 2, 2), 100), Observation::New);
        assert_eq!(t.candidates().len(), 1);
        assert_eq!(t.candidates()[0].claim.counter, 2);
    }

    #[test]
    fn lower_counter_is_stale() {
        let mut t = HeadTracker::default();
        t.observe(head(100, 5, 1), 100);
        assert_eq!(t.observe(head(100, 3, 2), 100), Observation::Stale);
        assert_eq!(t.candidates()[0].claim.counter, 5);
    }

    #[test]
    fn same_counter_same_hash_is_duplicate() {
        let mut t = HeadTracker::default();
        t.observe(head(100, 5, 1), 100);
        assert_eq!(t.observe(head(100, 5, 1), 100), Observation::Duplicate);
    }

    #[test]
    fn same_counter_other_hash_is_equivocation() {
        let mut t = HeadTracker::default();
        t.observe(head(100, 5, 1), 100);
        assert_eq!(
            t.observe(head(100, 5, 2), 100),
            Observation::Equivocation {
                kept: B256::repeat_byte(1)
            }
        );
        assert_eq!(t.candidates()[0].claim.block_hash, B256::repeat_byte(1));
    }

    #[test]
    fn restarted_source_outranks_old_epoch() {
        let mut t = HeadTracker::default();
        t.observe(head(100, 10_000, 1), 200);
        t.observe(head(200, 1, 2), 200);
        assert_eq!(t.candidates()[0].claim.epoch_ms, 200);
    }

    #[test]
    fn replay_after_restart_is_superseded() {
        let mut t = HeadTracker::default();
        assert_eq!(t.observe(head(100, 3, 1), 100), Observation::New);
        assert_eq!(t.observe(head(100, 9, 2), 100), Observation::New);
        assert_eq!(t.candidates()[0].claim.counter, 9);
    }

    #[test]
    fn future_epoch_is_rejected() {
        let mut t = HeadTracker::default();
        assert_eq!(t.observe(head(11_001, 1, 1), 1_000), Observation::Future);
        assert!(t.candidates().is_empty());
    }

    #[test]
    fn oldest_source_is_evicted_past_cap() {
        let mut t = HeadTracker::default();
        for e in 1..=9 {
            t.observe(head(e, 1, 1), 100);
        }
        let c = t.candidates();
        assert_eq!(c.len(), MAX_SOURCES);
        assert!(c.iter().all(|h| h.claim.epoch_ms != 1));
    }
}
