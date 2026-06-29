//! The composer's posted/attested ledger — the source of truth for the
//! composer-driven prover dispatch.
//!
//! The composer knows BOTH halves of the verified frontier with no new durable
//! state: what it POSTED (it records each deferred window here, via
//! [`PostedWindows::record_posted`]) and what is VERIFIED (the
//! [`ProofSink`](crate::proof_sink) flips `attested` via
//! [`PostedWindows::mark_attested`] AFTER cryptographically verifying the
//! prover's attestation). [`PostedWindows::next_unverified`] is the lowest
//! posted-but-unattested window — the directive the `ProverDispatch` driver
//! sends. The `verified_frontier` (highest contiguous attested `to_block`)
//! advances EXCLUSIVELY via a content-keyed (publicInputsHash) attestation,
//! never via a height the composer asserts.
//!
//! In-memory (like [`ProofStore`](crate::proof_sink::ProofStore)); rebuilt from
//! the L1 cursor on restart (in deferred-post mode everything at or below
//! confirmed-posted is attested by transitivity through L1 —
//! [`PostedWindows::reinit_from_cursor`]). See
//! `docs/composer-driven-prover-design.md`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use alloy_primitives::B256;

/// One posted (deferred) settlement window.
#[derive(Debug, Clone)]
pub struct PostedWindow {
    /// Batch boundary: `posted+1` (the OD-5 anchor block + 1).
    pub from_block: u64,
    /// `sync_height` — the settling block carrying composition.
    pub to_block: u64,
    /// The L2 this settles.
    pub rollup_id: u64,
    /// The hash the prover must reproduce + sign; the [`ProofStore`] key.
    pub public_inputs_hash: B256,
    /// `== PostBatch.current_state` — the directive HINT / cross-check.
    pub current_state: B256,
    /// Flipped true ONLY by a cryptographically-verified attestation.
    pub attested: bool,
    /// Set when the composer fast-forwarded past an unverifiable deep-gap
    /// window (a COVERAGE gap, NOT an attestation). Distinct from `attested`.
    pub fast_forwarded: bool,
    /// Set once this window was ATTESTED and its deferred `postBatch` was
    /// SUBMITTED to L1 (the bundle is in flight, awaiting confirmation). Such a
    /// window is NOT re-dispatched (the prover already verified it; re-verifying
    /// would race the very hash the deferred post is consuming). Cleared on L1
    /// confirmation ([`mark_settled_on_l1`](PostedWindows::mark_settled_on_l1))
    /// or on an L1 reorg demotion ([`demote_above_cursor`](PostedWindows::demote_above_cursor)).
    pub pending_l1: bool,
}

impl PostedWindow {
    /// True once this window no longer needs a directive (proven, skipped, or
    /// attested-and-submitted to L1).
    #[must_use]
    pub fn is_resolved(&self) -> bool {
        self.attested || self.fast_forwarded || self.pending_l1
    }
}

#[derive(Debug, Default)]
struct Ledger {
    /// `sync_height` (== `to_block`) → window. Per-rollup-monotone.
    by_height: BTreeMap<u64, PostedWindow>,
    /// Highest contiguous resolved `to_block`.
    verified_frontier: u64,
    /// Highest `to_block` ever posted.
    highest_posted: u64,
    /// Monotonic tick bumped on every mutation — the dispatch driver's wake
    /// signal (carried on the `notify` watch).
    generation: u64,
}

impl Ledger {
    /// Bump + return the mutation generation (the driver's wake value).
    fn bump(&mut self) -> u64 {
        self.generation += 1;
        self.generation
    }

    /// Advance `verified_frontier` to the highest contiguous resolved
    /// `to_block` above its current value. The first UNresolved window stops
    /// it (a coverage hole must not be jumped silently).
    fn recompute_frontier(&mut self) -> u64 {
        for w in self.by_height.values() {
            if w.to_block <= self.verified_frontier {
                continue;
            }
            if w.is_resolved() {
                self.verified_frontier = w.to_block;
            } else {
                break;
            }
        }
        self.verified_frontier
    }

    /// Drop windows whose anchor is now below the L1-confirmed cursor. They can
    /// never be proven or submitted as-is because their `currentState` belongs to
    /// the pre-cursor root; the next slot must rebuild from `cursor + 1`.
    fn prune_straddling_cursor(&mut self, rollup_id: u64, cursor: u64) -> Vec<PostedWindow> {
        let mut pruned = Vec::new();
        self.by_height.retain(|_, w| {
            let stale = w.rollup_id == rollup_id && w.from_block <= cursor && w.to_block > cursor;
            if stale {
                pruned.push(w.clone());
            }
            !stale
        });
        pruned
    }
}

/// Result of folding an L1-confirmed cursor into the posted-window ledger.
#[derive(Debug, Clone)]
pub struct L1SettleUpdate {
    /// Highest contiguous verified/resolved `to_block` after the update.
    pub frontier: u64,
    /// Windows invalidated because their anchor is at or below the L1-confirmed
    /// cursor while their target is above it. The composer should mark matching
    /// optimistic entries failed so the next sync slot can rebuild promptly.
    pub pruned_straddlers: Vec<PostedWindow>,
}

/// Shared posted/attested ledger. Writers: the composer ([`record_posted`])
/// and the ProofSink ([`mark_attested`]). Reader: the dispatch driver
/// ([`next_unverified`]).
///
/// [`record_posted`]: PostedWindows::record_posted
/// [`mark_attested`]: PostedWindows::mark_attested
/// [`next_unverified`]: PostedWindows::next_unverified
#[derive(Clone, Debug)]
pub struct PostedWindows {
    inner: Arc<Mutex<Ledger>>,
    /// Bumped (a monotonic `generation` tick) on EVERY ledger mutation so the
    /// Phase-2 dispatch driver wakes on BOTH a new post and a frontier advance.
    /// A `watch` (not a `Notify`) so MULTIPLE provers each observe every change
    /// and no wake-up is lost between the driver's state check and its await.
    notify: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Default for PostedWindows {
    fn default() -> Self {
        Self::new()
    }
}

impl PostedWindows {
    #[must_use]
    pub fn new() -> Self {
        let (notify, _rx) = tokio::sync::watch::channel(0u64);
        Self {
            inner: Arc::new(Mutex::new(Ledger::default())),
            notify: Arc::new(notify),
        }
    }

    /// A receiver the dispatch driver `changed().await`s to wake on any ledger
    /// mutation (new post or frontier advance).
    #[must_use]
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.notify.subscribe()
    }

    /// Record a window the composer just posted (deferred), `attested=false`.
    /// Idempotent by `to_block` (a re-post at the same height overwrites).
    pub fn record_posted(&self, w: PostedWindow) {
        let tick = {
            let Ok(mut l) = self.inner.lock() else {
                return;
            };
            // Keep-widest: a narrower UNRESOLVED window with the SAME anchor
            // (from_block) and a LOWER to_block is a strict prefix of this wider
            // one — verifying the wider re-executes it identically and signs the
            // hash the deferred post for the WIDER batch waits on, so the prefix
            // is redundant. Drop it so the dispatch directs the prover at the
            // WIDEST = the actually-deferred batch (else dispatch picks the prefix
            // while the deferred post waits on the wide hash → they never meet).
            // PRESERVE resolved (attested / pending_l1 / fast_forwarded) narrower
            // windows: they are ground truth and may anchor the verified frontier.
            l.by_height.retain(|_, existing| {
                !(existing.from_block == w.from_block
                    && existing.to_block < w.to_block
                    && !existing.is_resolved())
            });
            l.highest_posted = l.highest_posted.max(w.to_block);
            l.by_height.insert(w.to_block, w);
            l.bump()
        };
        self.notify.send_replace(tick);
    }

    /// Flip the window whose `public_inputs_hash` matches to attested and
    /// advance the verified frontier; returns the new frontier. Called by the
    /// ProofSink ONLY after `verify_attestation` passed — so the frontier
    /// advances exclusively on a real attestation, never on a composer-asserted
    /// height.
    pub fn mark_attested(&self, public_inputs_hash: B256) -> u64 {
        let (frontier, tick) = {
            let Ok(mut l) = self.inner.lock() else {
                return 0;
            };
            // Resolve the WIDEST UNRESOLVED window matching the signed hash —
            // iterate by descending to_block to MIRROR `next_to_dispatch` (which
            // dispatches the widest unresolved). The old code marked the FIRST
            // (narrowest, ascending) match and `break`ed: when the dispatched-
            // widest and the resolved-narrowest were different windows sharing a
            // near-constant empty-heartbeat hash, the widest stayed unresolved and
            // the dispatcher re-sent it forever — a re-verification tight loop that,
            // under a state-changing batch, stormed the out-of-process prover to
            // death. Skipping already-`attested` windows keeps this idempotent and,
            // for the (rare) case of several unresolved windows sharing one hash,
            // drains them widest-first across successive attestations.
            for w in l.by_height.values_mut().rev() {
                if !w.attested && w.public_inputs_hash == public_inputs_hash {
                    w.attested = true;
                    break;
                }
            }
            let frontier = l.recompute_frontier();
            (frontier, l.bump())
        };
        self.notify.send_replace(tick);
        frontier
    }

    /// Mark a window terminal WITHOUT an attestation — the bounded fast-forward
    /// recovery for an unverifiable deep-gap window (a coverage gap). Only safe
    /// for a window that never settled (deferred-post) or for a non-binding
    /// prover (self-sign); the caller (the driver/recovery) decides per-mode.
    pub fn mark_fast_forwarded(&self, to_block: u64) -> u64 {
        let (frontier, tick) = {
            let Ok(mut l) = self.inner.lock() else {
                return 0;
            };
            if let Some(w) = l.by_height.get_mut(&to_block) {
                w.fast_forwarded = true;
            }
            let frontier = l.recompute_frontier();
            (frontier, l.bump())
        };
        self.notify.send_replace(tick);
        frontier
    }

    /// Settled-by-transitivity: advance the verified frontier to the L1-confirmed
    /// cursor. Every window with `to_block <= l1_cursor` settled on L1, which
    /// required passing `ECDSAProofSystem.verify` (the registered attester's
    /// signature) — so it is verified by transitivity THROUGH L1 even without a
    /// returned attestation. Resolves the swept windows in place (marks them
    /// attested-by-L1, NOT dropped — preserves observability + stops the dispatch
    /// driver re-issuing their directives) and advances the frontier.
    ///
    /// MONOTONE (only advances): the shared L1 cursor can RETREAT on an L1 reorg,
    /// and a non-monotone follow would pull the frontier backward + un-resolve
    /// already-attested windows. Even when the cursor is not above the current
    /// attestation frontier, we still sweep pending flags and prune straddlers:
    /// a same-block competitor can L1-confirm the exact frontier we already
    /// learned from the ProofSink, making older-anchor wider windows dead.
    /// Returns the new frontier plus any pruned stale windows. Runtime counterpart of the startup-only
    /// [`reinit_from_cursor`](Self::reinit_from_cursor) (which DROPS the swept
    /// windows — correct at startup, wrong mid-flight).
    pub fn mark_settled_on_l1(&self, rollup_id: u64, l1_cursor: u64) -> L1SettleUpdate {
        let (frontier, pruned_straddlers, tick) = {
            let Ok(mut l) = self.inner.lock() else {
                return L1SettleUpdate {
                    frontier: 0,
                    pruned_straddlers: Vec::new(),
                };
            };
            for w in l.by_height.values_mut() {
                if w.rollup_id == rollup_id && w.to_block <= l1_cursor {
                    w.attested = true; // attested-by-transitivity through L1
                    w.pending_l1 = false; // confirmed → no longer in flight
                }
            }
            // Competing-composer safety: a window STRADDLING the new cursor
            // (from_block <= cursor < to_block) is now DEAD — its anchor sits at
            // or below the settled root, so it would revert StateRootMismatch on
            // L1 (e.g. a COMPETING composer settled a DIFFERENT width at this same
            // anchor). Drop it; the composer re-posts a fresh [cursor+1 ..] window.
            // After this, every remaining unresolved window shares the current
            // anchor (cursor+1), so next_to_dispatch's global-widest is the
            // in-flight batch.
            let pruned_straddlers = l.prune_straddling_cursor(rollup_id, l1_cursor);
            if l1_cursor > l.verified_frontier {
                l.verified_frontier = l1_cursor;
            }
            l.highest_posted = l.highest_posted.max(l1_cursor);
            let frontier = l.recompute_frontier();
            (frontier, pruned_straddlers, l.bump())
        };
        self.notify.send_replace(tick);
        L1SettleUpdate {
            frontier,
            pruned_straddlers,
        }
    }

    /// L1 REORG demotion (reorg safety): an L1 reorg retreated the cursor to
    /// `new_cursor` and rolled out the batches above it (possibly a COMPETING
    /// composer's batch that had advanced our rollup, then itself reorged). Every
    /// window with `to_block > new_cursor` that we marked resolved (attested,
    /// pending_l1, or attested-by-transitivity) lost its L1 backing — clear those
    /// flags so it re-enters POSTED and the dispatch re-verifies it before it can
    /// re-settle. WITHOUT this, the monotone [`mark_settled_on_l1`] +
    /// reorg-ignoring composer would leave a reorged-out window FALSELY resolved
    /// forever (a coverage hole). The frontier retracts to `new_cursor` for the
    /// swept range (those windows are now unresolved). Pair with the optimistic
    /// ledger's reorg re-queue, using the SAME `new_cursor` basis.
    pub fn demote_above_cursor(&self, new_cursor: u64) {
        let tick = {
            let Ok(mut l) = self.inner.lock() else {
                return;
            };
            for w in l.by_height.values_mut() {
                if w.to_block > new_cursor {
                    // Demote attestation/pending/fast-forward — the reorged range
                    // must be re-verified against the post-reorg chain.
                    w.attested = false;
                    w.pending_l1 = false;
                    w.fast_forwarded = false;
                }
            }
            // Retract the frontier so the resolved-prefix invariant holds (the
            // swept windows are now unresolved). recompute_frontier won't re-raise
            // it past `new_cursor` until those windows are re-resolved.
            l.verified_frontier = l.verified_frontier.min(new_cursor);
            l.bump()
        };
        self.notify.send_replace(tick);
    }

    /// Mark a window ATTESTED-and-SUBMITTED to L1 (pending confirmation): its
    /// deferred `postBatch` bundle is in flight. The composer calls this right
    /// after submitting the bundle. The window is then skipped by the dispatch
    /// (it is `is_resolved`) until L1 confirms ([`mark_settled_on_l1`]) or a
    /// reorg demotes it ([`demote_above_cursor`]). Keyed by the publicInputsHash
    /// the prover signed (== the window's hash). No-op if the hash isn't found.
    pub fn mark_deferred_pending(&self, public_inputs_hash: B256) {
        let tick = {
            let Ok(mut l) = self.inner.lock() else {
                return;
            };
            for w in l.by_height.values_mut() {
                if w.public_inputs_hash == public_inputs_hash {
                    w.pending_l1 = true;
                    break;
                }
            }
            l.bump()
        };
        self.notify.send_replace(tick);
    }

    /// The lowest posted-but-unresolved window. Retained for the existing
    /// contiguity tests; the driver uses [`next_to_dispatch`](Self::next_to_dispatch).
    #[must_use]
    pub fn next_unverified(&self) -> Option<PostedWindow> {
        let l = self.inner.lock().ok()?;
        l.by_height.values().find(|w| !w.is_resolved()).cloned()
    }

    /// The WIDEST posted-but-unresolved window (highest `to_block`) — the next
    /// dispatch directive in deferred-post + driven mode. Because the deferred
    /// post waits on the NEWEST sealed batch's hash and the in-flight windows
    /// share an anchor while the L1 cursor is frozen ([57..60] ⊂ [57..235]), the
    /// prover must verify the WIDEST (= the actually-deferred batch). Verifying it
    /// covers every interior block; the narrower prefixes were already superseded
    /// in [`record_posted`](Self::record_posted). Skips attested / pending_l1 /
    /// fast_forwarded windows.
    #[must_use]
    pub fn next_to_dispatch(&self) -> Option<PostedWindow> {
        let l = self.inner.lock().ok()?;
        l.by_height
            .values()
            .rev()
            .find(|w| !w.is_resolved())
            .cloned()
    }

    #[must_use]
    pub fn verified_frontier(&self) -> u64 {
        self.inner.lock().map(|l| l.verified_frontier).unwrap_or(0)
    }

    #[must_use]
    pub fn highest_posted(&self) -> u64 {
        self.inner.lock().map(|l| l.highest_posted).unwrap_or(0)
    }

    /// Reinitialize the frontier from the L1-confirmed cursor on (re)start.
    /// In deferred-post mode everything at or below `cursor` is attested by
    /// transitivity through L1 (a batch below cursor passed
    /// `ECDSAProofSystem.verify`, which requires the registered attester's
    /// signature), so seeding `verified_frontier := cursor` loses no real
    /// attestation; the in-flight set above cursor is re-dispatched.
    pub fn reinit_from_cursor(&self, cursor: u64) {
        let tick = {
            let Ok(mut l) = self.inner.lock() else {
                return;
            };
            l.verified_frontier = cursor;
            l.highest_posted = l.highest_posted.max(cursor);
            l.by_height
                .retain(|h, w| *h > cursor && w.from_block > cursor);
            l.bump()
        };
        self.notify.send_replace(tick);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn win(from: u64, to: u64, hash: u8) -> PostedWindow {
        PostedWindow {
            from_block: from,
            to_block: to,
            rollup_id: 1,
            public_inputs_hash: B256::repeat_byte(hash),
            current_state: B256::ZERO,
            attested: false,
            fast_forwarded: false,
            pending_l1: false,
        }
    }

    #[test]
    fn keep_widest_supersedes_unresolved_prefix_but_dispatches_widest() {
        let pw = PostedWindows::new();
        // Same anchor (from=57), growing while the L1 cursor is frozen.
        pw.record_posted(win(57, 60, 0xa)); // [57..60]
        pw.record_posted(win(57, 65, 0xb)); // [57..65] supersedes [57..60]
        pw.record_posted(win(57, 90, 0xc)); // [57..90] supersedes [57..65]
        // Only the WIDEST survives; the dispatch directs the prover at it.
        assert_eq!(pw.next_to_dispatch().unwrap().to_block, 90);
        assert_eq!(
            pw.next_to_dispatch().unwrap().public_inputs_hash,
            B256::repeat_byte(0xc)
        );
    }

    #[test]
    fn keep_widest_preserves_an_attested_prefix() {
        let pw = PostedWindows::new();
        pw.record_posted(win(57, 60, 0xa));
        pw.mark_attested(B256::repeat_byte(0xa)); // [57..60] attested (resolved)
        pw.record_posted(win(57, 90, 0xc)); // wider lands — must NOT drop the attested prefix
        // The attested [57..60] is preserved (frontier anchor); the widest
        // UNRESOLVED to dispatch is [57..90].
        assert_eq!(pw.verified_frontier(), 60);
        assert_eq!(pw.next_to_dispatch().unwrap().to_block, 90);
    }

    #[test]
    fn reattesting_a_shared_hash_resolves_the_widest_not_a_stale_attested_prefix() {
        // Regression for the prover-killing re-verification storm: empty-heartbeat
        // windows carry a near-constant publicInputsHash. When an attested narrow
        // prefix and an UNRESOLVED wider window share that hash, mark_attested must
        // resolve the WIDER (the one next_to_dispatch sent the prover), not no-op on
        // the already-attested prefix — else the dispatcher re-sends the widest
        // forever (729 re-dispatches in 6s observed live, OOM-killing the prover).
        let pw = PostedWindows::new();
        pw.record_posted(win(57, 60, 0xa));
        pw.mark_attested(B256::repeat_byte(0xa)); // [57..60] attested, frontier 60
        // A wider window with the SAME hash lands; keep-widest preserves the
        // attested prefix, so both now coexist sharing hash 0xa.
        pw.record_posted(win(57, 90, 0xa));
        assert_eq!(pw.next_to_dispatch().unwrap().to_block, 90); // widest unresolved
        // Re-attesting the shared hash resolves [57..90] (the dispatched widest),
        // NOT a no-op on [57..60]. Old code marked the narrowest match → churn.
        pw.mark_attested(B256::repeat_byte(0xa));
        assert!(
            pw.next_to_dispatch().is_none(),
            "widest must resolve, not churn"
        );
        assert_eq!(pw.verified_frontier(), 90);
    }

    #[test]
    fn pending_l1_window_is_not_redispatched_until_settle_or_reorg() {
        let pw = PostedWindows::new();
        pw.record_posted(win(57, 90, 0xc));
        pw.mark_attested(B256::repeat_byte(0xc));
        pw.mark_deferred_pending(B256::repeat_byte(0xc)); // bundle submitted
        // Skipped by dispatch while pending L1.
        assert!(pw.next_to_dispatch().is_none());
        // L1 confirms → cleared + frontier follows.
        assert_eq!(pw.mark_settled_on_l1(1, 90).frontier, 90);
        assert!(pw.next_to_dispatch().is_none());
    }

    #[test]
    fn reorg_demotes_attested_above_cursor_back_to_dispatchable() {
        let pw = PostedWindows::new();
        pw.record_posted(win(1, 50, 0xa));
        pw.record_posted(win(51, 90, 0xb));
        pw.mark_settled_on_l1(1, 90); // both resolved-by-transitivity, frontier=90
        assert!(pw.next_to_dispatch().is_none());
        // L1 reorg retreats the cursor to 40 (a competing composer's batch rolled
        // out). Windows above 40 must be re-verified.
        pw.demote_above_cursor(40);
        assert_eq!(pw.verified_frontier(), 40);
        // [51..90] (to_block 90 > 40) is dispatchable again; [1..50] also (50>40).
        assert_eq!(pw.next_to_dispatch().unwrap().to_block, 90);
    }

    #[test]
    fn frontier_advances_only_on_contiguous_attestation() {
        let pw = PostedWindows::new();
        pw.record_posted(win(1, 10, 0xa)); // [1..10]
        pw.record_posted(win(11, 20, 0xb)); // [11..20]
        pw.record_posted(win(21, 30, 0xc)); // [21..30]
        assert_eq!(pw.verified_frontier(), 0);
        assert_eq!(pw.highest_posted(), 30);
        assert_eq!(pw.next_unverified().unwrap().to_block, 10);

        // Attesting the SECOND window does NOT advance the frontier (the first
        // is still unattested — no silent jump over a hole).
        assert_eq!(pw.mark_attested(B256::repeat_byte(0xb)), 0);
        assert_eq!(pw.next_unverified().unwrap().to_block, 10);

        // Attesting the first window advances to 20 (10 + the now-contiguous 20).
        assert_eq!(pw.mark_attested(B256::repeat_byte(0xa)), 20);
        assert_eq!(pw.next_unverified().unwrap().to_block, 30);

        assert_eq!(pw.mark_attested(B256::repeat_byte(0xc)), 30);
        assert!(pw.next_unverified().is_none());
    }

    #[test]
    fn fast_forward_resolves_a_window_as_coverage_gap() {
        let pw = PostedWindows::new();
        pw.record_posted(win(1, 10, 0xa));
        pw.record_posted(win(11, 20, 0xb));
        // Skip the first (deep-gap recovery) → frontier jumps past it.
        assert_eq!(pw.mark_fast_forwarded(10), 10);
        assert_eq!(pw.next_unverified().unwrap().to_block, 20);
        assert_eq!(pw.mark_attested(B256::repeat_byte(0xb)), 20);
    }

    #[test]
    fn reinit_from_cursor_drops_settled_and_straddling_and_seeds_frontier() {
        let pw = PostedWindows::new();
        pw.record_posted(win(1, 10, 0xa));
        pw.record_posted(win(11, 20, 0xb));
        pw.record_posted(win(16, 30, 0xc));
        // Composer restart: cursor confirms [..15] settled on L1.
        pw.reinit_from_cursor(15);
        assert_eq!(pw.verified_frontier(), 15);
        // Windows <=15 and windows straddling 15 are stale; only a fresh
        // cursor+1 anchor remains to re-dispatch.
        assert_eq!(pw.next_unverified().unwrap().to_block, 30);
    }

    #[test]
    fn settled_on_l1_resolves_below_cursor_advances_frontier_and_is_monotone() {
        let pw = PostedWindows::new();
        pw.record_posted(win(1, 10, 0xa)); // [1..10]
        pw.record_posted(win(11, 20, 0xb)); // [11..20]
        pw.record_posted(win(21, 30, 0xc)); // [21..30]
        // The L1 cursor confirms [..20] settled WITHOUT the prover ever
        // attesting them (it fell behind / their composition was evicted): the
        // frontier follows the cursor by transitivity, and the swept windows are
        // RESOLVED (not dropped — unlike reinit) so dispatch skips to [21..30].
        assert_eq!(pw.mark_settled_on_l1(1, 20).frontier, 20);
        assert_eq!(pw.verified_frontier(), 20);
        assert_eq!(pw.next_unverified().unwrap().to_block, 30);
        // Monotone: a lower cursor (an L1 reorg retreat) is a no-op — never pulls
        // the frontier back or un-resolves an already-settled window.
        assert_eq!(pw.mark_settled_on_l1(1, 5).frontier, 20);
        assert_eq!(pw.verified_frontier(), 20);
        assert_eq!(pw.next_unverified().unwrap().to_block, 30);
    }

    #[test]
    fn settled_on_l1_prunes_straddler_even_when_cursor_equals_attested_frontier() {
        let pw = PostedWindows::new();
        pw.record_posted(win(1, 10, 0xa));
        pw.mark_attested(B256::repeat_byte(0xa));

        // A relay drop before L1 settlement can require a same-anchor wider
        // retry; do not treat the attested-only frontier as an L1 cursor.
        pw.record_posted(win(1, 20, 0xb));
        assert_eq!(pw.verified_frontier(), 10);
        assert_eq!(pw.next_to_dispatch().unwrap().to_block, 20);

        // Once L1 confirms cursor=10 (possibly via the competing composer), the
        // wider [1..20] anchor is stale and must not be dispatched again.
        let update = pw.mark_settled_on_l1(1, 10);
        assert_eq!(update.frontier, 10);
        assert_eq!(update.pruned_straddlers.len(), 1);
        assert_eq!(update.pruned_straddlers[0].to_block, 20);
        assert!(pw.next_to_dispatch().is_none());
    }
}
