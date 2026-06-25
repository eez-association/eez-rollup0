//! The composer-side `control.v1.ProverDispatch` server — the DIRECTIVE path of
//! the composer-driven prover protocol (Phase 2).
//!
//! Inverts the pull model. Instead of the prover self-picking `from_block` from
//! an in-memory cursor (the corruptible state behind the 4716 retreat class),
//! the composer — which owns the [`PostedWindows`](crate::posted_windows) ledger
//! (what it POSTED + what the `ProofSink` ATTESTED) — STREAMS the oldest
//! posted-but-unverified window as a [`VerifyRange`]. The prover re-executes that
//! whole posted batch and attests via `ProofSink`; the matching attestation
//! flips `attested`, advances `verified_frontier`, and the driver emits the next
//! directive.
//!
//! One directive in flight per stream — mirroring the composer's
//! one-deferred-post-in-flight gate. The directive's `claimed_current_state` /
//! `public_inputs_hash` are HINTS the prover re-derives from the authoritative
//! `PostBatch.abi_calldata`; the frontier advances EXCLUSIVELY on a content-keyed
//! attestation, so a lying directive cannot forge a verified state.
//!
//! Additive + gated: spawned only in deferred-post mode behind
//! `EEZ_COMPOSER_DRIVEN`. A prover that ignores `ProverDispatch` keeps
//! self-picking `from_block`, so a mixed fleet interoperates.

use eez_control_rpc::v1::{
    prover_dispatch_server::ProverDispatch, DispatchRequest, VerifyRange,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::posted_windows::{PostedWindow, PostedWindows};

/// Bounded directive queue. One directive is normally in flight; the slack
/// absorbs a burst of posts the prover hasn't drained yet.
const DISPATCH_QUEUE: usize = 16;

/// The composer's `ProverDispatch` tonic service. Reads the shared
/// [`PostedWindows`] ledger; each `Dispatch` connection gets its own
/// re-execution-driving stream.
#[derive(Debug, Clone)]
pub struct ProverDispatchSvc {
    windows: PostedWindows,
}

impl ProverDispatchSvc {
    #[must_use]
    pub fn new(windows: PostedWindows) -> Self {
        Self { windows }
    }
}

/// The verify directive for a posted window. `claimed_current_state` and
/// `public_inputs_hash` are cross-check HINTS only (the prover recomputes both
/// from `PostBatch.abi_calldata`).
fn to_verify_range(w: &PostedWindow) -> VerifyRange {
    VerifyRange {
        from_block: w.from_block,
        to_block: w.to_block,
        rollup_id: w.rollup_id,
        claimed_current_state: w.current_state.to_vec(),
        public_inputs_hash: w.public_inputs_hash.to_vec(),
    }
}

/// Per-connection dispatch loop: stream the oldest posted-but-unverified window,
/// then wait until it leaves `next_unverified` (its attestation advanced the
/// frontier, or it was fast-forwarded) before emitting the next. Parks on the
/// ledger watch when nothing is unresolved. Exits when the prover disconnects
/// (`out_tx` closed) or the composer drops the ledger (watch closed).
async fn dispatch_loop(windows: PostedWindows, out_tx: mpsc::Sender<Result<VerifyRange, Status>>) {
    // Subscribe BEFORE the first state read so a post landing between the read
    // and the await bumps the watch version — `changed()` then returns
    // immediately instead of losing the wake-up.
    let mut rx = windows.subscribe();
    loop {
        // Emit the WIDEST currently-unresolved window — the actually-deferred
        // batch the composer's deferred post is waiting on. (Narrower same-anchor
        // prefixes were superseded in record_posted; dispatching the prefix would
        // never satisfy the deferred post, which holds the wide batch's hash.)
        while let Some(w) = windows.next_to_dispatch() {
            debug!(
                from_block = w.from_block,
                to_block = w.to_block,
                rollup_id = w.rollup_id,
                "ProverDispatch: dispatching verify directive (widest in-flight)",
            );
            if out_tx.send(Ok(to_verify_range(&w))).await.is_err() {
                return; // prover disconnected
            }
            // Wait until THIS window (identified by to_block, the unique ledger
            // key) is no longer the widest unresolved one — it became attested /
            // pending_l1 / settled / fast-forwarded, OR a WIDER window was posted
            // (the composer sealed a newer sync block) and now leads, in which
            // case we re-emit the wider one.
            while matches!(
                windows.next_to_dispatch(),
                Some(n) if n.to_block == w.to_block
            ) {
                if !park(&mut rx, &out_tx).await {
                    return;
                }
            }
        }
        // Nothing to dispatch — park until the ledger changes (a new post) or
        // the prover disconnects.
        if !park(&mut rx, &out_tx).await {
            return;
        }
    }
}

/// Wait for either a ledger mutation (the watch ticked) or the prover
/// disconnecting (`out_tx` closed). Returns `true` to keep looping, `false` to
/// exit. Selecting on `out_tx.closed()` lets a parked driver notice a dropped
/// stream WITHOUT a ledger mutation — otherwise the task would linger until the
/// next post made `send` fail.
async fn park(
    rx: &mut tokio::sync::watch::Receiver<u64>,
    out_tx: &mpsc::Sender<Result<VerifyRange, Status>>,
) -> bool {
    tokio::select! {
        changed = rx.changed() => changed.is_ok(), // false ⇒ composer dropped the ledger
        () = out_tx.closed() => false,             // prover disconnected
    }
}

#[tonic::async_trait]
impl ProverDispatch for ProverDispatchSvc {
    type DispatchStream = ReceiverStream<Result<VerifyRange, Status>>;

    async fn dispatch(
        &self,
        req: Request<DispatchRequest>,
    ) -> Result<Response<Self::DispatchStream>, Status> {
        let prover_epoch = req.into_inner().prover_epoch;
        info!(
            prover_epoch,
            verified_frontier = self.windows.verified_frontier(),
            highest_posted = self.windows.highest_posted(),
            "ProverDispatch: prover connected; driving from the posted/attested ledger",
        );
        let windows = self.windows.clone();
        let (out_tx, out_rx) = mpsc::channel(DISPATCH_QUEUE);
        tokio::spawn(async move {
            dispatch_loop(windows, out_tx).await;
        });
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    fn win(from: u64, to: u64, hash: u8) -> PostedWindow {
        PostedWindow {
            from_block: from,
            to_block: to,
            rollup_id: 1,
            public_inputs_hash: B256::repeat_byte(hash),
            current_state: B256::repeat_byte(0x11),
            attested: false,
            fast_forwarded: false,
            pending_l1: false,
        }
    }

    #[tokio::test]
    async fn dispatches_widest_and_advances_on_attestation() {
        let windows = PostedWindows::new();
        windows.record_posted(win(1, 10, 0xa));

        let (tx, mut rx) = mpsc::channel(8);
        let driver = windows.clone();
        let handle = tokio::spawn(async move { dispatch_loop(driver, tx).await });

        // First directive = the oldest window (A).
        let d1 = rx.recv().await.unwrap().unwrap();
        assert_eq!(d1.from_block, 1);
        assert_eq!(d1.to_block, 10);
        assert_eq!(d1.public_inputs_hash, B256::repeat_byte(0xa).to_vec());
        assert_eq!(d1.claimed_current_state, B256::repeat_byte(0x11).to_vec());

        // Post B, attest A → the driver advances to B (A left next_unverified).
        windows.record_posted(win(11, 20, 0xb));
        windows.mark_attested(B256::repeat_byte(0xa));
        let d2 = rx.recv().await.unwrap().unwrap();
        assert_eq!(d2.from_block, 11);
        assert_eq!(d2.to_block, 20);

        // Prover disconnects → the loop exits via `out_tx.closed()`, no nudge.
        drop(rx);
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("dispatch_loop did not exit on disconnect")
            .unwrap();
    }

    #[tokio::test]
    async fn fast_forward_skips_the_widest_then_dispatches_the_next() {
        let windows = PostedWindows::new();
        windows.record_posted(win(1, 10, 0xa)); // [1..10]
        windows.record_posted(win(11, 20, 0xb)); // [11..20]

        let (tx, mut rx) = mpsc::channel(8);
        let driver = windows.clone();
        let handle = tokio::spawn(async move { dispatch_loop(driver, tx).await });

        // Widest-first: B (to_block 20) is dispatched first.
        assert_eq!(rx.recv().await.unwrap().unwrap().to_block, 20);
        // Fast-forward B (coverage gap, not an attestation) → driver moves to the
        // next-widest unresolved = A.
        windows.mark_fast_forwarded(20);
        assert_eq!(rx.recv().await.unwrap().unwrap().to_block, 10);

        drop(rx);
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("dispatch_loop did not exit on disconnect")
            .unwrap();
    }
}
