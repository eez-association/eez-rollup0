//! Control-plane feed server (composer → prover).
//!
//! The embedded driver publishes one [`ControlEvent`] per committed block
//! through a [`ControlPublisher`], which (a) appends it to a bounded
//! replay ring and (b) fans it out on a `broadcast` channel.
//! `ControlFeedSvc` serves `control.v1.ControlFeed`: a subscriber with
//! `from_block >= 1` first receives the buffered events (`block_number >=
//! from_block`, in order), then the live stream — so a reconnecting
//! prover resumes without a gap.
//!
//! Ordering is race-free by construction:
//! - the publisher pushes to the ring BEFORE broadcasting (single
//!   producer — the driver loop is sequential), so everything ever
//!   broadcast is already in the ring;
//! - a subscriber subscribes to the broadcast FIRST, then snapshots the
//!   ring: an event published in between appears in both and is deduped
//!   by the strictly-increasing `block_number` watermark.
//!
//! A subscriber that falls behind the live buffer is DISCONNECTED with
//! `DATA_LOSS` instead of silently skipped (the old `Lagged → continue`):
//! the prover reconnects with replay, turning the gap into recovery.
//!
//! Ported from based-rollup `composer-lib/src/control_feed.rs` (prover-chain
//! P2b). The publisher is fed by the `eez-witness-feed` task (eez-node).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use eez_control_rpc::v1::{control_feed_server::ControlFeed, ControlEvent, SubscribeRequest};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

/// Hard byte cap on the replay ring (witnesses of busy blocks are MBs;
/// the ring must never become the composer's biggest allocation).
const RING_MAX_BYTES: usize = 256 * 1024 * 1024;
/// Capacity of the LIVE broadcast buffer. Small on purpose: it only
/// covers transient subscriber slowness — a subscriber that overruns it
/// is cut with `DATA_LOSS` and recovers via replay.
const LIVE_CAPACITY: usize = 64;
/// Per-subscriber outbound queue (tonic boundary).
const SUBSCRIBER_QUEUE: usize = 64;

/// Publisher + bounded replay ring for the control feed.
///
/// The driver owns one and calls [`publish`](Self::publish) per committed
/// block; `ControlFeedSvc` shares it to serve replays. Events are stored
/// as `Arc` so the ring, the broadcast buffer, and every subscriber share
/// one allocation.
#[derive(Debug)]
pub struct ControlPublisher {
    ring: Mutex<Ring>,
    tx: broadcast::Sender<Arc<ControlEvent>>,
}

#[derive(Debug)]
struct Ring {
    events: VecDeque<Arc<ControlEvent>>,
    bytes: usize,
    max_events: usize,
}

fn event_bytes(ev: &ControlEvent) -> usize {
    let witness = ev.witness.as_ref().map_or(0, |w| {
        w.state
            .iter()
            .chain(&w.codes)
            .chain(&w.keys)
            .chain(&w.headers)
            .map(Vec::len)
            .sum()
    });
    witness + ev.block.len()
}

impl ControlPublisher {
    /// `blocks_per_slot` sizes the replay horizon: the ring holds at
    /// least the last two full slots plus reconnect slack, so a prover
    /// that reconnects within a slot or two can rebuild its window from
    /// the window's first block.
    #[must_use]
    pub fn new(blocks_per_slot: u64) -> Arc<Self> {
        // Floor the replay horizon high enough that a far-behind prover
        // recovering a large from-genesis backlog still finds its directive's
        // SETTLING event in the ring at (re)subscribe time. A settling
        // composition is NEVER reconstructable by backfill (interior blocks
        // carry composition=None — see `backfill_block`), so once the ring
        // evicts a dispatched window's to_block before the prover subscribes,
        // that window can never attest — the "streamed past the directive
        // without a settling composition" stall. ~8k events (~57MB at the
        // observed ~7KB/event, well under RING_MAX_BYTES) buys hours of horizon;
        // steady state is unaffected (the ring merely retains more history, and
        // RING_MAX_BYTES still bounds the memory).
        let max_events = usize::try_from(2 * blocks_per_slot + 8)
            .unwrap_or(usize::MAX)
            .max(8_192);
        let (tx, _) = broadcast::channel(LIVE_CAPACITY);
        Arc::new(Self {
            ring: Mutex::new(Ring {
                events: VecDeque::with_capacity(max_events),
                bytes: 0,
                max_events,
            }),
            tx,
        })
    }

    /// Publish one committed block's event: ring first (unconditional —
    /// an event must be replayable even if no subscriber is connected
    /// yet; `broadcast::send` with zero receivers DROPS the value), then
    /// the live fan-out.
    pub fn publish(&self, event: ControlEvent) {
        let event = Arc::new(event);
        {
            let mut ring = self.ring.lock().expect("control ring poisoned");
            ring.bytes += event_bytes(&event);
            ring.events.push_back(Arc::clone(&event));
            while ring.events.len() > ring.max_events
                || (ring.bytes > RING_MAX_BYTES && ring.events.len() > 1)
            {
                if let Some(evicted) = ring.events.pop_front() {
                    ring.bytes -= event_bytes(&evicted);
                }
            }
        }
        // No receivers → Err; fine — the event is already in the ring.
        let _ = self.tx.send(event);
    }

    /// Snapshot of buffered events with `block_number >= from_block`,
    /// oldest first.
    fn snapshot_from(&self, from_block: u64) -> Vec<Arc<ControlEvent>> {
        self.ring
            .lock()
            .expect("control ring poisoned")
            .events
            .iter()
            .filter(|ev| ev.block_number >= from_block)
            .cloned()
            .collect()
    }

    fn subscribe_live(&self) -> broadcast::Receiver<Arc<ControlEvent>> {
        self.tx.subscribe()
    }
}

/// Serves `control.v1.ControlFeed`: replay (per `from_block`) + live.
#[derive(Clone, Debug)]
pub struct ControlFeedSvc {
    publisher: Arc<ControlPublisher>,
}

impl ControlFeedSvc {
    #[must_use]
    pub fn new(publisher: Arc<ControlPublisher>) -> Self {
        Self { publisher }
    }
}

#[tonic::async_trait]
impl ControlFeed for ControlFeedSvc {
    type SubscribeStream = ReceiverStream<Result<ControlEvent, Status>>;

    async fn subscribe(
        &self,
        req: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let from_block = req.into_inner().from_block;
        // Subscribe FIRST, snapshot SECOND: an event published in between
        // appears in both and is deduped by the watermark below. (The
        // reverse order would *skip* such an event.)
        let mut rx = self.publisher.subscribe_live();
        let replay = if from_block == 0 {
            Vec::new() // live-only (old-client behavior)
        } else {
            self.publisher.snapshot_from(from_block)
        };
        debug!(from_block, replay = replay.len(), "control feed subscriber");

        let (out_tx, out_rx) = mpsc::channel(SUBSCRIBER_QUEUE);
        tokio::spawn(async move {
            // Strictly-increasing block-number watermark dedups duplicates on
            // BOTH the replay and the live path. The driver normally emits
            // strictly increasing numbers, but a reorg re-production can put a
            // number in the ring twice; skipping `<= watermark` keeps a
            // reconnecting prover's window contiguous (a repeated number would
            // break its parent_hash/number check and churn the window).
            let mut watermark: u64 = 0;
            for ev in replay {
                if ev.block_number <= watermark {
                    continue;
                }
                watermark = ev.block_number;
                if out_tx.send(Ok((*ev).clone())).await.is_err() {
                    return;
                }
            }
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        if ev.block_number <= watermark {
                            continue; // replay/live overlap duplicate
                        }
                        watermark = ev.block_number;
                        if out_tx.send(Ok((*ev).clone())).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Fell behind the live buffer: cut the stream so
                        // the subscriber reconnects WITH REPLAY instead of
                        // silently continuing past a hole (the prover's
                        // window would reset mid-slot and the OD-5 gate
                        // would refuse every later settlement).
                        warn!(missed = n, "control feed subscriber lagged; disconnecting");
                        let _ = out_tx
                            .send(Err(Status::data_loss(format!(
                                "subscriber lagged {n} events behind the live buffer; \
                                 reconnect with from_block to replay"
                            ))))
                            .await;
                        return;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(number: u64) -> ControlEvent {
        ControlEvent {
            block_hash: vec![u8::try_from(number % 256).unwrap(); 32],
            block_number: number,
            parent_hash: vec![u8::try_from((number - 1) % 256).unwrap(); 32],
            composition: None,
            witness: None,
            block: vec![0u8; 8],
        }
    }

    #[test]
    fn ring_keeps_newest_and_respects_event_cap() {
        // `new(_)` floors max_events at 8_192 (the replay-horizon floor), so
        // the cap is 8_192 regardless of blocks_per_slot. Publish past the
        // floor to force eviction and assert the ring keeps the newest 8_192.
        let p = ControlPublisher::new(4); // max_events floored to 8_192
        let total = 8_192 + 24; // exceed the floor to trigger eviction
        for n in 1..=total {
            p.publish(ev(n));
        }
        let snap = p.snapshot_from(1);
        assert_eq!(snap.len(), 8_192);
        assert_eq!(snap.first().unwrap().block_number, total - 8_192 + 1);
        assert_eq!(snap.last().unwrap().block_number, total);
    }

    #[test]
    fn snapshot_filters_by_from_block() {
        let p = ControlPublisher::new(4);
        for n in 1..=10 {
            p.publish(ev(n));
        }
        let snap = p.snapshot_from(7);
        assert_eq!(
            snap.iter().map(|e| e.block_number).collect::<Vec<_>>(),
            vec![7, 8, 9, 10]
        );
    }

    #[tokio::test]
    async fn publish_without_subscribers_is_replayable() {
        // The old broadcast-only feed DROPPED events sent with zero
        // receivers; the ring must retain them for late subscribers.
        let p = ControlPublisher::new(4);
        p.publish(ev(1));
        p.publish(ev(2));
        assert_eq!(p.snapshot_from(1).len(), 2);
    }

    #[tokio::test]
    async fn live_after_replay_dedups_overlap() {
        // Simulate the subscribe-then-snapshot overlap: an event that is
        // both in the snapshot AND in the live receiver must reach the
        // subscriber once.
        let p = ControlPublisher::new(4);
        p.publish(ev(1));
        let rx = p.subscribe_live();
        p.publish(ev(2)); // lands in BOTH the ring and rx
        let snapshot = p.snapshot_from(1);
        assert_eq!(snapshot.len(), 2);

        // Replay phase.
        let mut watermark = 0u64;
        let mut delivered: Vec<u64> = Vec::new();
        for e in snapshot {
            watermark = e.block_number;
            delivered.push(e.block_number);
        }
        // Live phase.
        let mut rx = rx;
        while let Ok(e) = rx.try_recv() {
            if e.block_number <= watermark {
                continue;
            }
            watermark = e.block_number;
            delivered.push(e.block_number);
        }
        assert_eq!(delivered, vec![1, 2], "overlap event must be delivered exactly once");
    }
}
