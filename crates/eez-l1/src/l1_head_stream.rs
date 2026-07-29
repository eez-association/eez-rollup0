//! [`L1HeadStream`]: adapter that converts the [`L1Watcher`]'s
//! `broadcast::Receiver<L1Event>` into the head feed the
//! L1-anchored scheduler in `eez-driver` polls.
//!
//! Filters [`L1Event::NewHead`] from the broadcast; other variants
//! (`Reorg` / `BatchPosted` / `Finalized`) are consumed but ignored —
//! the Composer / Deriver subscribe to those events through their own
//! receivers.

use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::l1_watcher::{L1Event, L1Watcher};

/// Minimal L1 head info the L1-anchored spawner needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct L1HeadInfo {
    pub block_number: u64,
    pub block_hash: [u8; 32],
    pub timestamp: u64,
}

/// Adapter wrapping a [`broadcast::Receiver<L1Event>`] and exposing
/// canonical L1 heads via [`next_head`](Self::next_head).
///
/// Construct via [`Self::from_watcher`]; pass into
/// `eez_driver::spawn_l1_anchored`.
#[derive(Debug)]
pub struct L1HeadStream {
    rx: broadcast::Receiver<L1Event>,
}

impl L1HeadStream {
    /// Subscribe to the watcher's broadcast. Each `L1HeadStream`
    /// instance gets its own
    /// receiver — passing the same `L1Watcher` to multiple constructors
    /// gives independent streams.
    #[must_use]
    pub fn from_watcher(watcher: &L1Watcher) -> Self {
        Self {
            rx: watcher.subscribe(),
        }
    }

    /// `next_head` returns `None` when the source closes (broadcast lagged
    /// past tolerance, `L1Watcher` task died, etc) — the spawner exits
    /// cleanly so the surrounding `spawn_critical_task` notices.
    ///
    /// `next_head` is cancel-safe: `spawn_l1_anchored` polls it
    /// inside a `tokio::select!` alongside its Live ticker, so the future
    /// is dropped and re-created whenever another branch wins. The
    /// cancel-safety comes via `broadcast::Receiver::recv`
    /// (no event is consumed by a cancelled call).
    pub async fn next_head(&mut self) -> Option<L1HeadInfo> {
        loop {
            match self.rx.recv().await {
                Ok(L1Event::NewHead {
                    block_number,
                    block_hash,
                    timestamp,
                }) => {
                    return Some(L1HeadInfo {
                        block_number,
                        block_hash: block_hash.0,
                        timestamp,
                    });
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    event!(
                        name: "eez.l1_head_stream.lagged",
                        Level::WARN,
                        skipped,
                        "L1 event stream lagged; resync on next NewHead",
                    );
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}
