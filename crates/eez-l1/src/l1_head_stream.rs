//! [`L1HeadStream`]: adapter that converts the [`L1Watcher`]'s
//! `broadcast::Receiver<L1Event>` into an [`L1HeadSource`] for the
//! L1-anchored scheduler in `eez-driver`.
//!
//! Filters [`L1Event::NewHead`] from the broadcast; other variants
//! (`Reorg` / `BatchPosted` / `Finalized`) are consumed but ignored —
//! the Composer / Deriver subscribe to those events through their own
//! receivers.

use async_trait::async_trait;
use eez_driver::{L1HeadInfo, L1HeadSource};
use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::l1_watcher::{L1Event, L1Watcher};

/// Adapter wrapping a [`broadcast::Receiver<L1Event>`] and exposing
/// it as an [`L1HeadSource`].
///
/// Construct via [`Self::from_watcher`]; pass into
/// [`eez_driver::spawn_l1_anchored`].
#[derive(Debug)]
pub struct L1HeadStream {
    rx: broadcast::Receiver<L1Event>,
}

impl L1HeadStream {
    /// Subscribe to the watcher's broadcast and wrap as an
    /// `L1HeadSource`. Each `L1HeadStream` instance gets its own
    /// receiver — passing the same `L1Watcher` to multiple constructors
    /// gives independent streams.
    #[must_use]
    pub fn from_watcher(watcher: &L1Watcher) -> Self {
        Self {
            rx: watcher.subscribe(),
        }
    }
}

#[async_trait]
impl L1HeadSource for L1HeadStream {
    async fn next_head(&mut self) -> Option<L1HeadInfo> {
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
