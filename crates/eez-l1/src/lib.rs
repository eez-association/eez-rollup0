//! L1 anchor primitives for eez-rollup0.
//!
//! Pure primitives — no orchestration. Stage-4 moved the Composer
//! orchestration into the [`eez-composer`](https://docs.rs/eez-composer)
//! umbrella crate; `eez-l1` now exposes only the L1-side building
//! blocks the umbrella (and the Deriver) compose with.
//!
//! - [`L1Reader`] — read-only historical batch scans and canonicality checks.
//! - [`Submitter`] — the signed L1 send primitive: `postAndVerifyBatch`
//!   via the bundle relay.
//! - [`L1Watcher`] — polls L1, fans out `NewHead` / `BatchPosted` /
//!   `Reorg` / `Finalized` over a broadcast channel.
//! - [`L1CanonicalHead`] — shared `posted_through` cursor (Deriver
//!   writes; Composer / Sequencer / others read).

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod config;
pub mod error;
pub mod l1_canonical_head;
pub mod l1_head_stream;
pub mod l1_reader;
pub mod l1_watcher;
pub mod scan;
pub mod submitter;

#[doc(inline)]
pub use config::{L1ReaderConfig, SubmitterConfig};
#[doc(inline)]
pub use error::{L1Error, L1Result};
#[doc(inline)]
pub use l1_canonical_head::{BatchRecord, L1CanonicalHead};
#[doc(inline)]
pub use l1_head_stream::{L1HeadInfo, L1HeadStream};
#[doc(inline)]
pub use l1_reader::{L1Reader, L1Readiness};
#[doc(inline)]
pub use l1_watcher::{L1Event, L1Watcher, L1WatcherConfig};
#[doc(inline)]
pub use scan::{BatchLogChunks, ScannedBatch, Settlement};
#[doc(inline)]
pub use submitter::{BundleTarget, SendOutcome, Submitter};
