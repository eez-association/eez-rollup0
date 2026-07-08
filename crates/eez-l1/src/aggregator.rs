//! Stage-N Aggregator scaffolding — when N Sequencers send
//! [`BatchCandidate`](eez_driver::BatchCandidate)s on per-rollup
//! channels, the Aggregator drains them in a `select!` and signals
//! the Submitter when a complete multi-rollup window is ready.
//!
//! Stage 4 ships single-rollup pass-through: the
//! [`Composer`](crate::Composer) owns the receiver directly and there
//! is no Aggregator struct. Only [`SubmitTrigger`] is defined here so
//! call sites can name the policy. The Aggregator type itself lands
//! when stage-N adds the second L2.

use std::time::Duration;

/// When should the Aggregator hand the assembled batch to the
/// Submitter?
///
/// Stage 4 only implements [`Self::Interval`]; the others are
/// scaffolding for stage-N's multi-rollup Aggregator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubmitTrigger {
    /// Submit on every received [`BatchCandidate`], then enforce at
    /// least `Duration` between submissions. Stage-4 single-rollup
    /// default; combined with
    /// [`eez_driver::BatchPolicy::EveryKBlocks`] on the producer, the
    /// effective cadence is `K * L2_block_time`.
    Interval(Duration),
    /// Fire the moment every participating rollup has signalled
    /// `terminal: true`. Stage-N sync-slot default.
    OnTerminal,
    /// Hybrid: fire on terminal-from-every-rollup OR after `Duration`,
    /// whichever first. Handles silent rollups (no cross-chain
    /// content) without making everyone wait.
    OnTerminalOrAfter(Duration),
}
