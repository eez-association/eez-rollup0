//! Top-level composition entry point.
//!
//! [`compose_transaction`] is the single-call entry point for running
//! one cross-chain composition end-to-end. Three lines of real work:
//! construct a [`CompositionBuilder`] with the pinned entry id + pre-
//! built rollup map, drive source simulation (which dispatches every
//! detected proxy call back into the builder), finalize.
//!
//! ```text
//!   compose_transaction(entry_client, raw_tx, entry_id, rollups):
//!     builder = CompositionBuilder::new(entry_id, rollups)
//!     entry_client.simulate_source_tx(raw_tx, &mut builder).await?
//!     builder.finalize(raw_tx).await
//! ```
//!
//! Used in tests or one-shot scripts when you already hold concrete
//! handles. Long-lived services use [`Composer`](crate::Composer), which
//! caches clients and opens sessions on each call before delegating
//! here.
//!
//! The pipeline has no reth dep, so the whole
//! composition can be tested against in-memory fakes.

use std::collections::HashMap;

use crate::composition::{CompositionBuilder, Rollup};
use crate::error::CompositionResult;
use crate::executor::EntryChainClient;
use crate::rollup_id::RollupId;
use crate::types::{Composition, ExecutedAction};

/// Run one cross-chain composition end-to-end.
///
/// Takes the entry client by reference (source simulation runs
/// in-process on the entry chain) and the full per-rollup `rollups`
/// map (including the entry). `entry_id` must be a key in `rollups`;
/// the composer layer enforces this before calling.
///
/// Per-target CCM verification runs inside `finalize` for every
/// **non-entry** rollup whose [`build_batch`](crate::entries::build_batch)
/// result is non-empty (per
/// [`EvmBatch::is_empty`](crate::batch::EvmBatch::is_empty)).
///
/// # Errors
///
/// Returns any [`CompositionError`](crate::error::CompositionError)
/// surfaced by `simulate_source_tx` (decode / provider / EVM / per-call
/// dispatch) or `finalize` (empty inputs, unknown target, invalid
/// checkpoint, CCM failure).
pub async fn compose_transaction(
    entry_client: &(dyn EntryChainClient + Send + Sync),
    raw_tx: &[u8],
    entry_id: RollupId,
    rollups: HashMap<RollupId, Rollup>,
) -> CompositionResult<Composition> {
    compose_transaction_recorded(entry_client, raw_tx, entry_id, rollups)
        .await
        .map(|(composition, _recorded)| composition)
}

/// Same as [`compose_transaction`] but ALSO returns the builder's
/// `recorded[..]` (the preorder list of dispatched cross-chain calls with
/// their resolved outcomes) captured BEFORE `finalize` consumes the
/// builder.
///
/// Callers that need a call's resolved `outcome` — e.g. the inbound L1→L2
/// delivery, which builds the byte-locked `executeIncomingCrossChainCall`
/// system tx from the L2 target's REAL `return_data` — use this; the
/// [`Composition`] alone does not carry verbatim per-call return data.
///
/// # Errors
///
/// Same as [`compose_transaction`].
#[tracing::instrument(
    level = "debug",
    name = "compose_transaction",
    skip_all,
    fields(entry = %entry_id, tx_len = raw_tx.len()),
    err,
)]
pub async fn compose_transaction_recorded(
    entry_client: &(dyn EntryChainClient + Send + Sync),
    raw_tx: &[u8],
    entry_id: RollupId,
    rollups: HashMap<RollupId, Rollup>,
) -> CompositionResult<(Composition, Vec<ExecutedAction>)> {
    let mut builder = CompositionBuilder::new(entry_id, rollups);
    entry_client
        .simulate_source_tx(raw_tx.to_vec(), &mut builder)
        .await?;
    let recorded = builder.recorded().to_vec();
    let composition = builder.finalize(raw_tx).await?;
    Ok((composition, recorded))
}

// This pipeline is exercised by the in-crate `#[cfg(test)]` modules
// (e.g. `composition`, `composer`) against scripted mock clients and
// the real EVM entry builders.
