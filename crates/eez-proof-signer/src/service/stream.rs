//! Conversion of one client-streaming RPC body into an admitted window.

use std::num::NonZeroU64;
use std::time::Duration;

use eez_control_rpc::v1::ProveChunk;
use tokio::time::timeout;
use tonic::{Status, Streaming};
use tracing::{Span, debug};

use crate::window;

/// Drain one stream into a complete, structurally admitted [`window::AdmittedWindow`].
///
/// Chunks are checked incrementally for ordering, quotas, adjacency between
/// streamed hash claims, and expected rollup identity. This phase does not execute
/// blocks or validate settlement calldata. `idle_timeout` bounds every wait for
/// a message or stream EOF; the caller enforces the end-to-end deadline. EOF is
/// accepted only after the complete declared span.
pub(super) async fn read_window(
    mut stream: Streaming<ProveChunk>,
    expected_rollup_id: NonZeroU64,
    limits: window::WindowLimits,
    idle_timeout: Duration,
) -> Result<window::AdmittedWindow, Status> {
    let first = next_message(&mut stream, idle_timeout)
        .await?
        .ok_or_else(|| window_status(window::WindowError::EmptyStream))?;
    let assembler = window::WindowAssembler::start(limits, first).map_err(window_status)?;
    let claimed_rollup_id = assembler.claimed_rollup_id();
    let header = assembler.header();
    // The request span exists before the streamed header is available. Record
    // the Composer-declared fields now, before comparing its rollup claim with
    // operator configuration, so a mismatch remains diagnosable.
    let request_span = Span::current();
    request_span.record("wire_rollup_id", claimed_rollup_id);
    request_span.record("declared_from_block", header.declared_from_block);
    request_span.record("declared_to_block", header.declared_to_block);
    request_span.record(
        "declared_blocks",
        header.declared_to_block - header.declared_from_block + 1,
    );
    let mut assembler = assembler
        .verify_rollup_identity(expected_rollup_id)
        .map_err(|_| Status::failed_precondition("window rollup identity rejected"))?;
    debug!(phase = "ingestion", "window header accepted");

    while let Some(chunk) = next_message(&mut stream, idle_timeout).await? {
        assembler.push(chunk).map_err(window_status)?;
    }
    assembler.finish().map_err(window_status)
}

/// Read one frame with an idle timeout and normalize decode-limit failures.
async fn next_message(
    stream: &mut Streaming<ProveChunk>,
    idle_timeout: Duration,
) -> Result<Option<ProveChunk>, Status> {
    timeout(idle_timeout, stream.message())
        .await
        .map_err(|_| Status::deadline_exceeded("Prove stream idle timeout"))?
        .map_err(|error| {
            if error.code() == tonic::Code::OutOfRange {
                Status::resource_exhausted("Prove message exceeds decoding limit")
            } else {
                error
            }
        })
}

/// Distinguish local capacity refusals from malformed stream structure.
fn window_status(error: window::WindowError) -> Status {
    if error.is_resource_exhausted() {
        Status::resource_exhausted(format!("window quota: {error}"))
    } else {
        Status::invalid_argument(format!("window: {error}"))
    }
}
