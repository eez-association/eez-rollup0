//! Cooperative cancellation shared by synchronous request workers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Shared cancellation flag for synchronous request-pipeline workers.
///
/// Running EVM execution is not interruptible. Workers poll this flag before
/// block execution and at selected settlement phase boundaries so detached
/// work stops after the request future exits.
#[derive(Debug, Clone, Default)]
pub(crate) struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
