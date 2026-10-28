//! Blob sequencing budget for locally produced blocks; imports use consensus rules.

use std::sync::{Arc, OnceLock, Weak};

use eez_driver::{SyncSlotComposer, SyncSlotComposerHandle};
use reth_primitives_traits::SealedHeader;

#[derive(Debug)]
pub struct SequencingDataGuard {
    rollup_id: u64,
    composer: OnceLock<Weak<dyn SyncSlotComposer>>,
}

impl SequencingDataGuard {
    pub fn new(rollup_id: u64) -> Self {
        Self {
            rollup_id,
            composer: OnceLock::new(),
        }
    }

    pub fn bind(&self, composer: &SyncSlotComposerHandle) -> eyre::Result<()> {
        self.composer
            .set(Arc::downgrade(composer))
            .map_err(|_| eyre::eyre!("sequencing DA guard already bound"))
    }

    pub(crate) fn budget(
        &self,
        parent: &SealedHeader<alloy_consensus::Header>,
        timestamp: u64,
    ) -> Result<usize, String> {
        self.composer
            .get()
            .and_then(Weak::upgrade)
            .ok_or("sequencing DA guard not ready")?
            .ordinary_da_budget(self.rollup_id, parent, timestamp)?
            .ok_or_else(|| "blob sequencing requires a DA budget".into())
    }
}
