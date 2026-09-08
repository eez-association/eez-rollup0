//! Boot checkpoint for the L1 scan.
//!
//! A fresh boot has an empty [`L1CanonicalHead`](eez_l1::L1CanonicalHead), so
//! catch-up rescans from the registry deploy block and re-verifies every batch
//! it already agreed with. That is cheap while the head tracks the cursor, but
//! a chain containing a same-height replacement (a resumed batch) forces a
//! state read tens of thousands of blocks below the head, and boot stops
//! finishing at all (`docs/issues/deep-reverify-cost.md`).
//!
//! This records the last batch whose effects are committed locally. Boot seeds
//! the index from it and scans forward, so the cost is O(gap) not O(history).
//!
//! Nothing here is trusted. The loader re-checks the root against local state;
//! the seeded record's L1 hash is re-checked by `revalidate_index_tail`. Either
//! check failing falls back to the full scan — slow, never wrong.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use alloy_primitives::B256;
use tracing::{Level, event};

/// File under the L2 datadir. Plain text so an operator can read it.
const FILE_NAME: &str = "eez-reconcile-checkpoint";

/// Bumped when the field set changes; an older file is discarded, not guessed.
const VERSION: &str = "eez-reconcile-checkpoint v1";

/// Last batch whose effects are committed to local L2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileCheckpoint {
    /// L1 block carrying the batch. The scan resumes here.
    pub l1_block: u64,
    /// Hash of that L1 block, so a reorg below it is detectable.
    pub l1_block_hash: B256,
    /// The batch's L1 tx hash; lets the rescan dedup the seeded record.
    pub tx_hash: B256,
    /// Highest L2 block this batch confirmed.
    pub l2_cursor: u64,
    /// Local L2 state root at `l2_cursor`, to catch a mismatched datadir.
    pub l2_state_root: B256,
}

impl ReconcileCheckpoint {
    /// Checkpoint path for an L2 datadir.
    #[must_use]
    pub fn path(datadir: &Path) -> PathBuf {
        datadir.join(FILE_NAME)
    }

    /// Write atomically: temp file, fsync, rename. A crash mid-write leaves
    /// either the previous checkpoint or none, never a torn one.
    ///
    /// # Errors
    /// Any filesystem failure creating, writing, syncing, or renaming.
    pub fn save(&self, datadir: &Path) -> io::Result<()> {
        let final_path = Self::path(datadir);
        let tmp_path = final_path.with_extension("tmp");
        {
            let mut file = fs::File::create(&tmp_path)?;
            io::Write::write_all(&mut file, self.encode().as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        // Durability of the rename itself; not fatal if the FS refuses.
        if let Ok(dir) = fs::File::open(datadir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Load, or `None` if absent, unreadable, or not parseable. A bad file is
    /// reported and ignored: the caller's fallback is a correct full scan.
    #[must_use]
    pub fn load(datadir: &Path) -> Option<Self> {
        let path = Self::path(datadir);
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return None,
            Err(err) => {
                event!(
                    name: "eez.deriver.checkpoint.unreadable",
                    Level::WARN,
                    path = %path.display(),
                    error = %err,
                    "boot checkpoint unreadable; falling back to a full scan",
                );
                return None;
            }
        };
        match Self::decode(&raw) {
            Some(cp) => Some(cp),
            None => {
                event!(
                    name: "eez.deriver.checkpoint.malformed",
                    Level::WARN,
                    path = %path.display(),
                    "boot checkpoint malformed; falling back to a full scan",
                );
                None
            }
        }
    }

    /// Whether this checkpoint may seed the boot scan. It is only a cache, so
    /// both recorded facts are re-proved against live L1 and local L2; `Err`
    /// names the reason and the caller rescans from the deploy block.
    ///
    /// Unknown counts as reject: an L1 that will not serve the block, or an L2
    /// missing that height, is exactly when trusting a stale cursor is unsafe.
    pub fn usable_with(
        &self,
        canonical_l1_hash: Option<B256>,
        local_l2_root: Option<B256>,
    ) -> Result<(), &'static str> {
        match canonical_l1_hash {
            None => return Err("L1 did not serve the checkpoint block"),
            Some(hash) if hash != self.l1_block_hash => {
                return Err("checkpoint L1 block was reorged out");
            }
            Some(_) => {}
        }
        match local_l2_root {
            None => return Err("local L2 has no block at the checkpoint cursor"),
            Some(root) if root != self.l2_state_root => {
                return Err("local L2 root differs from the checkpoint");
            }
            Some(_) => {}
        }
        Ok(())
    }

    fn encode(&self) -> String {
        format!(
            "{VERSION}\nl1_block={}\nl1_block_hash={:#x}\ntx_hash={:#x}\nl2_cursor={}\nl2_state_root={:#x}\n",
            self.l1_block, self.l1_block_hash, self.tx_hash, self.l2_cursor, self.l2_state_root,
        )
    }

    fn decode(raw: &str) -> Option<Self> {
        let mut lines = raw.lines();
        if lines.next()?.trim() != VERSION {
            return None;
        }
        let mut l1_block = None;
        let mut l1_block_hash = None;
        let mut tx_hash = None;
        let mut l2_cursor = None;
        let mut l2_state_root = None;
        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let (key, value) = line.split_once('=')?;
            match key {
                "l1_block" => l1_block = Some(value.parse().ok()?),
                "l1_block_hash" => l1_block_hash = Some(value.parse().ok()?),
                "tx_hash" => tx_hash = Some(value.parse().ok()?),
                "l2_cursor" => l2_cursor = Some(value.parse().ok()?),
                "l2_state_root" => l2_state_root = Some(value.parse().ok()?),
                // An unknown key means a newer writer; refuse rather than
                // guess which fields still mean what they used to.
                _ => return None,
            }
        }
        Some(Self {
            l1_block: l1_block?,
            l1_block_hash: l1_block_hash?,
            tx_hash: tx_hash?,
            l2_cursor: l2_cursor?,
            l2_state_root: l2_state_root?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ReconcileCheckpoint {
        ReconcileCheckpoint {
            l1_block: 27_081,
            l1_block_hash: B256::repeat_byte(0xa1),
            tx_hash: B256::repeat_byte(0xb2),
            l2_cursor: 111_540,
            l2_state_root: B256::repeat_byte(0xc3),
        }
    }

    #[test]
    fn roundtrips_through_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let cp = sample();
        cp.save(dir.path()).unwrap();
        assert_eq!(ReconcileCheckpoint::load(dir.path()), Some(cp));
    }

    #[test]
    fn absent_file_is_none_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(ReconcileCheckpoint::load(dir.path()), None);
    }

    #[test]
    fn a_second_save_replaces_the_first() {
        let dir = tempfile::tempdir().unwrap();
        sample().save(dir.path()).unwrap();
        let newer = ReconcileCheckpoint {
            l1_block: 30_000,
            ..sample()
        };
        newer.save(dir.path()).unwrap();
        assert_eq!(ReconcileCheckpoint::load(dir.path()), Some(newer));
    }

    /// Body of a valid file, without the version header.
    fn valid_body() -> String {
        let full = sample().encode();
        full.split_once('\n').unwrap().1.to_owned()
    }

    /// A torn or hand-edited file must not be half-believed.
    #[test]
    fn malformed_content_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = ReconcileCheckpoint::path(dir.path());
        let body = valid_body();
        let bad = [
            // truncated mid-write: header only
            VERSION.to_owned(),
            // missing a field
            format!("{VERSION}\nl1_block=1\nl2_cursor=2\n"),
            // unparseable number
            format!("{VERSION}\nl1_block=abc\n"),
            // empty
            String::new(),
            // Wrong version, everything else valid — only the version gate can
            // reject this, so the gate is not tested vacuously.
            format!("eez-reconcile-checkpoint v0\n{body}"),
            // Same for the unknown-key gate: a complete file plus one extra key.
            format!("{VERSION}\n{body}future_field=1\n"),
        ];
        for content in bad {
            fs::write(&path, &content).unwrap();
            assert_eq!(
                ReconcileCheckpoint::load(dir.path()),
                None,
                "must reject: {content:?}",
            );
        }
    }

    /// `save` renames into place, so a leftover temp file is inert.
    #[test]
    fn a_stale_temp_file_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let cp = sample();
        cp.save(dir.path()).unwrap();
        fs::write(
            ReconcileCheckpoint::path(dir.path()).with_extension("tmp"),
            "garbage",
        )
        .unwrap();
        assert_eq!(ReconcileCheckpoint::load(dir.path()), Some(cp));
    }

    /// The whole safety argument: a checkpoint is adopted ONLY when both
    /// recorded facts still hold. Every other combination must fall back.
    #[test]
    fn usable_only_when_both_facts_still_hold() {
        let cp = sample();
        let other = B256::repeat_byte(0xee);

        assert_eq!(
            cp.usable_with(Some(cp.l1_block_hash), Some(cp.l2_state_root)),
            Ok(()),
            "both facts hold — the only accepting case",
        );

        // L1 side.
        assert!(
            cp.usable_with(None, Some(cp.l2_state_root)).is_err(),
            "L1 not serving the block must reject, not be read as agreement",
        );
        assert!(
            cp.usable_with(Some(other), Some(cp.l2_state_root)).is_err(),
            "reorged-out L1 block must reject",
        );

        // L2 side.
        assert!(
            cp.usable_with(Some(cp.l1_block_hash), None).is_err(),
            "missing local block (wiped or rolled-back datadir) must reject",
        );
        assert!(
            cp.usable_with(Some(cp.l1_block_hash), Some(other)).is_err(),
            "different local root at the cursor must reject",
        );

        // Both wrong.
        assert!(cp.usable_with(None, None).is_err());
        assert!(cp.usable_with(Some(other), Some(other)).is_err());
    }

    /// A zero root is a real value, not "unknown" — it must not be conflated
    /// with `None` and must still be compared.
    #[test]
    fn zero_hashes_are_compared_not_treated_as_unknown() {
        let cp = ReconcileCheckpoint {
            l1_block_hash: B256::ZERO,
            l2_state_root: B256::ZERO,
            ..sample()
        };
        assert_eq!(cp.usable_with(Some(B256::ZERO), Some(B256::ZERO)), Ok(()));
        assert!(
            cp.usable_with(Some(B256::repeat_byte(1)), Some(B256::ZERO))
                .is_err()
        );
    }

    /// Hashes must survive the text round trip exactly; a truncated hex parse
    /// would silently point the loader at the wrong block.
    #[test]
    fn hashes_are_not_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let cp = ReconcileCheckpoint {
            l1_block: u64::MAX,
            l1_block_hash: B256::from([
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
                0x89, 0xab, 0xcd, 0xef,
            ]),
            tx_hash: B256::ZERO,
            l2_cursor: u64::MAX,
            l2_state_root: B256::repeat_byte(0xff),
        };
        cp.save(dir.path()).unwrap();
        assert_eq!(ReconcileCheckpoint::load(dir.path()), Some(cp));
    }
}
