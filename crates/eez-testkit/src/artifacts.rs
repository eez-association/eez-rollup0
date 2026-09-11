use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// An ephemeral test datadir persisted only for failed setup or test unwinding.
///
/// When `EEZ_TEST_DATADIR_DIR` is configured, the directory is created beneath
/// that artifact root. Passing tests delete it normally; panicking tests disable
/// cleanup so CI can upload the exact failing database without retaining every
/// successful run. Setup errors are retained before ownership reaches the test.
pub(crate) struct FailureDatadir {
    dir: Option<tempfile::TempDir>,
    artifact_backed: bool,
    fixture_ready: bool,
}

impl FailureDatadir {
    pub(crate) fn new(label: &str) -> Result<Self> {
        let artifact_root = std::env::var_os("EEZ_TEST_DATADIR_DIR").map(PathBuf::from);
        let dir = match artifact_root.as_deref() {
            Some(root) => {
                std::fs::create_dir_all(root).with_context(|| {
                    format!("create test datadir artifact root {}", root.display())
                })?;
                tempfile::Builder::new()
                    .prefix(&format!("{label}-{}-", std::process::id()))
                    .tempdir_in(root)
                    .context("create artifact-backed test datadir")?
            }
            None => tempfile::Builder::new()
                .prefix(&format!("{label}-"))
                .tempdir()
                .context("create ephemeral test datadir")?,
        };
        Ok(Self {
            dir: Some(dir),
            artifact_backed: artifact_root.is_some(),
            fixture_ready: false,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        self.dir.as_ref().expect("datadir already consumed").path()
    }

    /// Marks successful fixture setup. From this point, normal test teardown
    /// deletes the directory and panic unwinding retains it.
    pub(crate) fn fixture_ready(&mut self) {
        self.fixture_ready = true;
    }
}

impl Drop for FailureDatadir {
    fn drop(&mut self) {
        let setup_failed = !self.fixture_ready;
        if self.artifact_backed
            && (setup_failed || std::thread::panicking())
            && let Some(dir) = self.dir.take()
        {
            let retained = dir.keep();
            eprintln!("retained failing test datadir at {}", retained.display());
        }
    }
}
