use alloy_primitives::B256;
use anyhow::{Context, Result, anyhow};

pub const BUNDLE_ACCEPTED: &str = "eez.node.l1_embedded.bundle.accepted";
pub const BUNDLE_MEMPOOL_FALLBACK: &str = "eez.submitter.bundle.mempool_fallback";
pub const DERIVER_REORG_NOOP: &str = "eez.deriver.l1.reorg.noop";
pub const DERIVER_REORG_RETREATED: &str = "eez.deriver.l1.reorg.retreated";
pub const DERIVER_STATE_DIVERGED_POST: &str = "eez.deriver.state.diverged_post";
pub const DERIVER_STATE_DIVERGED_PRE: &str = "eez.deriver.state.diverged_pre";
pub const DERIVER_SAFE_ADVANCED: &str = "eez.deriver.safe.advanced";
pub const DERIVER_FINALIZED_ADVANCED: &str = "eez.deriver.finalized.advanced";
pub const DERIVER_SYNC_BLOCK_BUILT: &str = "eez.deriver.reconcile.sync_block_built";
pub const DERIVER_RESYNC_FAILED: &str = "eez.deriver.resync.failed";
pub const DERIVER_COMMITTER_CLOSED: &str = "eez.deriver.committer.closed";
pub const NODE_BOOT_CATCH_UP_FAILED: &str = "eez.node.deriver.boot_catch_up.failed";
pub const COMPOSER_BUNDLE_DISPATCHED: &str = "eez.composer.bundle.dispatched";
pub const COMPOSER_SYNC_SLOT_DRAIN: &str = "eez.composer.sync_slot.drain";
pub const COMPOSER_PHASE1_BUNDLE_DISPATCHED: &str = "eez.composer.phase1.bundle.dispatched";
pub const COMPOSER_OUTBOUND_MULTICALL_UNSUPPORTED: &str =
    "eez.composer.cc_compose.outbound_multicall_unsupported";
pub const COMPOSER_POISON_EVICTION_COMPLETED: &str =
    "eez.composer.cc_compose.poison_eviction_completed";
pub const FOLLOWER_HEAD_ADVANCED: &str = "eez.node.follower.head.advanced";
pub const FOLLOWER_HEAD_SYNCING: &str = "eez.node.follower.head.syncing";
pub const L1_REORG_DETECTED: &str = "eez.l1_watcher.reorg.detected";
pub const TX_NONCE_CHAIN_EVICTED: &str = "eez.composer.recovery.nonce_chain_evicted";
pub const TX_POISON_EVICTED: &str = "eez.composer.recovery.poison_evicted";

pub const FATAL: &[&str] = &[
    NODE_BOOT_CATCH_UP_FAILED,
    DERIVER_RESYNC_FAILED,
    DERIVER_COMMITTER_CLOSED,
    DERIVER_STATE_DIVERGED_PRE,
    DERIVER_STATE_DIVERGED_POST,
];

/// One machine-readable tracing record emitted by an EEZ process.
#[derive(Clone, Debug)]
pub struct NodeSignal {
    pub name: String,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

impl NodeSignal {
    pub fn u64(&self, field: &str) -> Result<u64> {
        let value = self
            .fields
            .get(field)
            .ok_or_else(|| anyhow!("signal {} has no {field} field", self.name))?;
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
            .ok_or_else(|| anyhow!("signal {} field {field} is not a u64: {value}", self.name))
    }

    pub fn b256(&self, field: &str) -> Result<B256> {
        let value = self
            .fields
            .get(field)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("signal {} has no string {field} field", self.name))?;
        value
            .parse()
            .with_context(|| format!("signal {} has invalid {field}: {value}", self.name))
    }
}

/// Parse one JSON tracing line using the stable event schema shared by the
/// node and proof signer. Non-JSON and ordinary tracing records are ignored.
pub(crate) fn parse(line: &str) -> Option<NodeSignal> {
    let record = serde_json::from_str::<serde_json::Value>(line).ok()?;
    let fields = record.get("fields")?.as_object()?.clone();
    let name = fields.get("event_name")?.as_str()?.to_owned();
    Some(NodeSignal { name, fields })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn quoted_value(line: &str) -> Option<&str> {
        let start = line.find('"')? + 1;
        let end = line[start..].find('"')? + start;
        Some(&line[start..end])
    }

    fn declared_signals() -> Vec<&'static str> {
        let mut signals = Vec::new();
        let mut lines = include_str!("signals.rs").lines();
        while let Some(line) = lines.next() {
            let line = line.trim();
            if !line.starts_with("pub const ") || !line.contains(": &str =") {
                continue;
            }
            let value = quoted_value(line).or_else(|| lines.next().and_then(quoted_value));
            signals.push(value.expect("signal constant must contain a string literal"));
        }
        signals
    }

    fn visit_rust_sources(dir: &Path, visit: &mut impl FnMut(&str)) {
        for entry in std::fs::read_dir(dir).expect("read source directory") {
            let path = entry.expect("read source entry").path();
            if path.is_dir() {
                visit_rust_sources(&path, visit);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = std::fs::read_to_string(&path).expect("read Rust source");
                visit(&source);
            }
        }
    }

    #[test]
    fn parses_stable_event_name_and_typed_fields() {
        let signal = parse(
            r#"{"fields":{"event_name":"eez.test.ready","rollup_id":7,"root":"0x000000000000000000000000000000000000000000000000000000000000002a"}}"#,
        )
        .expect("structured signal");

        assert_eq!(signal.name, "eez.test.ready");
        assert_eq!(signal.u64("rollup_id").unwrap(), 7);
        let mut expected_root = [0; 32];
        expected_root[31] = 42;
        assert_eq!(signal.b256("root").unwrap(), B256::from(expected_root));
    }

    #[test]
    fn ignores_human_only_or_malformed_records() {
        assert!(parse(r#"{"fields":{"message":"ready"}}"#).is_none());
        assert!(parse("not json").is_none());
    }

    #[test]
    fn every_tracked_signal_has_a_stable_event_name_field() {
        let mut sources = Vec::new();
        visit_rust_sources(&crate::repo_root().join("crates"), &mut |source| {
            sources.push(source.to_owned());
        });
        let source = sources.join("\n");

        // Tracing's JSON fields do not include the event metadata name. Every
        // signal in the E2E API therefore needs an explicit `event_name` field.
        for signal in declared_signals() {
            let metadata_name = format!("name: \"{signal}\"");
            let field_name = format!("event_name = \"{signal}\"");
            let emitted = source.matches(&metadata_name).count();
            let structured = source.matches(&field_name).count();
            assert!(emitted > 0, "tracked signal {signal} is never emitted");
            assert_eq!(
                structured, emitted,
                "every {signal} event must emit event_name so the E2E harness can observe it",
            );
        }
    }
}
