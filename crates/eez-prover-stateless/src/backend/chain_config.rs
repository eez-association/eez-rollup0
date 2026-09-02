//! Strict loading of the operator-configured execution-chain trust input.
//!
//! Deployments may provide either a complete Genesis document or a bare
//! `ChainConfig`. Both forms require an explicit chain ID and reject unknown
//! fields instead of silently accepting misspelled or unsupported settings.

use std::fs;
use std::path::Path;

use alloy_genesis::{ChainConfig, Genesis};
use eyre::WrapErr as _;

const GENESIS_FIELDS: &[&str] = &[
    "alloc",
    "baseFeePerGas",
    "blobGasUsed",
    "coinbase",
    "config",
    "difficulty",
    "excessBlobGas",
    "extraData",
    "gasLimit",
    "mixHash",
    "nonce",
    "number",
    "parentHash",
    "timestamp",
];
const CONSENSUS_CONFIG_FIELDS: &[&str] = &["epoch", "period"];
const BLOB_PARAMETER_FIELDS: &[&str] = &["baseFeeUpdateFraction", "max", "target"];
// Exact keys consumed by Alloy 2.1's `blob_schedule_blob_params`. Amsterdam is
// intentionally capitalized in that pinned implementation.
const BLOB_SCHEDULE_FORKS: &[&str] = &[
    "Amsterdam",
    "bpo1",
    "bpo2",
    "bpo3",
    "bpo4",
    "bpo5",
    "cancun",
    "osaka",
    "prague",
];

/// JSON shape used to obtain the chain configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChainDocumentKind {
    BareChainConfig,
    Genesis,
}

impl ChainDocumentKind {
    /// Stable source label used by startup tracing.
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::BareChainConfig => "chain_config",
            Self::Genesis => "genesis",
        }
    }
}

/// Read and strictly parse one operator-configured chain document.
pub(super) fn load_chain_document(path: &Path) -> eyre::Result<(Genesis, ChainDocumentKind)> {
    let encoded =
        fs::read(path).wrap_err_with(|| format!("read chain configuration {}", path.display()))?;
    parse_chain_document(&encoded)
        .wrap_err_with(|| format!("parse chain configuration {}", path.display()))
}

/// Parse either a complete Genesis document or a bare `ChainConfig`.
fn parse_chain_document(encoded: &[u8]) -> eyre::Result<(Genesis, ChainDocumentKind)> {
    let document: serde_json::Value = serde_json::from_slice(encoded)?;
    let object = document
        .as_object()
        .ok_or_else(|| eyre::eyre!("chain configuration must be a JSON object"))?;
    if let Some(config_value) = object.get("config") {
        ensure_only_known_genesis_fields(object)?;
        let config_object = config_value
            .as_object()
            .ok_or_else(|| eyre::eyre!("Genesis `config` must be a JSON object"))?;
        eyre::ensure!(
            config_object.contains_key("chainId"),
            "Genesis `config` must contain `chainId`"
        );
        ensure_known_nested_chain_config_fields(config_object, "Genesis `config`")?;
        let genesis: Genesis = serde_json::from_value(document)?;
        ensure_no_chain_config_extensions(&genesis.config, "Genesis `config`")?;
        Ok((genesis, ChainDocumentKind::Genesis))
    } else {
        if let Some(field) = GENESIS_FIELDS
            .iter()
            .copied()
            .find(|field| object.contains_key(*field))
        {
            eyre::bail!("Genesis-like document contains `{field}` but no top-level `config`");
        }
        eyre::ensure!(
            object.contains_key("chainId"),
            "bare ChainConfig must contain top-level `chainId`"
        );
        ensure_known_nested_chain_config_fields(object, "bare ChainConfig")?;
        let config: ChainConfig = serde_json::from_value(document)?;
        ensure_no_chain_config_extensions(&config, "bare ChainConfig")?;
        Ok((
            Genesis {
                config,
                ..Default::default()
            },
            ChainDocumentKind::BareChainConfig,
        ))
    }
}

/// Reject unknown top-level Genesis fields before deserialization drops them.
fn ensure_only_known_genesis_fields(
    object: &serde_json::Map<String, serde_json::Value>,
) -> eyre::Result<()> {
    let unknown = object
        .keys()
        .filter(|field| !GENESIS_FIELDS.contains(&field.as_str()))
        .map(String::as_str);
    ensure_no_unsupported_fields("Genesis", unknown)
}

/// Reject fields captured by `ChainConfig`'s extension map.
fn ensure_no_chain_config_extensions(config: &ChainConfig, context: &str) -> eyre::Result<()> {
    ensure_no_unsupported_fields(context, config.extra_fields.keys().map(String::as_str))
}

/// Reject unsupported fields inside extension-friendly nested objects.
fn ensure_known_nested_chain_config_fields(
    config: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> eyre::Result<()> {
    for field in ["clique", "parlia"] {
        ensure_nested_object_fields(config, field, CONSENSUS_CONFIG_FIELDS, context)?;
    }
    ensure_nested_object_fields(config, "ethash", &[], context)?;

    if let Some(serde_json::Value::Object(schedule)) = config.get("blobSchedule") {
        ensure_no_unsupported_fields(
            &format!("{context}.blobSchedule"),
            schedule
                .keys()
                .filter(|fork| !BLOB_SCHEDULE_FORKS.contains(&fork.as_str()))
                .map(String::as_str),
        )?;
        for (fork, params) in schedule {
            if let serde_json::Value::Object(params) = params {
                let nested_context = format!("{context}.blobSchedule.{fork}");
                ensure_no_unsupported_fields(
                    &nested_context,
                    params
                        .keys()
                        .filter(|field| !BLOB_PARAMETER_FIELDS.contains(&field.as_str()))
                        .map(String::as_str),
                )?;
            }
        }
    }
    Ok(())
}

/// Validate the fields of one nested object when that field is present.
fn ensure_nested_object_fields(
    config: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    supported: &[&str],
    context: &str,
) -> eyre::Result<()> {
    let Some(serde_json::Value::Object(object)) = config.get(field) else {
        return Ok(());
    };
    let nested_context = format!("{context}.{field}");
    ensure_no_unsupported_fields(
        &nested_context,
        object
            .keys()
            .filter(|field| !supported.contains(&field.as_str()))
            .map(String::as_str),
    )
}

/// Report unsupported fields in deterministic lexical order.
fn ensure_no_unsupported_fields<'a>(
    context: &str,
    fields: impl IntoIterator<Item = &'a str>,
) -> eyre::Result<()> {
    let mut fields = fields.into_iter().collect::<Vec<_>>();
    if fields.is_empty() {
        return Ok(());
    }
    fields.sort_unstable();
    eyre::bail!(
        "{context} contains unsupported fields: `{}`",
        fields.join("`, `"),
    )
}

#[cfg(test)]
mod tests;
