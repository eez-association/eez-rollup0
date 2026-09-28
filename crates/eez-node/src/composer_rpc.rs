//! Composer discovery JSON-RPC.
//!
//! The endpoint is installed only by the composer binary, so its presence also
//! acts as a cheap capability probe for clients connecting to an EEZ node.

use alloy_primitives::Address;
use jsonrpsee::RpcModule;
use reth_node_api::FullNodeComponents;
use reth_node_builder::rpc::RpcContext;
use reth_rpc_eth_api::FullEthApiServer;
use serde::Serialize;
use tracing::{Level, event};

/// Runtime configuration advertised by `eez_composerInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComposerInfo {
    /// EEZ contracts used by this composer, ordered to match `supported_networks`.
    pub eez_contracts: Vec<Address>,
    /// EVM chain IDs supported by this composer, ordered to match `eez_contracts`.
    pub supported_networks: Vec<u64>,
    /// Composer implementation version.
    pub version: &'static str,
}

impl ComposerInfo {
    /// Build the discovery response for the composer's supported networks.
    #[must_use]
    pub fn new(eez_contracts: Vec<Address>, supported_networks: Vec<u64>) -> Self {
        debug_assert_eq!(eez_contracts.len(), supported_networks.len());
        Self {
            eez_contracts,
            supported_networks,
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

/// Install the composer discovery method on every configured L2 RPC transport.
///
/// # Errors
///
/// Returns an error if the method name is already registered or the module
/// cannot be merged into the configured transports.
pub fn install_composer_rpc<Node, EthApi>(
    ctx: RpcContext<'_, Node, EthApi>,
    info: ComposerInfo,
) -> eyre::Result<()>
where
    Node: FullNodeComponents,
    EthApi: FullEthApiServer + Clone + Send + Sync + 'static,
{
    let module = composer_rpc_module(info)?;
    ctx.modules.merge_configured(module)?;
    event!(
        name: "eez.node.composer_rpc.installed",
        Level::INFO,
        "eez_composerInfo RPC installed",
    );
    Ok(())
}

fn composer_rpc_module(info: ComposerInfo) -> eyre::Result<RpcModule<ComposerInfo>> {
    let mut module = RpcModule::new(info);
    module.register_method("eez_composerInfo", |params, info, _ext| {
        // Accept the standard zero-argument forms while rejecting actual
        // arguments instead of silently ignoring a client mistake.
        if let Some(raw) = params.as_str() {
            let value: serde_json::Value = params.parse()?;
            let empty = matches!(&value, serde_json::Value::Array(values) if values.is_empty())
                || matches!(&value, serde_json::Value::Object(values) if values.is_empty());
            if !empty {
                return Err(jsonrpsee::types::ErrorObject::owned(
                    -32602,
                    "eez_composerInfo takes no parameters",
                    Some(raw),
                ));
            }
        }
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(info.clone())
    })?;
    Ok(module)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composer_info_uses_stable_camel_case_schema() {
        let l1_address = Address::repeat_byte(0x11);
        let l2_address = Address::repeat_byte(0x22);
        let value = serde_json::to_value(ComposerInfo::new(
            vec![l1_address, l2_address],
            vec![10_200, 10_201],
        ))
        .unwrap();

        assert_eq!(
            value["eezContracts"],
            serde_json::json!([format!("{l1_address:#x}"), format!("{l2_address:#x}")])
        );
        assert_eq!(
            value["supportedNetworks"],
            serde_json::json!([10_200, 10_201])
        );
        assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
        assert!(value.get("eez_contracts").is_none());
    }

    #[tokio::test]
    async fn composer_info_rpc_returns_config_and_rejects_params() {
        let l1_address = Address::repeat_byte(0x22);
        let l2_address = Address::repeat_byte(0x33);
        let module = composer_rpc_module(ComposerInfo::new(
            vec![l1_address, l2_address],
            vec![10_200, 10_201],
        ))
        .unwrap();
        let (response, _) = module
            .raw_json_request(
                r#"{"jsonrpc":"2.0","method":"eez_composerInfo","params":[],"id":1}"#,
                1,
            )
            .await
            .unwrap();
        let response: serde_json::Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(
            response["result"]["eezContracts"],
            serde_json::json!([format!("{l1_address:#x}"), format!("{l2_address:#x}")])
        );
        assert_eq!(
            response["result"]["supportedNetworks"],
            serde_json::json!([10_200, 10_201])
        );

        let (response, _) = module
            .raw_json_request(
                r#"{"jsonrpc":"2.0","method":"eez_composerInfo","params":[1],"id":2}"#,
                1,
            )
            .await
            .unwrap();
        let response: serde_json::Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(response["error"]["code"], -32602);
    }
}
