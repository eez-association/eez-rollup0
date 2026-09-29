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
    /// EEZ contracts configured for this composer.
    pub eez_contracts: ComposerContracts,
    /// Chain IDs supported by this composer.
    pub supported_networks: SupportedNetworks,
    /// Composer implementation version.
    pub version: &'static str,
}

impl ComposerInfo {
    /// Build the discovery response for the composer's supported networks.
    #[must_use]
    pub fn new(eez_contracts: ComposerContracts, supported_networks: SupportedNetworks) -> Self {
        Self {
            eez_contracts,
            supported_networks,
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

/// EVM chain IDs supported by the composer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportedNetworks {
    /// EEZ L1 chain ID.
    pub eez_l1: u64,
    /// EEZ L2 chain ID.
    pub eez_l2: u64,
}

/// Contract addresses advertised by the composer discovery endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComposerContracts {
    /// L1 EEZ registry contract.
    pub eez_registry_address: Address,
    /// L1 rollup manager, when configured by the deployment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eez_rollup_manager_address: Option<Address>,
    /// L1 bridge sender, when the bridge deployment is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eez_l1_bridge_sender: Option<Address>,
    /// L2 EEZ predeploy.
    pub eez_l2_address: Address,
    /// L2 bridge receiver, when the bridge deployment is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eez_l2_bridge_receiver: Option<Address>,
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
        let rollup_manager = Address::repeat_byte(0x33);
        let bridge_sender = Address::repeat_byte(0x44);
        let bridge_receiver = Address::repeat_byte(0x55);
        let value = serde_json::to_value(ComposerInfo::new(
            ComposerContracts {
                eez_registry_address: l1_address,
                eez_rollup_manager_address: Some(rollup_manager),
                eez_l1_bridge_sender: Some(bridge_sender),
                eez_l2_address: l2_address,
                eez_l2_bridge_receiver: Some(bridge_receiver),
            },
            SupportedNetworks {
                eez_l1: 10_200,
                eez_l2: 10_201,
            },
        ))
        .unwrap();

        assert_eq!(
            value["eezContracts"],
            serde_json::json!({
                "eezRegistryAddress": format!("{l1_address:#x}"),
                "eezRollupManagerAddress": format!("{rollup_manager:#x}"),
                "eezL1BridgeSender": format!("{bridge_sender:#x}"),
                "eezL2Address": format!("{l2_address:#x}"),
                "eezL2BridgeReceiver": format!("{bridge_receiver:#x}"),
            })
        );
        assert_eq!(
            value["supportedNetworks"],
            serde_json::json!({
                "eezL1": 10_200,
                "eezL2": 10_201,
            })
        );
        assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
        assert!(value.get("eez_contracts").is_none());
    }

    #[test]
    fn composer_info_omits_unconfigured_optional_contracts() {
        let value = serde_json::to_value(ComposerInfo::new(
            ComposerContracts {
                eez_registry_address: Address::repeat_byte(0x11),
                eez_rollup_manager_address: None,
                eez_l1_bridge_sender: None,
                eez_l2_address: Address::repeat_byte(0x22),
                eez_l2_bridge_receiver: None,
            },
            SupportedNetworks {
                eez_l1: 10_200,
                eez_l2: 10_201,
            },
        ))
        .unwrap();

        assert!(
            value["eezContracts"]
                .get("eezRollupManagerAddress")
                .is_none()
        );
        assert!(value["eezContracts"].get("eezL1BridgeSender").is_none());
        assert!(value["eezContracts"].get("eezL2BridgeReceiver").is_none());
    }

    #[tokio::test]
    async fn composer_info_rpc_returns_config_and_rejects_params() {
        let l1_address = Address::repeat_byte(0x22);
        let l2_address = Address::repeat_byte(0x33);
        let module = composer_rpc_module(ComposerInfo::new(
            ComposerContracts {
                eez_registry_address: l1_address,
                eez_rollup_manager_address: None,
                eez_l1_bridge_sender: None,
                eez_l2_address: l2_address,
                eez_l2_bridge_receiver: None,
            },
            SupportedNetworks {
                eez_l1: 10_200,
                eez_l2: 10_201,
            },
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
            serde_json::json!({
                "eezRegistryAddress": format!("{l1_address:#x}"),
                "eezL2Address": format!("{l2_address:#x}"),
            })
        );
        assert_eq!(
            response["result"]["supportedNetworks"],
            serde_json::json!({
                "eezL1": 10_200,
                "eezL2": 10_201,
            })
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
