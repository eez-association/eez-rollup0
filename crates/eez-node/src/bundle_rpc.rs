//! In-process `eth_sendBundle` support for the embedded **dev** L1.
//!
//! On chiado the composer submits batches to an external Flashbots-style
//! relay (`EEZ_L1_BUILDER_RPC_URL`) that speaks `eth_sendBundle`. The
//! local dev L1 has no such relay, so the `Submitter` used to get a
//! JSON-RPC `-32601` (method not found) and silently degrade to ordered
//! `eth_sendRawTransaction` mempool submission
//! (`eez_l1::submitter::post_bundle`). That divergence means the dev
//! harness never exercises the real bundle path chiado uses.
//!
//! This module closes the gap: [`install_dev_bundle_rpc`] registers an
//! `eth_sendBundle` method directly on the dev L1's own RPC server (via
//! the node builder's `extend_rpc_modules` hook in [`crate::run_composer`]). The
//! handler forwards each signed, 2718-encoded tx through the node's own
//! [`EthApiServer::send_raw_transaction`] — full validation + correct
//! pool insertion — in submitted order, so the dev auto-miner lands them
//! in the next block, postBatch first. The `Submitter` then takes its
//! primary bundle path and never falls back to `eth_sendRawTransaction`.
//!
//! This is deliberately not a real block builder: it simulates the bundle in
//! order and rejects a known non-whitelisted revert before forwarding, but
//! build-time atomic inclusion would require a bundle-aware payload builder.
//!
//! The function is generic over the node + eth-api types; the dev L1 it
//! serves is a vanilla `EthereumNode` (it would equally serve a
//! `reth_gnosis::GnosisNode`).

use alloy_consensus::transaction::SignerRecoverable as _;
use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::{Address, B256, U256};
use jsonrpsee::core::server::RpcModule;
use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use reth_node_api::FullNodeComponents;
use reth_node_builder::rpc::RpcContext;
use reth_rpc_eth_api::{EthApiServer, FullEthApiServer};
use serde::Deserialize;
use tracing::{Level, event};

/// Subset of the Flashbots `eth_sendBundle` request we honor. Extra
/// fields (`minTimestamp`, `maxTimestamp`, …) are accepted and ignored —
/// a single-node dev chain has no proposer auction or block-pinning to
/// enforce them against.
///
/// Matches the body produced by `eez_l1::submitter::post_bundle`:
/// `{ "txs": ["0x…", …], "blockNumber": "0x…" }`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleParams {
    /// Signed, EIP-2718-encoded transactions, in the order they must be
    /// included (postBatch first, then user txs).
    pub txs: Vec<alloy_primitives::Bytes>,
    /// Target L1 block, `0x`-hex. Advisory only on the dev chain — the
    /// auto-miner includes the txs in the next block regardless. Kept so
    /// the field deserializes; surfaced for logging.
    #[serde(default)]
    pub block_number: Option<String>,
    /// Transactions allowed to revert without failing the bundle.
    #[serde(default)]
    pub reverting_tx_hashes: Vec<B256>,
}

/// An `eth_call` request for `envelope` as sent by `from`.
fn call_request(envelope: &TxEnvelope, from: Address) -> serde_json::Value {
    let mut request = serde_json::json!({
        "from": from,
        "gas": format!("{:#x}", Transaction::gas_limit(envelope)),
        "value": Transaction::value(envelope),
        "input": Transaction::input(envelope),
    });
    if let Some(to) = Transaction::to(envelope) {
        request["to"] = serde_json::json!(to);
    }
    if let Some(chain_id) = Transaction::chain_id(envelope) {
        request["chainId"] = serde_json::json!(format!("{chain_id:#x}"));
    }
    if Transaction::is_dynamic_fee(envelope) {
        request["maxFeePerGas"] =
            serde_json::json!(format!("{:#x}", Transaction::max_fee_per_gas(envelope)));
        if let Some(tip) = Transaction::max_priority_fee_per_gas(envelope) {
            request["maxPriorityFeePerGas"] = serde_json::json!(format!("{tip:#x}"));
        }
    } else if let Some(price) = Transaction::gas_price(envelope) {
        request["gasPrice"] = serde_json::json!(format!("{price:#x}"));
    }
    if let Some(access_list) = Transaction::access_list(envelope) {
        request["accessList"] = serde_json::json!(access_list);
    }
    request
}

fn bundle_error(message: String) -> ErrorObjectOwned {
    ErrorObject::owned(-32000, message, None::<()>)
}

/// `extend_rpc_modules` hook: register `eth_sendBundle` on the embedded
/// dev L1's transports (http / ws / ipc).
///
/// # Errors
///
/// Returns an error if the method is already registered (name clash) or
/// the merge into the configured transports fails.
pub fn install_dev_bundle_rpc<Node, EthApi>(ctx: RpcContext<'_, Node, EthApi>) -> eyre::Result<()>
where
    Node: FullNodeComponents,
    EthApi: FullEthApiServer + Clone + Send + Sync + 'static,
{
    // Context for the method = a clone of the node's own eth-api handle;
    // the handler forwards each bundle tx through its validated send path.
    let eth_api = ctx.registry.eth_api().clone();
    let mut module = RpcModule::new(eth_api);

    module.register_async_method("eth_sendBundle", |params, eth_api, _ext| async move {
        // `eth_sendBundle` params are `[{ "txs": [...], ... }]` — one
        // object in a positional array.
        let bundle: BundleParams = params.one().map_err(|e| {
            ErrorObject::owned(
                -32602,
                format!("invalid eth_sendBundle params: {e}"),
                None::<()>,
            )
        })?;
        if bundle.txs.is_empty() {
            return Err(ErrorObject::owned(
                -32602,
                "eth_sendBundle: empty txs".to_string(),
                None::<()>,
            ));
        }

        // Simulate the bundle in order at the next block to reject known
        // inclusion-time reverts before forwarding anything to the pool.
        let mut hashes = Vec::with_capacity(bundle.txs.len());
        let mut requests = Vec::with_capacity(bundle.txs.len());
        for raw in &bundle.txs {
            let envelope = TxEnvelope::decode_2718(&mut raw.as_ref())
                .map_err(|e| bundle_error(format!("eth_sendBundle: undecodable tx: {e}")))?;
            let from = envelope
                .recover_signer()
                .map_err(|e| bundle_error(format!("eth_sendBundle: bad signature: {e}")))?;
            hashes.push(*envelope.tx_hash());
            requests.push(call_request(&envelope, from));
        }
        let next_block = EthApiServer::block_number(&*eth_api).await? + U256::from(1);
        let bundles = serde_json::from_value(serde_json::json!([{
            "transactions": requests,
            "blockOverride": { "number": next_block },
        }]))
        .map_err(|e| bundle_error(format!("eth_sendBundle: simulation request: {e}")))?;
        let state_context = serde_json::from_value(serde_json::json!({ "blockNumber": "latest" }))
            .map_err(|e| bundle_error(format!("eth_sendBundle: simulation context: {e}")))?;
        let results = EthApiServer::call_many(&*eth_api, bundles, Some(state_context), None)
            .await
            .map_err(|e| bundle_error(format!("eth_sendBundle: bundle not includable: {e}")))?;
        let outcomes = results.into_iter().next().unwrap_or_default();
        for (index, tx_hash) in hashes.iter().enumerate() {
            let reverted = outcomes
                .get(index)
                .is_none_or(|outcome| outcome.error.is_some());
            if reverted && !bundle.reverting_tx_hashes.contains(tx_hash) {
                event!(
                    name: "eez.node.l1_embedded.bundle.rejected",
                    Level::WARN,
                    event_name = "eez.node.l1_embedded.bundle.rejected",
                    tx_hash = %tx_hash,
                    tx_count = bundle.txs.len(),
                    "embedded dev L1 eth_sendBundle: tx reverts outside revertingTxHashes; bundle not included",
                );
                return Err(bundle_error(format!(
                    "eth_sendBundle: transaction {tx_hash} reverts and is not in \
                     revertingTxHashes; bundle not included"
                )));
            }
        }

        // Forward in submitted order. On a single-node dev chain with
        // no competing builder this yields postBatch-first inclusion
        // in the next block.
        let mut last_hash = B256::ZERO;
        for raw in &bundle.txs {
            // Disambiguate from the `EthTransactions` helper of the
            // same name — we want the server method returning a
            // jsonrpsee `RpcResult<B256>`.
            last_hash = EthApiServer::send_raw_transaction(&*eth_api, raw.clone()).await?;
        }

        event!(
            name: "eez.node.l1_embedded.bundle.accepted",
            Level::INFO,
            event_name = "eez.node.l1_embedded.bundle.accepted",
            tx_count = bundle.txs.len(),
            target_block = bundle.block_number.as_deref().unwrap_or("next"),
            "embedded dev L1 eth_sendBundle: forwarded txs to pool in order",
        );
        // Flashbots-shaped reply. The `Submitter` only checks for a
        // non-error `result`, so any object is fine.
        Ok::<_, ErrorObjectOwned>(serde_json::json!({ "bundleHash": last_hash }))
    })?;

    ctx.modules.merge_configured(module)?;
    event!(
        name: "eez.node.l1_embedded.bundle.installed",
        Level::INFO,
        "embedded dev L1: eth_sendBundle RPC installed (composer bundle path no longer \
         degrades to eth_sendRawTransaction)",
    );
    Ok(())
}
