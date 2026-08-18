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
//! the node builder's `extend_rpc_modules` hook in [`crate::main`]). The
//! handler forwards each signed, 2718-encoded tx through the node's own
//! [`EthApiServer::send_raw_transaction`] — full validation + correct
//! pool insertion — in submitted order, so the dev auto-miner lands them
//! in the next block, postBatch first. The `Submitter` then takes its
//! primary bundle path and never falls back to `eth_sendRawTransaction`.
//!
//! This is deliberately NOT a real block builder: there is no competing
//! builder on a single-node dev chain, so ordered mempool insertion is a
//! faithful `eth_sendBundle` endpoint. True build-time atomic inclusion
//! (all-or-nothing on EVM revert) would require a bundle-aware payload
//! builder — a follow-up.
//!
//! The function is generic over the node + eth-api types; the dev L1 it
//! serves is a vanilla `EthereumNode` (it would equally serve a
//! `reth_gnosis::GnosisNode`).

use alloy_primitives::B256;
use jsonrpsee::core::server::RpcModule;
use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use reth_node_api::FullNodeComponents;
use reth_node_builder::rpc::RpcContext;
use reth_rpc_eth_api::{EthApiServer, FullEthApiServer};
use serde::Deserialize;
use tracing::{Level, event};

/// Subset of the Flashbots `eth_sendBundle` request we honor. Extra
/// fields (`minTimestamp`, `maxTimestamp`, `revertingTxHashes`, …) are
/// accepted and ignored — a single-node dev chain has no proposer
/// auction or block-pinning to enforce them against.
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

        // Forward in submitted order. On a single-node dev chain with
        // no competing builder this yields postBatch-first inclusion
        // in the next block. A rejected tx fails the whole call (the
        // caller treats a non-`result` reply as a bundle failure and
        // retries), which is the strictest honest behavior we can
        // offer without build-time atomicity.
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
            test_signal = "eez.node.l1_embedded.bundle.accepted",
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
