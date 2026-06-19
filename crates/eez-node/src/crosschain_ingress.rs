//! Standalone L1-fronting JSON-RPC ingress for cross-chain transactions.
//!
//! Wallets and tooling point here to submit cross-chain calls. The
//! endpoint behaves like an L1 RPC: every method is forwarded verbatim
//! to the configured L1 node, EXCEPT `eth_sendRawTransaction`, which is
//! classified by [`IngressClassifier`]. Cross-chain txs are validated
//! against L1 (sender nonce + balance) and pushed to the shared
//! [`HeldPool`] for the next Sync slot; plain txs relay to L1 unchanged.
//!
//! A cross-chain tx executes on L1 (bundled with `postBatch`), so its
//! nonce, receipt, hash, chain id, and gas all live in L1's view.
//! Fronting this endpoint with L1 gives a wallet a coherent picture with
//! no per-method shimming. Native L2 txs use the L2 node's own RPC
//! instead — see [`crate::ingress`], which rejects cross-chain txs at
//! that endpoint and points callers here.

use std::net::SocketAddr;
use std::sync::Arc;

use alloy_consensus::transaction::SignerRecoverable as _;
use alloy_consensus::{Transaction as _, TxEnvelope};
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::{B256, Bytes, U256, keccak256};
use alloy_provider::{Provider as _, ProviderBuilder, RootProvider};
use eez_composer::{Classification, HeldPool, HeldTx, IngressClassifier};
use eyre::Result;
use jsonrpsee::core::middleware::{
    Batch, BatchEntry, Notification, RpcServiceBuilder, RpcServiceT,
};
use jsonrpsee::core::server::{BatchResponseBuilder, MethodResponse, ResponsePayload};
use jsonrpsee::server::{RpcModule, Server, ServerConfig};
use jsonrpsee::types::{ErrorObject, Id, Request};
use serde_json::value::RawValue;
use tower::Layer;
use tracing::{Level, event};

/// Default port; override with `EEZ_CROSSCHAIN_INGRESS_PORT`.
///
/// Chosen clear of the L2 node's RPC (`:18688`) and its WS sibling
/// (`:18689`), so the two ingress endpoints don't collide on a host.
pub const DEFAULT_PORT: u16 = 18699;

/// The one method we intercept; everything else forwards to L1.
const SEND_METHOD: &str = "eth_sendRawTransaction";

/// Cap on a forwarded response body. Generous because L1 reads like
/// `eth_getLogs` / `eth_getBlockByNumber` can be large; this is a
/// trusted local proxy, not a public endpoint.
const MAX_RESPONSE_BYTES: u32 = 100 * 1024 * 1024;

/// Run the cross-chain ingress until the server stops.
///
/// Binds `addr`, forwards every JSON-RPC method to `l1_rpc_url`, and
/// intercepts `eth_sendRawTransaction` to hold cross-chain txs in
/// `held_pool` (classified by `classifier`).
///
/// # Errors
///
/// Returns an error if `l1_rpc_url` is malformed or the listener can't
/// bind `addr`.
pub async fn serve(
    addr: SocketAddr,
    l1_rpc_url: &str,
    held_pool: Arc<HeldPool>,
    classifier: Arc<IngressClassifier>,
) -> Result<()> {
    let url: reqwest::Url = l1_rpc_url
        .parse()
        .map_err(|e| eyre::eyre!("cross-chain ingress L1 url malformed ({l1_rpc_url}): {e}"))?;
    let l1 = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(url);
    let state = Arc::new(IngressState {
        held_pool,
        classifier,
        l1,
    });

    let cfg = ServerConfig::builder()
        .max_response_body_size(MAX_RESPONSE_BYTES)
        .build();
    let server = Server::builder()
        .set_config(cfg)
        .set_rpc_middleware(RpcServiceBuilder::new().layer(ProxyLayer { state }))
        .build(addr)
        .await
        .map_err(|e| eyre::eyre!("cross-chain ingress bind {addr}: {e}"))?;

    event!(
        name: "eez.crosschain_ingress.listening",
        Level::INFO,
        %addr,
        l1 = %l1_rpc_url,
        "cross-chain ingress listening; L1-fronting, eth_sendRawTransaction intercepted",
    );

    let handle = server.start(RpcModule::new(()));
    handle.stopped().await;
    Ok(())
}

/// Shared state for the proxy service; cheaply cloned via `Arc`.
struct IngressState {
    held_pool: Arc<HeldPool>,
    classifier: Arc<IngressClassifier>,
    l1: RootProvider,
}

#[derive(Clone)]
struct ProxyLayer {
    state: Arc<IngressState>,
}

impl<S> Layer<S> for ProxyLayer {
    type Service = ProxyService;
    fn layer(&self, _inner: S) -> Self::Service {
        // The proxy answers every method itself (forward to L1, or hold
        // a cross-chain tx), so the wrapped base service is never used.
        ProxyService {
            state: Arc::clone(&self.state),
        }
    }
}

#[derive(Clone)]
struct ProxyService {
    state: Arc<IngressState>,
}

impl RpcServiceT for ProxyService {
    type MethodResponse = MethodResponse;
    type NotificationResponse = MethodResponse;
    type BatchResponse = MethodResponse;

    fn call<'a>(&self, req: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let state = Arc::clone(&self.state);
        async move { handle_call(&state, req).await }
    }

    fn batch<'a>(&self, batch: Batch<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let state = Arc::clone(&self.state);
        async move {
            let mut builder = BatchResponseBuilder::new_with_limit(MAX_RESPONSE_BYTES as usize);
            for entry in batch {
                let resp = match entry {
                    Ok(BatchEntry::Call(req)) => handle_call(&state, req).await,
                    // Notifications expect no response; HTTP wallets don't
                    // emit them, so skip rather than relay.
                    Ok(BatchEntry::Notification(_)) => continue,
                    Err(err) => {
                        let (e, id) = err.into_parts();
                        MethodResponse::error(id, e)
                    }
                };
                if builder.append(resp).is_err() {
                    break;
                }
            }
            MethodResponse::from_batch(builder.finish())
        }
    }

    fn notification<'a>(
        &self,
        _n: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        // Notifications have no response and aren't part of the
        // wallet-over-HTTP flow this endpoint serves.
        std::future::ready(MethodResponse::notification())
    }
}

/// Route one request: intercept a cross-chain `eth_sendRawTransaction`,
/// otherwise forward to L1.
async fn handle_call(state: &IngressState, req: Request<'_>) -> MethodResponse {
    if req.method.as_ref() != SEND_METHOD {
        return forward(state, req.id, req.method.as_ref(), req.params.as_deref()).await;
    }

    // Decode the raw tx to classify it. Any decode hiccup → forward to
    // L1 and let it produce the canonical error.
    let Some(params) = req.params.as_deref() else {
        return forward(state, req.id, SEND_METHOD, None).await;
    };
    let Ok((raw_tx,)) = serde_json::from_str::<(Bytes,)>(params.get()) else {
        return forward(state, req.id, SEND_METHOD, req.params.as_deref()).await;
    };
    let Ok(envelope) = TxEnvelope::decode_2718(&mut raw_tx.as_ref()) else {
        return forward(state, req.id, SEND_METHOD, req.params.as_deref()).await;
    };

    match state
        .classifier
        .classify(envelope.to().as_ref(), envelope.chain_id())
    {
        // Not cross-chain → a plain L1 tx; relay to L1 unchanged.
        Classification::L2Only => forward(state, req.id, SEND_METHOD, req.params.as_deref()).await,
        Classification::CrossChain => admit_cross_chain(state, req.id, raw_tx, &envelope).await,
    }
}

/// Forward a method to L1 verbatim and rewrap the response under `id`.
async fn forward(
    state: &IngressState,
    id: Id<'_>,
    method: &str,
    params: Option<&RawValue>,
) -> MethodResponse {
    let params: Box<RawValue> = params.map_or_else(empty_params, ToOwned::to_owned);
    let method: std::borrow::Cow<'static, str> = method.to_owned().into();
    match state
        .l1
        .raw_request::<Box<RawValue>, Box<RawValue>>(method, params)
        .await
    {
        Ok(result) => MethodResponse::response(
            id,
            ResponsePayload::success(result),
            MAX_RESPONSE_BYTES as usize,
        ),
        Err(e) => {
            if let Some(p) = e.as_error_resp() {
                let code = i32::try_from(p.code).unwrap_or(-32603);
                MethodResponse::error(
                    id,
                    ErrorObject::owned(code, p.message.to_string(), p.data.clone()),
                )
            } else {
                MethodResponse::error(
                    id,
                    ErrorObject::owned(-32603, format!("L1 forward failed: {e}"), None::<()>),
                )
            }
        }
    }
}

/// Validate a cross-chain tx against L1 and hold it for the next Sync
/// slot, or reject it loudly at the door.
async fn admit_cross_chain(
    state: &IngressState,
    id: Id<'_>,
    raw_tx: Bytes,
    envelope: &TxEnvelope,
) -> MethodResponse {
    // Admission validation (invariant 7: loud, at the door). A
    // cross-chain tx rides an all-or-nothing L1 bundle — one invalid tx
    // fails the whole bundle in builder sim, evicting innocent
    // bundle-mates and breaking their nonce chains. Validate like a
    // mempool would (sender, on-chain+held nonce, balance).
    let Ok(sender) = envelope.recover_signer() else {
        return reject(id, "signature recovery failed".to_owned());
    };
    let nonce = envelope.nonce();
    let on_chain = match state.l1.get_transaction_count(sender).await {
        Ok(n) => n,
        Err(e) => return reject(id, format!("L1 validation unavailable (nonce lookup): {e}")),
    };
    let held = state.held_pool.held_count_for(sender) as u64;
    let expected = on_chain + held;
    if nonce != expected {
        return reject(
            id,
            format!(
                "invalid nonce {nonce} for {sender}: expected {expected} \
                 (on-chain {on_chain} + {held} held)"
            ),
        );
    }
    let balance = match state.l1.get_balance(sender).await {
        Ok(b) => b,
        Err(e) => {
            return reject(
                id,
                format!("L1 validation unavailable (balance lookup): {e}"),
            );
        }
    };
    let cost = U256::from(envelope.value())
        + U256::from(envelope.gas_limit()) * U256::from(envelope.max_fee_per_gas());
    if balance < cost {
        return reject(
            id,
            format!(
                "insufficient L1 balance for {sender}: have {balance}, \
                 need {cost} (value + gas_limit * max_fee)"
            ),
        );
    }

    // tx_hash = keccak256(EIP-2718 envelope bytes), the canonical hash
    // for both legacy and typed txs.
    let hash: B256 = keccak256(raw_tx.as_ref());
    event!(
        name: "eez.crosschain_ingress.held",
        Level::INFO,
        tx_hash = %hash,
        sender = %sender,
        nonce,
        "cross-chain tx held for next Sync slot",
    );
    state.held_pool.push(HeldTx {
        raw_tx,
        hash,
        attempts: 0,
        sender,
        nonce,
    });
    MethodResponse::response(
        id,
        ResponsePayload::success(hash),
        MAX_RESPONSE_BYTES as usize,
    )
}

/// Build a clear JSON-RPC error for a rejected cross-chain submission.
fn reject(id: Id<'_>, msg: String) -> MethodResponse {
    event!(
        name: "eez.crosschain_ingress.rejected",
        Level::WARN,
        reason = %msg,
        "cross-chain tx rejected at ingress",
    );
    MethodResponse::error(id, ErrorObject::owned(-32000, msg, None::<()>))
}

/// An empty JSON-RPC params array for methods forwarded with no params.
fn empty_params() -> Box<RawValue> {
    RawValue::from_string("[]".to_owned()).expect("'[]' is valid JSON")
}
