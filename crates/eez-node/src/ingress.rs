//! Transparent cross-chain RPC fronts — one per SOURCE chain.
//!
//! A cross-chain tx is signed and tracked against its source chain, so wallets
//! need a real RPC for that chain. Each front is a transparent proxy: it
//! forwards every `eth_*` to the source chain's node and intercepts only
//! `eth_sendRawTransaction`, holding the tx in the per-rollup
//! [`HeldPool`](eez_composer::HeldPool) with the front's fixed [`Direction`]
//! (L1 front → `Inbound`, L2 front → `Outbound`) and returning its hash at once.
//! The receipt resolves when the held tx lands — an outbound in the L2 Sync
//! block, an inbound in the L1 postBatch bundle.
//!
//! No classification: the endpoint fixes the direction. Pure txs use the node's
//! normal mempool RPC; one misdirected here is held then poison-evicted at
//! compose time (`simulate_and_resolve` finds no cross-chain effect). One front
//! per source chain (invariant 8).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::transaction::SignerRecoverable as _;
use alloy_consensus::{Transaction as _, TxEnvelope};
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::{B256, Bytes, U256, keccak256};
use alloy_provider::{Provider as _, RootProvider};
use eez_composer::{Direction, HeldPool, HeldTx};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes as HyperBytes;
use hyper::header::CONTENT_LENGTH;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::TcpListener;
use tracing::{Level, event};

const MAX_BODY: usize = 10 * 1024 * 1024;

/// Outcome of the admission gate.
#[derive(Debug)]
pub enum Admission {
    /// Accepted into the HeldPool; carries the canonical tx hash.
    Held(B256),
    /// Rejected at the door (loud — invariant 7); carries the reason.
    Rejected(String),
}

/// Admission gate: recover the signer, validate nonce + balance against the
/// SOURCE chain (`validation_provider`), and push into `held_pool` with
/// `direction`.
///
/// The nonce must be contiguous with the sender's held chain for this direction
/// (`on_chain + held_count_for(sender, direction)`): a gapped held tx would
/// poison the all-or-nothing bundle and break bundle-mates' chains. `None`
/// provider skips validation (dev / no-RPC).
pub async fn gate_and_hold(
    envelope: &TxEnvelope,
    raw_tx: &Bytes,
    direction: Direction,
    held_pool: &HeldPool,
    validation_provider: Option<&RootProvider>,
) -> Admission {
    let Ok(sender) = envelope.recover_signer() else {
        return Admission::Rejected("signature recovery failed".into());
    };
    let nonce = envelope.nonce();
    if let Some(provider) = validation_provider {
        // `pending`, not `latest`: count a same-sender tx already in the source
        // mempool, else a correctly-nonced cross-chain tx collides with it.
        let on_chain = match provider.get_transaction_count(sender).pending().await {
            Ok(n) => n,
            Err(e) => return Admission::Rejected(format!("source-chain nonce lookup failed: {e}")),
        };
        let held = held_pool.held_count_for(sender, direction) as u64;
        let expected = on_chain + held;
        if nonce != expected {
            return Admission::Rejected(format!(
                "invalid nonce {nonce} for {sender}: expected {expected} (on-chain {on_chain} + {held} held)"
            ));
        }
        let balance = match provider.get_balance(sender).await {
            Ok(b) => b,
            Err(e) => {
                return Admission::Rejected(format!("source-chain balance lookup failed: {e}"));
            }
        };
        let cost = U256::from(envelope.value())
            + U256::from(envelope.gas_limit()) * U256::from(envelope.max_fee_per_gas());
        if balance < cost {
            return Admission::Rejected(format!(
                "insufficient balance for {sender}: have {balance}, need {cost} (value + gas_limit * max_fee)"
            ));
        }
    }
    // keccak256 of the EIP-2718 envelope — the canonical tx hash for legacy and typed txs alike.
    let hash: B256 = keccak256(raw_tx.as_ref());
    event!(
        name: "eez.ingress.cross_chain.push",
        Level::INFO,
        tx_hash = %hash,
        sender = %sender,
        nonce,
        direction = ?direction,
        "cross-chain tx held for next Sync slot",
    );
    held_pool.push(HeldTx {
        raw_tx: raw_tx.clone(),
        hash,
        attempts: 0,
        sender,
        nonce,
        direction,
    });
    Admission::Held(hash)
}

/// Shared, cheaply-clonable per-connection context.
#[derive(Clone)]
struct Ctx {
    client: reqwest::Client,
    upstream_rpc_url: String,
    direction: Direction,
    held_pool: Arc<HeldPool>,
    validation_provider: RootProvider,
}

/// Run a transparent cross-chain front on `port`, forwarding every `eth_*` to
/// `upstream_rpc_url` (the source chain's node) and holding intercepted
/// `eth_sendRawTransaction`s with `direction`. Never returns under normal op.
///
/// # Errors
/// Fails if the listener can't bind or the reqwest client can't be built.
pub async fn run_cross_chain_front(
    port: u16,
    upstream_rpc_url: String,
    direction: Direction,
    held_pool: Arc<HeldPool>,
    validation_provider: RootProvider,
) -> eyre::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    event!(
        name: "eez.xchain_front.listening",
        Level::INFO,
        %port,
        %upstream_rpc_url,
        direction = ?direction,
        "cross-chain front listening (forward eth_* to source chain; intercept sendRawTransaction)",
    );
    let ctx = Ctx {
        client,
        upstream_rpc_url,
        direction,
        held_pool,
        validation_provider,
    };
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                event!(name: "eez.xchain_front.accept_failed", Level::WARN, error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let service = service_fn(move |req| handle(req, ctx.clone()));
            let io = TokioIo::new(stream);
            if let Err(e) = http1::Builder::new()
                .keep_alive(true)
                .serve_connection(io, service)
                .await
                && !e.is_incomplete_message()
            {
                event!(name: "eez.xchain_front.conn_error", Level::DEBUG, error = %e, "connection error");
            }
        });
    }
}

fn json_response(body: String) -> Response<Full<HyperBytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(HyperBytes::from(body)))
        .expect("valid response")
}

fn rpc_error(id: Value, code: i64, message: &str) -> Response<Full<HyperBytes>> {
    json_response(
        serde_json::json!({
            "jsonrpc": "2.0",
            "error": { "code": code, "message": message },
            "id": id,
        })
        .to_string(),
    )
}

fn body_too_large() -> Response<Full<HyperBytes>> {
    Response::builder()
        .status(StatusCode::PAYLOAD_TOO_LARGE)
        .body(Full::new(HyperBytes::from("request body too large")))
        .expect("valid response")
}

async fn collect_body_limited(
    mut body: hyper::body::Incoming,
    max: usize,
) -> Result<Result<HyperBytes, ()>, hyper::Error> {
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.len() > max.saturating_sub(out.len()) {
            return Ok(Err(()));
        }
        out.extend_from_slice(&data);
    }
    Ok(Ok(HyperBytes::from(out)))
}

fn content_length_exceeds(req: &Request<hyper::body::Incoming>, max: usize) -> bool {
    req.headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|len| len > max)
}

fn batch_has_send_raw(json: &Value) -> bool {
    matches!(json, Value::Array(items) if items.iter().any(|it| {
        it.get("method").and_then(Value::as_str) == Some("eth_sendRawTransaction")
    }))
}

fn all_forwarded_methods_are_eth(json: &Value) -> bool {
    let method_allowed = |value: &Value| {
        value
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| method.starts_with("eth_"))
    };
    match json {
        Value::Object(_) => method_allowed(json),
        Value::Array(items) if !items.is_empty() => items.iter().all(|item| {
            method_allowed(item)
                && item.get("method").and_then(Value::as_str) != Some("eth_sendRawTransaction")
        }),
        _ => false,
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    ctx: Ctx,
) -> Result<Response<Full<HyperBytes>>, hyper::Error> {
    if req.method() == Method::OPTIONS {
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", "POST, OPTIONS")
            .header("Access-Control-Allow-Headers", "Content-Type")
            .body(Full::new(HyperBytes::new()))
            .expect("valid response"));
    }
    if req.method() != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(HyperBytes::from("method not allowed")))
            .expect("valid response"));
    }

    if content_length_exceeds(&req, MAX_BODY) {
        return Ok(body_too_large());
    }

    let body_bytes = match collect_body_limited(req.into_body(), MAX_BODY).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(())) => return Ok(body_too_large()),
        Err(e) => {
            event!(name: "eez.xchain_front.body_read_failed", Level::DEBUG, error = %e, "read body failed");
            return Ok(forward(&ctx, Vec::new()).await);
        }
    };

    let Ok(json) = serde_json::from_slice::<Value>(&body_bytes) else {
        return Ok(rpc_error(Value::Null, -32700, "parse error"));
    };

    // A JSON-RPC batch array would bypass the single-tx intercept and forward a
    // bundled cross-chain tx straight to the mempool; reject any batch carrying
    // `eth_sendRawTransaction`.
    if batch_has_send_raw(&json) {
        return Ok(rpc_error(
            Value::Null,
            -32600,
            "send cross-chain eth_sendRawTransaction singly, not in a JSON-RPC batch",
        ));
    }

    // Intercept a single `eth_sendRawTransaction`. Undecodable tx bytes fall
    // through so the upstream RPC produces the standard JSON-RPC error.
    if json.get("method").and_then(Value::as_str) == Some("eth_sendRawTransaction")
        && let Some(raw_hex) = json
            .get("params")
            .and_then(|p| p.get(0))
            .and_then(Value::as_str)
        && let Some(resp) = intercept_send_raw(&ctx, raw_hex, &json).await
    {
        return Ok(resp);
    }

    if !all_forwarded_methods_are_eth(&json) {
        let id = json.get("id").cloned().unwrap_or(Value::Null);
        return Ok(rpc_error(
            id,
            -32601,
            "cross-chain front forwards only eth_* JSON-RPC methods",
        ));
    }
    Ok(forward(&ctx, body_bytes.to_vec()).await)
}

/// `Some(response)` when held here; `None` to forward (undecodable tx → let the
/// node produce the standard JSON-RPC error).
async fn intercept_send_raw(
    ctx: &Ctx,
    raw_hex: &str,
    json: &Value,
) -> Option<Response<Full<HyperBytes>>> {
    let raw: Bytes = raw_hex.parse().ok()?;
    let envelope = TxEnvelope::decode_2718(&mut raw.as_ref()).ok()?;
    let id = json.get("id").cloned().unwrap_or(Value::Null);
    let resp_body = match gate_and_hold(
        &envelope,
        &raw,
        ctx.direction,
        &ctx.held_pool,
        Some(&ctx.validation_provider),
    )
    .await
    {
        Admission::Held(hash) => serde_json::json!({ "jsonrpc": "2.0", "result": hash, "id": id }),
        Admission::Rejected(msg) => {
            event!(
                name: "eez.xchain_front.rejected",
                Level::WARN,
                reason = %msg,
                direction = ?ctx.direction,
                "cross-chain tx rejected at front",
            );
            serde_json::json!({ "jsonrpc": "2.0", "error": { "code": -32000, "message": msg }, "id": id })
        }
    };
    Some(json_response(resp_body.to_string()))
}

/// Forward the request body verbatim to the source chain's RPC and relay back.
async fn forward(ctx: &Ctx, body: Vec<u8>) -> Response<Full<HyperBytes>> {
    match ctx
        .client
        .post(&ctx.upstream_rpc_url)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(r) => {
            let status = r.status();
            let bytes = r.bytes().await.unwrap_or_default();
            Response::builder()
                .status(status.as_u16())
                .header("Content-Type", "application/json")
                .header("Access-Control-Allow-Origin", "*")
                .body(Full::new(HyperBytes::from(bytes.to_vec())))
                .expect("valid response")
        }
        Err(e) => {
            event!(name: "eez.xchain_front.upstream_error", Level::WARN, error = %e, "upstream error");
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(HyperBytes::from(format!("upstream error: {e}"))))
                .expect("valid response")
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn batch_send_raw_is_not_forwardable() {
        let body = json!([
            {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
            {"jsonrpc": "2.0", "method": "eth_sendRawTransaction", "params": ["0xdead"], "id": 2}
        ]);
        assert!(batch_has_send_raw(&body));
        assert!(
            !all_forwarded_methods_are_eth(&body),
            "sendRaw batches must be rejected before forwarding"
        );
    }

    #[test]
    fn only_eth_methods_are_forwardable() {
        assert!(all_forwarded_methods_are_eth(&json!({
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1
        })));
        assert!(all_forwarded_methods_are_eth(&json!([
            {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
            {"jsonrpc": "2.0", "method": "eth_getBalance", "params": ["0x0000000000000000000000000000000000000000", "latest"], "id": 2}
        ])));
        assert!(!all_forwarded_methods_are_eth(&json!({
            "jsonrpc": "2.0",
            "method": "web3_clientVersion",
            "params": [],
            "id": 1
        })));
        assert!(!all_forwarded_methods_are_eth(&json!([
            {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
            {"jsonrpc": "2.0", "method": "admin_peers", "params": [], "id": 2}
        ])));
    }
}
