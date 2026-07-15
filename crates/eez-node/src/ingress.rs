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
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::TcpListener;
use tracing::{Level, event};

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
/// A new nonce must be the first unreserved nonce at or above the source-chain
/// nonce for this direction. An existing queued nonce may be replaced, which
/// also evicts its higher queued suffix. A gapped held tx would poison the
/// all-or-nothing bundle and break bundle-mates' chains.
pub async fn gate_and_hold(
    envelope: &TxEnvelope,
    raw_tx: &Bytes,
    direction: Direction,
    held_pool: &HeldPool,
    validation_provider: &RootProvider,
) -> Admission {
    let Ok(sender) = envelope.recover_signer() else {
        return Admission::Rejected("signature recovery failed".into());
    };
    let nonce = envelope.nonce();
    let balance = match validation_provider.get_balance(sender).await {
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
    // keccak256 of the EIP-2718 envelope — the canonical tx hash for legacy and typed txs alike.
    let hash: B256 = keccak256(raw_tx.as_ref());
    let tx = HeldTx {
        raw_tx: raw_tx.clone(),
        hash,
        attempts: 0,
        sender,
        nonce,
        direction,
    };
    let on_chain = match validation_provider
        .get_transaction_count(sender)
        .pending()
        .await
    {
        Ok(n) => n,
        Err(e) => return Admission::Rejected(format!("source-chain nonce lookup failed: {e}")),
    };
    if let Err(err) = held_pool.push_contiguous(tx, on_chain) {
        return Admission::Rejected(format!(
            "invalid nonce {nonce} for {sender}: expected next unreserved nonce {} (source-chain nonce {})",
            err.expected, err.on_chain
        ));
    }
    event!(
        name: "eez.ingress.cross_chain.push",
        Level::INFO,
        tx_hash = %hash,
        sender = %sender,
        nonce,
        direction = ?direction,
        "cross-chain tx held for next Sync slot",
    );
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

    const MAX_BODY: usize = 10 * 1024 * 1024;
    let body_bytes = match req.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            event!(name: "eez.xchain_front.body_read_failed", Level::DEBUG, error = %e, "read body failed");
            return Ok(forward(&ctx, Vec::new()).await);
        }
    };
    if body_bytes.len() > MAX_BODY {
        return Ok(Response::builder()
            .status(StatusCode::PAYLOAD_TOO_LARGE)
            .body(Full::new(HyperBytes::from("request body too large")))
            .expect("valid response"));
    }

    // Intercept a single `eth_sendRawTransaction`; batches and every other
    // method fall through to the transparent forward.
    if let Ok(json) = serde_json::from_slice::<Value>(&body_bytes)
        && json.get("method").and_then(Value::as_str) == Some("eth_sendRawTransaction")
        && let Some(raw_hex) = json
            .get("params")
            .and_then(|p| p.get(0))
            .and_then(Value::as_str)
        && let Some(resp) = intercept_send_raw(&ctx, raw_hex, &json).await
    {
        return Ok(resp);
    }
    // A JSON-RPC batch array would bypass the single-tx intercept and forward a
    // bundled cross-chain tx straight to the mempool; reject any batch carrying
    // `eth_sendRawTransaction` (read-only batches still forward transparently).
    if let Ok(Value::Array(items)) = serde_json::from_slice::<Value>(&body_bytes)
        && items
            .iter()
            .any(|it| it.get("method").and_then(Value::as_str) == Some("eth_sendRawTransaction"))
    {
        let resp = serde_json::json!({
            "jsonrpc": "2.0",
            "error": { "code": -32600, "message": "send cross-chain eth_sendRawTransaction singly, not in a JSON-RPC batch" },
            "id": Value::Null,
        });
        return Ok(json_response(resp.to_string()));
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
        &ctx.validation_provider,
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
    use super::*;
    use alloy_consensus::{SignableTransaction, TxEip1559};
    use alloy_network::TxSignerSync;
    use alloy_network::eip2718::Encodable2718;
    use alloy_primitives::{Address, TxKind};
    use alloy_provider::ProviderBuilder;
    use alloy_provider::mock::Asserter;
    use alloy_rpc_types_eth::AccessList;
    use alloy_signer_local::PrivateKeySigner;

    fn signed_transfer(signer: &PrivateKeySigner, nonce: u64, value: u64) -> (TxEnvelope, Bytes) {
        let mut tx = TxEip1559 {
            chain_id: 1,
            nonce,
            gas_limit: 21_000,
            max_fee_per_gas: 1,
            max_priority_fee_per_gas: 1,
            to: TxKind::Call(Address::repeat_byte(0x42)),
            value: U256::from(value),
            access_list: AccessList::default(),
            input: Bytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let envelope = TxEnvelope::from(tx.into_signed(sig));
        let raw = Bytes::from(envelope.encoded_2718());
        (envelope, raw)
    }

    #[tokio::test]
    async fn gate_handles_landed_but_not_derived_reservation() {
        let signer = PrivateKeySigner::from_bytes(&B256::with_last_byte(1)).unwrap();
        let (first, first_raw) = signed_transfer(&signer, 0, 1);
        let (same_nonce, same_nonce_raw) = signed_transfer(&signer, 0, 2);
        let (next, next_raw) = signed_transfer(&signer, 1, 3);
        let (gapped, gapped_raw) = signed_transfer(&signer, 2, 4);
        let pool = HeldPool::new();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::default().connect_mocked_client(asserter.clone());

        for _ in 0..2 {
            asserter.push_success(&U256::from(1_000_000u64));
            asserter.push_success(&0_u64);
        }
        asserter.push_success(&U256::from(1_000_000u64));
        asserter.push_success(&1_u64);
        asserter.push_success(&U256::from(1_000_000u64));
        asserter.push_success(&1_u64);

        assert!(matches!(
            gate_and_hold(&first, &first_raw, Direction::Inbound, &pool, &provider,).await,
            Admission::Held(_)
        ));

        let drained = pool.pop_n(1);
        assert_eq!(drained.len(), 1);
        assert_eq!(pool.len(), 0);

        let rejected = gate_and_hold(
            &same_nonce,
            &same_nonce_raw,
            Direction::Inbound,
            &pool,
            &provider,
        )
        .await;
        let Admission::Rejected(message) = rejected else {
            panic!("same nonce should be rejected while first tx is in flight");
        };
        assert!(message.contains("invalid nonce 0"), "{message}");
        assert!(message.contains("next unreserved nonce 1"), "{message}");

        let rejected =
            gate_and_hold(&gapped, &gapped_raw, Direction::Inbound, &pool, &provider).await;
        let Admission::Rejected(message) = rejected else {
            panic!("nonce 2 must not be admitted while nonce 1 is unreserved");
        };
        assert!(message.contains("nonce 1"), "{message}");

        assert!(matches!(
            gate_and_hold(&next, &next_raw, Direction::Inbound, &pool, &provider).await,
            Admission::Held(_)
        ));
    }

    #[tokio::test]
    async fn gate_replaces_queued_nonce_and_evicts_higher_suffix() {
        let signer = PrivateKeySigner::from_bytes(&B256::with_last_byte(1)).unwrap();
        let (original, original_raw) = signed_transfer(&signer, 0, 1);
        let (suffix, suffix_raw) = signed_transfer(&signer, 1, 2);
        let (replacement, replacement_raw) = signed_transfer(&signer, 0, 3);
        let pool = HeldPool::new();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::default().connect_mocked_client(asserter.clone());

        for _ in 0..3 {
            asserter.push_success(&U256::from(1_000_000u64));
            asserter.push_success(&0_u64);
        }

        let Admission::Held(original_hash) = gate_and_hold(
            &original,
            &original_raw,
            Direction::Inbound,
            &pool,
            &provider,
        )
        .await
        else {
            panic!("original transaction should be held");
        };
        assert!(matches!(
            gate_and_hold(&suffix, &suffix_raw, Direction::Inbound, &pool, &provider,).await,
            Admission::Held(_)
        ));
        let Admission::Held(replacement_hash) = gate_and_hold(
            &replacement,
            &replacement_raw,
            Direction::Inbound,
            &pool,
            &provider,
        )
        .await
        else {
            panic!("replacement transaction should be held");
        };

        assert_ne!(original_hash, replacement_hash);
        let queued = pool.pop_all();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].hash, replacement_hash);
        assert_eq!(queued[0].nonce, 0);
    }
}
