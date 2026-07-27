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
/// A new nonce must be the first unreserved nonce at or above the source-chain
/// nonce for this direction. An existing queued nonce may be replaced in
/// place. A gapped held tx would poison the all-or-nothing bundle and break
/// bundle-mates' chains.
pub async fn gate_and_hold(
    envelope: &TxEnvelope,
    raw_tx: &Bytes,
    direction: Direction,
    held_pool: &HeldPool,
    expected_source_chain_id: u64,
    validation_provider: &RootProvider,
) -> Admission {
    let cost = match validate_front_envelope(envelope, direction, expected_source_chain_id) {
        Ok(cost) => cost,
        Err(msg) => return Admission::Rejected(msg),
    };
    let Ok(sender) = envelope.recover_signer() else {
        return Admission::Rejected("signature recovery failed".into());
    };
    let nonce = envelope.nonce();
    // This checks one transaction; simulation enforces cumulative affordability.
    let balance = match validation_provider.get_balance(sender).await {
        Ok(b) => b,
        Err(e) => {
            return Admission::Rejected(format!("source-chain balance lookup failed: {e}"));
        }
    };
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
    // Pending state prevents collisions with source-chain mempool transactions.
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

fn validate_front_envelope(
    envelope: &TxEnvelope,
    direction: Direction,
    expected_source_chain_id: u64,
) -> Result<U256, String> {
    if matches!(envelope, TxEnvelope::Eip4844(_)) {
        return Err(
            "EIP-4844 blob transactions are unsupported by the cross-chain front pipeline".into(),
        );
    }
    let Some(chain_id) = envelope.chain_id() else {
        return Err(
            "missing source-chain id; replay-unprotected legacy transactions are not accepted by cross-chain fronts"
                .into(),
        );
    };
    if chain_id != expected_source_chain_id {
        return Err(format!(
            "wrong source-chain id {chain_id} for {direction:?} front: expected {expected_source_chain_id}"
        ));
    }
    let gas_cost = U256::from(envelope.gas_limit())
        .checked_mul(U256::from(envelope.max_fee_per_gas()))
        .ok_or_else(|| "upfront cost overflow: gas_limit * max_fee_per_gas".to_string())?;
    envelope
        .value()
        .checked_add(gas_cost)
        .ok_or_else(|| "upfront cost overflow: value + gas_limit * max_fee_per_gas".to_string())
}

/// Shared, cheaply-clonable per-connection context.
#[derive(Clone)]
struct Ctx {
    client: reqwest::Client,
    upstream_rpc_url: String,
    direction: Direction,
    held_pool: Arc<HeldPool>,
    validation_provider: RootProvider,
    expected_source_chain_id: u64,
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
    expected_source_chain_id: u64,
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
        expected_source_chain_id,
        "cross-chain front listening (forward eth_* to source chain; intercept sendRawTransaction)",
    );
    let ctx = Ctx {
        client,
        upstream_rpc_url,
        direction,
        held_pool,
        validation_provider,
        expected_source_chain_id,
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
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(HyperBytes::from("read body failed")))
                .expect("valid response"));
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
        ctx.expected_source_chain_id,
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
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEip4844, TxLegacy};
    use alloy_network::{TxSignerSync, eip2718::Encodable2718};
    use alloy_primitives::{Address, TxKind};
    use alloy_provider::ProviderBuilder;
    use alloy_provider::mock::Asserter;
    use alloy_rpc_types_eth::AccessList;
    use alloy_signer_local::PrivateKeySigner;
    use serde_json::json;

    use super::*;

    fn test_signer() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::with_last_byte(1)).expect("valid signer")
    }

    fn signed_eip1559(
        chain_id: u64,
        value: U256,
        gas_limit: u64,
        max_fee_per_gas: u128,
    ) -> (PrivateKeySigner, TxEnvelope, Bytes) {
        let signer = test_signer();
        let mut tx = TxEip1559 {
            chain_id,
            nonce: 0,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas: 1,
            to: TxKind::Call(Address::ZERO),
            value,
            access_list: alloy_eips::eip2930::AccessList::default(),
            input: Bytes::default(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).expect("tx signs");
        let envelope = TxEnvelope::from(tx.into_signed(sig));
        let raw = Bytes::from(envelope.encoded_2718());
        (signer, envelope, raw)
    }

    fn signed_legacy(chain_id: Option<u64>) -> (PrivateKeySigner, TxEnvelope, Bytes) {
        let signer = test_signer();
        let mut tx = TxLegacy {
            chain_id,
            nonce: 0,
            gas_price: 1,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::default(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).expect("tx signs");
        let envelope = TxEnvelope::from(tx.into_signed(sig));
        let raw = Bytes::from(envelope.encoded_2718());
        (signer, envelope, raw)
    }

    fn signed_blob(chain_id: u64) -> (PrivateKeySigner, TxEnvelope, Bytes) {
        let signer = test_signer();
        let mut tx = TxEip4844 {
            chain_id,
            nonce: 0,
            gas_limit: 21_000,
            max_fee_per_gas: 1,
            max_priority_fee_per_gas: 1,
            to: Address::ZERO,
            value: U256::ZERO,
            access_list: alloy_eips::eip2930::AccessList::default(),
            blob_versioned_hashes: vec![B256::with_last_byte(2)],
            max_fee_per_blob_gas: 1,
            input: Bytes::default(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).expect("tx signs");
        let envelope = TxEnvelope::from(tx.into_signed(sig));
        let raw = Bytes::from(envelope.encoded_2718());
        (signer, envelope, raw)
    }

    fn rejection(admission: Admission) -> String {
        match admission {
            Admission::Rejected(msg) => msg,
            Admission::Held(hash) => panic!("expected rejection, got held tx {hash}"),
        }
    }

    #[tokio::test]
    async fn matching_source_chain_id_is_accepted() {
        let (_, envelope, raw) = signed_eip1559(31337, U256::ZERO, 21_000, 1);
        let pool = HeldPool::new();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::default().connect_mocked_client(asserter.clone());
        asserter.push_success(&U256::from(1_000_000u64));
        asserter.push_success(&0_u64);

        let admission =
            gate_and_hold(&envelope, &raw, Direction::Inbound, &pool, 31337, &provider).await;

        assert!(matches!(admission, Admission::Held(_)));
        assert_eq!(pool.len(), 1);
    }

    #[tokio::test]
    async fn wrong_source_chain_id_is_rejected_without_pool_mutation() {
        let (signer, envelope, raw) = signed_eip1559(1, U256::ZERO, 21_000, 1);
        let pool = HeldPool::new();
        let provider = ProviderBuilder::default().connect_mocked_client(Asserter::new());

        let msg = rejection(
            gate_and_hold(&envelope, &raw, Direction::Outbound, &pool, 2, &provider).await,
        );

        assert!(msg.contains("wrong source-chain id 1"));
        assert_eq!(pool.len(), 0, "rejected tx from {signer:?} mutated pool");
    }

    #[tokio::test]
    async fn missing_source_chain_id_is_rejected() {
        let (signer, envelope, raw) = signed_legacy(None);
        let pool = HeldPool::new();
        let provider = ProviderBuilder::default().connect_mocked_client(Asserter::new());

        let msg = rejection(
            gate_and_hold(&envelope, &raw, Direction::Inbound, &pool, 31337, &provider).await,
        );

        assert!(msg.contains("missing source-chain id"));
        assert_eq!(pool.len(), 0, "rejected tx from {signer:?} mutated pool");
    }

    #[tokio::test]
    async fn blob_envelope_is_rejected() {
        let (signer, envelope, raw) = signed_blob(31337);
        let pool = HeldPool::new();
        let provider = ProviderBuilder::default().connect_mocked_client(Asserter::new());

        let msg = rejection(
            gate_and_hold(&envelope, &raw, Direction::Inbound, &pool, 31337, &provider).await,
        );

        assert!(msg.contains("EIP-4844 blob transactions are unsupported"));
        assert_eq!(pool.len(), 0, "rejected tx from {signer:?} mutated pool");
    }

    #[tokio::test]
    async fn upfront_cost_overflow_is_rejected() {
        let (signer, envelope, raw) = signed_eip1559(31337, U256::MAX, 1, 1);
        let pool = HeldPool::new();
        let provider = ProviderBuilder::default().connect_mocked_client(Asserter::new());

        let msg = rejection(
            gate_and_hold(&envelope, &raw, Direction::Inbound, &pool, 31337, &provider).await,
        );

        assert!(msg.contains("upfront cost overflow"));
        assert_eq!(pool.len(), 0, "rejected tx from {signer:?} mutated pool");
    }

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

    fn free_port() -> u16 {
        std::net::TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    async fn wait_for_front(port: u16) {
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("cross-chain front did not bind to port {port}");
    }

    async fn send_raw(port: u16, raw: &Bytes, id: u64) -> Value {
        reqwest::Client::new()
            .post(format!("http://127.0.0.1:{port}"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_sendRawTransaction",
                "params": [format!("{raw:#x}")],
                "id": id,
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

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
            gate_and_hold(&first, &first_raw, Direction::Inbound, &pool, 1, &provider,).await,
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
            1,
            &provider,
        )
        .await;
        let Admission::Rejected(message) = rejected else {
            panic!("same nonce should be rejected while first tx is in flight");
        };
        assert!(message.contains("invalid nonce 0"), "{message}");
        assert!(message.contains("next unreserved nonce 1"), "{message}");

        let rejected = gate_and_hold(
            &gapped,
            &gapped_raw,
            Direction::Inbound,
            &pool,
            1,
            &provider,
        )
        .await;
        let Admission::Rejected(message) = rejected else {
            panic!("nonce 2 must not be admitted while nonce 1 is unreserved");
        };
        assert!(message.contains("nonce 1"), "{message}");

        assert!(matches!(
            gate_and_hold(&next, &next_raw, Direction::Inbound, &pool, 1, &provider,).await,
            Admission::Held(_)
        ));
    }

    #[tokio::test]
    async fn gate_replaces_queued_nonce_and_preserves_higher_suffix() {
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
            1,
            &provider,
        )
        .await
        else {
            panic!("original transaction should be held");
        };
        let Admission::Held(suffix_hash) = gate_and_hold(
            &suffix,
            &suffix_raw,
            Direction::Inbound,
            &pool,
            1,
            &provider,
        )
        .await
        else {
            panic!("suffix transaction should be held");
        };
        let Admission::Held(replacement_hash) = gate_and_hold(
            &replacement,
            &replacement_raw,
            Direction::Inbound,
            &pool,
            1,
            &provider,
        )
        .await
        else {
            panic!("replacement transaction should be held");
        };

        assert_ne!(original_hash, replacement_hash);
        let queued = pool.pop_all();
        assert_eq!(queued.len(), 2);
        assert_eq!(queued[0].hash, replacement_hash);
        assert_eq!(queued[0].nonce, 0);
        assert_eq!(queued[1].hash, suffix_hash);
        assert_eq!(queued[1].nonce, 1);
    }

    #[tokio::test]
    async fn real_http_fronts_route_directions_and_preserve_replacement_suffix() {
        let inbound_signer = PrivateKeySigner::from_bytes(&B256::with_last_byte(1)).unwrap();
        let outbound_signer = PrivateKeySigner::from_bytes(&B256::with_last_byte(2)).unwrap();
        let (_, inbound_original) = signed_transfer(&inbound_signer, 0, 1);
        let (_, inbound_suffix) = signed_transfer(&inbound_signer, 1, 2);
        let (_, inbound_replacement) = signed_transfer(&inbound_signer, 0, 3);
        let (_, outbound) = signed_transfer(&outbound_signer, 0, 4);

        let pool = Arc::new(HeldPool::new());
        let inbound_port = free_port();
        let outbound_port = free_port();

        let inbound_asserter = Asserter::new();
        let inbound_provider =
            ProviderBuilder::default().connect_mocked_client(inbound_asserter.clone());
        for _ in 0..3 {
            inbound_asserter.push_success(&U256::from(1_000_000u64));
            inbound_asserter.push_success(&0_u64);
        }
        let outbound_asserter = Asserter::new();
        let outbound_provider =
            ProviderBuilder::default().connect_mocked_client(outbound_asserter.clone());
        outbound_asserter.push_success(&U256::from(1_000_000u64));
        outbound_asserter.push_success(&0_u64);

        let inbound_task = tokio::spawn(run_cross_chain_front(
            inbound_port,
            "http://127.0.0.1:1".into(),
            Direction::Inbound,
            Arc::clone(&pool),
            inbound_provider,
            1,
        ));
        let outbound_task = tokio::spawn(run_cross_chain_front(
            outbound_port,
            "http://127.0.0.1:1".into(),
            Direction::Outbound,
            Arc::clone(&pool),
            outbound_provider,
            1,
        ));
        wait_for_front(inbound_port).await;
        wait_for_front(outbound_port).await;

        let original = send_raw(inbound_port, &inbound_original, 1).await;
        let suffix = send_raw(inbound_port, &inbound_suffix, 2).await;
        let replacement = send_raw(inbound_port, &inbound_replacement, 3).await;
        let outbound_response = send_raw(outbound_port, &outbound, 4).await;
        for response in [&original, &suffix, &replacement, &outbound_response] {
            assert!(response.get("error").is_none(), "{response}");
            assert!(response.get("result").is_some(), "{response}");
        }

        let queued = pool.pop_all();
        assert_eq!(queued.len(), 3);
        assert_eq!(queued[0].hash.to_string(), replacement["result"]);
        assert_eq!(queued[0].direction, Direction::Inbound);
        assert_eq!(queued[0].nonce, 0);
        assert_eq!(queued[1].hash.to_string(), suffix["result"]);
        assert_eq!(queued[1].direction, Direction::Inbound);
        assert_eq!(queued[1].nonce, 1);
        assert_eq!(queued[2].hash.to_string(), outbound_response["result"]);
        assert_eq!(queued[2].direction, Direction::Outbound);
        assert_eq!(queued[2].nonce, 0);

        inbound_task.abort();
        outbound_task.abort();
    }
}
