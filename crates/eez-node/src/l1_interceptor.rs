//! B0 — the production L1->L2 entry: a transparent L1-RPC front.
//!
//! A wallet (MetaMask/Rabby) reads chainId/nonce/gas/balance from whatever RPC
//! it is connected to, so the L1->L2 entry endpoint must be a real L1 RPC. This
//! front forwards EVERY `eth_*` verbatim to the real L1 node (so the wallet
//! builds a correct L1 tx) and intercepts ONLY `eth_sendRawTransaction`: if the
//! tx targets an authorized L1 cross-chain proxy (an L1->L2 call), it runs the
//! SHARED admission gate (`crate::ingress::gate_and_hold`, identical to the L2
//! RPC ingress) and pushes a `HeldTx{direction: Inbound}` into the HeldPool for
//! the composer to drain+compose at the next Sync slot — returning the tx hash
//! WITHOUT forwarding to L1 (the composer submits it nonce-linked with the
//! postBatch). Every other tx is forwarded to L1 unchanged.
//!
//! This is the wallet-correct replacement for the `:18688` chain-id-mismatch dev
//! hack (which abuses the L2 RPC's ingress classifier). Detection here is by the
//! `authorizedProxies(to)` registry lookup (the based-rollup `l1_proxy.rs`
//! approach), NOT chain-id — an L1 tx always carries the L1 chain id, so only the
//! `to`-is-a-proxy signal distinguishes a cross-chain call from a normal L1 tx.
//! eez0 composes at DRAIN, so this front does "detect + push" ONLY — it does NOT
//! port based's ingress-time orchestration.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, Bytes};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::{SolCall, sol};
use eez_composer::{Direction, HeldPool};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes as HyperBytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::TcpListener;
use tracing::{Level, event};

use crate::ingress::{Admission, gate_and_hold};

sol! {
    // EEZBase.authorizedProxies(address) public view returns ProxyInfo.
    // The 6-field struct ABI-decodes head-first as (address originalAddress,
    // uint256 originalRollupId, ...); originalAddress != 0 ⇒ a registered proxy.
    function authorizedProxies(address proxy)
        external view returns (address originalAddress, uint256 originalRollupId);
}

/// Shared, cheaply-clonable context for each connection's request handler.
#[derive(Clone)]
struct Ctx {
    client: reqwest::Client,
    l1_rpc_url: String,
    eez_l1_address: Address,
    held_pool: Arc<HeldPool>,
    l1_provider: RootProvider,
}

/// Run the L1 interceptor front on `port`, forwarding to `l1_rpc_url` (the real
/// L1). `eez_l1_address` is the L1 EEZ contract the `authorizedProxies` lookup
/// hits; `l1_provider` backs both that lookup and the admission gate. Never
/// returns under normal operation.
pub async fn run_l1_interceptor(
    port: u16,
    l1_rpc_url: String,
    eez_l1_address: Address,
    held_pool: Arc<HeldPool>,
    l1_provider: RootProvider,
) -> eyre::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    event!(
        name: "eez.l1_interceptor.listening",
        Level::INFO,
        %port,
        %l1_rpc_url,
        %eez_l1_address,
        "L1->L2 interceptor front listening (forward eth_* to L1; intercept sendRawTransaction)",
    );

    let ctx = Ctx {
        client,
        l1_rpc_url,
        eez_l1_address,
        held_pool,
        l1_provider,
    };
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                event!(name: "eez.l1_interceptor.accept_failed", Level::WARN, error = %e, "accept failed");
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
            {
                if !e.is_incomplete_message() {
                    event!(name: "eez.l1_interceptor.conn_error", Level::DEBUG, error = %e, "connection error");
                }
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
    // CORS preflight + non-POST: pass through trivially.
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
            event!(name: "eez.l1_interceptor.body_read_failed", Level::DEBUG, error = %e, "read body failed");
            return Ok(forward(&ctx, Vec::new()).await);
        }
    };
    if body_bytes.len() > MAX_BODY {
        return Ok(Response::builder()
            .status(StatusCode::PAYLOAD_TOO_LARGE)
            .body(Full::new(HyperBytes::from("request body too large")))
            .expect("valid response"));
    }

    // Intercept ONLY a single `eth_sendRawTransaction` (the wallet shape).
    // Batches and every other method fall through to the L1 forward verbatim.
    if let Ok(json) = serde_json::from_slice::<Value>(&body_bytes) {
        if json.get("method").and_then(Value::as_str) == Some("eth_sendRawTransaction") {
            if let Some(raw_hex) = json
                .get("params")
                .and_then(|p| p.get(0))
                .and_then(Value::as_str)
            {
                if let Some(resp) = intercept_send_raw(&ctx, raw_hex, &json).await {
                    return Ok(resp);
                }
            }
        }
    }

    Ok(forward(&ctx, body_bytes.to_vec()).await)
}

/// Returns `Some(response)` if the raw tx is an L1->L2 cross-chain call (held +
/// answered here, NOT forwarded); `None` to forward to L1 normally.
async fn intercept_send_raw(
    ctx: &Ctx,
    raw_hex: &str,
    json: &Value,
) -> Option<Response<Full<HyperBytes>>> {
    let raw: Bytes = raw_hex.parse().ok()?;
    let envelope = TxEnvelope::decode_2718(&mut raw.as_ref()).ok()?;
    let to = envelope.to()?;

    // Detect: is `to` an authorized L1 cross-chain proxy (an L1->L2 call)?
    if !is_authorized_proxy(ctx, to).await {
        return None; // ordinary L1 tx — forward.
    }

    let id = json.get("id").cloned().unwrap_or(Value::Null);
    // B0 is INBOUND-only (an L1→L2 call to an authorized L1 proxy), so it
    // always validates against L1 and never uses the L2 slot — pass `None`
    // for the outbound provider. Inbound admission is byte-unchanged.
    let resp_body = match gate_and_hold(
        &envelope,
        &raw,
        Direction::Inbound,
        &ctx.held_pool,
        Some(&ctx.l1_provider),
        None,
    )
    .await
    {
        Admission::Held(hash) => {
            serde_json::json!({ "jsonrpc": "2.0", "result": hash, "id": id })
        }
        Admission::Rejected(msg) => {
            event!(name: "eez.l1_interceptor.rejected", Level::WARN, reason = %msg, "L1->L2 tx rejected at interceptor");
            serde_json::json!({
                "jsonrpc": "2.0",
                "error": { "code": -32000, "message": msg },
                "id": id,
            })
        }
    };
    Some(json_response(resp_body.to_string()))
}

/// `authorizedProxies(to).originalAddress != 0` on the L1 EEZ — the proxy
/// registry lookup that marks `to` as a cross-chain proxy. Any RPC/decode
/// failure ⇒ `false` (treat as a normal tx + forward; the worst case is a
/// missed interception, never a wrong hold).
async fn is_authorized_proxy(ctx: &Ctx, to: Address) -> bool {
    let call = authorizedProxiesCall { proxy: to };
    let req = TransactionRequest::default()
        .to(ctx.eez_l1_address)
        .input(call.abi_encode().into());
    let Ok(ret) = ctx.l1_provider.call(req).await else {
        return false;
    };
    authorizedProxiesCall::abi_decode_returns(&ret)
        .map(|r| !r.originalAddress.is_zero())
        .unwrap_or(false)
}

/// Forward the request body verbatim to the real L1 RPC and relay its response.
async fn forward(ctx: &Ctx, body: Vec<u8>) -> Response<Full<HyperBytes>> {
    match ctx
        .client
        .post(&ctx.l1_rpc_url)
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
            event!(name: "eez.l1_interceptor.upstream_error", Level::WARN, error = %e, "L1 upstream error");
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(HyperBytes::from(format!(
                    "L1 upstream error: {e}"
                ))))
                .expect("valid response")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxEip1559};
    use alloy_network::TxSignerSync;
    use alloy_primitives::{TxKind, U256, address, hex};
    use alloy_signer_local::PrivateKeySigner;

    const EEZ_L1: Address = address!("00000000000000000000000000000000000000ee");
    const PROXY: Address = address!("00000000000000000000000000000000000000bb");
    const PLAIN: Address = address!("00000000000000000000000000000000000000cc");

    /// Minimal mock L1 JSON-RPC: `authorizedProxies(PROXY)` non-zero (zero for
    /// anything else), passes the admission gate (`getTransactionCount`=0,
    /// `getBalance`=1 ETH), and tags every other call so the test can prove a
    /// forward.
    async fn spawn_mock_l1() -> String {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                tokio::spawn(async move {
                    let svc = service_fn(|req: Request<hyper::body::Incoming>| async move {
                        let body = req.collect().await.unwrap().to_bytes();
                        let j: Value = serde_json::from_slice(&body).unwrap_or_default();
                        let method = j.get("method").and_then(|m| m.as_str()).unwrap_or("");
                        let id = j.get("id").cloned().unwrap_or(Value::Null);
                        let result = match method {
                            "eth_call" => {
                                // alloy serializes calldata as `input` (modern) or
                                // `data` (legacy) — accept either.
                                let data = j["params"][0]["input"]
                                    .as_str()
                                    .or_else(|| j["params"][0]["data"].as_str())
                                    .unwrap_or("");
                                if data
                                    .to_lowercase()
                                    .ends_with(&hex::encode(PROXY.as_slice()))
                                {
                                    // ABI (address originalAddress=0x..dd, uint256
                                    // originalRollupId=1): two 32-byte words.
                                    format!(
                                        "0x{}{}",
                                        "00000000000000000000000000000000000000000000000000000000000000dd",
                                        "0000000000000000000000000000000000000000000000000000000000000001",
                                    )
                                } else {
                                    format!("0x{}", "00".repeat(64))
                                }
                            }
                            "eth_getTransactionCount" => "0x0".to_string(),
                            "eth_getBalance" => "0xde0b6b3a7640000".to_string(),
                            _ => "0xforwarded".to_string(),
                        };
                        let resp =
                            serde_json::json!({ "jsonrpc": "2.0", "result": result, "id": id });
                        Ok::<_, hyper::Error>(Response::new(Full::new(HyperBytes::from(
                            resp.to_string(),
                        ))))
                    });
                    let io = TokioIo::new(stream);
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        url
    }

    fn signed_tx_to(signer: &PrivateKeySigner, to: Address) -> Bytes {
        use alloy_eips::eip2718::Encodable2718;
        let mut tx = TxEip1559 {
            chain_id: 31337,
            nonce: 0,
            gas_limit: 100_000,
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(to),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let mut buf = Vec::new();
        tx.into_signed(sig).encode_2718(&mut buf);
        Bytes::from(buf)
    }

    async fn rpc(client: &reqwest::Client, url: &str, method: &str, params: Value) -> Value {
        client
            .post(url)
            .json(&serde_json::json!({ "jsonrpc": "2.0", "method": method, "params": params, "id": 1 }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn holds_crosschain_and_forwards_the_rest() {
        let l1_url = spawn_mock_l1().await;
        let held_pool = Arc::new(HeldPool::new());
        let provider = alloy_provider::RootProvider::new_http(l1_url.parse().unwrap());

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // free the port for the interceptor
        {
            let held_pool = Arc::clone(&held_pool);
            let l1_url = l1_url.clone();
            tokio::spawn(async move {
                run_l1_interceptor(port, l1_url, EEZ_L1, held_pool, provider)
                    .await
                    .unwrap();
            });
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let front = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();
        let signer = PrivateKeySigner::random();

        // (1) A plain eth_* is FORWARDED to L1.
        let r = rpc(&client, &front, "eth_chainId", serde_json::json!([])).await;
        assert_eq!(
            r["result"], "0xforwarded",
            "non-tx method must be forwarded"
        );

        // (2) A cross-chain tx (to the registered PROXY) is HELD (Inbound).
        let xchain = signed_tx_to(&signer, PROXY);
        let r = rpc(
            &client,
            &front,
            "eth_sendRawTransaction",
            serde_json::json!([xchain]),
        )
        .await;
        let hash = r["result"].as_str().expect("held tx returns its hash");
        assert!(
            hash.starts_with("0x") && hash.len() == 66,
            "expected a tx hash, got {hash}"
        );
        assert_eq!(
            held_pool.held_count_for(signer.address(), Direction::Inbound),
            1,
            "the cross-chain tx must be held for the composer to drain",
        );

        // (3) A tx to a PLAIN (non-proxy) contract is FORWARDED, not held.
        let plain = signed_tx_to(&signer, PLAIN);
        let r = rpc(
            &client,
            &front,
            "eth_sendRawTransaction",
            serde_json::json!([plain]),
        )
        .await;
        assert_eq!(
            r["result"], "0xforwarded",
            "a non-cross-chain tx must be forwarded"
        );
        assert_eq!(
            held_pool.held_count_for(signer.address(), Direction::Inbound),
            1,
            "a plain tx must NOT be held",
        );
    }
}
