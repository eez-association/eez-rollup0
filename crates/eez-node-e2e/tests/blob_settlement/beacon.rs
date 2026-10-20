use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use alloy_primitives::{B256, hex};
use alloy_provider::{Provider, ProviderBuilder};
use anyhow::{Context, Result};
use http_body_util::{BodyExt as _, Full};
use hyper::{
    Request, Response, StatusCode,
    body::{Bytes, Incoming},
};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub struct Capture {
    pub transaction_hash: B256,
    pub block_hash: B256,
    pub timestamp: u64,
    pub blobs: Vec<Vec<u8>>,
}

#[derive(Default, Debug)]
struct State {
    rpc: Mutex<String>,
    captures: Mutex<Vec<Capture>>,
    fee: AtomicU64,
}

#[derive(Debug)]
pub struct Beacon {
    pub url: String,
    state: Arc<State>,
    task: tokio::task::JoinHandle<()>,
}

impl Beacon {
    pub async fn spawn() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(State::default());
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let service =
                        hyper::service::service_fn(move |request| respond(request, state.clone()));
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Ok(Self { url, state, task })
    }

    pub fn connect(&self, rpc: &str) {
        *self.state.rpc.lock().unwrap() = rpc.to_owned();
    }

    pub fn quote_fee(&self, fee: u64) {
        self.state.fee.store(fee, Ordering::Relaxed);
    }

    pub async fn canonical(&self) -> Result<Vec<Capture>> {
        canonical(&self.state, None).await
    }
}

impl Drop for Beacon {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn canonical(state: &State, timestamp: Option<u64>) -> Result<Vec<Capture>> {
    let rpc = state.rpc.lock().unwrap().clone();
    let provider = ProviderBuilder::new().connect_http(rpc.parse()?);
    let captures = state.captures.lock().unwrap().clone();
    let mut result = Vec::new();
    for capture in captures {
        if timestamp.is_some_and(|timestamp| timestamp != capture.timestamp) {
            continue;
        }
        let Some(receipt) = provider
            .get_transaction_receipt(capture.transaction_hash)
            .await?
        else {
            continue;
        };
        let number = receipt
            .block_number
            .context("mined receipt has no block number")?;
        let block = provider.get_block_by_number(number.into()).await?;
        if block.is_some_and(|block| block.header.hash == capture.block_hash) {
            result.push(capture);
        }
    }
    Ok(result)
}

async fn handle(request: Request<Incoming>, state: &State) -> Result<Value> {
    let path = request.uri().path().to_owned();
    if request.method() == hyper::Method::POST {
        let body = request.into_body().collect().await?.to_bytes();
        let value: Value = serde_json::from_slice(&body)?;
        if path == "/ingest" {
            let raw = hex::decode(
                value["raw"]
                    .as_str()
                    .context("missing pooled transaction")?,
            )?;
            let capture = Capture {
                transaction_hash: eez_blob::pooled_blob_transaction_hash(&raw)?,
                block_hash: value["blockHash"]
                    .as_str()
                    .context("missing block hash")?
                    .parse()?,
                timestamp: value["timestamp"].as_u64().context("missing timestamp")?,
                blobs: eez_blob::pooled_blob_bytes(&raw)?,
            };
            state.captures.lock().unwrap().push(capture);
            return Ok(json!({}));
        }
        anyhow::ensure!(path == "/rpc", "unknown POST endpoint");
        let fee = state.fee.load(Ordering::Relaxed);
        if value["method"] == "eth_blobBaseFee" && fee != 0 {
            return Ok(
                json!({"jsonrpc": "2.0", "id": value["id"], "result": format!("0x{fee:x}")}),
            );
        }
        let rpc = state.rpc.lock().unwrap().clone();
        return Ok(reqwest::Client::new()
            .post(rpc)
            .json(&value)
            .send()
            .await?
            .json()
            .await?);
    }
    let rpc = state.rpc.lock().unwrap().clone();
    let provider = ProviderBuilder::new().connect_http(rpc.parse()?);
    let genesis = provider
        .get_block_by_number(0.into())
        .await?
        .context("missing genesis")?;
    if path == "/eth/v1/beacon/genesis" {
        return Ok(json!({"data": {"genesis_time": genesis.header.timestamp.to_string()}}));
    }
    if path == "/eth/v1/config/spec" {
        return Ok(
            json!({"data": {"SECONDS_PER_SLOT": eez_testkit::L1_BLOCK_TIME_SECS.to_string()}}),
        );
    }
    let slot: u64 = path.rsplit('/').next().context("missing slot")?.parse()?;
    let timestamp = genesis.header.timestamp + slot * eez_testkit::L1_BLOCK_TIME_SECS;
    let captures = canonical(state, Some(timestamp)).await?;
    if path.starts_with("/eth/v2/beacon/blocks/") {
        let hash = captures
            .first()
            .context("slot has no canonical captured blob post")?
            .block_hash;
        return Ok(
            json!({"data": {"message": {"body": {"execution_payload": {"block_hash": hash}}}}}),
        );
    }
    anyhow::ensure!(
        path.starts_with("/eth/v1/beacon/blobs/"),
        "unknown beacon endpoint"
    );
    Ok(
        json!({"data": captures.iter().flat_map(|capture| &capture.blobs)
        .map(|blob| format!("0x{}", hex::encode(blob))).collect::<Vec<_>>()}),
    )
}

async fn respond(
    request: Request<Incoming>,
    state: Arc<State>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (status, body) = match handle(request, &state).await {
        Ok(value) => (StatusCode::OK, value),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"message": error.to_string()}),
        ),
    };
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap())
}
