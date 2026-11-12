//! Beacon blob retrieval, checked against the posting transaction's versioned hashes.

use std::{collections::HashMap, time::Duration};

use alloy_primitives::{B256, hex};
use alloy_provider::Provider;
use serde_json::Value;
use tokio::sync::OnceCell;
use tracing::{Level, event};
use url::Url;

use crate::{L1Error, L1Result, scan::PostId};

#[derive(Debug, Clone, Copy)]
struct BeaconTiming {
    genesis_time: u64,
    seconds_per_slot: u64,
}

impl BeaconTiming {
    /// Slot of an execution block, checked against the beacon's slot grid.
    fn slot_at(self, timestamp: u64) -> L1Result<u64> {
        let elapsed = timestamp
            .checked_sub(self.genesis_time)
            .filter(|elapsed| elapsed.is_multiple_of(self.seconds_per_slot))
            .ok_or_else(|| {
                L1Error::Config(format!(
                    "L1 timestamp {timestamp} is not on the beacon slot grid \
                     (genesis {}, {}s slots); EEZ_L1_BEACON_URL serves another chain",
                    self.genesis_time, self.seconds_per_slot,
                ))
            })?;
        Ok(elapsed / self.seconds_per_slot)
    }
}

/// Bounds each beacon request, so a stalled node fails and is retried.
const BEACON_TIMEOUT: Duration = Duration::from_secs(30);

/// Beacon REST client for the blobs a settlement transaction committed to.
#[derive(Debug)]
pub(crate) struct BeaconBlobSource {
    base_url: Url,
    client: reqwest::Client,
    timing: OnceCell<BeaconTiming>,
}

impl BeaconBlobSource {
    #[must_use]
    pub(crate) fn new(mut base_url: Url) -> Self {
        // `Url::join` replaces the last segment unless the path ends in `/`.
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        Self {
            base_url,
            client: reqwest::Client::new(),
            timing: OnceCell::new(),
        }
    }

    /// Fetch a post's committed blobs in blobIndices order, pinned to its L1 block.
    pub(crate) async fn fetch(
        &self,
        provider: &impl Provider,
        id: &PostId,
        wanted: &[B256],
    ) -> L1Result<Vec<Vec<u8>>> {
        let block_hash = id.l1_block_hash;
        let timestamp = provider
            .get_block_by_hash(block_hash)
            .await
            .map_err(|e| L1Error::Provider(format!("get_block({block_hash}): {e}")))?
            .ok_or_else(|| L1Error::SourceIncomplete {
                block: id.l1_block,
                detail: format!("block {block_hash} is not served; retry"),
            })?
            .header
            .timestamp;
        let slot = self.timing().await?.slot_at(timestamp)?;
        let filter = wanted
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        // A client may reject or mishandle the filter, so any failure falls back
        // to the whole slot.
        let filtered = format!("eth/v1/beacon/blobs/{slot}?versioned_hashes={filter}");
        match self.blobs_at(&filtered, wanted).await {
            Ok(Some(blobs)) => return Ok(blobs),
            Ok(None) => {}
            Err(error) => event!(
                name: "eez.l1.beacon.filtered_blobs_failed",
                Level::WARN,
                slot,
                error = %error,
                "filtered blob request failed; fetching the whole slot",
            ),
        }
        // Pre-Fulu nodes only serve sidecars.
        for path in [
            format!("eth/v1/beacon/blobs/{slot}"),
            format!("eth/v1/beacon/blob_sidecars/{slot}"),
        ] {
            if let Some(blobs) = self.blobs_at(&path, wanted).await? {
                return Ok(blobs);
            }
        }
        Err(self.missing_blobs(id, slot).await?)
    }

    /// The wanted blobs from one beacon response, re-verified by hash; `None`
    /// when it is not served or lacks any of them.
    async fn blobs_at(&self, path: &str, wanted: &[B256]) -> L1Result<Option<Vec<Vec<u8>>>> {
        let Some(body) = self.get_optional(path).await? else {
            return Ok(None);
        };
        let served = parse_blobs(&body)?;
        let hashes = eez_blob::versioned_hashes(&served).map_err(|error| {
            L1Error::Provider(format!("beacon served an invalid blob: {error}"))
        })?;
        Ok(select_blobs(hashes, served, wanted))
    }

    async fn missing_blobs(&self, id: &PostId, slot: u64) -> L1Result<L1Error> {
        // A node still backfilling serves no block at the slot yet.
        let served = self
            .get_optional(&format!("eth/v1/beacon/blinded_blocks/{slot}"))
            .await?
            .map(|block| {
                json_b256(
                    &block,
                    "/data/message/body/execution_payload_header/block_hash",
                )
            })
            .transpose()?;
        let finalized = self.get("eth/v1/beacon/headers/finalized").await?;
        let finalized_slot = json_u64(&finalized, "/data/header/message/slot")?;
        Ok(missing_blob_error(
            id.l1_block,
            slot,
            finalized_slot,
            served == Some(id.l1_block_hash),
        ))
    }

    async fn timing(&self) -> L1Result<BeaconTiming> {
        self.timing
            .get_or_try_init(|| async {
                let genesis = self.get("eth/v1/beacon/genesis").await?;
                let spec = self.get("eth/v1/config/spec").await?;
                let seconds_per_slot = json_u64(&spec, "/data/SECONDS_PER_SLOT")?;
                if seconds_per_slot == 0 {
                    return Err(L1Error::Provider("beacon SECONDS_PER_SLOT is zero".into()));
                }
                Ok(BeaconTiming {
                    genesis_time: json_u64(&genesis, "/data/genesis_time")?,
                    seconds_per_slot,
                })
            })
            .await
            .copied()
    }

    async fn get(&self, path: &str) -> L1Result<Value> {
        self.get_optional(path)
            .await?
            .ok_or_else(|| L1Error::Provider(format!("beacon {path}: not served")))
    }

    async fn get_optional(&self, path: &str) -> L1Result<Option<Value>> {
        let url = self
            .base_url
            .join(path)
            .map_err(|error| L1Error::Config(format!("EEZ_L1_BEACON_URL: {error}")))?;
        let response = self
            .client
            .get(url)
            .timeout(BEACON_TIMEOUT)
            .send()
            .await
            .map_err(|error| L1Error::Provider(format!("beacon {path}: {error}")))?;
        // 405 is how some clients answer an endpoint they do not implement.
        if matches!(
            response.status(),
            reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::METHOD_NOT_ALLOWED
        ) {
            return Ok(None);
        }
        // Clients answer 400 for what they cannot serve, e.g. blobs without
        // full column custody; each words it differently.
        if response.status() == reqwest::StatusCode::BAD_REQUEST {
            let body = response.text().await.unwrap_or_default();
            event!(
                name: "eez.l1.beacon.bad_request",
                Level::WARN,
                path,
                body,
                "beacon cannot serve {{path}}",
            );
            return Ok(None);
        }
        let response = response
            .error_for_status()
            .map_err(|error| L1Error::Provider(format!("beacon {path}: {error}")))?;
        response
            .json()
            .await
            .map(Some)
            .map_err(|error| L1Error::Provider(format!("beacon {path}: {error}")))
    }
}

fn missing_blob_error(block: u64, slot: u64, finalized_slot: u64, block_served: bool) -> L1Error {
    if !block_served {
        L1Error::BeaconLagging {
            block,
            detail: format!(
                "beacon does not serve this execution block at slot {slot}; retry after synchronization"
            ),
        }
    } else if finalized_slot < slot {
        L1Error::BeaconLagging {
            block,
            detail: format!(
                "beacon has not finalized blob slot {slot}; retry after synchronization"
            ),
        }
    } else {
        L1Error::BlobUnavailable {
            block,
            detail: format!(
                "beacon serves the block but not all selected blobs at finalized slot {slot}; use a supernode retaining this history"
            ),
        }
    }
}

fn select_blobs(
    hashes: Vec<B256>,
    blobs: Vec<Vec<u8>>,
    requested: &[B256],
) -> Option<Vec<Vec<u8>>> {
    let by_hash: HashMap<_, _> = hashes.into_iter().zip(blobs).collect();
    requested
        .iter()
        .map(|hash| by_hash.get(hash).cloned())
        .collect()
}

/// Blob bodies from either response shape: `blobs` serves hex strings,
/// `blob_sidecars` objects with a `blob` field.
fn parse_blobs(body: &Value) -> L1Result<Vec<Vec<u8>>> {
    let malformed = |detail: &str| L1Error::Provider(format!("beacon blob response: {detail}"));
    body.pointer("/data")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("no data array"))?
        .iter()
        .map(|item| {
            let raw = item
                .as_str()
                .or_else(|| item.get("blob").and_then(Value::as_str))
                .ok_or_else(|| malformed("item is neither a blob nor a sidecar"))?;
            hex::decode(raw).map_err(|error| malformed(&format!("invalid blob hex: {error}")))
        })
        .collect()
}

fn json_b256(value: &Value, pointer: &str) -> L1Result<B256> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .and_then(|hash| hash.parse().ok())
        .ok_or_else(|| L1Error::Provider(format!("beacon response: invalid {pointer}")))
}

fn json_u64(value: &Value, pointer: &str) -> L1Result<u64> {
    let invalid = || L1Error::Provider(format!("beacon response: invalid {pointer}"));
    match value.pointer(pointer).ok_or_else(invalid)? {
        Value::String(value) => value.parse().map_err(|_| invalid()),
        Value::Number(value) => value.as_u64().ok_or_else(invalid),
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A beacon answering `routes` by path and query; anything else is a 404.
    async fn beacon(routes: Vec<(String, u16, String)>) -> BeaconBlobSource {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = BufReader::new(&mut socket);
                let mut line = String::new();
                request.read_line(&mut line).await.unwrap();
                let path = line.split(' ').nth(1).unwrap_or_default();
                let path = path.trim_start_matches('/').to_owned();
                let mut header = String::new();
                while request.read_line(&mut header).await.unwrap() > 2 {
                    header.clear();
                }
                let (status, body) = routes
                    .iter()
                    .find(|(route, ..)| *route == path)
                    .map_or((404, String::new()), |(_, status, body)| {
                        (*status, body.clone())
                    });
                let reply = format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        BeaconBlobSource::new(url)
    }

    #[tokio::test]
    async fn a_missing_blob_halts_only_when_the_beacon_serves_the_block_finalized() {
        use alloy_provider::ProviderBuilder;
        use alloy_transport::mock::Asserter;
        #[derive(Debug)]
        enum Want {
            Blob,
            Lagging,
            Unavailable,
            Provider,
        }
        let slot = 5_u64;
        let blob = vec![0_u8; eez_blob::BYTES_PER_BLOB];
        let mut other = blob.clone();
        other[31] = 1;
        let hashes = eez_blob::versioned_hashes(&[&blob[..], &other[..]]).unwrap();
        let (ours, theirs) = (hashes[0], hashes[1]);
        let id = PostId {
            l1_block: 9,
            l1_block_hash: B256::repeat_byte(9),
            log_index: 0,
        };
        let blobs = |b: &[u8]| json!({ "data": [format!("0x{}", hex::encode(b))] }).to_string();
        let block = |hash: B256| {
            json!({ "data": { "message": { "body": {
                "execution_payload_header": { "block_hash": hash } } } } })
            .to_string()
        };
        let filtered = format!("eth/v1/beacon/blobs/{slot}?versioned_hashes={ours}");
        let whole = format!("eth/v1/beacon/blobs/{slot}");
        let blinded = format!("eth/v1/beacon/blinded_blocks/{slot}");
        let cases = [
            // A rejected filter falls back to the whole slot.
            (
                vec![
                    (filtered.clone(), 500, String::new()),
                    (whole.clone(), 200, blobs(&blob)),
                ],
                10,
                Want::Blob,
            ),
            // A block the beacon does not serve yet, or serves on another
            // chain, retries even at a finalized slot.
            (
                vec![
                    (filtered.clone(), 400, String::new()),
                    (whole.clone(), 200, blobs(&other)),
                ],
                10,
                Want::Lagging,
            ),
            (
                vec![
                    (whole.clone(), 200, blobs(&other)),
                    (blinded.clone(), 200, block(theirs)),
                ],
                10,
                Want::Lagging,
            ),
            // Only the post's own block, served at a finalized slot, halts.
            (
                vec![(blinded.clone(), 200, block(id.l1_block_hash))],
                4,
                Want::Lagging,
            ),
            (
                vec![(blinded.clone(), 200, block(id.l1_block_hash))],
                10,
                Want::Unavailable,
            ),
            // A server error on the whole slot retries rather than halting.
            (
                vec![(whole.clone(), 500, String::new())],
                10,
                Want::Provider,
            ),
        ];
        for (mut routes, finalized, want) in cases {
            routes.extend([
                ("eth/v1/beacon/genesis".into(), 200, json!({ "data": { "genesis_time": "0" } }).to_string()),
                ("eth/v1/config/spec".into(), 200, json!({ "data": { "SECONDS_PER_SLOT": "12" } }).to_string()),
                (
                    "eth/v1/beacon/headers/finalized".into(),
                    200,
                    json!({ "data": { "header": { "message": { "slot": finalized.to_string() } } } }).to_string(),
                ),
            ]);
            let source = beacon(routes).await;
            let asserter = Asserter::new();
            let mut l1_block =
                alloy_rpc_types_eth::Block::<alloy_rpc_types_eth::Transaction>::default();
            l1_block.header.inner.timestamp = slot * 12;
            asserter.push_success(&l1_block);
            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let got = source.fetch(&provider, &id, &[ours]).await;
            let ok = match (&want, &got) {
                (Want::Blob, Ok(served)) => served == std::slice::from_ref(&blob),
                (Want::Lagging, Err(L1Error::BeaconLagging { .. }))
                | (Want::Unavailable, Err(L1Error::BlobUnavailable { .. }))
                | (Want::Provider, Err(L1Error::Provider(_))) => true,
                _ => false,
            };
            assert!(ok, "{want:?}: {got:?}");
        }
    }

    #[test]
    fn repeated_blob_hashes_reuse_the_matching_body() {
        let hash = B256::repeat_byte(1);
        let selected = select_blobs(vec![hash], vec![vec![1]], &[hash, hash]).unwrap();
        assert_eq!(selected, [vec![1], vec![1]]);
        assert!(select_blobs(vec![hash], vec![vec![1]], &[B256::repeat_byte(2)]).is_none());
    }

    #[test]
    fn beacon_responses_parse_blobs_and_reject_malformed_data() {
        let fulu = json!({ "data": ["0x0102", "0x03"] });
        let sidecars = json!({ "data": [{ "index": "0", "blob": "0x0102" }, { "blob": "0x03" }] });
        for body in [fulu, sidecars] {
            assert_eq!(parse_blobs(&body).unwrap(), [vec![1, 2], vec![3]]);
        }

        for body in [
            json!({}),
            json!({ "data": [{ "index": "0" }] }),
            json!({ "data": ["0xzz"] }),
        ] {
            assert!(
                matches!(parse_blobs(&body), Err(L1Error::Provider(_))),
                "{body}"
            );
        }
    }

    #[test]
    fn beacon_timing_rejects_invalid_slots_and_numbers() {
        for body in [
            json!({ "genesis": "100", "seconds_per_slot": 12 }),
            json!({ "genesis": 100, "seconds_per_slot": "12" }),
        ] {
            let timing = BeaconTiming {
                genesis_time: json_u64(&body, "/genesis").unwrap(),
                seconds_per_slot: json_u64(&body, "/seconds_per_slot").unwrap(),
            };
            for (timestamp, expected) in [(100, Some(0)), (136, Some(3)), (137, None), (88, None)] {
                match expected {
                    Some(slot) => assert_eq!(timing.slot_at(timestamp).unwrap(), slot),
                    None => assert!(matches!(timing.slot_at(timestamp), Err(L1Error::Config(_)))),
                }
            }
        }
        for body in [json!({ "genesis": true }), json!({})] {
            assert!(matches!(
                json_u64(&body, "/genesis"),
                Err(L1Error::Provider(_))
            ));
        }
    }
}
