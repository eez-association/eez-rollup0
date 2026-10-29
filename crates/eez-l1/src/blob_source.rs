//! Beacon blob retrieval, checked against the posting transaction's versioned hashes.

use std::{collections::HashMap, time::Duration};

use alloy_primitives::{B256, hex};
use alloy_provider::Provider;
use serde_json::Value;
use tokio::sync::OnceCell;
use url::Url;

use crate::{L1Error, L1Result, scan::Post};

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
        post: &Post,
    ) -> L1Result<Vec<Vec<u8>>> {
        let block = post.id.l1_block;
        let block_hash = post.id.l1_block_hash;
        let timestamp = provider
            .get_block_by_hash(block_hash)
            .await
            .map_err(|e| L1Error::Provider(format!("get_block({block_hash}): {e}")))?
            .ok_or_else(|| L1Error::SourceIncomplete {
                block,
                detail: format!("block {block_hash} is not served; retry"),
            })?
            .header
            .timestamp;
        let slot = self.timing().await?.slot_at(timestamp)?;
        // Fulu's `blobs` endpoint first; pre-Fulu nodes only serve sidecars.
        let body = match self
            .get_optional(&format!("eth/v1/beacon/blobs/{slot}"))
            .await?
        {
            Some(body) => Some(body),
            None => {
                self.get_optional(&format!("eth/v1/beacon/blob_sidecars/{slot}"))
                    .await?
            }
        };
        let Some(body) = body else {
            return Err(self.missing_blobs(block, slot).await?);
        };
        let served = parse_blobs(&body)?;
        let hashes = eez_blob::versioned_hashes(&served).map_err(|error| {
            L1Error::Provider(format!("beacon served an invalid blob: {error}"))
        })?;
        match select_blobs(hashes, served, &post.blob_hashes) {
            Some(blobs) => Ok(blobs),
            None => Err(self.missing_blobs(block, slot).await?),
        }
    }

    async fn missing_blobs(&self, block: u64, slot: u64) -> L1Result<L1Error> {
        let finalized = self.get("eth/v1/beacon/headers/finalized").await?;
        let finalized_slot = json_u64(&finalized, "/data/header/message/slot")?;
        Ok(missing_blob_error(block, slot, finalized_slot))
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
            .ok_or_else(|| L1Error::Provider(format!("beacon {path}: not found")))
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
        if response.status() == reqwest::StatusCode::BAD_REQUEST
            && (path.starts_with("eth/v1/beacon/blobs/")
                || path.starts_with("eth/v1/beacon/blob_sidecars/"))
        {
            let body = response
                .json::<Value>()
                .await
                .map_err(|error| L1Error::Provider(format!("beacon {path}: {error}")))?;
            if let Some(message) = body["message"].as_str().filter(|message| {
                message.starts_with("Insufficient data columns to reconstruct blobs:")
            }) {
                tracing::warn!(path, message, "beacon blob data unavailable");
                return Ok(None);
            }
            return Err(L1Error::Provider(format!(
                "beacon {path}: 400 Bad Request: {body}"
            )));
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

fn missing_blob_error(block: u64, slot: u64, finalized_slot: u64) -> L1Error {
    if finalized_slot < slot {
        L1Error::SourceIncomplete {
            block,
            detail: format!(
                "beacon has not finalized blob slot {slot}; retry after synchronization"
            ),
        }
    } else {
        L1Error::BlobUnavailable {
            block,
            detail: format!(
                "beacon does not serve the block or all selected blobs at finalized slot {slot}; use a supernode retaining this history"
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

    #[test]
    fn missing_blobs_wait_for_finality_but_halt_for_lost_history() {
        assert!(missing_blob_error(100, 20, 19).is_retryable());
        for finalized in [20, 21] {
            assert!(matches!(
                missing_blob_error(100, 20, finalized),
                L1Error::BlobUnavailable { block: 100, .. }
            ));
            assert!(missing_blob_error(100, 20, finalized).is_terminal());
        }
    }

    #[tokio::test]
    async fn only_insufficient_columns_are_treated_as_missing_data() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let blobs = "eth/v1/beacon/blobs/20";
        let columns = "Insufficient data columns to reconstruct blobs: required 64, found 0";
        let cases = [
            (blobs, 400, columns, true),
            ("eth/v1/beacon/blob_sidecars/20", 400, columns, true),
            (blobs, 400, "Invalid block ID", false),
            (blobs, 500, columns, false),
            ("eth/v2/beacon/blocks/20", 400, columns, false),
        ];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source = BeaconBlobSource::new(
            Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap(),
        );
        let server = tokio::spawn(async move {
            for (_, status, message, _) in cases {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = BufReader::new(&mut socket);
                loop {
                    let mut line = String::new();
                    assert!(request.read_line(&mut line).await.unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                let body = json!({"message":message}).to_string();
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        for (path, _, _, missing) in cases {
            let result = tokio::time::timeout(Duration::from_secs(5), source.get_optional(path))
                .await
                .unwrap();
            if missing {
                assert!(result.unwrap().is_none(), "{path}");
            } else {
                assert!(matches!(result, Err(L1Error::Provider(_))), "{path}");
            }
        }
        server.await.unwrap();
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
