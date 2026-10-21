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

    /// Fetch a post's committed blobs in transaction order, pinned to its L1 block.
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
        let beacon_block = self.get(&format!("eth/v2/beacon/blocks/{slot}")).await?;
        let execution_hash = json_b256(
            &beacon_block,
            "/data/message/body/execution_payload/block_hash",
        )?;
        if execution_hash != block_hash {
            return Err(L1Error::SourceIncomplete {
                block,
                detail: format!(
                    "beacon slot {slot} execution block {execution_hash} is not L1 block {block_hash}; retry after beacon synchronization"
                ),
            });
        }
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
        let body = body.ok_or_else(|| L1Error::SourceIncomplete {
            block,
            detail: format!("beacon serves no blobs for slot {slot}: behind or pruned"),
        })?;
        let served = parse_blobs(&body)?;
        let hashes = eez_blob::versioned_hashes(&served).map_err(|error| {
            L1Error::Provider(format!("beacon served an invalid blob: {error}"))
        })?;
        select_blobs(hashes, served, &post.blob_hashes).ok_or_else(|| L1Error::SourceIncomplete {
            block,
            detail: format!("beacon slot {slot} does not serve every transaction blob"),
        })
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
    let invalid = || L1Error::Provider(format!("beacon response: invalid {pointer}"));
    let raw = value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    raw.parse().map_err(|_| invalid())
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
    fn both_beacon_response_shapes_parse() {
        let fulu = json!({ "data": ["0x0102", "0x03"] });
        let sidecars = json!({ "data": [{ "index": "0", "blob": "0x0102" }, { "blob": "0x03" }] });
        for body in [fulu, sidecars] {
            assert_eq!(parse_blobs(&body).unwrap(), [vec![1, 2], vec![3]]);
        }
    }

    #[test]
    fn malformed_beacon_bodies_are_provider_errors() {
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
    fn slots_follow_the_beacon_grid() {
        let timing = BeaconTiming {
            genesis_time: 100,
            seconds_per_slot: 12,
        };
        assert_eq!(timing.slot_at(100).unwrap(), 0);
        assert_eq!(timing.slot_at(136).unwrap(), 3);
        assert!(matches!(timing.slot_at(137), Err(L1Error::Config(_))));
        assert!(matches!(timing.slot_at(88), Err(L1Error::Config(_))));
    }

    #[test]
    fn json_numbers_accept_strings_and_integers() {
        let body = json!({ "data": { "a": "7", "b": 8, "c": true } });
        assert_eq!(json_u64(&body, "/data/a").unwrap(), 7);
        assert_eq!(json_u64(&body, "/data/b").unwrap(), 8);
        assert!(json_u64(&body, "/data/c").is_err());
        assert!(json_u64(&body, "/data/missing").is_err());
    }

    #[test]
    fn repeated_blob_hashes_reuse_the_matching_body() {
        let hash = B256::repeat_byte(1);
        let selected = select_blobs(vec![hash], vec![vec![1]], &[hash, hash]).unwrap();
        assert_eq!(selected, [vec![1], vec![1]]);
        assert!(select_blobs(vec![hash], vec![vec![1]], &[B256::repeat_byte(2)]).is_none());
    }
}
