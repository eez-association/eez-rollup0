//! Canonical L1 lookup for L2 `prev_randao`.

use alloy_primitives::B256;

use crate::error::{L1Error, L1Result};

/// The L1 header fields the selection reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RandaoHeader {
    pub number: u64,
    pub timestamp: u64,
    /// Post-merge `mixHash`, which carries the beacon chain's RANDAO output.
    pub prev_randao: B256,
}

/// The Sync timestamp `T` whose interval `(T, T + l1_block_time_secs]`
/// contains `l2_timestamp`.
///
/// Sync timestamps sit on the L1 slot grid, so any L1 header timestamp
/// (`l1_anchor`) fixes the grid.
#[must_use]
pub fn sync_timestamp_for(l2_timestamp: u64, l1_anchor: u64, l1_block_time_secs: u64) -> u64 {
    let last = i128::from(l2_timestamp) - 1;
    let offset = (last - i128::from(l1_anchor)).rem_euclid(i128::from(l1_block_time_secs));
    // `offset <= last`, and both fit in u64 when `l2_timestamp > 0`.
    u64::try_from(last - offset).unwrap_or(0)
}

/// The `prevRandao` for the L2 block at `l2_timestamp`: the `mixHash` of the
/// latest L1 block strictly before the interval's Sync timestamp `T`.
///
/// `latest` is the source's current head and `fetch` reads the canonical header
/// by number. Returns [`B256::ZERO`] when no L1 block precedes `T`.
///
/// # Errors
///
/// - [`L1Error::Config`] for a zero `l1_block_time_secs`.
/// - [`L1Error::SourceIncomplete`] if the head is before the last slot
///   preceding `T`, or a header is missing during the search.
/// - Any `fetch` error.
pub async fn select_prev_randao<F, Fut>(
    latest: RandaoHeader,
    l2_timestamp: u64,
    l1_block_time_secs: u64,
    mut fetch: F,
) -> L1Result<B256>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = L1Result<Option<RandaoHeader>>>,
{
    if l1_block_time_secs == 0 {
        return Err(L1Error::Config("L1 block time must be nonzero".into()));
    }
    let sync = sync_timestamp_for(l2_timestamp, latest.timestamp, l1_block_time_secs);
    // On the slot grid, every block before `T` is at or before `T - slot`.
    // Once the head reaches that slot, nothing earlier than `T` can still land.
    // An older head is either missed slots or a lagging source; the two look
    // the same here, so wait.
    let last_slot = sync.saturating_sub(l1_block_time_secs);
    if latest.timestamp < last_slot {
        return Err(L1Error::SourceIncomplete {
            block: latest.number,
            detail: format!(
                "L1 head timestamp {} is before slot {last_slot}; prevRandao for Sync {sync} not final",
                latest.timestamp
            ),
        });
    }
    if latest.timestamp < sync {
        return Ok(latest.prev_randao);
    }

    // Use slot spacing to estimate the lower bound, then check the
    // bracket before searching.
    let mut hi = latest.number;
    let slots_back = (latest.timestamp - sync) / l1_block_time_secs + 1;
    let mut lo = fetch_at(&mut fetch, hi.saturating_sub(slots_back)).await?;
    if lo.timestamp >= sync {
        hi = lo.number;
        if hi == 0 {
            return Ok(B256::ZERO);
        }
        lo = fetch_at(&mut fetch, 0).await?;
        if lo.timestamp >= sync {
            return Ok(B256::ZERO);
        }
    }
    while hi - lo.number > 1 {
        let mid = fetch_at(&mut fetch, lo.number + (hi - lo.number) / 2).await?;
        if mid.timestamp < sync {
            lo = mid;
        } else {
            hi = mid.number;
        }
    }
    Ok(lo.prev_randao)
}

async fn fetch_at<F, Fut>(fetch: &mut F, number: u64) -> L1Result<RandaoHeader>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = L1Result<Option<RandaoHeader>>>,
{
    fetch(number)
        .await?
        .ok_or_else(|| L1Error::SourceIncomplete {
            block: number,
            detail: "L1 block vanished while selecting prevRandao".into(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const L1: u64 = 12;

    /// A chain with one block per timestamp, numbered from zero.
    fn chain(timestamps: &[u64]) -> Vec<RandaoHeader> {
        timestamps
            .iter()
            .enumerate()
            .map(|(i, &timestamp)| RandaoHeader {
                number: i as u64,
                timestamp,
                prev_randao: B256::with_last_byte(i as u8 + 1),
            })
            .collect()
    }

    async fn select(chain: &[RandaoHeader], l2_timestamp: u64) -> L1Result<B256> {
        let latest = *chain.last().unwrap();
        select_prev_randao(latest, l2_timestamp, L1, |n| {
            let header = chain.get(n as usize).copied();
            async move { Ok(header) }
        })
        .await
    }

    fn randao_of(chain: &[RandaoHeader], number: usize) -> B256 {
        chain[number].prev_randao
    }

    /// Spec example: blocks `P` at `T - 12` and `E` at `T`. Positions `T + 2`
    /// through `T + 12` use `P`; `E` first applies from `T + 14`.
    #[tokio::test]
    async fn interval_copies_the_block_strictly_before_its_sync_timestamp() {
        let t = 1_200;
        let c = chain(&[t - 36, t - 24, t - 12, t, t + 12, t + 24]);
        for offset in (2..=12).step_by(2) {
            assert_eq!(select(&c, t + offset).await.unwrap(), randao_of(&c, 2));
        }
        for offset in (14..=24).step_by(2) {
            assert_eq!(select(&c, t + offset).await.unwrap(), randao_of(&c, 3));
        }
    }

    /// A missed slot before `T` falls back to the latest earlier block.
    #[tokio::test]
    async fn missed_slots_use_the_latest_earlier_block() {
        let t = 1_200;
        let c = chain(&[t - 60, t - 48, t, t + 12, t + 24]);
        for offset in (2..=12).step_by(2) {
            assert_eq!(select(&c, t + offset).await.unwrap(), randao_of(&c, 1));
        }
    }

    /// The first and last L2 blocks of an interval resolve the same `T`.
    #[test]
    fn sync_timestamp_brackets_the_interval() {
        let t = 1_200;
        for anchor in [t - 36, t, t + 120] {
            assert_eq!(sync_timestamp_for(t + 2, anchor, L1), t);
            assert_eq!(sync_timestamp_for(t + 12, anchor, L1), t);
            assert_eq!(sync_timestamp_for(t + 14, anchor, L1), t + 12);
        }
    }

    /// The block at `T` itself is not needed: a head one slot before `T`
    /// already settles the interval, so a source lagging behind slot `T`
    /// (or a missed slot `T`) does not stall it.
    #[tokio::test]
    async fn head_one_slot_before_sync_is_final() {
        let t = 1_200;
        let c = chain(&[t - 36, t - 24, t - 12]);
        for offset in [2, 12] {
            assert_eq!(select(&c, t + offset).await.unwrap(), randao_of(&c, 2));
        }
    }

    /// Consecutive missed slots up to `T`: the latest earlier block still
    /// applies once a later block bounds the search.
    #[tokio::test]
    async fn consecutive_missed_slots_before_sync() {
        let t = 1_200;
        let c = chain(&[t - 48, t - 36, t + 12]);
        for offset in [2, 12] {
            assert_eq!(select(&c, t + offset).await.unwrap(), randao_of(&c, 1));
        }
    }

    /// A head older than the slot before `T` cannot tell missed slots from
    /// lag, so selection waits.
    #[tokio::test]
    async fn head_older_than_last_slot_waits() {
        let t = 1_200;
        let c = chain(&[t - 36, t - 24]);
        assert!(matches!(
            select(&c, t + 2).await,
            Err(L1Error::SourceIncomplete { block: 1, .. })
        ));
    }

    /// No L1 block before `T` (a dev L1 started with its L2).
    #[tokio::test]
    async fn no_earlier_block_selects_zero() {
        let c = chain(&[1_000, 1_012, 1_024]);
        assert_eq!(select(&c, 1_002).await.unwrap(), B256::ZERO);
        assert_eq!(select(&c, 1_012).await.unwrap(), B256::ZERO);
        assert_eq!(select(&c, 1_014).await.unwrap(), randao_of(&c, 0));
    }

    /// Blocks denser than one slot still select the latest one before `T`.
    #[tokio::test]
    async fn dense_blocks_fall_back_to_binary_search() {
        let c = chain(&[100, 101, 102, 103, 104, 105, 106, 107, 130]);
        // Grid anchored at 130: `T` = 106.
        assert_eq!(select(&c, 116).await.unwrap(), randao_of(&c, 5));
    }
}
