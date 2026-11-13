//! Shared source state for nested dispatch that re-enters a rollup.
//!
//! Dispatch is synchronous: the source EVM stays suspended inside its
//! inspector while the target runs. When the target calls back into the
//! source rollup, the re-entered session must see the source's in-flight
//! state, and the source must see what the session wrote.
//!
//! [`OverlayChannel`] carries both directions as evm2 [`Cache`]s. The
//! inspector pushes a snapshot of its live state before dispatching; a
//! re-entered session opens over that snapshot and publishes the writes of
//! each execution; after the dispatch the inspector replays those writes
//! through the source journal with [`apply_overlay_writes`], so a later revert
//! of the source frame undoes them.

use std::sync::{Arc, Mutex};

use alloy_primitives::Address;
use evm2::evm::{Cache, State};

/// Cache exchange for nested dispatches that re-enter a suspended rollup.
///
/// - `snapshots` holds this rollup's live state at each pending dispatch. A
///   session that re-enters the rollup opens over the latest one.
/// - `writes` holds the writes of each execution of a re-entered session. The
///   suspended inspector takes the ones published during its dispatch.
///
/// Both are stacks, so recursive re-entry pairs each frame with its own
/// entries. All access is same-thread; a poisoned lock means a dispatch
/// panicked mid-exchange, so methods panic rather than silently dropping state.
#[derive(Debug, Default)]
pub struct OverlayChannel {
    snapshots: Mutex<Vec<Cache>>,
    writes: Mutex<Vec<Cache>>,
}

/// A poisoned lock means a panic escaped mid-exchange; continuing would
/// silently drop state propagation.
const POISONED: &str = "overlay channel mutex poisoned by a panicked dispatch";

impl OverlayChannel {
    /// Clear all entries at a transaction-composition boundary.
    pub fn reset(&self) {
        self.snapshots.lock().expect(POISONED).clear();
        self.writes.lock().expect(POISONED).clear();
    }

    /// Push this rollup's live state before a dispatch.
    pub fn push_pre_snapshot(&self, cache: Cache) {
        self.snapshots.lock().expect(POISONED).push(cache);
    }

    /// Pop the snapshot pushed for the finished dispatch.
    pub fn pop_pre_snapshot(&self) -> Option<Cache> {
        self.snapshots.lock().expect(POISONED).pop()
    }

    /// Clone the latest snapshot for a re-entered session.
    pub fn peek_pre_snapshot(&self) -> Option<Cache> {
        self.snapshots.lock().expect(POISONED).last().cloned()
    }

    /// Publish the writes of one execution of this rollup's session.
    pub fn push_writes(&self, writes: Cache) {
        self.writes.lock().expect(POISONED).push(writes);
    }

    /// Number of published writes, marking the start of a dispatch.
    pub fn writes_len(&self) -> usize {
        self.writes.lock().expect(POISONED).len()
    }

    /// Take the writes published since `mark`, merged in publication order.
    pub fn take_writes_since(&self, mark: usize) -> Option<Cache> {
        let mut writes = self.writes.lock().expect(POISONED);
        if writes.len() <= mark {
            return None;
        }
        let mut merged = Cache::default();
        for cache in writes.drain(mark..) {
            merged.merge(cache);
        }
        Some(merged)
    }
}

/// Shared handle to an [`OverlayChannel`].
pub type OverlayChannelHandle = Arc<OverlayChannel>;

/// Build a fresh, empty [`OverlayChannelHandle`].
#[must_use]
pub fn new_overlay_channel() -> OverlayChannelHandle {
    Arc::new(OverlayChannel::default())
}

/// Errors surfaced by [`apply_overlay_writes`].
#[derive(Debug, thiserror::Error)]
pub enum OverlayError {
    /// The re-entered session created, destroyed or wiped an account. The
    /// source journal can replay value writes only, not account lifecycles.
    #[error("account {address} was created or destroyed in a re-entered session")]
    Lifecycle {
        /// Address of the created or destroyed account.
        address: Address,
    },
    /// The source state could not load an account or slot.
    #[error("source state load of {address} failed: {message}")]
    SourceLoad {
        /// Address whose load failed.
        address: Address,
        /// Underlying database error.
        message: String,
    },
}

/// Replay a re-entered session's writes on the suspended source state.
///
/// Balances, nonces and slots are written through the source journal, so a
/// revert of the frame that dispatched the call undoes them with the rest of
/// the frame.
///
/// # Errors
///
/// [`OverlayError::Lifecycle`] when the writes create, destroy or wipe an
/// account; [`OverlayError::SourceLoad`] when the source state cannot load an
/// account or slot.
pub fn apply_overlay_writes(state: &mut State<'_>, writes: &Cache) -> Result<(), OverlayError> {
    let load_err = |address: Address, error: evm2::DatabaseError| OverlayError::SourceLoad {
        address,
        message: error.to_string(),
    };
    for (address, info) in &writes.accounts {
        let Some(info) = info else {
            return Err(OverlayError::Lifecycle { address: *address });
        };
        let mut account = state
            .account(address)
            .map_err(|error| load_err(*address, error))?;
        if account.code_hash() != info.code_hash {
            return Err(OverlayError::Lifecycle { address: *address });
        }
        if account.balance() != info.balance {
            account.set_balance(info.balance);
        }
        if account.nonce() != info.nonce {
            account.set_nonce(info.nonce);
        }
    }
    for (address, storage) in &writes.storage {
        if storage.wiped {
            return Err(OverlayError::Lifecycle { address: *address });
        }
        for (key, value) in &storage.slots {
            state
                .storage_slot(address, *key)
                .map_err(|error| load_err(*address, error))?
                .set(*value);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;
    use evm2::evm::{AccountInfo, InMemoryDB};

    fn cache_with_slot(address: Address, value: u64) -> Cache {
        let mut cache = Cache::default();
        cache
            .storage
            .entry(address)
            .or_default()
            .slots
            .insert(U256::ZERO, U256::from(value));
        cache
    }

    #[test]
    fn snapshots_are_lifo_and_writes_drain_from_the_mark() {
        let address = Address::with_last_byte(0x22);
        let channel = OverlayChannel::default();
        channel.push_pre_snapshot(cache_with_slot(address, 1));
        channel.push_pre_snapshot(cache_with_slot(address, 2));
        assert_eq!(
            channel.peek_pre_snapshot(),
            Some(cache_with_slot(address, 2))
        );
        assert_eq!(
            channel.pop_pre_snapshot(),
            Some(cache_with_slot(address, 2))
        );
        assert_eq!(
            channel.pop_pre_snapshot(),
            Some(cache_with_slot(address, 1))
        );
        assert_eq!(channel.pop_pre_snapshot(), None);

        channel.push_writes(cache_with_slot(address, 1));
        let mark = channel.writes_len();
        assert_eq!(channel.take_writes_since(mark), None);
        channel.push_writes(cache_with_slot(address, 2));
        channel.push_writes(cache_with_slot(address, 3));
        // Later writes win; the entry published before the mark stays.
        assert_eq!(
            channel.take_writes_since(mark),
            Some(cache_with_slot(address, 3))
        );
        assert_eq!(channel.writes_len(), 1);

        channel.reset();
        assert_eq!(channel.writes_len(), 0);
    }

    #[test]
    fn writes_replay_through_the_source_journal() {
        let address = Address::with_last_byte(0x33);
        let mut db = InMemoryDB::default();
        db.insert_account_info(&address, AccountInfo::empty().with_balance(U256::from(5)));
        let mut state = State::new(db);
        let checkpoint = state.checkpoint();

        let mut writes = cache_with_slot(address, 7);
        writes.accounts.insert(
            address,
            Some(
                AccountInfo::empty()
                    .with_balance(U256::from(9))
                    .with_nonce(1),
            ),
        );
        apply_overlay_writes(&mut state, &writes).unwrap();
        assert_eq!(
            state.storage_slot_untracked(&address, &U256::ZERO).unwrap(),
            U256::from(7)
        );
        let info = state.account_info_untracked(&address).unwrap().unwrap();
        assert_eq!((info.balance, info.nonce), (U256::from(9), 1));

        state.rollback(
            checkpoint,
            evm2::Version::new(evm2::SpecId::PRAGUE).features,
        );
        assert_eq!(
            state.storage_slot_untracked(&address, &U256::ZERO).unwrap(),
            U256::ZERO
        );
        let info = state.account_info_untracked(&address).unwrap().unwrap();
        assert_eq!((info.balance, info.nonce), (U256::from(5), 0));
    }

    #[test]
    fn account_lifecycles_are_rejected() {
        let address = Address::with_last_byte(0x44);
        let mut state = State::new(InMemoryDB::default());

        let mut deleted = Cache::default();
        deleted.accounts.insert(address, None);
        let mut wiped = Cache::default();
        wiped.storage.entry(address).or_default().wipe();
        for writes in [deleted, wiped] {
            assert!(matches!(
                apply_overlay_writes(&mut state, &writes),
                Err(OverlayError::Lifecycle { address: a }) if a == address
            ));
        }
    }
}
