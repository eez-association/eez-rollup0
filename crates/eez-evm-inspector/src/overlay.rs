//! Overlay primitives for shared-source-state nested dispatch.
//!
//! When a target-session inspector dispatches a nested call back to
//! the entry rollup (e.g. L2→L1 during flash-loan or reentrant), the
//! nested call needs to see source-sim's in-flight state — direct
//! counterpart to how reth's revm executes nested CALL opcodes by
//! sharing the live `State<DB>`.
//!
//! The inspector's `tokio::task::block_in_place` bridge keeps
//! source-sim's `evm.transact`, target-session execution, and
//! nested-back-to-entry dispatch all on the same OS thread — no
//! Send/Sync requirements, no cross-thread cloning. The overlay
//! handle holds a fresh `State<DB>` constructed with
//! `StateBuilder::with_cached_prestate(source_cache.clone())`, runs
//! the nested call against it, and on commit walks the cache delta to
//! emit journal entries onto source-sim's live state via the
//! inspector's `&mut ctx`.
//!
//! # Why a manual clone of `State<DB>` is unnecessary
//!
//! revm's `State<DB>` lacks `Clone`, and reth's `StateProviderBox =
//! Box<dyn StateProvider + Send>` is neither `Clone` nor `Sync`.
//! Cloning `State<DB>` in full is impossible without `unsafe` fiat.
//! But we don't need to: revm's
//! [`StateBuilder::with_cached_prestate`](revm::database::states::StateBuilder::with_cached_prestate)
//! preloads a fresh `State` with another state's `CacheState` (which
//! IS `Clone`). The fresh `State` reads cold data from a fresh
//! database instance (cheap to obtain via `provider.latest()`) and
//! writes mutations to its own cache + journal. On commit, diff the
//! caches and apply.
//!
//! # Crate-layering invariants
//!
//! This module lives in `crosschain-evm-composer` because that crate
//! already depends on `revm`. It must not depend on `reth_*` or
//! `rollup-node`. The actual nested-call execution + diff-apply path
//! lives in this crate's `inspector.rs` (which has the necessary
//! `&mut Context` access during the `Inspector::call` hook).

use revm::context_interface::journaled_state::account::JournaledAccountTr;
use revm::context_interface::{ContextTr, Host, JournalTr};
use revm::database::{AccountStatus, CacheState, State};
use revm::primitives::Address;

// Note on trait choice:
//
// Reads on the OTHER direction of the overlay flow (source-sim
// inspector snapshotting cache before dispatch) go through `Host::sload`
// / `Host::load_account_info_skip_cold_load` so they pick up mid-tx
// journal writes. The natural symmetry would be `Host::sstore` here.
//
// We can't use it: `Host::sstore_skip_cold_load` delegates to
// `JournalImpl::sstore_skip_cold_load` which calls
// `inner.sstore_assume_account_present(...)`. The "assume present"
// check is stricter than `JournalTr::load_account`'s warm-up — even
// after explicitly loading the account, the journal returns
// `ColdLoadSkipped`. The plain `JournalTr::sstore(addr, key, value)`
// path (which calls `load_account_mut` internally and panics on cold
// load via `unwrap_db_error`) is the only one that actually works
// for our explicitly-warmed flow.
//
// Balance writes go through `JournaledAccountTr::set_balance` —
// `Host` exposes `balance(addr)` for reads but no public setter.

/// Field-by-field clone of revm [`State<DB>`].
///
/// Constructs the struct literally so any field addition in a future
/// revm version causes a compile error here (and the matching
/// `clone_state_field_parity` regression test catches drift in
/// existing fields). All seven fields are `pub` and individually
/// `Clone` when `DB: Clone`.
///
/// Retained as a primitive even though the live overlay path uses
/// `with_cached_prestate` instead. The field-parity test is the
/// canonical drift guard against revm version bumps.
///
/// # Errors
///
/// Infallible.
#[must_use]
pub fn clone_state<DB: Clone>(src: &State<DB>) -> State<DB> {
    State {
        cache: src.cache.clone(),
        database: src.database.clone(),
        transition_state: src.transition_state.clone(),
        bundle_state: src.bundle_state.clone(),
        use_preloaded_bundle: src.use_preloaded_bundle,
        block_hashes: src.block_hashes.clone(),
        bal_state: src.bal_state.clone(),
    }
}

/// Errors surfaced by the overlay diff-apply path.
///
/// SELFDESTRUCT is explicitly out of scope: revm's `JournalInner`
/// exposes no public method to mark an account destroyed from
/// outside, and the in-tree fixtures (`flash-loan`,
/// `reentrantCrossChainCalls`) don't destroy accounts. If the nested
/// state's cache shows a destroyed account that the source state
/// does not, the apply path surfaces this loud error rather than
/// silently miscomposing.
#[derive(Debug, thiserror::Error)]
pub enum OverlayError {
    /// The nested state shows an account in `SelfDestructed` state
    /// that the source state does not. Out of scope for the overlay
    /// diff-apply path.
    #[error("SELFDESTRUCT mutation at {address}: out of scope for overlay diff-apply")]
    Selfdestruct {
        /// Address of the destroyed account.
        address: Address,
    },
    /// Source-state journal returned a database error while loading an
    /// account during diff-apply. Surfaced verbatim (Display).
    #[error("source state load_account({address}) failed: {message}")]
    SourceLoad {
        /// Address whose load failed.
        address: Address,
        /// Underlying database error rendered via Display.
        message: String,
    },
    /// `journal.sstore` failed during diff-apply.
    #[error("source state sstore({address}, {key}) failed: {message}")]
    SourceSstore {
        /// Address whose storage write failed.
        address: Address,
        /// Storage key whose write failed.
        key: revm::primitives::StorageKey,
        /// Underlying database error rendered via Display.
        message: String,
    },
}

/// Apply the per-account, per-slot delta between two cache snapshots
/// onto a live revm context's source state, via journal entries so the
/// outer transaction's revert semantics propagate naturally.
///
/// # When this is called
///
/// Source-sim's inspector hook fires for a cross-chain CALL,
/// snapshots source's cache as `before`, populates the source-cache
/// side-channel, and drives the dispatch (`block_in_place`). During
/// that dispatch, an inner target session may dispatch back to the
/// entry rollup; the entry rollup's session opens its `State` preloaded
/// with `before` and accumulates per-overlay-call mutations. After the
/// outer dispatch returns, the inspector reads the entry session's
/// post-execute cache (`after`) from the second side-channel and calls
/// this function on source-sim's `&mut ctx`.
///
/// # Mutation classes
///
/// In scope for the diff-apply path:
///
/// - **Storage writes:** for each account in `after`, for each storage
///   slot whose value differs from `before`, emit
///   `ctx.journal_mut().sstore(addr, key, value)`. The journal entry
///   gives revert-safety (a user-tx revert in source-sim unwinds the
///   diff-applied entries together with source-sim's own writes).
///
/// - **Balance changes:** for each account whose balance differs,
///   emit `ctx.journal_mut().load_account_mut(addr).set_balance(...)`.
///   `set_balance` is the load-bearing primitive — `balance_incr` on
///   `JournalTr` is increment-only (`U256` no-wrap on decrease), and
///   overlay flows commonly involve net decreases on one side and
///   increases on the other. Going through `load_account_mut` reaches
///   `JournaledAccountTr::set_balance` which mutates the journaled
///   account directly.
///
/// Out of scope — surfaced as `Err(OverlayError::*)` or skipped:
///
/// - **SELFDESTRUCT:** loud failure via [`OverlayError::Selfdestruct`].
///   No public revm API to mark an account destroyed from outside, and
///   `flash-loan` / `reentrantCrossChainCalls` don't destroy accounts.
///
/// - **Code installation, nonce changes:** rare in overlay-relevant
///   patterns (`flash-loan`, `reentrant`); deferred. A future commit
///   can extend the function with `set_code` / `set_nonce` calls.
///
/// - **Transient storage:** EIP-1153 auto-clears at end of transaction;
///   no diff-apply needed.
///
/// # Idempotency
///
/// The inspector clears both side-channel slots after this function
/// returns. Calling it twice on the same `(before, after)` is
/// well-defined but emits redundant journal entries; callers should
/// not do this.
///
/// # Errors
///
/// - [`OverlayError::Selfdestruct`] if the overlay's cache shows an
///   account whose status flipped to destroyed during overlay execution.
/// - [`OverlayError::SourceLoad`] / [`OverlayError::SourceSstore`] if
///   the source state's database or journal surfaces an error during
///   apply.
pub fn apply_overlay_diff<CTX>(
    ctx: &mut CTX,
    before: &CacheState,
    after: &CacheState,
) -> Result<(), OverlayError>
where
    CTX: ContextTr + Host,
{
    for (addr, after_acc) in &after.accounts {
        // SELFDESTRUCT detection — `Destroyed` / `DestroyedChanged` /
        // `DestroyedAgain` are revm's "this account was selfdestructed"
        // status family. Loud-fail: no public API to apply this to
        // source state, and the in-tree overlay path doesn't need it.
        if matches!(
            after_acc.status,
            AccountStatus::Destroyed
                | AccountStatus::DestroyedChanged
                | AccountStatus::DestroyedAgain
        ) {
            return Err(OverlayError::Selfdestruct { address: *addr });
        }

        // Filter: only apply for accounts the overlay actually modified.
        // `Loaded` / `LoadedEmptyEIP161` / `LoadedNotExisting` mark
        // accounts the overlay merely *read* (cold-load to populate
        // the cache); applying their info to source is a no-op at best
        // and a "set_balance to disk value" misstep at worst.
        if !matches!(
            after_acc.status,
            AccountStatus::Changed | AccountStatus::InMemoryChange
        ) {
            continue;
        }

        let Some(after_plain) = after_acc.account.as_ref() else {
            continue;
        };
        let before_plain = before.accounts.get(addr).and_then(|a| a.account.as_ref());

        // Per-account warming. Both sstore and set_balance assume the
        // account is already journaled; outside the EVM's normal CALL/
        // SLOAD path we do the load explicitly. `load_account_mut`
        // also gives us a `JournaledAccount` for the balance set; we
        // bind it for the balance branch and let it drop before sstore.
        // (`load_account` is the lighter-weight version — used here
        // for the storage-only branch to avoid pulling code.)
        let balance_changed =
            before_plain.is_some_and(|b| b.info.balance != after_plain.info.balance);

        if balance_changed {
            let mut loaded = ctx.journal_mut().load_account_mut(*addr).map_err(|e| {
                OverlayError::SourceLoad {
                    address: *addr,
                    message: format!("{e:?}"),
                }
            })?;
            loaded.data.set_balance(after_plain.info.balance);
            // `loaded` borrow ends here so the `sstore` calls below
            // can re-borrow `ctx.journal_mut()`.
        } else {
            // Warm the account so the subsequent `sstore` calls don't
            // hit `JournalLoadError::ColdLoadSkipped` (which the
            // public `sstore` method panics on via
            // `unwrap_db_error`).
            ctx.journal_mut()
                .load_account(*addr)
                .map_err(|e| OverlayError::SourceLoad {
                    address: *addr,
                    message: format!("{e:?}"),
                })?;
        }

        // Iterate after's storage; emit sstore for every slot whose
        // value differs from before.
        for (key, after_val) in &after_plain.storage {
            let before_val = before_plain
                .and_then(|b| b.storage.get(key).copied())
                .unwrap_or_default();
            if before_val == *after_val {
                continue;
            }
            ctx.journal_mut()
                .sstore(*addr, *key, *after_val)
                .map_err(|e| OverlayError::SourceSstore {
                    address: *addr,
                    key: *key,
                    message: format!("{e:?}"),
                })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::database::EmptyDB;

    /// Regression: `clone_state` must produce a struct literally
    /// equivalent to the source. Any divergence (a field accidentally
    /// not cloned, a field whose Clone impl drifted) shows up as a
    /// non-equal Debug rendering of one of the seven fields.
    #[test]
    fn clone_state_field_parity_default() {
        let src: State<EmptyDB> = State::builder().with_database(EmptyDB::default()).build();
        let cloned = clone_state(&src);

        assert_eq!(format!("{:?}", src.cache), format!("{:?}", cloned.cache));
        assert_eq!(
            format!("{:?}", src.transition_state),
            format!("{:?}", cloned.transition_state),
        );
        assert_eq!(
            format!("{:?}", src.bundle_state),
            format!("{:?}", cloned.bundle_state),
        );
        assert_eq!(src.use_preloaded_bundle, cloned.use_preloaded_bundle);
        assert_eq!(
            format!("{:?}", src.block_hashes),
            format!("{:?}", cloned.block_hashes),
        );
        assert_eq!(
            format!("{:?}", src.bal_state),
            format!("{:?}", cloned.bal_state),
        );
    }
}
