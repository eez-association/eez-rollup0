//! Well-known addresses shared across the eez stack.
//!
//! These two addresses jointly define what a *system transaction* is: a
//! call from [`SYSTEM_ADDRESS`] to [`CCM_ADDRESS`] (the
//! `loadExecutionTable` that opens a Sync block). Both the L2 payload
//! builder (which signs the system tx) and the embedded driver (which
//! tells user txs from system txs in a sealed block) need them, so they
//! live here — one definition, no "keep these in sync" comments.

use alloy_primitives::{Address, address};

/// The cross-chain messenger (`EEZL2`), pre-allocated at `0xeeee…eeee`
/// in the `eez-dev` genesis. Every system tx targets it (its
/// `loadExecutionTable` entrypoint stages the slot's cross-chain calls).
pub const CCM_ADDRESS: Address = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

/// The system-tx signer (anvil dev key #0). The payload builder signs the
/// per-slot `loadExecutionTable` tx with this account, so a tx is
/// "system" iff its signer is this address and its target is
/// [`CCM_ADDRESS`].
pub const SYSTEM_ADDRESS: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
