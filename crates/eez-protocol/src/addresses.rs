//! Well-known addresses shared across the eez stack.

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
