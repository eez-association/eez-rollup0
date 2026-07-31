//! Well-known addresses shared across the eez stack.
//!
//! These two addresses jointly define what a *system transaction* is: a call
//! recovered from [`SYSTEM_ADDRESS`] and targeting the [`CCM_ADDRESS`] EEZL2
//! predeploy. This covers both outbound table loads and inbound deliveries.
//! Payload construction and block validation share these definitions.

use alloy_primitives::{Address, address};

/// The `EEZL2` predeploy. Every system transaction targets this address.
pub const CCM_ADDRESS: Address = address!("0x4200000000000000000000000000000000000007");

/// The system-transaction signer (anvil dev key #0). Generated outbound table
/// loads and inbound deliveries are signed by this account and target
/// [`CCM_ADDRESS`].
pub const SYSTEM_ADDRESS: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
