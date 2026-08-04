//! Well-known addresses shared across the eez stack.

use alloy_primitives::{Address, address};

/// The `EEZL2` predeploy. Every system transaction targets this address.
pub const EEZL2_ADDRESS: Address = address!("0x4200000000000000000000000000000000000007");
