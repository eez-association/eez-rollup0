//! Shared fixtures for the crate's test suites.
//!
//! Everything here is fixed test material, or a tiny constructor for it,
//! consumed from multiple test modules. Test support has three homes:
//! byte-identical fixtures live here, the stub validation backend lives in
//! `validate::testing`, and behavior-specific builders stay beside the test
//! suite that owns them.

use alloy_primitives::{Address, B256, address};

/// Deterministic system-transaction identity used only by tests.
pub(crate) const TEST_SYSTEM_ADDRESS: Address =
    address!("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076");

/// Native system envelope with empty calldata, for framing tests.
pub(crate) const SYSTEM_TX: &str = "76d901809442000000000000000000000000000000000000078080";

/// Canonical context for reconstructing system transactions in tests; tests
/// that need a noncanonical variant mutate one field of a fresh copy.
pub(crate) fn system_transaction_context() -> eez_protocol::system_tx::SystemTxContext {
    eez_protocol::system_tx::SystemTxContext {
        eezl2_address: crate::EEZL2_ADDRESS,
        l2_chain_id: 1,
        this_rollup_id: 1,
    }
}

pub(crate) fn test_proof_system_vkey() -> crate::attest::NonZeroProofSystemVkey {
    crate::attest::NonZeroProofSystemVkey::new(B256::repeat_byte(0x42)).unwrap()
}

pub(crate) fn checkpoint(
    transaction_index: usize,
    state_root: B256,
) -> crate::validate::TransactionStateCheckpoint {
    crate::validate::TransactionStateCheckpoint {
        transaction_index,
        state_root,
    }
}

pub(crate) const LEGACY_SIGNER_ADDRESS: Address =
    address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
