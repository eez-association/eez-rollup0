//! Shared fixtures for the crate's test suites.
//!
//! Everything here is fixed test material, or a tiny constructor for it,
//! consumed from multiple test modules. Test support has three homes:
//! byte-identical fixtures live here, the stub validation backend lives in
//! `validate::testing`, and behavior-specific builders stay beside the test
//! suite that owns them.

use alloy_primitives::{Address, B256, address, b256};

/// Deterministic system-transaction identity used only by tests.
pub(crate) const TEST_SYSTEM_ADDRESS_ARG: &str = "f39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

/// Parsed form of [`TEST_SYSTEM_ADDRESS_ARG`] for transaction and policy tests.
pub(crate) const TEST_SYSTEM_ADDRESS: Address =
    address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

/// Private key for [`TEST_SYSTEM_ADDRESS`]. Test-only and intentionally public.
pub(crate) const SYSTEM_PRIVATE_KEY: B256 =
    b256!("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80");

/// RLP-encoded legacy transaction with empty calldata, signed with
/// [`SYSTEM_PRIVATE_KEY`] and addressed to the configured EEZL2 address. Tests
/// classify it by recovering the sender from these bytes; no signer is
/// injected.
pub(crate) const SYSTEM_TX: &str = "f85f8001825208944200000000000000000000000000000000000007808026a0ed95c78ea14cbb6af669c61f27c5fb7fb0192101d4d706d055ab9ff9895c9f66a027c2e67303de8fa1cad36d0e59298a98df684e54295eb5f61ab99609c1738f73";

/// Canonical context for reconstructing system transactions in tests; tests
/// that need a noncanonical variant mutate one field of a fresh copy.
pub(crate) fn system_transaction_context() -> eez_protocol::system_tx::SystemTxContext {
    eez_protocol::system_tx::SystemTxContext {
        system_signer: SYSTEM_PRIVATE_KEY.to_string().parse().unwrap(),
        eezl2_address: crate::EEZL2_ADDRESS,
        l2_chain_id: 1,
        l2_gas_price: 1_000_000_000,
        l2_gas_limit: 2_000_000,
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
