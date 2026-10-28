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
    address!("fffffffffffffffffffffffffffffffffffffffe");

/// Native system envelope with empty calldata, for framing tests.
pub(crate) const SYSTEM_TX: &str = "45f83e01a0111111111111111111111111111111111111111111111111111111111111111184010000009442000000000000000000000000000000000000078080";

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

/// A checkpoint claiming `block_hash` as its candidate — the value settlement
/// gates compare against.
///
/// `state_root` is deliberately set to a *different*, index-derived value: no
/// gate compares it any more, so anything that starts to will not silently
/// agree with the candidate.
pub(crate) fn checkpoint(
    transaction_index: usize,
    block_hash: B256,
) -> crate::validate::StateCheckpoint {
    crate::validate::StateCheckpoint {
        at: crate::validate::CheckpointAt::Transaction(transaction_index),
        state_root: B256::with_last_byte(0xc0 ^ (transaction_index as u8)),
        block_hash,
    }
}

/// The candidate the anchor claims: the settling block sealed over no
/// transactions. Deliberately not the parent's hash.
pub(crate) fn empty_prefix_candidate() -> B256 {
    B256::repeat_byte(0xe0)
}

/// The anchor's candidate: the settling block sealed over no transactions.
pub(crate) fn pre_execution_checkpoint(block_hash: B256) -> crate::validate::StateCheckpoint {
    crate::validate::StateCheckpoint {
        at: crate::validate::CheckpointAt::PreExecution,
        state_root: B256::with_last_byte(0xc0),
        block_hash,
    }
}

/// Fixed live parent for fixture reconstruction. These unit fixtures mock execution.
pub(crate) fn native_source(index: u64) -> eez_protocol::native_source::SourceEntry {
    eez_protocol::native_source::LiveSourceContext::new(1, B256::repeat_byte(0x99))
        .unwrap()
        .entry(index)
}

pub(crate) fn live_context() -> eez_protocol::l1_context::LiveSettlementContext {
    eez_protocol::l1_context::LiveSettlementContext {
        parent_hash: B256::repeat_byte(0x99),
        timestamp: 1,
    }
}

pub(crate) fn block_header() -> alloy_consensus::Header {
    alloy_consensus::Header {
        gas_limit: 30_000_000,
        timestamp: 1,
        ..Default::default()
    }
}
