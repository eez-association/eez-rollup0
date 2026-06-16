//! EVM implementation of the cross-chain protocol.
//!
//! Sits between [`eez_protocol`] (abstract traits) and
//! `eez-evm-inspector` (orchestration). Provides the EVM-specific
//! materialization of every associated type and method on the
//! [`ChainProtocol`] trait:
//!
//! | `ChainProtocol` assoc type | EVM concrete type |
//! |---|---|
//! | `Address`  | [`alloy_primitives::Address`] (20 bytes) |
//! | `Value`    | [`alloy_primitives::U256`]     (32 bytes) |
//! | `Calldata` | [`alloy_primitives::Bytes`] |
//! | `Batch`    | [`EvmBatch`] — table-loading batch (entries + static-call lookup + transient counts) |
//! | `Overlay`  | [`EvmOverlay`] — in-process state accumulator |
//! | `Witness`  | [`EvmWitness`] — placeholder, zisk proving inputs in Phase 2 |
//! | `Dialect`  | [`ChainDialect`] — `EvmL1Style` \| `EvmL2Style` |
//!
//! # Module map
//!
//! - [`action`]             — 6-field cross-chain call hash derivation, per-rollup state-root slot
//! - [`batch`]              — [`EvmBatch`] (wraps the on-chain `ProofSystemBatchPerVerificationEntriesSol`)
//! - [`entries`]            — unified [`entries::build_batch`] emitter + ABI payload encoders
//! - [`dialect`]            — [`ChainDialect`] enum + per-dialect ABI helpers
//! - [`authorized_proxies`] — slot constants + storage-mapping helpers
//! - [`types`]              — `sol!`-generated ABI structs ([`ExecutionEntrySol`], [`L2ToL1CallSol`], [`LookupCallSol`], …) + events + `postAndVerifyBatch`/`loadExecutionTable` calls (the single ABI source)
//! - [`system_tx`]          — builds the signed inbound L2 system txs (shared composer↔deriver STF source)
//! - [`signer`]             — narrow ECDSA signer for the postBatch poster's proof system
//! - [`proof_plan`]         — EVM-side [`eez_protocol::ProofPlanResolver`] impl
//! - [`public_inputs`]      — two-stage `publicInputsHash` construction (mirrors on-chain)
//! - [`overlay`]            — [`EvmOverlay`] (`BundleState` equivalent)
//! - [`witness`]            — [`EvmWitness`] (Phase 2)
//!
//! # Where to start reading
//!
//! - The crate entry point is [`EvmProtocol`], a **unit struct** — all
//!   per-transaction state lives on the composition builder in
//!   `eez-protocol`.
//! - For the ABI boundary, [`entries::build_batch`] walks the preorder
//!   `recorded[..]` slice and materializes an [`EvmBatch`]; the
//!   per-dialect encoders (`encode_postbatch` / `encode_load_table`)
//!   produce the calldata wrappers.
//!
//! The core ABI materialization is client-agnostic (works against any
//! `ChainClient` / `EntryChainClient` — local reth, gRPC, or a test
//! fake). [`system_tx`] and [`signer`] additionally depend on
//! reth/alloy consensus tx types to build + sign the inbound L2 system
//! transactions.

pub mod action;
pub mod authorized_proxies;
pub mod batch;
pub mod dialect;
pub mod entries;
pub mod overlay;
pub mod proof_plan;
pub mod public_inputs;
pub mod signer;
pub mod system_tx;
pub mod types;
pub mod witness;

use alloy_primitives::{Address, Bytes, U256};
use eez_protocol::{ChainProtocol, ProtocolErrorKind, ProtocolResult, RecordedCall, RollupId};

#[doc(inline)]
pub use authorized_proxies::{
    CCM_AUTHORIZED_PROXIES_SLOT, ProxyInfo, ROLLUPS_AUTHORIZED_PROXIES_SLOT, decode_proxy_value,
    proxy_mapping_key,
};
#[doc(inline)]
pub use batch::EvmBatch;
#[doc(inline)]
pub use dialect::ChainDialect;
#[doc(inline)]
pub use overlay::EvmOverlay;
#[doc(inline)]
pub use types::{
    ActionSol, ExecutionEntrySol, ExpectedL1ToL2CallSol, L2ToL1CallSol, LookupCallSol,
    ProofSystemBatchPerVerificationEntriesSol, RollupIdWithProofSystemsSol, StateDeltaSol,
};
#[doc(inline)]
pub use witness::EvmWitness;

// ── Type aliases ────────────────────────────────────────────────

pub type EvmRecordedCall = RecordedCall<EvmProtocol>;
pub type EvmCheckpoint = eez_protocol::ExecutionCheckpoint<EvmOverlay, EvmWitness>;

// ── EvmProtocol ─────────────────────────────────────────────────

/// EVM cross-chain protocol — the first (and currently only) implementation.
///
/// Unit struct with no state. Protocol operations take the rollup ids
/// they need as explicit arguments — stateless by design.
#[derive(Debug, Clone, Copy, Default)]
pub struct EvmProtocol;

impl ChainProtocol for EvmProtocol {
    type Address = Address;
    type Value = U256;
    type Calldata = Bytes;
    type Batch = batch::EvmBatch;
    type Overlay = EvmOverlay;
    type Witness = EvmWitness;
    type Dialect = ChainDialect;

    fn build_batch(
        &self,
        recorded: &[RecordedCall<Self>],
        attribution: &eez_protocol::SourceAttribution<'_>,
        dialect: &Self::Dialect,
        source_rollup_id: RollupId,
        raw_tx: &[u8],
    ) -> ProtocolResult<Self::Batch> {
        entries::build_batch(recorded, attribution, dialect, source_rollup_id, raw_tx)
    }

    fn encode_postbatch(&self, batch: &Self::Batch) -> Vec<u8> {
        entries::encode_postbatch(batch)
    }

    fn encode_load_table(&self, batch: &Self::Batch) -> Vec<u8> {
        entries::encode_load_table(batch)
    }

    fn dialect_is_zk_poster(&self, dialect: &Self::Dialect) -> bool {
        matches!(dialect, ChainDialect::EvmL1Style)
    }

    fn batch_is_empty(&self, batch: &Self::Batch) -> bool {
        batch.is_empty()
    }

    fn encode_follower_trigger(
        &self,
        call: &RecordedCall<Self>,
        source_rollup_id: RollupId,
        raw_tx: &[u8],
        dialect: &Self::Dialect,
    ) -> Vec<u8> {
        dialect.encode_follower_trigger(call, source_rollup_id, raw_tx)
    }

    fn encode_inbound_delivery(
        &self,
        outer: &RecordedCall<Self>,
        batch: &Self::Batch,
        dialect: &Self::Dialect,
    ) -> Option<Vec<u8>> {
        // Only L2-style followers route inbound through
        // `executeIncomingCrossChainCall`; the L1-style entry rollup
        // is never the follower here, so it returns `None`.
        match dialect {
            ChainDialect::EvmL2Style => Some(dialect::encode_execute_incoming_cross_chain_call(
                outer.original_address,
                outer.value,
                &outer.calldata,
                outer.caller,
                outer.caller_rollup_id.0,
                batch.inner.entries.clone(),
                batch.inner.l1ToL2lookupCalls.clone(),
            )),
            ChainDialect::EvmL1Style => None,
        }
    }

    fn encode_address(&self, addr: &Address) -> Vec<u8> {
        addr.to_vec()
    }

    fn decode_address(&self, bytes: &[u8]) -> ProtocolResult<Address> {
        expect_len(bytes, 20, "address")?;
        Ok(Address::from_slice(bytes))
    }

    fn encode_value(&self, val: &U256) -> Vec<u8> {
        val.to_be_bytes_vec()
    }

    fn decode_value(&self, bytes: &[u8]) -> ProtocolResult<U256> {
        expect_len(bytes, 32, "U256")?;
        Ok(U256::from_be_slice(bytes))
    }

    fn encode_calldata(&self, data: &Bytes) -> Vec<u8> {
        data.to_vec()
    }

    fn decode_calldata(&self, bytes: &[u8]) -> ProtocolResult<Bytes> {
        Ok(Bytes::from(bytes.to_vec()))
    }
}

/// Internal helper: check a byte slice has exactly `expected` bytes.
fn expect_len(bytes: &[u8], expected: usize, label: &str) -> ProtocolResult<()> {
    if bytes.len() == expected {
        Ok(())
    } else {
        Err(ProtocolErrorKind::InvalidEncoding(format!(
            "{label} must be {expected} bytes, got {}",
            bytes.len()
        ))
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_sol_types::SolCall;
    use types::{loadExecutionTableCall, postAndVerifyBatchCall};

    #[test]
    fn table_payload_l1_style_emits_post_verify_and_execute_or_save() {
        let proto = EvmProtocol;
        let batch = batch::EvmBatch::empty();
        let data = proto.encode_table_payload(&batch, &ChainDialect::EvmL1Style);
        assert_eq!(&data[..4], &postAndVerifyBatchCall::SELECTOR);
    }

    #[test]
    #[allow(
        non_snake_case,
        reason = "fn name mirrors the Solidity selector for grep"
    )]
    fn table_payload_l2_style_emits_loadExecutionTable() {
        let proto = EvmProtocol;
        let batch = batch::EvmBatch::empty();
        let data = proto.encode_table_payload(&batch, &ChainDialect::EvmL2Style);
        assert_eq!(&data[..4], &loadExecutionTableCall::SELECTOR);
    }

    #[test]
    fn table_payload_dialects_differ() {
        let proto = EvmProtocol;
        let batch = batch::EvmBatch::empty();
        let l1 = proto.encode_table_payload(&batch, &ChainDialect::EvmL1Style);
        let l2 = proto.encode_table_payload(&batch, &ChainDialect::EvmL2Style);
        assert_ne!(&l1[..4], &l2[..4]);
    }
}
