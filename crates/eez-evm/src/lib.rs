//! EVM implementation of `eez_protocol::ChainProtocol` —
//! Solidity ABI types, flat action hashing, sequencing machinery, ZK
//! substrate.
//!
//! Vendored and adapted from the sibling `rollup-node` project's
//! `eez-evm` crate. Sits between [`eez_protocol`]
//! (abstract traits) and the runtime composer (Step 7).
//!
//! # Soundness model
//!
//! These types assemble and commit cross-chain state, so an integrator building
//! the prover side MUST respect what is **trusted** vs **independently proven**:
//!
//! - **PROVE INBOUND.** An L1→L2 delivery's outcome (`success` / `returnData`)
//!   is NOT a composer claim — the prover re-derives it from the sealed block's
//!   own `executeIncomingCrossChainCall` system tx, whose call args are bound
//!   into the entry's `proxyEntryHash` and whose result is bound into the
//!   `rollingHash` (see [`entries::decode_inbound`] / [`entries::DecodedInbound`]).
//! - **COMMIT OUTBOUND.** An L2→L1 call is committed bound to the L2 post-state
//!   root via the consume's `proxyEntryHash` + `rollingHash` gates; L1 verifies
//!   the proof signature, it does not re-execute.
//! - **One hash, both sides.** [`public_inputs::public_inputs_hashes`] is THE
//!   place the `publicInputsHash` is reconstructed from a batch. The composer
//!   AND an independently-built prover MUST call this same helper, byte-for-byte,
//!   or their hashes diverge (the proof fails — or, if both share a wrong
//!   assumption, a wrong hash verifies). It bakes in two assumptions today: a
//!   single uniform verification key across all rollups, and `(timestamp,
//!   blockHash) = (0, 0)` per rollup — a rollup that overrides
//!   `getTimestampAndBlockHash` breaks the hash *silently*.
//! - **Settlement root.** Before signing, the prover checks the settlement
//!   `StateDelta.newState` against the root reth actually produced for the block.
//!
//! The submit pipeline an integrator wires: resolve a `ProofPlan`
//! ([`proof_plan::EvmProofPlanResolver`]) → [`entries::build_batch`] →
//! [`public_inputs::public_inputs_hashes`] → sign each digest with
//! [`EcdsaProofSigner`] → fill `batch.inner.proofs[]` → [`entries::encode_postbatch`].
//!
//! | `ChainProtocol` assoc type | EVM concrete type |
//! |---|---|
//! | `Address`  | [`alloy_primitives::Address`] (20 bytes) |
//! | `Value`    | [`alloy_primitives::U256`]     (32 bytes) |
//! | `Calldata` | [`alloy_primitives::Bytes`] |
//! | `Batch`    | [`EvmBatch`] — table-loading batch (entries + static-call lookup + transient counts) |
//! | `Overlay`  | [`EvmOverlay`] — in-process state accumulator |
//! | `Witness`  | [`EvmWitness`] — inert placeholder; the real proving witness flows via reth-node's `eez_executionWitness` through the composer control feed, not through this type |
//! | `Dialect`  | [`ChainDialect`] — `EvmL1Style` \| `EvmL2Style` |
//!
//! # Module map
//!
//! - [`action`]             — 6-field cross-chain call hash derivation, per-rollup state-root slot
//! - [`batch`]              — [`EvmBatch`] (wraps the on-chain `ProofSystemBatchPerVerificationEntriesSol`)
//! - [`entries`]            — unified [`entries::build_batch`] emitter + ABI payload encoders
//! - [`dialect`]            — [`ChainDialect`] enum + per-dialect ABI helpers
//! - [`authorized_proxies`] — slot constants + storage-mapping helpers
//! - [`types`]              — `sol!`-generated ABI structs
//! - [`overlay`]            — [`EvmOverlay`] (`BundleState` equivalent)
//! - [`witness`]            — [`EvmWitness`] (placeholder)
//!
//! # Where to start reading
//!
//! - The crate entry point is [`EvmProtocol`], a **unit struct** — all
//!   per-transaction state lives on the composition builder in
//!   `eez_protocol`.
//! - For the ABI boundary, [`entries::build_batch`] walks the preorder
//!   `recorded[..]` slice and materializes an [`EvmBatch`]; the
//!   per-dialect encoders (`encode_postbatch` / `encode_load_table`)
//!   produce the calldata wrappers.
//!
//! Zero reth deps. Works against any `ChainClient` / `EntryChainClient`
//! implementation (local reth, gRPC, or a test fake — all landing in
//! Step 7).
#![deny(missing_docs)]

pub mod action;
pub mod addresses;
pub mod authorized_proxies;
pub mod batch;
pub mod dialect;
pub mod entries;
pub mod outbound_gate;
pub mod overlay;
pub mod proof_plan;
pub mod public_inputs;
pub mod settlement;
pub mod signer;
pub mod system_tx;
pub mod types;
pub mod witness;

use alloy_primitives::{Address, Bytes, U256};
use eez_protocol::{
    ChainProtocol, ConsumesInbound, Delivery, ExecutedAction, Message, ProtocolErrorKind,
    ProtocolResult, SettlesOutbound,
};
/// Re-export `RollupId` so consumers of `entries::OutboundEntry` and the
/// system-tx builders (e.g. the deriver) can name it without taking a direct
/// dependency on `eez-protocol`.
pub use eez_protocol::RollupId;

#[doc(inline)]
pub use action::{compute_state_root_slot, cross_chain_call_hash};
#[doc(inline)]
pub use addresses::{CCM_ADDRESS, SYSTEM_ADDRESS};
#[doc(inline)]
pub use authorized_proxies::{
    decode_proxy_value, proxy_mapping_key, ProxyInfo, CCM_AUTHORIZED_PROXIES_SLOT,
    ROLLUPS_AUTHORIZED_PROXIES_SLOT,
};
#[doc(inline)]
pub use batch::EvmBatch;
#[doc(inline)]
pub use dialect::ChainDialect;
#[doc(inline)]
pub use overlay::{
    AccountInfo, AccountOverlay, AccountStatus, ContractCode, EvmOverlay, StorageOverlay,
};
#[doc(inline)]
pub use signer::{EcdsaProofSigner, SignerError};
// The submit/prover surface (see the crate-level "Soundness model"): the hash
// helpers + the proof-plan resolver, re-exported so they're discoverable at the
// root rather than only by module path.
#[doc(inline)]
pub use proof_plan::{AlloyRollupReader, EvmProofPlanResolver, ResolverConfigError};
#[doc(inline)]
pub use public_inputs::{
    all_per_ps_hashes, entry_hash, public_inputs_hashes, shared_public_input,
};
#[doc(inline)]
pub use types::{
    ActionSol, ExecutionEntrySol, ExpectedL1ToL2CallSol, L2ToL1CallSol, LookupCallSol,
    ProofSystemBatchPerVerificationEntriesSol, RollupIdWithProofSystemsSol, StateDeltaSol,
};
#[doc(inline)]
pub use witness::EvmWitness;

// ── Type aliases ────────────────────────────────────────────────

/// Type alias for [`eez_protocol::ExecutedAction`] monomorphized
/// over [`EvmProtocol`]. Convenience for downstream consumers.
pub type EvmExecutedAction = ExecutedAction<EvmProtocol>;

/// Type alias for [`eez_protocol::ExecutionCheckpoint`]
/// monomorphized with the EVM overlay + witness types.
///
/// Note: `eez_protocol::ExecutionCheckpoint` derives `Eq`,
/// which requires `O: Eq, W: Eq` at use sites. `EvmOverlay` and
/// `EvmWitness` are `PartialEq` but NOT `Eq`. `EvmCheckpoint` is
/// therefore `PartialEq`-only; downstream `Eq`-requiring containers
/// (e.g. `HashSet<EvmCheckpoint>`) must wrap.
pub type EvmCheckpoint =
    eez_protocol::ExecutionCheckpoint<EvmOverlay, EvmWitness>;

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
        recorded: &[ExecutedAction<Self>],
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
        call: &ExecutedAction<Self>,
        source_rollup_id: RollupId,
        raw_tx: &[u8],
        dialect: &Self::Dialect,
    ) -> Vec<u8> {
        dialect.encode_follower_trigger(call, source_rollup_id, raw_tx)
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

    fn message_id(
        &self,
        m: &eez_protocol::message::Message<'_, Self>,
    ) -> [u8; 32] {
        action::cross_chain_call_hash(
            m.to_rollup,
            *m.to_addr,
            *m.value,
            m.data,
            *m.from_addr,
            m.from_rollup,
        )
        .0
    }
}

impl SettlesOutbound for EvmProtocol {
    fn build_settlement_batch(
        &self,
        calls: &[ExecutedAction<Self>],
        destination_rollup_id: RollupId,
    ) -> ProtocolResult<Self::Batch> {
        Ok(entries::build_l1_postbatch(calls, destination_rollup_id))
    }
}

impl ConsumesInbound for EvmProtocol {
    fn encode_delivery(&self, m: &Message<'_, Self>, d: &Delivery<Self>) -> Vec<u8> {
        // The consumer L2 mirror entry, then the system-tx calldata — ExecutionEntrySol
        // is constructed and consumed entirely here, never crossing the trait.
        let entry = entries::build_l2_incoming_entry(entries::IncomingEntry {
            target: *m.to_addr,
            source: *m.from_addr,
            value: *m.value,
            data: m.data.clone(),
            source_rollup_id: m.from_rollup,
            l2_rollup_id: m.to_rollup,
            return_data: d.return_data.clone(),
            success: d.success,
        });
        entries::encode_execute_incoming(
            *m.to_addr,
            *m.value,
            m.data.clone(),
            *m.from_addr,
            m.from_rollup,
            entry,
        )
    }

    // NOTE (audit #1, 2026-06-11): the live inbound SUCCESS path no longer
    // calls this — `orchestrate_inbound_l1` takes the batch from
    // `Composition.source` (finalize's `build_batch`), the single
    // construction site; the success branch below survives as the
    // byte-lock oracle (`finalize_source_entry_matches_deferred_view`).
    // The FAILURE branch stays live: settlement-only entry + failed
    // LookupCall is a shape `build_batch` does not emit.
    fn build_return(&self, m: &Message<'_, Self>, d: &Delivery<Self>) -> ProtocolResult<Self::Batch> {
        let batch = if d.success {
            entries::build_l1_inbound_entry(
                *m.to_addr,
                *m.value,
                m.data.clone(),
                *m.from_addr,
                m.to_rollup,
                d.return_data.clone(),
            )
        } else {
            entries::build_l1_inbound_failed(
                *m.to_addr,
                *m.value,
                m.data.clone(),
                *m.from_addr,
                m.to_rollup,
                d.return_data.clone(),
            )
        };
        Ok(batch)
    }

    fn build_settlement_only(&self, settled_rollup: RollupId) -> Self::Batch {
        entries::build_l1_settlement_only(settled_rollup)
    }

    fn build_inbound_target_batch(
        &self,
        calls: &[ExecutedAction<Self>],
        target_rollup_id: RollupId,
    ) -> ProtocolResult<Self::Batch> {
        Ok(entries::build_l1_inbound_sidecar(calls, target_rollup_id))
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
        assert_eq!(
            &data[..4],
            &postAndVerifyBatchCall::SELECTOR
        );
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
    fn message_id_is_the_six_field_hash() {
        use alloy_primitives::{Address, Bytes, U256};
        use eez_protocol::{message::Message, RollupId};
        let to_addr = Address::from([0x11; 20]);
        let from_addr = Address::from([0x22; 20]);
        let value = U256::from(7u64);
        let data = Bytes::from(vec![0xab, 0xcd]);
        let m = Message::<EvmProtocol> {
            from_rollup: RollupId(5),
            from_addr: &from_addr,
            to_rollup: RollupId(9),
            to_addr: &to_addr,
            value: &value,
            data: &data,
        };
        // message_id is the SINGLE source of H: byte-identical to the on-chain
        // 6-field cross-chain-call hash.
        assert_eq!(
            EvmProtocol.message_id(&m),
            action::cross_chain_call_hash(RollupId(9), to_addr, value, &data, from_addr, RollupId(5)).0,
        );
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
