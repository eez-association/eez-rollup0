//! Optional cross-chain capabilities a [`ChainProtocol`] MAY support.
//!
//! The core [`crate::ChainProtocol`] trait holds only the
//! universal operations every chain family has. Roles a chain plays for a
//! cross-chain message that NOT every chain plays — e.g. settling L2→L1 calls
//! by hosting the canonical proof — are split into capability sub-traits here.
//!
//! This keeps optional roles out of the core trait, which stays CLOSE to the
//! upstream `rollup-node` definition (no longer byte-identical — `message_id`
//! has since been added here): the capabilities live in this separate file the
//! vendor never touches, so re-vendoring the core sees minimal conflicts.
//! It also makes "does this chain settle outbound?" a compile-time fact — code
//! that needs it bounds on the capability rather than calling a core method that
//! returns an error for non-settlers.

use crate::error::ProtocolResult;
use crate::message::{Delivery, Message};
use crate::rollup_id::RollupId;
use crate::types::ExecutedAction;
use crate::ChainProtocol;

/// A chain that hosts the canonical proof and **settles L2→L1 calls targeting
/// it** by posting an EXECUTING batch: each call becomes an IMMEDIATE entry
/// (`proxyEntryHash = 0`) the chain runs on-chain. This is the PRODUCER role on
/// the outbound (L2→L1) axis.
///
/// Formerly `ChainProtocol::build_l1_postbatch` (a core method with a default
/// that returned an error for non-settlers). Lifting it here deletes that
/// footgun: a chain that cannot settle outbound simply does not implement this
/// trait, and `CompositionBuilder::finalize` bounds on it — so the settle path
/// is unreachable unless the capability is present, enforced by the compiler.
pub trait SettlesOutbound: ChainProtocol {
    /// Build the L1-side executing batch for the cross-chain `calls` targeting
    /// this (zk-poster) rollup. `calls` are `finalize`'s `group_calls_for(L1)`;
    /// `destination_rollup_id` is the settled rollup.
    fn build_settlement_batch(
        &self,
        calls: &[ExecutedAction<Self>],
        destination_rollup_id: RollupId,
    ) -> ProtocolResult<Self::Batch>;
}

/// A chain that participates in cross-chain DELIVERY of a message it received —
/// as the CONSUMER (re-executes the call and binds the message identity `H` +
/// the real outcome into its own block) and the PRODUCER-RETURN (commits the
/// result it owes the originator, deferred). The inbound counterpart of the
/// outbound builders on the core trait, lifted off the EVM free functions so a
/// non-EVM chain family supplies its own inbound shape the same way.
///
/// Both halves derive `H` only through [`ChainProtocol::message_id`], so the
/// matching proof (producer.outbound == consumer.inbound) is expressible
/// generically over `H`.
pub trait ConsumesInbound: ChainProtocol {
    /// Encode the CONSUMER delivery — the system-tx calldata the receiving
    /// chain seals to deliver `m` with outcome `d`. Binds `proxyEntryHash =
    /// message_id(m)` under the consumer's own (registry) id and folds the real
    /// outcome into the rolling hash. (EVM: `executeIncomingCrossChainCall`.)
    fn encode_delivery(&self, m: &Message<'_, Self>, d: &Delivery<Self>) -> Vec<u8>;

    /// Build the PRODUCER-RETURN batch — what the originating chain commits to
    /// return the result `d` of message `m` to the caller, branching INTERNALLY
    /// on `d.success`: success → a deferred entry (`proxyEntryHash = H`) that
    /// returns `d.return_data`; failure → a settlement-only entry + a failed
    /// lookup keyed on `H` (so the caller's consume reverts with the bytes).
    fn build_return(&self, m: &Message<'_, Self>, d: &Delivery<Self>) -> ProtocolResult<Self::Batch>;

    /// Build a pure settlement-only batch (no call) carrying the settled
    /// rollup's root — one immediate entry, no `L2ToL1Calls`. Infallible: a
    /// pure constructor from the settled rollup id.
    fn build_settlement_only(&self, settled_rollup: RollupId) -> Self::Batch;

    /// Build the CONSUMER-side INBOUND delivery batch for the incoming
    /// cross-chain `calls` whose TARGET is this rollup — the follower-only
    /// entry (shipped in the opaque `callData` DA sidecar) the deriver lowers
    /// back into the `executeIncomingCrossChainCall` system tx.
    ///
    /// The inbound mirror of [`SettlesOutbound::build_settlement_batch`]:
    /// `ChainProtocol::build_batch(source = this rollup)` cannot express an
    /// INCOMING call (its top-level rule keys on the call's SOURCE, not its
    /// TARGET), so when that batch comes back empty for a target that has an
    /// incoming call, the composer builds this shape directly instead.
    ///
    /// Unlike the LEAN on-chain entry (empty `L2ToL1Calls`, `callCount = 0`,
    /// which would revert on-chain if populated), this sidecar entry carries
    /// the call in `L2ToL1Calls[0]` (`callCount = 1`) so the follower can
    /// re-derive it; it never reaches the contract (callData is opaque, only
    /// hashed for proof binding). One entry per incoming call.
    fn build_inbound_target_batch(
        &self,
        calls: &[ExecutedAction<Self>],
        target_rollup_id: RollupId,
    ) -> ProtocolResult<Self::Batch>;
}
