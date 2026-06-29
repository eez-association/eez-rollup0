//! The cross-chain message — the role-agnostic unit of a cross-chain
//! interaction, and its single identity.
//!
//! A cross-chain call is one **message** with one identity `H`, and every chain
//! plays exactly one **role** per message: it is the [`MessageRole::Producer`]
//! (it originated the call and commits what it sent, bound to its own post-state
//! root — COMMIT OUTBOUND) or the [`MessageRole::Consumer`] (it received the
//! call and re-executes it, binding `H` + the real outcome into its own block —
//! PROVE INBOUND). "Inbound/outbound" is an L1-centric framing that dissolves
//! into these roles: L1↔L2 and L2↔L2 differ only in which [`RollupId`] carries
//! which role, never in code path.

use crate::ChainProtocol;
use crate::rollup_id::RollupId;
use crate::types::ExecutedAction;

/// The role a chain plays for ONE cross-chain message. Replaces the
/// "inbound/outbound" axis: it is the PROVE-INBOUND / COMMIT-OUTBOUND split,
/// one-to-one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageRole {
    /// Originated the call. Commits what it SENT (deferred/queued or executed),
    /// bound to its own post-state root. (COMMIT OUTBOUND.)
    Producer,
    /// Received the call. Re-executes it, binding the message identity `H` and
    /// the real outcome into its own block. (PROVE INBOUND.)
    Consumer,
}

/// A single resolved cross-chain message — the role-agnostic 6-field view that
/// the identity hash [`ChainProtocol::message_id`] is computed over.
///
/// It is a borrowing *view* over an [`ExecutedAction`] (see [`Message::of`]):
/// every field is already a field of `ExecutedAction`, so the message vocabulary
/// introduces no new state — the recorded action slice stays the source of truth.
///
/// **The `RollupId`s are REGISTRY ids**, not chain ids. The cross-chain identity
/// `H` must be the id BOTH chains agree on, which is the id the L1 registry
/// assigned (`registerRollup` → 1, 2, …) — NOT a chain's native chain id (e.g.
/// 33333). A chain's chain id is a node/admission routing key; the composer
/// resolves it to the registry id before constructing a `Message` for hashing.
/// Hashing under the wrong id namespace silently breaks the cross-layer match.
pub struct Message<'a, P: ChainProtocol + ?Sized> {
    /// Registry id of the chain that originated the call.
    pub from_rollup: RollupId,
    /// The caller on the source chain.
    pub from_addr: &'a P::Address,
    /// Registry id of the chain the call targets.
    pub to_rollup: RollupId,
    /// The target on the destination chain.
    pub to_addr: &'a P::Address,
    /// Value carried by the call.
    pub value: &'a P::Value,
    /// The call's calldata.
    pub data: &'a P::Calldata,
}

impl<P: ChainProtocol + ?Sized> std::fmt::Debug for Message<'_, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("from_rollup", &self.from_rollup)
            .field("from_addr", &self.from_addr)
            .field("to_rollup", &self.to_rollup)
            .field("to_addr", &self.to_addr)
            .field("value", &self.value)
            .field("data", &self.data)
            .finish()
    }
}

/// The resolved OUTCOME of a delivered cross-chain message — the I/O projection
/// the consumer proved and the producer-return commits. The generic form of the
/// `(success, return_data)` an EVM `DecodedInbound` recovers.
///
/// `success` is ground truth recovered by re-execution / rolling-hash matching,
/// never a producer claim.
#[derive(Clone, Debug)]
pub struct Delivery<P: ChainProtocol + ?Sized> {
    /// Whether the delivered call succeeded.
    pub success: bool,
    /// The call's result bytes (folded into the consumer's rolling hash and
    /// returned to the originator).
    pub return_data: P::Calldata,
}

impl<'a, P: ChainProtocol + ?Sized> Message<'a, P> {
    /// View an [`ExecutedAction`] as the cross-chain message it carries.
    ///
    /// Note the rollup ids come straight from the action's `source_rollup_id` /
    /// `target_rollup_id`; the caller is responsible for those being REGISTRY
    /// ids (see the type-level note) before relying on [`ChainProtocol::message_id`].
    #[must_use]
    pub fn of(a: &'a ExecutedAction<P>) -> Self {
        Self {
            from_rollup: a.source_rollup_id,
            from_addr: &a.source_address,
            to_rollup: a.target_rollup_id,
            to_addr: &a.target_address,
            value: &a.value,
            data: &a.data,
        }
    }
}
