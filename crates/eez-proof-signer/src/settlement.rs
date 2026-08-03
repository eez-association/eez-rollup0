//! Settlement-input decoding, claim binding, and public-input recomputation.
//!
//! Values cross explicit trust boundaries in this module:
//!
//! 1. Composer bytes become [`CanonicalPostBatch`] after complete canonical decoding.
//! 2. Claimed state and effects are bound to locally validated execution.
//! 3. Inbound and outbound effects are authorized against those observations.
//! 4. DA is checked against the same validated window and the public-input hash
//!    is recomputed locally.

mod blocks;
mod da;
mod effect_binding;
mod inbound;
mod outbound;
mod post_batch;
mod state_chain;
mod system_transactions;

use blocks::EthereumBlock;
#[cfg(test)]
use blocks::RESERVED_SYSTEM_TRANSACTION_TYPE;
#[cfg(test)]
use da::encoded_bytes_match;
#[cfg(test)]
use inbound::inspect_inbound_candidate;

pub(crate) use blocks::{
    BlockInspectionError, inspect_validated_settling_block, verify_validated_intermediate_blocks,
};
#[cfg(test)]
pub(crate) use blocks::{
    SettlingBlockObservations, inspect_settling_block, verify_no_intermediate_system_transactions,
};
pub(crate) use da::{DaPayloadError, verify_da_payload};
#[cfg(test)]
pub(crate) use da::{encode_da_payload, verify_da_payload_for_test};
#[cfg(test)]
pub(crate) use effect_binding::{BoundEffectSequence, ClaimedEntryShape, ObservedEffectKind};
pub(crate) use effect_binding::{EffectPrefixError, bind_effects_to_execution};
pub(crate) use inbound::{AuthorizedInboundEffects, InboundEffectError, authorize_inbound_effects};
#[cfg(test)]
pub(crate) use inbound::{InboundCandidate, InboundObservationError};
pub(crate) use outbound::{
    AuthorizedOutboundEffects, OutboundEffectError, authorize_outbound_effects,
};
#[cfg(test)]
pub(crate) use post_batch::recompute_public_input_hash;
pub(crate) use post_batch::{
    CanonicalPostBatch, CheckedPublicInputProfile, PostBatchDecodeError, PublicInputError,
    RecomputedPublicInputsHash, decode_canonical_post_batch,
};
pub(crate) use state_chain::{StateDeltaChainError, verify_state_delta_chain};
pub(crate) use system_transactions::{SystemTransactionKey, SystemTransactionReconstructor};

#[cfg(test)]
mod tests;
