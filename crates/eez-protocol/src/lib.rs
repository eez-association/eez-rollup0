//! The eez cross-chain protocol: composition engine, composer,
//! Solidity ABI types, cross-chain call hashing, sequencing machinery, ZK
//! substrate, checkpoint format.
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
//!   assumption, a wrong hash verifies). The initial integration supports one
//!   uniform verification key, block number zero, no blobs, and no sender
//!   binding; unsupported profiles are rejected explicitly.
//! - **Settlement root.** Before signing, the prover checks the settlement
//!   `StateUpdate.newState` against the root reth actually produced for the block.
//!
//! # Where to start reading
//!
//! - [`CompositionBuilder`] runs one cross-chain composition
//!   end-to-end: source simulation dispatches into it, `finalize`
//!   emits the [`Composition`].
//! - For the ABI boundary, [`entries::build_batch`] walks the preorder
//!   `recorded[..]` slice and materializes an [`EvmBatch`].
//!   [`entries::encode_postbatch`] wraps an L1 batch for submission, while
//!   [`system_tx`] constructs canonical L2 system transactions from lean L2
//!   entries.
#![deny(missing_docs)]

pub mod abi;
pub mod action;
pub mod addresses;
pub mod authorized_proxies;
pub mod batch;
pub mod composer;
pub mod composition;
pub mod dialect;
pub mod entries;
pub mod error;
pub mod executor;
pub mod outbound_gate;
pub mod overlay;
pub mod proof_plan;
pub mod public_inputs;
pub mod rolling_hash;
pub mod rollup_id;
pub mod settlement;
pub mod signer;
pub mod system_tx;
pub mod types;

mod assertions;

#[doc(inline)]
pub use action::{
    CallHashInput, CallMode, common_cross_chain_call_hash, compute_state_root_slot,
    l2_outbound_call_hash,
};
#[doc(inline)]
pub use addresses::EEZL2_ADDRESS;
#[doc(inline)]
pub use authorized_proxies::{
    EEZ_AUTHORIZED_PROXIES_SLOT, EEZL2_AUTHORIZED_PROXIES_SLOT, ProxyInfo, decode_proxy_value,
    proxy_mapping_key,
};
#[doc(inline)]
pub use batch::EvmBatch;
#[doc(inline)]
pub use composer::{ProxyLookupConfig, SourceAttribution, TargetConfig};
#[doc(inline)]
pub use composition::{CompositionBuilder, Rollup};
#[doc(inline)]
pub use dialect::ChainDialect;
#[doc(inline)]
pub use error::{
    ComposerError, ComposerErrorKind, ComposerResult, CompositionError, CompositionErrorKind,
    CompositionResult, ExecutorError, ExecutorErrorKind, ExecutorResult, ProtocolError,
    ProtocolErrorKind, ProtocolResult,
};
#[doc(inline)]
pub use executor::{ChainClient, ExecutionRequest, SessionSnapshot, TargetExecutionSession};
#[doc(inline)]
pub use overlay::{
    AccountInfo, AccountOverlay, AccountStatus, ContractCode, EvmOverlay, StorageOverlay,
};
#[doc(inline)]
pub use proof_plan::{ProofPlan, ProofPlanInvariantError, RollupProofAssignment};
#[doc(inline)]
pub use public_inputs::{
    PublicInputsError, all_per_ps_hashes, entry_hash, public_inputs_hashes, shared_public_input,
    static_entry_hash,
};
#[doc(inline)]
pub use rolling_hash::{EntryRollingHash, StaticCallRollingHash};
#[doc(inline)]
pub use rollup_id::RollupId;
#[doc(inline)]
pub use signer::{EcdsaProofSigner, SignerError};
#[doc(inline)]
pub use types::{
    Composition, ExecutedAction, ExecutionOutcome, SourceComposition, TargetComposition,
};
