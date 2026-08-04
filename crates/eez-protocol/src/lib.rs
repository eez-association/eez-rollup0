//! Cross-chain composition, ABI types, call hashing, proof inputs, settlement
//! helpers, and canonical system-transaction construction.
//!
//! # Soundness model
//!
//! These types construct data used to commit cross-chain state, so proof-signing
//! integrations must preserve the boundary between trusted and independently
//! verified inputs:
//!
//! - **PROVE INBOUND.** An L1→L2 delivery's outcome (`success` / `returnData`)
//!   is not a composer claim: the proof signer re-derives it from the sealed block's
//!   own `executeIncomingCrossChainCall` system tx, whose call args are bound
//!   into the entry's `proxyEntryHash` and whose result is bound into the
//!   `rollingHash` (see [`entries::decode_inbound`] / [`entries::DecodedInbound`]).
//! - **COMMIT OUTBOUND.** `proxyEntryHash` and `rollingHash` bind the outbound
//!   call chain. The settlement `StateUpdate` carries the L2 post-state root,
//!   which the proof signer verifies rather than re-executing the call on L1.
//! - **Canonical public inputs.** [`public_inputs::public_inputs_hashes`] is the
//!   reconstruction used by the proof signer; another encoding changes the
//!   signed inputs. The supported profile requires one uniform verification
//!   key, block number zero, no blobs, and no sender binding; other profiles are
//!   rejected.
//! - **Settlement root.** Before signing, the proof signer checks
//!   `StateUpdate.newState` against the root reth produced for the block.
//!
//! # Where to start reading
//!
//! - [`CompositionBuilder`] records calls dispatched during source simulation
//!   and finalizes them into a [`Composition`].
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
pub use action::{CallHashInput, CallMode, common_cross_chain_call_hash, l2_outbound_call_hash};
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
pub use composer::{ProxyLookupConfig, TargetConfig};
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
