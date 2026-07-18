//! The eez cross-chain protocol: composition engine, composer,
//! Solidity ABI types, flat action hashing, sequencing machinery, ZK
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
//!   assumption, a wrong hash verifies). It bakes in two assumptions today: a
//!   single uniform verification key across all rollups, and `(timestamp,
//!   blockHash) = (0, 0)` per rollup — a rollup that overrides
//!   `getTimestampAndBlockHash` breaks the hash *silently*.
//! - **Settlement root.** Before signing, the prover checks the settlement
//!   `StateDelta.newState` against the root reth actually produced for the block.
//!
//! The submit pipeline an integrator wires: resolve a `ProofPlan`
//! ([`proof_resolver::ProofPlanResolver`]) → [`entries::build_batch`] →
//! [`public_inputs::public_inputs_hashes`] → sign each digest with
//! [`EcdsaProofSigner`] → fill `batch.inner.proofs[]` → [`entries::encode_postbatch`].
//!
//! # Where to start reading
//!
//! - [`Composer::simulate_and_resolve`] runs one cross-chain
//!   composition end-to-end over the long-lived, client-caching
//!   [`Composer`].
//! - For the ABI boundary, [`entries::build_batch`] walks the preorder
//!   `recorded[..]` slice and materializes an [`EvmBatch`]; the
//!   per-dialect encoders (`encode_postbatch` / `encode_load_table`)
//!   produce the calldata wrappers.
#![deny(missing_docs)]

pub mod abi;
pub mod action;
pub mod addresses;
pub mod authorized_proxies;
pub mod batch;
pub mod checkpoint;
pub mod composer;
pub mod composition;
pub mod dialect;
pub mod entries;
pub mod error;
pub mod executor;
pub mod outbound_gate;
pub mod overlay;
pub mod proof_plan;
pub mod proof_resolver;
pub mod public_inputs;
pub mod rolling_hash;
pub mod rollup_id;
pub mod settlement;
pub mod signer;
pub mod system_tx;
pub mod types;
pub mod witness;

/// Test doubles (`FakeChainClient` / `FakeChainSession`) for unit-testing
/// against the `ChainClient` traits. Gated by the `testing` feature so
/// consumers opt in via `features = ["testing"]`; visible to this crate's own
/// tests without the flag.
#[cfg(any(test, feature = "testing"))]
pub mod testing;

mod assertions;

#[doc(inline)]
pub use action::{compute_state_root_slot, cross_chain_call_hash};
#[doc(inline)]
pub use addresses::{CCM_ADDRESS, SYSTEM_ADDRESS};
#[doc(inline)]
pub use authorized_proxies::{
    CCM_AUTHORIZED_PROXIES_SLOT, ProxyInfo, ROLLUPS_AUTHORIZED_PROXIES_SLOT, decode_proxy_value,
    proxy_mapping_key,
};
#[doc(inline)]
pub use batch::EvmBatch;
#[doc(inline)]
pub use checkpoint::ExecutionCheckpoint;
#[doc(inline)]
pub use composer::{Composer, ComposerBuilder, ProxyLookupConfig, SourceAttribution, TargetConfig};
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
pub use executor::{
    ChainClient, CommittedRootReader, EntryChainClient, ExecutionRequest, ExecutionResponse,
    SessionSnapshot, TargetExecutionSession,
};
#[doc(inline)]
pub use overlay::{
    AccountInfo, AccountOverlay, AccountStatus, ContractCode, EvmOverlay, StorageOverlay,
};
#[doc(inline)]
pub use proof_plan::{
    ProofPlan, ProofPlanInvariantError, RollupProofAssignment, TimestampAndBlockHash,
};
// The submit/prover surface (see the crate-level "Soundness model"): the hash
// helpers + the proof-plan resolver, re-exported so they're discoverable at the
// root rather than only by module path.
#[doc(inline)]
pub use abi::{
    ActionSol, ExecutionEntrySol, ExpectedL1ToL2CallSol, L2ToL1CallSol, LookupCallSol,
    ProofSystemBatchPerVerificationEntriesSol, RollupIdWithProofSystemsSol, StateDeltaSol,
};
#[doc(inline)]
pub use proof_resolver::{
    AlloyRollupReader, IEEZReader, ProofPlanResolver, ResolverConfigError, RollupReader,
};
#[doc(inline)]
pub use public_inputs::{all_per_ps_hashes, entry_hash, public_inputs_hashes, shared_public_input};
#[doc(inline)]
pub use rolling_hash::{
    CALL_BEGIN, CALL_END, EntryRollingHash, NESTED_BEGIN, NESTED_END, StaticCallRollingHash,
};
#[doc(inline)]
pub use rollup_id::{ChainIdentity, RollupId};
#[doc(inline)]
pub use signer::{EcdsaProofSigner, SignerError};
#[doc(inline)]
pub use types::{
    Composition, ExecutedAction, ExecutionOutcome, SourceComposition, StaticMeta, TargetComposition,
};
#[doc(inline)]
pub use witness::EvmWitness;
