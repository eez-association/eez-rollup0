//! Chain-agnostic cross-chain composition: protocol traits, generic
//! composer, and checkpoint format.
//!
//! Vendored and adapted from the sibling `rollup-node` project's
//! `eez-protocol` crate. Strictly chain-family-agnostic: no
//! alloy, no reth, no revm. The EVM-specific impl lives in
//! `eez-evm` (Step 5).
#![deny(missing_docs)]

pub mod capabilities;
pub mod checkpoint;
pub mod compose;
pub mod composer;
pub mod composition;
pub mod error;
pub mod executor;
pub mod message;
pub mod proof_plan;
pub mod protocol;
pub mod rolling_hash;
pub mod rollup_id;
pub mod types;

/// Test doubles (`FakeChainClient` / `FakeChainSession`) for unit-testing a
/// `ChainProtocol` / `ChainClient` impl. Gated by the `testing` feature so
/// consumers opt in via `features = ["testing"]`; visible to this crate's own
/// tests without the flag.
#[cfg(any(test, feature = "testing"))]
pub mod testing;

mod assertions;

#[doc(inline)]
pub use capabilities::{ConsumesInbound, SettlesOutbound};
#[doc(inline)]
pub use checkpoint::ExecutionCheckpoint;
#[doc(inline)]
pub use compose::{compose_transaction, compose_transaction_recorded};
#[doc(inline)]
pub use composer::{
    Composer, ComposerBuilder, DEFAULT_CCM_GAS_LIMIT, ProxyLookupConfig, SourceAttribution,
    TargetConfig,
};
#[doc(inline)]
pub use composition::{CompositionBuilder, Dispatcher, Rollup};
#[doc(inline)]
pub use error::{
    ComposerError, ComposerErrorKind, ComposerResult, CompositionError, CompositionErrorKind,
    CompositionResult, ExecutorError, ExecutorErrorKind, ExecutorResult, ProtocolError,
    ProtocolErrorKind, ProtocolResult,
};
#[doc(inline)]
pub use executor::{
    ChainClient, CommittedRootReader, EntryChainClient, ExecutionRequest, ExecutionResponse,
    ProtocolCheckpoint, SessionSnapshot, TargetBatchSimulation, TargetExecutionSession,
    TargetTransaction, TargetVerificationContext,
};
pub use message::{Delivery, Message, MessageRole};
#[doc(inline)]
pub use proof_plan::{
    ProofPlan, ProofPlanInvariantError, ProofPlanResolver, RollupProofAssignment,
    TimestampAndBlockHash,
};
pub use protocol::ChainProtocol;
#[doc(inline)]
pub use rolling_hash::{
    CALL_BEGIN, CALL_END, EntryRollingHash, NESTED_BEGIN, NESTED_END, StaticCallRollingHash,
};
#[doc(inline)]
pub use rollup_id::{ChainIdentity, RollupId};
#[doc(inline)]
pub use types::{
    Composition, ExecutedAction, ExecutionOutcome, SourceComposition, StaticMeta, TargetComposition,
};
