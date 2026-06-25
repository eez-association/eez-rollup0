//! The [`ChainProtocol`] trait — the extension point for chain-specific logic.
//!
//! EVM is the first implementation (in `eez-evm`, Step 5).
//! Non-EVM chains implement the same trait with their own types.
//!
//! # Static dispatch by design
//!
//! [`ChainProtocol`] is intentionally not dyn-compatible: it has seven
//! associated types (`Address`, `Value`, `Calldata`, `Batch`, `Overlay`,
//! `Witness`, `Dialect`) and `Self` appears in argument positions via
//! [`ExecutedAction<Self>`]. Both independently block use as
//! `dyn ChainProtocol`.
//!
//! This is the right shape for what the trait encodes: the chain
//! family is fixed at compile time per binary. Generics over
//! `P: ChainProtocol` give strong type-level separation of
//! chain-specific data — an EVM `Batch` can never be mixed with
//! another chain's `Batch` by accident — at zero runtime cost.
//!
//! # What a sibling implementation looks like
//!
//! A new chain family contributes exactly one `impl ChainProtocol`:
//!
//! ```ignore
//! use eez_protocol::{ChainProtocol, ExecutedAction, RollupId, ProtocolResult};
//! use eez_protocol::composer::SourceAttribution;
//!
//! /// Unit struct — state lives on CompositionBuilder, not here.
//! pub struct MyChainProtocol;
//!
//! impl ChainProtocol for MyChainProtocol {
//!     type Address  = MyAddress;     // Clone + Debug + Send + Sync
//!     type Value    = MyValue;       // + Default (zero value for system txs)
//!     type Calldata = Vec<u8>;
//!     type Batch    = MyBatch;       // chain-shaped table-loading batch
//!     type Overlay  = MyOverlay;     // Serialize + DeserializeOwned
//!     type Witness  = MyWitness;
//!     type Dialect  = MyDialect;     // per-rollup ABI/emission rules
//!
//!     fn build_batch(
//!         &self,
//!         recorded: &[ExecutedAction<Self>],
//!         attribution: &SourceAttribution<'_>,
//!         dialect: &Self::Dialect,
//!         source_rollup_id: RollupId,
//!         raw_tx: &[u8],
//!     ) -> ProtocolResult<Self::Batch> { /* ... */ }
//!
//!     // encode_postbatch, encode_load_table, encode_follower_trigger,
//!     // dialect_is_zk_poster, batch_is_empty, encode_*/decode_* pairs
//!     // for Address / Value / Calldata transport.
//! }
//! ```
//!
//! Everything upstream (composer, source simulator, gRPC transport) picks up
//! the new chain family by generic substitution; no other code changes.
//!
//! # Purity contract
//!
//! All methods on [`ChainProtocol`] must be **pure functions of their
//! arguments** — idempotent, deterministic, free of observable side
//! effects. [`CompositionBuilder::finalize`](crate::composition::CompositionBuilder::finalize)
//! calls [`build_batch`](ChainProtocol::build_batch),
//! [`encode_table_payload`](ChainProtocol::encode_table_payload), and
//! [`encode_follower_trigger`](ChainProtocol::encode_follower_trigger)
//! **twice per target per composition** (once to build CCM simulation
//! inputs, once to emit the final composition output), and expects
//! byte-identical results on both calls. Implementations that cache
//! results across calls or mutate external state break this contract
//! and produce nondeterministic compositions.

use serde::{de::DeserializeOwned, Serialize};

#[allow(
    unused_imports,
    reason = "ProtocolError / ProtocolErrorKind used in rustdoc intra-doc links"
)]
use crate::error::{ProtocolError, ProtocolErrorKind, ProtocolResult};
use crate::rollup_id::RollupId;
use crate::types::ExecutedAction;

/// Defines how a specific chain constructs cross-chain entries.
///
/// Associated types allow each chain to use its native types:
/// - EVM: `Address = alloy::Address`, `Entry = ExecutionEntrySol`
/// - Non-EVM: custom address/entry/overlay/witness types
///
/// Static dispatch only. Not dyn-compatible — see the module-level
/// "Static dispatch by design" section for the reasoning.
pub trait ChainProtocol: Send + Sync {
    /// Chain-specific address type (EVM: 20-byte `Address`).
    type Address: Clone + std::fmt::Debug + Send + Sync;
    /// Chain-specific value type (EVM: `U256`). `Default` required:
    /// every chain has a "zero value" concept (no native currency
    /// sent), which composition needs for non-value-carrying system
    /// transactions like the CCM `loadExecutionTable` call.
    type Value: Clone + Default + std::fmt::Debug + Send + Sync;
    /// Chain-specific calldata type (EVM: `Bytes`).
    type Calldata: Clone + std::fmt::Debug + Send + Sync;
    /// Chain-shaped batch destined for one rollup's table-loading
    /// entry point — `EEZ.postAndVerifyBatch`
    /// (L1-style) or `EEZL2.loadExecutionTable`
    /// (L2-style) on EVM. Built by
    /// [`build_batch`](Self::build_batch); encoded into calldata by
    /// [`encode_postbatch`](Self::encode_postbatch) /
    /// [`encode_load_table`](Self::encode_load_table). Carries the
    /// deferred-execution table, lookup queue, transient-prefix
    /// counts, and DA-blob plumbing the on-chain entry point
    /// requires.
    type Batch: Clone + Send + Sync;
    /// Chain-specific state overlay (accumulated changes for gRPC continuation).
    type Overlay: Serialize + DeserializeOwned + Clone + Send;
    /// Chain-specific state witness (for proving).
    type Witness: Serialize + DeserializeOwned + Clone + Send;
    /// Per-chain dialect: ABI-selection + entry-emission rules.
    ///
    /// Captures the concrete differences between chain families
    /// (e.g. L1 `EEZ.executeL1ToL2Call` vs L2
    /// `EEZL2.executeIncomingCrossChainCall`) so that
    /// generic orchestration code in `eez-protocol` can
    /// dispatch to a single `encode_follower_trigger` method without
    /// naming chain-specific types.
    ///
    /// Stored on [`crate::composer::TargetConfig`] so each rollup
    /// carries its own dialect without global mutable state. The
    /// concrete type lives in the chain-specific crate
    /// (`eez-evm`), keeping `eez-protocol` free
    /// of EVM imports.
    type Dialect: Clone + std::fmt::Debug + Send + Sync + 'static;

    // ── Protocol logic ──────────────────────────────────────────

    /// Build the chain-shaped batch destined for `source_rollup_id`'s
    /// table-loading entry point. Walks the preorder `recorded` slice,
    /// classifies each call (top-level / nested-success /
    /// nested-failed / static), folds per-entry rolling hashes, and
    /// emits the resulting [`Batch`](Self::Batch).
    ///
    /// `attribution` carries per-rollup initial state roots + per-tx
    /// CCM-verify post-state roots so per-entry stateDeltas chain
    /// correctly. `dialect` controls per-rollup emission rules
    /// (whether the rollup hosts the canonical ZK proof or is loaded
    /// via the system address). `raw_tx` is plumbed through for
    /// chains/dialects that need access to the originating user-tx
    /// bytes.
    ///
    /// # Errors
    ///
    /// Impl-dependent — returns any [`ProtocolError`] the chain
    /// surfaces for input validation (missing initial roots,
    /// invalid call ordering, …).
    fn build_batch(
        &self,
        recorded: &[ExecutedAction<Self>],
        attribution: &crate::composer::SourceAttribution<'_>,
        dialect: &Self::Dialect,
        source_rollup_id: RollupId,
        raw_tx: &[u8],
    ) -> ProtocolResult<Self::Batch>;

    // The L1-side settle-outbound builder ("build_l1_postbatch") is NOT a core
    // method — it is the [`SettlesOutbound`](crate::capabilities::SettlesOutbound)
    // capability. Only chains that host the canonical proof + settle L2→L1 calls
    // implement it; `CompositionBuilder::finalize` bounds on it. This keeps the
    // core trait outbound-vendor-identical and makes "settles outbound" a
    // compile-time fact rather than a default that errors for non-settlers.

    /// Encode `batch` as the L1-style ZK-batch poster's calldata
    /// (`EEZ.postAndVerifyBatch` — single
    /// struct arg). Under the multi-prover ABI, proofs live inside
    /// the batch struct itself (`batch.inner.proofs[]`); callers
    /// populate `proofs[]` before encoding — see
    /// `composer-lib::post_batch_submitter` (`submit_with_proof`) for
    /// the canonical fill+encode+submit pipeline.
    fn encode_postbatch(&self, batch: &Self::Batch) -> Vec<u8>;

    /// Encode `batch` as the L2-style table-loader's calldata
    /// (`EEZL2.loadExecutionTable` — system-address
    /// gated, no proof field).
    fn encode_load_table(&self, batch: &Self::Batch) -> Vec<u8>;

    /// Encode the table-loading payload for the rollup whose
    /// `dialect` is supplied. Dispatches between
    /// [`encode_postbatch`](Self::encode_postbatch) (L1-style) and
    /// [`encode_load_table`](Self::encode_load_table) (L2-style) so
    /// callers don't have to peek at the dialect themselves.
    /// Default impl wires the dispatch via the chain's own
    /// `dialect_is_zk_poster` predicate (overridden per-protocol).
    fn encode_table_payload(&self, batch: &Self::Batch, dialect: &Self::Dialect) -> Vec<u8> {
        if self.dialect_is_zk_poster(dialect) {
            self.encode_postbatch(batch)
        } else {
            self.encode_load_table(batch)
        }
    }

    /// Whether this dialect routes its table-loading payload through
    /// the canonical proof-bundle poster (e.g. EVM L1-style →
    /// `EEZ.postAndVerifyBatch`). Drives
    /// [`encode_table_payload`](Self::encode_table_payload)'s
    /// dispatch.
    fn dialect_is_zk_poster(&self, dialect: &Self::Dialect) -> bool {
        let _ = dialect;
        false
    }

    /// Whether `batch` is empty — used by `composition::finalize` to
    /// short-circuit CCM verify and target-composition emission for
    /// fully-reverted call sequences. Default returns `false` (never
    /// short-circuit).
    fn batch_is_empty(&self, batch: &Self::Batch) -> bool {
        let _ = batch;
        false
    }

    /// Encode the first call of a per-target group as the target
    /// chain's execute payload. Only this first call is triggered
    /// explicitly on the target chain; subsequent calls to the same
    /// target resolve through the preloaded execution table sequence.
    ///
    /// Dialect-dependent (EVM example):
    /// - `EvmL2Style` follower → `executeL1ToL2Call(sourceAddress,
    ///   callData)` — permissionless, called through the registered
    ///   proxy. `raw_tx` is unused.
    /// - `EvmL1Style` follower under an L2 entry →
    ///   `executeL2TX(uint256 rollupId)` — the rollup id became
    ///   explicit in the multi-prover refactor. `raw_tx` is unused
    ///   at the encoding boundary.
    ///
    /// `raw_tx` is plumbed end-to-end for chains/dialects that need
    /// it; current EVM dialects ignore it.
    fn encode_follower_trigger(
        &self,
        call: &ExecutedAction<Self>,
        source_rollup_id: RollupId,
        raw_tx: &[u8],
        dialect: &Self::Dialect,
    ) -> Vec<u8>;

    // ── Transport encoding (for gRPC byte serialization) ────────
    //
    // Round-trip invariant: for every `x` of the appropriate type,
    // `decode_X(&encode_X(&x))` must return a value equal to `x`.
    // The gRPC transport (and any future wire format) relies on
    // this — `ExecutionRequest` / `ExecutedAction` fields cross the
    // wire by encoding each chain-specific field individually.
    // Impls that add padding, timestamps, or other non-deterministic
    // decoration break this contract.

    /// Encode a chain-specific address to raw bytes for transport.
    fn encode_address(&self, addr: &Self::Address) -> Vec<u8>;
    /// Decode a chain-specific address from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolErrorKind::InvalidEncoding`] if `bytes` cannot be
    /// parsed as a valid address for this chain.
    fn decode_address(&self, bytes: &[u8]) -> ProtocolResult<Self::Address>;
    /// Encode a chain-specific value to raw bytes for transport.
    fn encode_value(&self, val: &Self::Value) -> Vec<u8>;
    /// Decode a chain-specific value from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolErrorKind::InvalidEncoding`] if `bytes` cannot be
    /// parsed as a valid value for this chain.
    fn decode_value(&self, bytes: &[u8]) -> ProtocolResult<Self::Value>;
    /// Encode chain-specific calldata to raw bytes for transport.
    fn encode_calldata(&self, data: &Self::Calldata) -> Vec<u8>;
    /// Decode chain-specific calldata from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolErrorKind::InvalidEncoding`] if `bytes` cannot be
    /// parsed as valid calldata for this chain.
    fn decode_calldata(&self, bytes: &[u8]) -> ProtocolResult<Self::Calldata>;

    /// The cross-chain message identity `H` — the single cross-layer join key.
    ///
    /// Both the PRODUCER and the CONSUMER of a message MUST derive `H` through
    /// this one method (the layer-2 matching proof keys on it), so an
    /// implementor cannot supply `H` out-of-band. Required: the hash preimage is
    /// chain-specific.
    ///
    /// `m`'s rollup ids are REGISTRY ids (see [`Message`](crate::message::Message)):
    /// `H` must be the identity BOTH chains agree on — the L1-registry-assigned
    /// id, not a chain's native chain id.
    fn message_id(&self, m: &crate::message::Message<'_, Self>) -> [u8; 32];
}
