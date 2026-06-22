//! Solidity ABI types matching the multi-prover protocol on
//! `sync-rollups-protocol` (`feature/multi-prover-flatten`).
//!
//! These are the on-chain types used by `EEZ.sol` (central registry)
//! and `CrossChainManagerL2.sol` (per-L2 manager). The [`sol!`] macro
//! generates Rust types plus ABI encoding from the Solidity definitions
//! at `sync-rollups-protocol/src/{ICrossChainManager,EEZ}.sol`.
//!
//! The protocol is **multi-prover**: each rollup picks the subset of
//! the batch-global `proofSystems[]` it accepts via
//! [`RollupIdWithProofSystemsSol`], and the per-rollup `IRollupContract`
//! enforces both threshold and per-PS membership.
//!
//! Per-entry shape: every [`ExecutionEntrySol`] carries flat
//! [`L2ToL1CallSol`] + [`ExpectedL1ToL2CallSol`] arrays, the
//! cross-chain-call hash on the entry's `proxyEntryHash` field
//! (matched at consumption against the proxy's computed
//! `crossChainCallHash`), and `StateDelta[]` with `currentState`
//! present on every delta (checked on-chain via `_applyStateDeltas`;
//! see invariant 6 + `EEZ.sol:1032`).
//!
//! Failed reentrant calls / read-only sub-calls land in
//! [`LookupCallSol`] (formerly `StaticCall`), content-addressed by
//! `(crossChainCallHash, callNumber, lastNestedActionConsumed)`.

use alloy_sol_types::sol;

sol! {
    /// Per-rollup state delta on an entry.
    ///
    /// `currentState` is the rollup's expected state root immediately
    /// before this delta is applied; checked on-chain at consumption
    /// against `EEZ.rollups(rollupId).stateRoot`. Mismatch reverts
    /// `StateRootMismatch(rollupId)` (`EEZ.sol:1032`) — the soundness
    /// backstop that makes per-rollup same-block append safe
    /// (invariant 6).
    struct StateDeltaSol {
        uint256 rollupId;
        bytes32 currentState;
        bytes32 newState;
        int256  etherDelta;
    }

    /// Off-chain-only action representation. Mirrors the upstream
    /// `Action` struct (`script/e2e/shared/E2EHelpers.sol`); used by
    /// Rust tooling to compute the 6-field `crossChainCallHash`. Field
    /// declaration order is the `abi.encode` preimage order — do not
    /// reorder without updating `EEZ.computeCrossChainCallHash`.
    struct ActionSol {
        uint256 targetRollupId;
        address targetAddress;
        uint256 value;
        bytes   data;
        address sourceAddress;
        uint256 sourceRollupId;
    }

    /// One cross-chain call inside an entry's flat `L2ToL1Calls[]`
    /// list (formerly `CrossChainCall`).
    ///
    /// `revertSpan > 0` opens an isolated revert context spanning the
    /// next `revertSpan` calls (inclusive of this one). `revertSpan = 0`
    /// is a normal call.
    struct L2ToL1CallSol {
        address targetAddress;
        uint256 value;
        bytes   data;
        address sourceAddress;
        uint256 sourceRollupId;
        uint256 revertSpan;
    }

    /// Pre-computed result for a successful reentrant cross-chain call
    /// triggered during entry execution (formerly `NestedAction`).
    /// Consumed sequentially from the entry's `expectedL1ToL2Calls`
    /// array. All entries here must succeed — failed reentrant calls
    /// go through [`LookupCallSol`].
    struct ExpectedL1ToL2CallSol {
        bytes32 crossChainCallHash;
        uint256 callCount;
        bytes   returnData;
    }

    /// One execution entry. `proxyEntryHash == bytes32(0)` marks an
    /// immediate / `executeL2TX` entry; non-zero `proxyEntryHash` is
    /// consumed via a proxy `executeL1ToL2Call` matching the same hash
    /// against the proxy's computed `crossChainCallHash`.
    ///
    /// Top-level entries always succeed; entry-level revert is
    /// expressed via `LookupCallSol { failed: true }`. The legacy
    /// `failed` field is gone.
    struct ExecutionEntrySol {
        StateDeltaSol[]            stateDeltas;
        bytes32                    proxyEntryHash;
        uint256                    destinationRollupId;
        L2ToL1CallSol[]            L2ToL1Calls;
        ExpectedL1ToL2CallSol[]    expectedL1ToL2Calls;
        uint256                    callCount;
        bytes                      returnData;
        bytes32                    rollingHash;
    }

    /// Pre-computed result for a lookup call or a call that reverts
    /// (formerly `StaticCall`). Content-addressed by
    /// `(crossChainCallHash, callNumber, lastNestedActionConsumed)`.
    /// `destinationRollupId` routes the lookup into the per-rollup
    /// `lookupQueue` on L1.
    struct LookupCallSol {
        bytes32         crossChainCallHash;
        uint256         destinationRollupId;
        bytes           returnData;
        bool            failed;
        uint64          callNumber;
        uint64          lastNestedActionConsumed;
        L2ToL1CallSol[] calls;
        bytes32         rollingHash;
    }

    /// One participating rollup in a
    /// [`ProofSystemBatchPerVerificationEntriesSol`] together with the
    /// SUBSET of the batch's global `proofSystems[]` that this rollup
    /// accepts. Indices are `uint64`, strictly increasing.
    struct RollupIdWithProofSystemsSol {
        uint256   rollupId;
        uint64[]  proofSystemIndex;
    }

    /// The single struct argument to
    /// [`postAndVerifyBatchCall`]. Carries
    /// the deferred-execution table + lookup queue + per-rollup
    /// proof-system subsets + DA carriers + per-PS proofs.
    struct ProofSystemBatchPerVerificationEntriesSol {
        ExecutionEntrySol[]              entries;
        LookupCallSol[]                  l1ToL2lookupCalls;
        uint256                          transientExecutionEntryCount;
        uint256                          transientLookupCallCount;
        address[]                        proofSystems;
        RollupIdWithProofSystemsSol[]    rollupIdsWithProofSystems;
        bytes32                          crossProofSystemInteractions;
        uint256[]                        blobIndices;
        bytes                            callData;
        bytes[]                          proofs;
        // L1 block the batch binds to — forwarded to each rollup's
        // `getTimestampAndBlockHash`. 0 = no context, u64::MAX =
        // "latest" (`EEZ.sol:46-48`).
        uint64                           blockNumber;
    }

    /// L2 (`CrossChainManagerL2.loadExecutionTable`) — system-only.
    /// The L2 side has no proof systems / queue routing; entries and
    /// lookup calls flow through verbatim.
    function loadExecutionTable(
        ExecutionEntrySol[] entries,
        LookupCallSol[] _lookupCalls
    ) external;

    /// L1 (`EEZ.postAndVerifyBatch`) — single-struct entrypoint.
    /// Verifies the proof systems, marks each touched rollup
    /// verified-this-block, drains the immediate (`proxyEntryHash == 0`)
    /// prefix inline, drives the meta hook, then publishes the deferred
    /// remainder into per-rollup queues (`src/EEZ.sol:329`).
    function postAndVerifyBatch(
        ProofSystemBatchPerVerificationEntriesSol batch
    ) external;

    /// L1 (`EEZ.executeL2TX`) — permissionless replay of the next
    /// `proxyEntryHash == 0` entry in `verificationByRollup[rollupId]
    /// .queue`. The rollup id is explicit per the multi-prover
    /// refactor.
    function executeL2TX(uint256 rollupId) external;

    /// `EEZ.executeL1ToL2Call` / `CrossChainManagerL2.executeL1ToL2Call`
    /// — single permissionless entry point on both chains, called
    /// through a registered proxy. The proxy's `msg.sender` carries
    /// rollup identity; `sourceAddress` is the original caller on the
    /// source chain; `callData` is forwarded raw. Function rename
    /// from the prior `executeCrossChainCall`.
    function executeL1ToL2Call(address sourceAddress, bytes callData) external payable returns (bytes);

    /// L2 (`EEZL2.executeIncomingCrossChainCall`) — system-only,
    /// atomic inbound delivery for a cross-chain call from another
    /// rollup. Atomically loads the execution table and consumes
    /// `entries[0]`. `msg.value == value` strict equality (system
    /// mints exactly the call's value). Reverts `EntryHashMismatch`
    /// if `entries[0].proxyEntryHash != computeCrossChainCallHash(...)`.
    /// (`src/L2/EEZL2.sol:174-211`.)
    function executeIncomingCrossChainCall(
        address                  destination,
        uint256                  value,
        bytes                    data,
        address                  sourceAddress,
        uint256                  sourceRollup,
        ExecutionEntrySol[]      entries,
        LookupCallSol[]          _lookupCalls
    ) external payable returns (bytes);

    /// `staticCallLookup` — view on both chains. Looks up a cached
    /// [`LookupCallSol`] by `(crossChainCallHash, callNumber,
    /// lastNestedActionConsumed)` and either returns its `returnData`
    /// or reverts with it (when `failed == true`).
    function staticCallLookup(address sourceAddress, bytes callData) external view returns (bytes);

    /// Emitted once per `postAndVerifyBatch` with the participating
    /// rollup count (`EEZ.sol:185`). The L1 watcher locates posted
    /// batches by this event signature.
    event BatchPosted(uint256 indexed rollupCount);

    /// Emitted by `_applyStateDeltas` (`EEZ.sol:176`) each time a
    /// rollup's state delta applies, carrying the new stored root. The
    /// submitter counts these per rollup to derive `settled_count` /
    /// `settled_final_state`; absence marks a loser batch.
    event L2ExecutionPerformed(uint256 indexed rollupId, bytes32 newState);
}

#[cfg(test)]
mod selector_tests {
    use super::postAndVerifyBatchCall;
    use alloy_sol_types::SolCall;

    /// Pins `postAndVerifyBatch`'s selector to the canonical value
    /// (verified via `forge inspect EEZ methodIdentifiers`). A mismatch
    /// means a `*Sol` struct field diverged from `EEZ.sol`. Single
    /// ABI-drift guard for the workspace (eez-l1 / eez-prover reuse
    /// these types).
    #[test]
    fn post_and_verify_batch_selector_matches_contract() {
        assert_eq!(
            postAndVerifyBatchCall::SELECTOR,
            [0x51, 0xdd, 0x0a, 0xf6],
            "selector diverged from EEZ.sol @ fe7bf66 (canonical 0x51dd0af6)",
        );
    }
}
