//! Solidity ABI types matching the upstream multi-prover protocol.
//!
//! These are the on-chain types used by `EEZ.sol` (central registry)
//! and `EEZL2.sol` (per-L2 manager). The [`sol!`] macro
//! generates Rust types plus ABI encoding from the Solidity definitions
//! in `sync-rollups-protocol` (in-tree as the
//! `sync-rollups-protocol` submodule).
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
//! see upstream's invariant 6 enforced by `EEZ.sol`).
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
    /// `StateRootMismatch(rollupId)` (`EEZ.sol`) — the soundness
    /// backstop that makes per-rollup same-block append safe
    /// (upstream's invariant 6).
    struct StateDeltaSol {
        uint256 rollupId;
        bytes32 currentState;
        bytes32 newState;
        int256  etherDelta;
    }

    /// Off-chain-only action representation. Mirrors the upstream
    /// `Action` struct (`E2EHelpers.sol`); used by Rust tooling to
    /// compute the 6-field `crossChainCallHash`. Field declaration
    /// order is the `abi.encode` preimage order — do not reorder
    /// without updating `EEZ.computeCrossChainCallHash`.
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

    /// NESTED lookup (upstream PR #28): a reentrant cross-chain call that
    /// is looked up rather than executed — a reentrant STATICCALL (static
    /// mode) or a try/catch'd reverting reentrant call (reverted mode).
    /// Lives INSIDE the entry (`ExecutionEntrySol.expectedLookups`),
    /// entry-scoped. Matched on-chain by
    /// `(crossChainCallHash, l2ToL1CallNumber, lastL1ToL2CallConsumed,
    /// executingLookupIndex)`. Field order IS the L1 `IEEZ.ExpectedLookup`
    /// abi.encode preimage — do not reorder.
    struct ExpectedLookupSol {
        bytes32                 crossChainCallHash;
        bytes                   returnData;
        bool                    failed;
        uint64                  l2ToL1CallNumber;
        uint64                  lastL1ToL2CallConsumed;
        uint64                  executingLookupIndex;
        L2ToL1CallSol[]         l2ToL1Calls;
        ExpectedL1ToL2CallSol[] expectedL1ToL2Calls;
        uint256                 callCount;
        bytes32                 rollingHash;
    }

    /// A rollup's expected state root at the moment a TOP-LEVEL lookup is
    /// observed (upstream PR #28). Part of the lookup MATCH predicate
    /// (full-scan: a candidate matches only when every pin equals the live
    /// `EEZ.rollups(rollupId).stateRoot`). L1-only.
    struct ExpectedStateRootPerRollupSol {
        uint256 rollupId;
        bytes32 stateRoot;
    }

    /// One L1 execution entry. `proxyEntryHash == bytes32(0)` marks an
    /// immediate / `executeL2TX` entry; non-zero `proxyEntryHash` is
    /// consumed via a proxy `executeCrossChainCall` matching the same
    /// hash against the proxy's computed `crossChainCallHash`.
    ///
    /// Top-level entries always succeed; entry-level revert is
    /// expressed via `LookupCallSol { failed: true }`. Field order IS the
    /// `IEEZ.ExecutionEntry` abi.encode preimage (origin/main 5c51e02):
    /// `expectedLookups` sits between `expectedL1ToL2Calls` and `callCount`.
    struct ExecutionEntrySol {
        StateDeltaSol[]            stateDeltas;
        bytes32                    proxyEntryHash;
        uint256                    destinationRollupId;
        L2ToL1CallSol[]            l2ToL1Calls;
        ExpectedL1ToL2CallSol[]    expectedL1ToL2Calls;
        ExpectedLookupSol[]        expectedLookups;
        uint256                    callCount;
        bytes                      returnData;
        bytes32                    rollingHash;
    }

    /// TOP-LEVEL lookup (upstream PR #28): the pre-computed result of a
    /// top-level cross-chain call looked up rather than executed. Lives in
    /// the per-rollup `lookupQueue` (`destinationRollupId`). Matched by
    /// `crossChainCallHash` + every `expectedStateRoots` pin live (no more
    /// cursor key). Field order IS the `IEEZ.LookupCall` abi.encode
    /// preimage; `expectedStateRoots` is LAST.
    struct LookupCallSol {
        bytes32                          crossChainCallHash;
        uint256                          destinationRollupId;
        bytes                            returnData;
        bool                             failed;
        L2ToL1CallSol[]                  l2ToL1Calls;
        ExpectedL1ToL2CallSol[]          expectedL1ToL2Calls;
        ExpectedLookupSol[]              expectedLookups;
        uint256                          callCount;
        bytes32                          rollingHash;
        ExpectedStateRootPerRollupSol[]  expectedStateRoots;
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
    /// `blockNumber` (upstream PR #28) is the single L1 block the whole
    /// batch binds to; the registry forwards it to every rollup's
    /// `getTimestampAndBlockHash(blockNumber)`. We ALWAYS bind a recent
    /// L1 head N (`prepare_post_batch`; `0` is the contract's timeless
    /// sentinel, refused on BOTH sides) and both sides fold
    /// `(0, blockhash(N))` — see `public_inputs_hashes`. It is the LAST
    /// tuple field.
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
        uint64                           blockNumber;
    }

    /// L1 (`EEZ.postAndVerifyBatch`) —
    /// single-struct entrypoint that replaces the prior 7-arg
    /// `postBatch`. Performs proof-system verification, marks each
    /// touched rollup verified-this-block, drains the immediate
    /// (`proxyEntryHash == 0`) prefix inline, drives the meta hook,
    /// then publishes the deferred remainder into per-rollup queues.
    function postAndVerifyBatch(
        ProofSystemBatchPerVerificationEntriesSol batch
    ) external;

    /// L1 (`EEZ.executeL2TX`) — permissionless replay of the next
    /// `proxyEntryHash == 0` entry in `verificationByRollup[rollupId]
    /// .queue`. The rollup id is explicit per the multi-prover
    /// refactor.
    function executeL2TX(uint256 rollupId) external;

    /// `staticCallLookup` — view on both chains. Looks up a cached
    /// top-level lookup by `crossChainCallHash` (+ state-root pins on
    /// L1) and either returns its `returnData` or reverts with it (when
    /// `failed == true`).
    function staticCallLookup(address sourceAddress, bytes callData) external view returns (bytes);

    /// Emitted once per `postAndVerifyBatch` (`EEZ.sol:154`); `rollupCount` is
    /// the global batch counter. eez-l1's watcher rings on this to detect new
    /// batches. (Re-added for eez0's centralized event decoding — eez-l1 imports
    /// it from here; upstream crosschain-evm dropped it as each consumer defines
    /// its own.)
    event BatchPosted(uint256 indexed rollupCount);

    /// Emitted by `_applyStateDeltas` (`EEZ.sol:143`) each time a rollup's state
    /// delta applies, carrying the new stored root. The submitter counts these
    /// per rollup to derive settled_count / winner-loser tagging.
    event L2ExecutionPerformed(uint256 indexed rollupId, bytes32 newState);

    /// Emitted by `_consumeAndExecute` (`EEZ.sol:903`) each time a DEFERRED
    /// (inbound, `proxyEntryHash != 0`) entry is consumed by its bundled user
    /// tx. NEVER emitted for an immediate (outbound, `proxyEntryHash == 0`)
    /// entry — those execute inline (emitting `L2ExecutionPerformed`) or are
    /// skipped (`ImmediateEntrySkipped`). The deriver counts these per rollupId
    /// to get the ACTUAL consumed-deferred count DIRECTLY, independent of the
    /// static outbound count — robust to a skipped outbound immediate (which
    /// would otherwise deflate `settled_count` and truncate an inbound
    /// delivery). `rollupId` is the SECOND indexed param ⇒ `topic2` when
    /// filtering.
    event ExecutionConsumed(
        bytes32 indexed crossChainCallHash, uint256 indexed rollupId, uint256 indexed executionQueueIndex
    );

    /// Emitted by `EEZL2.executeCrossChainCall` (`EEZL2.sol:200`, decl
    /// `EEZBase.sol:97`) each time an OUTBOUND L2→L1 call is consumed on L2 —
    /// `crossChainCallHash` (topic1) is `computeCrossChainCallHash(targetRollupId,
    /// target, value, data, sourceAddress, ROLLUP_ID)` over the ACTUAL call
    /// (source = the proxy's immediate caller, at ANY depth — so a wrapper
    /// contract is the source). The outbound authorization gate matches each
    /// settlement entry's computed hash against these topic1 values from the
    /// re-executed Sync block: a phantom withdrawal has no matching event. INBOUND
    /// delivery emits `IncomingCrossChainCallExecuted` instead, so filtering on
    /// this topic0 isolates outbound consumptions.
    event CrossChainCallExecuted(
        bytes32 indexed crossChainCallHash,
        address indexed proxy,
        address sourceAddress,
        bytes callData,
        uint256 value
    );
}

// L2 (`EEZL2`, IEEZL2) execution table types — a SEPARATE, leaner family
// from the L1 structs (upstream PR #28 split `IEEZL2.sol` out): an L2 entry
// drops `stateDeltas` and `destinationRollupId` (single rollup, no per-rollup
// queue interleaving), and uses self-relative directional names
// (`incomingCalls` / `expectedOutgoingCalls`) because an L2's cross-chain
// counterparty may be L1 OR another L2. Our pipeline stays L1-shaped
// internally; `encode_load_table` / `encode_execute_incoming` LOWER an
// L1-shaped batch into these at the encode boundary (no info needed that the
// L1 shape lacks). Field orders ARE the IEEZL2 abi.encode preimages.
sol! {
    /// One cross-chain call inside an L2 entry's flat `incomingCalls[]`.
    struct CrossChainCallSol {
        address targetAddress;
        uint256 value;
        bytes   data;
        address sourceAddress;
        uint256 sourceRollupId;
        uint256 revertSpan;
    }

    /// Pre-computed result for a successful reentrant OUTGOING cross-chain
    /// call fired from this L2 during execution.
    struct ExpectedOutgoingCrossChainCallSol {
        bytes32 crossChainCallHash;
        uint256 callCount;
        bytes   returnData;
    }

    /// L2 nested lookup — entry-scoped, matched by
    /// `(crossChainCallHash, callNumber, lastOutgoingCallConsumed,
    /// executingLookupIndex)`. (Note: L2 names the cursor `callNumber` /
    /// `lastOutgoingCallConsumed`, vs L1's `l2ToL1CallNumber` /
    /// `lastL1ToL2CallConsumed` — same roles, self-relative names.)
    struct L2ExpectedLookupSol {
        bytes32                              crossChainCallHash;
        bytes                                returnData;
        bool                                 failed;
        uint64                               callNumber;
        uint64                               lastOutgoingCallConsumed;
        uint64                               executingLookupIndex;
        CrossChainCallSol[]                  incomingCalls;
        ExpectedOutgoingCrossChainCallSol[]  expectedOutgoingCalls;
        uint256                              callCount;
        bytes32                              rollingHash;
    }

    /// One L2 execution entry. Leaner than L1: no `stateDeltas`, no
    /// `destinationRollupId`. `proxyEntryHash` is never `bytes32(0)` on L2.
    struct L2ExecutionEntrySol {
        bytes32                              proxyEntryHash;
        CrossChainCallSol[]                  incomingCalls;
        ExpectedOutgoingCrossChainCallSol[]  expectedOutgoingCalls;
        L2ExpectedLookupSol[]                expectedLookups;
        uint256                              callCount;
        bytes                                returnData;
        bytes32                              rollingHash;
    }

    /// L2 top-level lookup — matched by `crossChainCallHash` alone (L2 has
    /// no state roots to pin). No `destinationRollupId` / `expectedStateRoots`.
    struct L2LookupCallSol {
        bytes32                              crossChainCallHash;
        bytes                                returnData;
        bool                                 failed;
        CrossChainCallSol[]                  incomingCalls;
        ExpectedOutgoingCrossChainCallSol[]  expectedOutgoingCalls;
        L2ExpectedLookupSol[]                expectedLookups;
        uint256                              callCount;
        bytes32                              rollingHash;
    }

    /// L2 (`EEZL2.loadExecutionTable`) — system-only. Selector 0x59683c8b.
    function loadExecutionTable(
        L2ExecutionEntrySol[] entries,
        L2LookupCallSol[] _lookupCalls
    ) external;

    /// L2 (`EEZL2.executeIncomingCrossChainCall`) — system-only inbound
    /// (L1→L2). Loads the table, binds the top-level inbound call's hash to
    /// `entries[0].proxyEntryHash`, drives `_processNCalls` (the inbound call
    /// delivered through the lazily-created source proxy), returns the entry's
    /// `returnData`. `msg.value` must equal `value`. Selector 0xeb494246.
    function executeIncomingCrossChainCall(
        address destination,
        uint256 value,
        bytes data,
        address sourceAddress,
        uint256 sourceRollup,
        L2ExecutionEntrySol[] entries,
        L2LookupCallSol[] lookupCalls
    ) external payable returns (bytes);
}

#[cfg(test)]
mod selector_locks {
    //! Upstream ABI pins (origin/main 5c51e02). Computed with
    //! `cast sig` against the IEEZL2 tuple layouts; a drift here means our
    //! sol! shapes diverged from the deployed contract — a settlement-
    //! breaking error caught at `cargo test`, not on-chain.
    use super::*;
    use alloy_sol_types::SolCall;

    #[test]
    fn l2_selectors_match_upstream() {
        assert_eq!(
            loadExecutionTableCall::SELECTOR,
            [0x59, 0x68, 0x3c, 0x8b],
            "loadExecutionTable selector drifted from upstream 0x59683c8b"
        );
        assert_eq!(
            executeIncomingCrossChainCallCall::SELECTOR,
            [0xeb, 0x49, 0x42, 0x46],
            "executeIncomingCrossChainCall selector drifted from upstream 0xeb494246"
        );
    }

    /// The `postAndVerifyBatch` selector encodes the FULL L1 batch tuple —
    /// `ExecutionEntrySol` (incl. `expectedLookups`), `LookupCallSol` (incl.
    /// `expectedStateRoots`), and the trailing `blockNumber`. Matching upstream
    /// (`out/EEZ.sol/EEZ.json` methodIdentifiers @ 5c51e02) pins our L1
    /// entry/lookup/batch ABI byte-for-byte — a single field reorder/add/remove
    /// flips it — which is what makes the `keccak256(abi.encode(entry))`
    /// byte-locks (counterl1_vectors.rs) a true on-chain oracle.
    #[test]
    fn l1_postbatch_selector_matches_upstream() {
        assert_eq!(
            postAndVerifyBatchCall::SELECTOR,
            [0x8b, 0x1a, 0x09, 0x5a],
            "postAndVerifyBatch selector drifted — L1 batch ABI layout diverged from upstream"
        );
    }
}
