//! Solidity ABI types for the pinned `eez-core-protocol` contracts.
//!
//! The L1 types mirror `IEEZ.sol`; the separate L2 family mirrors
//! `IEEZL2.sol`. Field order and integer widths are part of the wire ABI.

use alloy_sol_types::sol;

sol! {
    /// One rollup state transition carried by an L1 entry.
    #[derive(Debug)]
    struct StateUpdateSol {
        uint64 rollupId;
        bytes32 currentState;
        bytes32 newState;
        int256 etherDelta;
    }

    /// A composer assertion about a rollup's live state root.
    #[derive(Debug)]
    struct ExpectedStateRootPerRollupSol {
        uint64 rollupId;
        bytes32 stateRoot;
    }

    /// One cross-chain call executed on L1.
    #[derive(Debug)]
    struct L2ToL1CallSol {
        uint16 revertNextNCalls;
        bool isStatic;
        uint64 gas;
        address sourceAddress;
        uint64 sourceRollupId;
        address targetAddress;
        uint256 value;
        bytes data;
    }

    /// The expected result of a reentrant L1-to-L2 call.
    #[derive(Debug)]
    struct ExpectedL1ToL2CallSol {
        bytes32 expectedL1toL2Hash;
        L2ToL1CallSol[] l2ToL1Calls;
        bytes32 revertedOrStaticRollingHash;
        bool success;
        bytes returnData;
    }

    /// One mutable top-level L1 execution entry.
    #[derive(Debug, Default)]
    struct ExecutionEntrySol {
        StateUpdateSol[] stateUpdates;
        bytes32 proxyEntryHash;
        L2ToL1CallSol[] l2ToL1Calls;
        ExpectedL1ToL2CallSol[] expectedL1ToL2Calls;
        bytes32 rollingHash;
        uint64 destinationRollupId;
        bool success;
        bytes returnData;
    }

    /// One read-only top-level L1 execution entry.
    #[derive(Debug, Default)]
    struct StaticExecutionEntrySol {
        ExpectedStateRootPerRollupSol[] expectedStateRoots;
        bytes32 proxyEntryHash;
        L2ToL1CallSol[] l2ToL1Calls;
        bytes32 rollingHash;
        uint64 destinationRollupId;
        bool success;
        bytes returnData;
    }

    /// A participating rollup and its selected proof-system indices.
    #[derive(Debug)]
    struct RollupIdWithProofSystemsSol {
        uint64 rollupId;
        uint64[] proofSystemIndexes;
    }

    /// The single argument to `EEZ.postAndVerifyBatch`.
    #[derive(Debug, Default)]
    struct ProofSystemBatchPerVerificationEntriesSol {
        ExpectedStateRootPerRollupSol[] expectedStateRootPerRollup;
        ExecutionEntrySol[] entries;
        StaticExecutionEntrySol[] staticEntries;
        uint256 immediateEntryCount;
        uint256 immediateStaticEntryCount;
        address[] proofSystems;
        RollupIdWithProofSystemsSol[] rollupIdsWithProofSystems;
        uint256[] blobIndices;
        bytes callData;
        bytes[] proofs;
        uint64 blockNumber;
        bool bindMsgSenderInPublicInput;
    }

    function postAndVerifyBatch(
        ProofSystemBatchPerVerificationEntriesSol batch
    ) external;

    function executeL2Txs(uint64 rollupId) external returns (bytes);

    function staticCrossChainCall(address sourceAddress, bytes callData) external view returns (bytes);

    event BatchPosted(uint256 indexed rollupCount);

    event L2ExecutionPerformed(uint64 indexed rollupId, bytes32 newState);

    event ExecutionConsumed(
        bytes32 indexed crossChainCallHash, uint64 indexed rollupId, uint256 indexed entryQueueIndex
    );

    event CrossChainCallExecuted(
        bytes32 indexed crossChainCallHash,
        address indexed proxy,
        address sourceAddress,
        bytes callData,
        uint256 value
    );
}

/// Alias for the batch accepted by the protocol's `postAndVerifyBatch` ABI.
///
/// Composition builds partial batches containing source-side or target-side
/// entries. Downstream settlement may merge them and attach state updates,
/// proof-system metadata, and proofs.
pub type EvmBatch = ProofSystemBatchPerVerificationEntriesSol;

/// Events whose ABI differs specifically on `EEZL2`.
pub mod eez_l2_events {
    use alloy_sol_types::sol;

    sol! {
        /// Emitted when a mutable call leaves an L2 through a proxy.
        ///
        /// `callGas` is the manager-entry gas value included in the call hash.
        /// It is zero when the deployed manager has `USE_GAS_LEFT` disabled.
        event CrossChainCallExecuted(
            bytes32 indexed crossChainCallHash,
            address indexed proxy,
            address sourceAddress,
            bytes callData,
            uint256 value,
            uint64 callGas
        );
    }
}

// The L2 ABI is a separate, leaner family: one rollup means entries do not
// carry L1 state updates or destination routing.
sol! {
    /// One cross-chain call executed on this L2.
    #[derive(Debug)]
    struct CrossChainCallSol {
        uint16 revertNextNCalls;
        bool isStatic;
        uint64 gas;
        address sourceAddress;
        uint64 sourceRollupId;
        address targetAddress;
        uint256 value;
        bytes data;
    }

    /// The expected result of a reentrant call leaving this L2.
    #[derive(Debug)]
    struct ExpectedOutgoingCrossChainCallSol {
        bytes32 expectedOutgoingHash;
        CrossChainCallSol[] incomingCalls;
        bytes32 revertedOrStaticRollingHash;
        bool success;
        bytes returnData;
    }

    /// One mutable top-level L2 execution entry.
    #[derive(Debug, Default)]
    struct L2ExecutionEntrySol {
        bytes32 proxyEntryHash;
        CrossChainCallSol[] incomingCalls;
        ExpectedOutgoingCrossChainCallSol[] expectedOutgoingCalls;
        bytes32 rollingHash;
        bool success;
        bytes returnData;
    }

    /// One read-only top-level L2 execution entry.
    #[derive(Debug, Default)]
    struct L2StaticExecutionEntrySol {
        bytes32 proxyEntryHash;
        CrossChainCallSol[] incomingCalls;
        bytes32 rollingHash;
        bool success;
        bytes returnData;
    }

    function loadExecutionTable(
        L2ExecutionEntrySol[] _entries,
        L2StaticExecutionEntrySol[] _staticEntries
    ) external;

    function executeIncomingCrossChainCall(
        address destination,
        uint256 value,
        bytes data,
        address sourceAddress,
        uint64 sourceRollup,
        L2ExecutionEntrySol[] _entries,
        L2StaticExecutionEntrySol[] _staticEntries
    ) external payable returns (bytes);
}

// `EEZBase` surface shared by `EEZ` (L1) and `EEZL2` (L2), plus the
// `CrossChainProxy` entry point the managers drive. Slot composition replays
// these frames verbatim instead of shortcutting the manager path, so the
// signatures live next to the batch ABI rather than at each call site.
sol! {
    /// Public getter over `mapping(address => ProxyInfo) authorizedProxies`.
    /// Solidity flattens the struct into its three members.
    function authorizedProxies(address proxy)
        external
        view
        returns (bool isProxy, address originalAddress, uint64 originalRollupId);

    /// Permissionless CREATE2 deployment + registration of a remote address's proxy.
    function createCrossChainProxy(address originalAddress, uint64 originalRollupId)
        external
        returns (address proxy);

    /// Deterministic CREATE2 address of `originalAddress`'s proxy on this chain.
    function computeCrossChainProxyAddress(address originalAddress, uint64 originalRollupId)
        external
        view
        returns (address);

    /// `CrossChainProxy.executeOnBehalf` — forwards `value` + `data` to
    /// `destination` when the caller is the manager.
    function executeOnBehalf(address destination, uint64 callGas, bytes data) external payable;
}

#[cfg(test)]
mod selector_locks {
    //! ABI pins from `eez-core-protocol` commit 6fcc90b.
    use super::*;
    use alloy_sol_types::SolCall;

    #[test]
    fn manager_and_proxy_selectors_match_upstream() {
        assert_eq!(
            executeOnBehalfCall::SELECTOR,
            [0x82, 0x05, 0xf3, 0xe1],
            "CrossChainProxy.executeOnBehalf selector drifted from pinned protocol"
        );
        assert_eq!(
            createCrossChainProxyCall::SELECTOR,
            [0xa7, 0x58, 0x7c, 0x62],
            "createCrossChainProxy selector drifted from pinned protocol"
        );
        assert_eq!(
            computeCrossChainProxyAddressCall::SELECTOR,
            [0xeb, 0x20, 0xc0, 0xaa],
            "computeCrossChainProxyAddress selector drifted from pinned protocol"
        );
        assert_eq!(
            authorizedProxiesCall::SELECTOR,
            [0x36, 0x0d, 0x95, 0xb6],
            "authorizedProxies getter selector drifted from pinned protocol"
        );
    }

    #[test]
    fn l2_selectors_match_upstream() {
        assert_eq!(
            loadExecutionTableCall::SELECTOR,
            [0xb3, 0x01, 0xbc, 0x80],
            "loadExecutionTable selector drifted from pinned protocol"
        );
        assert_eq!(
            executeIncomingCrossChainCallCall::SELECTOR,
            [0x8d, 0x84, 0x61, 0xd9],
            "executeIncomingCrossChainCall selector drifted from pinned protocol"
        );
    }

    #[test]
    fn l1_selectors_match_upstream() {
        assert_eq!(
            postAndVerifyBatchCall::SELECTOR,
            [0xca, 0xfe, 0xf1, 0x25],
            "postAndVerifyBatch selector drifted from pinned protocol"
        );
        assert_eq!(
            executeL2TxsCall::SELECTOR,
            [0xdc, 0x6d, 0x11, 0xfa],
            "executeL2Txs selector drifted from pinned protocol"
        );
        assert_eq!(
            staticCrossChainCallCall::SELECTOR,
            [0x31, 0x34, 0x4a, 0xde],
            "staticCrossChainCall selector drifted from pinned protocol"
        );
    }
}
