// SPDX-License-Identifier: MIT
pragma solidity 0.8.34;

import {IEEZRegistry} from "eez-core-protocol/src/interfaces/IEEZRegistry.sol";
import {IRollupContract} from "eez-core-protocol/src/interfaces/IRollup.sol";
import {OwnableUpgradeable} from "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";

/// @title Rollup0Manager
/// @notice Rollup0 proof policy, blob-settlement domain, and settlement-session gate.
contract Rollup0Manager is IRollupContract, OwnableUpgradeable {
    bytes32 public constant CANDIDATE_DOMAIN_TAG = 0xba3ba53e31274af0c3f0f70c908597cbb1bc9a6a33006333feea258597f384b7;

    address public immutable EEZContract;
    uint256 public immutable rollup0ChainId;
    uint64 public rollupId;
    uint256 public activeProofSystemCount;
    mapping(address proofSystem => bytes32 vkey) public verificationKey;
    address public activeSettlementWrapper;
    uint256 public lastSettlementBlock;
    bool private transient _settlementActive;

    event ProofSystemAdded(address indexed proofSystem, bytes32 verificationKey);
    event ProofSystemRemoved(address indexed proofSystem);
    event VerificationKeyUpdated(address indexed proofSystem, bytes32 newVerificationKey);
    event ThresholdChanged(uint256 newThreshold);
    event RootEscape(bytes32 newRoot);
    event SettlementWrapperChanged(address indexed previousWrapper, address indexed newWrapper);

    error NotEEZRegistry();
    error InvalidEEZContract();
    error InvalidRollup0ChainId();
    error InvalidConfig();
    error AlreadyRegistered();
    error UnauthorizedRegistrantAccount(address registrant);
    error ProofSystemAlreadyAllowed(address proofSystem);
    error ProofSystemNotAllowed(address proofSystem);
    error ProofSystemNotAdded(address proofSystem);
    error ThresholdNotMet(uint256 submitted, uint256 required);
    error InvalidSettlementWrapper();
    error NotActiveSettlementWrapper();
    error SettlementAlreadyActive();
    error SettlementNotActive();
    error SettlementAlreadyAttemptedThisBlock();
    error UnsupportedCandidateBlockNumber(uint64 blockNumber);
    error UnauthorizedCustomDataRead();

    constructor(address eez, uint256 chainId) {
        if (eez == address(0)) revert InvalidEEZContract();
        if (chainId == 0) revert InvalidRollup0ChainId();
        EEZContract = eez;
        rollup0ChainId = chainId;
        _disableInitializers();
    }

    function initialize(
        address initialOwner,
        address[] memory proofSystems,
        bytes32[] memory vkeys
    )
        external
        initializer
    {
        __Ownable_init(initialOwner);
        if (proofSystems.length != vkeys.length) revert InvalidConfig();
        for (uint256 i = 0; i < proofSystems.length; i++) {
            _addProofSystem(proofSystems[i], vkeys[i]);
        }
        if (activeProofSystemCount == 0) revert InvalidConfig();
    }

    function checkProofSystemsAndGetVkeys(address[] calldata proofSystems)
        external
        view
        returns (bytes32[] memory vkeys)
    {
        uint256 required = threshold();
        if (proofSystems.length < required) revert ThresholdNotMet(proofSystems.length, required);
        vkeys = new bytes32[](proofSystems.length);
        address previous;
        for (uint256 i = 0; i < proofSystems.length; i++) {
            address proofSystem = proofSystems[i];
            if (uint160(proofSystem) <= uint160(previous)) revert ProofSystemNotAllowed(proofSystem);
            bytes32 vkey = verificationKey[proofSystem];
            if (vkey == bytes32(0)) revert ProofSystemNotAllowed(proofSystem);
            vkeys[i] = vkey;
            previous = proofSystem;
        }
    }

    function rollupContractRegistered(uint64 assignedRollupId, address registrant) external {
        if (msg.sender != EEZContract) revert NotEEZRegistry();
        if (rollupId != 0) revert AlreadyRegistered();
        if (registrant != owner()) revert UnauthorizedRegistrantAccount(registrant);
        rollupId = assignedRollupId;
    }

    function setSettlementWrapper(address wrapper) external onlyOwner {
        if (wrapper == address(0)) revert InvalidSettlementWrapper();
        if (_settlementActive) revert SettlementAlreadyActive();
        address previous = activeSettlementWrapper;
        activeSettlementWrapper = wrapper;
        emit SettlementWrapperChanged(previous, wrapper);
    }

    function beginSettlement() external {
        if (msg.sender != activeSettlementWrapper) revert NotActiveSettlementWrapper();
        if (_settlementActive) revert SettlementAlreadyActive();
        if (lastSettlementBlock == block.number) revert SettlementAlreadyAttemptedThisBlock();
        lastSettlementBlock = block.number;
        _settlementActive = true;
    }

    function endSettlement() external {
        if (msg.sender != activeSettlementWrapper) revert NotActiveSettlementWrapper();
        if (!_settlementActive) revert SettlementNotActive();
        _settlementActive = false;
    }

    function getCustomData(uint64 blockNumber) external view returns (bytes memory customData) {
        if (msg.sender != EEZContract || !_settlementActive) revert UnauthorizedCustomDataRead();
        if (blockNumber != type(uint64).max) revert UnsupportedCandidateBlockNumber(blockNumber);
        return abi.encode(
            CANDIDATE_DOMAIN_TAG,
            block.chainid,
            EEZContract,
            address(this),
            activeSettlementWrapper,
            uint256(rollupId),
            rollup0ChainId,
            block.timestamp,
            blockhash(block.number - 1)
        );
    }

    function addProofSystem(address proofSystem, bytes32 vkey) external onlyOwner {
        _addProofSystem(proofSystem, vkey);
    }

    function removeProofSystem(address proofSystem) external onlyOwner {
        if (verificationKey[proofSystem] == bytes32(0)) revert ProofSystemNotAdded(proofSystem);
        delete verificationKey[proofSystem];
        activeProofSystemCount--;
        emit ProofSystemRemoved(proofSystem);
        emit ThresholdChanged(threshold());
    }

    function updateVerificationKey(address proofSystem, bytes32 newVkey) external onlyOwner {
        if (newVkey == bytes32(0)) revert InvalidConfig();
        if (verificationKey[proofSystem] == bytes32(0)) revert ProofSystemNotAdded(proofSystem);
        verificationKey[proofSystem] = newVkey;
        emit VerificationKeyUpdated(proofSystem, newVkey);
    }

    function setRoot(bytes32 newRoot) external onlyOwner {
        IEEZRegistry(EEZContract).setRoot(rollupId, newRoot);
        emit RootEscape(newRoot);
    }

    function _addProofSystem(address proofSystem, bytes32 vkey) private {
        if (proofSystem == address(0) || vkey == bytes32(0)) revert InvalidConfig();
        if (verificationKey[proofSystem] != bytes32(0)) revert ProofSystemAlreadyAllowed(proofSystem);
        verificationKey[proofSystem] = vkey;
        activeProofSystemCount++;
        emit ProofSystemAdded(proofSystem, vkey);
        emit ThresholdChanged(threshold());
    }

    /// @notice Quorum derived from the active proof-system set.
    function threshold() public view returns (uint256) {
        return (2 * activeProofSystemCount) / 3 + 1;
    }
}
