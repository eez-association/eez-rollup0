// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {IProofSystem} from "sync-rollups-protocol/src/interfaces/IProofSystem.sol";

/// @title MockECDSAProofSystem
/// @notice Devnet-only `IProofSystem` that accepts any 65-byte ECDSA signature
///         on a fixed digest, signed by an authorized signer set at deploy
///         time. **Not a real proof system — does NOT bind to the batch.**
///
/// @dev    `publicInputsHash` is ignored. The contract recovers the signer
///         against `MOCK_PROVER_DIGEST` regardless of what the EEZ multi-prover
///         dispatcher passes in. The corresponding off-chain `MockEcdsaProver`
///         in `eez-prover` signs the same digest. A real prover (zk or
///         stateless-execution ECDSA) replaces this contract with one that
///         binds to a per-batch `publicInputsHash`.
///
///         Proof shape: `abi.encodePacked(r, s, v)` — 32 + 32 + 1 = 65 bytes.
///
///         `vkey` is the registry-membership ticket only: the per-rollup
///         `IRollupContract` records `vkey = bytes32(uint256(uint160(signer)))`
///         to mark this PS as accepted; the signer address itself lives on
///         this contract's `signer()` view.
contract MockECDSAProofSystem is IProofSystem {
    /// @notice Fixed digest that valid mock proofs are signatures over.
    ///         Equals `keccak256("eez-mock-prover")`. The off-chain
    ///         `MockEcdsaProver` MUST sign exactly this value or `verify`
    ///         returns false.
    bytes32 public constant MOCK_PROVER_DIGEST =
        0x02753eb401fed50317a35a1cfa1c67c003b761ba4009cbe36632c724ef0a06df;

    /// @notice Authorized signer address. ECDSA recovery against
    ///         `MOCK_PROVER_DIGEST` must equal this value.
    address public immutable signer;

    /// @notice Emitted at deployment so off-chain tooling can capture the
    ///         signer without ABI-decoding the constructor args.
    event SignerSet(address indexed signer);

    /// @notice Reverts on the zero address — silent success against
    ///         `address(0)` would happen if `ecrecover` returned 0 on a
    ///         malformed signature.
    error SignerCannotBeZero();

    constructor(address initialSigner) {
        if (initialSigner == address(0)) revert SignerCannotBeZero();
        signer = initialSigner;
        emit SignerSet(initialSigner);
    }

    /// @inheritdoc IProofSystem
    /// @dev Ignores `publicInputsHash` — this is a mock. Returns false on
    ///      malformed proofs (wrong length, bad signature) without
    ///      reverting, so the EEZ quorum surfaces `InvalidProof` uniformly.
    function verify(bytes calldata proof, bytes32 /* publicInputsHash */)
        external
        view
        returns (bool valid)
    {
        if (proof.length != 65) return false;

        bytes32 r;
        bytes32 s;
        uint8 v;
        // Decode `abi.encodePacked(r, s, v)` — three fixed-width fields
        // concatenated with no padding. `calldataload` reads 32 bytes at the
        // given offset; `proof.offset` already accounts for the bytes-array
        // length prefix.
        assembly {
            r := calldataload(proof.offset)
            s := calldataload(add(proof.offset, 32))
            v := byte(0, calldataload(add(proof.offset, 64)))
        }

        // `ecrecover` returns address(0) on any malformed (r, s, v) — bad
        // recovery id, non-canonical s, etc. `signer` is non-zero per the
        // constructor revert, so the equality below rejects all of those.
        address recovered = ecrecover(MOCK_PROVER_DIGEST, v, r, s);
        return recovered == signer;
    }
}
