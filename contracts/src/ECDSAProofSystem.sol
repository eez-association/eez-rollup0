// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {IProofSystem} from "sync-rollups-protocol/src/interfaces/IProofSystem.sol";

/// @title ECDSAProofSystem
/// @notice `IProofSystem` that binds to the per-batch `publicInputsHash`: a
///         valid proof is a 65-byte ECDSA signature over the `publicInputsHash`
///         the EEZ dispatcher recomputes on-chain, by the authorized attester.
///
/// @dev    This is the real counterpart of `MockECDSAProofSystem` (which ignores
///         the hash and recovers a fixed digest). Here the off-chain attester —
///         `eez-proverd`, after stateless re-execution + the settlement gates —
///         signs the recomputed `publicInputsHash`; `verify` recovers against
///         that same hash, so the proof is BOUND to the batch. Sound only when
///         the attester signs a hash it actually re-executed (proverd enforces
///         this: validator mandatory, sign only if every gate passes).
///
///         Proof shape: `abi.encodePacked(r, s, v)` — 32 + 32 + 1 = 65 bytes.
///
///         `vkey = bytes32(uint256(uint160(signer)))` is the registry-membership
///         ticket; the attester address lives on this contract's `signer()` view.
contract ECDSAProofSystem is IProofSystem {
    /// @notice Authorized attester. ECDSA recovery against the batch's
    ///         `publicInputsHash` must equal this value.
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
    /// @dev Recovers against `publicInputsHash` (the batch binding). Returns
    ///      false on malformed proofs (wrong length, bad signature) without
    ///      reverting, so the EEZ quorum surfaces `InvalidProof` uniformly.
    function verify(bytes calldata proof, bytes32 publicInputsHash)
        external
        view
        returns (bool valid)
    {
        if (proof.length != 65) return false;

        bytes32 r;
        bytes32 s;
        uint8 v;
        // Decode `abi.encodePacked(r, s, v)` — three fixed-width fields
        // concatenated with no padding. `proof.offset` already accounts for the
        // bytes-array length prefix.
        assembly {
            r := calldataload(proof.offset)
            s := calldataload(add(proof.offset, 32))
            v := byte(0, calldataload(add(proof.offset, 64)))
        }

        // Reject high-`s` (malleable) signatures: EIP-2 canonical form requires
        // `s <= secp256k1_n/2`. The off-chain signer already emits low-`s` (see
        // eez-evm/src/signer.rs, which documents this verifier enforcing it), so
        // a malleated twin of a valid attestation must not also verify. Constant
        // is secp256k1_n/2 (OpenZeppelin ECDSA `_HALF_N`).
        if (uint256(s) > 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF5D576E7357A4501DDFE92F46681B20A0) {
            return false;
        }

        // `ecrecover` returns address(0) on any malformed (r, s, v); `signer`
        // is non-zero per the constructor revert, so those are all rejected.
        address recovered = ecrecover(publicInputsHash, v, r, s);
        return recovered == signer;
    }
}
