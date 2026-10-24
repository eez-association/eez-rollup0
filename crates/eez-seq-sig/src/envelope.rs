use alloy_primitives::{Address, Bytes};
use alloy_rlp::Decodable;
use alloy_signer::Signature;
use reth_primitives_traits::SealedBlock;

use crate::{HeadClaim, HeadDomain};

/// A claim, its 65-byte `r‖s‖v` signature and optionally the consensus-RLP block.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Envelope {
    pub head: HeadClaim,
    pub signature: Bytes,
    pub block: Option<Bytes>,
}

#[derive(Clone, Debug)]
pub struct VerifiedHead {
    pub claim: HeadClaim,
    pub signer: Address,
    pub block: Option<SealedBlock<eez_primitives::Block>>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("wrong rollup: expected {expected}, got {got}")]
    WrongRollup { expected: u64, got: u64 },
    #[error("bad signature: {0}")]
    BadSignature(String),
    #[error("unauthorized signer {0}")]
    UnauthorizedSigner(Address),
    #[error("block decode failed: {0}")]
    BlockDecode(String),
    #[error("block {0} does not match claim")]
    BlockMismatch(&'static str),
}

pub fn decode_block(rlp: &[u8]) -> Result<SealedBlock<eez_primitives::Block>, VerifyError> {
    let mut buf = rlp;
    let block = eez_primitives::Block::decode(&mut buf)
        .map_err(|e| VerifyError::BlockDecode(e.to_string()))?;
    if !buf.is_empty() {
        return Err(VerifyError::BlockDecode("trailing bytes".into()));
    }
    Ok(SealedBlock::seal_slow(block))
}

impl Envelope {
    pub fn verify(
        self,
        domain: &HeadDomain,
        authorized: Address,
    ) -> Result<VerifiedHead, VerifyError> {
        let Self {
            head,
            signature,
            block,
        } = self;
        if head.rollup_id != domain.rollup_id {
            return Err(VerifyError::WrongRollup {
                expected: domain.rollup_id,
                got: head.rollup_id,
            });
        }
        let sig = Signature::try_from(signature.as_ref())
            .map_err(|e| VerifyError::BadSignature(e.to_string()))?;
        let signer = sig
            .recover_address_from_prehash(&head.signing_hash(domain))
            .map_err(|e| VerifyError::BadSignature(e.to_string()))?;
        if signer != authorized {
            return Err(VerifyError::UnauthorizedSigner(signer));
        }
        let block = block.map(|rlp| decode_block(&rlp)).transpose()?;
        if let Some(b) = &block {
            if b.hash() != head.block_hash {
                return Err(VerifyError::BlockMismatch("hash"));
            }
            if b.parent_hash != head.parent_hash {
                return Err(VerifyError::BlockMismatch("parent"));
            }
            if b.number != head.number {
                return Err(VerifyError::BlockMismatch("number"));
            }
        }
        Ok(VerifiedHead {
            claim: head,
            signer,
            block,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeadSigner;
    use alloy_consensus::Header;
    use alloy_primitives::{B256, address, b256, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use alloy_sol_types::SolValue;
    use reth_primitives_traits::SealedHeader;

    const KEY: B256 = b256!("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80");

    fn domain() -> HeadDomain {
        HeadDomain {
            l2_chain_id: 1337,
            registry: address!("00000000000000000000000000000000000000aa"),
            rollup_id: 1,
        }
    }

    fn signer() -> HeadSigner {
        HeadSigner::new(
            PrivateKeySigner::from_bytes(&KEY).unwrap(),
            domain(),
            1_700_000_000_000,
        )
    }

    fn block(number: u64) -> (SealedHeader<Header>, Bytes) {
        let b = eez_primitives::Block {
            header: Header {
                number,
                parent_hash: B256::repeat_byte(0x11),
                ..Default::default()
            },
            body: Default::default(),
        };
        let sealed = SealedBlock::seal_slow(b);
        (
            sealed.clone_sealed_header(),
            Bytes::from(alloy_rlp::encode(sealed.into_block())),
        )
    }

    #[test]
    fn sign_then_verify_round_trips() {
        let mut s = signer();
        let (header, rlp) = block(7);
        let env = s.sign(&header, rlp.clone()).unwrap();
        assert_eq!(env.head.counter, 1);
        let claim = env.head.clone();
        let v = env.verify(&domain(), s.address()).unwrap();
        assert_eq!(v.claim, claim);
        assert_eq!(v.signer, s.address());
        assert_eq!(v.block.unwrap().hash(), header.hash());
        assert_eq!(s.sign(&header, rlp).unwrap().head.counter, 2);
    }

    #[test]
    fn wrong_authorized_address_is_rejected() {
        let mut s = signer();
        let (header, rlp) = block(7);
        let env = s.sign(&header, rlp).unwrap();
        let err = env.verify(&domain(), Address::repeat_byte(9)).unwrap_err();
        assert_eq!(err, VerifyError::UnauthorizedSigner(s.address()));
    }

    #[test]
    fn tampered_claim_is_rejected() {
        let mut s = signer();
        let (header, rlp) = block(7);
        let mut env = s.sign(&header, rlp).unwrap();
        env.head.number += 1;
        assert!(matches!(
            env.verify(&domain(), s.address()),
            Err(VerifyError::UnauthorizedSigner(_))
        ));
    }

    #[test]
    fn wrong_rollup_is_rejected() {
        let mut s = signer();
        let (header, rlp) = block(7);
        let env = s.sign(&header, rlp).unwrap();
        let other = HeadDomain {
            rollup_id: 2,
            ..domain()
        };
        assert_eq!(
            env.verify(&other, s.address()).unwrap_err(),
            VerifyError::WrongRollup {
                expected: 2,
                got: 1
            }
        );
    }

    #[test]
    fn block_hash_mismatch_is_rejected() {
        let mut s = signer();
        let (header_a, _) = block(7);
        let (_, rlp_b) = block(8);
        let env = s.sign(&header_a, rlp_b).unwrap();
        assert_eq!(
            env.verify(&domain(), s.address()).unwrap_err(),
            VerifyError::BlockMismatch("hash")
        );
    }

    #[test]
    fn signing_hash_matches_manual_eip712() {
        let mut s = signer();
        let (header, rlp) = block(7);
        let c = s.sign(&header, rlp).unwrap().head;
        let d = domain();

        let domain_type = keccak256(
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let separator = keccak256(
            (
                domain_type,
                keccak256("EEZ Sequencer"),
                keccak256("0"),
                alloy_primitives::U256::from(d.l2_chain_id),
                d.registry,
            )
                .abi_encode(),
        );
        let typehash = keccak256(
            "SignedHead(uint64 rollupId,uint64 number,bytes32 blockHash,bytes32 parentHash,uint64 epochMs,uint64 counter)",
        );
        let struct_hash = keccak256(
            (
                typehash,
                c.rollup_id,
                c.number,
                c.block_hash,
                c.parent_hash,
                c.epoch_ms,
                c.counter,
            )
                .abi_encode(),
        );
        let mut buf = vec![0x19, 0x01];
        buf.extend_from_slice(separator.as_slice());
        buf.extend_from_slice(struct_hash.as_slice());
        assert_eq!(c.signing_hash(&d), keccak256(buf));
    }

    #[test]
    fn envelope_json_round_trips() {
        let mut s = signer();
        let (header, rlp) = block(7);
        let env = s.sign(&header, rlp).unwrap();
        let json = serde_json::to_string(&env).unwrap();
        assert!(json.contains("\"blockHash\""));
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), env);
    }
}
