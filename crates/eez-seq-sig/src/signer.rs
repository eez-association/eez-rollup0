use alloy_consensus::Header;
use alloy_primitives::{Address, Bytes};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use reth_primitives_traits::SealedHeader;

use crate::{Envelope, HeadClaim, HeadDomain};

/// Signs head claims with a strictly increasing `(epoch_ms, counter)`.
pub struct HeadSigner {
    key: PrivateKeySigner,
    domain: HeadDomain,
    epoch_ms: u64,
    counter: u64,
}

impl std::fmt::Debug for HeadSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeadSigner")
            .field("address", &self.key.address())
            .field("epoch_ms", &self.epoch_ms)
            .field("counter", &self.counter)
            .finish_non_exhaustive()
    }
}

impl HeadSigner {
    pub fn new(key: PrivateKeySigner, domain: HeadDomain, epoch_ms: u64) -> Self {
        Self {
            key,
            domain,
            epoch_ms,
            counter: 0,
        }
    }

    pub fn address(&self) -> Address {
        self.key.address()
    }

    /// Increments the counter first, so the first envelope carries counter 1.
    pub fn sign(
        &mut self,
        header: &SealedHeader<Header>,
        block_rlp: Bytes,
    ) -> Result<Envelope, alloy_signer::Error> {
        self.counter += 1;
        let head = HeadClaim {
            rollup_id: self.domain.rollup_id,
            number: header.number,
            block_hash: header.hash(),
            parent_hash: header.parent_hash,
            epoch_ms: self.epoch_ms,
            counter: self.counter,
        };
        let sig = self.key.sign_hash_sync(&head.signing_hash(&self.domain))?;
        Ok(Envelope {
            head,
            signature: Bytes::from(sig.as_bytes().to_vec()),
            block: Some(block_rlp),
        })
    }
}
