use alloy_primitives::Address;
use eez_seq_sig::{HeadDomain, HeadTracker, Observation, VerifiedHead};

use crate::wire;

/// Gossip verdict for one received message.
#[derive(Debug)]
pub enum Verdict {
    /// Newer than its source's latest head; forward it.
    Accept(Box<VerifiedHead>),
    /// Already seen, stale or future: harmless but not worth forwarding.
    Ignore,
    /// Invalid; not forwarded. v0 does not score peers.
    Reject(String),
}

/// Decodes, verifies and de-duplicates gossiped envelopes.
#[derive(Debug)]
pub struct Validator {
    domain: HeadDomain,
    authorized: Address,
    tracker: HeadTracker,
}

impl Validator {
    pub fn new(domain: HeadDomain, authorized: Address) -> Self {
        Self {
            domain,
            authorized,
            tracker: HeadTracker::default(),
        }
    }

    pub fn check(&mut self, data: &[u8], now_ms: u64) -> Verdict {
        let envelope = match wire::decode(data) {
            Ok(e) => e,
            Err(e) => return Verdict::Reject(e.to_string()),
        };
        let verified = match envelope.verify(&self.domain, self.authorized) {
            Ok(v) => v,
            Err(e) => return Verdict::Reject(e.to_string()),
        };
        match self.tracker.observe(verified.clone(), now_ms) {
            Observation::New => Verdict::Accept(Box::new(verified)),
            // A seen head is ignored however it is encoded: Snappy has many encodings
            // of one payload, and relaying each would let a peer flood the mesh.
            Observation::Duplicate | Observation::Stale | Observation::Future => Verdict::Ignore,
            Observation::Equivocation { kept } => {
                Verdict::Reject(format!("equivocation, kept {kept}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B256, Bytes};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use eez_seq_sig::{Envelope, HeadClaim};

    const DOMAIN: HeadDomain = HeadDomain {
        l2_chain_id: 1337,
        registry: Address::repeat_byte(0xaa),
        rollup_id: 1,
    };

    fn key(n: u8) -> PrivateKeySigner {
        PrivateKeySigner::from_slice(&[n; 32]).unwrap()
    }

    fn msg(key: &PrivateKeySigner, epoch_ms: u64, counter: u64, hash: u8) -> Vec<u8> {
        let head = HeadClaim {
            rollup_id: 1,
            number: 1,
            block_hash: B256::repeat_byte(hash),
            parent_hash: B256::ZERO,
            epoch_ms,
            counter,
        };
        let sig = key.sign_hash_sync(&head.signing_hash(&DOMAIN)).unwrap();
        let envelope = Envelope {
            head,
            signature: Bytes::from(sig.as_bytes().to_vec()),
            block: None,
        };
        wire::encode(&envelope).unwrap()
    }

    /// The same payload as `data`, Snappy-encoded as one-byte literals.
    fn reencode(data: &[u8]) -> Vec<u8> {
        let raw = snap::raw::Decoder::new().decompress_vec(data).unwrap();
        let mut out = Vec::new();
        let mut len = raw.len();
        while len >= 0x80 {
            out.push((len as u8) | 0x80);
            len >>= 7;
        }
        out.push(len as u8);
        for byte in raw {
            // Tag 0 is a literal of length 1.
            out.extend([0, byte]);
        }
        out
    }

    #[test]
    fn garbage_is_rejected() {
        let mut v = Validator::new(DOMAIN, key(1).address());
        assert!(matches!(v.check(b"junk", 0), Verdict::Reject(_)));
    }

    #[test]
    fn validator_verdicts() {
        let (k0, k1) = (key(1), key(2));
        let mut v = Validator::new(DOMAIN, k0.address());
        let first = msg(&k0, 100, 1, 1);
        assert!(matches!(v.check(&first, 1_000), Verdict::Accept(_)));
        assert!(matches!(v.check(&first, 1_000), Verdict::Ignore));
        let copy = reencode(&first);
        assert_ne!(copy, first);
        assert!(matches!(v.check(&copy, 1_000), Verdict::Ignore));
        assert!(matches!(
            v.check(&msg(&k0, 100, 0, 1), 1_000),
            Verdict::Ignore
        ));
        assert!(matches!(
            v.check(&msg(&k0, 20_000, 1, 1), 1_000),
            Verdict::Ignore
        ));
        assert!(matches!(
            v.check(&msg(&k1, 100, 2, 1), 1_000),
            Verdict::Reject(_)
        ));
        assert!(matches!(
            v.check(&msg(&k0, 100, 1, 2), 1_000),
            Verdict::Reject(_)
        ));
    }
}
