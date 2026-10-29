use libp2p::{
    PeerId,
    identity::{self, Keypair, secp256k1},
};

#[derive(Debug, thiserror::Error)]
#[error("invalid secp256k1 key: {0}")]
pub struct IdentityError(String);

/// The libp2p identity of a node, from its secp256k1 node key.
pub fn keypair(secret: &[u8; 32]) -> Result<Keypair, IdentityError> {
    let mut bytes = *secret;
    let sk = secp256k1::SecretKey::try_from_bytes(&mut bytes)
        .map_err(|e| IdentityError(e.to_string()))?;
    Ok(Keypair::from(secp256k1::Keypair::from(sk)))
}

/// The peer id of a node from its compressed public key, as carried in an ENR.
pub fn peer_id(compressed_pubkey: &[u8; 33]) -> Result<PeerId, IdentityError> {
    let pk = secp256k1::PublicKey::try_from_bytes(compressed_pubkey)
        .map_err(|e| IdentityError(e.to_string()))?;
    Ok(identity::PublicKey::from(pk).to_peer_id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer_local::PrivateKeySigner;

    #[test]
    fn peer_id_matches_keypair() {
        let secret = [7u8; 32];
        let signer = PrivateKeySigner::from_slice(&secret).unwrap();
        let compressed: [u8; 33] = signer
            .credential()
            .verifying_key()
            .to_encoded_point(true)
            .as_bytes()
            .try_into()
            .unwrap();
        assert_eq!(
            keypair(&secret).unwrap().public().to_peer_id(),
            peer_id(&compressed).unwrap()
        );
    }
}
