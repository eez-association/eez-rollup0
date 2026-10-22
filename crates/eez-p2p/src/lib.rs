//! Gossip of sequencer-signed head envelopes over libp2p.

pub mod identity;
pub mod overlay;
pub mod validate;
pub mod wire;

pub use identity::{IdentityError, keypair, peer_id};
pub use overlay::{Discover, Overlay, P2pConfig, listen_port_from_env, topic};
pub use validate::{Validator, Verdict};
pub use wire::{MAX_MESSAGE_SIZE, WireError, decode, encode};
