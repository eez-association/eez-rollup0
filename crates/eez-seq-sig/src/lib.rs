//! Sequencer-signed head claims, transport-agnostic.

mod envelope;
mod head;
mod signer;

pub use envelope::{Envelope, VerifiedHead, VerifyError, decode_block};
pub use head::{HeadClaim, HeadDomain, Version};
pub use signer::{HeadSigner, SignError};
