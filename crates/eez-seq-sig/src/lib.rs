//! Sequencer-signed head claims, transport-agnostic.

mod envelope;
mod head;
mod signer;
mod tracker;

pub use envelope::{Envelope, VerifiedHead, VerifyError, decode_block};
pub use head::{HeadClaim, HeadDomain, Version};
pub use signer::{HeadSigner, SignError};
pub use tracker::{HeadTracker, MAX_FUTURE_MS, MAX_SOURCES, Observation, Source};
