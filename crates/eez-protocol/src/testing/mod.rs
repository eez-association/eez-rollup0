//! Shared test doubles.
//!
//! Gated by the `testing` Cargo feature so library consumers never
//! pull the fake into their production binary. Test code opts in via
//! `eez-protocol = { ..., features = ["testing"] }` in
//! `[dev-dependencies]`.
//!
//! This module is also visible in this crate's own `#[cfg(test)]` code
//! (no feature flag needed internally).
//!
//! # What lives here
//!
//! - [`FakeChainClient`] — one canonical fake that implements both
//!   [`ChainClient`](crate::ChainClient) and
//!   [`EntryChainClient`](crate::EntryChainClient). Shared across
//!   every test module that needs a non-network client double.
//! - [`FakeChainSession`] — companion session that replays scripted
//!   outcomes; used by the fake's `begin_execution_session`.

mod fake_client;

pub use fake_client::{FakeChainClient, FakeChainSession, FakeRole};
