//! eez-proverd library: the `prove.v1` wire contract ([`control_rpc`]),
//! the composer-side client ([`client`]), and the fail-closed settlement
//! gates ([`gates`]) behind the daemon binary.
//!
//! Client and server versions of the wire live in one crate so both ends
//! of the `Prove` RPC always agree; `eez-node` depends on this library
//! for [`client::RemoteProver`] and the generated `prove.v1` types.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod client;
pub mod control_rpc;
pub mod gates;

// Used by the daemon binary (src/main.rs), not the library.
use clap as _;
use tracing_subscriber as _;
