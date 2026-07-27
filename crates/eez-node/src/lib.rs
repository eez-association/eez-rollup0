//! eez Rollup-0 node — the application library behind the `eez-node` binary.
//!
//! Deployment/trust boundaries are the only crate boundaries in this
//! workspace: `eez-protocol` is the vocabulary kernel both sides agree
//! on byte-for-byte, `eez-proverd` is the prover side, and everything
//! the node process runs lives here as plain modules:
//!
//! - [`composer`] — Composer umbrella: per-rollup Sequencer/Scheduler/
//!   HeldPool maps plus the shared Aggregator/Submitter/prover client,
//!   and the local reth-backed chain client + execution sessions.
//! - [`driver`] — Engine-API consumer that drives reth to produce
//!   blocks on a fixed cadence (Sequencer, BlockCommitter, slots).
//! - [`deriver`] — Consumes L1 events, decodes posted batches, keeps
//!   L2 in sync with L1.
//! - [`l1`] — L1 RPC client + Submitter that posts batches to
//!   BatchPoster.sol.
//! - [`inspector`] — revm Inspector for cross-chain proxy-call
//!   detection during source simulation.
//! - Node plumbing: [`bundle_rpc`], [`follower`], [`ingress`],
//!   [`l1_embedded`], [`mock_prover`], [`payload`], [`witness_source`].

/// Concrete reth provider of the eez L2 node and the embedded dev L1
/// (both launched as `EthereumNode` over mdbx).
pub type EthNodeProvider = reth_provider::providers::BlockchainProvider<
    reth_node_builder::NodeTypesWithDBAdapter<
        reth_node_ethereum::EthereumNode,
        reth_db::DatabaseEnv,
    >,
>;

/// Concrete reth provider of the embedded chiado L1
/// (`reth_gnosis::GnosisNode` over mdbx).
pub type ChiadoNodeProvider = reth_provider::providers::BlockchainProvider<
    reth_node_builder::NodeTypesWithDBAdapter<reth_gnosis::GnosisNode, reth_db::DatabaseEnv>,
>;

pub mod bundle_rpc;
pub mod composer;
pub mod deriver;
pub mod driver;
pub mod follower;
pub mod ingress;
pub mod inspector;
pub mod l1;
pub mod l1_embedded;
pub mod mock_prover;
pub mod payload;
pub mod witness_source;
