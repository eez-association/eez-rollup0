//! eez Rollup-0 node — the application library behind the `eez-node` binary.
//!
//! Everything the node process runs lives here as plain modules; the only
//! other crates are `eez-protocol` (the vocabulary both sides of the trust
//! boundary share) and `eez-proverd` (the prover side of the wire).

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
