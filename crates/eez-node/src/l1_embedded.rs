//! Embedded L1 reth — config builder for the second `NodeBuilder` that
//! composer mode launches alongside the L2 reth. Three modes:
//!   - **Dev** (vanilla EthereumNode, 5s auto-mine) — local smokes;
//!     fast, deterministic, no beacon dependency. `EEZ_L1_CHAIN=dev`.
//!   - **Devnet** (vanilla EthereumNode, CL-driven) — private PoS
//!     devnet on a generated genesis, driven via engine-API by an
//!     external lighthouse CL (beacon + validator). Real fork-choice,
//!     so it can host MEV/rbuilder + reorg testing. `EEZ_L1_CHAIN=devnet`.
//!   - **Chiado** (reth_gnosis::GnosisNode) — real chiado state from the
//!     mounted datadir, driven via engine-API by an external lighthouse
//!     CL. `EEZ_L1_CHAIN=chiado`.
//!
//! The `NodeBuilder` launch is inline in [`crate::main`] — the
//! nested-generic `NodeHandle` AddOns types resist a typed helper.

use std::{path::PathBuf, sync::Arc};

use eyre::Result;
use reth_chainspec::ChainSpec;
use reth_gnosis::spec::{chains::CHIADO_GENESIS, gnosis_spec::GnosisChainSpec};
use reth_node_core::{
    args::{DatadirArgs, NetworkArgs, RpcServerArgs},
    node_config::NodeConfig,
};

/// Block time for the dev embedded L1. 5s gives the composer a wide
/// window to land `postBatch + user_tx` in the same L1 block (invariant
/// 4's atomicity requirement); a 1s window lets dev-reth's auto-mine
/// split the two `send_raw_transaction` roundtrips across blocks.
pub const DEV_L1_BLOCK_TIME: std::time::Duration = std::time::Duration::from_secs(5);

/// Which L1 chain the embedded L1 NodeBuilder should serve. Selected
/// by `EEZ_L1_CHAIN` env at startup; defaults to `Dev` for backwards-
/// compatibility with existing smokes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L1ChainKind {
    /// Vanilla `EthereumNode` in dev mode (5s auto-mine, --chain dev).
    Dev,
    /// `reth_gnosis::GnosisNode` loading the chiado preset; no
    /// auto-mine — engine API driven by an external lighthouse CL.
    Chiado,
    /// Vanilla `EthereumNode` on a private PoS devnet genesis; no
    /// auto-mine — engine API driven by an external lighthouse CL
    /// (beacon + validator).
    Devnet,
}

impl L1ChainKind {
    pub fn from_env() -> Self {
        match std::env::var("EEZ_L1_CHAIN").as_deref() {
            Ok("chiado") => Self::Chiado,
            Ok("devnet") => Self::Devnet,
            _ => Self::Dev,
        }
    }

    /// Short label for structured-logging `kind` fields. Single source
    /// of truth for `main.rs`'s embedded-L1 launch events.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Chiado => "chiado",
            Self::Devnet => "devnet",
        }
    }
}

/// Config knobs for the embedded L1 launch. All read from env in
/// `eez-node::main`; defaults match a no-config-needed dev workflow.
#[derive(Debug, Clone)]
pub struct EmbeddedL1Config {
    /// L1 dev chain spec (genesis + hardforks). Used when
    /// `kind == Dev`; ignored when `kind == Chiado` (chiado loads its
    /// own GnosisChainSpec from the bundled preset).
    pub dev_chain_spec: Arc<ChainSpec>,
    /// Which L1 chain the embedded node serves.
    pub kind: L1ChainKind,
    /// Where reth stores L1 chain data + static files. For chiado
    /// this should be the pre-synced chiado-db (symlink or rsync
    /// from the operator's snapshot).
    pub datadir: PathBuf,
    /// HTTP RPC port (so smoke harnesses + `cast` can POST txs to L1).
    pub http_port: u16,
    /// Auth RPC port (engine API). Chiado mode: external lighthouse
    /// dials in here.
    pub auth_port: u16,
    /// P2P + discv4 discovery port. Dev mode: disabled. Chiado mode:
    /// needed for libp2p peering to chiado bootnodes.
    pub p2p_port: u16,
    /// discv5 UDP port. Separate from `p2p_port` so the embedded L1's
    /// discv5 doesn't bind reth's default port and collide with another
    /// node on the host.
    pub discv5_port: u16,
    /// Path to JWT secret file. Required in chiado mode (shared with
    /// lighthouse via volume mount).
    pub jwtsecret: Option<PathBuf>,
}

/// Build a reth `NodeConfig<ChainSpec>` for the embedded L1 dev node
/// (vanilla EthereumNode). Used only when `kind == Dev`.
pub fn build_dev_node_config(cfg: &EmbeddedL1Config) -> Result<NodeConfig<ChainSpec>> {
    let (network_args, rpc_args) = build_network_rpc_args(cfg)?;
    let mut node_cfg = NodeConfig::new(cfg.dev_chain_spec.clone())
        .with_datadir_args(DatadirArgs {
            datadir: cfg.datadir.clone().into(),
            ..DatadirArgs::default()
        })
        .with_network(network_args)
        .with_rpc(rpc_args)
        .dev();
    node_cfg.dev.block_time = Some(DEV_L1_BLOCK_TIME);
    // Synchronous state-root path for dev determinism.
    node_cfg.engine.legacy_state_root_task_enabled = true;
    Ok(node_cfg)
}

/// Build a reth `NodeConfig<GnosisChainSpec>` for chiado. NOT dev
/// mode — block production happens via engine-API from an external
/// lighthouse CL (which we expect the operator to launch separately
/// via `scripts/launch-chiado-lighthouse.sh` / docker-compose).
pub fn build_chiado_node_config(cfg: &EmbeddedL1Config) -> Result<NodeConfig<GnosisChainSpec>> {
    let (network_args, mut rpc_args) = build_network_rpc_args(cfg)?;
    // Keep discovery ENABLED. `newPayload` only lets the EL *execute*
    // blocks whose parent it already holds — it is not a sync path. A
    // snapshot behind the chain tip must backfill the gap over P2P, so
    // the EL needs peers (chiado bootnodes come from the GnosisChainSpec;
    // discovery runs on `cfg.p2p_port`, set in build_network_rpc_args, so
    // it won't collide with another reth on the host).
    // Bind auth RPC to all interfaces so docker-compose lighthouse
    // (host-network mode) can reach it. JWT secret guards access.
    rpc_args.auth_addr = "0.0.0.0".parse().expect("static addr");
    rpc_args.auth_jwtsecret = cfg.jwtsecret.clone();
    let chain_spec: Arc<GnosisChainSpec> = Arc::new(GnosisChainSpec::from(CHIADO_GENESIS.clone()));
    let mut node_cfg = NodeConfig::new(chain_spec)
        .with_datadir_args(DatadirArgs {
            datadir: cfg.datadir.clone().into(),
            ..DatadirArgs::default()
        })
        .with_network(network_args)
        .with_rpc(rpc_args);
    // Reth's async StateRootTask computes wrong roots for gnosis chains
    // in this reth pin (→ "incorrect state root" / SIGABRT on chiado
    // newPayload); force the legacy synchronous path (as the Dev L1 does).
    node_cfg.engine.legacy_state_root_task_enabled = true;
    Ok(node_cfg)
}

/// Build a reth `NodeConfig<ChainSpec>` for the private PoS devnet L1
/// (vanilla `EthereumNode`). Used when `kind == Devnet`.
pub fn build_devnet_node_config(cfg: &EmbeddedL1Config) -> Result<NodeConfig<ChainSpec>> {
    let (network_args, mut rpc_args) = build_network_rpc_args(cfg)?;
    // External lighthouse (docker/host) dials the engine API in — bind
    // to all interfaces (JWT guards it), same as the chiado path.
    rpc_args.auth_addr = "0.0.0.0".parse().expect("static addr");
    rpc_args.auth_jwtsecret = cfg.jwtsecret.clone();
    let mut node_cfg = NodeConfig::new(cfg.dev_chain_spec.clone())
        .with_datadir_args(DatadirArgs {
            datadir: cfg.datadir.clone().into(),
            ..DatadirArgs::default()
        })
        .with_network(network_args)
        .with_rpc(rpc_args);
    // No `.dev()` / block_time: blocks are CL-driven via engine API.
    // Synchronous state-root path for determinism (matches Dev/Chiado).
    node_cfg.engine.legacy_state_root_task_enabled = true;
    Ok(node_cfg)
}

fn build_network_rpc_args(cfg: &EmbeddedL1Config) -> Result<(NetworkArgs, RpcServerArgs)> {
    let ws_port = cfg.http_port.checked_add(1).ok_or_else(|| {
        eyre::eyre!(
            "L1 http_port {} too high for WS = http_port + 1",
            cfg.http_port
        )
    })?;
    let mut network_args = NetworkArgs {
        port: cfg.p2p_port,
        ..NetworkArgs::default()
    };
    network_args.discovery.port = cfg.p2p_port;
    network_args.discovery.discv5_port = Some(cfg.discv5_port);
    let rpc_args = RpcServerArgs {
        http: true,
        ws: true,
        http_port: cfg.http_port,
        ws_port,
        auth_port: cfg.auth_port,
        ipcdisable: true,
        ..RpcServerArgs::default()
    };
    Ok((network_args, rpc_args))
}
