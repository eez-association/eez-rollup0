//! Reusable Anvil-driven EEZ integration framework.
//!
//! The crate intentionally lives as a dev-dependency of protocol components:
//! it owns process lifecycle, deterministic chain fixtures, transaction helpers,
//! structured node signals, and the declarative scenario runner.

#![allow(missing_debug_implementations)]

use std::{
    collections::HashSet,
    net::{TcpListener, UdpSocket},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256, Bytes, U256, address, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{BlockNumHash, BlockNumberOrTag, TransactionReceipt, TransactionRequest};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolEvent, SolValue, sol};
use anyhow::{Context, Result, anyhow, bail};
use eez_protocol::EEZL2_ADDRESS;

/// Anvil's first default account (mnemonic `test test test test test test test test test test test junk`).
pub const ANVIL_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const ANVIL_ADDR: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
/// Anvil's second default account — used for tests that need a key
/// distinct from the deployer / authorized signer.
pub const ANVIL_KEY_1: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
pub const ANVIL_KEY_2: &str = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a";
pub const ANVIL_KEY_3: &str = "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6";
pub const ANVIL_KEY_4: &str = "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a";
pub const ANVIL_ADDR_3: Address = address!("0x90F79bf6EB2c4f870365E785982E1f101E93b906");
/// Dedicated deterministic L2 system identity; deliberately not an Anvil account.
pub const L2_SYSTEM_KEY: &str =
    "0x6f7d72ecb79c8bf1bd8e7c49a1c4a22741ab708f06bb19e5b5d44a6f0934a7c1";

/// External anvil cadence for composer-mode e2e tests. K = L1/L2 = 3 (not 2):
/// `RollupTiming::validate` needs proof+slack (1100ms) ≤ (K−1)·L2 = 2000ms.
/// `EEZ_L1_BLOCK_TIME_MS` derives from this and must match the miner.
pub const L1_BLOCK_TIME_SECS: u64 = 3;

/// L2 genesis timestamp for the reorg fixture (`0x6490fdd2`); `for_reorg`
/// anvil aligns to it. The dev path stamps genesis at wall-clock `now`
/// ([`Harness::fresh`]) — the lateness gate reads a backdated genesis as late.
pub const L2_GENESIS_TIMESTAMP: u64 = 0x6490_fdd2;

/// Carries the Harness's shared L2 genesis path to every node it spawns
/// (sequencer, follower, restarts) so they build the same chain; → `--chain`.
const TEST_L2_GENESIS_ENV: &str = "EEZ_TEST_L2_GENESIS_PATH";

/// Composer tick cadence for single-composer tests — max speed.
pub const COMPOSER_INTERVAL_SINGLE: Duration = Duration::from_secs(1);
/// Composer tick cadence for multi-composer contention — gives the
/// deriver time to re-sync between ticks (1-tick-per-2-blocks ratio).
pub const COMPOSER_INTERVAL_MULTI: Duration = Duration::from_secs(2);

static LOG_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Stable values carried in the `test_signal` field of JSON tracing events.
pub mod signals {
    pub const BUNDLE_ACCEPTED: &str = "eez.node.l1_embedded.bundle.accepted";
    pub const BUNDLE_MEMPOOL_FALLBACK: &str = "eez.submitter.bundle.mempool_fallback";
    pub const DERIVER_REORG_NOOP: &str = "eez.deriver.l1.reorg.noop";
    pub const DERIVER_REORG_RETREATED: &str = "eez.deriver.l1.reorg.retreated";
    pub const DERIVER_STATE_DIVERGED_POST: &str = "eez.deriver.state.diverged_post";
    pub const DERIVER_STATE_DIVERGED_PRE: &str = "eez.deriver.state.diverged_pre";
    pub const DERIVER_SAFE_ADVANCED: &str = "eez.deriver.safe.advanced";
    pub const DERIVER_FINALIZED_ADVANCED: &str = "eez.deriver.finalized.advanced";
    pub const DERIVER_SYNC_BLOCK_BUILT: &str = "eez.deriver.reconcile.sync_block_built";
    pub const DERIVER_RESYNC_FAILED: &str = "eez.deriver.resync.failed";
    pub const DERIVER_COMMITTER_CLOSED: &str = "eez.deriver.committer.closed";
    pub const NODE_BOOT_CATCH_UP_FAILED: &str = "eez.node.deriver.boot_catch_up.failed";
    pub const NODE_PANIC: &str = "eez.node.panic";
    pub const COMPOSER_BUNDLE_DISPATCHED: &str = "eez.composer.bundle.dispatched";
    pub const COMPOSER_PHASE1_BUNDLE_DISPATCHED: &str = "eez.composer.phase1.bundle.dispatched";
    pub const FOLLOWER_HEAD_ADVANCED: &str = "eez.node.follower.head.advanced";
    pub const FOLLOWER_HEAD_SYNCING: &str = "eez.node.follower.head.syncing";
    pub const L1_REORG_DETECTED: &str = "eez.l1_watcher.ring.rewind";
    pub const TX_NONCE_CHAIN_EVICTED: &str = "eez.composer.recovery.nonce_chain_evicted";
    pub const TX_POISON_EVICTED: &str = "eez.composer.recovery.poison_evicted";
}

/// One machine-readable tracing record emitted by an EEZ process.
#[derive(Clone, Debug)]
pub struct NodeSignal {
    pub name: String,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

impl NodeSignal {
    pub fn u64(&self, field: &str) -> Result<u64> {
        let value = self
            .fields
            .get(field)
            .ok_or_else(|| anyhow!("signal {} has no {field} field", self.name))?;
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|v| v.parse().ok()))
            .ok_or_else(|| anyhow!("signal {} field {field} is not a u64: {value}", self.name))
    }

    pub fn b256(&self, field: &str) -> Result<B256> {
        let value = self
            .fields
            .get(field)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("signal {} has no string {field} field", self.name))?;
        value
            .parse()
            .with_context(|| format!("signal {} has invalid {field}: {value}", self.name))
    }
}

pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

/// Resolve the node executable without tying this crate to one Cargo test
/// target. `EEZ_TEST_NODE_BIN` is the explicit cross-crate escape hatch; the
/// Cargo integration-test variable and the normal debug output are fallbacks.
pub fn eez_node_bin() -> Result<PathBuf> {
    if let Some(path) =
        std::env::var_os("EEZ_TEST_NODE_BIN").or_else(|| std::env::var_os("CARGO_BIN_EXE_eez-node"))
    {
        return Ok(path.into());
    }
    let path = repo_root().join("target/debug/eez-node");
    if path.is_file() {
        return Ok(path);
    }
    bail!(
        "eez-node binary not found; set EEZ_TEST_NODE_BIN or build it with `cargo build -p eez-node`"
    )
}

pub fn anvil_bin() -> String {
    for path in [
        "/root/.foundry/bin/anvil",
        &format!(
            "{}/.foundry/bin/anvil",
            std::env::var("HOME").unwrap_or_default()
        ),
    ] {
        if std::path::Path::new(path).exists() {
            return path.to_string();
        }
    }
    "anvil".to_string()
}

/// Probe a currently available TCP port.
///
/// The listener is released on return, so this is not a reservation.
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("local_addr").port()
}

fn probe_unique_tcp_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP probe");
        let port = listener.local_addr().expect("TCP probe local_addr").port();
        if used.insert(port) {
            return port;
        }
    }
}

fn probe_unique_udp_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("bind UDP probe");
        let port = socket.local_addr().expect("UDP probe local_addr").port();
        if used.insert(port) {
            return port;
        }
    }
}

fn probe_unique_tcp_udp_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP probe");
        let port = listener.local_addr().expect("TCP probe local_addr").port();
        if used.contains(&port) {
            continue;
        }
        let Ok(socket) = UdpSocket::bind(("127.0.0.1", port)) else {
            continue;
        };
        drop(socket);
        used.insert(port);
        return port;
    }
}

/// Probe an HTTP port whose implicit `port + 1` WS listener is also available.
fn probe_unique_http_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let http_listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP probe");
        let http_port = http_listener
            .local_addr()
            .expect("HTTP probe local_addr")
            .port();
        let Some(ws_port) = http_port.checked_add(1) else {
            continue;
        };
        if used.contains(&http_port) || used.contains(&ws_port) {
            continue;
        }
        let Ok(ws_listener) = TcpListener::bind(("127.0.0.1", ws_port)) else {
            continue;
        };
        drop(ws_listener);
        used.insert(http_port);
        used.insert(ws_port);
        return http_port;
    }
}

pub struct Anvil {
    child: Child,
    pub rpc_url: String,
}

/// Anvil configuration. `Anvil::spawn(port)` is the default (3s block,
/// random mnemonic). The multi-composer reorg test uses
/// [`AnvilConfig::for_reorg`] which matches the hardhat mnemonic (so we
/// have predictable prefunded EOAs) and enables the cancun hardfork
/// (required by `anvil_reorg`).
pub struct AnvilConfig {
    pub block_time_secs: u64,
    pub mnemonic: Option<&'static str>,
    pub hardfork: Option<&'static str>,
    pub gas_limit: Option<u64>,
    pub genesis_timestamp: Option<u64>,
}

impl Default for AnvilConfig {
    fn default() -> Self {
        Self {
            block_time_secs: L1_BLOCK_TIME_SECS,
            mnemonic: None,
            hardfork: None,
            gas_limit: None,
            genesis_timestamp: Some(L2_GENESIS_TIMESTAMP),
        }
    }
}

impl AnvilConfig {
    /// 3s block time, hardhat mnemonic, cancun hardfork, 30M gas.
    /// (Chiado uses 5s; tests prefer speed over fidelity. 3s blocks +
    /// cancun still permit `anvil_reorg`.)
    pub fn for_reorg() -> Self {
        Self {
            block_time_secs: L1_BLOCK_TIME_SECS,
            mnemonic: Some(HARDHAT_MNEMONIC),
            hardfork: Some("cancun"),
            gas_limit: Some(30_000_000),
            genesis_timestamp: Some(L2_GENESIS_TIMESTAMP),
        }
    }
}

pub const HARDHAT_MNEMONIC: &str = "test test test test test test test test test test test junk";

impl Anvil {
    pub async fn spawn(port: u16) -> Result<Self> {
        Self::spawn_with(port, AnvilConfig::default()).await
    }

    pub async fn spawn_with(port: u16, cfg: AnvilConfig) -> Result<Self> {
        let mut cmd = Command::new(anvil_bin());
        cmd.args([
            "--port",
            &port.to_string(),
            "--chain-id",
            &DEV_CHAIN_ID.to_string(),
            "--block-time",
            &cfg.block_time_secs.to_string(),
            "--silent",
        ]);
        if let Some(m) = cfg.mnemonic {
            cmd.args(["--mnemonic", m]);
        }
        if let Some(h) = cfg.hardfork {
            cmd.args(["--hardfork", h]);
        }
        if let Some(g) = cfg.gas_limit {
            cmd.args(["--gas-limit", &g.to_string()]);
        }
        if let Some(t) = cfg.genesis_timestamp {
            cmd.args(["--timestamp", &t.to_string()]);
        }
        let child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn anvil")?;
        let rpc_url = format!("http://127.0.0.1:{port}");
        let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if provider.get_block_number().await.is_ok() {
                return Ok(Self { child, rpc_url });
            }
        }
        bail!("anvil did not start within 10s on port {port}");
    }
}

impl Anvil {
    /// SIGSTOP — pauses the anvil process. RPC stops responding.
    /// Used to simulate L1 outage (rougher: kills RPC entirely).
    pub fn pause(&self) -> Result<()> {
        signal(self.child.id(), "STOP")
    }

    /// SIGCONT — resumes the anvil process.
    pub fn resume(&self) -> Result<()> {
        signal(self.child.id(), "CONT")
    }

    /// `anvil_setBalance` — sets the on-chain ETH balance of `addr`.
    /// Used to simulate the more realistic "poster ran out of gas"
    /// outage: anvil + RPC stay alive, but the node's tx broadcasts
    /// revert at the simulation step with `insufficient funds`.
    pub async fn set_balance(&self, addr: Address, wei: U256) -> Result<()> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url.parse()?);
        let _: serde_json::Value = provider
            .client()
            .request("anvil_setBalance", (addr, wei))
            .await
            .context("anvil_setBalance")?;
        Ok(())
    }

    /// `anvil_reorg` — drops the most recent `depth` L1 blocks. Requires
    /// the cancun hardfork (see [`AnvilConfig::for_reorg`]). The empty
    /// array is the optional list of replacement txs (we use none —
    /// blocks are dropped, nodes notice via head moving backward).
    pub async fn reorg(&self, depth: u64) -> Result<()> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url.parse()?);
        let _: serde_json::Value = provider
            .client()
            .request("anvil_reorg", (depth, Vec::<serde_json::Value>::new()))
            .await
            .context("anvil_reorg")?;
        Ok(())
    }
}

fn signal(pid: u32, sig: &str) -> Result<()> {
    let status = Command::new("kill")
        .args([&format!("-{sig}"), &pid.to_string()])
        .status()
        .context("spawn kill")?;
    if !status.success() {
        bail!("kill -{sig} {pid} failed");
    }
    Ok(())
}

impl Drop for Anvil {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Minimal `eth_sendBundle` stub: forwards bundles to anvil via
/// `eth_sendRawTransaction`. Required because PR #6 routes submissions
/// through `eth_sendBundle`, which anvil doesn't implement.
pub struct BundleStub {
    child: Child,
    pub url: String,
}

impl BundleStub {
    pub async fn spawn(port: u16, upstream: &str) -> Result<Self> {
        let script = repo_root().join("scripts/builder-stub.py");
        let listen = format!("127.0.0.1:{port}");
        let child = Command::new("python3")
            .arg(script)
            .args(["--listen", &listen, "--upstream", upstream])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn builder-stub.py")?;
        let url = format!("http://{listen}");
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if std::net::TcpStream::connect(&listen).is_ok() {
                return Ok(Self { child, url });
            }
        }
        bail!("builder-stub did not bind within 3s on {listen}");
    }
}

impl Drop for BundleStub {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Per-test fixture: anvil + bundle stub + deployed protocol. Every
/// test starts with `let harness = Harness::fresh().await` (or
/// `with_anvil_config` for the reorg test); eliminates the 4-line
/// preamble duplicated across tests.
pub struct Harness {
    pub anvil: Anvil,
    pub stub: BundleStub,
    pub dep: Deployment,
    /// Wall-clock-stamped dev genesis shared by every node via
    /// [`TEST_L2_GENESIS_ENV`] (the tempdir keeps it alive). `None` on the
    /// reorg path, which passes its own genesis.
    l2_genesis: Option<(PathBuf, tempfile::TempDir)>,
}

impl Harness {
    /// Default: dev-chain anvil + dev-genesis initial state. Anvil + L2
    /// genesis share one wall-clock `now` so the lateness gate doesn't fire.
    pub async fn fresh() -> Result<Self> {
        let ts = now_unix_secs();
        let (gpath, gdir) = write_dev_genesis_at(ts)?;
        let cfg = AnvilConfig {
            genesis_timestamp: Some(ts),
            ..AnvilConfig::default()
        };
        let mut h = Self::with_anvil_config(cfg, dev_genesis_state_root()).await?;
        h.l2_genesis = Some((gpath, gdir));
        Ok(h)
    }

    /// Custom anvil config + explicit initial state root. Used by the
    /// reorg test which needs cancun + hardhat mnemonic + custom L2 genesis.
    pub async fn with_anvil_config(cfg: AnvilConfig, initial_state: B256) -> Result<Self> {
        let anvil = Anvil::spawn_with(free_port(), cfg).await?;
        let stub = BundleStub::spawn(free_port(), &anvil.rpc_url).await?;
        let dep = deploy_contracts_with_initial(&anvil.rpc_url, ANVIL_KEY, initial_state).await?;
        Ok(Self {
            anvil,
            stub,
            dep,
            l2_genesis: None,
        })
    }

    pub fn chain(&self) -> Chain<'_> {
        Chain::new(&self.anvil, &self.dep)
    }

    /// Default env for spawning `eez-node`. Poster + proof signer = anvil#0.
    pub fn env(&self) -> Vec<(&'static str, String)> {
        self.env_for(ANVIL_KEY, false)
    }

    /// Env with parameterised poster key and external-batches flag. Used
    /// by the multi-composer reorg test to spawn c1/c2 with different
    /// poster EOAs (same proof signer per the contract's `authorizedSigner`).
    pub fn env_for(
        &self,
        poster_key: &str,
        expect_external_batches: bool,
    ) -> Vec<(&'static str, String)> {
        self.env_for_options(NodeEnvOptions {
            poster_key,
            proof_signer_key: Some(ANVIL_KEY),
            rollup_id: self.dep.rollup_id,
            expect_external_batches,
            sequencer_rpc: None,
        })
    }

    pub fn follower_env(&self, sequencer_rpc: Option<&str>) -> Vec<(&'static str, String)> {
        self.env_for_options(NodeEnvOptions {
            poster_key: ANVIL_KEY,
            proof_signer_key: None,
            rollup_id: self.dep.rollup_id,
            expect_external_batches: true,
            sequencer_rpc,
        })
    }

    pub fn env_with_rollup_id(&self, rollup_id: u64) -> Vec<(&'static str, String)> {
        self.env_for_options(NodeEnvOptions {
            poster_key: ANVIL_KEY,
            proof_signer_key: Some(ANVIL_KEY),
            rollup_id,
            expect_external_batches: false,
            sequencer_rpc: None,
        })
    }

    pub fn env_with_proof_signer(&self, proof_signer_key: &str) -> Vec<(&'static str, String)> {
        self.env_for_options(NodeEnvOptions {
            poster_key: ANVIL_KEY,
            proof_signer_key: Some(proof_signer_key),
            rollup_id: self.dep.rollup_id,
            expect_external_batches: false,
            sequencer_rpc: None,
        })
    }

    fn env_for_options(&self, opts: NodeEnvOptions<'_>) -> Vec<(&'static str, String)> {
        let mut env = vec![
            ("EEZ_L1_RPC_URL", self.anvil.rpc_url.clone()),
            ("EEZ_L1_BUILDER_RPC_URL", self.stub.url.clone()),
            ("EEZ_L1_POSTER_KEY", opts.poster_key.to_string()),
            ("EEZ_L1_CHAIN_ID", DEV_CHAIN_ID.to_string()),
            ("EEZ_L1_CHAIN", "testing".to_string()),
            ("EEZ_L2_SYSTEM_KEY", L2_SYSTEM_KEY.to_string()),
            ("EEZL2_ADDRESS", format!("{EEZL2_ADDRESS:#x}")),
            (
                "EEZ_L1_BLOCK_TIME_MS",
                (L1_BLOCK_TIME_SECS * 1000).to_string(),
            ),
            ("EEZ_L2_BLOCK_TIME_MS", "1000".to_string()),
            ("EEZ_PROOF_TIME_MS", "1000".to_string()),
            ("EEZ_SUBMISSION_SLACK_MS", "100".to_string()),
            (
                "EEZ_REGISTRY_ADDRESS",
                format!("{:#x}", self.dep.eez_address),
            ),
            (
                "EEZ_REGISTRY_DEPLOY_BLOCK",
                self.dep.deploy_block.to_string(),
            ),
            (
                "EEZ_ECDSA_PROOF_SYSTEM_ADDRESS",
                format!("{:#x}", self.dep.mock_ps_address),
            ),
            (
                "EEZ_ROLLUP_MANAGER_ADDRESS",
                format!("{:#x}", self.dep.rollup_manager_address),
            ),
            ("EEZ_ROLLUP_ID", opts.rollup_id.to_string()),
            (
                "EEZ_COMPOSER_INTERVAL_SECS",
                if opts.expect_external_batches {
                    COMPOSER_INTERVAL_MULTI
                } else {
                    COMPOSER_INTERVAL_SINGLE
                }
                .as_secs()
                .to_string(),
            ),
            (
                "EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES",
                opts.expect_external_batches.to_string(),
            ),
            (
                "EEZ_L2_DATADIR",
                "/tmp/unused-overridden-by-flag".to_string(),
            ),
            (
                "RUST_LOG",
                std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string()),
            ),
            (
                TEST_L2_GENESIS_ENV,
                self.l2_genesis
                    .as_ref()
                    .map(|(p, _)| p.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            ),
        ];

        if let Some(proof_signer_key) = opts.proof_signer_key {
            env.push(("EEZ_PROOF_SIGNER_KEY", proof_signer_key.to_string()));
        }
        if let Some(sequencer_rpc) = opts.sequencer_rpc {
            env.push(("EEZ_SEQUENCER_RPC", sequencer_rpc.to_string()));
        }
        env
    }
}

struct NodeEnvOptions<'a> {
    poster_key: &'a str,
    proof_signer_key: Option<&'a str>,
    rollup_id: u64,
    expect_external_batches: bool,
    sequencer_rpc: Option<&'a str>,
}

pub struct Deployment {
    pub eez_address: Address,
    pub deploy_block: u64,
    pub mock_ps_address: Address,
    pub rollup_manager_address: Address,
    pub rollup_id: u64,
}

sol! {
    #[sol(rpc)]
    interface IEEZ {
        event BatchPosted(uint256 rollupCount);
        event L2ExecutionPerformed(uint64 indexed rollupId, bytes32 newState);
        event L2TxSkipped(uint256 indexed transientIdx, bytes revertData);
        function rollups(uint64 rollupId) external view returns (address rollupContract, bytes32 stateRoot, uint256 etherBalance);
        function rollupCounter() external view returns (uint256);
        function registerRollup(address rollupContract, bytes32 initialState) external returns (uint64 rollupId);
    }
}

/// Reth's `--chain dev` genesis state root. Used as the `initialState`
/// when registering the rollup so the very first batch's prestate
/// (`l2_state_root(0)`) matches the on-chain `rollups[rid].stateRoot`.
/// With the default `B256::ZERO`, every batch's `_applyStateUpdates`
/// reverts with `StateRootMismatch`, caught by the try/catch,
/// emitting `L2TxSkipped` instead of `L2ExecutionPerformed`.
pub fn dev_genesis_state_root() -> B256 {
    reth_chainspec::DEV.genesis_header().state_root
}

/// Wall-clock seconds for stamping test genesis + anvil, so the sequencer's
/// defer-on-lateness gate doesn't read every trigger as late.
fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}

/// Reth's dev genesis with timestamp set to `ts`, written to a temp file for
/// `--chain`. Same alloc as `--chain dev` → state root still equals
/// [`dev_genesis_state_root`]. The tempdir must outlive nodes using the path.
fn write_dev_genesis_at(ts: u64) -> Result<(PathBuf, tempfile::TempDir)> {
    let mut genesis: alloy_genesis::Genesis = reth_chainspec::DEV.genesis().clone();
    genesis.timestamp = ts;
    let dir = tempfile::tempdir().context("genesis tempdir")?;
    let path = dir.path().join("genesis.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&genesis).context("serialize dev genesis")?,
    )
    .context("write dev genesis")?;
    Ok((path, dir))
}

/// Path to the multi-composer reorg test's L2 genesis fixture. 23
/// prefunded accounts (hardhat defaults), all hardforks at block 0,
/// matches what the team uses for local reorg testing.
pub fn reorg_genesis_path() -> PathBuf {
    repo_root().join("crates/eez-node/tests/fixtures/genesis.json")
}

/// Genesis state root of `reorg_genesis_path()`. Computed by reading the
/// JSON into reth's `Genesis` and converting to a `ChainSpec`.
pub fn reorg_genesis_state_root() -> Result<B256> {
    let raw = std::fs::read_to_string(reorg_genesis_path()).context("read genesis.json")?;
    let genesis: alloy_genesis::Genesis =
        serde_json::from_str(&raw).context("parse genesis.json")?;
    let spec: reth_chainspec::ChainSpec = genesis.into();
    Ok(spec.genesis_header().state_root)
}

/// Send one EIP-1559 value transfer on the L2 at `rpc_url` from
/// `signing_key` to `to`. Used by the multi-composer reorg test's
/// collector load. Builds,
/// signs, and submits in one call; returns the tx hash. Rejects with
/// nonce/funds/RPC errors propagated.
pub async fn send_l2_value_transfer(
    rpc_url: &str,
    signing_key: &str,
    to: Address,
    value: U256,
) -> Result<alloy_primitives::TxHash> {
    let signer: PrivateKeySigner = signing_key
        .strip_prefix("0x")
        .unwrap_or(signing_key)
        .parse()
        .context("parse signing key")?;
    let from = signer.address();

    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let chain_id = provider.get_chain_id().await?;
    let nonce = provider.get_transaction_count(from).await?;
    let fees = provider.estimate_eip1559_fees().await?;

    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit: 21_000,
        max_fee_per_gas: fees.max_fee_per_gas,
        max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
        to: alloy_primitives::TxKind::Call(to),
        value,
        access_list: alloy_rpc_types_eth::AccessList::default(),
        input: alloy_primitives::Bytes::default(),
    };
    let sig = signer.sign_transaction_sync(&mut tx)?;
    let envelope = TxEnvelope::from(tx.into_signed(sig));
    let raw = envelope.encoded_2718();

    let hash = provider
        .send_raw_transaction(&raw)
        .await?
        .tx_hash()
        .to_owned();
    Ok(hash)
}

/// Send one L2 value transfer and wait for its receipt. Use this when a test
/// needs to prove a later L1 attestation includes a fresh L2 state transition.
pub async fn send_l2_value_transfer_confirmed(
    rpc_url: &str,
    signing_key: &str,
    to: Address,
    value: U256,
    timeout: Duration,
) -> Result<alloy_primitives::TxHash> {
    let hash = send_l2_value_transfer(rpc_url, signing_key, to, value).await?;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for(timeout, || async {
        let Some(receipt) = provider.get_transaction_receipt(hash).await? else {
            return Ok(None);
        };
        if !receipt.status() {
            bail!("L2 value transfer reverted: {hash}");
        }
        Ok(Some(()))
    })
    .await?;
    Ok(hash)
}

/// Wait until `eth_blockNumber` responds at `rpc_url`. Used to confirm a
/// just-spawned eez-node's L2 RPC is up before we send txs at it.
pub async fn wait_for_l2_rpc(rpc_url: &str, timeout: Duration) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for(timeout, || async {
        Ok(provider.get_block_number().await.ok().map(|_| ()))
    })
    .await
}

/// Read the `stateRoot` of the latest `safe` block on the L2 at `rpc_url`.
/// Returns `Ok(None)` while the L2 hasn't yet adopted any safe block
/// (genesis L1 derivation pending). Used by the multi-composer reorg
/// test to verify both composers settle on the same canonical L2 head.
pub async fn safe_block_state_root(rpc_url: &str) -> Result<Option<B256>> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider
        .get_block_by_number(alloy_rpc_types_eth::BlockNumberOrTag::Safe)
        .await?;
    Ok(block.map(|b| b.header.state_root))
}

/// Block number and hash at a named tag (`latest`, `safe`, `finalized`, …).
/// `None` when no block exists at that tag yet.
pub async fn block_number_and_hash_at(
    rpc_url: &str,
    tag: alloy_rpc_types_eth::BlockNumberOrTag,
) -> Result<Option<(u64, B256)>> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider.get_block_by_number(tag).await?;
    Ok(block.map(|b| (b.header.number, b.header.hash)))
}

/// Deploy `EEZ` + `MockECDSAProofSystem` + `Rollup`, then register the rollup.
/// Pure alloy — reads compiled foundry artifacts and sends each deploy
/// as an in-process tx. Mirrors `sync-rollups-composer`'s
/// `tests/e2e_anvil.rs` pattern (and that of every other Rust rollup
/// codebase surveyed). Prereq: `forge build` must have run in
/// `contracts/`.
pub async fn deploy_contracts(rpc_url: &str, key: &str) -> Result<Deployment> {
    deploy_contracts_with_initial(rpc_url, key, dev_genesis_state_root()).await
}

/// Deploy with an explicit `initialState`. Use this when registering a
/// rollup whose L2 will run a non-`--chain dev` genesis (the reorg
/// test uses `reorg_genesis_state_root()`).
pub async fn deploy_contracts_with_initial(
    rpc_url: &str,
    key: &str,
    initial_state: B256,
) -> Result<Deployment> {
    // Anvil auto-signs for its default accounts when `from` is set; no wallet
    // filler needed. Matches sync-rollups-composer's pattern.
    let signer: PrivateKeySigner = key
        .strip_prefix("0x")
        .unwrap_or(key)
        .parse()
        .context("parse signer key")?;
    let signer_addr = signer.address();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    let out = repo_root().join("contracts/out");

    let eez_address = deploy(
        &provider,
        signer_addr,
        &out.join("EEZ.sol/EEZ.json"),
        signer_addr.abi_encode(),
    )
    .await?;
    let deploy_block = provider.get_block_number().await?;

    let mock_ps_address = deploy(
        &provider,
        signer_addr,
        &out.join("MockECDSAProofSystem.sol/MockECDSAProofSystem.json"),
        signer_addr.abi_encode(),
    )
    .await?;

    // Rollup(address eez, address owner, uint256 threshold,
    //        address[] proofSystems, bytes32[] vkeys)
    let proof_systems: Vec<Address> = vec![mock_ps_address];
    // vkey embeds the authorized signer address; the registry treats vkey as
    // opaque but checks non-zero + membership (see DeployRollup.s.sol:60).
    let vkeys: Vec<B256> = vec![B256::from_slice(&{
        let mut padded = [0u8; 32];
        padded[12..].copy_from_slice(signer_addr.as_slice());
        padded
    })];
    let rollup_manager_address = deploy(
        &provider,
        signer_addr,
        &out.join("Rollup.sol/Rollup.json"),
        (
            eez_address,
            signer_addr,
            U256::from(1u64),
            proof_systems,
            vkeys,
        )
            .abi_encode_params(),
    )
    .await?;

    // registerRollup via a plain eth_sendTransaction (anvil signs for default
    // accounts); avoids the alloy wallet-filler requirement on `to`.
    let calldata = IEEZ::registerRollupCall {
        rollupContract: rollup_manager_address,
        initialState: initial_state,
    }
    .abi_encode();
    let receipt = provider
        .send_transaction(
            TransactionRequest::default()
                .from(signer_addr)
                .to(eez_address)
                .input(calldata.into()),
        )
        .await?
        .get_receipt()
        .await?;
    if !receipt.status() {
        bail!("registerRollup tx reverted");
    }
    let registry = IEEZ::new(eez_address, &provider);
    let rollup_id = registry.rollupCounter().call().await?.try_into()?;

    Ok(Deployment {
        eez_address,
        deploy_block,
        mock_ps_address,
        rollup_manager_address,
        rollup_id,
    })
}

async fn deploy<P: Provider>(
    provider: &P,
    from: Address,
    artifact_path: &std::path::Path,
    constructor_args: Vec<u8>,
) -> Result<Address> {
    let artifact: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(artifact_path)
            .with_context(|| format!("read {}", artifact_path.display()))?,
    )?;
    let bytecode_hex = artifact["bytecode"]["object"]
        .as_str()
        .ok_or_else(|| anyhow!("bytecode.object not found in {}", artifact_path.display()))?
        .strip_prefix("0x")
        .unwrap_or_default();
    let mut deploy_data = hex::decode(bytecode_hex).context("decode bytecode hex")?;
    deploy_data.extend_from_slice(&constructor_args);
    let receipt = provider
        .send_transaction(
            TransactionRequest::default()
                .from(from)
                .input(deploy_data.into()),
        )
        .await?
        .get_receipt()
        .await?;
    if !receipt.status() {
        bail!("deploy of {} reverted", artifact_path.display());
    }
    receipt.contract_address.ok_or_else(|| {
        anyhow!(
            "no contract_address in receipt for {}",
            artifact_path.display()
        )
    })
}

pub struct NodeHandle {
    child: Mutex<Child>,
    /// Human label for assertion messages ("c1", "c2", or the default "node").
    pub name: String,
    /// Where the node's merged stdout+stderr is written. Goes to
    /// `EEZ_TEST_LOG_DIR/eez-node-<pid>.log` if that env var is set,
    /// otherwise inside a tempdir held in `keep_alive`.
    pub log_path: PathBuf,
    /// Tempdirs whose lifetime is tied to this handle (the log dir if
    /// we allocated one, and the datadir if the handle created it via
    /// [`Self::start`]). They drop with the handle.
    keep_alive: Vec<tempfile::TempDir>,
    /// L2 HTTP RPC port for this node — tracked so tests that need to
    /// send L2 txs to it (the multi-composer reorg test's collector) can target it.
    pub http_port: u16,
}

#[derive(Default)]
pub struct NodeConfig<'a> {
    /// Path to a custom genesis JSON. `None` uses `--chain dev`.
    pub genesis_path: Option<&'a std::path::Path>,
}

impl NodeHandle {
    /// Spawn `eez-node` against `datadir` with `--chain dev` and the
    /// given env. See [`Self::spawn_with`] for custom genesis.
    pub fn spawn(datadir: &std::path::Path, env: &[(&'static str, String)]) -> Result<Self> {
        Self::spawn_with("node", datadir, &NodeConfig::default(), env)
    }

    /// Spawn `eez-node` against `datadir` with the given config + env.
    /// Caller owns the datadir (e.g. a `tempfile::TempDir`) so
    /// kill+respawn tests can share state across handles. Uses the
    /// test-built binary path (`CARGO_BIN_EXE_eez-node`) directly —
    /// skips the `cargo run` metadata-resolution overhead per spawn.
    pub fn spawn_with(
        name: &str,
        datadir: &std::path::Path,
        cfg: &NodeConfig<'_>,
        env: &[(&'static str, String)],
    ) -> Result<Self> {
        let (log_path, log_tempdir) = if let Ok(d) = std::env::var("EEZ_TEST_LOG_DIR") {
            let suffix = LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
            let p = std::path::PathBuf::from(d).join(format!(
                "eez-node-{name}-{}-{suffix}.log",
                std::process::id()
            ));
            (p, None)
        } else {
            let td = tempfile::tempdir().context("log tempdir")?;
            let p = td.path().join("eez-node.log");
            (p, Some(td))
        };
        // tracing_subscriber's default writer is stdout; reth's panics go to stderr.
        // Merge both into one log file so the reorg test can grep
        // `reorg.retreated` and `Fatal | UnexpectedStaticFile` from a single stream.
        let f = std::fs::File::create(&log_path).context("create log file")?;
        let f2 = f.try_clone().context("clone log file")?;
        let (stdout, stderr) = (Stdio::from(f), Stdio::from(f2));
        // Reth defaults collide if any test or unrelated process holds them.
        // Each NodeHandle picks its own ephemeral ports for authrpc / http / ws / p2p.
        // These are availability probes, not reservations: the sockets are
        // released before the child binds. The set prevents deterministic
        // collisions among one node's listeners.
        let mut used_ports = HashSet::new();
        let authrpc_port = probe_unique_tcp_port(&mut used_ports);
        let http_port = probe_unique_tcp_port(&mut used_ports);
        let ws_port = probe_unique_tcp_port(&mut used_ports);
        let p2p_port = probe_unique_tcp_port(&mut used_ports);
        let l1_http_port = probe_unique_http_port(&mut used_ports);
        let l1_auth_port = probe_unique_tcp_port(&mut used_ports);
        // Embedded L1 uses this numeric port for RLPx TCP and discovery UDP.
        let l1_p2p_port = probe_unique_tcp_udp_port(&mut used_ports);
        let l1_discv5_port = probe_unique_udp_port(&mut used_ports);
        let l1_xchain_port = probe_unique_tcp_port(&mut used_ports);
        let l2_xchain_port = probe_unique_tcp_port(&mut used_ports);
        let l1_datadir = datadir.join("embedded-l1");
        // Genesis: an explicit genesis_path (reorg fixture) wins, else the
        // Harness's shared wall-clock genesis (TEST_L2_GENESIS_ENV — same chain
        // for sequencer/follower/restarts), else `--chain dev`. Wall-clock is
        // required by the sequencer's defer-on-lateness gate.
        let env_genesis = env
            .iter()
            .find(|(k, _)| *k == TEST_L2_GENESIS_ENV)
            .map(|(_, v)| v.as_str())
            .filter(|v| !v.is_empty())
            .map(std::ffi::OsString::from);
        let chain_arg: std::ffi::OsString = cfg
            .genesis_path
            .map(|p| p.as_os_str().to_owned())
            .or(env_genesis)
            .unwrap_or_else(|| std::ffi::OsString::from("dev"));
        let mut cmd = Command::new(eez_node_bin()?);
        let signal_filter = std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| {
            "warn,eez_node=info,eez_l1=info,eez_composer=info,eez_deriver=info".to_string()
        });
        cmd.current_dir(repo_root())
            .args(["node", "--chain"])
            .arg(&chain_arg)
            .arg("--datadir")
            .arg(datadir)
            .stdout(stdout)
            .args([
                "--log.stdout.format",
                "json",
                "--log.stdout.filter",
                &signal_filter,
                "--log.file.max-files",
                "0",
                "--http",
                "--http.addr",
                "127.0.0.1",
                "--http.port",
                &http_port.to_string(),
                "--ws.port",
                &ws_port.to_string(),
                "--authrpc.port",
                &authrpc_port.to_string(),
                "--port",
                &p2p_port.to_string(),
                "--disable-discovery",
                "--ipcdisable",
            ])
            .stderr(stderr)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", std::env::var("HOME").unwrap_or_default())
            .env(
                "RUSTUP_HOME",
                std::env::var("RUSTUP_HOME").unwrap_or_default(),
            )
            .env(
                "CARGO_HOME",
                std::env::var("CARGO_HOME").unwrap_or_default(),
            );
        cmd.env("EEZ_L1_HTTP_PORT", l1_http_port.to_string())
            .env("EEZ_L1_AUTH_PORT", l1_auth_port.to_string())
            .env("EEZ_L1_P2P_PORT", l1_p2p_port.to_string())
            .env("EEZ_L1_DISCV5_PORT", l1_discv5_port.to_string())
            .env("EEZ_L1_XCHAIN_PORT", l1_xchain_port.to_string())
            .env("EEZ_L2_XCHAIN_PORT", l2_xchain_port.to_string())
            .env("EEZ_L1_DATADIR", &l1_datadir)
            // May be overridden below when a test uses another L2 upstream.
            .env("EEZ_L2_RPC_URL", format!("http://127.0.0.1:{http_port}"));
        for (k, v) in env {
            if *k == TEST_L2_GENESIS_ENV {
                continue; // test-only marker (consumed as --chain above)
            }
            cmd.env(*k, v);
        }
        let child = cmd.spawn().context("spawn eez-node")?;
        Ok(Self {
            child: Mutex::new(child),
            name: name.to_string(),
            log_path,
            keep_alive: log_tempdir.into_iter().collect(),
            http_port,
        })
    }

    /// Async convenience: allocate a fresh datadir tempdir, spawn the
    /// node, wait for its L2 HTTP RPC to come up. Datadir is owned by
    /// the returned handle (dropped with it). Use [`Self::spawn_with`]
    /// directly when a test needs to share a datadir across handles
    /// (kill+respawn).
    pub async fn start(
        name: &str,
        cfg: &NodeConfig<'_>,
        env: &[(&'static str, String)],
    ) -> Result<Self> {
        let datadir = tempfile::tempdir().context("datadir tempdir")?;
        let mut handle = Self::start_with_datadir(name, datadir.path(), cfg, env).await?;
        handle.keep_alive.push(datadir);
        Ok(handle)
    }

    pub async fn start_with_datadir(
        name: &str,
        datadir: &std::path::Path,
        cfg: &NodeConfig<'_>,
        env: &[(&'static str, String)],
    ) -> Result<Self> {
        let handle = Self::spawn_with(name, datadir, cfg, env)?;
        wait_for_l2_rpc(&handle.l2_rpc_url(), Duration::from_mins(3)).await?;
        Ok(handle)
    }

    pub fn l2_rpc_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    /// Detached 1-wei spammer from `from_key`. Errors swallowed
    /// (`anvil_reorg` stales nonces). Aborted by runtime on test return.
    pub fn run_tx_spammer(&self, from_key: &'static str) {
        // 2× composer cadence: enough that every tick has ≥1 pending
        // tx; faster just hammers the RPC.
        let interval = COMPOSER_INTERVAL_MULTI / 2;
        let url = self.l2_rpc_url();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let _ =
                    send_l2_value_transfer(&url, from_key, ANVIL_ADDR_3, U256::from(1u64)).await;
            }
        });
    }

    /// Wait until this node observes the L1 reorg. Some runs legitimately
    /// have no stale confirmed batch on one node, in which case the
    /// correct deriver action is an explicit no-op. Without this check the
    /// reorg test would silently pass even if reorg detection regressed
    /// and some unrelated re-derivation path re-converged state.
    pub async fn wait_for_reorg_seen(&self, timeout: Duration) -> Result<()> {
        wait_for(timeout, || async {
            Ok((self.count_signals(&[
                signals::L1_REORG_DETECTED,
                signals::DERIVER_REORG_RETREATED,
                signals::DERIVER_REORG_NOOP,
            ])? > 0)
                .then_some(()))
        })
        .await
        .with_context(|| format!("{} deriver missed the reorg", self.name))
    }

    /// Assert that the node process is still running. `Child::try_wait`
    /// reaps and reports an exited child, unlike a PID signal probe, which
    /// also succeeds for an unreaped zombie process.
    pub fn assert_no_process_death(&self) {
        let mut child = self.child.lock().expect("lock node child process");
        match child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => panic!("{} process exited with {status}", self.name),
            Err(err) => panic!("failed to query {} process status: {err}", self.name),
        }
    }

    pub fn assert_no_divergence_failure_logs(&self) {
        self.assert_no_process_death();
        assert_eq!(
            self.count_signals(&[
                signals::DERIVER_STATE_DIVERGED_PRE,
                signals::DERIVER_STATE_DIVERGED_POST,
            ])
            .unwrap(),
            0,
            "{} emitted a state-divergence signal",
            self.name,
        );
        assert_eq!(
            self.count_log_lines_containing_any(&[
                "Fatal",
                "UnexpectedStaticFile",
                "engine rejected safe/finalized FCU",
                "payload builder returned no payload",
            ])
            .unwrap(),
            0,
            "{} logged a fatal-class failure",
            self.name,
        );
    }

    fn count_log_lines_containing_any(&self, patterns: &[&str]) -> Result<usize> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read node log {}", self.log_path.display()))?;
        Ok(contents
            .lines()
            .filter(|line| patterns.iter().any(|pattern| line.contains(pattern)))
            .count())
    }

    /// Decode the node's JSON tracing stream and count a stable signal field.
    /// Human messages may change without invalidating assertions.
    pub fn count_signal(&self, signal: &str) -> Result<usize> {
        self.count_signals(&[signal])
    }

    pub fn count_signals(&self, signals: &[&str]) -> Result<usize> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read signal stream {}", self.log_path.display()))?;
        Ok(contents
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|record| {
                record
                    .get("fields")
                    .and_then(|fields| fields.get("test_signal"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .filter(|signal| signals.contains(&signal.as_str()))
            .count())
    }

    /// Line cursor for a subsequent [`Self::signals_since`] query.
    pub fn signal_cursor(&self) -> Result<usize> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read signal stream {}", self.log_path.display()))?;
        Ok(contents.lines().count())
    }

    /// Structured records emitted after `cursor`. Signal values are exact
    /// tracing event names, while human-readable messages remain irrelevant.
    pub fn signals_since(&self, cursor: usize) -> Result<Vec<NodeSignal>> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read signal stream {}", self.log_path.display()))?;
        Ok(contents
            .lines()
            .skip(cursor)
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|record| {
                let fields = record.get("fields")?.as_object()?.clone();
                let name = fields.get("test_signal")?.as_str()?.to_owned();
                Some(NodeSignal { name, fields })
            })
            .collect())
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        let child = self
            .child
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = child.kill();
        let _ = child.wait();
    }
}

pub async fn l2_block_by_tag(rpc_url: &str, tag: BlockNumberOrTag) -> Result<Option<BlockNumHash>> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider.get_block_by_number(tag).await?;
    Ok(block.map(|b| BlockNumHash::new(b.header.number, b.header.hash)))
}

pub async fn l2_block_by_number(rpc_url: &str, number: u64) -> Result<Option<BlockNumHash>> {
    l2_block_by_tag(rpc_url, BlockNumberOrTag::Number(number)).await
}

pub async fn latest_block_snapshot(node: &NodeHandle) -> Result<Option<BlockNumHash>> {
    l2_block_by_tag(&node.l2_rpc_url(), BlockNumberOrTag::Latest).await
}

pub async fn wait_for_latest_height(
    node: &NodeHandle,
    min_height: u64,
    timeout: Duration,
) -> Result<BlockNumHash> {
    wait_for(timeout, || async {
        let latest = latest_block_snapshot(node).await?;
        Ok(latest.filter(|b| b.number >= min_height))
    })
    .await
}

/// Wait until all nodes' safe tags have reached at least `min_height` and the
/// nodes agree on the block hash at the common safe prefix height.
///
/// `min_height` is part of the test's contract: callers must choose a height
/// above the stale/setup baseline for the scenario under test. Otherwise this
/// helper could pass because every node still agrees on genesis or another old
/// safe block, without proving the scenario actually advanced.
pub async fn wait_for_safe_prefix_convergence(
    nodes: &[&NodeHandle],
    min_height: u64,
    timeout: Duration,
) -> Result<BlockNumHash> {
    wait_for_tag_prefix_convergence(nodes, BlockNumberOrTag::Safe, min_height, timeout).await
}

async fn wait_for_tag_prefix_convergence(
    nodes: &[&NodeHandle],
    tag: BlockNumberOrTag,
    min_height: u64,
    timeout: Duration,
) -> Result<BlockNumHash> {
    wait_for(timeout, || async {
        let mut tag_blocks = Vec::with_capacity(nodes.len());
        for node in nodes {
            let Some(block) = l2_block_by_tag(&node.l2_rpc_url(), tag).await? else {
                return Ok(None);
            };
            tag_blocks.push(block);
        }

        let target = tag_blocks
            .iter()
            .map(|b| b.number)
            .min()
            .unwrap_or_default();
        if target < min_height {
            return Ok(None);
        }

        let mut blocks = Vec::with_capacity(nodes.len());
        for node in nodes {
            let Some(block) = l2_block_by_number(&node.l2_rpc_url(), target).await? else {
                return Ok(None);
            };
            blocks.push(block);
        }
        let first = blocks[0];
        Ok(blocks.iter().all(|b| b.hash == first.hash).then_some(first))
    })
    .await
}

pub async fn wait_for<F, Fut, T>(timeout: Duration, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(v) = f().await? {
            return Ok(v);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!("timed out after {timeout:?}");
}

/// Intent-revealing wrapper around `rpc_url + deployment` for queries
/// that tests make repeatedly. Methods read like prose so tests stay
/// short and the invariants stay visible.
pub struct Chain<'a> {
    rpc_url: &'a str,
    eez_address: Address,
    deploy_block: u64,
    rollup_id: u64,
}

impl<'a> Chain<'a> {
    pub fn new(anvil: &'a Anvil, dep: &Deployment) -> Self {
        Self {
            rpc_url: &anvil.rpc_url,
            eez_address: dep.eez_address,
            deploy_block: dep.deploy_block,
            rollup_id: dep.rollup_id,
        }
    }

    pub async fn batches_posted(&self) -> Result<usize> {
        count_events(
            self.rpc_url,
            self.eez_address,
            IEEZ::BatchPosted::SIGNATURE_HASH,
            self.deploy_block,
        )
        .await
    }

    pub async fn executions_performed(&self) -> Result<usize> {
        count_events(
            self.rpc_url,
            self.eez_address,
            IEEZ::L2ExecutionPerformed::SIGNATURE_HASH,
            self.deploy_block,
        )
        .await
    }

    pub async fn entries_skipped(&self) -> Result<usize> {
        count_events(
            self.rpc_url,
            self.eez_address,
            IEEZ::L2TxSkipped::SIGNATURE_HASH,
            self.deploy_block,
        )
        .await
    }

    pub async fn state_root(&self) -> Result<B256> {
        state_root(self.rpc_url, self.eez_address, self.rollup_id).await
    }

    pub async fn latest_execution_state(&self) -> Result<Option<B256>> {
        latest_l2_execution_state(
            self.rpc_url,
            self.eez_address,
            self.rollup_id,
            self.deploy_block,
        )
        .await
    }

    /// All `newState` values the contract has attested via
    /// `L2ExecutionPerformed`. Use this to assert "the node imported
    /// a block whose stateRoot the contract has ever attested" without
    /// racing against the contract's advancing head.
    pub async fn executed_states(&self) -> Result<Vec<B256>> {
        all_l2_execution_states(
            self.rpc_url,
            self.eez_address,
            self.rollup_id,
            self.deploy_block,
        )
        .await
    }

    pub async fn block_number(&self) -> Result<u64> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url.parse()?);
        Ok(provider.get_block_number().await?)
    }

    /// Wait for L1 to advance `n` more blocks from now.
    pub async fn wait_for_l1_blocks(&self, n: u64, timeout: Duration) -> Result<u64> {
        let from = self.block_number().await?;
        wait_for_l1_blocks(self.rpc_url, from + n, timeout).await
    }

    /// Wait until ≥ `n` `BatchPosted` events visible.
    pub async fn wait_for_batches(&self, n: usize, timeout: Duration) -> Result<usize> {
        wait_for(timeout, || async {
            let count = self.batches_posted().await?;
            Ok((count >= n).then_some(count))
        })
        .await
    }

    /// Wait until `state_root()` becomes something other than `from`.
    pub async fn wait_for_state_change(&self, from: B256, timeout: Duration) -> Result<B256> {
        wait_for(timeout, || async {
            let root = self.state_root().await?;
            Ok((root != from).then_some(root))
        })
        .await
    }
}

/// Wait until L1's `block_number >= target`. Lets tests assert "the
/// composer had at least N opportunities to act" without arbitrary
/// sleeps — target = current + N is tied to L1 progress instead of wall-clock
/// assumptions.
pub async fn wait_for_l1_blocks(rpc_url: &str, target: u64, timeout: Duration) -> Result<u64> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for(timeout, || async {
        let n = provider.get_block_number().await?;
        Ok((n >= target).then_some(n))
    })
    .await
}

pub async fn state_root(rpc_url: &str, eez: Address, rollup_id: u64) -> Result<B256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let registry = IEEZ::new(eez, &provider);
    let r = registry.rollups(rollup_id).call().await?;
    Ok(r.stateRoot)
}

pub async fn rollup_ether_balance(rpc_url: &str, eez: Address, rollup_id: u64) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let registry = IEEZ::new(eez, &provider);
    Ok(registry.rollups(rollup_id).call().await?.etherBalance)
}

async fn account_balance(rpc_url: &str, address: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(provider.get_balance(address).await?)
}

/// Count events of `event_sig_hash` emitted by `contract` since
/// `from_block`. Used by tests that assert exact event counts.
pub async fn count_events(
    rpc_url: &str,
    contract: Address,
    event_sig_hash: B256,
    from_block: u64,
) -> Result<usize> {
    use alloy_rpc_types_eth::Filter;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let filter = Filter::new()
        .address(contract)
        .event_signature(event_sig_hash)
        .from_block(from_block);
    let logs = provider.get_logs(&filter).await?;
    Ok(logs.len())
}

/// Count `BatchPosted` events on the EEZ contract since `from_block`.
pub async fn batches_posted(l1_rpc: &str, eez: Address, from_block: u64) -> Result<usize> {
    count_events(l1_rpc, eez, IEEZ::BatchPosted::SIGNATURE_HASH, from_block).await
}

/// Return the `newState` of the latest `L2ExecutionPerformed` event
/// emitted by `contract` for `rollup_id`, or `None` if none exist.
/// Cross-checks the on-chain `stateRoot` against the per-batch event.
pub async fn latest_l2_execution_state(
    rpc_url: &str,
    contract: Address,
    rollup_id: u64,
    from_block: u64,
) -> Result<Option<B256>> {
    use alloy_rpc_types_eth::Filter;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let filter = Filter::new()
        .address(contract)
        .event_signature(IEEZ::L2ExecutionPerformed::SIGNATURE_HASH)
        .topic1(B256::from(U256::from(rollup_id)))
        .from_block(from_block);
    let mut logs = provider.get_logs(&filter).await?;
    let Some(last) = logs.pop() else {
        return Ok(None);
    };
    let decoded = IEEZ::L2ExecutionPerformed::decode_log(&last.inner)?;
    Ok(Some(decoded.newState))
}

/// All `newState` values from `L2ExecutionPerformed` events for
/// `rollup_id`, in emission order.
pub async fn all_l2_execution_states(
    rpc_url: &str,
    contract: Address,
    rollup_id: u64,
    from_block: u64,
) -> Result<Vec<B256>> {
    use alloy_rpc_types_eth::Filter;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let filter = Filter::new()
        .address(contract)
        .event_signature(IEEZ::L2ExecutionPerformed::SIGNATURE_HASH)
        .topic1(B256::from(U256::from(rollup_id)))
        .from_block(from_block);
    let logs = provider.get_logs(&filter).await?;
    logs.into_iter()
        .map(|log| {
            IEEZ::L2ExecutionPerformed::decode_log(&log.inner)
                .map(|d| d.newState)
                .map_err(Into::into)
        })
        .collect()
}

/// Wait until `node.safe.stateRoot` appears in the contract's
/// `L2ExecutionPerformed` history for this `Chain`. The attestation
/// set grows monotonically, so this doesn't race the contract's
/// advancing head: any past attestation matching the node's current
/// safe head proves the node imported a block the contract has, at
/// some point, declared canonical.
pub async fn wait_for_node_caught_up(
    node: &NodeHandle,
    chain: &Chain<'_>,
    timeout: Duration,
) -> Result<()> {
    wait_for(timeout, || async {
        let node_root = safe_block_state_root(&node.l2_rpc_url())
            .await
            .ok()
            .flatten();
        let attested = chain.executed_states().await.unwrap_or_default();
        Ok(match node_root {
            Some(n) if n != B256::ZERO && attested.contains(&n) => Some(()),
            _ => None,
        })
    })
    .await
}

/// Wait until the node's safe `stateRoot` appears in the contract's
/// `L2ExecutionPerformed` history for this `Chain` and differs from
/// `genesis_root`. The attestation set grows monotonically, so this doesn't
/// race the contract's advancing head: any past attestation matching the
/// node's current safe head proves the node imported a block the contract
/// has, at some point, declared canonical. Excluding `genesis_root` stops a
/// node stuck at genesis from trivially passing by matching an empty-block
/// attestation whose `newState` equals the registered initial state.
pub async fn wait_for_safe_state(
    node: &NodeHandle,
    chain: &Chain<'_>,
    genesis_root: B256,
    timeout: Duration,
) -> Result<()> {
    wait_for(timeout, || async {
        let node_root = async {
            let provider = ProviderBuilder::new().connect_http(node.l2_rpc_url().parse()?);
            let block = provider
                .get_block_by_number(alloy_rpc_types_eth::BlockNumberOrTag::Safe)
                .await?;
            Ok::<Option<B256>, anyhow::Error>(block.map(|b| b.header.state_root))
        }
        .await
        .ok()
        .flatten();
        let attested = chain.executed_states().await.unwrap_or_default();
        Ok(match node_root {
            Some(n) if n != B256::ZERO && n != genesis_root && attested.contains(&n) => Some(()),
            _ => None,
        })
    })
    .await
}

/// Wait until `node`'s safe block carries a state root that was not attested
/// before the scenario and is now present in the contract's execution history.
/// Returns that safe block's `(number, hash)` so peers can be checked against
/// the exact block, not just a repeated state root.
pub async fn wait_for_new_attested_safe_block(
    node: &NodeHandle,
    chain: &Chain<'_>,
    previous_states: &[B256],
    timeout: Duration,
) -> Result<(u64, B256)> {
    wait_for(timeout, || {
        let rpc = node.l2_rpc_url();
        async move {
            let provider = ProviderBuilder::new().connect_http(rpc.parse()?);
            let Some(block) = provider
                .get_block_by_number(alloy_rpc_types_eth::BlockNumberOrTag::Safe)
                .await?
            else {
                return Ok(None);
            };
            let number = block.header.number;
            let hash = block.header.hash;
            let root = block.header.state_root;
            if number == 0 || root == B256::ZERO || previous_states.contains(&root) {
                return Ok(None);
            }
            let attested = chain.executed_states().await?;
            Ok(attested.contains(&root).then_some((number, hash)))
        }
    })
    .await
}

/// Wait until `node`'s safe chain includes `hash` at `number`.
pub async fn wait_for_safe_chain_contains(
    node: &NodeHandle,
    number: u64,
    hash: B256,
    timeout: Duration,
) -> Result<()> {
    wait_for(timeout, || {
        let rpc = node.l2_rpc_url();
        async move {
            let Some((safe_number, _)) =
                block_number_and_hash_at(&rpc, alloy_rpc_types_eth::BlockNumberOrTag::Safe).await?
            else {
                return Ok(None);
            };
            if safe_number < number {
                return Ok(None);
            }
            let Some((_, actual_hash)) = block_number_and_hash_at(
                &rpc,
                alloy_rpc_types_eth::BlockNumberOrTag::Number(number),
            )
            .await?
            else {
                return Ok(None);
            };
            Ok((actual_hash == hash).then_some(()))
        }
    })
    .await
}

/// Return `env` with `key`'s value replaced by `value`. No-op if `key`
/// isn't present.
pub fn override_env(
    mut env: Vec<(&'static str, String)>,
    key: &str,
    value: &str,
) -> Vec<(&'static str, String)> {
    for (k, v) in &mut env {
        if *k == key {
            *v = value.to_string();
        }
    }
    env
}

// Cross-chain test fixture.

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_network::TxSignerSync;
use alloy_network::eip2718::Encodable2718;

/// Chain ID used by the embedded dev L1.
pub const DEV_CHAIN_ID: u64 = 1337;

pub const FIRST_ROLLUP_ID: u64 = 1;

sol! {
    #[sol(rpc)]
    interface IEEZProxy {
        event CrossChainProxyCreated(address indexed proxy, address indexed originalAddress, uint64 indexed originalRollupId);
        function createCrossChainProxy(address originalAddress, uint64 originalRollupId) external returns (address proxy);
    }
    #[sol(rpc)]
    interface IEEZL2Proxy {
        function createCrossChainProxy(address originalAddress, uint64 originalRollupId) external returns (address proxy);
        function computeCrossChainProxyAddress(address originalAddress, uint64 originalRollupId) external view returns (address proxy);
    }
    #[sol(rpc)]
    interface IValue {
        event ValueSet(address indexed by, uint256 newValue);
        function value() external view returns (uint256);
        function setValue(uint256 v) external returns (bool changed, uint256 newValue);
    }
    #[sol(rpc)]
    interface IValueNoRet {
        function value() external view returns (uint256);
        function setValue(uint256 v) external;
    }
    #[sol(rpc)]
    interface IEmptyCall {
        function calls() external view returns (uint256);
        function received() external view returns (uint256);
        function lastValue() external view returns (uint256);
        function setValue(uint256 next) external payable returns (uint256);
    }
    #[sol(rpc)]
    interface ISetterWrapper {
        function setViaProxy(uint256 v) external;
        function setSameValueTwice(uint256 v) external;
        function completedProxyCalls() external view returns (uint256);
        function lastChanged() external view returns (bool);
        function lastNewValue() external view returns (uint256);
    }
    #[sol(rpc)]
    interface IReturnData {
        function echo(bytes calldata value) external returns (bytes memory);
        function emptyBytes() external returns (bytes memory);
    }
    #[sol(rpc)]
    interface IReturnDataWrapper {
        function callAndRecord(bytes calldata data) external;
        function lastReturnDataLength() external view returns (uint256);
        function lastReturnDataHash() external view returns (bytes32);
    }
    #[sol(rpc)]
    interface INestedSetterInner {
        function completedProxyCalls() external view returns (uint256);
    }
    #[sol(rpc)]
    interface INestedSetterOuter {
        function setViaInner(uint256 v) external;
    }
    #[sol(rpc)]
    interface IEEZL2Direct {
        function executeCrossChainCall(address sourceAddress, bytes calldata callData) external payable returns (bytes memory);
    }
}

/// Write the L2 fixture genesis with a current timestamp.
fn write_l2_genesis_at(ts: u64) -> Result<(PathBuf, tempfile::TempDir)> {
    write_fixture_genesis(ts, None, "l2-genesis.json")
}

/// Write the L1 fixture genesis with the dev chain ID.
fn write_l1_dev_genesis_at(ts: u64) -> Result<(PathBuf, tempfile::TempDir)> {
    write_fixture_genesis(ts, Some(DEV_CHAIN_ID), "l1-genesis.json")
}

fn write_fixture_genesis(
    ts: u64,
    chain_id: Option<u64>,
    filename: &str,
) -> Result<(PathBuf, tempfile::TempDir)> {
    let raw = std::fs::read_to_string(reorg_genesis_path()).context("read fixture genesis")?;
    let mut genesis: alloy_genesis::Genesis =
        serde_json::from_str(&raw).context("parse fixture genesis")?;
    genesis.timestamp = ts;
    if let Some(id) = chain_id {
        genesis.config.chain_id = id;
    }
    let dir = tempfile::tempdir().context("genesis tempdir")?;
    let path = dir.path().join(filename);
    std::fs::write(&path, serde_json::to_vec(&genesis)?).context("write genesis")?;
    Ok((path, dir))
}

fn signer_of(key: &str) -> Result<PrivateKeySigner> {
    key.strip_prefix("0x")
        .unwrap_or(key)
        .parse()
        .context("parse signer key")
}

pub fn signer_address(key: &str) -> Result<Address> {
    Ok(signer_of(key)?.address())
}

/// Sign and submit an EIP-1559 transaction. `to == None` is a CREATE.
#[allow(clippy::too_many_arguments)]
pub async fn sign_and_send(
    rpc_url: &str,
    key: &str,
    chain_id: u64,
    nonce: u64,
    to: Option<Address>,
    value: U256,
    input: Vec<u8>,
    gas_limit: u64,
) -> Result<alloy_primitives::TxHash> {
    let signer = signer_of(key)?;
    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit,
        // Keep transactions includable while postBatch retains priority.
        max_fee_per_gas: 100_000_000_000,
        max_priority_fee_per_gas: 1_000_000_000,
        to: to.map_or(
            alloy_primitives::TxKind::Create,
            alloy_primitives::TxKind::Call,
        ),
        value,
        access_list: alloy_rpc_types_eth::AccessList::default(),
        input: input.into(),
    };
    let sig = signer.sign_transaction_sync(&mut tx)?;
    let env = TxEnvelope::from(tx.into_signed(sig));
    let hash = *env.tx_hash();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let _ = provider.send_raw_transaction(&env.encoded_2718()).await?;
    Ok(hash)
}

pub async fn pending_nonce(rpc_url: &str, key: &str) -> Result<u64> {
    let addr = signer_of(key)?.address();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(provider.get_transaction_count(addr).await?)
}

async fn wait_for_successful_receipt(
    rpc_url: &str,
    hash: alloy_primitives::TxHash,
    action: &str,
) -> Result<TransactionReceipt> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let receipt = wait_for(Duration::from_mins(1), || {
        let provider = provider.clone();
        async move { Ok(provider.get_transaction_receipt(hash).await?) }
    })
    .await?;
    if !receipt.status() {
        bail!("{action} reverted");
    }
    Ok(receipt)
}

async fn deploy_raw(
    rpc_url: &str,
    key: &str,
    chain_id: u64,
    artifact_path: &std::path::Path,
    constructor_args: Vec<u8>,
) -> Result<Address> {
    let artifact: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(artifact_path)
            .with_context(|| format!("read {}", artifact_path.display()))?,
    )?;
    let bytecode_hex = artifact["bytecode"]["object"]
        .as_str()
        .ok_or_else(|| anyhow!("bytecode.object missing in {}", artifact_path.display()))?
        .strip_prefix("0x")
        .unwrap_or_default();
    let mut data = hex::decode(bytecode_hex).context("decode bytecode")?;
    data.extend_from_slice(&constructor_args);

    let nonce = pending_nonce(rpc_url, key).await?;
    let hash = sign_and_send(
        rpc_url,
        key,
        chain_id,
        nonce,
        None,
        U256::ZERO,
        data,
        6_000_000,
    )
    .await?;
    let action = format!("deploy of {}", artifact_path.display());
    let receipt = wait_for_successful_receipt(rpc_url, hash, &action).await?;
    receipt
        .contract_address
        .ok_or_else(|| anyhow!("no contract_address for {}", artifact_path.display()))
}

/// Deploy EEZ + MockECDSAProofSystem + Rollup on the embedded dev L1 and
/// register the rollup. Addresses are deterministic (`CREATE(deployer, 0/1/2)`).
pub async fn deploy_protocol_dev(
    l1_rpc: &str,
    key: &str,
    initial_state: B256,
) -> Result<Deployment> {
    let signer = signer_of(key)?;
    let signer_addr = signer.address();
    let out = repo_root().join("contracts/out");

    let eez_address = deploy_raw(
        l1_rpc,
        key,
        DEV_CHAIN_ID,
        &out.join("EEZ.sol/EEZ.json"),
        signer_addr.abi_encode(),
    )
    .await?;
    let provider = ProviderBuilder::new().connect_http(l1_rpc.parse()?);
    let deploy_block = provider.get_block_number().await?;

    let mock_ps_address = deploy_raw(
        l1_rpc,
        key,
        DEV_CHAIN_ID,
        &out.join("MockECDSAProofSystem.sol/MockECDSAProofSystem.json"),
        signer_addr.abi_encode(),
    )
    .await?;

    let vkeys: Vec<B256> = vec![B256::from_slice(&{
        let mut padded = [0u8; 32];
        padded[12..].copy_from_slice(signer_addr.as_slice());
        padded
    })];
    let rollup_manager_address = deploy_raw(
        l1_rpc,
        key,
        DEV_CHAIN_ID,
        &out.join("Rollup.sol/Rollup.json"),
        (
            eez_address,
            signer_addr,
            U256::from(1u64),
            vec![mock_ps_address],
            vkeys,
        )
            .abi_encode_params(),
    )
    .await?;

    let calldata = IEEZ::registerRollupCall {
        rollupContract: rollup_manager_address,
        initialState: initial_state,
    }
    .abi_encode();
    let nonce = pending_nonce(l1_rpc, key).await?;
    let hash = sign_and_send(
        l1_rpc,
        key,
        DEV_CHAIN_ID,
        nonce,
        Some(eez_address),
        U256::ZERO,
        calldata,
        1_000_000,
    )
    .await?;
    wait_for_successful_receipt(l1_rpc, hash, "registerRollup").await?;
    let registry = IEEZ::new(eez_address, &provider);
    let rollup_id: u64 = registry.rollupCounter().call().await?.try_into()?;

    Ok(Deployment {
        eez_address,
        deploy_block,
        mock_ps_address,
        rollup_manager_address,
        rollup_id,
    })
}

async fn deploy_value(rpc_url: &str, key: &str, chain_id: u64, initial: U256) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("Value.sol/Value.json"),
        initial.abi_encode(),
    )
    .await
}

async fn deploy_empty_call(rpc_url: &str, key: &str, chain_id: u64) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("EmptyCall.sol/EmptyCall.json"),
        Vec::new(),
    )
    .await
}

pub async fn deploy_value_no_ret(
    rpc_url: &str,
    key: &str,
    chain_id: u64,
    initial: U256,
) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("ValueNoRet.sol/ValueNoRet.json"),
        initial.abi_encode(),
    )
    .await
}

async fn deploy_return_data(rpc_url: &str, key: &str, chain_id: u64) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("ReturnData.sol/ReturnData.json"),
        Vec::new(),
    )
    .await
}

async fn deploy_return_data_wrapper(
    rpc_url: &str,
    key: &str,
    chain_id: u64,
    proxy: Address,
) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("ReturnDataWrapper.sol/ReturnDataWrapper.json"),
        proxy.abi_encode(),
    )
    .await
}

pub async fn deploy_nested_setter_inner(
    rpc_url: &str,
    key: &str,
    chain_id: u64,
    proxy: Address,
) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("NestedSetterWrapper.sol/NestedSetterInner.json"),
        proxy.abi_encode(),
    )
    .await
}

pub async fn deploy_nested_setter_outer(
    rpc_url: &str,
    key: &str,
    chain_id: u64,
    inner: Address,
) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("NestedSetterWrapper.sol/NestedSetterOuter.json"),
        inner.abi_encode(),
    )
    .await
}

pub async fn deploy_setter_wrapper(
    rpc_url: &str,
    key: &str,
    chain_id: u64,
    proxy: Address,
) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("SetterWrapper.sol/SetterWrapper.json"),
        proxy.abi_encode(),
    )
    .await
}

pub async fn value_no_ret(rpc_url: &str, value_addr: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(IValueNoRet::new(value_addr, &provider)
        .value()
        .call()
        .await?)
}

pub async fn empty_call_state(rpc_url: &str, target: Address) -> Result<(U256, U256, U256)> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let target = IEmptyCall::new(target, &provider);
    Ok((
        target.calls().call().await?,
        target.received().call().await?,
        target.lastValue().call().await?,
    ))
}

pub async fn completed_proxy_calls(rpc_url: &str, wrapper: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(ISetterWrapper::new(wrapper, &provider)
        .completedProxyCalls()
        .call()
        .await?)
}

pub async fn last_proxy_result(rpc_url: &str, wrapper: Address) -> Result<(bool, U256)> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let wrapper = ISetterWrapper::new(wrapper, &provider);
    Ok((
        wrapper.lastChanged().call().await?,
        wrapper.lastNewValue().call().await?,
    ))
}

pub async fn return_data_length(rpc_url: &str, wrapper: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let wrapper = IReturnDataWrapper::new(wrapper, &provider);
    Ok(wrapper.lastReturnDataLength().call().await?)
}

pub async fn return_data_hash(rpc_url: &str, wrapper: Address) -> Result<B256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(IReturnDataWrapper::new(wrapper, &provider)
        .lastReturnDataHash()
        .call()
        .await?)
}

pub async fn nested_proxy_calls(rpc_url: &str, inner: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(INestedSetterInner::new(inner, &provider)
        .completedProxyCalls()
        .call()
        .await?)
}

/// Create an outbound proxy through the `EEZL2` predeploy.
pub async fn create_l2_cross_chain_proxy(
    l2_rpc: &str,
    key: &str,
    target: Address,
    original_rollup_id: u64,
) -> Result<Address> {
    let provider = ProviderBuilder::new().connect_http(l2_rpc.parse()?);
    let eezl2 = IEEZL2Proxy::new(EEZL2_ADDRESS, &provider);
    let proxy = eezl2
        .computeCrossChainProxyAddress(target, original_rollup_id)
        .call()
        .await?;
    let chain_id = provider.get_chain_id().await?;
    let nonce = pending_nonce(l2_rpc, key).await?;
    let hash = sign_and_send(
        l2_rpc,
        key,
        chain_id,
        nonce,
        Some(EEZL2_ADDRESS),
        U256::ZERO,
        IEEZL2Proxy::createCrossChainProxyCall {
            originalAddress: target,
            originalRollupId: original_rollup_id,
        }
        .abi_encode(),
        1_500_000,
    )
    .await?;
    wait_for_successful_receipt(l2_rpc, hash, "create L2 cross-chain proxy").await?;
    Ok(proxy)
}

/// Create an inbound proxy and read its address from the emitted event.
pub async fn create_cross_chain_proxy(
    l1_rpc: &str,
    key: &str,
    eez: Address,
    target: Address,
    rollup_id: u64,
) -> Result<Address> {
    let calldata = IEEZProxy::createCrossChainProxyCall {
        originalAddress: target,
        originalRollupId: rollup_id,
    }
    .abi_encode();
    let nonce = pending_nonce(l1_rpc, key).await?;
    let hash = sign_and_send(
        l1_rpc,
        key,
        DEV_CHAIN_ID,
        nonce,
        Some(eez),
        U256::ZERO,
        calldata,
        2_000_000,
    )
    .await?;
    let receipt = wait_for_successful_receipt(l1_rpc, hash, "createCrossChainProxy").await?;
    receipt
        .inner
        .logs()
        .iter()
        .find_map(|log| {
            IEEZProxy::CrossChainProxyCreated::decode_log(&log.inner)
                .ok()
                .map(|e| e.proxy)
        })
        .ok_or_else(|| anyhow!("CrossChainProxyCreated event not found in receipt"))
}

pub async fn l2_value(l2_rpc: &str, value_addr: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(l2_rpc.parse()?);
    Ok(IValue::new(value_addr, &provider).value().call().await?)
}

pub async fn l2_balance(l2_rpc: &str, addr: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(l2_rpc.parse()?);
    Ok(provider.get_balance(addr).await?)
}

pub async fn account_code(rpc_url: &str, addr: Address) -> Result<Bytes> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(provider.get_code_at(addr).await?)
}

pub async fn receipt_ok(rpc_url: &str, hash: alloy_primitives::TxHash) -> Result<Option<bool>> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(provider
        .get_transaction_receipt(hash)
        .await?
        .map(|r| r.status()))
}

/// Cross-chain test node configuration.
pub struct CrossChainConfig {
    pub l1_http_port: u16,
    pub l1_auth_port: u16,
    pub l1_p2p_port: u16,
    pub l1_xchain_port: u16,
    pub l2_xchain_port: u16,
    pub eez_address: Address,
    pub mock_ps_address: Address,
    pub rollup_manager_address: Address,
    pub rollup_id: u64,
    pub initial_state: B256,
    /// Kept separate from the poster key for deterministic CREATE addresses.
    pub deployer_key: &'static str,
    pub poster_key: &'static str,
    pub l1_genesis: (PathBuf, tempfile::TempDir),
    pub l2_genesis: (PathBuf, tempfile::TempDir),
}

impl CrossChainConfig {
    pub fn new() -> Result<Self> {
        let deployer_key = ANVIL_KEY_1;
        let poster_key = ANVIL_KEY;
        let deployer = signer_of(deployer_key)?.address();
        // Deploy order: EEZ(0), MockPS(1), Rollup(2).
        let eez_address = deployer.create(0);
        let mock_ps_address = deployer.create(1);
        let rollup_manager_address = deployer.create(2);
        let initial_state = reorg_genesis_state_root()?;
        let ts = now_unix_secs();
        // Avoid the HTTP and adjacent WS ports.
        let l1_http_port = free_port();
        let mut l1_auth_port = free_port();
        while l1_auth_port == l1_http_port || l1_auth_port == l1_http_port.saturating_add(1) {
            l1_auth_port = free_port();
        }
        Ok(Self {
            l1_http_port,
            l1_auth_port,
            l1_p2p_port: free_port(),
            l1_xchain_port: free_port(),
            l2_xchain_port: free_port(),
            eez_address,
            mock_ps_address,
            rollup_manager_address,
            rollup_id: FIRST_ROLLUP_ID,
            initial_state,
            deployer_key,
            poster_key,
            l1_genesis: write_l1_dev_genesis_at(ts)?,
            l2_genesis: write_l2_genesis_at(ts)?,
        })
    }

    pub fn l1_rpc_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.l1_http_port)
    }

    /// L1→L2 transaction ingress.
    pub fn l1_xchain_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.l1_xchain_port)
    }

    /// L2→L1 transaction ingress.
    pub fn l2_xchain_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.l2_xchain_port)
    }

    pub fn env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("EEZ_L1_EMBEDDED", "1".to_string()),
            ("EEZ_L1_CHAIN", "testing".to_string()),
            ("EEZ_L1_CHAIN_ID", DEV_CHAIN_ID.to_string()),
            ("EEZ_L1_RPC_URL", self.l1_rpc_url()),
            ("EEZ_L1_BUILDER_RPC_URL", self.l1_rpc_url()),
            ("EEZ_L1_XCHAIN_PORT", self.l1_xchain_port.to_string()),
            ("EEZ_L2_XCHAIN_PORT", self.l2_xchain_port.to_string()),
            ("EEZ_L1_HTTP_PORT", self.l1_http_port.to_string()),
            ("EEZ_L1_AUTH_PORT", self.l1_auth_port.to_string()),
            ("EEZ_L1_P2P_PORT", self.l1_p2p_port.to_string()),
            (
                "EEZ_L1_CHAIN_PATH",
                self.l1_genesis.0.to_string_lossy().into_owned(),
            ),
            ("EEZ_L1_POSTER_KEY", self.poster_key.to_string()),
            // MockECDSA authorizes the deployer.
            ("EEZ_PROOF_SIGNER_KEY", self.deployer_key.to_string()),
            ("EEZ_L2_SYSTEM_KEY", L2_SYSTEM_KEY.to_string()),
            ("EEZL2_ADDRESS", format!("{EEZL2_ADDRESS:#x}")),
            (
                "EEZ_L1_BLOCK_TIME_MS",
                CROSS_CHAIN_L1_BLOCK_TIME_MS.to_string(),
            ),
            (
                "EEZ_L2_BLOCK_TIME_MS",
                CROSS_CHAIN_L2_BLOCK_TIME_MS.to_string(),
            ),
            ("EEZ_PROOF_TIME_MS", "1000".to_string()),
            ("EEZ_SUBMISSION_SLACK_MS", "100".to_string()),
            ("EEZ_REGISTRY_ADDRESS", format!("{:#x}", self.eez_address)),
            ("EEZ_REGISTRY_DEPLOY_BLOCK", "0".to_string()),
            (
                "EEZ_ECDSA_PROOF_SYSTEM_ADDRESS",
                format!("{:#x}", self.mock_ps_address),
            ),
            (
                "EEZ_ROLLUP_MANAGER_ADDRESS",
                format!("{:#x}", self.rollup_manager_address),
            ),
            ("EEZ_ROLLUP_ID", self.rollup_id.to_string()),
            ("EEZ_COMPOSER_INTERVAL_SECS", "1".to_string()),
            ("EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES", "false".to_string()),
            (
                "EEZ_L2_DATADIR",
                "/tmp/unused-overridden-by-flag".to_string(),
            ),
            (
                "RUST_LOG",
                std::env::var("EEZ_TEST_LOG")
                    .unwrap_or_else(|_| "warn,eez_node=info,eez_l1=info".to_string()),
            ),
            (
                TEST_L2_GENESIS_ENV,
                self.l2_genesis.0.to_string_lossy().into_owned(),
            ),
        ]
    }
}

pub const SETUP_TIMEOUT: Duration = Duration::from_secs(90);
pub const SETTLE_TIMEOUT: Duration = Duration::from_mins(3);
const CROSS_CHAIN_L1_BLOCK_TIME_MS: u64 = 5_000;
const CROSS_CHAIN_L2_BLOCK_TIME_MS: u64 = 1_000;
const CROSS_CHAIN_SYNC_INTERVAL: u64 = CROSS_CHAIN_L1_BLOCK_TIME_MS / CROSS_CHAIN_L2_BLOCK_TIME_MS;

/// Separate deployer keeps CREATE addresses deterministic.
pub const TARGET_DEPLOYER: &str = ANVIL_KEY_3;
pub const INBOUND_USER: &str = ANVIL_KEY_2;
pub const OUTBOUND_USER: &str = ANVIL_KEY_4;
/// Valid key without genesis funding.
pub const UNFUNDED_KEY: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

/// Shared cross-chain integration fixture.
pub struct CrossChainWorld {
    pub node: NodeHandle,
    pub cfg: CrossChainConfig,
    pub dep: Deployment,
    pub l2_chain_id: u64,
    pub value_l2: Address,
    pub inbound_no_ret: Address,
    pub outbound_value: Address,
    pub outbound_no_ret: Address,
    pub recipient: Address,
    pub withdrawal_recipient: Address,
    pub setter_proxy: Address,
    pub deposit_proxy: Address,
    pub inbound_no_ret_proxy: Address,
    pub inbound_wrapper: Address,
    pub outbound_proxy: Address,
    pub outbound_no_ret_proxy: Address,
    pub withdrawal_proxy: Address,
    pub outbound_wrapper: Address,
    _datadir: tempfile::TempDir,
}

pub struct CrossChainEmptyCallWorld {
    pub world: CrossChainWorld,
    pub empty_call_l2: Address,
    pub empty_call_proxy: Address,
}

impl std::ops::Deref for CrossChainEmptyCallWorld {
    type Target = CrossChainWorld;

    fn deref(&self) -> &Self::Target {
        &self.world
    }
}

pub struct CrossChainReturnDataWorld {
    pub world: CrossChainWorld,
    pub return_data_wrapper: Address,
}

impl std::ops::Deref for CrossChainReturnDataWorld {
    type Target = CrossChainWorld;

    fn deref(&self) -> &Self::Target {
        &self.world
    }
}

pub struct CrossChainNestedSetterWorld {
    pub world: CrossChainWorld,
    pub nested_setter_inner: Address,
    pub nested_setter_outer: Address,
}

impl std::ops::Deref for CrossChainNestedSetterWorld {
    type Target = CrossChainWorld;

    fn deref(&self) -> &Self::Target {
        &self.world
    }
}

pub struct CrossChainOutboundReturnDataWorld {
    pub world: CrossChainWorld,
    pub return_data_wrapper: Address,
}

impl std::ops::Deref for CrossChainOutboundReturnDataWorld {
    type Target = CrossChainWorld;

    fn deref(&self) -> &Self::Target {
        &self.world
    }
}

pub struct CrossChainCodelessWorld {
    pub world: CrossChainWorld,
    pub inbound_wrapper: Address,
    pub outbound_wrapper: Address,
}

impl std::ops::Deref for CrossChainCodelessWorld {
    type Target = CrossChainWorld;

    fn deref(&self) -> &Self::Target {
        &self.world
    }
}

impl CrossChainWorld {
    pub fn l1_rpc(&self) -> String {
        self.cfg.l1_rpc_url()
    }
    pub fn l1_xchain(&self) -> String {
        self.cfg.l1_xchain_url()
    }
    pub fn l2_xchain(&self) -> String {
        self.cfg.l2_xchain_url()
    }
    pub fn l2_rpc(&self) -> String {
        self.node.l2_rpc_url()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScenarioDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Debug)]
pub struct ScenarioCall {
    pub to: Address,
    pub value: U256,
    pub data: Vec<u8>,
    pub gas_limit: u64,
}

impl ScenarioCall {
    pub fn new(to: Address, data: Vec<u8>) -> Self {
        Self {
            to,
            value: U256::ZERO,
            data,
            gas_limit: 900_000,
        }
    }
}

pub trait IntoTestU256 {
    fn into_test_u256(self) -> U256;
}

impl IntoTestU256 for U256 {
    fn into_test_u256(self) -> U256 {
        self
    }
}

impl IntoTestU256 for u64 {
    fn into_test_u256(self) -> U256 {
        U256::from(self)
    }
}

pub fn setter_call(proxy: Address, value: impl IntoTestU256) -> ScenarioCall {
    ScenarioCall::new(
        proxy,
        IValue::setValueCall {
            v: value.into_test_u256(),
        }
        .abi_encode(),
    )
}

#[derive(Clone, Copy, Debug)]
pub struct ValueRead(Address);

pub const fn value_read(contract: Address) -> ValueRead {
    ValueRead(contract)
}

#[derive(Clone, Copy, Debug)]
enum StateSide {
    L1,
    L2,
}

#[derive(Clone, Copy, Debug)]
struct StateExpectation {
    side: StateSide,
    read: ValueRead,
    expected: U256,
}

#[derive(Clone, Debug)]
struct ExecutedScenarioAction {
    direction: ScenarioDirection,
    value: U256,
    nonce: u64,
    hash: alloy_primitives::TxHash,
}

#[derive(Debug)]
struct StandardOracleSnapshot {
    signal_cursor: usize,
    execution_states: Vec<B256>,
    safe: Option<(u64, B256)>,
    finalized: Option<(u64, B256)>,
    rollup_ether_balance: U256,
    eez_contract_balance: U256,
    inbound_nonce: u64,
    outbound_nonce: u64,
    grid_residue: Option<u64>,
}

impl StandardOracleSnapshot {
    async fn capture(world: &CrossChainWorld) -> Result<Self> {
        let l1_rpc = world.l1_rpc();
        let l2_rpc = world.l2_rpc();
        let prior_signals = world.node.signals_since(0)?;
        let grid_residue = prior_signals.iter().rev().find_map(|signal| {
            signal
                .fields
                .get("sync_height")
                .and_then(serde_json::Value::as_u64)
                .map(|height| height % CROSS_CHAIN_SYNC_INTERVAL)
        });
        Ok(Self {
            signal_cursor: world.node.signal_cursor()?,
            execution_states: all_l2_execution_states(
                &l1_rpc,
                world.cfg.eez_address,
                world.cfg.rollup_id,
                world.dep.deploy_block,
            )
            .await?,
            safe: block_number_and_hash_at(&l2_rpc, BlockNumberOrTag::Safe).await?,
            finalized: block_number_and_hash_at(&l2_rpc, BlockNumberOrTag::Finalized).await?,
            rollup_ether_balance: rollup_ether_balance(
                &l1_rpc,
                world.cfg.eez_address,
                world.cfg.rollup_id,
            )
            .await?,
            eez_contract_balance: account_balance(&l1_rpc, world.cfg.eez_address).await?,
            inbound_nonce: pending_nonce(&l1_rpc, INBOUND_USER).await?,
            outbound_nonce: pending_nonce(&l2_rpc, OUTBOUND_USER).await?,
            grid_residue,
        })
    }

    async fn assert_after(
        &self,
        world: &CrossChainWorld,
        actions: &[ExecutedScenarioAction],
        expect_settled: bool,
        scenario_name: &str,
    ) -> Result<()> {
        world.node.assert_no_process_death();

        let l1_rpc = world.l1_rpc();
        let l2_rpc = world.l2_rpc();
        let records = world.node.signals_since(self.signal_cursor)?;
        let has_reorg = records.iter().any(|record| {
            matches!(
                record.name.as_str(),
                signals::L1_REORG_DETECTED
                    | signals::DERIVER_REORG_RETREATED
                    | signals::DERIVER_REORG_NOOP
            )
        });
        if records.iter().any(|record| {
            matches!(
                record.name.as_str(),
                signals::DERIVER_STATE_DIVERGED_PRE
                    | signals::DERIVER_STATE_DIVERGED_POST
                    | signals::DERIVER_RESYNC_FAILED
                    | signals::DERIVER_COMMITTER_CLOSED
                    | signals::NODE_BOOT_CATCH_UP_FAILED
                    | signals::NODE_PANIC
            )
        }) {
            bail!("{scenario_name}: node emitted a divergence, fatal, or panic signal");
        }

        let safe = block_number_and_hash_at(&l2_rpc, BlockNumberOrTag::Safe).await?;
        let finalized = block_number_and_hash_at(&l2_rpc, BlockNumberOrTag::Finalized).await?;
        if let (Some(before), Some(after)) = (self.safe, safe)
            && after.0 < before.0
            && !has_reorg
        {
            bail!(
                "{scenario_name}: safe head retreated {} -> {} without an L1 reorg signal",
                before.0,
                after.0
            );
        }
        if let (Some(before), Some(after)) = (self.finalized, finalized)
            && after.0 < before.0
        {
            bail!(
                "{scenario_name}: finalized head retreated {} -> {}",
                before.0,
                after.0
            );
        }

        let mut observed_safe = self.safe.map(|head| head.0).unwrap_or(0);
        let mut reorg_authorized = false;
        let mut observed_finalized = self.finalized.map(|head| head.0).unwrap_or(0);
        let mut applied_entries = 0usize;
        let mut settlement_points = Vec::new();
        let mut grid_residue = self.grid_residue;
        for record in &records {
            match record.name.as_str() {
                signals::L1_REORG_DETECTED
                | signals::DERIVER_REORG_RETREATED
                | signals::DERIVER_REORG_NOOP => reorg_authorized = true,
                signals::DERIVER_SAFE_ADVANCED => {
                    let next = record.u64("to_block")?;
                    if next < observed_safe && !reorg_authorized {
                        bail!(
                            "{scenario_name}: safe signal retreated {observed_safe} -> {next} without an L1 reorg signal"
                        );
                    }
                    observed_safe = next;
                    reorg_authorized = false;
                    let applied = record.u64("applied_entries")? as usize;
                    applied_entries = applied_entries
                        .checked_add(applied)
                        .ok_or_else(|| anyhow!("{scenario_name}: applied-entry count overflow"))?;
                    settlement_points.push((
                        applied,
                        record.b256("settled_state_root")?,
                        record.b256("new_safe_state_root")?,
                    ));
                }
                signals::DERIVER_FINALIZED_ADVANCED => {
                    let next = record.u64("l2_finalized")?;
                    if next < observed_finalized {
                        bail!(
                            "{scenario_name}: finalized signal retreated {observed_finalized} -> {next}"
                        );
                    }
                    observed_finalized = next;
                }
                signals::COMPOSER_BUNDLE_DISPATCHED
                | signals::COMPOSER_PHASE1_BUNDLE_DISPATCHED => {
                    assert_sync_grid(record, &mut grid_residue, scenario_name)?;
                }
                signals::DERIVER_SYNC_BLOCK_BUILT => {
                    assert_sync_grid(record, &mut grid_residue, scenario_name)?;
                }
                _ => {}
            }
        }

        let execution_states = all_l2_execution_states(
            &l1_rpc,
            world.cfg.eez_address,
            world.cfg.rollup_id,
            world.dep.deploy_block,
        )
        .await?;
        let new_execution_states = execution_states
            .get(self.execution_states.len()..)
            .ok_or_else(|| anyhow!("{scenario_name}: L2ExecutionPerformed history retreated"))?;
        if new_execution_states.len() != applied_entries {
            bail!(
                "{scenario_name}: {} L2ExecutionPerformed events != {applied_entries} applied entries",
                new_execution_states.len()
            );
        }
        let mut event_offset = 0usize;
        for (applied, settled_root, safe_root) in settlement_points {
            event_offset += applied;
            let event_root = new_execution_states
                .get(event_offset.saturating_sub(1))
                .ok_or_else(|| anyhow!("{scenario_name}: settlement has no matching L1 event"))?;
            if *event_root != settled_root || settled_root != safe_root {
                bail!("{scenario_name}: L1 and L2 roots differ at settlement point {event_offset}");
            }
        }
        if safe.is_some() {
            if expect_settled {
                let committed =
                    state_root(&l1_rpc, world.cfg.eez_address, world.cfg.rollup_id).await?;
                let safe_root = safe_block_state_root(&l2_rpc)
                    .await?
                    .ok_or_else(|| anyhow!("{scenario_name}: safe L2 block is absent"))?;
                if committed != safe_root {
                    bail!("{scenario_name}: L1 committed root != L2 safe root");
                }
            }
        }

        assert_action_and_nonce_invariants(self, world, actions, scenario_name).await?;
        if expect_settled {
            assert_value_conservation(self, world, actions, scenario_name).await?;
        }
        Ok(())
    }
}

fn assert_sync_grid(
    record: &NodeSignal,
    residue: &mut Option<u64>,
    scenario_name: &str,
) -> Result<()> {
    let height = record.u64("sync_height")?;
    let actual = height % CROSS_CHAIN_SYNC_INTERVAL;
    match *residue {
        Some(expected) if expected != actual => bail!(
            "{scenario_name}: sync block {height} is off the deterministic K={} grid (residue {actual}, expected {expected})",
            CROSS_CHAIN_SYNC_INTERVAL
        ),
        None => *residue = Some(actual),
        _ => {}
    }
    Ok(())
}

async fn assert_action_and_nonce_invariants(
    before: &StandardOracleSnapshot,
    world: &CrossChainWorld,
    actions: &[ExecutedScenarioAction],
    scenario_name: &str,
) -> Result<()> {
    let mut hashes = HashSet::with_capacity(actions.len());
    let mut inbound = before.inbound_nonce;
    let mut outbound = before.outbound_nonce;
    for action in actions {
        if !hashes.insert(action.hash) {
            bail!("{scenario_name}: action hash {} was reused", action.hash);
        }
        let expected = match action.direction {
            ScenarioDirection::Inbound => &mut inbound,
            ScenarioDirection::Outbound => &mut outbound,
        };
        if action.nonce != *expected {
            bail!(
                "{scenario_name}: {:?} sender nonce {} was reused or skipped; expected {}",
                action.direction,
                action.nonce,
                *expected
            );
        }
        *expected += 1;
    }
    let actual_inbound = pending_nonce(&world.l1_rpc(), INBOUND_USER).await?;
    let actual_outbound = pending_nonce(&world.l2_rpc(), OUTBOUND_USER).await?;
    if actual_inbound < inbound || actual_outbound < outbound {
        bail!("{scenario_name}: a sender nonce failed to advance monotonically");
    }
    Ok(())
}

async fn assert_value_conservation(
    before: &StandardOracleSnapshot,
    world: &CrossChainWorld,
    actions: &[ExecutedScenarioAction],
    scenario_name: &str,
) -> Result<()> {
    let mut inbound = U256::ZERO;
    let mut outbound = U256::ZERO;
    for action in actions {
        match action.direction {
            ScenarioDirection::Inbound => inbound += action.value,
            ScenarioDirection::Outbound => outbound += action.value,
        }
    }
    let after =
        rollup_ether_balance(&world.l1_rpc(), world.cfg.eez_address, world.cfg.rollup_id).await?;
    if before.rollup_ether_balance + inbound != after + outbound {
        bail!(
            "{scenario_name}: bridged value was not conserved (before={}, inbound={}, after={}, outbound={})",
            before.rollup_ether_balance,
            inbound,
            after,
            outbound
        );
    }
    let contract_balance = account_balance(&world.l1_rpc(), world.cfg.eez_address).await?;
    if contract_balance < after
        || contract_balance + outbound < before.eez_contract_balance + inbound
    {
        bail!("{scenario_name}: L1 EEZ balance no longer backs bridged rollup funds");
    }
    Ok(())
}

/// Declarative cross-chain case. Calls and state oracles are data, so a matrix
/// can be expressed as a `Vec<Scenario>` and run by [`run_scenarios`].
#[derive(Debug)]
pub struct Scenario {
    name: String,
    calls: Vec<(ScenarioDirection, ScenarioCall)>,
    states: Vec<StateExpectation>,
    expect_settled: bool,
}

impl Scenario {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            calls: Vec::new(),
            states: Vec::new(),
            expect_settled: false,
        }
    }

    pub fn inbound(mut self, call: ScenarioCall) -> Self {
        self.calls.push((ScenarioDirection::Inbound, call));
        self
    }

    pub fn outbound(mut self, call: ScenarioCall) -> Self {
        self.calls.push((ScenarioDirection::Outbound, call));
        self
    }

    pub fn expect_l2_state(mut self, read: ValueRead, expected: impl IntoTestU256) -> Self {
        self.states.push(StateExpectation {
            side: StateSide::L2,
            read,
            expected: expected.into_test_u256(),
        });
        self
    }

    pub fn expect_l1_state(mut self, read: ValueRead, expected: impl IntoTestU256) -> Self {
        self.states.push(StateExpectation {
            side: StateSide::L1,
            read,
            expected: expected.into_test_u256(),
        });
        self
    }

    pub const fn expect_settled_fully(mut self) -> Self {
        self.expect_settled = true;
        self
    }

    pub async fn run(self, world: &CrossChainWorld) -> Result<()> {
        let l1_rpc = world.l1_rpc();
        let l2_rpc = world.l2_rpc();
        let oracle = StandardOracleSnapshot::capture(world).await?;
        let mut actions = Vec::with_capacity(self.calls.len());
        for (direction, call) in self.calls {
            let (ingress, receipt_rpc, key, chain_id) = match direction {
                ScenarioDirection::Inbound => (
                    world.l1_xchain(),
                    l1_rpc.as_str(),
                    INBOUND_USER,
                    DEV_CHAIN_ID,
                ),
                ScenarioDirection::Outbound => (
                    world.l2_xchain(),
                    l2_rpc.as_str(),
                    OUTBOUND_USER,
                    world.l2_chain_id,
                ),
            };
            let nonce = pending_nonce(receipt_rpc, key).await?;
            let hash = sign_and_send(
                &ingress,
                key,
                chain_id,
                nonce,
                Some(call.to),
                call.value,
                call.data,
                call.gas_limit,
            )
            .await
            .with_context(|| format!("{}: submit {direction:?} call", self.name))?;
            wait_for(SETTLE_TIMEOUT, || async {
                Ok(receipt_ok(receipt_rpc, hash)
                    .await?
                    .filter(|success| *success))
            })
            .await
            .with_context(|| {
                format!(
                    "{}: {direction:?} call did not settle successfully",
                    self.name
                )
            })?;
            actions.push(ExecutedScenarioAction {
                direction,
                value: call.value,
                nonce,
                hash,
            });
        }

        for expectation in self.states {
            let rpc = match expectation.side {
                StateSide::L1 => l1_rpc.as_str(),
                StateSide::L2 => l2_rpc.as_str(),
            };
            wait_for(SETTLE_TIMEOUT, || async {
                Ok(
                    (l2_value(rpc, expectation.read.0).await? == expectation.expected)
                        .then_some(()),
                )
            })
            .await
            .with_context(|| format!("{}: state expectation failed", self.name))?;
        }

        if self.expect_settled {
            wait_for(SETTLE_TIMEOUT, || async {
                let committed =
                    state_root(&l1_rpc, world.cfg.eez_address, world.cfg.rollup_id).await?;
                let safe = safe_block_state_root(&l2_rpc).await?;
                Ok(safe.filter(|root| *root == committed).map(|_| ()))
            })
            .await
            .with_context(|| {
                format!("{}: committed and safe roots did not reconcile", self.name)
            })?;
        }

        oracle
            .assert_after(world, &actions, self.expect_settled, &self.name)
            .await
    }
}

pub async fn run_scenarios(
    world: &CrossChainWorld,
    scenarios: impl IntoIterator<Item = Scenario>,
) -> Result<()> {
    for scenario in scenarios {
        scenario.run(world).await?;
    }
    Ok(())
}

#[cfg(test)]
mod framework_tests {
    use super::*;

    #[test]
    fn reorg_fixture_is_resolvable_after_testkit_extraction() {
        assert!(reorg_genesis_path().is_file());
        reorg_genesis_state_root().expect("parse reorg genesis fixture");
    }
}

/// Start the shared cross-chain fixture.
pub async fn setup_cross_chain() -> Result<CrossChainWorld> {
    setup_cross_chain_with_env(&[]).await
}

pub async fn setup_cross_chain_empty_call() -> Result<CrossChainEmptyCallWorld> {
    let world = setup_cross_chain().await?;
    let l1_rpc = world.l1_rpc();
    let l2_rpc = world.l2_rpc();
    let empty_call_l2 = deploy_empty_call(&l2_rpc, TARGET_DEPLOYER, world.l2_chain_id).await?;
    let empty_call_proxy = create_cross_chain_proxy(
        &l1_rpc,
        world.cfg.deployer_key,
        world.cfg.eez_address,
        empty_call_l2,
        world.cfg.rollup_id,
    )
    .await?;

    Ok(CrossChainEmptyCallWorld {
        world,
        empty_call_l2,
        empty_call_proxy,
    })
}

pub async fn setup_cross_chain_return_data() -> Result<CrossChainReturnDataWorld> {
    let world = setup_cross_chain().await?;
    let l1_rpc = world.l1_rpc();
    let l2_rpc = world.l2_rpc();
    let return_data_l2 = deploy_return_data(&l2_rpc, TARGET_DEPLOYER, world.l2_chain_id).await?;
    let return_data_proxy = create_cross_chain_proxy(
        &l1_rpc,
        world.cfg.deployer_key,
        world.cfg.eez_address,
        return_data_l2,
        world.cfg.rollup_id,
    )
    .await?;
    let return_data_wrapper =
        deploy_return_data_wrapper(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, return_data_proxy)
            .await?;

    Ok(CrossChainReturnDataWorld {
        world,
        return_data_wrapper,
    })
}

pub async fn setup_cross_chain_nested_setter() -> Result<CrossChainNestedSetterWorld> {
    let world = setup_cross_chain().await?;
    let l1_rpc = world.l1_rpc();
    let nested_setter_inner =
        deploy_nested_setter_inner(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, world.setter_proxy)
            .await?;
    let nested_setter_outer =
        deploy_nested_setter_outer(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, nested_setter_inner)
            .await?;

    Ok(CrossChainNestedSetterWorld {
        world,
        nested_setter_inner,
        nested_setter_outer,
    })
}

pub async fn setup_cross_chain_outbound_return_data() -> Result<CrossChainOutboundReturnDataWorld> {
    let world = setup_cross_chain().await?;
    let l1_rpc = world.l1_rpc();
    let l2_rpc = world.l2_rpc();
    let return_data_l1 = deploy_return_data(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID).await?;
    let return_data_proxy =
        create_l2_cross_chain_proxy(&l2_rpc, TARGET_DEPLOYER, return_data_l1, 0).await?;
    let return_data_wrapper = deploy_return_data_wrapper(
        &l2_rpc,
        TARGET_DEPLOYER,
        world.l2_chain_id,
        return_data_proxy,
    )
    .await?;

    Ok(CrossChainOutboundReturnDataWorld {
        world,
        return_data_wrapper,
    })
}

pub async fn setup_cross_chain_codeless() -> Result<CrossChainCodelessWorld> {
    let world = setup_cross_chain().await?;
    let l1_rpc = world.l1_rpc();
    let l2_rpc = world.l2_rpc();
    let inbound_wrapper =
        deploy_return_data_wrapper(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, world.deposit_proxy)
            .await?;
    let outbound_wrapper = deploy_return_data_wrapper(
        &l2_rpc,
        TARGET_DEPLOYER,
        world.l2_chain_id,
        world.withdrawal_proxy,
    )
    .await?;

    Ok(CrossChainCodelessWorld {
        world,
        inbound_wrapper,
        outbound_wrapper,
    })
}

/// Start the fixture with additional node environment variables.
pub async fn setup_cross_chain_with_env(
    extra_env: &[(&'static str, String)],
) -> Result<CrossChainWorld> {
    let cfg = CrossChainConfig::new()?;
    let datadir = tempfile::tempdir()?;
    let mut env = cfg.env();
    env.extend_from_slice(extra_env);
    let node = NodeHandle::spawn(datadir.path(), &env)?;
    let l1_rpc = cfg.l1_rpc_url();
    let l2_rpc = node.l2_rpc_url();

    let recipient: Address = address!("0x2222222222222222222222222222222222222222");
    let withdrawal_recipient: Address = address!("0x3333333333333333333333333333333333333333");

    wait_for_l2_rpc(&l1_rpc, SETUP_TIMEOUT).await?;
    let dep = deploy_protocol_dev(&l1_rpc, cfg.deployer_key, cfg.initial_state).await?;
    if dep.eez_address != cfg.eez_address {
        bail!("EEZ address not deterministic");
    }
    if dep.rollup_id != cfg.rollup_id {
        bail!(
            "unexpected rollup id: deployed {}, expected {}",
            dep.rollup_id,
            cfg.rollup_id
        );
    }

    wait_for_l2_rpc(&l2_rpc, SETUP_TIMEOUT).await?;
    let l2_chain_id = ProviderBuilder::new()
        .connect_http(l2_rpc.parse()?)
        .get_chain_id()
        .await?;

    let value_l2 = deploy_value(&l2_rpc, TARGET_DEPLOYER, l2_chain_id, U256::from(5u64)).await?;
    let expected_value = signer_address(TARGET_DEPLOYER)?.create(0);
    if value_l2 != expected_value {
        bail!("Value address not deterministic");
    }
    let inbound_no_ret =
        deploy_value_no_ret(&l2_rpc, TARGET_DEPLOYER, l2_chain_id, U256::from(5u64)).await?;
    let outbound_value =
        deploy_value(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, U256::from(5u64)).await?;
    let outbound_no_ret =
        deploy_value_no_ret(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, U256::from(5u64)).await?;

    let setter_proxy = create_cross_chain_proxy(
        &l1_rpc,
        cfg.deployer_key,
        cfg.eez_address,
        value_l2,
        cfg.rollup_id,
    )
    .await?;
    let deposit_proxy = create_cross_chain_proxy(
        &l1_rpc,
        cfg.deployer_key,
        cfg.eez_address,
        recipient,
        cfg.rollup_id,
    )
    .await?;
    let inbound_no_ret_proxy = create_cross_chain_proxy(
        &l1_rpc,
        cfg.deployer_key,
        cfg.eez_address,
        inbound_no_ret,
        cfg.rollup_id,
    )
    .await?;
    let inbound_wrapper =
        deploy_setter_wrapper(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, setter_proxy).await?;

    let outbound_proxy =
        create_l2_cross_chain_proxy(&l2_rpc, TARGET_DEPLOYER, outbound_value, 0).await?;
    let outbound_no_ret_proxy =
        create_l2_cross_chain_proxy(&l2_rpc, TARGET_DEPLOYER, outbound_no_ret, 0).await?;
    let withdrawal_proxy =
        create_l2_cross_chain_proxy(&l2_rpc, TARGET_DEPLOYER, withdrawal_recipient, 0).await?;
    let outbound_wrapper =
        deploy_setter_wrapper(&l2_rpc, TARGET_DEPLOYER, l2_chain_id, outbound_proxy).await?;

    Ok(CrossChainWorld {
        node,
        cfg,
        dep,
        l2_chain_id,
        value_l2,
        inbound_no_ret,
        outbound_value,
        outbound_no_ret,
        recipient,
        withdrawal_recipient,
        setter_proxy,
        deposit_proxy,
        inbound_no_ret_proxy,
        inbound_wrapper,
        outbound_proxy,
        outbound_no_ret_proxy,
        withdrawal_proxy,
        outbound_wrapper,
        _datadir: datadir,
    })
}
