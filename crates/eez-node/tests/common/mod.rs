//! Anvil-driven e2e harness: spawn L1, deploy protocol, spawn eez-node.
//!
//! Each test owns its own anvil port + datadir; harness drops kill both.

#![allow(dead_code)]

use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256, U256, address, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolEvent, SolValue, sol};
use anyhow::{Context, Result, anyhow, bail};

/// Anvil's first default account (mnemonic `test test test test test test test test test test test junk`).
pub const ANVIL_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const ANVIL_ADDR: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
/// Anvil's second default account — used for tests that need a key
/// distinct from the deployer / authorized signer.
pub const ANVIL_KEY_1: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
pub const ANVIL_KEY_2: &str = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a";
pub const ANVIL_KEY_3: &str = "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6";
pub const ANVIL_KEY_4: &str = "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a";
/// Anvil's #5 (0x9965…) — funded on both the embedded L1 (anvil --dev) and the
/// L2 genesis alloc, and L2-INGRESS-FRESH (used by no other harness leg), so it
/// can submit a fresh-nonce L2→L1 outbound without colliding with a prior ingress
/// nonce. Used by the K=2 outbound test as the second, distinct withdrawer.
pub const ANVIL_KEY_5: &str = "0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba";
pub const ANVIL_ADDR_3: Address = address!("0x90F79bf6EB2c4f870365E785982E1f101E93b906");

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

pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
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

pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("local_addr").port()
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
            // Non-cross-chain tests use the external anvil, not an embedded L1.
            ("EEZ_L1_EMBEDDED", "0".to_string()),
            ("EEZ_L1_RPC_URL", self.anvil.rpc_url.clone()),
            ("EEZ_L1_BUILDER_RPC_URL", self.stub.url.clone()),
            ("EEZ_L1_POSTER_KEY", opts.poster_key.to_string()),
            ("EEZ_L1_CHAIN_ID", "31337".to_string()),
            ("EEZ_L2_SYSTEM_ADDRESS", format!("{ANVIL_ADDR:#x}")),
            ("EEZ_L2_SYSTEM_KEY", ANVIL_KEY.to_string()),
            (
                "EEZ_CCM_L2_ADDRESS",
                "0x4200000000000000000000000000000000000007".to_string(),
            ),
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
                "EEZ_MOCK_PROOF_SYSTEM_ADDRESS",
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

/// L2 system signer (== anvil/hardhat key #0). Used in cross-chain mode
/// as `EEZ_L2_SYSTEM_KEY` — the signer of the `loadExecutionTable`
/// system tx the composer emits in each Sync block.
pub const EEZ_L2_SYSTEM_KEY: &str = ANVIL_KEY;
/// Address of [`EEZ_L2_SYSTEM_KEY`] (`EEZ_L2_SYSTEM_ADDRESS`).
pub const EEZ_L2_SYSTEM_ADDR: Address = ANVIL_ADDR;
/// EEZL2 cross-chain manager — genesis predeploy in
/// [`reorg_genesis_path`] (codelen ~24390). `EEZ_CCM_L2_ADDRESS`.
pub const CCM_L2_ADDRESS: Address = address!("0x4200000000000000000000000000000000000007");
/// Placeholder registry address for Phase A (codeless → composer reads
/// `rollups[]` from an empty address, posts nothing, just retries; no
/// crash). Replaced by the real address in Phase B.
pub const PLACEHOLDER_ADDRESS: Address = address!("0x0000000000000000000000000000000000000001");

/// Allocated ports + datadir for one embedded-L1 reth, plus the derived
/// RPC URL. Pinned across the Phase-A → Phase-B restart so the embedded
/// L1 keeps its state (the deployed EEZ). The `_datadir` tempdir drops
/// with this struct — keep it alive for the node's whole lifetime.
pub struct EmbeddedL1 {
    /// HTTP RPC port (`EEZ_L1_HTTP_PORT`). WS = +1, auth = +2.
    pub http_port: u16,
    /// Auth RPC (engine) port (`EEZ_L1_AUTH_PORT`).
    pub auth_port: u16,
    /// P2P port (`EEZ_L1_P2P_PORT`).
    pub p2p_port: u16,
    /// discv5 UDP port (`EEZ_L1_DISCV5_PORT`).
    pub discv5_port: u16,
    /// Embedded-reth datadir (`EEZ_L1_DATADIR`). Persists across the
    /// Phase-A/Phase-B restart.
    pub datadir: PathBuf,
    /// `http://127.0.0.1:<http_port>` — the embedded L1's own RPC, used
    /// for `EEZ_L1_RPC_URL`, `EEZ_L1_BUILDER_RPC_URL`, contract deploys,
    /// and the [`L1Chain`] reader.
    pub rpc_url: String,
    /// Keeps the datadir tempdir alive for this struct's lifetime.
    _datadir: tempfile::TempDir,
}

impl EmbeddedL1 {
    /// Allocate fresh ports + a tempdir datadir for an embedded L1. The
    /// HTTP port reserves http, http+1 (WS) and http+2 (auth) — pick
    /// auth/p2p from independent free ports so two parallel embedded L1s
    /// never collide.
    pub fn alloc() -> Result<Self> {
        let http_port = free_port();
        let auth_port = free_port();
        let p2p_port = free_port();
        let mut discv5_port = free_port();
        while discv5_port == p2p_port {
            discv5_port = free_port();
        }
        let datadir = tempfile::Builder::new()
            .prefix("eez-l1-embedded-")
            .tempdir()
            .context("embedded L1 datadir")?;
        let rpc_url = format!("http://127.0.0.1:{http_port}");
        Ok(Self {
            http_port,
            auth_port,
            p2p_port,
            discv5_port,
            datadir: datadir.path().to_path_buf(),
            rpc_url,
            _datadir: datadir,
        })
    }

    /// The embedded-L1 env knobs (`EEZ_L1_*`) shared by both phases.
    /// Pins ports + datadir so the Phase-B restart reuses the same L1.
    fn l1_env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("EEZ_L1_EMBEDDED", "1".to_string()),
            ("EEZ_L1_HTTP_PORT", self.http_port.to_string()),
            ("EEZ_L1_AUTH_PORT", self.auth_port.to_string()),
            ("EEZ_L1_P2P_PORT", self.p2p_port.to_string()),
            ("EEZ_L1_DISCV5_PORT", self.discv5_port.to_string()),
            (
                "EEZ_L1_DATADIR",
                self.datadir.to_string_lossy().into_owned(),
            ),
            // In embedded mode the composer's L1 RPC IS the embedded
            // reth's own HTTP (main.rs:651-654). Builder RPC = same URL;
            // the submitter detects the plain RPC (no eth_sendBundle) and
            // degrades to ordered mempool submission (config.rs / error.rs
            // BundleRpcUnsupported).
            ("EEZ_L1_RPC_URL", self.rpc_url.clone()),
            ("EEZ_L1_BUILDER_RPC_URL", self.rpc_url.clone()),
            // reth --chain dev chainId. Used for the L1 postBatch tx's
            // chain id (main.rs:684).
            ("EEZ_L1_CHAIN_ID", "1337".to_string()),
        ]
    }
}

/// Build the env for a cross-chain (embedded-L1) composer node.
///
/// `registry` / `proof_system` / `deploy_block` are the placeholders in
/// Phase A and the real deployed values in Phase B. `proxy_addresses`
/// (the L2 cross-chain proxy) is only set in Phase B (S2+); S1 passes
/// `None`. The L2 system signer + CCM-L2 predeploy + timing complete the
/// composer's cross-chain requirements (main.rs:458-695). Timing matches
/// `env_for` (L1=2s, L2=1s, proof=1s) so sync slots close ~1s after each
/// embedded-L1 head.
pub fn cross_chain_env(
    l1: &EmbeddedL1,
    poster_key: &str,
    registry: Address,
    proof_system: Address,
    deploy_block: u64,
    rollup_id: u64,
    proxy_addresses: Option<&[Address]>,
) -> Vec<(&'static str, String)> {
    let mut env = l1.l1_env();
    env.extend(vec![
        ("EEZ_L1_POSTER_KEY", poster_key.to_string()),
        ("EEZ_PROOF_SIGNER_KEY", ANVIL_KEY.to_string()),
        ("EEZ_REGISTRY_ADDRESS", format!("{registry:#x}")),
        ("EEZ_REGISTRY_DEPLOY_BLOCK", deploy_block.to_string()),
        (
            "EEZ_MOCK_PROOF_SYSTEM_ADDRESS",
            format!("{proof_system:#x}"),
        ),
        ("EEZ_ROLLUP_ID", rollup_id.to_string()),
        // SYNCHRONOUS mock proof system: the composer self-signs the
        // postBatch and submits immediately. The repo's `deployments.env`
        // (loaded by `dotenvy::from_filename` in main.rs:99 from the
        // node's working dir == repo_root) sets `EEZ_PROOF_SYSTEM_KIND=real`,
        // which would put the composer in DEFERRED-POST mode — it would
        // wait forever for an out-of-process prover that no test runs, time
        // out, and never settle a batch. dotenvy does NOT override an
        // already-set var, so pinning `mock` here wins over the file.
        ("EEZ_PROOF_SYSTEM_KIND", "mock".to_string()),
        // Cross-chain composer requirements (main.rs:461-674).
        ("EEZ_CCM_L2_ADDRESS", format!("{CCM_L2_ADDRESS:#x}")),
        ("EEZ_L2_SYSTEM_KEY", EEZ_L2_SYSTEM_KEY.to_string()),
        ("EEZ_L2_SYSTEM_ADDRESS", format!("{EEZ_L2_SYSTEM_ADDR:#x}")),
        // Valid timing (see env_for): K=2, proof_window_open = 1s.
        ("EEZ_L1_BLOCK_TIME_MS", "2000".to_string()),
        ("EEZ_L2_BLOCK_TIME_MS", "1000".to_string()),
        ("EEZ_PROOF_TIME_MS", "1000".to_string()),
        ("EEZ_SUBMISSION_SLACK_MS", "100".to_string()),
        // Lift the speculative-depth cap (main.rs default = 64). It
        // bounds `head - confirmed_head` where `confirmed_head` = the
        // highest L2 block an L1-landed batch confirms — which is 0
        // until the FIRST postBatch settles. With a 1s L2 block time the
        // L2 head races wall clock during Phase A; if it crosses 64
        // before the first batch lands, the cap pauses the very Sync
        // block needed to settle that first batch → bootstrap deadlock
        // (head frozen, `Catchup{live}` growing without bound, no
        // postBatch ever). A large cap lets the bootstrap catchup reach
        // its terminal Sync block; once batches flow, confirmed_head
        // tracks head and the cap is never approached. Not a production
        // concern — production embedded reth syncs a chain where EEZ is
        // already deployed, so the very first slot settles immediately.
        ("EEZ_MAX_SPECULATIVE_DEPTH", "100000".to_string()),
        ("EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES", "false".to_string()),
        (
            "EEZ_L2_DATADIR",
            "/tmp/unused-overridden-by-flag".to_string(),
        ),
        (
            "RUST_LOG",
            std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string()),
        ),
    ]);
    if let Some(proxies) = proxy_addresses {
        let joined = proxies
            .iter()
            .map(|a| format!("{a:#x}"))
            .collect::<Vec<_>>()
            .join(",");
        env.push(("EEZ_CROSS_CHAIN_PROXY_ADDRESSES", joined));
    }
    env
}

/// Append `EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS=<ids>` to a `cross_chain_env`
/// so the ingress classifier tags INBOUND (L1→L2) txs.
///
/// INBOUND is matched by `tx.chain_id ∈ cross_chain_source_chain_ids`
/// (`ingress.rs`: an L1-source intent POSTed to L2's RPC — the chainId
/// mismatch is the signal). `cross_chain_env` only sets the OUTBOUND
/// proxy-address var; this is the INBOUND mirror. Composes on top of
/// `cross_chain_env(...)` so the OUTBOUND helper signature stays
/// untouched. Pass the EMBEDDED L1's chain id (`EEZ_L1_CHAIN_ID`, == the
/// chain id the inbound user tx is signed for).
pub fn with_inbound_source_chain_ids(
    mut env: Vec<(&'static str, String)>,
    chain_ids: &[u64],
) -> Vec<(&'static str, String)> {
    let joined = chain_ids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    // Replace any pre-existing entry (idempotent across Phase A/B reuse).
    env.retain(|(k, _)| *k != "EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS");
    env.push(("EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS", joined));
    env
}

/// `eth_chainId` at `rpc_url`. Used by the INBOUND test to read the
/// embedded L1's chain id (the value the inbound user tx must be signed
/// for, and the value `EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS` must carry).
pub async fn read_chain_id(rpc_url: &str) -> Result<u64> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(provider.get_chain_id().await?)
}

/// Build + sign + submit an INBOUND (L1→L2) user tx: an EIP-1559 tx with
/// `chain_id = l1_chain_id`, `to = l1_proxy`, `input = setValue(v)`,
/// from `key`, submitted to the **L2 ingress** (`eth_sendRawTransaction`
/// @ `l2_rpc_url`). The chainId mismatch (L1 id on an L2 RPC) is the
/// ingress classifier's INBOUND signal; the composer holds it, drains it,
/// and `EEZL2.executeIncomingCrossChainCall` materializes `setValue(v)`
/// on the L2 `Value`.
///
/// The `nonce` is read from `l1_nonce_rpc_url` — the ingress admission
/// gate validates the sender's nonce + balance against the **L1**
/// (`ingress.rs`: `l1_provider.get_transaction_count`), so the tx must
/// carry the sender's L1 nonce, NOT its L2 nonce. `key` must be a funded
/// L1 EOA (the embedded reth `--dev` genesis funds the full hardhat set).
/// `gas_*` are L1-shaped constants (the embedded L1 is reth `--dev`); a
/// generous gas limit covers the proxy fallback + EEZ deposit.
pub async fn send_inbound_set_value(
    l2_rpc_url: &str,
    l1_nonce_rpc_url: &str,
    key: &str,
    l1_chain_id: u64,
    l1_proxy: Address,
    v: u64,
    // ETH (`msg.value`) the user attaches on L1. The L1 proxy fallback
    // forwards it into `executeCrossChainCall` (`_entryEtherIn`), so a
    // value-bearing inbound deposits this V on L1 (rollup `etherBalance`)
    // and delivers it to the L2 target via the system delivery tx. Pass
    // `U256::ZERO` for a value-free inbound (byte-identical to before).
    value: U256,
) -> Result<alloy_primitives::TxHash> {
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
    use alloy_network::TxSignerSync;
    use alloy_network::eip2718::Encodable2718;

    let signer: PrivateKeySigner = key
        .strip_prefix("0x")
        .unwrap_or(key)
        .parse()
        .context("parse inbound sender key")?;
    let from = signer.address();

    // Nonce from the L1 (the ingress gate checks the L1 nonce), fees +
    // submission from the L2 RPC (the tx is POSTed to the L2 ingress).
    let l1_provider = ProviderBuilder::new().connect_http(l1_nonce_rpc_url.parse()?);
    let nonce = l1_provider.get_transaction_count(from).await?;

    let l2_provider = ProviderBuilder::new().connect_http(l2_rpc_url.parse()?);

    let input = IValue::setValueCall { v: U256::from(v) }.abi_encode();

    let mut tx = TxEip1559 {
        // L1 chain id — the INBOUND signal. The tx is signed for the L1
        // chain but submitted to the L2 RPC; the chainId mismatch is what
        // the classifier keys on.
        chain_id: l1_chain_id,
        nonce,
        // Generous: the L1 proxy fallback forwards to EEZ; matches the
        // devnet inbound test's 600k user-tx gas.
        gas_limit: 600_000,
        // The embedded L1 is reth `--dev`; fixed L1-shaped fees (the gate
        // only checks `value + gas_limit * max_fee <= L1 balance`, and a
        // funded hardhat EOA has 10000 ETH).
        max_fee_per_gas: 2_000_000_000,
        max_priority_fee_per_gas: 1_000_000_000,
        to: alloy_primitives::TxKind::Call(l1_proxy),
        value,
        access_list: alloy_rpc_types_eth::AccessList::default(),
        input: input.into(),
    };
    let sig = signer.sign_transaction_sync(&mut tx)?;
    let envelope = TxEnvelope::from(tx.into_signed(sig));
    let raw = envelope.encoded_2718();

    let hash = l2_provider
        .send_raw_transaction(&raw)
        .await?
        .tx_hash()
        .to_owned();
    Ok(hash)
}

/// `Chain`-style reader pointed at the embedded L1 RPC + the EEZ
/// deployed there. `Chain` is bound to the external `Anvil`; the
/// embedded L1 has no `Anvil` struct, so this owns its URL by value.
pub struct L1Chain {
    pub rpc_url: String,
    pub eez_address: Address,
    pub deploy_block: u64,
    pub rollup_id: u64,
}

impl L1Chain {
    pub fn new(rpc_url: &str, dep: &Deployment) -> Self {
        Self {
            rpc_url: rpc_url.to_string(),
            eez_address: dep.eez_address,
            deploy_block: dep.deploy_block,
            rollup_id: dep.rollup_id,
        }
    }

    pub async fn batches_posted(&self) -> Result<usize> {
        count_events(
            &self.rpc_url,
            self.eez_address,
            IEEZ::BatchPosted::SIGNATURE_HASH,
            self.deploy_block,
        )
        .await
    }

    pub async fn executions_performed(&self) -> Result<usize> {
        count_events(
            &self.rpc_url,
            self.eez_address,
            IEEZ::L2ExecutionPerformed::SIGNATURE_HASH,
            self.deploy_block,
        )
        .await
    }

    pub async fn state_root(&self) -> Result<B256> {
        state_root(&self.rpc_url, self.eez_address, self.rollup_id).await
    }

    pub async fn block_number(&self) -> Result<u64> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url.parse()?);
        Ok(provider.get_block_number().await?)
    }

    /// Wait until ≥ `n` `BatchPosted` events are visible on the embedded L1.
    pub async fn wait_for_batches(&self, n: usize, timeout: Duration) -> Result<usize> {
        wait_for(timeout, || async {
            let count = self.batches_posted().await?;
            Ok((count >= n).then_some(count))
        })
        .await
    }
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
        event L2ExecutionPerformed(uint256 indexed rollupId, bytes32 newState);
        event ImmediateEntrySkipped(uint256 indexed transientIdx, bytes revertData);
        function rollups(uint256 rollupId) external view returns (address rollupContract, bytes32 stateRoot, uint256 etherBalance);
        function rollupCounter() external view returns (uint256);
        function registerRollup(address rollupContract, bytes32 initialState) external returns (uint256 rollupId);
    }
}

sol! {
    /// EEZL2 cross-chain manager surface used by the OUTBOUND test.
    /// `createCrossChainProxy` / `computeCrossChainProxyAddress` /
    /// `authorizedProxies` are inherited from EEZBase. The
    /// `CrossChainProxyCreated` event is emitted by `createCrossChainProxy`;
    /// all three of its params are `indexed` (proxy in topics[1]).
    #[sol(rpc)]
    interface IEEZL2 {
        event CrossChainProxyCreated(address indexed proxy, address indexed originalAddress, uint256 indexed originalRollupId);
        function createCrossChainProxy(address originalAddress, uint256 originalRollupId) external returns (address proxy);
        function computeCrossChainProxyAddress(address originalAddress, uint256 originalRollupId) external view returns (address);
        function authorizedProxies(address proxy) external view returns (address originalAddress, uint64 originalRollupId);
    }

    /// The L1 settlement target. The OUTBOUND user tx routes
    /// `setValue(uint256)` through the L2 proxy → `executeCrossChainCall`
    /// → an L1 settlement entry that the EEZ `_processNCalls` executes on
    /// L1, mutating this contract's `value`.
    #[sol(rpc)]
    interface IValue {
        event ValueSet(address indexed by, uint256 newValue);
        function setValue(uint256 v) external returns (bool changed, uint256 newValue);
        function value() external view returns (uint256);
    }
}

/// Reth's `--chain dev` genesis state root. Used as the `initialState`
/// when registering the rollup so the very first batch's prestate
/// (`l2_state_root(0)`) matches the on-chain `rollups[rid].stateRoot`.
/// With the default `B256::ZERO`, every batch's `_applyStateDeltas`
/// reverts with `StateRootMismatch`, caught by the try/catch,
/// emitting `ImmediateEntrySkipped` instead of `L2ExecutionPerformed`.
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/genesis.json")
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

/// A materialized L2 genesis JSON with a **current** `timestamp`, plus
/// its state root. The temp file is kept alive by the caller (drop
/// removes it). See [`fresh_genesis`] for why the timestamp must be
/// recent.
pub struct FreshGenesis {
    /// Tempfile holding the genesis JSON. Kept alive so the path stays
    /// valid for the node's lifetime; drops with this struct.
    _file: tempfile::NamedTempFile,
    /// Absolute path to the genesis JSON (passed as `--chain <path>`).
    pub path: PathBuf,
    /// Genesis state root (== `initialState` for `registerRollup`).
    /// Independent of `timestamp` — it's the trie root over `alloc`.
    pub state_root: B256,
}

/// Materialize an L2 genesis JSON whose header `timestamp` is the
/// current unix time, written to a tempfile.
///
/// **Why a fresh timestamp is required.** The L1-anchored slot scheduler
/// computes the sync-slot block height as
/// `(L1.timestamp + L1_block_time - l2_genesis_timestamp) / l2_block_time`
/// (`slot.rs:318`). With a stale genesis timestamp (reth's `--chain dev`
/// genesis is `0`; the reorg fixture is June 2023) and a live anvil
/// whose head timestamp is the real wall clock, that height is ~90M
/// while the L2 head is near 0 — the Sequencer stays in perpetual
/// `Catchup` (capped at `MAX_BLOCKS_PER_CATCHUP=300`/trigger) and never
/// closes a sync slot, so **no `postBatch` is ever submitted**. Pinning
/// the genesis timestamp to ~now makes the height arithmetic small and
/// the very first trigger close a slot.
///
/// `base` is the source `Genesis` (`reth_chainspec::DEV.genesis()` for
/// the dev-shaped tests, or the parsed reorg fixture for the
/// cross-chain predeploy). Only `timestamp` is rewritten; `alloc` /
/// `config` are untouched, so the returned `state_root` equals the
/// base's.
pub fn fresh_genesis(base: &alloy_genesis::Genesis) -> Result<FreshGenesis> {
    use std::io::Write;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system time before unix epoch")?
        .as_secs();
    let mut genesis = base.clone();
    genesis.timestamp = now;

    // State root is the trie root over `alloc`; `timestamp` doesn't
    // enter it. Compute from the (timestamp-rewritten) genesis so the
    // value is self-consistent.
    let spec: reth_chainspec::ChainSpec = genesis.clone().into();
    let state_root = spec.genesis_header().state_root;

    let json = serde_json::to_vec_pretty(&genesis).context("serialize fresh genesis")?;
    let mut file = tempfile::Builder::new()
        .prefix("eez-l2-genesis-")
        .suffix(".json")
        .tempfile()
        .context("create genesis tempfile")?;
    file.write_all(&json).context("write genesis tempfile")?;
    file.flush().context("flush genesis tempfile")?;
    let path = file.path().to_path_buf();
    Ok(FreshGenesis {
        _file: file,
        path,
        state_root,
    })
}

/// [`fresh_genesis`] over reth's built-in `--chain dev` genesis — a
/// dev-shaped L2 with a current timestamp. Used by the standard
/// (non-cross-chain) tests so their composer actually closes sync slots.
pub fn fresh_dev_genesis() -> Result<FreshGenesis> {
    fresh_genesis(reth_chainspec::DEV.genesis())
}

/// [`fresh_genesis`] over the cross-chain reorg fixture (EEZL2 predeploy
/// at `0x4200…0007`) with a current timestamp. Used by the cross-chain
/// (embedded-L1) tests.
pub fn fresh_cross_chain_genesis() -> Result<FreshGenesis> {
    let raw = std::fs::read_to_string(reorg_genesis_path()).context("read genesis.json")?;
    let base: alloy_genesis::Genesis = serde_json::from_str(&raw).context("parse genesis.json")?;
    fresh_genesis(&base)
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
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
    use alloy_network::TxSignerSync;
    use alloy_network::eip2718::Encodable2718;

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

/// Wait until `eth_blockNumber` responds at `rpc_url`. Used to confirm a
/// just-spawned eez-node's L2 RPC is up before we send txs at it.
pub async fn wait_for_l2_rpc(rpc_url: &str, timeout: Duration) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for(timeout, || async {
        Ok(provider.get_block_number().await.ok().map(|_| ()))
    })
    .await
}

/// `eth_blockNumber` at `rpc_url`, or `0` on error. Convenience for
/// computing a relative target for [`wait_for_l1_blocks`].
pub async fn l1_block_number(rpc_url: &str) -> u64 {
    match rpc_url.parse() {
        Ok(url) => ProviderBuilder::new()
            .connect_http(url)
            .get_block_number()
            .await
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Wait until `eth_blockNumber` at `rpc_url` STOPS responding (the node
/// owning it has exited and released the port). Used between the
/// placeholder→real restart so the embedded L1's pinned port + datadir
/// lock are free before the next node binds them.
pub async fn wait_for_rpc_down(rpc_url: &str, timeout: Duration) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for(timeout, || async {
        Ok(provider.get_block_number().await.err().map(|_| ()))
    })
    .await
}

/// Read the `stateRoot` of the latest `safe` block on the L2 at `rpc_url`.
/// Returns `Ok(None)` while the L2 hasn't yet adopted any safe block
/// (genesis L1 derivation pending). Used by the multi-composer reorg
/// test to verify both composers settle on the same canonical L2 head.
pub async fn safe_block_state_root(rpc_url: &str) -> Result<Option<B256>> {
    use alloy_rpc_types_eth::BlockNumberOrTag;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider.get_block_by_number(BlockNumberOrTag::Safe).await?;
    Ok(block.map(|b| b.header.state_root))
}

/// Read `(number, stateRoot)` of the latest `safe` block on the L2 at
/// `rpc_url`. `Ok(None)` until a safe block exists. Used by S4 to PIN
/// the outbound-settled block's HEIGHT at S3-acceptance time so the
/// follower can be checked at that exact height (the composer's safe
/// head keeps advancing past it).
pub async fn safe_block_number_and_root(rpc_url: &str) -> Result<Option<(u64, B256)>> {
    use alloy_rpc_types_eth::BlockNumberOrTag;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider.get_block_by_number(BlockNumberOrTag::Safe).await?;
    Ok(block.map(|b| (b.header.number, b.header.state_root)))
}

/// Read the `stateRoot` of block `number` on the L2 at `rpc_url`.
/// `Ok(None)` if the node hasn't imported a block at that height yet.
/// Used by S4 to assert the fresh follower re-derived the SAME state
/// root the composer settled at the outbound block's height — a
/// height-pinned comparison that doesn't race the advancing safe head.
pub async fn block_state_root_at(rpc_url: &str, number: u64) -> Result<Option<B256>> {
    use alloy_rpc_types_eth::BlockNumberOrTag;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(number))
        .await?;
    Ok(block.map(|b| b.header.state_root))
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
    // Wallet-filled provider: `send_transaction` signs locally and
    // sends raw. Anvil auto-signs `eth_sendTransaction` for its default
    // accounts, but the embedded reth `--dev` rejects an `eth_send-
    // Transaction` it can't sign (`-32602 invalid transaction request`).
    // A `WalletFiller` makes both paths work (anvil accepts the raw tx
    // too). The dev account is prefunded on both chains (reth's DEV
    // genesis funds the full hardhat account set).
    let signer: PrivateKeySigner = key
        .strip_prefix("0x")
        .unwrap_or(key)
        .parse()
        .context("parse signer key")?;
    let signer_addr = signer.address();
    let wallet = alloy_network::EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse()?);

    let out = repo_root().join("contracts/out");

    let eez_address = deploy(
        &provider,
        signer_addr,
        &out.join("EEZ.sol/EEZ.json"),
        Vec::new(),
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
                // Mark as contract-creation (to = TxKind::Create). The
                // WalletFiller requires an explicit kind even for deploys;
                // without it alloy errors `missing properties [Wallet, to]`.
                .create()
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

// ─── OUTBOUND (L2→L1) cross-chain helpers (P-1 S2/S3) ────────────────

/// Deploy `Value(initial)` to the L1 at `rpc_url` from `key`. The L1-side
/// settlement target for the OUTBOUND test: the EEZ `_processNCalls` executes
/// the cross-chain `setValue` against it during `postBatch`. Uses the generic
/// `deploy` helper with the `uint256 initial` ctor arg abi-encoded.
///
/// NON-VOID returnData (follow-up #2, DONE): `Value.setValue(uint256)` returns
/// `(bool changed, uint256 newValue)`, NOT nothing. This settles correctly with
/// no code change because the outbound source-sim runs the L2→L1 call against
/// the composer's L1 entry client (L1 IS registered in the rollup map, see
/// `main.rs` `.entry(entry_client_view)`), so `ExecutedAction.outcome
/// .return_data()` captures the REAL L1 return — the composed entry's
/// `rollingHash` (fold over that return) then matches `EEZ._processNCalls`'
/// on-chain recompute. (`ValueNoRet`, the void isolation control, also still
/// works; `Value` is the strictly-more-general target so the suite uses it.)
pub async fn deploy_value(rpc_url: &str, key: &str, initial: u64) -> Result<Address> {
    let signer: PrivateKeySigner = key
        .strip_prefix("0x")
        .unwrap_or(key)
        .parse()
        .context("parse value-deployer key")?;
    let signer_addr = signer.address();
    let wallet = alloy_network::EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse()?);
    let out = repo_root().join("contracts/out");
    deploy(
        &provider,
        signer_addr,
        &out.join("Value.sol/Value.json"),
        U256::from(initial).abi_encode(),
    )
    .await
}

/// Deploy `ValuePayable(initial)` — the payable `Value` variant that can
/// RECEIVE the ETH a value-bearing inbound (L1->L2) deposit delivers (the
/// plain `Value` reverts on any incoming ETH). Same `value()` / `setValue`
/// ABI, so [`read_value`] reads it unchanged.
pub async fn deploy_value_payable(rpc_url: &str, key: &str, initial: u64) -> Result<Address> {
    let signer: PrivateKeySigner = key
        .strip_prefix("0x")
        .unwrap_or(key)
        .parse()
        .context("parse value-deployer key")?;
    let signer_addr = signer.address();
    let wallet = alloy_network::EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse()?);
    let out = repo_root().join("contracts/out");
    deploy(
        &provider,
        signer_addr,
        &out.join("ValuePayable.sol/ValuePayable.json"),
        U256::from(initial).abi_encode(),
    )
    .await
}

/// Read `Value.value()` (a `uint256`) from `value_addr` on the chain at
/// `rpc_url`. Used for the S3 acceptance: `== 42` after the OUTBOUND
/// `setValue(42)` settles on L1.
pub async fn read_value(rpc_url: &str, value_addr: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let value = IValue::new(value_addr, &provider);
    Ok(value.value().call().await?)
}

/// Raw `eth_getBalance(addr)` at `rpc_url` — the ETH balance. Used to assert
/// value actually MOVED cross-chain (distinct from [`read_value`], a
/// contract-state read): the L2 target's balance rises by V after a
/// value-bearing inbound, the L1 target's after a value-bearing outbound.
pub async fn eth_get_balance(rpc_url: &str, addr: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(provider.get_balance(addr).await?)
}

/// `IValue.value()` pinned at L2 block `number` (not the advancing head).
/// Used by the INBOUND test's S3(b) reconciliation to require the SAFE-head
/// root it pins to be DELIVERY-INCLUSIVE (`value == 42` at that height) —
/// otherwise the safe head (which lags the live delivery block) yields a
/// PRE-delivery anchor root, and a follower re-deriving it never walks the
/// inbound reconstruction (inbound=0).
pub async fn read_value_at_block(rpc_url: &str, value_addr: Address, number: u64) -> Result<U256> {
    use alloy_eips::BlockId;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let value = IValue::new(value_addr, &provider);
    Ok(value.value().block(BlockId::number(number)).call().await?)
}

/// `EEZL2.createCrossChainProxy(original, original_rollup_id)` on the L2
/// at `l2_rpc_url`, signed by `key`. Returns the proxy address `P` read
/// from the `CrossChainProxyCreated` log (`proxy` is `topics[1]`),
/// cross-checked against the `computeCrossChainProxyAddress` view.
///
/// For the OUTBOUND test `original` = the L1 `Value` address and
/// `original_rollup_id` = `MAINNET` (0).
pub async fn create_l2_cross_chain_proxy(
    l2_rpc_url: &str,
    key: &str,
    ccm_l2: Address,
    original: Address,
    original_rollup_id: u64,
) -> Result<Address> {
    let signer: PrivateKeySigner = key
        .strip_prefix("0x")
        .unwrap_or(key)
        .parse()
        .context("parse proxy-creator key")?;
    let wallet = alloy_network::EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(l2_rpc_url.parse()?);

    let eez_l2 = IEEZL2::new(ccm_l2, &provider);

    // Predict the address first (pure view) — used to cross-check the log.
    let predicted = eez_l2
        .computeCrossChainProxyAddress(original, U256::from(original_rollup_id))
        .call()
        .await
        .context("computeCrossChainProxyAddress")?;

    let receipt = eez_l2
        .createCrossChainProxy(original, U256::from(original_rollup_id))
        .send()
        .await
        .context("send createCrossChainProxy")?
        .get_receipt()
        .await
        .context("createCrossChainProxy receipt")?;
    if !receipt.status() {
        bail!("createCrossChainProxy reverted");
    }

    // Decode P from the CrossChainProxyCreated log (proxy = topics[1]).
    let mut proxy_from_log: Option<Address> = None;
    for log in receipt.inner.logs() {
        if let Ok(decoded) = IEEZL2::CrossChainProxyCreated::decode_log(&log.inner) {
            proxy_from_log = Some(decoded.proxy);
            break;
        }
    }
    let proxy = proxy_from_log
        .ok_or_else(|| anyhow!("no CrossChainProxyCreated log in createCrossChainProxy receipt"))?;

    if proxy != predicted {
        bail!(
            "proxy address mismatch: log={proxy:#x} predicted={predicted:#x} \
             (createCrossChainProxy vs computeCrossChainProxyAddress disagree)"
        );
    }
    Ok(proxy)
}

/// Read `EEZL2.authorizedProxies(proxy).originalAddress` on the L2 at
/// `l2_rpc_url`. Used by S2 to assert the just-created proxy `P` is
/// registered against the L1 `Value` address.
pub async fn proxy_original_address(
    l2_rpc_url: &str,
    ccm_l2: Address,
    proxy: Address,
) -> Result<Address> {
    let provider = ProviderBuilder::new().connect_http(l2_rpc_url.parse()?);
    let eez_l2 = IEEZL2::new(ccm_l2, &provider);
    Ok(eez_l2
        .authorizedProxies(proxy)
        .call()
        .await?
        .originalAddress)
}

/// Build + sign + submit an OUTBOUND user tx: an EIP-1559 tx with
/// `chain_id = l2_chain_id`, `to = proxy`, `input = setValue(v)`, from
/// `key`, to the L2 ingress (`eth_sendRawTransaction` @ `l2_rpc_url`).
/// `CrossChainProxy._fallback()` forwards it to
/// `EEZL2.executeCrossChainCall(Value, setValue(v))`, recording the
/// L2→L1 entry the composer drains.
///
/// `chain_id` and `nonce` are passed explicitly: the L2 fixture's
/// chainId is `1` (not the L1 1337), and the caller owns the proxy
/// creator's vs sender's nonce separation.
pub async fn send_outbound_set_value(
    l2_rpc_url: &str,
    key: &str,
    proxy: Address,
    v: u64,
    // ETH (`msg.value`) the user attaches on L2. EEZL2 burns it to
    // SYSTEM_ADDRESS and hash-binds it; on L1 the rollup sends this V from
    // its escrowed `etherBalance` to the outbound target (a withdrawal).
    // Pass `U256::ZERO` for a value-free outbound (byte-identical to before).
    value: U256,
) -> Result<alloy_primitives::TxHash> {
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
    use alloy_network::TxSignerSync;
    use alloy_network::eip2718::Encodable2718;

    let signer: PrivateKeySigner = key
        .strip_prefix("0x")
        .unwrap_or(key)
        .parse()
        .context("parse outbound sender key")?;
    let from = signer.address();

    let provider = ProviderBuilder::new().connect_http(l2_rpc_url.parse()?);
    let chain_id = provider.get_chain_id().await?;
    let nonce = provider.get_transaction_count(from).await?;
    let fees = provider.estimate_eip1559_fees().await?;

    let input = IValue::setValueCall { v: U256::from(v) }.abi_encode();

    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        // Generous: the proxy fallback does an extra self-call + EEZ
        // call; 600k matches the inbound devnet test's user-tx gas.
        gas_limit: 600_000,
        max_fee_per_gas: fees.max_fee_per_gas,
        max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
        to: alloy_primitives::TxKind::Call(proxy),
        value,
        access_list: alloy_rpc_types_eth::AccessList::default(),
        input: input.into(),
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

pub struct NodeHandle {
    child: Child,
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

static LOG_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
pub struct NodeConfig<'a> {
    /// Path to a custom genesis JSON. `None` uses `--chain dev`.
    pub genesis_path: Option<&'a std::path::Path>,
    /// Run the node process from a FRESH (empty) working directory
    /// instead of the repo root. The node calls `dotenvy::dotenv()` +
    /// `dotenvy::from_filename("deployments.env")` at startup
    /// (main.rs:98-99), which load `.env` / `deployments.env` RELATIVE
    /// TO THE CWD. The repo's `.env` sets `EEZ_PROOF_SIGNER_KEY`; dotenvy
    /// fills any var the spawn env left UNSET, so a follower spawned with
    /// the proof-signer key removed would have it silently re-added from
    /// `.env` → `Mode::from_env` boots it as a COMPOSER (which spawns its
    /// own empty embedded L1 and never re-derives the settled batch).
    /// Running from a `.env`-free cwd makes both dotenvy calls no-ops, so
    /// the removed key STAYS removed and `Mode::Follower` is selected.
    /// Composer phases keep the repo-root cwd (default) — they pin every
    /// var they need explicitly and rely on `deployments.env` NOT
    /// overriding them.
    pub clean_cwd: bool,
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
            // Unique per spawn: `std::process::id()` is the TEST process,
            // shared by every node a test spawns (Phase A/B, c1/c2), so a
            // bare PID filename collides and later nodes overwrite earlier
            // logs. Disambiguate with the node name + a global spawn
            // counter (`LOG_COUNTER`, defined at module scope).
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
        let authrpc_port = free_port();
        let http_port = free_port();
        let ws_port = free_port();
        let p2p_port = free_port();
        let l1_http_port = free_port();
        let mut l1_auth_port = free_port();
        while l1_auth_port == l1_http_port || l1_auth_port == l1_http_port.saturating_add(1) {
            l1_auth_port = free_port();
        }
        let l1_p2p_port = free_port();
        let mut l1_discv5_port = free_port();
        while l1_discv5_port == l1_p2p_port {
            l1_discv5_port = free_port();
        }
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
        // Working dir: the repo root by default (so `dotenvy` loads the
        // repo's `.env` / `deployments.env`), or a fresh empty tempdir
        // when `clean_cwd` — see `NodeConfig::clean_cwd`. The tempdir must
        // outlive the process, so it is pushed into `keep_alive` below.
        let (cwd, cwd_tempdir): (PathBuf, Option<tempfile::TempDir>) = if cfg.clean_cwd {
            let td = tempfile::Builder::new()
                .prefix("eez-node-cwd-")
                .tempdir()
                .context("clean cwd tempdir")?;
            (td.path().to_path_buf(), Some(td))
        } else {
            (repo_root(), None)
        };
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_eez-node"));
        cmd.current_dir(&cwd)
            .args(["node", "--chain"])
            .arg(&chain_arg)
            .arg("--datadir")
            .arg(datadir)
            .stdout(stdout)
            .args([
                "--http",
                "--http.api",
                "eth,net,web3,debug,txpool",
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
                // Force the SEQUENTIAL (fallback) state-root computation. Under
                // the multi-node test load (composer + embedded L1 + follower
                // all running reth), the async parallel state-root task times
                // out (`state_root_task_timeout`, default ~1s) for the heavier
                // outbound blocks and the canonicalizing FCU then races ahead of
                // the slow sequential recompute, getting rejected ("block hash
                // does not exist in Headers table") — which strands the follower
                // deriver in a "parent N missing" loop so re-derived blocks never
                // become queryable. The fallback path is synchronous (no async
                // task, no timeout, no race) and reth documents this flag as
                // "useful for testing". Production never has competing reth
                // nodes, so this is purely a test-environment determinism knob.
                "--engine.state-root-fallback",
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
            .env("EEZ_L1_DATADIR", &l1_datadir);
        for (k, v) in env {
            if *k == TEST_L2_GENESIS_ENV {
                continue; // test-only marker (consumed as --chain above)
            }
            cmd.env(*k, v);
        }
        // Point the node's OUTBOUND L2 provider at its OWN L2 RPC (this random
        // http_port), not the :18688 production default. The ingress middleware
        // reads L2 state here for BOTH the outbound admission gate (nonce/
        // balance) AND the dynamic outbound classification (`authorizedProxies`
        // lookup) — both must hit THIS node's state, where the test's proxy +
        // funded sender live. Mirrors production, where :18688 IS the node's own
        // RPC. Set last so it wins over any inherited value.
        cmd.env("EEZ_L2_RPC_URL", format!("http://127.0.0.1:{http_port}"));
        let child = cmd.spawn().context("spawn eez-node")?;
        Ok(Self {
            child,
            name: name.to_string(),
            log_path,
            keep_alive: log_tempdir.into_iter().chain(cwd_tempdir).collect(),
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
        let mut handle = Self::spawn_with(name, datadir.path(), cfg, env)?;
        handle.keep_alive.push(datadir);
        wait_for_l2_rpc(&handle.l2_rpc_url(), Duration::from_secs(90)).await?;
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
        let patterns = [
            "reorg rolled out",
            "rewinding ring to common ancestor",
            "l1.reorg.retreated",
            "L1 reorg reported",
        ];
        wait_for(timeout, || async {
            Ok((self.log_count_matching(&patterns)? > 0).then_some(()))
        })
        .await
        .with_context(|| format!("{} deriver missed the reorg", self.name))
    }

    /// Assert this node never logged a fatal-class line (process death
    /// markers). Without this, every other invariant check is moot —
    /// a dead node trivially "agrees" by virtue of having stopped.
    pub fn assert_no_process_death(&self) {
        let patterns = ["Fatal", "UnexpectedStaticFile"];
        assert_eq!(
            self.log_count_matching(&patterns).unwrap(),
            0,
            "{} fatal error",
            self.name,
        );
    }

    /// Count lines in `log_path` matching ANY of `patterns` (substring
    /// match). Used by the multi-composer reorg test to assert
    /// reorg handling on both composers AND zero `Fatal` /
    /// `UnexpectedStaticFile` events.
    pub fn log_count_matching(&self, patterns: &[&str]) -> Result<usize> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read log {}", self.log_path.display()))?;
        Ok(contents
            .lines()
            .filter(|line| patterns.iter().any(|p| line.contains(p)))
            .count())
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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
            IEEZ::ImmediateEntrySkipped::SIGNATURE_HASH,
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
    let r = registry.rollups(U256::from(rollup_id)).call().await?;
    Ok(r.stateRoot)
}

/// Read `rollups[rollupId].etherBalance` on the L1 EEZ — the rollup's
/// escrowed ETH reserve. A value-bearing INBOUND deposit credits it (+V);
/// a value-bearing OUTBOUND withdrawal debits it (−V, gated by
/// InsufficientRollupBalance). Used by the value-outbound test to wait for
/// the deposit to fund the reserve and to assert the withdrawal debit.
pub async fn rollup_ether_balance(rpc_url: &str, eez: Address, rollup_id: u64) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let registry = IEEZ::new(eez, &provider);
    let r = registry.rollups(U256::from(rollup_id)).call().await?;
    Ok(r.etherBalance)
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

/// Return `env` with EVERY entry for `key` removed. Used to demote a
/// COMPOSER env into a FOLLOWER env: `Mode::from_env` (main.rs:73-80)
/// keys "follower" on `EEZ_PROOF_SIGNER_KEY` being UNSET while
/// `EEZ_L1_RPC_URL` is SET. `cross_chain_env` always sets the proof
/// signer (it builds a composer), so we strip it here. A bare
/// `override_env(..., "")` would NOT work: `env_var_os(..).is_some()`
/// is true even for an empty value, so the node would still boot as a
/// composer and spawn its OWN empty embedded L1.
pub fn remove_env(env: Vec<(&'static str, String)>, key: &str) -> Vec<(&'static str, String)> {
    env.into_iter().filter(|(k, _)| *k != key).collect()
}

/// Poll for the receipt of an L2 tx at `rpc_url`. Returns the receipt's
/// success status once mined, or times out. Used by S5's negative
/// control to prove a plain (non-proxy) L2 value transfer mines as a
/// NORMAL L2 tx (gets a receipt) — i.e. it is not held / classified
/// outbound.
pub async fn wait_for_l2_tx_receipt(
    rpc_url: &str,
    tx_hash: alloy_primitives::TxHash,
    timeout: Duration,
) -> Result<bool> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for(timeout, || async {
        Ok(provider
            .get_transaction_receipt(tx_hash)
            .await?
            .map(|r| r.status()))
    })
    .await
}

/// Follower variant of [`wait_for_node_caught_up`] for the embedded-L1
/// (cross-chain) topology. `wait_for_node_caught_up` takes a [`Chain`]
/// bound to an external [`Anvil`]; the embedded L1 has only an
/// [`L1Chain`]. This waits until the follower's `safe` head state root
/// appears in the embedded L1 EEZ's `L2ExecutionPerformed` attestation
/// history (which grows monotonically, so it never races the contract's
/// advancing head). Returns the matched root so the caller can assert it
/// equals the S3 settled root.
pub async fn wait_for_follower_caught_up(
    follower: &NodeHandle,
    l1: &L1Chain,
    timeout: Duration,
) -> Result<B256> {
    wait_for(timeout, || async {
        let node_root = safe_block_state_root(&follower.l2_rpc_url())
            .await
            .ok()
            .flatten();
        let attested =
            all_l2_execution_states(&l1.rpc_url, l1.eez_address, l1.rollup_id, l1.deploy_block)
                .await
                .unwrap_or_default();
        Ok(match node_root {
            Some(n) if n != B256::ZERO && attested.contains(&n) => Some(n),
            _ => None,
        })
    })
    .await
}
