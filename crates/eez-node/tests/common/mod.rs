//! Shared process and chain fixtures for node integration tests.

#![allow(dead_code)]

use std::{
    collections::HashSet,
    fmt::Write as _,
    net::{TcpListener, UdpSocket},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, B256, Signature, U256, address, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{BlockNumHash, BlockNumberOrTag, TransactionReceipt, TransactionRequest};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolError, SolEvent, SolValue, sol};
use anyhow::{Context, Result, anyhow, bail};
use eez_control_rpc::{
    MAX_MESSAGE_BYTES,
    v1::{
        ProveChunk, ProveResponse, prove_chunk,
        prover_client::ProverClient,
        prover_server::{Prover, ProverServer},
    },
};
use eez_protocol::EEZL2_ADDRESS;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};

/// Anvil's first default account (mnemonic `test test test test test test test test test test test junk`).
pub const ANVIL_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const ANVIL_ADDR: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
pub const ANVIL_KEY_1: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
pub const ANVIL_KEY_2: &str = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a";
pub const ANVIL_KEY_3: &str = "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6";
pub const ANVIL_KEY_4: &str = "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a";
// Account #0 derives the reserved L2 system address and cannot attest.
pub const ANVIL_ATTESTER_KEY: &str =
    "0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba";
pub const ANVIL_ADDR_3: Address = address!("0x90F79bf6EB2c4f870365E785982E1f101E93b906");
/// Dedicated deterministic L2 system identity; deliberately not an Anvil account.
pub const L2_SYSTEM_KEY: &str =
    "0x6f7d72ecb79c8bf1bd8e7c49a1c4a22741ab708f06bb19e5b5d44a6f0934a7c1";

// K = L1/L2 = 2 matches standalone's 2s cadence and leaves one L2 slot for proving.
const L1_BLOCK_TIME_SECS: u64 = 4;

// Consumed by the launcher as `--chain`; never forwarded to `eez-node`.
const TEST_L2_GENESIS_ENV: &str = "EEZ_TEST_L2_GENESIS_PATH";

const TX_SPAM_INTERVAL: Duration = Duration::from_secs(1);

static LOG_COUNTER: AtomicUsize = AtomicUsize::new(0);
// Keep released probe ports unique within each integration-test process.
static ASSIGNED_PORTS: LazyLock<Mutex<HashSet<u16>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
static WORKSPACE_BUILD_LOCK: Mutex<()> = Mutex::new(());

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

fn anvil_bin() -> String {
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
    log_path: PathBuf,
    _log_dir: Option<tempfile::TempDir>,
}

struct AnvilConfig {
    block_time_secs: u64,
    mnemonic: Option<&'static str>,
    hardfork: Option<&'static str>,
    gas_limit: Option<u64>,
    genesis_timestamp: u64,
}

impl AnvilConfig {
    fn standard(genesis_timestamp: u64) -> Self {
        Self {
            block_time_secs: L1_BLOCK_TIME_SECS,
            mnemonic: None,
            hardfork: None,
            gas_limit: None,
            genesis_timestamp,
        }
    }

    fn for_reorg(genesis_timestamp: u64) -> Self {
        Self {
            block_time_secs: L1_BLOCK_TIME_SECS,
            mnemonic: Some(HARDHAT_MNEMONIC),
            hardfork: Some("cancun"),
            gas_limit: Some(30_000_000),
            genesis_timestamp,
        }
    }
}

const HARDHAT_MNEMONIC: &str = "test test test test test test test test test test test junk";

impl Anvil {
    async fn spawn_with(port: u16, cfg: AnvilConfig) -> Result<Self> {
        let (log_path, log_dir) = test_log_destination("anvil")?;
        let log = std::fs::File::create(&log_path).context("create anvil log")?;
        let err_log = log.try_clone().context("clone anvil log")?;
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
        cmd.args(["--timestamp", &cfg.genesis_timestamp.to_string()]);
        let mut child = cmd
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err_log))
            .spawn()
            .context("spawn anvil")?;
        let rpc_url = format!("http://127.0.0.1:{port}");
        let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
        let startup_timeout = Duration::from_secs(10);
        let start = Instant::now();
        while start.elapsed() < startup_timeout {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let probe = tokio::time::timeout(Duration::from_secs(1), provider.get_block_number());
            if matches!(probe.await, Ok(Ok(_))) {
                return Ok(Self {
                    child,
                    rpc_url,
                    log_path,
                    _log_dir: log_dir,
                });
            }
            if let Some(status) = child.try_wait().context("poll anvil process")? {
                bail!(
                    "anvil exited before RPC became ready with {status}; log:\n{}",
                    std::fs::read_to_string(&log_path).unwrap_or_default(),
                );
            }
        }
        let _ = child.kill();
        let _ = child.wait();
        bail!(
            "anvil did not start within {startup_timeout:?} on port {port}; log:\n{}",
            std::fs::read_to_string(&log_path).unwrap_or_default(),
        );
    }

    pub async fn set_balance(&self, addr: Address, wei: U256) -> Result<()> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url.parse()?);
        let _: serde_json::Value = provider
            .client()
            .request("anvil_setBalance", (addr, wei))
            .await
            .context("anvil_setBalance")?;
        Ok(())
    }

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

impl Drop for Anvil {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "\n== anvil log tail ==\n{}",
                last_lines(&self.log_path, 40, None)
            );
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Forwards single-transaction `eth_sendBundle` payloads to Anvil.
///
/// Anvil has no builder API, so the stub explicitly rejects bundles containing
/// zero or multiple transactions instead of pretending to preserve atomicity.
struct BundleStub {
    child: Child,
    url: String,
    log_path: PathBuf,
    _log_dir: Option<tempfile::TempDir>,
}

impl BundleStub {
    async fn spawn(port: u16, upstream: &str) -> Result<Self> {
        let script = repo_root().join("scripts/builder-stub.py");
        let listen = format!("127.0.0.1:{port}");
        let (log_path, log_dir) = test_log_destination("builder-stub")?;
        let log = std::fs::File::create(&log_path).context("create builder stub log")?;
        let err_log = log.try_clone().context("clone builder stub log")?;
        let mut child = Command::new("python3")
            .arg(script)
            .args(["--listen", &listen, "--upstream", upstream])
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err_log))
            .spawn()
            .context("spawn builder-stub.py")?;
        let url = format!("http://{listen}");
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if std::net::TcpStream::connect(&listen).is_ok() {
                return Ok(Self {
                    child,
                    url,
                    log_path,
                    _log_dir: log_dir,
                });
            }
            if let Some(status) = child.try_wait().context("poll builder stub process")? {
                bail!(
                    "builder stub exited before binding {listen} with {status}; log:\n{}",
                    std::fs::read_to_string(&log_path).unwrap_or_default(),
                );
            }
        }
        let _ = child.kill();
        let _ = child.wait();
        bail!(
            "builder-stub did not bind within 3s on {listen}; log:\n{}",
            std::fs::read_to_string(&log_path).unwrap_or_default(),
        );
    }
}

impl Drop for BundleStub {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "\n== builder stub log tail ==\n{}",
                last_lines(&self.log_path, 40, None),
            );
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Persists logs under `EEZ_TEST_LOG_DIR`, or owns them in a temporary directory.
fn test_log_destination(name: &str) -> Result<(PathBuf, Option<tempfile::TempDir>)> {
    if let Ok(dir) = std::env::var("EEZ_TEST_LOG_DIR") {
        std::fs::create_dir_all(&dir).with_context(|| format!("create EEZ_TEST_LOG_DIR {dir}"))?;
        let suffix = LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from(dir).join(format!("{name}-{}-{suffix}.log", std::process::id()));
        return Ok((path, None));
    }
    let dir = tempfile::tempdir().context("log tempdir")?;
    let path = dir.path().join(format!("{name}.log"));
    Ok((path, Some(dir)))
}

fn proof_signer_binary() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("EEZ_TEST_PROOF_SIGNER_BIN") {
        return Ok(path.into());
    }
    let current = std::env::current_exe().context("current test executable")?;
    let target_profile = current
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or_else(|| anyhow!("test executable has no target profile directory"))?;
    let path = target_profile.join(format!("eez-proof-signer{}", std::env::consts::EXE_SUFFIX));
    if path.is_file() {
        return Ok(path);
    }

    // Keeps concurrent test fns in this binary from each spawning their own
    // build.
    let _build_guard = WORKSPACE_BUILD_LOCK
        .lock()
        .map_err(|_| anyhow!("workspace build lock poisoned"))?;
    if !path.is_file() {
        let profile = target_profile
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("target profile directory is not valid UTF-8"))?;
        let mut command = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
        command.current_dir(repo_root()).args([
            "build",
            "--locked",
            "-p",
            "eez-proof-signer",
            "--bin",
            "eez-proof-signer",
        ]);
        if profile != "debug" {
            command.args(["--profile", profile]);
        }
        let status = command.status().context("build eez-proof-signer")?;
        if !status.success() {
            bail!("building eez-proof-signer failed with {status}");
        }
    }
    path.is_file()
        .then_some(path)
        .ok_or_else(|| anyhow!("eez-proof-signer build completed without producing the binary"))
}

/// Configuration for one real `eez-proof-signer` process.
#[derive(Debug)]
struct ProofSignerConfig<'a> {
    chain_config: &'a std::path::Path,
    rollup_id: u64,
    signer_key: &'a str,
    /// Registry vkey for `proof_system`; it may differ from `signer_key` in
    /// unauthorized-signer tests.
    vkey: B256,
    proof_system: Address,
}

/// Owns a proof-signer process and its diagnostic log.
#[derive(Debug)]
pub struct ProofSignerHandle {
    child: std::sync::Mutex<Child>,
    endpoint: String,
    log_path: PathBuf,
    _log_dir: Option<tempfile::TempDir>,
    _working_dir: tempfile::TempDir,
}

impl ProofSignerHandle {
    async fn spawn(cfg: &ProofSignerConfig<'_>) -> Result<Self> {
        let listen = format!("127.0.0.1:{}", free_port());
        let attester = signer_address(cfg.signer_key)?;
        let l2_system_address = signer_address(L2_SYSTEM_KEY)?;
        let (log_path, log_dir) = test_log_destination("eez-proof-signer")?;
        let working_dir = tempfile::tempdir().context("proof signer working directory")?;
        let log = std::fs::File::create(&log_path).context("create proof signer log")?;
        let err_log = log.try_clone().context("clone proof signer log")?;
        let mut child = Command::new(proof_signer_binary()?)
            // The signer also loads dotenv at startup. Keep its explicitly
            // constructed test environment isolated from repository config.
            .current_dir(working_dir.path())
            .args([
                "--listen-addr",
                &listen,
                "--chain-config",
                cfg.chain_config
                    .to_str()
                    .context("non-UTF-8 L2 genesis path")?,
                "--rollup-id",
                &cfg.rollup_id.to_string(),
                "--vkey",
                &format!("{:#x}", cfg.vkey),
                "--attester-address",
                &format!("{attester:#x}"),
                "--proof-system",
                &format!("{:#x}", cfg.proof_system),
                "--l2-system-address",
                &format!("{l2_system_address:#x}"),
            ])
            .env_clear()
            .env("EEZ_PROOF_SIGNER_KEY", cfg.signer_key)
            .env("EEZ_L2_SYSTEM_KEY", L2_SYSTEM_KEY)
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err_log))
            .spawn()
            .context("spawn eez-proof-signer")?;

        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if tokio::net::TcpStream::connect(&listen).await.is_ok() {
                return Ok(Self {
                    child: std::sync::Mutex::new(child),
                    endpoint: format!("http://{listen}"),
                    log_path,
                    _log_dir: log_dir,
                    _working_dir: working_dir,
                });
            }
            if let Some(status) = child.try_wait().context("poll proof signer process")? {
                bail!(
                    "eez-proof-signer exited before binding with {status}; log:\n{}",
                    std::fs::read_to_string(&log_path).unwrap_or_default()
                );
            }
        }
        let _ = child.kill();
        let _ = child.wait();
        bail!(
            "eez-proof-signer did not bind within 10s; log:\n{}",
            std::fs::read_to_string(&log_path).unwrap_or_default()
        )
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn log_tail(&self, max: usize) -> String {
        last_lines(&self.log_path, max, None)
    }

    fn successful_attestations(&self) -> Result<usize> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read signer log {}", self.log_path.display()))?;
        Ok(contents
            .lines()
            .filter(|line| line.contains("window validated and signed"))
            .count())
    }

    pub fn exit_status(&self) -> Option<std::process::ExitStatus> {
        self.child
            .lock()
            .expect("proof signer child mutex poisoned")
            .try_wait()
            .ok()
            .flatten()
    }

    pub fn assert_alive(&self) {
        if let Ok(Some(status)) = self
            .child
            .lock()
            .expect("proof signer child mutex poisoned")
            .try_wait()
        {
            panic!(
                "eez-proof-signer exited with {status}; log:\n{}",
                std::fs::read_to_string(&self.log_path).unwrap_or_default()
            );
        }
    }
}

impl Drop for ProofSignerHandle {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "\n== proof signer log tail ==\n{}",
                last_lines(&self.log_path, 60, None),
            );
        }
        let child = self
            .child
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = child.kill();
        let _ = child.wait();
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ProverMutation {
    None,
    PostBatch,
    Witness,
}

impl ProverMutation {
    fn apply(self, chunks: &mut [ProveChunk]) -> Result<(), Status> {
        match self {
            Self::None => {}
            Self::PostBatch => {
                let calldata = chunks
                    .iter_mut()
                    .find_map(|chunk| match chunk.kind.as_mut() {
                        Some(prove_chunk::Kind::Header(header)) => header
                            .post_batch
                            .as_mut()
                            .map(|post_batch| &mut post_batch.abi_calldata),
                        _ => None,
                    })
                    .ok_or_else(|| Status::internal("missing Prove header"))?;
                let byte = calldata
                    .last_mut()
                    .ok_or_else(|| Status::internal("empty PostBatch calldata"))?;
                *byte ^= 1;
            }
            Self::Witness => {
                let witness = chunks
                    .iter_mut()
                    .find_map(|chunk| match chunk.kind.as_mut() {
                        Some(prove_chunk::Kind::Block(block)) => block.witness.as_mut(),
                        _ => None,
                    })
                    .ok_or_else(|| Status::internal("missing block witness"))?;
                if let Some(byte) = witness.state.iter_mut().find_map(|node| node.first_mut()) {
                    *byte ^= 1;
                } else {
                    witness.state.push(vec![0xff]);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct ProxyCounters {
    attempts: AtomicUsize,
    successes: AtomicUsize,
    rejections: AtomicUsize,
}

#[derive(Clone, Debug)]
struct ProverProxyService {
    upstream: String,
    mutation: ProverMutation,
    counters: Arc<ProxyCounters>,
}

#[tonic::async_trait]
impl Prover for ProverProxyService {
    async fn prove(
        &self,
        request: Request<Streaming<ProveChunk>>,
    ) -> Result<Response<ProveResponse>, Status> {
        self.counters.attempts.fetch_add(1, Ordering::Relaxed);
        let mut input = request.into_inner();
        let mut chunks = Vec::new();
        while let Some(chunk) = input.message().await? {
            chunks.push(chunk);
        }
        self.mutation.apply(&mut chunks)?;

        let mut client = ProverClient::connect(self.upstream.clone())
            .await
            .map_err(|error| Status::unavailable(format!("connect upstream signer: {error}")))?
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
            .max_decoding_message_size(MAX_MESSAGE_BYTES);
        match client.prove(tokio_stream::iter(chunks)).await {
            Ok(response) => {
                self.counters.successes.fetch_add(1, Ordering::Relaxed);
                Ok(response)
            }
            Err(status) => {
                // Transport failures do not prove that the signer rejected the input.
                if matches!(
                    status.code(),
                    tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition
                ) {
                    self.counters.rejections.fetch_add(1, Ordering::Relaxed);
                }
                Err(Status::new(status.code(), status.message().to_string()))
            }
        }
    }
}

/// Optional mutation proxy in front of a real proof signer.
#[derive(Debug)]
pub struct ProverProxyHandle {
    endpoint: String,
    counters: Arc<ProxyCounters>,
    task: JoinHandle<()>,
}

impl ProverProxyHandle {
    async fn spawn(upstream: String, mutation: ProverMutation) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let counters = Arc::new(ProxyCounters::default());
        let service = ProverProxyService {
            upstream,
            mutation,
            counters: Arc::clone(&counters),
        };
        let task = tokio::spawn(async move {
            let server = ProverServer::new(service)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES);
            let _ = tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        Ok(Self {
            endpoint: format!("http://{addr}"),
            counters,
            task,
        })
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn attempts(&self) -> usize {
        self.counters.attempts.load(Ordering::Relaxed)
    }

    pub fn successes(&self) -> usize {
        self.counters.successes.load(Ordering::Relaxed)
    }

    pub fn rejections(&self) -> usize {
        self.counters.rejections.load(Ordering::Relaxed)
    }
}

impl Drop for ProverProxyHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Owns the L1, builder stub, deployment, genesis, and proof signers for a test.
pub struct Harness {
    pub anvil: Anvil,
    stub: BundleStub,
    pub dep: Deployment,
    // The owning tempdir must outlive every node and signer using this genesis.
    l2_genesis: (PathBuf, tempfile::TempDir),
    // Each composer gets isolated state because the signer admits one request at a time.
    provers: std::sync::Mutex<Vec<(ProofSignerHandle, tempfile::TempDir)>>,
}

impl Harness {
    /// Starts a fixture with L1 and L2 genesis timestamps aligned to wall time.
    pub async fn fresh() -> Result<Self> {
        let ts = now_unix_secs();
        let l2_genesis = write_dev_genesis_at(ts)?;
        let cfg = AnvilConfig::standard(ts);
        Self::with_anvil_config(cfg, dev_genesis_state_root(), l2_genesis).await
    }

    /// Starts the reorg fixture with one timestamp shared by Anvil and the L2.
    pub async fn for_reorg() -> Result<Self> {
        let ts = now_unix_secs();
        let l2_genesis = write_l2_genesis_at(ts)?;
        let cfg = AnvilConfig::for_reorg(ts);
        Self::with_anvil_config(cfg, reorg_genesis_state_root()?, l2_genesis).await
    }

    async fn with_anvil_config(
        cfg: AnvilConfig,
        initial_state: B256,
        l2_genesis: (PathBuf, tempfile::TempDir),
    ) -> Result<Self> {
        let anvil = Anvil::spawn_with(free_port(), cfg).await?;
        let stub = BundleStub::spawn(free_port(), &anvil.rpc_url).await?;
        let dep = deploy_contracts_with_initial(&anvil.rpc_url, ANVIL_KEY, initial_state).await?;
        Ok(Self {
            anvil,
            stub,
            dep,
            l2_genesis,
            provers: std::sync::Mutex::new(Vec::new()),
        })
    }

    pub fn chain(&self) -> Chain<'_> {
        Chain::new(&self.anvil, &self.dep)
    }

    /// Stages a local chain that can later restart as a composer or follower.
    pub fn standalone_env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("EEZ_L2_BLOCK_TIME_MS", "2000".to_string()),
            (
                "RUST_LOG",
                std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string()),
            ),
            (
                TEST_L2_GENESIS_ENV,
                self.l2_genesis.0.to_string_lossy().into_owned(),
            ),
        ]
    }

    /// Counts signer successes so negative tests cannot pass because proving stalled.
    pub fn successful_attestations(&self) -> Result<usize> {
        self.provers
            .lock()
            .map_err(|_| anyhow!("prover registry poisoned"))?
            .iter()
            .try_fold(0usize, |total, (signer, _)| {
                Ok(total + signer.successful_attestations()?)
            })
    }

    pub fn l2_genesis_path(&self) -> &std::path::Path {
        &self.l2_genesis.0
    }

    pub async fn env(&self) -> Result<Vec<(&'static str, String)>> {
        self.env_for(ANVIL_KEY, false).await
    }

    pub async fn env_for(
        &self,
        poster_key: &str,
        expect_external_batches: bool,
    ) -> Result<Vec<(&'static str, String)>> {
        self.env_for_options(NodeEnvOptions {
            poster_key: Some(poster_key),
            proof_signer_key: Some(ANVIL_ATTESTER_KEY),
            rollup_id: self.dep.rollup_id,
            expect_external_batches,
            sequencer_rpc: None,
        })
        .await
    }

    pub async fn follower_env(
        &self,
        sequencer_rpc: Option<&str>,
    ) -> Result<Vec<(&'static str, String)>> {
        self.env_for_options(NodeEnvOptions {
            poster_key: None,
            proof_signer_key: None,
            rollup_id: self.dep.rollup_id,
            expect_external_batches: true,
            sequencer_rpc,
        })
        .await
    }

    pub async fn env_with_rollup_id(&self, rollup_id: u64) -> Result<Vec<(&'static str, String)>> {
        self.env_for_options(NodeEnvOptions {
            poster_key: Some(ANVIL_KEY),
            proof_signer_key: Some(ANVIL_ATTESTER_KEY),
            rollup_id,
            expect_external_batches: false,
            sequencer_rpc: None,
        })
        .await
    }

    pub async fn env_with_proof_signer(
        &self,
        proof_signer_key: &str,
    ) -> Result<Vec<(&'static str, String)>> {
        self.env_for_options(NodeEnvOptions {
            poster_key: Some(ANVIL_KEY),
            proof_signer_key: Some(proof_signer_key),
            rollup_id: self.dep.rollup_id,
            expect_external_batches: false,
            sequencer_rpc: None,
        })
        .await
    }

    async fn env_for_options(
        &self,
        opts: NodeEnvOptions<'_>,
    ) -> Result<Vec<(&'static str, String)>> {
        let mut env = vec![
            ("EEZ_L1_CHAIN_PATH", "dev".to_string()),
            ("EEZ_L1_RPC_URL", self.anvil.rpc_url.clone()),
            ("EEZ_L1_CHAIN_ID", DEV_CHAIN_ID.to_string()),
            ("EEZ_L1_CHAIN", "testing".to_string()),
            ("EEZ_L2_SYSTEM_KEY", L2_SYSTEM_KEY.to_string()),
            ("EEZL2_ADDRESS", format!("{EEZL2_ADDRESS:#x}")),
            (
                "EEZ_L1_BLOCK_TIME_MS",
                (L1_BLOCK_TIME_SECS * 1000).to_string(),
            ),
            ("EEZ_L2_BLOCK_TIME_MS", "2000".to_string()),
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
                format!("{:#x}", self.dep.proof_system_address),
            ),
            (
                "EEZ_ROLLUP_MANAGER_ADDRESS",
                format!("{:#x}", self.dep.rollup_manager_address),
            ),
            ("EEZ_ROLLUP_ID", opts.rollup_id.to_string()),
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
                self.l2_genesis.0.to_string_lossy().into_owned(),
            ),
        ];

        if let Some(poster_key) = opts.poster_key {
            env.extend([
                ("EEZ_L1_TARGET_RPC_URL", self.anvil.rpc_url.clone()),
                ("EEZ_L1_BUILDER_RPC_URL", self.stub.url.clone()),
                ("EEZ_L1_POSTER_KEY", poster_key.to_string()),
            ]);
        }

        // Composer tests use the real remote-prover path. Followers omit it.
        if let Some(signer_key) = opts.proof_signer_key {
            let genesis = self.l2_genesis_path();
            let attester = signer_address(signer_key)?;
            let signer = ProofSignerHandle::spawn(&ProofSignerConfig {
                chain_config: genesis,
                rollup_id: opts.rollup_id,
                signer_key,
                // Unauthorized-signer tests still hash the registry's vkey.
                vkey: signer_address(ANVIL_ATTESTER_KEY)?.into_word(),
                proof_system: self.dep.proof_system_address,
            })
            .await?;
            let witness_dir = tempfile::tempdir().context("witness DB tempdir")?;
            env.extend([
                ("EEZ_PROVER_URL", signer.endpoint().to_owned()),
                ("EEZ_ATTESTER_ADDRESS", format!("{attester:#x}")),
                (
                    "EEZ_WITNESS_DB_PATH",
                    witness_dir.path().to_string_lossy().into_owned(),
                ),
            ]);
            self.provers
                .lock()
                .map_err(|_| anyhow!("prover registry poisoned"))?
                .push((signer, witness_dir));
        }
        if let Some(sequencer_rpc) = opts.sequencer_rpc {
            env.push(("EEZ_SEQUENCER_RPC", sequencer_rpc.to_string()));
        }
        Ok(env)
    }
}

struct NodeEnvOptions<'a> {
    poster_key: Option<&'a str>,
    proof_signer_key: Option<&'a str>,
    rollup_id: u64,
    expect_external_batches: bool,
    sequencer_rpc: Option<&'a str>,
}

pub struct Deployment {
    pub eez_address: Address,
    pub deploy_block: u64,
    pub proof_system_address: Address,
    pub rollup_manager_address: Address,
    pub rollup_id: u64,
}

sol! {
    #[sol(rpc)]
    interface IEEZ {
        error InvalidProof();
        error InvalidProofSystemConfig();
        event BatchPosted(uint256 rollupCount);
        event L2ExecutionPerformed(uint64 indexed rollupId, bytes32 newState);
        event L2TxSkipped(uint256 indexed transientIdx, bytes revertData);
        function rollups(uint64 rollupId) external view returns (address rollupContract, bytes32 stateRoot, uint256 etherBalance);
        function rollupCounter() external view returns (uint256);
        function registerRollup(address rollupContract, bytes32 initialState) external returns (uint64 rollupId);
    }
}

pub const INVALID_PROOF_SELECTOR: [u8; 4] = IEEZ::InvalidProof::SELECTOR;
pub const INVALID_PROOF_SYSTEM_CONFIG_SELECTOR: [u8; 4] = IEEZ::InvalidProofSystemConfig::SELECTOR;

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

/// L2 genesis fixture used by reorg scenarios.
fn reorg_genesis_path() -> PathBuf {
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

/// Sign and submit one legacy L2 value transfer. The default dev L2 does not
/// expose EIP-1559 fee estimation, so a legacy transaction keeps background
/// traffic valid on both the dev chain and the Cancun reorg fixture.
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
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price: provider.get_gas_price().await?,
        gas_limit: 21_000,
        to: alloy_primitives::TxKind::Call(to),
        value,
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

/// Deploy and register the protocol with an explicit initial L2 state.
async fn deploy_contracts_with_initial(
    rpc_url: &str,
    key: &str,
    initial_state: B256,
) -> Result<Deployment> {
    // Anvil signs transactions from its default accounts.
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

    // The attester is NOT the deployer: `key` is also the L2 system key here,
    // and the signer refuses to attest with a key deriving the system address.
    let attester = signer_address(ANVIL_ATTESTER_KEY)?;
    let proof_system_address = deploy(
        &provider,
        signer_addr,
        &out.join("ECDSAProofSystem.sol/ECDSAProofSystem.json"),
        attester.abi_encode(),
    )
    .await?;

    // Rollup(address eez, address owner, uint256 threshold,
    //        address[] proofSystems, bytes32[] vkeys)
    let proof_systems: Vec<Address> = vec![proof_system_address];
    // vkey embeds the authorized signer address; the registry treats vkey as
    // opaque but checks non-zero + membership (see DeployRollup.s.sol:60).
    let vkeys: Vec<B256> = vec![attester.into_word()];
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
        proof_system_address,
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
    child: std::sync::Mutex<Child>,
    background_tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
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
    pub http_port: u16,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum NodeBinary {
    #[default]
    Composer,
    Follower,
    Dev,
}

impl NodeBinary {
    fn path(self) -> &'static str {
        match self {
            Self::Composer => env!("CARGO_BIN_EXE_eez-composer"),
            Self::Follower => env!("CARGO_BIN_EXE_eez-follower"),
            Self::Dev => env!("CARGO_BIN_EXE_eez-dev-node"),
        }
    }
}

#[derive(Default)]
pub struct NodeConfig<'a> {
    /// Explicit node-role executable to launch.
    pub binary: NodeBinary,
    pub genesis_path: Option<&'a std::path::Path>,
}

impl NodeHandle {
    fn spawn(datadir: &std::path::Path, env: &[(&'static str, String)]) -> Result<Self> {
        Self::spawn_with("node", datadir, &NodeConfig::default(), env)
    }

    fn spawn_with(
        name: &str,
        datadir: &std::path::Path,
        cfg: &NodeConfig<'_>,
        env: &[(&'static str, String)],
    ) -> Result<Self> {
        let (log_path, log_tempdir) = test_log_destination(&format!("eez-node-{name}"))?;
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
        let mut cmd = Command::new(cfg.binary.path());
        // Each role binary loads dotenv files from its working directory. Running from
        // the datadir prevents repository settings from changing the explicit
        // test configuration or redirecting L1 traffic to a developer endpoint.
        cmd.current_dir(datadir)
            .args(["node", "--chain"])
            .arg(&chain_arg)
            .arg("--datadir")
            .arg(datadir)
            .stdout(stdout)
            .args([
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
            .env("NO_COLOR", "1")
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
        let child = cmd.spawn().context("spawn eez node role")?;
        Ok(Self {
            child: std::sync::Mutex::new(child),
            background_tasks: std::sync::Mutex::new(Vec::new()),
            name: name.to_string(),
            log_path,
            keep_alive: log_tempdir.into_iter().collect(),
            http_port,
        })
    }

    pub async fn start(
        name: &str,
        cfg: &NodeConfig<'_>,
        env: &[(&'static str, String)],
    ) -> Result<Self> {
        if let Ok(root) = std::env::var("EEZ_TEST_DATADIR_DIR") {
            let suffix = LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
            let datadir = PathBuf::from(root)
                .join(format!("eez-node-{name}-{}-{suffix}", std::process::id()));
            std::fs::create_dir_all(&datadir)
                .with_context(|| format!("create retained test datadir {}", datadir.display()))?;
            return Self::start_with_datadir(name, &datadir, cfg, env).await;
        }
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
        handle
            .wait_for_rpc(&handle.l2_rpc_url(), Duration::from_mins(3), "L2 RPC")
            .await?;
        Ok(handle)
    }

    pub fn l2_rpc_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    /// Waits for RPC readiness while surfacing early process exits and logs.
    pub async fn wait_for_rpc(&self, rpc_url: &str, timeout: Duration, label: &str) -> Result<()> {
        let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
        let start = Instant::now();
        while start.elapsed() < timeout {
            let probe = tokio::time::timeout(Duration::from_secs(1), provider.get_block_number());
            if matches!(probe.await, Ok(Ok(_))) {
                return Ok(());
            }
            if let Some(status) = self.exit_status() {
                bail!(
                    "{} exited with {status} before {label} became ready; log:\n{}",
                    self.name,
                    self.log_tail(80),
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        bail!(
            "{} {label} at {rpc_url} did not become ready within {timeout:?}; log:\n{}",
            self.name,
            self.log_tail(80),
        )
    }

    /// Starts managed background traffic; the task is aborted with the node.
    pub fn run_tx_spammer(&self, from_key: &'static str) {
        let url = self.l2_rpc_url();
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(TX_SPAM_INTERVAL).await;
                let _ =
                    send_l2_value_transfer(&url, from_key, ANVIL_ADDR_3, U256::from(1u64)).await;
            }
        });
        self.background_tasks
            .lock()
            .expect("node background task mutex poisoned")
            .push(task);
    }

    // Convergence alone can pass without exercising the reorg path.
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
        if let Some(status) = self.exit_status() {
            panic!(
                "{} exited unexpectedly with {status}; log:\n{}",
                self.name,
                self.log_tail(80),
            );
        }
        let patterns = ["Fatal", "UnexpectedStaticFile"];
        assert_eq!(
            self.log_count_matching(&patterns).unwrap(),
            0,
            "{} fatal error",
            self.name,
        );
    }

    pub fn assert_no_divergence_failure_logs(&self) {
        self.assert_no_process_death();
        let patterns = [
            "eez.deriver.state.diverged",
            "local L2 state root differs",
            "engine rejected safe/finalized FCU",
            "payload builder returned no payload",
        ];
        assert_eq!(
            self.log_count_matching(&patterns).unwrap(),
            0,
            "{} logged a divergence/fatal-class failure",
            self.name,
        );
    }

    pub fn log_count_matching(&self, patterns: &[&str]) -> Result<usize> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read log {}", self.log_path.display()))?;
        Ok(contents
            .lines()
            .filter(|line| patterns.iter().any(|p| line.contains(p)))
            .count())
    }

    pub fn log_lines_matching(&self, patterns: &[&str], max: usize) -> String {
        last_lines(&self.log_path, max, Some(patterns))
    }

    pub fn log_tail(&self, max: usize) -> String {
        last_lines(&self.log_path, max, None)
    }

    pub fn exit_status(&self) -> Option<std::process::ExitStatus> {
        self.child
            .lock()
            .expect("node child mutex poisoned")
            .try_wait()
            .ok()
            .flatten()
    }
}

fn last_lines(path: &std::path::Path, max: usize, patterns: Option<&[&str]>) -> String {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return format!("<unreadable log {}>", path.display());
    };
    let selected: Vec<&str> = contents
        .lines()
        .filter(|line| patterns.is_none_or(|ps| ps.iter().any(|p| line.contains(p))))
        .collect();
    let skip = selected.len().saturating_sub(max);
    selected[skip..].join("\n")
}

// The fmt subscriber prints these messages, not tracing event names.
const SETTLEMENT_LOG_MARKERS: &[&str] = &[
    "remote prover attested the window",
    "dispatching bundle to builder",
    "eth_sendBundle response received",
    "bundle dropped",
    "compose",
    "postBatch",
    "postAndVerifyBatch",
    "InvalidProof",
    "prover",
    "attest",
    "revert",
    "ERROR",
    "WARN",
];

impl Drop for NodeHandle {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "\n== {} node log tail ==\n{}",
                self.name,
                self.log_tail(100),
            );
        }
        let tasks = self
            .background_tasks
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for task in tasks.drain(..) {
            task.abort();
        }
        let child = self
            .child
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = child.kill();
        let _ = child.wait();
    }
}

async fn l2_block_by_tag(rpc_url: &str, tag: BlockNumberOrTag) -> Result<Option<BlockNumHash>> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider.get_block_by_number(tag).await?;
    Ok(block.map(|b| BlockNumHash::new(b.header.number, b.header.hash)))
}

async fn l2_block_by_number(rpc_url: &str, number: u64) -> Result<Option<BlockNumHash>> {
    l2_block_by_tag(rpc_url, BlockNumberOrTag::Number(number)).await
}

async fn latest_block_snapshot(node: &NodeHandle) -> Result<Option<BlockNumHash>> {
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

/// Waits for nodes to agree at a safe height at or above `min_height`.
/// The lower bound prevents stale agreement from satisfying convergence.
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
    let result = wait_for(timeout, || async {
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
    .await;
    let err = match result {
        Ok(block) => return Ok(block),
        Err(err) => err,
    };

    let mut diagnostics = String::new();
    let mut diagnostic_height = None;
    for node in nodes {
        if let Ok(Some(block)) = l2_block_by_tag(&node.l2_rpc_url(), tag).await {
            diagnostic_height = Some(
                diagnostic_height.map_or(block.number, |height: u64| height.min(block.number)),
            );
        }
    }
    for node in nodes {
        let latest = l2_block_by_tag(&node.l2_rpc_url(), BlockNumberOrTag::Latest)
            .await
            .ok()
            .flatten();
        let tagged = l2_block_by_tag(&node.l2_rpc_url(), tag)
            .await
            .ok()
            .flatten();
        let header = if let Some(height) = diagnostic_height {
            let provider = ProviderBuilder::new().connect_http(node.l2_rpc_url().parse()?);
            provider
                .get_block_by_number(BlockNumberOrTag::Number(height))
                .await
                .ok()
                .flatten()
                .map(|block| block.header)
        } else {
            None
        };
        let _ = write!(
            diagnostics,
            "\n== {} convergence state ==\nexit={:?} latest={latest:?} target_tag={tagged:?} common_header={header:#?}\n{}\n",
            node.name,
            node.exit_status(),
            node.log_lines_matching(
                &[
                    "advanced L2 safe head",
                    "reconcile",
                    "diverged",
                    "reorg",
                    "ERROR",
                    "WARN",
                ],
                40,
            ),
        );
    }
    Err(err.context(format!("nodes did not converge:{diagnostics}")))
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

pub struct Chain<'a> {
    rpc_url: &'a str,
    eez_address: Address,
    deploy_block: u64,
    rollup_id: u64,
}

impl<'a> Chain<'a> {
    fn new(anvil: &'a Anvil, dep: &Deployment) -> Self {
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

    /// Assert that a submitted `postAndVerifyBatch` for `expected_rollup_id`
    /// was mined, reverted, and reproduces `expected_revert_selector` via
    /// `eth_call` against the resulting chain state.
    pub async fn assert_failed_post_and_verify_batch(
        &self,
        expected_rollup_id: u64,
        expected_revert_selector: [u8; 4],
    ) -> Result<()> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url.parse()?);
        let latest = provider.get_block_number().await?;

        for block_number in self.deploy_block..=latest {
            let Some(block) = provider
                .get_block_by_number(BlockNumberOrTag::Number(block_number))
                .full()
                .await?
            else {
                continue;
            };
            for transaction in block.transactions.txns() {
                if transaction.inner.to() != Some(self.eez_address)
                    || !transaction
                        .inner
                        .input()
                        .starts_with(&eez_protocol::abi::postAndVerifyBatchCall::SELECTOR)
                {
                    continue;
                }
                let call = eez_protocol::abi::postAndVerifyBatchCall::abi_decode(
                    transaction.inner.input(),
                )?;
                if !call
                    .batch
                    .rollupIdsWithProofSystems
                    .iter()
                    .any(|rollup| rollup.rollupId == expected_rollup_id)
                {
                    continue;
                }

                let tx_hash = *transaction.inner.tx_hash();
                let receipt = provider
                    .get_transaction_receipt(tx_hash)
                    .await?
                    .ok_or_else(|| anyhow!("postAndVerifyBatch receipt {tx_hash} is missing"))?;
                if receipt.status() {
                    bail!(
                        "postAndVerifyBatch transaction {tx_hash} for rollup {expected_rollup_id} succeeded"
                    );
                }

                let replay = TransactionRequest::default()
                    .from(transaction.inner.signer())
                    .to(self.eez_address)
                    .input(transaction.inner.input().clone().into());
                let err = provider
                    .call(replay)
                    .await
                    .expect_err("failed postAndVerifyBatch replay unexpectedly succeeded");
                let expected = format!("0x{}", hex::encode(expected_revert_selector));
                let error_response = err.as_error_resp().ok_or_else(|| {
                    anyhow!("postAndVerifyBatch replay returned non-RPC error: {err}")
                })?;
                let observed = error_response.data.as_ref().map_or_else(
                    || error_response.message.to_string(),
                    |data| format!("{} {}", error_response.message, data.get()),
                );
                if !observed.contains(&expected) {
                    bail!(
                        "postAndVerifyBatch replay returned {observed}, expected selector {expected}"
                    );
                }
                return Ok(());
            }
        }

        bail!("no mined postAndVerifyBatch transaction found for rollup {expected_rollup_id}")
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

/// Count events of `event_sig_hash` emitted by `contract` since
/// `from_block`. Used by tests that assert exact event counts.
async fn count_events(
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

/// Recomputes the latest batch's public-input hash and verifies its signature.
pub async fn assert_latest_batch_signature(
    l1_rpc: &str,
    dep: &Deployment,
    expected_attester: Address,
) -> Result<()> {
    use alloy_rpc_types_eth::Filter;

    let provider = ProviderBuilder::new().connect_http(l1_rpc.parse()?);
    let filter = Filter::new()
        .address(dep.eez_address)
        .event_signature(IEEZ::BatchPosted::SIGNATURE_HASH)
        .from_block(dep.deploy_block);
    let log = provider
        .get_logs(&filter)
        .await?
        .pop()
        .ok_or_else(|| anyhow!("no BatchPosted event"))?;
    let tx_hash = log
        .transaction_hash
        .ok_or_else(|| anyhow!("BatchPosted log has no transaction hash"))?;
    let receipt = provider
        .get_transaction_receipt(tx_hash)
        .await?
        .ok_or_else(|| anyhow!("postAndVerifyBatch receipt is missing"))?;
    if !receipt.status() {
        bail!("postAndVerifyBatch transaction reverted");
    }
    let transaction = provider
        .get_transaction_by_hash(tx_hash)
        .await?
        .ok_or_else(|| anyhow!("postAndVerifyBatch transaction is missing"))?;
    let call = eez_protocol::abi::postAndVerifyBatchCall::abi_decode(transaction.inner.input())?;
    let proof = call
        .batch
        .proofs
        .first()
        .ok_or_else(|| anyhow!("posted batch has no proof"))?;
    let public_inputs_hash = eez_protocol::public_inputs::public_inputs_hashes(
        &call.batch,
        expected_attester.into_word(),
    )?
    .into_iter()
    .next()
    .ok_or_else(|| anyhow!("posted batch has no public-input hash"))?;
    let signature = Signature::try_from(proof.as_ref()).context("decode posted signature")?;
    let recovered = signature
        .recover_address_from_prehash(&public_inputs_hash)
        .context("recover posted signature")?;
    if recovered != expected_attester {
        bail!("posted proof recovered {recovered}, expected {expected_attester}");
    }
    Ok(())
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

/// Waits for a non-genesis safe state that appears anywhere in L1 execution history.
/// Using the full history avoids racing an advancing on-chain head.
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

/// Waits for a safe block whose state was newly attested after `previous_states`.
/// The returned number and hash distinguish the exact block when state roots repeat.
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

/// Waits until the safe chain contains the exact block hash at `number`.
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

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope, TxLegacy};
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
        function value() external view returns (uint256);
        function setValue(uint256 v) external returns (bool changed, uint256 newValue);
    }
    #[sol(rpc)]
    interface IValueNoRet {
        function value() external view returns (uint256);
        function setValue(uint256 v) external;
    }
    interface ISetterWrapper {
        function setViaProxy(uint256 v) external;
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
    // Cross-chain fronts refuse submissions until the node has reconciled with
    // L1; that is a transient boot state, so wait it out rather than failing.
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    loop {
        match provider.send_raw_transaction(&env.encoded_2718()).await {
            Ok(_) => return Ok(hash),
            Err(err)
                if err.to_string().contains("starting up")
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
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

/// Deploy EEZ + ECDSAProofSystem + Rollup on the embedded dev L1 and
/// register the rollup. Contract addresses are deterministic.
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

    let proof_system_address = deploy_raw(
        l1_rpc,
        key,
        DEV_CHAIN_ID,
        &out.join("ECDSAProofSystem.sol/ECDSAProofSystem.json"),
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
            vec![proof_system_address],
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
        proof_system_address,
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

pub async fn receipt_ok(rpc_url: &str, hash: alloy_primitives::TxHash) -> Result<Option<bool>> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(provider
        .get_transaction_receipt(hash)
        .await?
        .map(|r| r.status()))
}

/// Ports, addresses, keys, and genesis files for the cross-chain fixture.
pub struct CrossChainConfig {
    pub l1_http_port: u16,
    pub l1_auth_port: u16,
    pub l1_p2p_port: u16,
    pub l1_xchain_port: u16,
    pub l2_xchain_port: u16,
    pub eez_address: Address,
    pub proof_system_address: Address,
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
    fn new() -> Result<Self> {
        let deployer_key = ANVIL_KEY_1;
        let poster_key = ANVIL_KEY;
        let deployer = signer_of(deployer_key)?.address();
        // These CREATE nonces must match `deploy_protocol_dev`.
        let eez_address = deployer.create(0);
        let proof_system_address = deployer.create(1);
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
            proof_system_address,
            rollup_manager_address,
            rollup_id: FIRST_ROLLUP_ID,
            initial_state,
            deployer_key,
            poster_key,
            l1_genesis: write_l1_dev_genesis_at(ts)?,
            l2_genesis: write_l2_genesis_at(ts)?,
        })
    }

    fn l1_rpc_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.l1_http_port)
    }

    fn l1_xchain_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.l1_xchain_port)
    }

    fn l2_xchain_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.l2_xchain_port)
    }

    fn env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("EEZ_L1_EMBEDDED", "1".to_string()),
            ("EEZ_L1_CHAIN", "testing".to_string()),
            ("EEZ_L1_CHAIN_ID", DEV_CHAIN_ID.to_string()),
            ("EEZ_L1_RPC_URL", self.l1_rpc_url()),
            ("EEZ_L1_TARGET_RPC_URL", self.l1_rpc_url()),
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
            ("EEZ_L2_SYSTEM_KEY", L2_SYSTEM_KEY.to_string()),
            ("EEZL2_ADDRESS", format!("{EEZL2_ADDRESS:#x}")),
            ("EEZ_L1_BLOCK_TIME_MS", "5000".to_string()),
            ("EEZ_L2_BLOCK_TIME_MS", "1000".to_string()),
            ("EEZ_PROOF_TIME_MS", "1000".to_string()),
            ("EEZ_SUBMISSION_SLACK_MS", "100".to_string()),
            ("EEZ_REGISTRY_ADDRESS", format!("{:#x}", self.eez_address)),
            ("EEZ_REGISTRY_DEPLOY_BLOCK", "0".to_string()),
            (
                "EEZ_ECDSA_PROOF_SYSTEM_ADDRESS",
                format!("{:#x}", self.proof_system_address),
            ),
            (
                "EEZ_ROLLUP_MANAGER_ADDRESS",
                format!("{:#x}", self.rollup_manager_address),
            ),
            ("EEZ_ROLLUP_ID", self.rollup_id.to_string()),
            ("EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES", "false".to_string()),
            (
                "EEZ_L2_DATADIR",
                "/tmp/unused-overridden-by-flag".to_string(),
            ),
            (
                "RUST_LOG",
                std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| {
                    "warn,eez_node=info,eez_l1=info,eez_composer=info,eez_prover_client=info"
                        .to_string()
                }),
            ),
            (
                TEST_L2_GENESIS_ENV,
                self.l2_genesis.0.to_string_lossy().into_owned(),
            ),
        ]
    }
}

pub const SETUP_TIMEOUT: Duration = Duration::from_secs(90);
pub const SETTLE_TIMEOUT: Duration = Duration::from_mins(5);

/// Separate deployer keeps CREATE addresses deterministic.
pub const TARGET_DEPLOYER: &str = ANVIL_KEY_3;
pub const INBOUND_USER: &str = ANVIL_KEY_2;
pub const OUTBOUND_USER: &str = ANVIL_KEY_4;

/// Shared cross-chain integration fixture and its spawned processes.
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
    /// Present only in signer-mutation tests.
    pub prover_proxy: Option<ProverProxyHandle>,
    pub proof_signer: ProofSignerHandle,
    _witness_dir: tempfile::TempDir,
    _datadir: CrossChainDatadir,
}

enum CrossChainDatadir {
    Ephemeral(tempfile::TempDir),
    Retained(PathBuf),
}

impl CrossChainDatadir {
    fn new() -> Result<Self> {
        let Ok(root) = std::env::var("EEZ_TEST_DATADIR_DIR") else {
            return Ok(Self::Ephemeral(tempfile::tempdir()?));
        };
        let suffix = LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            PathBuf::from(root).join(format!("eez-cross-chain-{}-{suffix}", std::process::id()));
        std::fs::create_dir_all(&path)
            .with_context(|| format!("create retained cross-chain datadir {}", path.display()))?;
        Ok(Self::Retained(path))
    }

    fn path(&self) -> &std::path::Path {
        match self {
            Self::Ephemeral(dir) => dir.path(),
            Self::Retained(path) => path,
        }
    }
}

impl CrossChainWorld {
    /// Collects node and signer state for settlement timeout failures.
    pub fn settlement_diagnostics(&self) -> String {
        let node_exit = self.node.exit_status();
        let signer_exit = self.proof_signer.exit_status();
        let proxy = self.prover_proxy.as_ref().map_or_else(
            || "none (composer dials the signer directly)".to_owned(),
            |proxy| {
                format!(
                    "{} attempts, {} attested, {} rejected",
                    proxy.attempts(),
                    proxy.successes(),
                    proxy.rejections(),
                )
            },
        );
        format!(
            "\n== settlement diagnostics ==\n\
             node exit: {node_exit:?} (None = still running)\n\
             signer exit: {signer_exit:?} (None = still running)\n\
             prover proxy: {proxy}\n\
             \n== node log, settlement lines ==\n{}\n\
             \n== node log, tail ==\n{}\n\
             \n== proof-signer log, tail ==\n{}\n",
            self.node.log_lines_matching(SETTLEMENT_LOG_MARKERS, 60),
            self.node.log_tail(40),
            self.proof_signer.log_tail(60),
        )
    }

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

/// Start the shared cross-chain fixture.
pub async fn setup_cross_chain() -> Result<CrossChainWorld> {
    setup_cross_chain_inner(None, None).await
}

/// Starts the fixture with a mutation proxy and expected attester override.
pub async fn setup_cross_chain_proxied(
    mutation: ProverMutation,
    attester: Address,
) -> Result<CrossChainWorld> {
    setup_cross_chain_inner(Some(mutation), Some(attester)).await
}

async fn setup_cross_chain_inner(
    mutation: Option<ProverMutation>,
    attester_override: Option<Address>,
) -> Result<CrossChainWorld> {
    let cfg = CrossChainConfig::new()?;
    let signer_attester = signer_address(cfg.deployer_key)?;
    let proof_signer = ProofSignerHandle::spawn(&ProofSignerConfig {
        chain_config: &cfg.l2_genesis.0,
        rollup_id: cfg.rollup_id,
        signer_key: cfg.deployer_key,
        vkey: signer_address(cfg.deployer_key)?.into_word(),
        proof_system: cfg.proof_system_address,
    })
    .await?;
    let prover_proxy = match mutation {
        Some(mutation) => {
            Some(ProverProxyHandle::spawn(proof_signer.endpoint().to_owned(), mutation).await?)
        }
        None => None,
    };
    let prover_url = prover_proxy
        .as_ref()
        .map_or(proof_signer.endpoint(), ProverProxyHandle::endpoint);
    let attester = attester_override.unwrap_or(signer_attester);
    let witness_dir = tempfile::tempdir().context("witness DB tempdir")?;
    let datadir = CrossChainDatadir::new()?;
    let mut env = cfg.env();
    env.extend([
        ("EEZ_PROVER_URL", prover_url.to_string()),
        ("EEZ_ATTESTER_ADDRESS", format!("{attester:#x}")),
        (
            "EEZ_WITNESS_DB_PATH",
            witness_dir.path().to_string_lossy().into_owned(),
        ),
    ]);
    assert!(
        !env.iter().any(|(name, _)| *name == "EEZ_PROOF_SIGNER_KEY"),
        "remote composer environment must not contain the proof-signer key",
    );
    let node = NodeHandle::spawn(datadir.path(), &env)?;
    let l1_rpc = cfg.l1_rpc_url();
    let l2_rpc = node.l2_rpc_url();

    let recipient: Address = address!("0x2222222222222222222222222222222222222222");
    let withdrawal_recipient: Address = address!("0x3333333333333333333333333333333333333333");

    node.wait_for_rpc(&l1_rpc, SETUP_TIMEOUT, "embedded L1 RPC")
        .await?;
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

    node.wait_for_rpc(&l2_rpc, SETUP_TIMEOUT, "L2 RPC").await?;
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
        prover_proxy,
        proof_signer,
        _witness_dir: witness_dir,
        _datadir: datadir,
    })
}
