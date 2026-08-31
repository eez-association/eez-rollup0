//! Reusable Anvil-driven EEZ integration framework.
//!
//! The crate intentionally lives as a dev-dependency of protocol components:
//! it owns process lifecycle, deterministic chain fixtures, transaction helpers,
//! structured node signals, and the declarative scenario runner.

#![allow(missing_debug_implementations)]

mod artifacts;
mod ports;
pub mod signals;

use std::{
    collections::HashSet,
    fmt::Write as _,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, B256, Bytes, Signature, U256, address, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{BlockNumHash, BlockNumberOrTag, TransactionReceipt, TransactionRequest};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolError, SolEvent, SolValue, sol};
use anyhow::{Context, Result, anyhow, bail};
use eez_control_rpc::{
    MAX_MESSAGE_BYTES, encode_prove_failure,
    v1::{
        InboundFailure, ProveChunk, ProveFailure, ProveResponse, prove_chunk, prove_failure,
        prover_client::ProverClient,
        prover_server::{Prover, ProverServer},
    },
};
use eez_protocol::EEZL2_ADDRESS;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};

pub use crate::signals::NodeSignal;
use crate::{artifacts::FailureDatadir, ports::PortLease};

/// Anvil's first default account (mnemonic `test test test test test test test test test test test junk`).
pub const ANVIL_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const ANVIL_ADDR: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
pub const ANVIL_KEY_1: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
pub const ANVIL_KEY_2: &str = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a";
pub const ANVIL_KEY_3: &str = "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6";
pub const ANVIL_KEY_4: &str = "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a";
/// Second cross-chain sender. Eviction cascades along one sender's nonce chain,
/// so a poison tx needs its own sender to leave co-bundled survivors alone.
pub const ANVIL_KEY_6: &str = "0x92db14e403b83dfe3df233f83dfa3a0d7096f21ca9b0d6d6b8d88b2b4ec1564e";
/// Unfunded proof-signer identity, separate from every transaction sender.
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
static WORKSPACE_BUILD_LOCK: Mutex<()> = Mutex::new(());

pub const FATAL_SIGNALS: &[&str] = signals::FATAL;

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

pub struct Anvil {
    child: Child,
    pub rpc_url: String,
    log_path: PathBuf,
    _log_dir: Option<tempfile::TempDir>,
}

struct AnvilConfig {
    block_time_secs: u64,
    gas_limit: u64,
    genesis_timestamp: u64,
}

impl AnvilConfig {
    fn standard(genesis_timestamp: u64) -> Self {
        Self {
            block_time_secs: L1_BLOCK_TIME_SECS,
            gas_limit: 30_000_000,
            genesis_timestamp,
        }
    }
}

impl Anvil {
    async fn spawn_with(port_lease: PortLease, cfg: AnvilConfig) -> Result<Self> {
        let port = port_lease.port();
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
        cmd.args(["--gas-limit", &cfg.gas_limit.to_string()]);
        cmd.args(["--timestamp", &cfg.genesis_timestamp.to_string()]);
        cmd.stdout(Stdio::from(log)).stderr(Stdio::from(err_log));
        drop(port_lease);
        let mut child = cmd.spawn().context("spawn anvil")?;
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
    async fn spawn(port_lease: PortLease, upstream: &str) -> Result<Self> {
        let port = port_lease.port();
        let script = repo_root().join("scripts/builder-stub.py");
        let listen = format!("127.0.0.1:{port}");
        let (log_path, log_dir) = test_log_destination("builder-stub")?;
        let log = std::fs::File::create(&log_path).context("create builder stub log")?;
        let err_log = log.try_clone().context("clone builder stub log")?;
        let mut command = Command::new("python3");
        command
            .arg(script)
            .args(["--listen", &listen, "--upstream", upstream])
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err_log));
        drop(port_lease);
        let mut child = command.spawn().context("spawn builder-stub.py")?;
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
        let port_lease = PortLease::tcp();
        let listen = format!("127.0.0.1:{}", port_lease.port());
        let attester = signer_address(cfg.signer_key)?;
        let l2_system_address = signer_address(L2_SYSTEM_KEY)?;
        let (log_path, log_dir) = test_log_destination("eez-proof-signer")?;
        let working_dir = tempfile::tempdir().context("proof signer working directory")?;
        let log = std::fs::File::create(&log_path).context("create proof signer log")?;
        let err_log = log.try_clone().context("clone proof signer log")?;
        let mut command = Command::new(proof_signer_binary()?);
        command
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
            .stderr(Stdio::from(err_log));
        drop(port_lease);
        let mut child = command.spawn().context("spawn eez-proof-signer")?;

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
            .filter_map(signals::parse)
            .filter(|signal| signal.name == "eez.proof_signer.window_signed")
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
    /// Deterministic opaque rejection used to exercise Composer recovery. This
    /// is the status returned by the real signer when its checkpoint quota is
    /// exceeded.
    ResourceExhausted,
    /// A typed rejection whose candidate identity cannot belong to the current
    /// request. The Composer must treat it as opaque rather than acting on it.
    MismatchedActionable,
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
            Self::ResourceExhausted | Self::MismatchedActionable => {
                let calldata = chunks
                    .iter()
                    .find_map(|chunk| match chunk.kind.as_ref() {
                        Some(prove_chunk::Kind::Header(header)) => header
                            .post_batch
                            .as_ref()
                            .map(|post_batch| post_batch.abi_calldata.as_slice()),
                        _ => None,
                    })
                    .ok_or_else(|| Status::internal("missing Prove header"))?;
                let batch = eez_protocol::entries::decode_postbatch(calldata)
                    .map_err(|error| Status::internal(format!("decode PostBatch: {error}")))?;
                // Entry zero is the state-chain anchor. The real checkpoint
                // quota is exercised only by effect candidates, so leave
                // anchor-only historical/minimal proofs healthy.
                if batch.entries.len() > 1 {
                    return match self {
                        Self::ResourceExhausted => Err(Status::resource_exhausted(
                            "window validation checkpoint quota exceeded",
                        )),
                        Self::MismatchedActionable => {
                            let failure = ProveFailure {
                                actionable_failure: Some(
                                    prove_failure::ActionableFailure::Inbound(InboundFailure {
                                        entry_index: u32::MAX,
                                        entry_hash: vec![0xff; 32],
                                    }),
                                ),
                            };
                            Err(Status::with_details(
                                tonic::Code::FailedPrecondition,
                                "candidate identity does not match the request",
                                encode_prove_failure(&failure).into(),
                            ))
                        }
                        Self::None | Self::PostBatch | Self::Witness => unreachable!(),
                    };
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
        if let Err(status) = self.mutation.apply(&mut chunks) {
            if matches!(
                status.code(),
                tonic::Code::ResourceExhausted | tonic::Code::FailedPrecondition
            ) {
                self.counters.rejections.fetch_add(1, Ordering::Relaxed);
            }
            return Err(status);
        }

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
                    tonic::Code::InvalidArgument
                        | tonic::Code::FailedPrecondition
                        | tonic::Code::ResourceExhausted
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
        let l2_genesis = write_l2_genesis_at(ts)?;
        let cfg = AnvilConfig::standard(ts);
        Self::with_anvil_config(cfg, l2_genesis_state_root(), l2_genesis).await
    }

    async fn with_anvil_config(
        cfg: AnvilConfig,
        initial_state: B256,
        l2_genesis: (PathBuf, tempfile::TempDir),
    ) -> Result<Self> {
        let anvil = Anvil::spawn_with(PortLease::tcp(), cfg).await?;
        let stub = BundleStub::spawn(PortLease::tcp(), &anvil.rpc_url).await?;
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
            (
                "EEZ_L1_BLOCK_TIME_MS",
                (L1_BLOCK_TIME_SECS * 1000).to_string(),
            ),
            ("EEZ_L2_BLOCK_TIME_MS", "2000".to_string()),
            ("EEZ_PROOF_TIME_MS", "1000".to_string()),
            ("EEZ_SUBMISSION_SLACK_MS", "100".to_string()),
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

/// Wall-clock seconds for stamping test genesis + anvil, so the sequencer's
/// defer-on-lateness gate doesn't read every trigger as late.
fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}

/// The shared L2 genesis fixture used by every harness mode.
pub fn l2_genesis_fixture_path() -> PathBuf {
    repo_root().join("crates/eez-node/tests/fixtures/genesis.json")
}

fn fixture_genesis() -> Result<alloy_genesis::Genesis> {
    let raw = std::fs::read_to_string(l2_genesis_fixture_path()).context("read genesis.json")?;
    serde_json::from_str(&raw).context("parse genesis.json")
}

/// State root registered on L1 for the shared L2 genesis fixture.
pub fn l2_genesis_state_root() -> B256 {
    static ROOT: LazyLock<B256> = LazyLock::new(|| {
        let spec: reth_chainspec::ChainSpec =
            fixture_genesis().expect("read L2 genesis fixture").into();
        spec.genesis_header().state_root
    });
    *ROOT
}

/// Sign and submit one EIP-1559 L2 value transfer using the pool nonce.
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
    let nonce = provider.get_transaction_count(from).pending().await?;
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

/// Return the latest safe block's state root, or `None` before one exists.
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

    // Keep the authorized attester separate from the poster/deployer key.
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
    child: Mutex<Child>,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Human label for assertion messages ("c1", "c2", or the default "node").
    pub name: String,
    /// Where the node's merged stdout+stderr is written. Goes to
    /// `EEZ_TEST_LOG_DIR/eez-node-<pid>.log` if that env var is set,
    /// otherwise inside a tempdir held by this handle.
    pub log_path: PathBuf,
    /// Tempdirs whose lifetime is tied to this handle (currently the log dir).
    _keep_alive: Vec<tempfile::TempDir>,
    /// Owned node database. CI retains it only if this test unwinds in failure.
    owned_datadir: Option<FailureDatadir>,
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
    const fn name(self) -> &'static str {
        match self {
            Self::Composer => "eez-composer",
            Self::Follower => "eez-follower",
            Self::Dev => "eez-dev-node",
        }
    }

    fn path(self) -> Result<PathBuf> {
        let name = self.name();
        if let Some(path) = std::env::var_os(format!("CARGO_BIN_EXE_{name}")) {
            return Ok(path.into());
        }

        // `eez-testkit` is compiled as a library, where Cargo does not expose
        // `CARGO_BIN_EXE_*` to `env!`. At test runtime the executable sits in
        // `<target>/<profile>/deps`, alongside the role binaries' parent dir.
        let current = std::env::current_exe().context("current test executable")?;
        let target_profile = current
            .parent()
            .and_then(std::path::Path::parent)
            .ok_or_else(|| anyhow!("test executable has no target profile directory"))?;
        let path = target_profile.join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
        if path.is_file() {
            return Ok(path);
        }

        bail!(
            "{name} binary not found next to the test profile at {}; build the eez-node package binaries before running the harness",
            target_profile.display(),
        )
    }
}

#[derive(Default)]
pub struct NodeConfig<'a> {
    /// Explicit node-role executable to launch.
    pub binary: NodeBinary,
    pub genesis_path: Option<&'a std::path::Path>,
}

impl NodeHandle {
    fn spawn_with(
        name: &str,
        datadir: &std::path::Path,
        cfg: &NodeConfig<'_>,
        env: &[(&'static str, String)],
    ) -> Result<Self> {
        Self::spawn_with_reservations(name, datadir, cfg, env, Vec::new())
    }

    fn spawn_with_reservations(
        name: &str,
        datadir: &std::path::Path,
        cfg: &NodeConfig<'_>,
        env: &[(&'static str, String)],
        mut port_leases: Vec<PortLease>,
    ) -> Result<Self> {
        let (log_path, log_tempdir) = test_log_destination(&format!("eez-node-{name}"))?;
        let f = std::fs::File::create(&log_path).context("create log file")?;
        let f2 = f.try_clone().context("clone log file")?;
        let (stdout, stderr) = (Stdio::from(f), Stdio::from(f2));
        let authrpc_lease = PortLease::tcp();
        let http_lease = PortLease::tcp();
        let ws_lease = PortLease::tcp();
        let p2p_lease = PortLease::tcp();
        let l1_http_lease = PortLease::http_pair();
        let l1_auth_lease = PortLease::tcp();
        // Embedded L1 uses this numeric port for RLPx TCP and discovery UDP.
        let l1_p2p_lease = PortLease::tcp_udp();
        let l1_discv5_lease = PortLease::udp();
        let l1_xchain_lease = PortLease::tcp();
        let l2_xchain_lease = PortLease::tcp();
        let authrpc_port = authrpc_lease.port();
        let http_port = http_lease.port();
        let ws_port = ws_lease.port();
        let p2p_port = p2p_lease.port();
        let l1_http_port = l1_http_lease.port();
        let l1_auth_port = l1_auth_lease.port();
        let l1_p2p_port = l1_p2p_lease.port();
        let l1_discv5_port = l1_discv5_lease.port();
        let l1_xchain_port = l1_xchain_lease.port();
        let l2_xchain_port = l2_xchain_lease.port();
        port_leases.extend([
            authrpc_lease,
            http_lease,
            ws_lease,
            p2p_lease,
            l1_http_lease,
            l1_auth_lease,
            l1_p2p_lease,
            l1_discv5_lease,
            l1_xchain_lease,
            l2_xchain_lease,
        ]);
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
        let mut cmd = Command::new(cfg.binary.path()?);
        let signal_filter = std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| {
            "warn,eez_node=info,eez_l1=info,eez_composer=info,eez_deriver=info".to_string()
        });
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
        // Keep every selected port bound until the child command is complete.
        // The subprocess bind itself cannot be atomic, but this leaves only the
        // unavoidable drop/spawn boundary exposed to another process.
        drop(port_leases);
        let child = cmd.spawn().context("spawn eez node role")?;
        Ok(Self {
            child: Mutex::new(child),
            background_tasks: Mutex::new(Vec::new()),
            name: name.to_string(),
            log_path,
            _keep_alive: log_tempdir.into_iter().collect(),
            owned_datadir: None,
            http_port,
        })
    }

    pub async fn start(
        name: &str,
        cfg: &NodeConfig<'_>,
        env: &[(&'static str, String)],
    ) -> Result<Self> {
        let mut datadir = FailureDatadir::new(&format!("eez-node-{name}"))?;
        let mut handle = Self::start_with_datadir(name, datadir.path(), cfg, env).await?;
        datadir.fixture_ready();
        handle.owned_datadir = Some(datadir);
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

    pub fn assert_no_process_death(&self) {
        if let Some(status) = self.exit_status() {
            panic!(
                "{} exited unexpectedly with {status}; log:\n{}",
                self.name,
                self.log_tail(80),
            );
        }
        assert_eq!(
            self.count_signals(FATAL_SIGNALS).unwrap(),
            0,
            "{} emitted a panic, divergence, or fatal-lifecycle signal",
            self.name,
        );
        assert_eq!(
            self.log_count_matching(&["Fatal", "UnexpectedStaticFile"])
                .unwrap(),
            0,
            "{} logged a fatal-class failure",
            self.name,
        );
    }

    pub fn assert_no_divergence_failure_logs(&self) {
        self.assert_no_process_death();
        assert_eq!(
            self.log_count_matching(&[
                "engine rejected safe/finalized FCU",
                "payload builder returned no payload",
            ])
            .unwrap(),
            0,
            "{} logged a fatal-class failure",
            self.name,
        );
    }

    pub fn log_count_matching(&self, patterns: &[&str]) -> Result<usize> {
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
            .filter_map(signals::parse)
            .filter(|signal| signals.contains(&signal.name.as_str()))
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

    /// Line cursor for a subsequent [`Self::signals_since`] query.
    pub fn signal_cursor(&self) -> Result<usize> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read signal stream {}", self.log_path.display()))?;
        // Do not advance past a record whose write is still in progress.
        Ok(contents.bytes().filter(|byte| *byte == b'\n').count())
    }

    /// Structured records emitted after `cursor`. Signal values are exact
    /// tracing event names, while human-readable messages remain irrelevant.
    pub fn signals_since(&self, cursor: usize) -> Result<Vec<NodeSignal>> {
        let contents = std::fs::read_to_string(&self.log_path)
            .with_context(|| format!("read signal stream {}", self.log_path.display()))?;
        Ok(contents
            .lines()
            .skip(cursor)
            .filter_map(signals::parse)
            .collect())
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
                self.log_tail(100)
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
    let deadline = Instant::now() + timeout;
    let mut last_error: Option<anyhow::Error> = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, f()).await {
            Ok(Ok(Some(value))) => return Ok(value),
            Ok(Ok(None)) => {}
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => break,
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep(remaining.min(Duration::from_millis(500))).await;
    }
    match last_error {
        Some(error) => Err(error.context(format!("timed out after {timeout:?}; last poll failed"))),
        None => bail!("timed out after {timeout:?}"),
    }
}

/// Registry counters and roots read from one L1 block.
#[derive(Clone, Copy, Debug)]
pub struct ChainSnapshot {
    pub batches_posted: usize,
    pub executions_performed: usize,
    pub entries_skipped: usize,
    pub state_root: B256,
    pub latest_execution_state: Option<B256>,
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

    /// Pin related reads to one L1 block so a batch landing between RPC calls
    /// cannot manufacture a lockstep failure.
    pub async fn snapshot(&self) -> Result<ChainSnapshot> {
        let block = self.block_number().await?;
        Ok(ChainSnapshot {
            batches_posted: count_events_at(
                self.rpc_url,
                self.eez_address,
                IEEZ::BatchPosted::SIGNATURE_HASH,
                self.deploy_block,
                Some(block),
            )
            .await?,
            executions_performed: count_events_at(
                self.rpc_url,
                self.eez_address,
                IEEZ::L2ExecutionPerformed::SIGNATURE_HASH,
                self.deploy_block,
                Some(block),
            )
            .await?,
            entries_skipped: count_events_at(
                self.rpc_url,
                self.eez_address,
                IEEZ::L2TxSkipped::SIGNATURE_HASH,
                self.deploy_block,
                Some(block),
            )
            .await?,
            state_root: state_root_at(self.rpc_url, self.eez_address, self.rollup_id, Some(block))
                .await?,
            latest_execution_state: latest_l2_execution_state_at(
                self.rpc_url,
                self.eez_address,
                self.rollup_id,
                self.deploy_block,
                Some(block),
            )
            .await?,
        })
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
    state_root_at(rpc_url, eez, rollup_id, None).await
}

async fn state_root_at(
    rpc_url: &str,
    eez: Address,
    rollup_id: u64,
    block: Option<u64>,
) -> Result<B256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let registry = IEEZ::new(eez, &provider);
    let mut call = registry.rollups(rollup_id);
    if let Some(block) = block {
        call = call.block(BlockNumberOrTag::Number(block).into());
    }
    Ok(call.call().await?.stateRoot)
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

/// Derive the address seen as `msg.sender` on the destination chain.
pub async fn cross_chain_source_proxy(
    rpc_url: &str,
    manager: Address,
    original: Address,
    original_rollup_id: u64,
) -> Result<Address> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(IProxyDerivation::new(manager, &provider)
        .computeCrossChainProxyAddress(original, original_rollup_id)
        .call()
        .await?)
}

/// Return the revert data from an `eth_call` that is expected to fail.
pub async fn call_revert_data(
    rpc_url: &str,
    from: Address,
    to: Address,
    data: Vec<u8>,
) -> Result<Bytes> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let request = TransactionRequest::default()
        .from(from)
        .to(to)
        .input(Bytes::from(data).into());
    match provider.call(request).await {
        Ok(returned) => bail!("eth_call unexpectedly succeeded, returning {returned}"),
        Err(err) => {
            let payload = err
                .as_error_resp()
                .ok_or_else(|| anyhow!("eth_call failed without an RPC error payload: {err}"))?;
            payload
                .as_revert_data()
                .ok_or_else(|| anyhow!("eth_call failed without revert data: {err}"))
        }
    }
}

/// Return matching logs in chain order.
pub async fn events_since(
    rpc_url: &str,
    contract: Address,
    event_sig_hash: B256,
    from_block: u64,
) -> Result<Vec<alloy_rpc_types_eth::Log>> {
    use alloy_rpc_types_eth::Filter;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let filter = Filter::new()
        .address(contract)
        .event_signature(event_sig_hash)
        .from_block(from_block);
    Ok(provider.get_logs(&filter).await?)
}

/// Count matching events emitted at or after `from_block`.
pub async fn count_events(
    rpc_url: &str,
    contract: Address,
    event_sig_hash: B256,
    from_block: u64,
) -> Result<usize> {
    count_events_at(rpc_url, contract, event_sig_hash, from_block, None).await
}

async fn count_events_at(
    rpc_url: &str,
    contract: Address,
    event_sig_hash: B256,
    from_block: u64,
    to_block: Option<u64>,
) -> Result<usize> {
    use alloy_rpc_types_eth::Filter;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let mut filter = Filter::new()
        .address(contract)
        .event_signature(event_sig_hash)
        .from_block(from_block);
    if let Some(to_block) = to_block {
        filter = filter.to_block(to_block);
    }
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

async fn latest_l2_execution_state_at(
    rpc_url: &str,
    contract: Address,
    rollup_id: u64,
    from_block: u64,
    to_block: Option<u64>,
) -> Result<Option<B256>> {
    use alloy_rpc_types_eth::Filter;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let mut filter = Filter::new()
        .address(contract)
        .event_signature(IEEZ::L2ExecutionPerformed::SIGNATURE_HASH)
        .topic1(B256::from(U256::from(rollup_id)))
        .from_block(from_block);
    if let Some(to_block) = to_block {
        filter = filter.to_block(to_block);
    }
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
        event Wrapped(uint256 input, bool ok, bool changed, uint256 newValue);
        function setViaProxy(uint256 v) external;
        function setSameValueTwice(uint256 v) external;
        function completedProxyCalls() external view returns (uint256);
        function lastChanged() external view returns (bool);
        function lastNewValue() external view returns (uint256);
    }
    #[sol(rpc)]
    interface IRevertingTarget {
        error Rejected(uint256 seen);
        function calls() external view returns (uint256);
        function lastValue() external view returns (uint256);
        function revertCustom(uint256 v) external payable;
        function revertString(uint256 v) external payable;
        function writeThenRevert(uint256 v) external payable;
        function succeed(uint256 v) external payable returns (uint256);
    }
    #[sol(rpc)]
    interface IRevertBubbleWrapper {
        function callAndRecord(bytes calldata data) external;
        function failures() external view returns (uint256);
        function successes() external view returns (uint256);
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
    /// Derives the source proxy used on the destination chain.
    #[sol(rpc)]
    interface IProxyDerivation {
        function computeCrossChainProxyAddress(address originalAddress, uint64 originalRollupId) external view returns (address);
    }
    #[sol(rpc)]
    interface IEEZL2Direct {
        error UnauthorizedProxy();
        function executeCrossChainCall(address sourceAddress, bytes calldata callData) external payable returns (bytes memory);
    }
    /// Order-dependent target: each call returns state left by the previous one.
    #[sol(rpc)]
    interface ICounter {
        function count() external view returns (uint256);
        function increment() external returns (uint256 newCount);
        function add(uint256 x) external returns (uint256 newCount);
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
    let mut genesis = fixture_genesis()?;
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
    // Cross-chain fronts temporarily refuse submissions while starting.
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

/// Return the confirmed nonce. Cross-chain ingress validates against the
/// confirmed nonce plus held transactions, so using the pool's pending nonce
/// here would count held transactions twice.
pub async fn onchain_nonce(rpc_url: &str, key: &str) -> Result<u64> {
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

    let nonce = onchain_nonce(rpc_url, key).await?;
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
    deployer_key: &str,
    attester_key: &str,
    initial_state: B256,
) -> Result<Deployment> {
    let signer = signer_of(deployer_key)?;
    let signer_addr = signer.address();
    let attester = signer_address(attester_key)?;
    let out = repo_root().join("contracts/out");

    let eez_address = deploy_raw(
        l1_rpc,
        deployer_key,
        DEV_CHAIN_ID,
        &out.join("EEZ.sol/EEZ.json"),
        signer_addr.abi_encode(),
    )
    .await?;
    let provider = ProviderBuilder::new().connect_http(l1_rpc.parse()?);
    let deploy_block = provider.get_block_number().await?;

    let proof_system_address = deploy_raw(
        l1_rpc,
        deployer_key,
        DEV_CHAIN_ID,
        &out.join("ECDSAProofSystem.sol/ECDSAProofSystem.json"),
        attester.abi_encode(),
    )
    .await?;

    let vkeys: Vec<B256> = vec![B256::from_slice(&{
        let mut padded = [0u8; 32];
        padded[12..].copy_from_slice(attester.as_slice());
        padded
    })];
    let rollup_manager_address = deploy_raw(
        l1_rpc,
        deployer_key,
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
    let nonce = onchain_nonce(l1_rpc, deployer_key).await?;
    let hash = sign_and_send(
        l1_rpc,
        deployer_key,
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

async fn deploy_reverting_target(rpc_url: &str, key: &str, chain_id: u64) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("RevertingTarget.sol/RevertingTarget.json"),
        Vec::new(),
    )
    .await
}

async fn deploy_revert_bubble_wrapper(
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
        &out.join("RevertBubbleWrapper.sol/RevertBubbleWrapper.json"),
        proxy.abi_encode(),
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

/// Deploy `Counter` (no constructor args) on either chain.
pub async fn deploy_counter(rpc_url: &str, key: &str, chain_id: u64) -> Result<Address> {
    let out = repo_root().join("contracts/out");
    deploy_raw(
        rpc_url,
        key,
        chain_id,
        &out.join("Counter.sol/Counter.json"),
        Vec::new(),
    )
    .await
}

pub async fn counter_count(rpc_url: &str, counter: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(ICounter::new(counter, &provider).count().call().await?)
}

pub async fn value_no_ret(rpc_url: &str, value_addr: Address) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(IValueNoRet::new(value_addr, &provider)
        .value()
        .call()
        .await?)
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
    let nonce = onchain_nonce(l2_rpc, key).await?;
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
    let nonce = onchain_nonce(l1_rpc, key).await?;
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
    /// Unfunded identity used only by the standalone proof signer.
    pub attester_key: &'static str,
    pub poster_key: &'static str,
    pub l1_genesis: (PathBuf, tempfile::TempDir),
    pub l2_genesis: (PathBuf, tempfile::TempDir),
    port_leases: Vec<PortLease>,
}

impl CrossChainConfig {
    fn new() -> Result<Self> {
        let deployer_key = ANVIL_KEY_1;
        let attester_key = ANVIL_ATTESTER_KEY;
        let poster_key = ANVIL_KEY;
        let deployer = signer_of(deployer_key)?.address();
        // These CREATE nonces must match `deploy_protocol_dev`.
        let eez_address = deployer.create(0);
        let proof_system_address = deployer.create(1);
        let rollup_manager_address = deployer.create(2);
        let initial_state = l2_genesis_state_root();
        let ts = now_unix_secs();
        let l1_http_lease = PortLease::http_pair();
        let l1_auth_lease = PortLease::tcp();
        let l1_p2p_lease = PortLease::tcp_udp();
        let l1_xchain_lease = PortLease::tcp();
        let l2_xchain_lease = PortLease::tcp();
        let l1_http_port = l1_http_lease.port();
        let l1_auth_port = l1_auth_lease.port();
        let l1_p2p_port = l1_p2p_lease.port();
        let l1_xchain_port = l1_xchain_lease.port();
        let l2_xchain_port = l2_xchain_lease.port();
        Ok(Self {
            l1_http_port,
            l1_auth_port,
            l1_p2p_port,
            l1_xchain_port,
            l2_xchain_port,
            eez_address,
            proof_system_address,
            rollup_manager_address,
            rollup_id: FIRST_ROLLUP_ID,
            initial_state,
            deployer_key,
            attester_key,
            poster_key,
            l1_genesis: write_l1_dev_genesis_at(ts)?,
            l2_genesis: write_l2_genesis_at(ts)?,
            port_leases: vec![
                l1_http_lease,
                l1_auth_lease,
                l1_p2p_lease,
                l1_xchain_lease,
                l2_xchain_lease,
            ],
        })
    }

    fn take_port_leases(&mut self) -> Vec<PortLease> {
        std::mem::take(&mut self.port_leases)
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
            ("EEZ_L1_CHAIN", "testing".to_string()),
            ("EEZ_L1_CHAIN_ID", DEV_CHAIN_ID.to_string()),
            ("EEZ_L1_RPC_URL", self.l1_rpc_url()),
            ("EEZ_L1_BUILDER_RPC_URL", self.l1_rpc_url()),
            ("EEZ_L1_TARGET_RPC_URL", self.l1_rpc_url()),
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
pub const SETTLE_TIMEOUT: Duration = Duration::from_secs(90);
const CROSS_CHAIN_L1_BLOCK_TIME_MS: u64 = 5_000;
const CROSS_CHAIN_L2_BLOCK_TIME_MS: u64 = 1_000;
const CROSS_CHAIN_SYNC_INTERVAL: u64 = CROSS_CHAIN_L1_BLOCK_TIME_MS / CROSS_CHAIN_L2_BLOCK_TIME_MS;

/// L1's rollup ID in cross-chain identities.
pub const L1_ROLLUP_ID: u64 = 0;

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
    _datadir: FailureDatadir,
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

/// A destination that reverts, reached through a source-side wrapper. Both
/// directions are wired so one fixture covers inbound and outbound.
pub struct CrossChainRevertWorld {
    pub world: CrossChainWorld,
    /// Reverting target on L2, called by `inbound_wrapper` on L1.
    pub reverting_target_l2: Address,
    pub inbound_wrapper: Address,
    /// Reverting target on L1, called by `outbound_wrapper` on L2.
    pub reverting_target_l1: Address,
    pub outbound_wrapper: Address,
}

impl std::ops::Deref for CrossChainRevertWorld {
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

    /// Set the value bridged by this call.
    #[must_use]
    pub fn with_value(mut self, value: impl IntoTestU256) -> Self {
        self.value = value.into_test_u256();
        self
    }

    #[must_use]
    pub const fn with_gas_limit(mut self, gas_limit: u64) -> Self {
        self.gas_limit = gas_limit;
        self
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

// Represent a `bytes32` getter result as a single word.
impl IntoTestU256 for B256 {
    fn into_test_u256(self) -> U256 {
        U256::from_be_bytes(self.0)
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

/// A labeled single-word state read.
#[derive(Clone, Debug)]
pub struct StateRead {
    target: Address,
    calldata: Vec<u8>,
    label: String,
}

/// Read `Value.value()`.
pub fn value_read(contract: Address) -> StateRead {
    call_read(contract, "value()", IValue::valueCall {}.abi_encode())
}

/// Define a state read for a getter that returns one word.
pub fn call_read(target: Address, label: impl Into<String>, calldata: Vec<u8>) -> StateRead {
    StateRead {
        target,
        calldata,
        label: label.into(),
    }
}

#[derive(Clone, Copy, Debug)]
enum StateSide {
    L1,
    L2,
}

#[derive(Clone, Debug)]
struct StateExpectation {
    side: StateSide,
    read: StateRead,
    expected: U256,
}

// Execute a state read and decode its first 32-byte word.
pub async fn read_state_word(rpc_url: &str, read: &StateRead) -> Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let returned = provider
        .call(
            TransactionRequest::default()
                .to(read.target)
                .input(Bytes::from(read.calldata.clone()).into()),
        )
        .await
        .with_context(|| format!("eth_call {} on {}", read.label, read.target))?;
    let word: [u8; 32] = returned
        .get(..32)
        .and_then(|w| w.try_into().ok())
        .ok_or_else(|| {
            anyhow!(
                "{} on {} returned {} bytes; expected at least one word",
                read.label,
                read.target,
                returned.len()
            )
        })?;
    Ok(U256::from_be_bytes(word))
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
            inbound_nonce: onchain_nonce(&l1_rpc, INBOUND_USER).await?,
            outbound_nonce: onchain_nonce(&l2_rpc, OUTBOUND_USER).await?,
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
        world.node.assert_no_divergence_failure_logs();

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
        if records
            .iter()
            .any(|record| FATAL_SIGNALS.contains(&record.name.as_str()))
        {
            bail!("{scenario_name}: node emitted a divergence, fatal, or panic signal");
        }
        // The scenario runner only submits valid calls, so any eviction is a bug.
        if records.iter().any(|record| {
            matches!(
                record.name.as_str(),
                signals::TX_POISON_EVICTED | signals::TX_NONCE_CHAIN_EVICTED
            )
        }) {
            bail!("{scenario_name}: a valid scenario transaction was evicted");
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
                    settlement_points.push((
                        record.u64("applied_entries")? as usize,
                        record.b256("l1_settled_state_root")?,
                        record.b256("l2_safe_state_root")?,
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
                | signals::COMPOSER_PHASE1_BUNDLE_DISPATCHED
                | signals::DERIVER_SYNC_BLOCK_BUILT => {
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
        if execution_states.len() < self.execution_states.len() {
            bail!("{scenario_name}: L2ExecutionPerformed history retreated");
        }
        // Anchored on roots and positions inside the full history, never on the
        // baseline length. The snapshot's signal cursor and its L1 event query
        // cannot be taken atomically while the deriver runs, so a settlement can
        // land between them and be counted on one side only; its position in the
        // event sequence is immune to that. Each settlement must appear after the
        // previous one, and consecutive settlements must be exactly
        // `applied_entries` events apart — a replay or a dropped entry breaks it.
        let mut previous_index: Option<usize> = None;
        for (applied, l1_settled_root, l2_safe_root) in settlement_points {
            if l1_settled_root != l2_safe_root {
                bail!(
                    "{scenario_name}: L1 settled root {l1_settled_root} != L2 safe block root {l2_safe_root}"
                );
            }
            let search_from = previous_index.map_or(0, |previous| previous + 1);
            let index = execution_states
                .get(search_from..)
                .and_then(|tail| tail.iter().position(|root| *root == l1_settled_root))
                .map(|offset| search_from + offset)
                .ok_or_else(|| {
                    anyhow!(
                        "{scenario_name}: L1 settled root {l1_settled_root} has no L2ExecutionPerformed event after index {search_from}"
                    )
                })?;
            if let Some(previous) = previous_index
                && index - previous != applied
            {
                bail!(
                    "{scenario_name}: settlement reported {applied} applied entries but sits {} L1 events after the previous one",
                    index - previous
                );
            }
            previous_index = Some(index);
        }
        if safe.is_some() && expect_settled {
            let committed = state_root(&l1_rpc, world.cfg.eez_address, world.cfg.rollup_id).await?;
            let safe_root = safe_block_state_root(&l2_rpc)
                .await?
                .ok_or_else(|| anyhow!("{scenario_name}: safe L2 block is absent"))?;
            if committed != safe_root {
                bail!("{scenario_name}: L1 committed root != L2 safe root");
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
            "{scenario_name}: sync block {height} is off the deterministic K={CROSS_CHAIN_SYNC_INTERVAL} grid (residue {actual}, expected {expected})"
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
    // Exact, not `>=`: these actors are scenario-private, so any nonce the
    // scenario did not submit is unaccounted-for activity.
    let actual_inbound = onchain_nonce(&world.l1_rpc(), INBOUND_USER).await?;
    let actual_outbound = onchain_nonce(&world.l2_rpc(), OUTBOUND_USER).await?;
    if actual_inbound != inbound || actual_outbound != outbound {
        bail!(
            "{scenario_name}: sender nonces are {actual_inbound}/{actual_outbound}, expected {inbound}/{outbound}"
        );
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

    #[must_use]
    pub fn inbound(mut self, call: ScenarioCall) -> Self {
        self.calls.push((ScenarioDirection::Inbound, call));
        self
    }

    #[must_use]
    pub fn outbound(mut self, call: ScenarioCall) -> Self {
        self.calls.push((ScenarioDirection::Outbound, call));
        self
    }

    #[must_use]
    pub fn expect_l2_state(mut self, read: StateRead, expected: impl IntoTestU256) -> Self {
        self.states.push(StateExpectation {
            side: StateSide::L2,
            read,
            expected: expected.into_test_u256(),
        });
        self
    }

    #[must_use]
    pub fn expect_l1_state(mut self, read: StateRead, expected: impl IntoTestU256) -> Self {
        self.states.push(StateExpectation {
            side: StateSide::L1,
            read,
            expected: expected.into_test_u256(),
        });
        self
    }

    #[must_use]
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
            let nonce = onchain_nonce(receipt_rpc, key).await?;
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
            let succeeded = wait_for(SETTLE_TIMEOUT, || async {
                receipt_ok(receipt_rpc, hash).await
            })
            .await
            .with_context(|| format!("{}: {direction:?} call was not mined", self.name))?;
            if !succeeded {
                bail!("{}: {direction:?} call reverted", self.name);
            }
            actions.push(ExecutedScenarioAction {
                direction,
                value: call.value,
                nonce,
                hash,
            });
        }

        for expectation in &self.states {
            let rpc = match expectation.side {
                StateSide::L1 => l1_rpc.as_str(),
                StateSide::L2 => l2_rpc.as_str(),
            };
            let converged = wait_for(SETTLE_TIMEOUT, || async {
                Ok(
                    (read_state_word(rpc, &expectation.read).await? == expectation.expected)
                        .then_some(()),
                )
            })
            .await;
            // Include the last observed value when a state expectation times out.
            if converged.is_err() {
                let actual = read_state_word(rpc, &expectation.read).await;
                bail!(
                    "{}: {:?} {} on {} is {:?}, expected {}",
                    self.name,
                    expectation.side,
                    expectation.read.label,
                    expectation.read.target,
                    actual,
                    expectation.expected,
                );
            }
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

/// Start the shared cross-chain fixture.
pub async fn setup_cross_chain() -> Result<CrossChainWorld> {
    setup_cross_chain_inner(None, None, &[]).await
}

/// Start the fixture with additional node environment variables.
pub async fn setup_cross_chain_with_env(
    extra_env: &[(&'static str, String)],
) -> Result<CrossChainWorld> {
    setup_cross_chain_inner(None, None, extra_env).await
}

/// Starts the fixture with a mutation proxy and expected attester override.
pub async fn setup_cross_chain_proxied(
    mutation: ProverMutation,
    attester: Address,
) -> Result<CrossChainWorld> {
    setup_cross_chain_inner(Some(mutation), Some(attester), &[]).await
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

pub async fn setup_cross_chain_reverting() -> Result<CrossChainRevertWorld> {
    let world = setup_cross_chain().await?;
    let l1_rpc = world.l1_rpc();
    let l2_rpc = world.l2_rpc();

    let reverting_target_l2 =
        deploy_reverting_target(&l2_rpc, TARGET_DEPLOYER, world.l2_chain_id).await?;
    let inbound_proxy = create_cross_chain_proxy(
        &l1_rpc,
        world.cfg.deployer_key,
        world.cfg.eez_address,
        reverting_target_l2,
        world.cfg.rollup_id,
    )
    .await?;
    let inbound_wrapper =
        deploy_revert_bubble_wrapper(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID, inbound_proxy).await?;

    let reverting_target_l1 =
        deploy_reverting_target(&l1_rpc, TARGET_DEPLOYER, DEV_CHAIN_ID).await?;
    let outbound_proxy =
        create_l2_cross_chain_proxy(&l2_rpc, TARGET_DEPLOYER, reverting_target_l1, L1_ROLLUP_ID)
            .await?;
    let outbound_wrapper =
        deploy_revert_bubble_wrapper(&l2_rpc, TARGET_DEPLOYER, world.l2_chain_id, outbound_proxy)
            .await?;

    Ok(CrossChainRevertWorld {
        world,
        reverting_target_l2,
        inbound_wrapper,
        reverting_target_l1,
        outbound_wrapper,
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

async fn setup_cross_chain_inner(
    mutation: Option<ProverMutation>,
    attester_override: Option<Address>,
    extra_env: &[(&'static str, String)],
) -> Result<CrossChainWorld> {
    let mut cfg = CrossChainConfig::new()?;
    let signer_attester = signer_address(cfg.attester_key)?;
    let proof_signer = ProofSignerHandle::spawn(&ProofSignerConfig {
        chain_config: &cfg.l2_genesis.0,
        rollup_id: cfg.rollup_id,
        signer_key: cfg.attester_key,
        vkey: signer_address(cfg.attester_key)?.into_word(),
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
    let mut datadir = FailureDatadir::new("eez-cross-chain")?;
    let mut env = cfg.env();
    env.extend([
        ("EEZ_PROVER_URL", prover_url.to_string()),
        ("EEZ_ATTESTER_ADDRESS", format!("{attester:#x}")),
        (
            "EEZ_WITNESS_DB_PATH",
            witness_dir.path().to_string_lossy().into_owned(),
        ),
    ]);
    // Before the assertion below, so caller-supplied vars are checked too.
    env.extend_from_slice(extra_env);
    assert!(
        !env.iter().any(|(name, _)| *name == "EEZ_PROOF_SIGNER_KEY"),
        "remote composer environment must not contain the proof-signer key",
    );
    let node = NodeHandle::spawn_with_reservations(
        "node",
        datadir.path(),
        &NodeConfig::default(),
        &env,
        cfg.take_port_leases(),
    )?;
    let l1_rpc = cfg.l1_rpc_url();
    let l2_rpc = node.l2_rpc_url();

    let recipient: Address = address!("0x2222222222222222222222222222222222222222");
    let withdrawal_recipient: Address = address!("0x3333333333333333333333333333333333333333");

    node.wait_for_rpc(&l1_rpc, SETUP_TIMEOUT, "embedded L1 RPC")
        .await?;
    let dep = deploy_protocol_dev(
        &l1_rpc,
        cfg.deployer_key,
        cfg.attester_key,
        cfg.initial_state,
    )
    .await?;
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

    datadir.fixture_ready();
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

#[cfg(test)]
mod framework_tests {
    use super::*;

    #[test]
    fn l2_genesis_fixture_is_resolvable_after_testkit_extraction() {
        assert!(l2_genesis_fixture_path().is_file());
        let _ = l2_genesis_state_root();
    }
}
