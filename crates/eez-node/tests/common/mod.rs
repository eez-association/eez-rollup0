//! Anvil-driven e2e harness: spawn L1, deploy protocol, spawn eez-node.
//!
//! Each test owns its own anvil port + datadir; harness drops kill both.

#![allow(dead_code)]

use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;
use anyhow::{Context, Result, anyhow, bail};

/// Anvil's first default account (mnemonic `test test test test test test test test test test test junk`).
pub const ANVIL_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const ANVIL_ADDR: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
/// Anvil's second default account — used for tests that need a key
/// distinct from the deployer / authorized signer.
pub const ANVIL_KEY_1: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

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

impl Anvil {
    pub async fn spawn(port: u16) -> Result<Self> {
        let child = Command::new(anvil_bin())
            .args(["--port", &port.to_string(), "--block-time", "1", "--silent"])
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
    /// Used to simulate L1 outage.
    pub fn pause(&self) -> Result<()> {
        signal(self.child.id(), "STOP")
    }

    /// SIGCONT — resumes the anvil process.
    pub fn resume(&self) -> Result<()> {
        signal(self.child.id(), "CONT")
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
        function rollups(uint256 rollupId) external view returns (address rollupContract, bytes32 stateRoot, uint256 etherBalance);
    }
}

/// Run the 4-step deploy via forge scripts. Inlined here rather than
/// shelling to `scripts/deploy.sh` so each test gets isolated state
/// (the bash script writes a shared `deployments.env`).
pub fn deploy_contracts(rpc_url: &str, key: &str) -> Result<Deployment> {
    let signer_addr = derive_address(key)?;
    let contracts_dir = repo_root().join("contracts");

    let eez_address = forge_extract(
        &contracts_dir,
        &["script/DeployEEZ.s.sol:DeployEEZ"],
        rpc_url,
        key,
        "EEZ",
    )?;
    let deploy_block = current_block(rpc_url)?;

    let mock_ps_address = forge_extract(
        &contracts_dir,
        &[
            "script/DeployMockECDSAProofSystem.s.sol:DeployMockECDSAProofSystem",
            "--sig",
            "run(address)",
            &format!("{signer_addr:#x}"),
        ],
        rpc_url,
        key,
        "MOCK_PS",
    )?;

    let rollup_manager_address = forge_extract(
        &contracts_dir,
        &[
            "script/DeployRollup.s.sol:DeployRollup",
            "--sig",
            "run(address,address,address,address)",
            &format!("{eez_address:#x}"),
            &format!("{mock_ps_address:#x}"),
            &format!("{signer_addr:#x}"),
            &format!("{signer_addr:#x}"),
        ],
        rpc_url,
        key,
        "ROLLUP_CONTRACT",
    )?;

    let initial_state = B256::ZERO;
    let rollup_id = forge_extract_uint(
        &contracts_dir,
        &[
            "script/RegisterRollup.s.sol:RegisterRollup",
            "--sig",
            "run(address,address,bytes32)",
            &format!("{eez_address:#x}"),
            &format!("{rollup_manager_address:#x}"),
            &format!("{initial_state:#x}"),
        ],
        rpc_url,
        key,
        "L2_ROLLUP_ID",
    )?;

    Ok(Deployment {
        eez_address,
        deploy_block,
        mock_ps_address,
        rollup_manager_address,
        rollup_id,
    })
}

fn forge_extract(
    cwd: &std::path::Path,
    args: &[&str],
    rpc_url: &str,
    key: &str,
    label: &str,
) -> Result<Address> {
    let out = forge_run(cwd, args, rpc_url, key)?;
    let needle = format!("{label}=");
    let raw = out
        .lines()
        .filter_map(|l| l.find(&needle).map(|i| &l[i + needle.len()..]))
        .filter_map(|s| s.split_whitespace().next())
        .next_back()
        .ok_or_else(|| anyhow!("forge: {label} not found in output:\n{out}"))?;
    raw.parse::<Address>()
        .with_context(|| format!("parse {label} = {raw}"))
}

fn forge_extract_uint(
    cwd: &std::path::Path,
    args: &[&str],
    rpc_url: &str,
    key: &str,
    label: &str,
) -> Result<u64> {
    let out = forge_run(cwd, args, rpc_url, key)?;
    let needle = format!("{label}=");
    let raw = out
        .lines()
        .filter_map(|l| l.find(&needle).map(|i| &l[i + needle.len()..]))
        .filter_map(|s| s.split_whitespace().next())
        .next_back()
        .ok_or_else(|| anyhow!("forge: {label} not found in output:\n{out}"))?;
    raw.parse::<u64>()
        .with_context(|| format!("parse {label} = {raw}"))
}

fn forge_run(cwd: &std::path::Path, args: &[&str], rpc_url: &str, key: &str) -> Result<String> {
    let mut cmd = Command::new("forge");
    cmd.current_dir(cwd).arg("script").args(args).args([
        "--rpc-url",
        rpc_url,
        "--private-key",
        key,
        "--broadcast",
    ]);
    let out = cmd.output().context("spawn forge")?;
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    if !out.status.success() {
        bail!("forge failed: {combined}");
    }
    Ok(combined)
}

fn derive_address(key: &str) -> Result<Address> {
    let out = Command::new("cast")
        .args(["wallet", "address", "--private-key", key])
        .output()
        .context("spawn cast")?;
    if !out.status.success() {
        bail!("cast failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse::<Address>()
        .context("parse cast address")
}

fn current_block(rpc_url: &str) -> Result<u64> {
    let out = Command::new("cast")
        .args(["block-number", "--rpc-url", rpc_url])
        .output()
        .context("spawn cast block-number")?;
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .context("parse block number")
}

pub struct NodeHandle {
    child: Child,
    pub log_path: Option<PathBuf>,
}

impl NodeHandle {
    /// Spawn `eez-node` against `datadir` with the given env. Caller owns
    /// the datadir (e.g. a `tempfile::TempDir`) so kill+respawn tests can
    /// share state across handles.
    pub fn spawn(datadir: &std::path::Path, env: &[(&'static str, String)]) -> Result<Self> {
        let log_path = std::env::var("EEZ_TEST_LOG_DIR")
            .ok()
            .map(|d| std::path::PathBuf::from(d).join(format!("eez-node-{}.log", std::process::id())));
        let (stdout, stderr) = match &log_path {
            Some(p) => {
                // tracing_subscriber's default writer is stdout; reth's panics go to stderr.
                // Merge both into one log file.
                let f = std::fs::File::create(p).context("create log file")?;
                let f2 = f.try_clone().context("clone log file")?;
                (Stdio::from(f), Stdio::from(f2))
            }
            None => (Stdio::null(), Stdio::null()),
        };
        // Reth defaults collide if any test or unrelated process holds them.
        // Each NodeHandle picks its own ephemeral ports for authrpc / http / ws / p2p.
        let authrpc_port = free_port();
        let http_port = free_port();
        let ws_port = free_port();
        let p2p_port = free_port();
        let mut cmd = Command::new("cargo");
        cmd.current_dir(repo_root())
            .args([
                "run",
                "--quiet",
                "--release",
                "-p",
                "eez-node",
                "--",
                "node",
                "--chain",
                "dev",
                "--datadir",
            ])
            .arg(datadir)
            .stdout(stdout)
            .args([
                "--authrpc.port",
                &authrpc_port.to_string(),
                "--http.port",
                &http_port.to_string(),
                "--ws.port",
                &ws_port.to_string(),
                "--port",
                &p2p_port.to_string(),
                "--disable-discovery",
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
        for (k, v) in env {
            cmd.env(*k, v);
        }
        let child = cmd.spawn().context("spawn eez-node")?;
        Ok(Self { child, log_path })
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

pub async fn state_root(rpc_url: &str, eez: Address, rollup_id: u64) -> Result<B256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let registry = IEEZ::new(eez, &provider);
    let r = registry.rollups(U256::from(rollup_id)).call().await?;
    Ok(r.stateRoot)
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
