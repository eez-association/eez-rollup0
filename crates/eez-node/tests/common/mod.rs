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
/// test starts with `let bench = Bench::fresh().await`; eliminates the
/// 4-line preamble duplicated across tests.
pub struct Bench {
    pub anvil: Anvil,
    pub stub: BundleStub,
    pub dep: Deployment,
}

impl Bench {
    pub async fn fresh() -> Result<Self> {
        let anvil = Anvil::spawn(free_port()).await?;
        let stub = BundleStub::spawn(free_port(), &anvil.rpc_url).await?;
        let dep = deploy_contracts(&anvil.rpc_url, ANVIL_KEY).await?;
        Ok(Self { anvil, stub, dep })
    }

    pub fn chain(&self) -> Chain<'_> {
        Chain::new(&self.anvil, &self.dep)
    }

    /// Default env for spawning `eez-node` against this bench.
    pub fn env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("EEZ_L1_RPC_URL", self.anvil.rpc_url.clone()),
            ("EEZ_L1_BUILDER_RPC_URL", self.stub.url.clone()),
            ("EEZ_L1_POSTER_KEY", ANVIL_KEY.to_string()),
            ("EEZ_PROOF_SIGNER_KEY", ANVIL_KEY.to_string()),
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
            ("EEZ_ROLLUP_ID", self.dep.rollup_id.to_string()),
            ("EEZ_COMPOSER_INTERVAL_SECS", "1".to_string()),
            (
                "EEZ_L2_DATADIR",
                "/tmp/unused-overridden-by-flag".to_string(),
            ),
            (
                "RUST_LOG",
                std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string()),
            ),
        ]
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

/// Reth's `--chain dev` genesis state root. Used as the `initialState`
/// when registering the rollup so the very first batch's prestate
/// (`l2_state_root(0)`) matches the on-chain `rollups[rid].stateRoot`.
/// With the default `B256::ZERO`, every batch's `_applyStateDeltas`
/// reverts with `StateRootMismatch`, caught by the try/catch,
/// emitting `ImmediateEntrySkipped` instead of `L2ExecutionPerformed`.
pub fn dev_genesis_state_root() -> B256 {
    reth_chainspec::DEV.genesis_header().state_root
}

/// Deploy `EEZ` + `MockECDSAProofSystem` + `Rollup`, then register the rollup.
/// Pure alloy — reads compiled foundry artifacts and sends each deploy
/// as an in-process tx. Mirrors `sync-rollups-composer`'s
/// `tests/e2e_anvil.rs` pattern (and that of every other Rust rollup
/// codebase surveyed). Prereq: `forge build` must have run in
/// `contracts/`.
pub async fn deploy_contracts(rpc_url: &str, key: &str) -> Result<Deployment> {
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
        initialState: dev_genesis_state_root(),
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
    child: Child,
    pub log_path: Option<PathBuf>,
}

impl NodeHandle {
    /// Spawn `eez-node` against `datadir` with the given env. Caller owns
    /// the datadir (e.g. a `tempfile::TempDir`) so kill+respawn tests can
    /// share state across handles. Uses the test-built binary path
    /// (`CARGO_BIN_EXE_eez-node`) directly — skips the `cargo run`
    /// metadata-resolution overhead per spawn.
    pub fn spawn(datadir: &std::path::Path, env: &[(&'static str, String)]) -> Result<Self> {
        let log_path = std::env::var("EEZ_TEST_LOG_DIR").ok().map(|d| {
            std::path::PathBuf::from(d).join(format!("eez-node-{}.log", std::process::id()))
        });
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
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_eez-node"));
        cmd.current_dir(repo_root())
            .args(["node", "--chain", "dev", "--datadir"])
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
/// sleeps — at anvil's 1s block time, target = current + N is ~N
/// seconds of real time but tied to L1 progress.
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
