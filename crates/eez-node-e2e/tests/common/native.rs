//! Native-type devnet support. The second Composer's L1 is a real independent
//! Reth database driven through Engine API by the first development L1's blocks.
//! This is a development consensus feed, not a PoS/MEV builder simulation.
use super::*;
use alloy_rpc_types_engine::{Claims, ExecutionPayloadV3, JwtSecret};
use anyhow::ensure;

pub struct MirrorComposer {
    pub node: NodeHandle,
    pub l2_xchain: String,
    feed: tokio::task::JoinHandle<()>,
    error: Arc<Mutex<Option<String>>>,
    _datadir: tempfile::TempDir,
    _witness: tempfile::TempDir,
    _jwt_dir: tempfile::TempDir,
}
impl MirrorComposer {
    pub fn assert_healthy(&self) {
        self.node.assert_no_process_death();
        let error = self.error.lock().unwrap();
        assert!(error.is_none(), "L1 mirror failed: {error:?}");
    }
}
impl Drop for MirrorComposer {
    fn drop(&mut self) {
        self.feed.abort();
    }
}

pub async fn spawn_composer(world: &CrossChainWorld, builder_url: &str) -> Result<MirrorComposer> {
    // Embedded L1 reserves http + 1 for WebSocket RPC.
    let l1_http = probe_unique_http_port(&mut handed_out_ports());
    let l1_auth = free_port();
    let l2_xchain = free_port();
    let datadir = tempfile::tempdir()?;
    let witness = tempfile::tempdir()?;
    let jwt_dir = tempfile::tempdir()?;
    let jwt_path = jwt_dir.path().join("jwt");
    // Test-only Engine authentication, unrelated to transaction authorization.
    let jwt_hex = "11".repeat(32);
    std::fs::write(&jwt_path, &jwt_hex)?;
    let jwt = JwtSecret::from_hex(&jwt_hex)?;
    let mut env = world.cfg.env();
    for (name, value) in [
        ("EEZ_L1_CHAIN", "devnet".to_string()),
        ("EEZ_L1_BUILDER_RPC_URL", builder_url.to_owned()),
        ("EEZ_L1_HTTP_PORT", l1_http.to_string()),
        ("EEZ_L1_AUTH_PORT", l1_auth.to_string()),
        ("EEZ_L1_P2P_PORT", free_port().to_string()),
        ("EEZ_L1_DISCV5_PORT", free_port().to_string()),
        ("EEZ_L1_RPC_URL", format!("http://127.0.0.1:{l1_http}")),
        ("EEZ_L1_JWT_SECRET", jwt_path.to_string_lossy().into_owned()),
        ("EEZ_L1_POSTER_KEY", ANVIL_KEY_3.to_owned()),
        ("EEZ_L1_XCHAIN_PORT", free_port().to_string()),
        ("EEZ_L2_XCHAIN_PORT", l2_xchain.to_string()),
        ("EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES", "true".to_string()),
        ("EEZ_PROVER_URL", world.proof_signer.endpoint().to_owned()),
        (
            "EEZ_ATTESTER_ADDRESS",
            format!("{:#x}", signer_address(world.cfg.deployer_key)?),
        ),
        (
            "EEZ_WITNESS_DB_PATH",
            witness.path().to_string_lossy().into_owned(),
        ),
    ] {
        env.retain(|(key, _)| *key != name);
        env.push((name, value));
    }
    assert!(!env.iter().any(|(name, _)| *name == "EEZ_L2_SYSTEM_KEY"));
    let node = NodeHandle::spawn(datadir.path(), &env)?;
    let error = Arc::new(Mutex::new(None));
    let feed_error = error.clone();
    let source = world.l1_rpc();
    let destination = format!("http://127.0.0.1:{l1_auth}");
    let feed = tokio::spawn(async move {
        let result = mirror_l1(&source, &destination, jwt).await;
        if let Err(err) = result {
            *feed_error.lock().unwrap() = Some(format!("{err:#}"));
        }
    });
    let mirror = MirrorComposer {
        node,
        l2_xchain: format!("http://127.0.0.1:{l2_xchain}"),
        feed,
        error,
        _datadir: datadir,
        _witness: witness,
        _jwt_dir: jwt_dir,
    };
    mirror
        .node
        .wait_for_rpc(&mirror.node.l2_rpc_url(), SETUP_TIMEOUT, "mirror L2 RPC")
        .await?;
    Ok(mirror)
}

async fn mirror_l1(source: &str, destination: &str, jwt: JwtSecret) -> Result<()> {
    let source = ProviderBuilder::new().connect_http(source.parse()?);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    // Wait for the Engine listener, then require VALID for every copied block.
    wait_for(SETUP_TIMEOUT, || async {
        let response = client.post(destination).bearer_auth(jwt.encode(&Claims::with_current_timestamp())?)
            .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"engine_exchangeCapabilities","params":[[]]})).send().await;
        Ok(response.ok().filter(|r| r.status().is_success()).map(|_| ()))
    }).await?;
    let mut number = 1;
    loop {
        let Some(rpc_block) = source
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .full()
            .await?
        else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let block: reth_ethereum_primitives::Block =
            rpc_block.into_consensus().convert_transactions();
        let hash = block.header.hash_slow();
        let beacon = block.header.parent_beacon_block_root.unwrap_or_default();
        let payload = ExecutionPayloadV3::from_block_unchecked(hash, &block);
        eprintln!("L1 mirror importing block {number}");
        let result = engine_call(
            &client,
            destination,
            &jwt,
            "engine_newPayloadV3",
            serde_json::json!([payload, [], beacon]),
        )
        .await?;
        ensure!(
            result["status"] == "VALID",
            "L1 newPayload {number}: {result}"
        );
        let mut safe = B256::ZERO;
        let mut finalized = B256::ZERO;
        for (tag, slot) in [
            (BlockNumberOrTag::Safe, &mut safe),
            (BlockNumberOrTag::Finalized, &mut finalized),
        ] {
            if let Some(block) = source.get_block_by_number(tag).await?
                && block.header.number <= number
            {
                *slot = block.header.hash;
            }
        }
        let result = engine_call(
            &client,
            destination,
            &jwt,
            "engine_forkchoiceUpdatedV3",
            serde_json::json!([{
            "headBlockHash": hash, "safeBlockHash": safe, "finalizedBlockHash": finalized
        }, null]),
        )
        .await?;
        ensure!(
            result["payloadStatus"]["status"] == "VALID",
            "L1 forkchoice {number}: {result}"
        );
        number += 1;
    }
}
async fn engine_call(
    client: &reqwest::Client,
    url: &str,
    jwt: &JwtSecret,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    let response: serde_json::Value = client
        .post(url)
        .bearer_auth(jwt.encode(&Claims::with_current_timestamp())?)
        .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(response.get("error").is_none(), "{method}: {response}");
    Ok(response["result"].clone())
}

pub async fn spawn_follower(world: &CrossChainWorld, name: &str) -> Result<NodeHandle> {
    let env = vec![
        ("EEZ_L1_RPC_URL", world.l1_rpc()),
        ("EEZ_L1_CHAIN_ID", DEV_CHAIN_ID.to_string()),
        ("EEZL2_ADDRESS", format!("{EEZL2_ADDRESS:#x}")),
        ("EEZ_ROLLUP_ID", world.cfg.rollup_id.to_string()),
        (
            "EEZ_REGISTRY_ADDRESS",
            format!("{:#x}", world.cfg.eez_address),
        ),
        (
            "EEZ_REGISTRY_DEPLOY_BLOCK",
            world.dep.deploy_block.to_string(),
        ),
        ("EEZ_L1_BLOCK_TIME_MS", "5000".to_string()),
        ("EEZ_L2_BLOCK_TIME_MS", "1000".to_string()),
        ("EEZ_PROOF_TIME_MS", "1000".to_string()),
        ("EEZ_SUBMISSION_SLACK_MS", "100".to_string()),
    ];
    NodeHandle::start(
        name,
        &NodeConfig {
            binary: NodeBinary::Follower,
            genesis_path: Some(&world.cfg.l2_genesis.0),
        },
        &env,
    )
    .await
}

/// The embedded L1 relay forwards bundles into a mempool, without atomicity.
/// Select one posting Composer at a time; all node execution, proving and L1
/// derivation remain live. This fixture does not test production builder ordering.
pub struct SelectedBuilder {
    pub url: String,
    state: Arc<Mutex<(Option<String>, Address)>>,
    server: jsonrpsee::server::ServerHandle,
}
impl SelectedBuilder {
    pub async fn start() -> Result<Self> {
        use alloy_consensus::transaction::SignerRecoverable;
        use alloy_eips::Decodable2718;
        use jsonrpsee::{RpcModule, server::ServerBuilder, types::ErrorObjectOwned};
        let server = ServerBuilder::default()
            .build((std::net::Ipv4Addr::LOCALHOST, free_port()))
            .await?;
        let url = format!("http://{}", server.local_addr()?);
        let state = Arc::new(Mutex::new((None::<String>, ANVIL_ADDR)));
        let mut module = RpcModule::new(state.clone());
        module.register_async_method("eth_sendBundle", |params, state, _| async move {
            let error = |message: String| ErrorObjectOwned::owned(-32000, message, None::<()>);
            let params: Vec<serde_json::Value> = params.parse()?;
            let raw = params.first().and_then(|p| p["txs"][0].as_str()).ok_or_else(|| error("missing bundle transaction".into()))?;
            let bytes = hex::decode(raw).map_err(|err| error(err.to_string()))?;
            let tx = alloy_consensus::TxEnvelope::decode_2718_exact(&bytes).map_err(|err| error(err.to_string()))?;
            let sender = tx.recover_signer().map_err(|err| error(err.to_string()))?;
            let (upstream, selected) = state.lock().unwrap().clone();
            if sender != selected { return Err(error("development builder selected another Composer".into())); }
            let upstream = upstream.ok_or_else(|| error("development builder awaits fixture deployment".into()))?;
            let response: serde_json::Value = reqwest::Client::new().post(&upstream)
                .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":params}))
                .send().await.map_err(|err| error(err.to_string()))?
                .json().await.map_err(|err| error(err.to_string()))?;
            if let Some(err) = response.get("error") { return Err(error(err.to_string())); }
            Ok(response["result"].clone())
        })?;
        Ok(Self {
            url,
            state,
            server: server.start(module),
        })
    }
    pub fn select(&self, upstream: String, poster: Address) {
        *self.state.lock().unwrap() = (Some(upstream), poster);
    }
}
impl Drop for SelectedBuilder {
    fn drop(&mut self) {
        let _ = self.server.stop();
    }
}
