//! Real-engine regression: an unselected fork is not an evicted fork.
use std::time::Duration;

use alloy_primitives::{B256, U256};
use anyhow::{Result, ensure};
use eez_control_rpc::{v1, v2};
use eez_testkit::*;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_stateless_prover_composers_recover_from_l1_reorg() -> Result<()> {
    shared_prover_reorg(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_stateful_prover_composers_recover_from_l1_reorg() -> Result<()> {
    shared_prover_reorg(true).await
}

// First prove with one sequential Composer, then race a pipelined Composer on
// that very same prover and replace the L1 suffix underneath both live sessions.
async fn shared_prover_reorg(stateful: bool) -> Result<()> {
    let harness = Harness::fresh().await?;
    let chain = harness.chain();
    let config = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        ..Default::default()
    };
    let witness_a = tempfile::tempdir()?;
    let witness_b = tempfile::tempdir()?;
    let stateful_node;
    let env_a;
    let endpoint;
    if stateful {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        drop(listener);
        let mut env = harness.follower_env(None).await?;
        env.extend([
            ("EEZ_STATEFUL_PROOF_SIGNER_ADDR", address.to_string()),
            (
                "EEZ_STATEFUL_PROOF_SIGNER_KEY",
                ANVIL_ATTESTER_KEY.to_owned(),
            ),
            (
                "EEZ_ATTESTER_ADDRESS",
                format!("{:#x}", signer_address(ANVIL_ATTESTER_KEY)?),
            ),
            (
                "EEZ_VKEY",
                format!("{:#x}", signer_address(ANVIL_ATTESTER_KEY)?.into_word()),
            ),
            (
                "EEZ_PROOF_SYSTEM",
                format!("{:#x}", harness.dep.proof_system_address),
            ),
            (
                "EEZ_L2_SYSTEM_ADDRESS",
                format!("{:#x}", eez_primitives::SYSTEM_ADDRESS),
            ),
        ]);
        stateful_node = Some(
            NodeHandle::start(
                "shared-stateful-prover",
                &NodeConfig {
                    binary: NodeBinary::Follower,
                    genesis_path: Some(harness.l2_genesis_path()),
                },
                &env,
            )
            .await?,
        );
        endpoint = format!("http://{address}");
        env_a = harness
            .env_for_prover(ANVIL_KEY, &endpoint, witness_a.path())
            .await?;
    } else {
        stateful_node = None;
        env_a = harness.env_for(ANVIL_KEY, true).await?;
        endpoint = env_a
            .iter()
            .find(|(key, _)| *key == "EEZ_PROVER_URL")
            .unwrap()
            .1
            .clone();
    }
    let timeout = Duration::from_mins(3);
    wait_for(Duration::from_secs(30), || async {
        Ok(v2::prover_client::ProverClient::connect(endpoint.clone())
            .await
            .ok())
    })
    .await?;
    let a = NodeHandle::start("shared-baseline-a", &config, &env_a).await?;
    a.run_tx_spammer(ANVIL_KEY_1);
    chain
        .wait_for_batches_or_node_failure(2, &[&a], timeout)
        .await?;
    ensure!(
        a.log_count_matching(&["eez.prover_client.attested"])? > 0,
        "baseline Composer never received a proof"
    );

    let mut env_b = harness
        .env_for_prover(ANVIL_KEY_4, &endpoint, witness_b.path())
        .await?;
    env_b.push(("EEZ_PROVER_PIPELINING", "true".to_owned()));
    let b = NodeHandle::start("shared-pipelined-b", &config, &env_b).await?;
    b.run_tx_spammer(ANVIL_KEY_2);
    chain
        .wait_for_batches_or_node_failure(6, &[&a, &b], timeout)
        .await?;
    wait_for(timeout, || async {
        Ok((b.log_count_matching(&["eez.prover_client.attested"])? > 0).then_some(()))
    })
    .await?;
    let previous_states = chain.executed_states().await?;
    let previous_batches = chain.batches_posted().await?;
    let proofs_a = a.log_count_matching(&["eez.prover_client.attested"])?;
    let proofs_b = b.log_count_matching(&["eez.prover_client.attested"])?;
    harness.anvil.reorg(3).await?;
    chain
        .wait_for_batches_or_node_failure(previous_batches + 3, &[&a, &b], timeout)
        .await?;
    tokio::try_join!(
        a.wait_for_reorg_seen(timeout),
        b.wait_for_reorg_seen(timeout)
    )?;
    let (number, hash) =
        wait_for_new_attested_safe_block(&a, &chain, &previous_states, timeout).await?;
    wait_for_safe_chain_contains(&b, number, hash, timeout).await?;
    wait_for_safe_prefix_convergence(&[&a, &b], number, timeout).await?;
    wait_for(timeout, || async {
        Ok(
            (a.log_count_matching(&["eez.prover_client.attested"])? > proofs_a
                && b.log_count_matching(&["eez.prover_client.attested"])? > proofs_b)
                .then_some(()),
        )
    })
    .await?;
    a.assert_no_process_death();
    b.assert_no_process_death();
    if let Some(prover) = stateful_node.as_ref() {
        prover.assert_no_process_death();
    }
    eprintln!(
        "shared prover QA: backend={}, batches={} -> {}, recovered safe={number}:{hash}, proofs A={} B={}",
        if stateful { "stateful" } else { "stateless" },
        previous_batches,
        chain.batches_posted().await?,
        a.log_count_matching(&["eez.prover_client.attested"])?,
        b.log_count_matching(&["eez.prover_client.attested"])?
    );
    Ok(())
}

async fn rpc(url: &str, method: &str, params: Value) -> Result<Value> {
    let response: Value = reqwest::Client::new()
        .post(url)
        .timeout(Duration::from_secs(10))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await?
        .json()
        .await?;
    ensure!(response.get("error").is_none(), "{method}: {response}");
    Ok(response["result"].clone())
}

async fn exchange(
    requests: &mpsc::Sender<v2::ClientFrame>,
    responses: &mut tonic::Streaming<v2::ServerFrame>,
    binding: &v2::ServerFrame,
    kind: v2::client_frame::Kind,
) -> Result<v2::ServerFrame> {
    requests
        .send(v2::ClientFrame {
            session_id: binding.session_id.clone(),
            epoch: binding.epoch,
            request_id: 1,
            kind: Some(kind),
        })
        .await?;
    Ok(
        tokio::time::timeout(Duration::from_secs(10), responses.message())
            .await??
            .expect("response"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stateful_sessions_can_extend_and_finalize_an_unselected_branch() -> Result<()> {
    let harness = Harness::fresh().await?;
    let config = NodeConfig {
        genesis_path: Some(harness.l2_genesis_path()),
        ..Default::default()
    };
    // Neither source can post to L1 while we deliberately switch the prover's unsafe fork.
    let env = harness.env_for(STAGING_USER_KEY, true).await?;
    let a = NodeHandle::start("fork-source-a", &config, &env).await?;
    let b = NodeHandle::start("fork-source-b", &config, &env).await?;
    let tx =
        send_l2_value_transfer(&a.l2_rpc_url(), ANVIL_KEY_1, ANVIL_ADDR_3, U256::from(1)).await?;
    let receipt = wait_for(Duration::from_secs(40), || async {
        let r = rpc(&a.l2_rpc_url(), "eth_getTransactionReceipt", json!([tx])).await?;
        Ok((!r.is_null()).then_some(r))
    })
    .await?;
    let fork_height = u64::from_str_radix(
        receipt["blockNumber"]
            .as_str()
            .unwrap()
            .trim_start_matches("0x"),
        16,
    )?;
    let height = fork_height + 1;
    tokio::try_join!(
        wait_for_latest_height(&a, height + 1, Duration::from_secs(40)),
        wait_for_latest_height(&b, height, Duration::from_secs(40)),
    )?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    drop(listener);
    let mut env = harness.follower_env(None).await?;
    for (key, value) in [
        ("EEZ_STATEFUL_PROOF_SIGNER_ADDR", addr.to_string()),
        (
            "EEZ_STATEFUL_PROOF_SIGNER_KEY",
            ANVIL_ATTESTER_KEY.to_owned(),
        ),
        (
            "EEZ_ATTESTER_ADDRESS",
            format!("{:#x}", signer_address(ANVIL_ATTESTER_KEY)?),
        ),
        (
            "EEZ_VKEY",
            format!("{:#x}", signer_address(ANVIL_ATTESTER_KEY)?.into_word()),
        ),
        (
            "EEZ_PROOF_SYSTEM",
            format!("{:#x}", harness.dep.proof_system_address),
        ),
        (
            "EEZ_L2_SYSTEM_ADDRESS",
            format!("{:#x}", eez_primitives::SYSTEM_ADDRESS),
        ),
    ] {
        env.retain(|(k, _)| *k != key);
        env.push((key, value));
    }
    let prover = NodeHandle::start(
        "fork-prover",
        &NodeConfig {
            binary: NodeBinary::Follower,
            genesis_path: Some(harness.l2_genesis_path()),
        },
        &env,
    )
    .await?;
    let endpoint = format!("http://{addr}");
    let mut client = wait_for(Duration::from_secs(30), || async {
        Ok(v2::prover_client::ProverClient::connect(endpoint.clone())
            .await
            .ok())
    })
    .await?;
    let genesis = rpc(
        &a.l2_rpc_url(),
        "eth_getBlockByNumber",
        json!(["0x0", false]),
    )
    .await?;
    let decode = |value: &Value| alloy_primitives::hex::decode(value.as_str().unwrap());
    let begin = v2::client_frame::Kind::Begin(v2::Begin {
        rollup_id: 1,
        anchor_number: 0,
        anchor_hash: decode(&genesis["hash"])?,
        anchor_state_root: decode(&genesis["stateRoot"])?,
    });
    let mut streams = Vec::new();
    let mut branches = Vec::new();
    for (index, node) in [&a, &b].into_iter().enumerate() {
        let (requests, receiver) = mpsc::channel(4);
        let mut responses = client
            .prove_stream(ReceiverStream::new(receiver))
            .await?
            .into_inner();
        let ready = exchange(
            &requests,
            &mut responses,
            &v2::ServerFrame::default(),
            begin.clone(),
        )
        .await?;
        ensure!(
            matches!(ready.kind, Some(v2::server_frame::Kind::Ready(_))),
            "{ready:?}"
        );
        let mut blocks = Vec::new();
        for number in 1..=height + u64::from(index == 0) {
            let tag = format!("{number:#x}");
            let header = rpc(
                &node.l2_rpc_url(),
                "eth_getBlockByNumber",
                json!([tag, true]),
            )
            .await?;
            let block: alloy_rpc_types_eth::Block<eez_primitives::EezTxEnvelope> =
                serde_json::from_value(header.clone())?;
            blocks.push(v2::Block {
                data: Some(v1::BlockWitness {
                    number,
                    hash: decode(&header["hash"])?,
                    parent_hash: decode(&header["parentHash"])?,
                    rlp: alloy_rlp::encode(block.into_consensus()),
                    witness: Some(v1::ExecutionWitness::default()),
                }),
            });
        }
        for block in blocks.iter().take(height as usize) {
            let response = exchange(
                &requests,
                &mut responses,
                &ready,
                v2::client_frame::Kind::Block(block.clone()),
            )
            .await?;
            ensure!(
                matches!(response.kind, Some(v2::server_frame::Kind::Validated(_))),
                "{response:?}"
            );
        }
        streams.push((requests, responses, ready));
        branches.push(blocks);
    }
    let a_parent = &branches[0][height as usize - 1].data.as_ref().unwrap().hash;
    ensure!(
        *a_parent != branches[1][height as usize - 1].data.as_ref().unwrap().hash,
        "sources must diverge"
    );
    let a_hash = B256::from_slice(a_parent);
    // eth_getBlockByHash has a separate RPC cache and can still return A.
    // State lookup exercises the selected provider view that validation needs.
    let unavailable = rpc(
        &prover.l2_rpc_url(),
        "eth_getBalance",
        json!([ANVIL_ADDR_3, {"blockHash": a_hash, "requireCanonical": false}]),
    )
    .await
    .unwrap_err()
    .to_string();
    ensure!(
        unavailable.contains("not found"),
        "expected unavailable A state, got {unavailable}"
    );
    let (requests, responses, ready) = &mut streams[0];
    let response = exchange(
        requests,
        responses,
        ready,
        v2::client_frame::Kind::Block(branches[0][height as usize].clone()),
    )
    .await?;
    ensure!(
        matches!(response.kind, Some(v2::server_frame::Kind::Validated(_))),
        "{response:?}"
    );

    // B is now unselected. Deliberately invalid calldata distinguishes reaching
    // settlement from the old premature backend-availability ABORTED. Valid proofs
    // and settlement are covered by the multi-composer live stress run.
    let (requests, responses, ready) = &mut streams[1];
    let response = exchange(
        requests,
        responses,
        ready,
        v2::client_frame::Kind::Finalize(v2::Finalize {
            from_block: 1,
            to_block: height,
            terminal_hash: branches[1][height as usize - 1]
                .data
                .as_ref()
                .unwrap()
                .hash
                .clone(),
            settlement: Some(v2::SettlementInput {
                abi_calldata: vec![0],
                ..Default::default()
            }),
        }),
    )
    .await?;
    let Some(v2::server_frame::Kind::Rejected(rejection)) = response.kind else {
        anyhow::bail!("expected calldata rejection")
    };
    ensure!(
        tonic::Code::from_i32(rejection.code) == tonic::Code::InvalidArgument,
        "{rejection:?}"
    );
    prover.assert_no_process_death();
    Ok(())
}
