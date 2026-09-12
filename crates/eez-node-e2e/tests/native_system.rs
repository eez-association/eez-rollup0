//! Live native transaction QA with two independent Composers and two keyless followers.
use alloy_primitives::U256;
use alloy_sol_types::SolCall;
use anyhow::{Result, ensure};
use serde_json::{Value, json};

mod common;
use common::*;

async fn rpc(url: &str, method: &str, params: Value) -> Result<Value> {
    let response: Value = reqwest::Client::new()
        .post(url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(response.get("error").is_none(), "{method}: {response}");
    Ok(response["result"].clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_composers_two_keyless_followers() -> Result<()> {
    let genesis: Value = serde_json::from_str(include_str!("fixtures/genesis.json"))?;
    ensure!(
        genesis["alloc"]
            .get(format!("{:#x}", eez_primitives::SYSTEM_ADDRESS))
            .is_none(),
        "genesis must omit the system account entirely"
    );
    let builder = common::native::SelectedBuilder::start().await?;
    let world = setup_cross_chain_with_env(&[
        ("EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES", "true".to_owned()),
        ("EEZ_L1_BUILDER_RPC_URL", builder.url.clone()),
    ])
    .await?;
    builder.select(world.l1_rpc(), ANVIL_ADDR);
    let composer = common::native::spawn_composer(&world, &builder.url).await?;
    let follower = common::native::spawn_follower(&world, "native-follower-1").await?;
    let primary_rpc = world.l2_rpc();
    let mirror_rpc = composer.node.l2_rpc_url();
    let follower_rpc = follower.l2_rpc_url();
    let l1_rpc = world.l1_rpc();
    let system = eez_primitives::SYSTEM_ADDRESS;
    let deposit = U256::from(1_000_000_000_000_000_000u128);
    let withdrawal = deposit / U256::from(4);
    let recipient_before = l2_balance(&primary_rpc, world.recipient).await?;
    let withdrawal_before = l2_balance(&l1_rpc, world.withdrawal_recipient).await?;
    for url in [&primary_rpc, &mirror_rpc, &follower_rpc] {
        ensure!(
            rpc(url, "eth_getBalance", json!([system, "0x0"])).await? == "0x0",
            "system prefunding at {url}"
        );
        ensure!(
            rpc(url, "eth_getTransactionCount", json!([system, "0x0"])).await? == "0x0",
            "system genesis nonce at {url}"
        );
    }

    // Composer 1 mints a real deposit from an initially absent system account.
    let inbound = sign_and_send(
        &world.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        pending_nonce(&l1_rpc, INBOUND_USER).await?,
        Some(world.deposit_proxy),
        deposit,
        Vec::new(),
        600_000,
    )
    .await?;
    ensure!(
        wait_for(SETTLE_TIMEOUT, || async {
            receipt_ok(&l1_rpc, inbound).await
        })
        .await?,
        "inbound reverted"
    );
    wait_for(SETTLE_TIMEOUT, || async {
        composer.assert_healthy();
        for url in [&mirror_rpc, &follower_rpc] {
            if rpc(url, "eth_getCode", json!([world.value_l2, "latest"])).await? == "0x" {
                return Ok(None);
            }
        }
        Ok(
            (l2_balance(&mirror_rpc, world.recipient).await? == recipient_before + deposit
                && l2_balance(&follower_rpc, world.recipient).await? == recipient_before + deposit)
                .then_some(()),
        )
    })
    .await?;
    for url in [&primary_rpc, &mirror_rpc, &follower_rpc] {
        ensure!(
            l2_balance(url, system).await? == U256::ZERO,
            "deposit consumed or retained system funds at {url}"
        );
        ensure!(
            rpc(url, "eth_getTransactionCount", json!([system, "latest"])).await? == "0x1",
            "deposit nonce at {url}"
        );
    }

    // Drain any already accepted Composer 1 bundle before switching producers.
    builder.select(l1_rpc.clone(), ANVIL_ADDR_3);
    let l1 = alloy_provider::ProviderBuilder::new().connect_http(l1_rpc.parse()?);
    use alloy_provider::Provider;
    let switch_height = l1.get_block_number().await?;
    wait_for(SETTLE_TIMEOUT, || async {
        Ok((l1.get_block_number().await? > switch_height).then_some(()))
    })
    .await?;
    // Composer 2 must actually build/prove a new native load + outbound user pair.
    let outbound = sign_and_send(
        &composer.l2_xchain,
        OUTBOUND_USER,
        world.l2_chain_id,
        pending_nonce(&mirror_rpc, OUTBOUND_USER).await?,
        Some(world.withdrawal_proxy),
        withdrawal,
        Vec::new(),
        900_000,
    )
    .await?;
    ensure!(
        wait_for(SETTLE_TIMEOUT, || async {
            composer.assert_healthy();
            receipt_ok(&mirror_rpc, outbound).await
        })
        .await?,
        "outbound reverted"
    );
    wait_for(SETTLE_TIMEOUT, || async {
        composer.assert_healthy();
        let root = state_root(&l1_rpc, world.cfg.eez_address, world.cfg.rollup_id).await?;
        Ok((l2_balance(&l1_rpc, world.withdrawal_recipient).await?
            == withdrawal_before + withdrawal
            && safe_block_state_root(&primary_rpc).await? == Some(root)
            && safe_block_state_root(&mirror_rpc).await? == Some(root))
        .then_some(()))
    })
    .await?;

    // Joining after both native directions settled exercises reconstruction from L1 history.
    let late = common::native::spawn_follower(&world, "native-follower-late").await?;
    let late_rpc = late.l2_rpc_url();
    let urls = [&primary_rpc, &mirror_rpc, &follower_rpc, &late_rpc];
    let receipt = rpc(&primary_rpc, "eth_getTransactionReceipt", json!([outbound])).await?;
    ensure!(
        !receipt.is_null(),
        "outbound did not become canonical on Composer 1"
    );
    let final_height = u64::from_str_radix(
        receipt["blockNumber"]
            .as_str()
            .unwrap()
            .trim_start_matches("0x"),
        16,
    )?;
    wait_for(SETTLE_TIMEOUT, || async {
        composer.assert_healthy();
        for url in urls {
            let safe = rpc(url, "eth_getBlockByNumber", json!(["safe", false])).await?;
            if safe.is_null() {
                return Ok(None);
            }
            let number = u64::from_str_radix(
                safe["number"].as_str().unwrap().trim_start_matches("0x"),
                16,
            )?;
            if number < final_height {
                return Ok(None);
            }
        }
        Ok(Some(()))
    })
    .await?;

    for url in urls {
        ensure!(
            l2_balance(url, system).await? == withdrawal,
            "outbound ETH must remain at the system address on {url}"
        );
        ensure!(
            rpc(url, "eth_getTransactionCount", json!([system, "latest"])).await? == "0x2",
            "native nonces differ at {url}"
        );
        ensure!(
            l2_balance(url, world.recipient).await? == recipient_before + deposit,
            "deposit balance differs at {url}"
        );
    }
    println!(
        "native accounting evidence: {}",
        json!({
            "systemGenesisBalance": "0x0", "systemGenesisNonce": "0x0",
            "deposit": deposit, "withdrawal": withdrawal,
            "finalSystemBalance": withdrawal, "finalSystemNonce": 2,
        })
    );

    let mut native_count = 0;
    let mut incoming_count = 0;
    let mut load_count = 0;
    for number in 1..=final_height {
        let block = rpc(
            &primary_rpc,
            "eth_getBlockByNumber",
            json!([format!("{number:#x}"), true]),
        )
        .await?;
        for url in &urls[1..] {
            let other = rpc(
                url,
                "eth_getBlockByNumber",
                json!([format!("{number:#x}"), true]),
            )
            .await?;
            ensure!(block == other, "block {number} differs at {url}");
        }
        for tx in block["transactions"].as_array().unwrap() {
            if tx["type"] != "0x76" {
                continue;
            }
            native_count += 1;
            ensure!(
                tx["from"] == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076",
                "native caller: {tx}"
            );
            ensure!(
                tx.get("r").is_none() && tx.get("s").is_none() && tx.get("v").is_none(),
                "native signature: {tx}"
            );
            let input = tx["input"].as_str().unwrap();
            if input.starts_with(&format!(
                "0x{}",
                alloy_primitives::hex::encode(
                    eez_protocol::abi::executeIncomingCrossChainCallCall::SELECTOR
                )
            )) {
                incoming_count += 1;
            }
            if input.starts_with(&format!(
                "0x{}",
                alloy_primitives::hex::encode(eez_protocol::abi::loadExecutionTableCall::SELECTOR)
            )) {
                load_count += 1;
            }
            let hash = &tx["hash"];
            let native_receipt =
                rpc(&primary_rpc, "eth_getTransactionReceipt", json!([hash])).await?;
            ensure!(
                native_receipt["type"] == "0x76" && native_receipt["status"] == "0x1",
                "native receipt: {native_receipt}"
            );
            ensure!(
                native_receipt["effectiveGasPrice"] == "0x0",
                "native fee: {native_receipt}"
            );
            ensure!(
                native_receipt["gasUsed"] != "0x0",
                "native execution must still be metered"
            );
            println!(
                "native transaction evidence: {}",
                json!({
                    "blockNumber": block["number"], "blockHash": block["hash"],
                    "transactionsRoot": block["transactionsRoot"], "receiptsRoot": block["receiptsRoot"],
                    "stateRoot": block["stateRoot"], "transaction": tx, "receipt": native_receipt
                })
            );
            let raw = rpc(&primary_rpc, "eth_getRawTransactionByHash", json!([hash])).await?;
            ensure!(
                raw.as_str().is_some_and(|raw| raw.starts_with("0x76")),
                "native raw bytes: {raw}"
            );
            for url in urls {
                ensure!(
                    rpc(url, "eth_getTransactionReceipt", json!([hash])).await? == native_receipt,
                    "native receipt differs at {url}"
                );
                // The real public RPC must reject a correctly encoded native envelope.
                let response: Value = reqwest::Client::new().post(url)
                    .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":[raw]}))
                    .send().await?.json().await?;
                ensure!(
                    response["error"]["message"] == "failed to decode signed transaction",
                    "public RPC must reject the native envelope at decoding, before nonce or balance checks at {url}: {response}"
                );
            }
        }
    }
    ensure!(
        incoming_count > 0 && load_count > 0,
        "did not exercise both native directions: incoming={incoming_count}, load={load_count}"
    );
    world.node.assert_no_divergence_failure_logs();
    composer.node.assert_no_divergence_failure_logs();
    follower.assert_no_divergence_failure_logs();
    late.assert_no_divergence_failure_logs();
    world.proof_signer.assert_alive();
    println!(
        "native devnet verified: 2 Composers, 2 keyless followers (one late joiner), {final_height} identical blocks, {native_count} successful native transactions ({incoming_count} incoming, {load_count} load), identical receipts, RPC rejection on all nodes"
    );
    Ok(())
}
