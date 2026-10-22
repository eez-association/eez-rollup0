//! Batches posted through a contract (issue #175): the scan reads their bytes
//! from a block trace, so a node that traces must derive exactly what L1
//! applied, and one that cannot must stop loudly.

use std::time::{Duration, Instant};

use alloy_consensus::Transaction as _;
use alloy_network::EthereumWallet;
use alloy_primitives::{Address, B256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionRequest};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::SolEvent;
use eez_testkit::{
    ANVIL_KEY_6, CrossChainWorld, IEEZ, NodeBinary, NodeConfig, NodeHandle,
    deploy_eez_post_batcher, rollup_root_at, setup_cross_chain_with_env, signer_address, wait_for,
    wait_for_safe_chain_contains,
};

const TIMEOUT: Duration = Duration::from_mins(6);
/// Above the composer's 10 gwei postBatch tip, so a copy in its block runs first.
const RIVAL_TIP: u128 = 100_000_000_000;

async fn follower(w: &CrossChainWorld, name: &str, trace_rpc: String) -> NodeHandle {
    let mut env = w.follower_env(None);
    env.push(("EEZ_L1_TRACE_RPC_URL", trace_rpc));
    let cfg = NodeConfig {
        binary: NodeBinary::Follower,
        genesis_path: Some(w.cfg.l2_genesis.0.as_path()),
        ..Default::default()
    };
    NodeHandle::start(name, &cfg, &env).await.unwrap()
}

async fn commitment_at(w: &CrossChainWorld, l1_block: u64) -> anyhow::Result<B256> {
    let (eez, rollup) = (w.cfg.eez_address, w.cfg.rollup_id);
    rollup_root_at(&w.l1_rpc(), eez, rollup, Some(l1_block)).await
}

/// Sends the calldata of the composer's pending postBatch to `EEZPostBatcher`
/// (EEZ's selector, but `tx.to` is the batcher) until a copy lands ahead of
/// the original and applies it. Returns the copy's L1 block.
async fn repost_ahead(w: &CrossChainWorld, batcher: Address) -> u64 {
    let poster = signer_address(w.cfg.poster_key).unwrap();
    let rival: PrivateKeySigner = ANVIL_KEY_6[2..].parse().unwrap();
    let l1 = ProviderBuilder::new()
        .wallet(EthereumWallet::from(rival))
        .connect_http(w.l1_rpc().parse().unwrap());
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        let pending = l1.get_block_by_number(BlockNumberOrTag::Pending).full();
        let post = pending.await.unwrap().and_then(|block| {
            let mut txs = block.transactions.into_transactions();
            txs.find(|tx| tx.inner.signer() == poster && tx.to() == Some(w.cfg.eez_address))
        });
        let Some(post) = post else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let copy = TransactionRequest::default()
            .to(batcher)
            .input(post.input().clone().into())
            .gas_limit(4_000_000)
            .max_priority_fee_per_gas(RIVAL_TIP)
            .max_fee_per_gas(2 * RIVAL_TIP);
        let sent = l1.send_transaction(copy).await.unwrap();
        let receipt = sent.with_timeout(Some(TIMEOUT)).get_receipt();
        let receipt = receipt.await.unwrap();
        // A copy mined after the original reverts on its stale anchor.
        let applied = (receipt.inner.logs().iter())
            .any(|log| log.topic0() == Some(&IEEZ::L2ExecutionPerformed::SIGNATURE_HASH));
        if receipt.status() && applied {
            return receipt.block_number.expect("mined");
        }
    }
    panic!("no copy landed first\n{}", w.settlement_diagnostics());
}

/// Every node's safe chain holds the L2 block L1 committed to at `l1_block`.
async fn assert_safe_holds_commitment(w: &CrossChainWorld, nodes: &[&NodeHandle], l1_block: u64) {
    let commitment = commitment_at(w, l1_block).await.unwrap();
    let l2 = ProviderBuilder::new().connect_http(w.l2_rpc().parse().unwrap());
    let number = wait_for(TIMEOUT, || async {
        let block = l2.get_block_by_hash(commitment).await?;
        Ok(block.map(|block| block.header.number))
    });
    let number = number.await.expect("composer holds L1's commitment");
    for node in nodes {
        let held = wait_for_safe_chain_contains(node, number, commitment, TIMEOUT).await;
        held.unwrap_or_else(|err| panic!("{}: no safe {commitment}: {err:#}", node.name));
    }
}

/// A rival lands a copy of the composer's batch through `EEZPostBatcher`. The
/// composer and a tracing follower derive what L1 applied and keep settling; a
/// follower whose trace endpoint lacks `debug` stops at that block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_contract_post_derives_identically_and_an_untraced_node_stops() {
    let external = ("EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES", "true".to_owned());
    let w = setup_cross_chain_with_env(&[external]).await.unwrap();
    let l1_rpc = w.l1_rpc();
    let batcher = deploy_eez_post_batcher(&l1_rpc, ANVIL_KEY_6, w.cfg.eez_address);
    let batcher = batcher.await.unwrap();
    let traced = follower(&w, "traced-follower", l1_rpc.clone()).await;
    // The composer's L1 front forwards only `eth_*`: `debug_*` is -32601.
    let blind = follower(&w, "blind-follower", w.l1_xchain()).await;

    let copied = repost_ahead(&w, batcher).await;
    assert_safe_holds_commitment(&w, &[&w.node, &traced], copied).await;
    let first = commitment_at(&w, copied).await.unwrap();
    let l1 = ProviderBuilder::new().connect_http(l1_rpc.parse().unwrap());
    let later = wait_for(TIMEOUT, || async {
        let head = l1.get_block_number().await?;
        Ok((commitment_at(&w, head).await? != first).then_some(head))
    });
    let later = later.await.expect("the composer settles after the copy");
    assert_safe_holds_commitment(&w, &[&w.node, &traced], later).await;

    let exit = wait_for(TIMEOUT, || async { Ok(blind.exit_status()) }).await;
    assert!(!exit.expect("the blind follower stops").success());
    let alarm = "EEZ_L1_TRACE_RPC_URL";
    let alarms = blind.log_lines_matching(&[alarm], usize::MAX);
    let at_block = format!("trace unavailable at block {copied}:");
    let loud = |line: &str| line.contains(r#""level":"ERROR""#) && line.contains(&at_block);
    assert!(alarms.lines().any(loud), "{}", blind.log_tail(40));
    assert_eq!(traced.log_count_matching(&[alarm]).unwrap(), 0);
    let traced_post = "tracing a block whose contract post lists our rollup";
    assert!(traced.log_count_matching(&[traced_post]).unwrap() > 0);
    w.node.assert_no_divergence_failure_logs();
    traced.assert_no_divergence_failure_logs();
}
