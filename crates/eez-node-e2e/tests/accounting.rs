//! Live accounting against real EEZ contracts, Composer and stateless prover.
use alloy_primitives::{Address, B256, U256, hex};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Result, ensure};
use eez_testkit::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

sol! {
    function accept() external payable;
    function calls() external view returns (uint256);
    function fail() external payable;
    function force(address target) external payable;
    function catchCall(address target) external payable;
}

async fn rpc(url: &str, method: &str, params: Value) -> Result<Value> {
    let out: Value = reqwest::Client::new()
        .post(url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(out.get("error").is_none(), "{method}: {out}");
    Ok(out["result"].clone())
}

fn uint(value: &Value) -> Result<U256> {
    Ok(value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("not a quantity: {value}"))?
        .parse()?)
}

async fn receipt(url: &str, hash: B256) -> Result<Value> {
    wait_for(SETTLE_TIMEOUT, || async {
        let r = rpc(url, "eth_getTransactionReceipt", json!([hash])).await?;
        Ok((!r.is_null()).then_some(r))
    })
    .await
}

async fn settle(w: &CrossChainWorld, minimum: u64) -> Result<()> {
    wait_for(SETTLE_TIMEOUT, || async {
        let root = rollup_commitment(&w.l1_rpc(), w.cfg.eez_address, w.cfg.rollup_id).await?;
        let b = rpc(&w.l2_rpc(), "eth_getBlockByHash", json!([root, false])).await?;
        Ok((!b.is_null()
            && uint(&b["number"])?.to::<u64>() >= minimum
            && safe_block_hash(&w.l2_rpc()).await? == Some(root))
        .then_some(()))
    })
    .await
}

// Enumerate all changed accounts from execution state differences, then verify
// their actual RPC balances at every settled block. No inferred call recipients,
// test-supplied state, or final-only offsetting of mint/burn errors.
struct Audit {
    height: u64,
    balances: BTreeMap<Address, U256>,
    genesis: U256,
    withdrawals: BTreeMap<B256, U256>,
    transactions: usize,
}

impl Audit {
    fn new() -> Result<Self> {
        let genesis: Value = serde_json::from_str(include_str!("fixtures/genesis.json"))?;
        let mut balances = BTreeMap::new();
        for (address, entry) in genesis["alloc"].as_object().unwrap() {
            balances.insert(address.parse()?, uint(&entry["balance"])?);
        }
        Ok(Self {
            height: 0,
            genesis: balances.values().copied().sum(),
            balances,
            withdrawals: BTreeMap::new(),
            transactions: 0,
        })
    }

    async fn checkpoint(&mut self, w: &CrossChainWorld, label: &str, escrow: U256) -> Result<()> {
        let l1 = w.l1_rpc();
        let l2 = w.l2_rpc();
        let l1_height = rpc(&l1, "eth_blockNumber", json!([])).await?;
        let call = eez_protocol::abi::rollupsCall {
            rollupId: w.cfg.rollup_id,
        };
        let raw = rpc(&l1, "eth_call", json!([{"to":w.cfg.eez_address,"data":format!("0x{}", hex::encode(call.abi_encode()))},l1_height])).await?;
        let info = eez_protocol::abi::rollupsCall::abi_decode_returns(&hex::decode(
            raw.as_str().unwrap().trim_start_matches("0x"),
        )?)?;
        ensure!(
            info.etherBalance == escrow,
            "unexpected escrow at {label}: {} != {escrow}",
            info.etherBalance
        );
        let block = wait_for(SETTLE_TIMEOUT, || async {
            let b = rpc(&l2, "eth_getBlockByHash", json!([info.root, false])).await?;
            Ok((!b.is_null()).then_some(b))
        })
        .await?;
        let height = uint(&block["number"])?.to::<u64>();
        for n in self.height + 1..=height {
            let tag = format!("{n:#x}");
            let b = rpc(&l2, "eth_getBlockByNumber", json!([tag, true])).await?;
            let txs = b["transactions"].as_array().unwrap();
            if !txs.is_empty() {
                let traces = rpc(&l2, "debug_traceBlockByNumber", json!([tag, {
                    "tracer":"prestateTracer", "tracerConfig":{"diffMode":true,"disableCode":true,"disableStorage":true}
                }])).await?;
                let traces = traces
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("bad state traces: {traces}"))?;
                ensure!(traces.len() == txs.len(), "missing transaction traces");
                for (tx, trace) in txs.iter().zip(traces) {
                    ensure!(trace.get("error").is_none(), "trace failed: {trace}");
                    let hash: B256 = tx["hash"].as_str().unwrap().parse()?;
                    let r = receipt(&l2, hash).await?;
                    let success = r["status"] == "0x1";
                    let pre = trace["result"]["pre"].as_object().unwrap();
                    let post = trace["result"]["post"].as_object().unwrap();
                    let mut before = U256::ZERO;
                    let mut after = U256::ZERO;
                    for (address, state) in pre {
                        let address: Address = address.parse()?;
                        let balance = uint(&state["balance"])?;
                        ensure!(
                            self.balances.get(&address).copied().unwrap_or_default() == balance,
                            "trace prestate disagrees at {hash} for {address}"
                        );
                        before += balance;
                        self.balances.insert(address, U256::ZERO);
                    }
                    for (address, state) in post {
                        let balance = if state["balance"].is_null() {
                            pre.get(address)
                                .map(|s| uint(&s["balance"]))
                                .transpose()?
                                .unwrap_or_default()
                        } else {
                            uint(&state["balance"])?
                        };
                        after += balance;
                        self.balances.insert(address.parse()?, balance);
                    }
                    let minted = if success && tx["type"] == "0x76" {
                        uint(&tx["value"])?
                    } else {
                        U256::ZERO
                    };
                    // Expected burns come from submitted test intents, independently
                    // of the contract logs consumed by the implementation.
                    let burned = if success {
                        self.withdrawals.get(&hash).copied().unwrap_or_default()
                    } else {
                        U256::ZERO
                    };
                    ensure!(
                        before + minted == after + burned,
                        "transaction/group accounting mismatch at {hash}: pre={before}, post={after}, mint={minted}, burn={burned}"
                    );
                    self.transactions += 1;
                    println!(
                        "ACCOUNTING_GROUP {}",
                        json!({"block":n,"tx":hash,"success":success,"mint":minted.to_string(),"burn":burned.to_string(),"total":self.balances.values().copied().sum::<U256>().to_string()})
                    );
                }
            }
            let mut actual = U256::ZERO;
            for (address, expected) in &self.balances {
                let balance = uint(
                    &rpc(
                        &l2,
                        "eth_getBalance",
                        json!([address,{"blockHash":b["hash"]}]),
                    )
                    .await?,
                )?;
                ensure!(
                    balance == *expected,
                    "RPC/trace balance mismatch at block {n} for {address}: {balance} != {expected}"
                );
                actual += balance;
            }
            ensure!(
                actual == self.balances.values().copied().sum::<U256>(),
                "block sum mismatch"
            );
        }
        self.height = height;
        let total: U256 = self.balances.values().copied().sum();
        let physical =
            uint(&rpc(&l1, "eth_getBalance", json!([w.cfg.eez_address, l1_height])).await?)?;
        ensure!(
            physical == escrow,
            "L1 contract/bookkeeping mismatch at {label}"
        );
        ensure!(
            total == self.genesis + escrow,
            "L2 supply/L1 backing mismatch at {label}: {total} != {} + {escrow}",
            self.genesis
        );
        println!(
            "ACCOUNTING_CHECKPOINT {}",
            json!({"label":label,"l1Height":l1_height,"l2Height":height,"root":info.root,"l2Total":total.to_string(),"genesisTotal":self.genesis.to_string(),"escrow":escrow.to_string(),"transactions":self.transactions})
        );
        Ok(())
    }
}

async fn deploy(w: &CrossChainWorld, l1: bool) -> Result<Address> {
    let artifact: Value = serde_json::from_str(&std::fs::read_to_string(
        repo_root().join("contracts/out/AccountingTarget.sol/AccountingTarget.json"),
    )?)?;
    let url = if l1 { w.l1_rpc() } else { w.l2_rpc() };
    let hash = sign_and_send(
        &url,
        TARGET_DEPLOYER,
        if l1 { DEV_CHAIN_ID } else { w.l2_chain_id },
        onchain_nonce(&url, TARGET_DEPLOYER).await?,
        None,
        U256::ZERO,
        hex::decode(
            artifact["bytecode"]["object"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )?,
        2_000_000,
    )
    .await?;
    let r = receipt(&url, hash).await?;
    ensure!(r["status"] == "0x1", "fixture deployment failed");
    if !l1 {
        settle(w, uint(&r["blockNumber"])?.to()).await?;
    }
    Ok(r["contractAddress"].as_str().unwrap().parse()?)
}

async fn rejected(w: &CrossChainWorld, inbound: bool, proxy: Address, value: U256) -> Result<()> {
    let (url, source, key, chain) = if inbound {
        (w.l1_xchain(), w.l1_rpc(), INBOUND_USER, DEV_CHAIN_ID)
    } else {
        (w.l2_xchain(), w.l2_rpc(), OUTBOUND_USER, w.l2_chain_id)
    };
    let address = signer_address(key)?;
    let before = l2_balance(&source, address).await?;
    let nonce = onchain_nonce(&source, key).await?;
    let cursor = w.node.signal_cursor()?;
    let hash = sign_and_send(
        &url,
        key,
        chain,
        nonce,
        Some(proxy),
        value,
        failCall {}.abi_encode(),
        900_000,
    )
    .await?;
    wait_for(SETTLE_TIMEOUT, || async {
        Ok(w.node
            .signals_since(cursor)?
            .iter()
            .any(|s| {
                s.name == signals::COMPOSER_POISON_EVICTION_COMPLETED
                    && s.fields.get("tx_hash").and_then(Value::as_str)
                        == Some(hash.to_string().as_str())
            })
            .then_some(()))
    })
    .await?;
    ensure!(
        receipt_ok(&source, hash).await?.is_none(),
        "failed cross-chain call settled"
    );
    ensure!(
        onchain_nonce(&source, key).await? == nonce,
        "rejected call consumed nonce"
    );
    ensure!(
        l2_balance(&source, address).await? == before,
        "rejected call consumed value or fees"
    );
    println!(
        "ACCOUNTING_REJECTED {}",
        json!({"inbound":inbound,"tx":hash,"value":value.to_string(),"nonce":nonce})
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn live_groups_conserve_backing_and_preserve_protocol_donations() -> Result<()> {
    let w = setup_cross_chain().await?;
    let target = deploy(&w, false).await?;
    let l1_target = deploy(&w, true).await?;
    let deposit_proxy = create_cross_chain_proxy(
        &w.l1_rpc(),
        w.cfg.deployer_key,
        w.cfg.eez_address,
        target,
        w.cfg.rollup_id,
    )
    .await?;
    let withdrawal_proxy =
        create_l2_cross_chain_proxy(&w.l2_rpc(), TARGET_DEPLOYER, l1_target, 0).await?;
    // Settle all fixture creation before taking the baseline.
    let latest = uint(&rpc(&w.l2_rpc(), "eth_blockNumber", json!([])).await?)?.to();
    settle(&w, latest).await?;
    let mut audit = Audit::new()?;
    let mut escrow = U256::ZERO;
    audit.checkpoint(&w, "baseline", escrow).await?;

    for (label, to, value, data, success) in [
        (
            "direct_SYSTEM",
            eez_primitives::SYSTEM_ADDRESS,
            12345,
            vec![],
            true,
        ),
        (
            "forced_SYSTEM",
            target,
            6789,
            forceCall {
                target: eez_primitives::SYSTEM_ADDRESS,
            }
            .abi_encode(),
            true,
        ),
        (
            "forced_EEZL2",
            target,
            23456,
            forceCall {
                target: eez_primitives::EEZL2_ADDRESS,
            }
            .abi_encode(),
            true,
        ),
        (
            "direct_EEZL2_reverts",
            eez_primitives::EEZL2_ADDRESS,
            41,
            vec![],
            false,
        ),
        (
            "withdrawal_without_table_reverts",
            withdrawal_proxy,
            43,
            acceptCall {}.abi_encode(),
            false,
        ),
        (
            "caught_withdrawal_without_table",
            target,
            47,
            catchCallCall {
                target: withdrawal_proxy,
            }
            .abi_encode(),
            true,
        ),
    ] {
        let hash = sign_and_send(
            &w.l2_rpc(),
            OUTBOUND_USER,
            w.l2_chain_id,
            onchain_nonce(&w.l2_rpc(), OUTBOUND_USER).await?,
            Some(to),
            U256::from(value),
            data,
            900_000,
        )
        .await?;
        let r = receipt(&w.l2_rpc(), hash).await?;
        ensure!(
            (r["status"] == "0x1") == success,
            "{label}: wrong receipt status"
        );
        settle(&w, uint(&r["blockNumber"])?.to()).await?;
        audit.checkpoint(&w, label, escrow).await?;
    }

    // Keep backing available during failed withdrawals so they actually reach
    // the deliberately reverting L1 target instead of failing for lack of ETH.
    let reserve = U256::from(1_000_000);
    let hash = sign_and_send(
        &w.l1_xchain(),
        INBOUND_USER,
        DEV_CHAIN_ID,
        onchain_nonce(&w.l1_rpc(), INBOUND_USER).await?,
        Some(deposit_proxy),
        reserve,
        acceptCall {}.abi_encode(),
        900_000,
    )
    .await?;
    ensure!(
        receipt(&w.l1_rpc(), hash).await?["status"] == "0x1",
        "reserve deposit reverted"
    );
    escrow += reserve;
    audit.checkpoint(&w, "reserve_deposit", escrow).await?;

    for i in 0..12u64 {
        let value = match i {
            0 => U256::ZERO,
            1 => U256::ONE,
            _ => U256::from(i * 1_000_000_000_007),
        };
        let hash = sign_and_send(
            &w.l1_xchain(),
            INBOUND_USER,
            DEV_CHAIN_ID,
            onchain_nonce(&w.l1_rpc(), INBOUND_USER).await?,
            Some(deposit_proxy),
            value,
            acceptCall {}.abi_encode(),
            900_000,
        )
        .await?;
        ensure!(
            receipt(&w.l1_rpc(), hash).await?["status"] == "0x1",
            "deposit reverted"
        );
        escrow += value;
        wait_for(SETTLE_TIMEOUT, || async {
            Ok(
                (rollup_ether_balance(&w.l1_rpc(), w.cfg.eez_address, w.cfg.rollup_id).await?
                    == escrow)
                    .then_some(()),
            )
        })
        .await?;
        audit
            .checkpoint(&w, &format!("deposit_{i}"), escrow)
            .await?;
        let hash = sign_and_send(
            &w.l2_xchain(),
            OUTBOUND_USER,
            w.l2_chain_id,
            onchain_nonce(&w.l2_rpc(), OUTBOUND_USER).await?,
            Some(withdrawal_proxy),
            value,
            acceptCall {}.abi_encode(),
            900_000,
        )
        .await?;
        audit.withdrawals.insert(hash, value);
        let r = receipt(&w.l2_rpc(), hash).await?;
        ensure!(r["status"] == "0x1", "withdrawal reverted");
        settle(&w, uint(&r["blockNumber"])?.to()).await?;
        escrow -= value;
        audit
            .checkpoint(&w, &format!("withdrawal_{i}"), escrow)
            .await?;
        if i % 3 == 0 {
            for inbound in [true, false] {
                rejected(
                    &w,
                    inbound,
                    if inbound {
                        deposit_proxy
                    } else {
                        withdrawal_proxy
                    },
                    U256::from(1000 + i),
                )
                .await?;
                audit
                    .checkpoint(&w, &format!("rejected_{inbound}_{i}"), escrow)
                    .await?;
            }
        }
    }
    // Queue several groups without waiting between submissions. The state-diff
    // audit checks every individual group even when they share a settled block.
    for wave in 0..3u64 {
        let values: Vec<_> = (0..4u64)
            .map(|i| U256::from((wave + 1) * (i + 1) * 103))
            .collect();
        for inbound in [true, false] {
            let (url, source, key, chain, proxy) = if inbound {
                (
                    w.l1_xchain(),
                    w.l1_rpc(),
                    INBOUND_USER,
                    DEV_CHAIN_ID,
                    deposit_proxy,
                )
            } else {
                (
                    w.l2_xchain(),
                    w.l2_rpc(),
                    OUTBOUND_USER,
                    w.l2_chain_id,
                    withdrawal_proxy,
                )
            };
            let nonce = onchain_nonce(&source, key).await?;
            let mut hashes = Vec::new();
            for (i, value) in values.iter().enumerate() {
                let hash = sign_and_send(
                    &url,
                    key,
                    chain,
                    nonce + i as u64,
                    Some(proxy),
                    *value,
                    acceptCall {}.abi_encode(),
                    900_000,
                )
                .await?;
                if !inbound {
                    audit.withdrawals.insert(hash, *value);
                }
                hashes.push(hash);
            }
            let mut minimum = 0;
            for hash in hashes {
                let r = receipt(&source, hash).await?;
                ensure!(r["status"] == "0x1", "queued group reverted");
                minimum = minimum.max(uint(&r["blockNumber"])?.to::<u64>());
            }
            let sum: U256 = values.iter().copied().sum();
            if inbound {
                escrow += sum;
            } else {
                settle(&w, minimum).await?;
                escrow -= sum;
            }
            audit
                .checkpoint(&w, &format!("wave_{wave}_inbound_{inbound}"), escrow)
                .await?;
        }
    }
    let hash = sign_and_send(
        &w.l2_xchain(),
        OUTBOUND_USER,
        w.l2_chain_id,
        onchain_nonce(&w.l2_rpc(), OUTBOUND_USER).await?,
        Some(withdrawal_proxy),
        reserve,
        acceptCall {}.abi_encode(),
        900_000,
    )
    .await?;
    audit.withdrawals.insert(hash, reserve);
    let r = receipt(&w.l2_rpc(), hash).await?;
    ensure!(r["status"] == "0x1", "reserve withdrawal reverted");
    settle(&w, uint(&r["blockNumber"])?.to()).await?;
    escrow -= reserve;
    audit.checkpoint(&w, "withdraw_all_backing", escrow).await?;

    // A deposit can itself return value to a protocol address. It is still
    // backed on L1 and must survive subsequent groups, just like a donation.
    for address in [
        eez_primitives::SYSTEM_ADDRESS,
        eez_primitives::EEZL2_ADDRESS,
    ] {
        let value = U256::from(101);
        let hash = sign_and_send(
            &w.l1_xchain(),
            INBOUND_USER,
            DEV_CHAIN_ID,
            onchain_nonce(&w.l1_rpc(), INBOUND_USER).await?,
            Some(deposit_proxy),
            value,
            forceCall { target: address }.abi_encode(),
            900_000,
        )
        .await?;
        ensure!(
            receipt(&w.l1_rpc(), hash).await?["status"] == "0x1",
            "forced deposit reverted"
        );
        escrow += value;
        audit
            .checkpoint(&w, &format!("deposit_to_{address}"), escrow)
            .await?;
    }
    ensure!(
        l2_balance(&w.l2_rpc(), eez_primitives::SYSTEM_ADDRESS).await?
            == U256::from(12345 + 6789 + 101),
        "SYSTEM donations lost"
    );
    ensure!(
        l2_balance(&w.l2_rpc(), eez_primitives::EEZL2_ADDRESS).await? == U256::from(23456 + 101),
        "EEZL2 donation lost"
    );
    ensure!(
        l2_balance(&w.l2_rpc(), target).await?
            == U256::from(47) + l2_balance(&w.l1_rpc(), l1_target).await?,
        "successful/reverted target value mismatch"
    );
    for (url, address, expected) in [(w.l2_rpc(), target, 26u64), (w.l1_rpc(), l1_target, 25)] {
        let output = rpc(&url, "eth_call", json!([{"to":address,"data":format!("0x{}", hex::encode(callsCall {}.abi_encode()))},"latest"])).await?;
        ensure!(
            uint(&output)? == U256::from(expected),
            "reverted destination writes leaked at {address}"
        );
    }
    w.node.assert_no_process_death();
    Ok(())
}
