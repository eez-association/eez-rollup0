//! Temporary decoder for the block-1251 postBatch divergence probe.
use alloy_primitives::{B256, hex};
use alloy_sol_types::SolCall;
use eez_protocol::abi::{ExecutionEntrySol, postAndVerifyBatchCall};

fn dump_entry(label: &str, e: &ExecutionEntrySol) {
    println!("== {label} ==");
    println!("  proxyEntryHash = {}", e.proxyEntryHash);
    println!("  destinationRollupId = {}", e.destinationRollupId);
    println!("  stateUpdates.len() = {}", e.stateUpdates.len());
    for (j, d) in e.stateUpdates.iter().enumerate() {
        println!(
            "    update[{j}] rollupId={} currentState={} newState={} etherDelta={}",
            d.rollupId, d.currentState, d.newState, d.etherDelta
        );
    }
    println!("  l2ToL1Calls.len() = {}", e.l2ToL1Calls.len());
    for (j, c) in e.l2ToL1Calls.iter().enumerate() {
        println!(
            "    call[{j}] target={} value={} source={} sourceRollupId={} revertNextNCalls={} isStatic={} gas={} data=0x{}",
            c.targetAddress,
            c.value,
            c.sourceAddress,
            c.sourceRollupId,
            c.revertNextNCalls,
            c.isStatic,
            c.gas,
            hex::encode(&c.data)
        );
    }
    println!(
        "  expectedL1ToL2Calls.len() = {}",
        e.expectedL1ToL2Calls.len()
    );
    println!("  success = {}", e.success);
    println!("  returnData = 0x{}", hex::encode(&e.returnData));
    println!("  rollingHash = {}", e.rollingHash);
    let dir = if e.proxyEntryHash == B256::ZERO && !e.l2ToL1Calls.is_empty() {
        "OUTBOUND immediate"
    } else if e.proxyEntryHash != B256::ZERO && !e.l2ToL1Calls.is_empty() {
        "INBOUND deferred"
    } else if e.proxyEntryHash == B256::ZERO && e.l2ToL1Calls.is_empty() {
        "ANCHOR (settlement-only, signs NO system tx)"
    } else {
        "?? unknown"
    };
    println!("  >>> classified: {dir}");
    println!();
}

fn main() {
    let raw = std::fs::read_to_string("/tmp/batch_calldata_raw.hex").unwrap();
    let raw = raw.trim();
    let bytes = hex::decode(raw).unwrap();
    let decoded = postAndVerifyBatchCall::abi_decode(&bytes).unwrap();
    let b = decoded.batch;

    println!("== BATCH TOP-LEVEL ==");
    println!("entries.len() = {}", b.entries.len());
    println!("immediateEntryCount = {}", b.immediateEntryCount);
    println!(
        "immediateStaticEntryCount = {}",
        b.immediateStaticEntryCount
    );
    println!("staticEntries.len() = {}", b.staticEntries.len());
    println!("callData.len() = {}", b.callData.len());
    println!("proofs.len() = {}", b.proofs.len());
    println!();

    // ── on-chain entries[] (the CLAIMED state-delta chain) ──
    println!("######## ON-CHAIN batch.entries[] (CLAIMED chain) ########");
    for (i, e) in b.entries.iter().enumerate() {
        dump_entry(&format!("ON-CHAIN ENTRY {i}"), e);
    }

    // ── DA payload ──
    let eez_payload_codec::DecodedContainer { span: d, actions } =
        eez_payload_codec::decode_container(&b.callData).unwrap();
    println!("######## DA PAYLOAD ########");
    let from_block = 1u64;
    println!("block_count = {}", d.block_count());
    println!("transactions.len() = {}", d.transactions.len());
    println!("actions.len() = {}", actions.len());
    for (idx, c) in d
        .block_tx_counts
        .iter()
        .enumerate()
        .filter(|(_, c)| **c > 0)
    {
        println!(
            "   block idx {idx} -> L2 block {} : {c} tx(s)",
            from_block + idx as u64
        );
    }
    println!();

    // ── DA actions (what the DERIVER rebuilds entries from) ──
    println!("######## DA actions (DERIVER input) ########");
    let rollup_id = eez_protocol::RollupId(1);
    let entries: Vec<ExecutionEntrySol> = actions
        .iter()
        .enumerate()
        .map(|(i, a)| {
            println!(
                "action[{i}]: {} -> {} | {} -> {} | value 0x{} | data {}B | success {}",
                a.source_rollup_id,
                a.target_rollup_id,
                alloy_primitives::Address::from(a.source_address),
                alloy_primitives::Address::from(a.target_address),
                hex::encode(a.value),
                a.data.len(),
                a.success,
            );
            let e = eez_protocol::entries::manifest::entry_from_action(a, rollup_id).unwrap();
            dump_entry(&format!("DA action[{i}] -> entry"), &e);
            e
        })
        .collect();

    // ── Deriver-style partition over the rebuilt entries ──
    println!("######## DERIVER PARTITION (over rebuilt entries) ########");
    let (outbound, inbound): (Vec<_>, Vec<_>) = entries
        .clone()
        .into_iter()
        .filter(|e| !e.l2ToL1Calls.is_empty())
        .partition(|e| e.proxyEntryHash == B256::ZERO);
    println!(
        "outbound (proxyEntryHash==0, non-empty l2ToL1Calls) = {}",
        outbound.len()
    );
    println!(
        "inbound (proxyEntryHash!=0, non-empty l2ToL1Calls) = {}",
        inbound.len()
    );
    let sync_user_count = d.block_tx_counts.last().map_or(0, |c| *c as usize);
    let user_start = d.transactions.len().saturating_sub(sync_user_count);
    println!("sync_user_count (last block tx count) = {sync_user_count}");
    println!("user_start index = {user_start}");
    for (i, t) in d.transactions[user_start..].iter().enumerate() {
        println!(
            "   sync user tx[{i}] (len {}) = 0x{}",
            t.len(),
            hex::encode(&t[..t.len().min(80)])
        );
    }
    println!();
    println!("######## CLAIMED CHAIN (on-chain entries[] deltas, in order) ########");
    for (i, e) in b.entries.iter().enumerate() {
        for d in &e.stateUpdates {
            println!("entry[{i}]: {} -> {}", d.currentState, d.newState);
        }
    }

    println!();
    println!(
        "######## DA actions rebuilt into entries (what the deriver feeds the builder) ########"
    );
    for (i, e) in entries.iter().enumerate() {
        dump_entry(&format!("DA action[{i}] -> entry"), e);
    }

    println!();
    println!("######## DERIVER SyncPair RECONSTRUCTION (build_cross_chain_sync_pairs) ########");
    {
        use alloy_signer_local::PrivateKeySigner;
        use eez_protocol::system_tx::{
            SystemTxContext, build_cross_chain_sync_pairs, interleave_sync_block_txs,
        };
        // Partition the rebuilt entries exactly like the deriver.
        let (da_outbound, da_inbound): (Vec<_>, Vec<_>) = entries
            .clone()
            .into_iter()
            .filter(|e| !e.l2ToL1Calls.is_empty())
            .partition(|e| e.proxyEntryHash == B256::ZERO);
        // Pair outbound positionally with last-block user txs.
        let suc = d.block_tx_counts.last().map_or(0, |c| *c as usize);
        let us = d.transactions.len().saturating_sub(suc);
        let outbound_paired: Vec<(ExecutionEntrySol, alloy_primitives::Bytes)> = da_outbound
            .iter()
            .cloned()
            .zip(
                d.transactions[us..]
                    .iter()
                    .map(|t| alloy_primitives::Bytes::from(t.clone())),
            )
            .collect();
        // This diagnostic only needs the transaction shape and nonce assignment,
        // so an arbitrary example key is sufficient. The printed load calldata is
        // independent of that key.
        let cfg = SystemTxContext {
            system_signer: PrivateKeySigner::from_bytes(&alloy_primitives::B256::with_last_byte(1))
                .unwrap(),
            eezl2_address: alloy_primitives::address!("4200000000000000000000000000000000000007"),
            l2_chain_id: 1,
            l2_gas_price: 1_000_000_000,
            l2_gas_limit: 1_500_000,
            this_rollup_id: 1,
        };
        for start_nonce in [0u64, 1u64] {
            let pairs =
                build_cross_chain_sync_pairs(&outbound_paired, &da_inbound, &cfg, start_nonce)
                    .unwrap();
            let txs = interleave_sync_block_txs(&pairs);
            println!(
                "-- starting_nonce={start_nonce}: {} sync-block txs --",
                txs.len()
            );
            for (i, t) in txs.iter().enumerate() {
                use alloy_consensus::Transaction as _;
                use alloy_eips::eip2718::Decodable2718 as _;
                let mut s: &[u8] = t.as_ref();
                let stx = TransactionSigned::decode_2718(&mut s).unwrap();
                println!(
                    "   synctx[{i}] nonce={} to={:?} sel=0x{} len={}",
                    stx.nonce(),
                    stx.to(),
                    hex::encode(stx.input().get(..4).unwrap_or(&[])),
                    t.len(),
                );
            }
        }
    }

    println!();
    println!("######## FULL TX DECODE (block-major) ########");
    use alloy_consensus::Transaction as _;
    use alloy_eips::eip2718::Decodable2718 as _;
    use reth_ethereum_primitives::TransactionSigned;
    use reth_primitives_traits::SignerRecoverable as _;
    let from_block = 501u64;
    // map flat tx index -> L2 block
    let mut flat_to_block: Vec<u64> = Vec::new();
    for (bi, c) in d.block_tx_counts.iter().enumerate() {
        for _ in 0..*c {
            flat_to_block.push(from_block + bi as u64);
        }
    }
    for (ti, t) in d.transactions.iter().enumerate() {
        let mut s: &[u8] = t.as_ref();
        match TransactionSigned::decode_2718(&mut s) {
            Ok(stx) => {
                let from = stx
                    .recover_signer()
                    .map(|a| format!("{a}"))
                    .unwrap_or_else(|_| "RECOVER_FAIL".into());
                println!(
                    "tx[{ti}] L2block={} chain_id={:?} nonce={} to={:?} value={} gas={} sel=0x{} from={}",
                    flat_to_block.get(ti).copied().unwrap_or(0),
                    stx.chain_id(),
                    stx.nonce(),
                    stx.to(),
                    stx.value(),
                    stx.gas_limit(),
                    hex::encode(stx.input().get(..4).unwrap_or(&[])),
                    from,
                );
                println!("        input = 0x{}", hex::encode(stx.input()));
            }
            Err(e) => println!("tx[{ti}] decode_2718 FAILED: {e}"),
        }
    }
}
