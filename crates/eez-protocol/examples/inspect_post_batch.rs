//! Decode one or more landed `postAndVerifyBatch` calls for live-network QA.
//!
//! Each argument is a file containing 0x-prefixed transaction input. One JSON
//! object is printed per file; malformed ABI or DA payloads fail the process.
use alloy_primitives::hex;
use alloy_sol_types::SolCall;
use eez_protocol::abi::postAndVerifyBatchCall;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files: Vec<_> = std::env::args().skip(1).collect();
    if files.is_empty() {
        return Err("usage: inspect_post_batch <calldata-file>...".into());
    }

    for file in files {
        let encoded = std::fs::read_to_string(&file)?;
        let bytes = hex::decode(encoded.trim().trim_start_matches("0x"))?;
        let call = postAndVerifyBatchCall::abi_decode(&bytes)?;
        let decoded = eez_payload_codec::decode_container(&call.batch.callData)?;
        let pure_transactions: usize = decoded
            .span
            .block_tx_counts
            .iter()
            .map(|count| *count as usize)
            .sum();

        println!(
            "{}",
            json!({
                "file": file,
                "rollup_id": decoded.rollup_id,
                "block_count": decoded.span.block_count(),
                "pure_transaction_count": pure_transactions,
                "per_block_pure_transaction_counts": decoded.span.block_tx_counts,
                "action_count": decoded.actions.len(),
                "entry_count": call.batch.entries.len(),
                "immediate_entry_count": call.batch.immediateEntryCount.to_string(),
                "static_entry_count": call.batch.staticEntries.len(),
                "proof_count": call.batch.proofs.len(),
                "declared_l1_block": call.batch.blockNumber,
                "call_data_bytes": call.batch.callData.len(),
            })
        );
    }
    Ok(())
}
