//! Block traces for batches a contract posted: their bytes, sender and log
//! range come only from a trace, taken by block hash so it matches the logs' fork.

use std::collections::HashMap;
use std::time::Duration;

use alloy_primitives::B256;
use alloy_provider::{Provider, ext::DebugApi};
use alloy_rpc_types_trace::geth::{
    CallConfig, CallFrame, GethDebugTracingOptions, GethTrace, TraceResult,
};
use tracing::{Level, event};

use crate::error::{L1Error, L1Result};

/// JSON-RPC "method not found": the endpoint serves no `debug` namespace.
const METHOD_NOT_FOUND: i64 = -32601;

/// Replaces geth's 5s per-tx default; a large batch traces slower.
const TRACE_TIMEOUT: Duration = Duration::from_mins(1);

/// The callTracer frame, with logs, of each `needed` tx in block `block_hash`,
/// in `needed` order. An endpoint that does not know the block yet lags behind the logs.
pub(crate) async fn trace_block(
    trace: &impl Provider,
    block: u64,
    block_hash: B256,
    needed: &[B256],
) -> L1Result<Vec<CallFrame>> {
    let served = trace.get_block_by_hash(block_hash).await.map_err(|e| {
        L1Error::Provider(format!(
            "trace endpoint get_block_by_hash({block_hash}): {e}"
        ))
    })?;
    if served.is_none() {
        return Err(L1Error::SourceIncomplete {
            block,
            detail: format!("the trace endpoint does not serve block {block_hash} yet; retry"),
        });
    }
    let failed = |detail: String| trace_failed(block, block_hash, detail);
    let results = match trace
        .debug_trace_block_by_hash(block_hash, trace_options())
        .await
    {
        Ok(results) => results,
        Err(e) if e.error_code() == Some(METHOD_NOT_FOUND) => {
            return Err(L1Error::TraceUnavailable {
                block,
                detail: format!(
                    "the trace endpoint has no debug_traceBlockByHash ({e}), and block \
                     {block_hash} holds a batch a contract posted for this rollup; set \
                     EEZ_L1_TRACE_RPC_URL to an L1 node that serves the debug namespace \
                     and keeps state for this block"
                ),
            });
        }
        Err(e) => return Err(failed(format!("debug_traceBlockByHash: {e}"))),
    };
    // Another tx failing to trace says nothing about the posts the scan reads.
    let mut traced = HashMap::new();
    for result in results {
        if let Some(tx) = result.tx_hash().filter(|tx| needed.contains(tx)) {
            traced.insert(tx, result);
        }
    }
    (needed.iter())
        .map(|tx| match traced.remove(tx) {
            Some(TraceResult::Success {
                result: GethTrace::CallTracer(frame),
                ..
            }) => Ok(frame),
            Some(TraceResult::Success { .. }) => Err(failed(format!(
                "tx {tx}: the result is not a callTracer frame"
            ))),
            Some(TraceResult::Error { error, .. }) => {
                Err(failed(format!("tx {tx} failed to trace: {error}")))
            }
            // The trace is of the logs' block, so a missing tx is the endpoint's fault, not lag.
            None => Err(failed(format!("no result for tx {tx}"))),
        })
        .collect()
}

/// callTracer over the whole call tree, with each frame's logs.
fn trace_options() -> GethDebugTracingOptions {
    GethDebugTracingOptions::call_tracer(CallConfig {
        only_top_call: Some(false),
        with_log: Some(true),
    })
    .with_timeout(TRACE_TIMEOUT)
}

/// Logs a failed trace with its remediation and returns it as retryable.
fn trace_failed(block: u64, block_hash: B256, detail: String) -> L1Error {
    event!(
        name: "eez.l1_scan.trace_failed",
        Level::ERROR,
        block,
        block_hash = %block_hash,
        detail = %detail,
        "L1 block trace failed; the trace endpoint may lack state for this block or cap the response (--rpc.max-response-size, trace timeout) — point EEZ_L1_TRACE_RPC_URL at an L1 node that has the state and room",
    );
    L1Error::Provider(format!(
        "trace of L1 block {block} ({block_hash}): {detail}"
    ))
}

#[cfg(test)]
mod tests {
    use super::{trace_block, trace_options};
    use alloy_json_rpc::ErrorPayload;
    use alloy_primitives::{Address, B256};
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;
    use serde_json::{Value, json};

    const BLOCK: u64 = 700;
    const BLOCK_HASH: B256 = B256::repeat_byte(0xB1);
    const NEEDED: B256 = B256::repeat_byte(0xA1);
    const OTHER: B256 = B256::repeat_byte(0xA2);

    /// `eth_getBlockByHash` finding the block; its content is never read.
    fn known_block() -> Value {
        let block: alloy_rpc_types_eth::Block = Default::default();
        serde_json::to_value(block).expect("block json")
    }

    /// A lagging endpoint retries without being asked for a trace; one without
    /// `debug` halts naming the fix; a needed tx's failure retries as
    /// `Provider`, and another tx's is ignored.
    #[tokio::test]
    async fn trace_failures_are_classified() {
        let lagging = "L1 source incomplete at block 700";
        let unavailable = "L1 trace unavailable at block 700";
        let retried = "L1 provider error";
        let traced = |answer| vec![Ok(known_block()), answer];
        let no_debug = ErrorPayload::method_not_found();
        let no_state = ErrorPayload::internal_error_message("no state for block 699".into());
        let failed = |tx: B256| json!({ "error": "execution timeout", "txHash": tx });
        let needed = |result: Value| json!({ "result": result, "txHash": NEEDED });
        let frame = json!({ "type": "CALL", "from": Address::ZERO, "input": "0x" });
        #[rustfmt::skip]
        let cases = [
            (vec![Ok(Value::Null)], lagging, "does not serve"),
            (traced(Err(no_debug)), unavailable, "EEZ_L1_TRACE_RPC_URL"),
            (traced(Err(no_state)), retried, "no state for block 699"),
            (traced(Ok(json!([failed(NEEDED)]))), retried, "execution timeout"),
            (traced(Ok(json!([needed(json!({}))]))), retried, "not a callTracer frame"),
            (traced(Ok(json!([failed(OTHER), needed(frame)]))), "traced 1", ""),
        ];
        for (answers, class, cause) in cases {
            let asserter = Asserter::new();
            for answer in answers {
                match answer {
                    Ok(value) => asserter.push_success(&value),
                    Err(payload) => asserter.push_failure(payload),
                }
            }
            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let got = match trace_block(&provider, BLOCK, BLOCK_HASH, &[NEEDED]).await {
                Ok(frames) => format!("traced {}", frames.len()),
                Err(err) => err.to_string(),
            };
            assert!(got.starts_with(class) && got.contains(cause), "{got}");
        }
    }

    /// The mock cannot see request params, so the wire options are pinned here.
    #[test]
    fn traces_request_the_call_tracer_with_logs_over_the_whole_tree() {
        assert_eq!(
            serde_json::to_value(trace_options()).expect("serializable"),
            json!({
                "tracer": "callTracer",
                "tracerConfig": { "onlyTopCall": false, "withLog": true },
                "timeout": "60000ms"
            })
        );
    }
}
