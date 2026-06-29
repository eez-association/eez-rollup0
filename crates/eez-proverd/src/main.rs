//! Out-of-process prover daemon — P3 (control-feed client + native-validate
//! window validation).
//!
//! Subscribes to the composer's `control.v1.ControlFeed`, accumulates the
//! per-block [`ControlEvent`]s into a WINDOW (the postBatch unit — the N L2
//! blocks committed in one L1 slot), and closes the window on a SETTLING block
//! (one carrying a `composition`, A.2) or a size bound. On close, when a
//! `--validator-bin` (ZisK's `native-validate`) + `--chain-config` are
//! configured, the daemon stages every block's `block-<n>.rlp` + `witness-<n>.json`
//! into a dir and runs `native-validate <cfg> --dir <dir>` — the same guest-reth
//! stateless re-execution the zkVM runs — then asserts the validator's per-block
//! hashes equal the composer-claimed hashes. This is the verification an ECDSA
//! attester runs BEFORE signing.
//!
//! Window/resume contract: the resume cursor (`from_block`) only advances on a
//! COMPLETED window, so a reconnect replays the in-flight window from its first
//! block (the feed's ring-buffer replay + the strictly-increasing block numbers
//! make this gap-free). Intra-window contiguity (parent_hash + number) is
//! checked as events arrive.
//!
//! NOT YET PORTED (the remaining P3-full — based `bin/eez-prover`): the 9
//! fail-closed settlement gates that consume the window's `composition.post_batch`
//! + the validator's `pair_roots`/`tx_statuses`/`batch_commitment`, the
//! publicInputsHash recompute (`eez-evm`), the ECDSA attestation, and the
//! `control.v1.ProofSink` return path. Without a `--validator-bin` the daemon is
//! a pure feed observer.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use clap::Parser;
use eez_control_rpc::v1::{
    ControlEvent, DispatchRequest, ExecutionWitness, SlotProof, SubscribeRequest, VerifyRange,
    control_feed_client::ControlFeedClient, proof_sink_client::ProofSinkClient,
    prover_dispatch_client::ProverDispatchClient,
};
use eez_evm::entries::decode_postbatch;
use eez_evm::public_inputs::public_inputs_hashes;
use eez_evm::signer::EcdsaProofSigner;
use futures_util::{StreamExt, stream::FuturesUnordered};
use tracing::{error, info, warn};

/// Parse a 32-byte hex `B256` (the vkey arg).
fn parse_b256(s: &str) -> Result<B256, String> {
    s.parse::<B256>().map_err(|e| e.to_string())
}

/// Derive the gate vkey from the attester address: `bytes32(uint160(address))` —
/// the per-rollup `IRollupContract` membership-ticket convention (matches the
/// composer's `Prover::vkey`).
fn vkey_from_address(addr: alloy_primitives::Address) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(addr.as_slice());
    B256::from(bytes)
}

/// Submit a signed attestation to the composer's `ProofSink` (connect-per-submit;
/// settlements are infrequent). The composer fills `batch.proofs[]` with the
/// 65-byte signature and posts the batch to L1. Returns the ack's `accepted`.
async fn submit_slot_proof(url: &str, proof: SlotProof) -> eyre::Result<bool> {
    let mut client = ProofSinkClient::connect(url.to_string())
        .await
        .map_err(|e| eyre::eyre!("ProofSink connect {url}: {e}"))?;
    let ack = client
        .submit_slot_proof(proof)
        .await
        .map_err(|e| eyre::eyre!("SubmitSlotProof: {e}"))?
        .into_inner();
    Ok(ack.accepted)
}

/// Composer-driven dispatch (Phase 3): open a FRESH `ProverDispatch` connection,
/// receive exactly ONE directive (the oldest posted-but-unverified window), and
/// drop the stream. A fresh connection per directive keeps the prover STATELESS:
/// the composer's per-connection `dispatch_loop` re-reads `next_unverified` on
/// every new connection (and its old loop exits via `out_tx.closed()` when this
/// stream drops), so a dropped/failed iteration simply re-receives the SAME
/// oldest-unverified window next time — no directive is ever lost. Returns `None`
/// on any connect/stream error (logged); the caller backs off + retries.
async fn dispatch_one(control_addr: &str, prover_epoch: u64) -> Option<VerifyRange> {
    let mut client = match ProverDispatchClient::connect(control_addr.to_string()).await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "ProverDispatch connect failed");
            return None;
        }
    };
    let mut stream = match client.dispatch(DispatchRequest { prover_epoch }).await {
        Ok(s) => s.into_inner(),
        Err(e) => {
            warn!(error = %e, "ProverDispatch dispatch failed");
            return None;
        }
    };
    match stream.message().await {
        Ok(Some(vr)) => Some(vr),
        Ok(None) => {
            warn!("ProverDispatch stream closed before a directive");
            None
        }
        Err(e) => {
            warn!(error = %e, "ProverDispatch stream error");
            None
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "eez-proverd", about = "EEZ out-of-process prover daemon (P3)")]
struct Args {
    /// Composer control-feed endpoint (gRPC). The ProofSink return path shares
    /// the same endpoint once the attestation is wired.
    #[arg(
        long,
        env = "EEZ_CONTROL_RPC_URL",
        default_value = "http://127.0.0.1:50051"
    )]
    control_addr: String,

    /// Path to the ZisK stateless validator (`native-validate`). When unset,
    /// the daemon only OBSERVES the feed (no stateless re-execution).
    #[arg(long, env = "EEZ_VALIDATOR_BIN")]
    validator_bin: Option<String>,

    /// Path to the L2 chain-config JSON (alloy `ChainConfig`; all forks at 0
    /// for the eez-dev chain). Required alongside `--validator-bin`.
    #[arg(long, env = "EEZ_CHAIN_CONFIG")]
    chain_config: Option<String>,

    /// Scratch dir for the per-window validator inputs.
    #[arg(
        long,
        env = "EEZ_VALIDATOR_WORKDIR",
        default_value = "/tmp/eez-proverd"
    )]
    work_dir: String,

    /// Max L2 blocks per window before forcing a validator pass (a window also
    /// closes on a settling block carrying a composition).
    #[arg(long, env = "EEZ_MAX_WINDOW", default_value_t = 8)]
    max_window: usize,

    /// The proof-system verification key (32-byte hex) the composer's prover
    /// used. REQUIRED to recompute the settlement publicInputsHash; it MUST
    /// match the composer or the cross-check fail-closes. Defaults to zero.
    #[arg(long, env = "EEZ_VKEY", value_parser = parse_b256, default_value_t = B256::ZERO)]
    vkey: B256,

    /// The ECDSA private key (32-byte hex) the prover signs the publicInputsHash
    /// with — the proof system's REGISTERED attester key. When set, the daemon
    /// ATTESTS: on a FULLY-verified settling window it signs the hash and returns
    /// it via `ProofSink`. The gate vkey is DERIVED from this key's address
    /// (overriding `--vkey`), so the two can't drift.
    #[arg(long, env = "EEZ_PROOF_SIGNER_KEY", value_parser = parse_b256)]
    signer_key: Option<B256>,

    /// The `ProofSink` endpoint (gRPC) to submit attestations to. Defaults to the
    /// control-feed endpoint (the same composer).
    #[arg(long, env = "EEZ_PROOF_SINK_URL")]
    proof_sink_url: Option<String>,

    /// The L2 node's HTTP JSON-RPC endpoint, used to DURABLY BACKFILL blocks the
    /// composer's bounded replay ring already evicted (`debug_executionWitness` +
    /// `debug_getRawBlock` against archive state). On a replay gap the prover
    /// reconstructs the missing `[cursor+1 .. live-1]` ControlEvents from here
    /// rather than fast-forwarding past unproven blocks. When unset (`None`), the
    /// daemon keeps the fail-loud, no-backfill behavior — a gap drops the batch
    /// and retries on the next replay (observer mode is unchanged).
    #[arg(long, env = "EEZ_L2_RPC_URL", default_value = "http://127.0.0.1:18688")]
    l2_rpc_url: Option<String>,

    /// Bounded parallelism for archive replay-gap recovery.
    #[arg(long, env = "EEZ_BACKFILL_CONCURRENCY", default_value_t = 1)]
    backfill_concurrency: usize,
}

/// Serialize a control-feed witness to native-validate's `witness.json`
/// (`alloy_rpc_types_debug::ExecutionWitness` shape: four hex-string arrays).
fn witness_to_json(w: &ExecutionWitness) -> String {
    let hexed = |v: &[Vec<u8>]| -> Vec<String> {
        v.iter().map(|b| format!("0x{}", hex::encode(b))).collect()
    };
    serde_json::json!({
        "state": hexed(&w.state),
        "codes": hexed(&w.codes),
        "keys": hexed(&w.keys),
        "headers": hexed(&w.headers),
    })
    .to_string()
}

/// DURABLE BACKFILL — reconstruct the [`ControlEvent`] for block `n` directly
/// from the L2 node's ARCHIVE state when the composer's bounded replay ring
/// already evicted it (a disconnect that outlasted the ring horizon, or a
/// stuck prover whose `posted << tip`). Issues two reth debug JSON-RPC calls:
///
///   - `debug_executionWitness(n)` → the block's minimal execution witness.
///     reth returns `{state,codes,keys,headers}` (`Vec<Bytes>`), the exact
///     structural mirror of the proto `ExecutionWitness` — mapped 1:1.
///   - `debug_getRawBlock(n)` → the consensus RLP (header + body), the same
///     bytes the live witness task RLP-encodes.
///
/// The returned event carries `composition: None`: a backfilled block is
/// interior, non-settling history. It is fed through the IDENTICAL staging
/// path (`validate_window`'s `block-N.rlp` + `witness-N.json`) as a live
/// event, so the on-disk inputs are byte-identical and the contiguity guard
/// (`parent_hash == tip.block_hash && number == tip.number+1`) accepts it.
///
/// VALIDATION-TIME CHECK (not a compile-time guarantee): the composer builds
/// its witness with `ExecutionWitnessMode::Canonical` (the minimized v2
/// format; see `eez-driver/src/witness.rs`). reth's `debug_executionWitness`
/// must serve the SAME mode for the re-executed witness to match what the
/// validator expects. If reth's default differs, pass a mode param here; this
/// is flagged at deploy time, not blocked at compile time.
async fn backfill_block(l2_rpc_url: &str, n: u64) -> eyre::Result<ControlEvent> {
    use alloy_rpc_client::RpcClient;

    let url = l2_rpc_url
        .parse()
        .map_err(|e| eyre::eyre!("invalid --l2-rpc-url {l2_rpc_url:?}: {e}"))?;
    let client = RpcClient::new_http(url);
    // reth debug RPC takes the block number as a hex QUANTITY string.
    let block_hex = format!("0x{n:x}");

    // 1. Execution witness (alloy's ExecutionWitness deserializes the result;
    //    its fields are the structural mirror of the proto type).
    let witness: alloy_rpc_types_debug::ExecutionWitness = client
        .request("debug_executionWitness", (block_hex.clone(),))
        .await
        .map_err(|e| eyre::eyre!("debug_executionWitness({n}): {e}"))?;

    // 2. Raw consensus RLP (header + body) — the SAME bytes the live witness
    //    task ships in `ControlEvent.block`.
    let raw_block: alloy_primitives::Bytes = client
        .request("debug_getRawBlock", (block_hex,))
        .await
        .map_err(|e| eyre::eyre!("debug_getRawBlock({n}): {e}"))?;

    // Re-derive (block_hash, parent_hash, number) from the SAME RLP we ship,
    // so the contiguity guard chains on the bytes the validator re-executes
    // (never a side-channel header field that could disagree with the RLP).
    use alloy_rlp::Decodable as _;
    let block = reth_ethereum_primitives::Block::decode(&mut &raw_block[..])
        .map_err(|e| eyre::eyre!("decode backfilled block {n} RLP: {e}"))?;
    let block_number = block.header.number;
    if block_number != n {
        eyre::bail!("backfilled block number {block_number} != requested {n}");
    }
    let parent_hash = block.header.parent_hash;
    let block_hash = block.header.hash_slow();

    Ok(ControlEvent {
        block_hash: block_hash.to_vec(),
        block_number,
        parent_hash: parent_hash.to_vec(),
        // Interior, non-settling history: no composition (settlement is only
        // ever attested from live settling events, never reconstructed here).
        composition: None,
        witness: Some(ExecutionWitness {
            state: witness.state.into_iter().map(|b| b.to_vec()).collect(),
            codes: witness.codes.into_iter().map(|b| b.to_vec()).collect(),
            keys: witness.keys.into_iter().map(|b| b.to_vec()).collect(),
            headers: witness.headers.into_iter().map(|b| b.to_vec()).collect(),
        }),
        block: raw_block.to_vec(),
    })
}

fn is_transient_backfill_error(err: &eyre::Report) -> bool {
    let msg = format!("{err:?}").to_ascii_lowercase();
    [
        "-96000",
        "read transaction has been timed out",
        "error sending request",
        "connection closed",
        "connection reset",
        "connection refused",
        "broken pipe",
        "operation timed out",
        "request timed out",
        "deadline",
        "temporarily unavailable",
        "transport error",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

async fn backfill_block_with_retries(
    url: Arc<String>,
    n: u64,
    expected: u64,
    got: u64,
) -> eyre::Result<ControlEvent> {
    let mut attempt = 0u32;
    let block_started = Instant::now();
    loop {
        match backfill_block(&url, n).await {
            Ok(ev) => {
                if attempt > 0 {
                    info!(
                        block = n,
                        attempts = attempt + 1,
                        elapsed_secs = block_started.elapsed().as_secs(),
                        "backfill block recovered after archive RPC retries",
                    );
                }
                return Ok(ev);
            }
            Err(e) if is_transient_backfill_error(&e) => {
                attempt += 1;
                let delay_ms = (300u64 << attempt.min(6)).min(30_000);
                warn!(
                    block = n,
                    attempt,
                    elapsed_secs = block_started.elapsed().as_secs(),
                    backfilled = n.saturating_sub(expected),
                    remaining = got.saturating_sub(n),
                    delay_ms,
                    error = %e,
                    "backfill block failed — retaining fetched history and retrying transient L2-archive error",
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn backfill_task(
    url: Arc<String>,
    n: u64,
    expected: u64,
    got: u64,
) -> (u64, eyre::Result<ControlEvent>) {
    let result = backfill_block_with_retries(url, n, expected, got).await;
    (n, result)
}

async fn backfill_gap_events(
    url: &str,
    expected: u64,
    got: u64,
    prev_chunk_last_hash: Option<&[u8]>,
    concurrency: usize,
) -> eyre::Result<Vec<ControlEvent>> {
    let concurrency = concurrency.clamp(1, 64);
    let total = got.saturating_sub(expected);
    let url = Arc::new(url.to_owned());
    let mut in_flight = FuturesUnordered::new();
    let mut ready: BTreeMap<u64, ControlEvent> = BTreeMap::new();
    let mut events: Vec<ControlEvent> = Vec::new();
    let mut next_to_fetch = expected;
    let mut next_to_emit = expected;

    while next_to_fetch < got && in_flight.len() < concurrency {
        let url = Arc::clone(&url);
        let n = next_to_fetch;
        in_flight.push(backfill_task(url, n, expected, got));
        next_to_fetch += 1;
    }

    while next_to_emit < got {
        let Some((n, result)) = in_flight.next().await else {
            eyre::bail!("backfill worker set ended before block {next_to_emit}");
        };
        let ev = result.map_err(|e| {
            eyre::eyre!("backfill FAILED on non-transient L2-archive error at block {n}: {e}")
        })?;
        ready.insert(n, ev);

        while let Some(ev) = ready.remove(&next_to_emit) {
            if next_to_emit == expected {
                if let Some(prev_hash) = prev_chunk_last_hash {
                    if ev.parent_hash != prev_hash {
                        eyre::bail!(
                            "cross-chunk block-hash break: first backfilled block's parent_hash != previous chunk's last block hash at block {next_to_emit}"
                        );
                    }
                }
            }
            if let Some(tip) = events.last() {
                if ev.parent_hash != tip.block_hash || ev.block_number != tip.block_number + 1 {
                    eyre::bail!(
                        "backfilled block {next_to_emit} does not extend previous backfilled tip"
                    );
                }
            }
            events.push(ev);
            let done = next_to_emit.saturating_sub(expected) + 1;
            if done == 1 || done % 100 == 0 || next_to_emit + 1 == got {
                info!(
                    from = expected,
                    to = got - 1,
                    current = next_to_emit,
                    backfilled = done,
                    total,
                    remaining = total.saturating_sub(done),
                    concurrency,
                    "backfill progress",
                );
            }
            next_to_emit += 1;

            while next_to_fetch < got && in_flight.len() < concurrency {
                let url = Arc::clone(&url);
                let n = next_to_fetch;
                in_flight.push(backfill_task(url, n, expected, got));
                next_to_fetch += 1;
            }
        }
    }

    Ok(events)
}

/// The RE-EXECUTED facts native-validate returns for a window — what the
/// settlement-chain gate checks the composer's CLAIMED batch against. `sync_*`
/// are the last (Sync) block's per-tx data.
struct VerifiedWindow {
    parent_state_root: B256,
    final_state_root: B256,
    /// Every window block's PROVEN post-state root (re-executed `state_root`),
    /// in block order. Recognizes inter-block settlement boundaries the two
    /// top-level roots miss — chiefly the no-op leading-immediate entry's
    /// `newState = state(sync_block-1)` when the settling chunk carries more
    /// than just the Sync block. `None` if the validator omits the field (an
    /// older binary) — the interior gate then degrades to parent/final/per-tx
    /// recognition + the placeholder path (fail-closed, never falsely accepts).
    per_block_roots: Option<Vec<B256>>,
    /// The Sync (last) block's per-tx re-executed roots (`pair_roots`), for the
    /// interior-boundaries gate. `None` if the validator omits them.
    sync_per_tx_roots: Option<Vec<B256>>,
    /// The Sync block's per-tx re-executed receipt statuses, for the
    /// reverted-system-tx (#10) gate. `None` if the validator omits them.
    sync_tx_statuses: Option<Vec<bool>>,
}

/// Stage the window's blocks + witnesses, run `native-validate --dir`, check
/// each verified block hash matches the composer-claimed one, and return the
/// re-executed roots/per-tx data. Errors = the validator rejected the window
/// (a real attester would refuse to sign).
async fn validate_window(
    window: &[ControlEvent],
    validator_bin: &str,
    chain_config: &str,
    work_dir: &str,
) -> eyre::Result<VerifiedWindow> {
    let from = window.first().map_or(0, |e| e.block_number);
    let to = window.last().map_or(0, |e| e.block_number);

    let dir = Path::new(work_dir).join(format!("{from}-{to}"));
    tokio::fs::create_dir_all(&dir).await?;
    // Timing: split STAGING (writing block+witness files; grows with witness
    // size) from the native-validate SUBPROCESS (the suspected bottleneck), so
    // the per-window trace shows where the wall-clock goes.
    let t_stage = Instant::now();
    let mut staged_bytes: usize = 0;
    for event in window {
        let n = event.block_number;
        let witness = event
            .witness
            .as_ref()
            .ok_or_else(|| eyre::eyre!("control event #{n} carries no witness"))?;
        let wjson = witness_to_json(witness);
        staged_bytes += event.block.len() + wjson.len();
        tokio::fs::write(dir.join(format!("block-{n}.rlp")), &event.block).await?;
        tokio::fs::write(dir.join(format!("witness-{n}.json")), wjson).await?;
    }
    let stage_ms = t_stage.elapsed().as_millis();

    let t_exec = Instant::now();
    let out = tokio::process::Command::new(validator_bin)
        .arg(chain_config)
        .arg("--dir")
        .arg(&dir)
        .output()
        .await
        .map_err(|e| eyre::eyre!("spawn {validator_bin}: {e}"))?;
    let exec_ms = t_exec.elapsed().as_millis();
    info!(
        from,
        to,
        blocks = window.len(),
        stage_ms,
        exec_ms,
        staged_kb = staged_bytes / 1024,
        accepted = out.status.success(),
        "native-validate ran",
    );
    if !out.status.success() {
        eyre::bail!(
            "native-validate rejected window {from}-{to} (exit {:?}): {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }

    // Verified public inputs — native-validate prints the JSON summary as the
    // LAST stdout line; tolerate any leading log line (reth components write to
    // stdout) by taking the last line that begins with '{'.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json_line = stdout
        .lines()
        .rev()
        .find(|l| l.trim_start().starts_with('{'))
        .ok_or_else(|| eyre::eyre!("native-validate produced no JSON summary line"))?;
    let summary: serde_json::Value =
        serde_json::from_str(json_line).map_err(|e| eyre::eyre!("parse validator JSON: {e}"))?;
    // The validator must return exactly the blocks we staged — makes the
    // positional hash pairing below explicit (the contiguity guard orders them).
    let n_validated = summary["blocks"].as_array().map_or(0, Vec::len);
    if n_validated != window.len() {
        eyre::bail!(
            "validator returned {n_validated} blocks, window has {}",
            window.len()
        );
    }

    // Each validator block hash MUST match the hash the composer claimed for
    // that event — otherwise the feed lied about which block this witness proves.
    for (i, event) in window.iter().enumerate() {
        let validated = summary["blocks"][i]["hash"].as_str().unwrap_or("");
        let claimed = format!("0x{}", hex::encode(&event.block_hash));
        if validated != claimed {
            eyre::bail!(
                "window block #{} hash mismatch: validator {validated} != claimed {claimed}",
                event.block_number
            );
        }
    }

    // Parse the RE-EXECUTED roots + the Sync (last) block's per-tx data — the
    // PROVEN facts the settlement-chain gate checks the composer's batch against.
    let parse_root = |key: &str| -> eyre::Result<B256> {
        summary[key]
            .as_str()
            .ok_or_else(|| eyre::eyre!("validator output: missing {key}"))?
            .parse::<B256>()
            .map_err(|e| eyre::eyre!("validator output: bad {key}: {e}"))
    };
    let parent_state_root = parse_root("parent_state_root")?;
    let final_state_root = parse_root("final_state_root")?;
    // Every block's PROVEN post-state root, in block order — the inter-block
    // settlement boundaries (the no-op immediate's `state(sync_block-1)`) the
    // two top-level roots can't express. Present only if EVERY block carries a
    // parseable `state_root` (an all-or-nothing collect → `None` on any miss),
    // so a partial / older-binary output degrades cleanly rather than admitting
    // a half-populated set. `None` ⇒ the interior gate falls back to
    // parent/final/per-tx + placeholder recognition.
    let per_block_roots: Option<Vec<B256>> = summary["blocks"].as_array().and_then(|arr| {
        arr.iter()
            .map(|b| {
                b["state_root"]
                    .as_str()
                    .and_then(|s| s.parse::<B256>().ok())
            })
            .collect::<Option<Vec<_>>>()
    });
    // The Sync (last) block's per-tx re-executed data (re-derived, never composer
    // claims), for the interior + reverted-system-tx gates.
    let last_block = &summary["blocks"][window.len() - 1];
    let sync_per_tx_roots = last_block["pair_roots"].as_array().map(|arr| {
        arr.iter()
            .filter_map(serde_json::Value::as_str)
            .filter_map(|s| s.parse::<B256>().ok())
            .collect::<Vec<_>>()
    });
    let sync_tx_statuses = last_block["tx_statuses"].as_array().map(|arr| {
        arr.iter()
            .filter_map(serde_json::Value::as_bool)
            .collect::<Vec<_>>()
    });
    let settling = window.iter().any(|e| e.composition.is_some());
    info!(
        from,
        to,
        blocks = window.len(),
        settling,
        %final_state_root,
        %parent_state_root,
        "✓ native-validate accepted window (stateless re-execution OK)",
    );
    if let Err(e) = tokio::fs::remove_dir_all(&dir).await {
        warn!(
            from,
            to,
            error = %e,
            "validated window workdir cleanup failed",
        );
    }
    Ok(VerifiedWindow {
        parent_state_root,
        final_state_root,
        per_block_roots,
        sync_per_tx_roots,
        sync_tx_statuses,
    })
}

/// Settlement gate (P3-full step 2a): decode the settling block's PostBatch and
/// recompute its `publicInputsHash` BYTE-FOR-BYTE (`decode_postbatch` →
/// `public_inputs_hashes`), cross-checking the composer's claimed hash. This is
/// exactly what an attester recomputes before signing; any mismatch — a tampered
/// claim, the wrong vkey, or a malformed block-context binding — fail-closes.
/// Returns the recomputed hash on agreement.
///
/// Fail-closed on the two assumptions of the current single-rollup/timeless
/// deployment, so a config change can't silently widen the trust surface:
/// - SINGLE proof system: the wire carries ONE publicInputsHash, so a multi-PS
///   batch (each PS rebuilds its own hash on-chain) would leave PS[1..]
///   unconstrained. Refuse until the wire carries the full vector.
/// - TIMELESS (blockNumber 0): the bound arm folds a composer-supplied L1
///   blockhash with NO independent L1 oracle here to check it. Refuse until
///   such an oracle exists.
fn verify_settlement_public_inputs(
    pb: &eez_control_rpc::v1::PostBatch,
    vkey: B256,
) -> eyre::Result<B256> {
    let batch = decode_postbatch(&pb.abi_calldata)
        .map_err(|e| eyre::eyre!("decode postBatch calldata: {e}"))?;

    let n_ps = batch.inner.proofSystems.len();
    if n_ps != 1 {
        eyre::bail!("settlement has {n_ps} proof systems; this gate verifies a single PS only");
    }
    if batch.inner.blockNumber != 0 {
        eyre::bail!(
            "settlement batch blockNumber={} is BOUND; only timeless (0) is verifiable without an L1 oracle",
            batch.inner.blockNumber
        );
    }

    // The L1 binding the composer claimed: must be empty for a timeless batch
    // (public_inputs_hashes also rejects a (0, Some) pairing).
    let l1_block_hash = match pb.l1_block_hash.len() {
        0 => None,
        32 => Some(B256::from_slice(&pb.l1_block_hash)),
        n => eyre::bail!("l1_block_hash must be 0 or 32 bytes, got {n}"),
    };

    let hashes = public_inputs_hashes(&batch, vkey, l1_block_hash)
        .map_err(|e| eyre::eyre!("recompute publicInputsHash: {e:?}"))?;
    let recomputed = match hashes.as_slice() {
        [h] => *h,
        other => eyre::bail!("expected exactly 1 per-PS hash, got {}", other.len()),
    };

    if pb.public_inputs_hash.len() != 32 {
        eyre::bail!(
            "composer publicInputsHash must be 32 bytes, got {}",
            pb.public_inputs_hash.len()
        );
    }
    let claimed = B256::from_slice(&pb.public_inputs_hash);
    if recomputed != claimed {
        eyre::bail!(
            "publicInputsHash mismatch: recomputed {recomputed} != composer-claimed {claimed}"
        );
    }
    Ok(recomputed)
}

/// Per-tx SYSTEM flags of a block, re-derived from its OWN RLP (signer ==
/// `SYSTEM_ADDRESS` && target == the EEZL2 predeploy). Shared by the pair-end
/// classification + the reverted-system-tx (#10) gate. Undecodable RLP → empty
/// (an empty block then passes both gates vacuously).
///
/// The target is `EEZL2_ADDR` (0x42..07, the real on-chain EEZL2 = `ccm_l2_address`
/// in every deployment), NOT the stale `eez_evm::CCM_ADDRESS` (0xeeee..). Using
/// 0xeeee misclassified every system tx as a user tx, silently defeating the
/// interior-boundary + reverted-system-tx gates (they re-derive system flags from
/// this) — the same stale-address footgun fixed in `outbound_user_txs_from_block`.
fn system_tx_flags_from_rlp(block_rlp: &[u8]) -> Vec<bool> {
    use alloy_consensus::Transaction as _;
    use alloy_rlp::Decodable as _;
    use reth_primitives_traits::SignerRecoverable as _;

    let Ok(block) = reth_ethereum_primitives::Block::decode(&mut &block_rlp[..]) else {
        return Vec::new();
    };
    block
        .body
        .transactions
        .iter()
        .map(|tx| {
            tx.recover_signer().is_ok_and(|signer| {
                signer == eez_evm::SYSTEM_ADDRESS
                    && tx.to() == Some(eez_evm::outbound_gate::EEZL2_ADDR)
            })
        })
        .collect()
}

/// Pair-end classification over per-tx system flags: position `i` ends a pair
/// iff tx `i` is a USER tx, or a SYSTEM tx NOT followed by a user tx (a
/// standalone inbound delivery).
fn pair_end_positions(is_system: &[bool]) -> Vec<usize> {
    (0..is_system.len())
        .filter(|&i| !is_system[i] || is_system.get(i + 1).copied().unwrap_or(true))
        .collect()
}

/// (#10) `true` iff no SYSTEM tx in the block REVERTED, per the validator's
/// re-executed receipt statuses. A reverted-but-sealed system tx passes every
/// calldata-derived gate vacuously (the inbound result is read from CALLDATA),
/// so it must be caught here. `statuses = None` (older validator) skips with a
/// warning; a status count shorter than the tx count refuses (drift).
fn system_txs_succeeded(system_flags: &[bool], statuses: Option<&[bool]>) -> bool {
    let Some(statuses) = statuses else {
        warn!("validator emitted no per-tx statuses — the reverted-system-tx gate is SKIPPED");
        return true;
    };
    if statuses.len() < system_flags.len() {
        warn!(
            flags = system_flags.len(),
            statuses = statuses.len(),
            "per-tx statuses shorter than the block's tx count — refusing (count drift)"
        );
        return false;
    }
    system_flags
        .iter()
        .zip(statuses)
        .all(|(sys, ok)| !*sys || *ok)
}

/// Re-derive ALL inbound L1→L2 calls (`DecodedInbound`: call args + returnData +
/// success) from a sealed block's `executeIncomingCrossChainCall` system txs.
/// Every field is REAL, not a composer claim: the block only sealed because
/// `EEZL2` bound the call args into the entry `proxyEntryHash` AND checked the
/// entry `rollingHash` against the real `(success, returnData)`. Returns ALL of
/// them — `multi_inbound_outcome_gate` (GAP-3) binds the FULL set to the batch's
/// deferred entries under a strict N:N bijection (every sealed delivery consumes
/// a DISTINCT deferred carrier, in canonical consumption order, no phantom /
/// double-match / unmatched). A `find_map` (first-only) would leave every
/// delivery past the first completely ungated, so the gate consumes the whole
/// vector.
fn extract_inbounds(block_rlp: &[u8]) -> Vec<eez_evm::entries::DecodedInbound> {
    use alloy_consensus::Transaction as _;
    use alloy_rlp::Decodable as _;

    let Ok(block) = reth_ethereum_primitives::Block::decode(&mut &block_rlp[..]) else {
        return Vec::new();
    };
    block
        .body
        .transactions
        .iter()
        .filter_map(|tx| eez_evm::entries::decode_inbound(tx.input()))
        .collect()
}

/// Multi-delivery inbound bijection gate (GAP-3): the N:N inbound outcome gate —
/// the anti-equivocation close "L2 ran EXACTLY the inbounds L1 settled" for a
/// window sealing `K >= 0` inbound executions against the settling batch's
/// deferred inbound entries + failed lookup calls. Subsumes the prior
/// single-delivery gate (K=1 is just the 1:1 case of the bijection); the
/// per-pair check (deferred `proxyEntryHash == H` AND `returnData == d.return_data`
/// for success; a failed `LookupCall` with matching `crossChainCallHash` + bytes
/// for failure) is identical to the old single gate, now applied N times under a
/// strict cardinality + distinctness bijection.
///
/// Returns `Ok(())` iff there is a TOTAL BIJECTION between the sealed inbounds
/// and the batch's consumed inbound carriers:
///   - every SUCCESS sealed inbound matches a DISTINCT deferred entry
///     (`proxyEntryHash == H_i` AND `returnData == d_i.return_data`);
///   - every FAILURE sealed inbound matches a DISTINCT failed `LookupCall`
///     (`failed && crossChainCallHash == H_i && returnData == d_i.return_data`);
///   - NO phantom carrier (a deferred entry / a failed lookup with no sealed
///     inbound), NO double-match, NO unmatched sealed inbound.
/// `H_i = cross_chain_call_hash(settled_rollup, target, value, data, source,
/// MAINNET=0)` — the same on-chain hash the user's proxy computes.
///
/// CONSUMPTION ORDER. The deferred entries appear in the batch in the SAME order
/// the composer drained the inbound user txs (`composer.rs`: `pending_in`), and
/// `build_inbound_system_txs` (`eez-evm/system_tx.rs`) seals the
/// `executeIncomingCrossChainCall` deliveries in that SAME order into the Sync
/// block — so the i-th sealed success inbound (window/block order) pairs
/// POSITIONALLY with the i-th deferred entry (batch order, `proxyEntryHash != 0`).
/// This is exactly the deriver's canonical consumption order
/// (`deriver.rs` reconcile → `build_cross_chain_sync_pairs`), so the prover's
/// matching mirrors what L1 consumes. DUPLICATE identical calls (same H) are
/// handled correctly: two legitimately-identical inbounds map to two distinct
/// deferred entries CONSUMED IN ORDER (positional), not by hash-set membership.
/// Failures (dormant in eez0 — reverting inbounds are poison-evicted at compose)
/// are matched by consuming a distinct unconsumed failed lookup per failed
/// inbound; the path is complete + ready for deliver-as-failed.
///
/// Fail-closed on ANY mismatch (cardinality, per-pair hash/bytes, leftover
/// carrier). `settled_rollup` is read from `entries[0]`'s settlement StateDelta
/// (the single-rollup chain anchor); its absence refuses.
///
/// PARTIAL CONSUMPTION (auditor gap #1) is NOT a concern at THIS layer. The
/// strict `success.len() == deferred.len()` cardinality is correct here because
/// the composer assembles the postBatch's deferred entries and the Sync block's
/// `executeIncomingCrossChainCall` deliveries from the SAME survivor set in ONE
/// `compose_sync_slot` (`build_cross_chain_sync_pairs` + the postBatch share
/// `pending_in`), and caps each bundle at `MAX_USER_TXS_PER_BUNDLE = 3` so a
/// bundle is 100% atomic (a backlog spills to the next slot, never a partial
/// post). So the window the prover RE-EXECUTES always seals exactly M deliveries
/// for M deferred entries — an M:M bijection by construction. L1-runtime partial
/// consumption (rbuilder including only a prefix of a bundle's user txs) is a
/// DERIVER concern: the deriver truncates the inbound FIFO to L1's authoritative
/// `ExecutionConsumed` count (`deriver.rs`: `inbound_deferred.truncate`). The
/// prover deliberately has NO L1 view (`ControlEvent`/`PostBatch` carry no
/// consume signal) and attests the composer's well-formed full batch BEFORE it
/// lands; binding it to a not-yet-known `consumed_count` would require either an
/// L1 view (breaks the stateless-re-execution model) or carrying a runtime fact
/// the composer can't know at compose time — so the strict M:M is the correct,
/// sound gate here.
fn multi_inbound_outcome_gate(
    batch: &eez_evm::EvmBatch,
    sealed: &[eez_evm::entries::DecodedInbound],
) -> Result<(), String> {
    use eez_protocol::RollupId;
    let entries = &batch.inner.entries;
    let lookups = &batch.inner.l1ToL2lookupCalls;

    // The settled rollup anchors every H. entries[0] is the leading-immediate
    // entry carrying the settlement StateDelta (rollupId == our L2). Without it
    // we can't bind any H → refuse.
    let settled_rollup = entries
        .first()
        .and_then(|e| e.stateDeltas.first())
        .map(|delta| RollupId(delta.rollupId.to::<u64>()))
        .ok_or_else(|| {
            "multi-inbound gate REFUSE: entry[0] carries no settlement StateDelta — cannot bind H"
                .to_string()
        })?;

    let hash_of = |d: &eez_evm::entries::DecodedInbound| -> B256 {
        eez_evm::cross_chain_call_hash(
            settled_rollup,
            d.target,
            d.value,
            &d.data,
            d.source,
            RollupId(0), // MAINNET — the L1 consume's source rollup
        )
    };

    // The batch's DEFERRED inbound entries, in BATCH ORDER (the composer's drain
    // order). A deferred inbound entry is `proxyEntryHash != 0` — this EXCLUDES
    // the leading-immediate entry AND the outbound settlement entries (both
    // `proxyEntryHash == 0`), so a mixed inbound+outbound batch gates the inbound
    // half only. (An outbound entry has non-empty l2ToL1Calls but proxyEntryHash
    // 0; the partition mirrors the deriver's `partition(|e| proxyEntryHash == 0)`.)
    let deferred: Vec<&eez_evm::types::ExecutionEntrySol> = entries
        .iter()
        .filter(|e| e.proxyEntryHash != B256::ZERO)
        .collect();

    // SUCCESS sealed inbounds in window/block order — positional bijection to the
    // deferred entries. FAILURE sealed inbounds — bijection to failed lookups.
    let (success, failure): (Vec<&_>, Vec<&_>) = sealed.iter().partition(|d| d.success);

    // ── SUCCESS half: positional N:N bijection to the deferred entries ──
    // Cardinality: every deferred entry consumed exactly once, every success
    // sealed inbound matched exactly once. A count mismatch is a phantom deferred
    // entry (L1 settles a delivery L2 never ran) OR an unmatched sealed inbound
    // (L2 ran a delivery L1 doesn't settle) — either way an equivocation.
    if success.len() != deferred.len() {
        return Err(format!(
            "multi-inbound gate REFUSE: {} success sealed inbounds but {} deferred entries (no bijection: phantom entry or unmatched delivery)",
            success.len(),
            deferred.len(),
        ));
    }
    for (i, (d, entry)) in success.iter().zip(deferred.iter()).enumerate() {
        let h = hash_of(d);
        if entry.proxyEntryHash != h {
            return Err(format!(
                "multi-inbound gate REFUSE: success inbound #{i} hash {h} != deferred entry #{i} proxyEntryHash {} (consumption order / forged hash)",
                entry.proxyEntryHash,
            ));
        }
        if entry.returnData != d.return_data {
            return Err(format!(
                "multi-inbound gate REFUSE: success inbound #{i} returnData != deferred entry #{i} returnData (delivers X on L2, settles Y on L1)",
            ));
        }
    }

    // ── FAILURE half: each failed inbound consumes a DISTINCT failed lookup ──
    // Dormant today (no sealed failures), but kept rigorous: consume failed
    // lookups by index so duplicate-identical failures map to distinct lookups
    // and a phantom failed lookup (no sealed failure) is caught by the leftover
    // check below.
    let mut consumed_lookup = vec![false; lookups.len()];
    for (i, d) in failure.iter().enumerate() {
        let h = hash_of(d);
        let pos = lookups.iter().enumerate().position(|(j, l)| {
            !consumed_lookup[j]
                && l.failed
                && l.crossChainCallHash == h
                && l.returnData == d.return_data
        });
        match pos {
            Some(j) => consumed_lookup[j] = true,
            None => {
                return Err(format!(
                    "multi-inbound gate REFUSE: failure inbound #{i} (hash {h}) matches NO distinct unconsumed failed lookup",
                ));
            }
        }
    }
    // No PHANTOM failed lookup: every `failed` lookup must back a sealed failure.
    // (Non-failed lookups are unrelated — only `failed` ones assert a revert the
    // user would consume.) A leftover failed lookup means L1 would revert the
    // user's consume on a call L2 never failed → reject.
    for (j, l) in lookups.iter().enumerate() {
        if l.failed && !consumed_lookup[j] {
            return Err(format!(
                "multi-inbound gate REFUSE: failed lookup #{j} (hash {}) has NO sealed failure inbound (phantom revert)",
                l.crossChainCallHash,
            ));
        }
    }

    Ok(())
}

/// The raw 2718-encoded USER txs of a sealed L2 block, in order — the Sync
/// block's outbound `executeCrossChainCall` users. A USER tx is any tx that is
/// NOT a SYSTEM tx (signer == `SYSTEM_ADDRESS` && to == the CCM); the
/// interleaved Sync block is `[load(sys), user, …, delivery(sys), …]`, so its
/// non-system txs are exactly the outbound users the i-th outbound entry pairs
/// with. Undecodable RLP → empty (the gate then no-ops / fails closed on a
/// phantom). Mirrors the deriver's `decoded.transactions[user_start..]` pairing.
fn outbound_user_txs_from_block(block_rlp: &[u8]) -> Vec<alloy_primitives::Bytes> {
    use alloy_consensus::Transaction as _;
    use alloy_eips::eip2718::Encodable2718 as _;
    use alloy_rlp::Decodable as _;
    use reth_primitives_traits::SignerRecoverable as _;

    let Ok(block) = reth_ethereum_primitives::Block::decode(&mut &block_rlp[..]) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for tx in &block.body.transactions {
        // A SYSTEM tx targets the EEZL2 predeploy (loadExecutionTable /
        // executeIncomingCrossChainCall) signed by SYSTEM_ADDRESS. NOTE: the
        // target is EEZL2_ADDR (0x42..07, the real on-chain EEZL2 = the gate's
        // CREATE2 deployer), NOT the stale `eez_evm::CCM_ADDRESS` (0xeeee..) — the
        // system txs are sent to `ccm_l2_address` which resolves to 0x42..07 in
        // every deployment (genesis predeploy). Using 0xeeee would misclassify
        // every system tx as a user tx and break the outbound pairing.
        let is_system = tx.recover_signer().is_ok_and(|signer| {
            signer == eez_evm::SYSTEM_ADDRESS && tx.to() == Some(eez_evm::outbound_gate::EEZL2_ADDR)
        });
        if is_system {
            continue;
        }
        let mut buf = Vec::new();
        tx.encode_2718(&mut buf);
        out.push(alloy_primitives::Bytes::from(buf));
    }
    out
}

/// Outbound authorization gate (A3): every outbound L2->L1 settlement entry must
/// be authorized by its paired, SIGNED Sync-block user tx (signer / value / data
/// / proxy-target binds — a composer can't forge the ECDSA signature). Thin
/// wrapper over the SHARED `eez_evm::outbound_gate::verify_outbound_authorized`
/// — the SAME check the deriver (A4) runs against the same DA tx-list, so the
/// prover and the follower can never drift. `l2_rollup_id` is the rollup the
/// settlement StateDelta advances (the batch's authoritative L2 id). See that
/// module for the full soundness note (incl. why the old log model was wrong).
fn verify_outbound_authorized(
    batch: &eez_evm::EvmBatch,
    user_txs: &[alloy_primitives::Bytes],
    l2_rollup_id: u64,
) -> eyre::Result<()> {
    // The outbound immediates only (proxyEntryHash == 0, non-empty calls), in DA
    // order — the SAME partition the deriver pairs (deriver.rs reconcile).
    let outbound: Vec<eez_evm::types::ExecutionEntrySol> = batch
        .inner
        .entries
        .iter()
        .filter(|e| e.proxyEntryHash == B256::ZERO && !e.l2ToL1Calls.is_empty())
        .cloned()
        .collect();
    eez_evm::outbound_gate::verify_outbound_authorized(&outbound, user_txs, l2_rollup_id)
        .map_err(|e| eyre::eyre!("A3: {e}"))
}

/// Settlement-chain gate (P3-full step 2b): the composer's CLAIMED StateDelta
/// chain (Position B — exactly one delta per entry) must telescope from the
/// RE-EXECUTED BATCH-ANCHOR root to the RE-EXECUTED final root:
/// - endpoint-match: `last.newState == final_state_root` (the proven R_N of the
///   SETTLING window — `state(sync_block)`);
/// - parent-anchor (OD-5): `first.currentState == batch_anchor_root` (the proven
///   R0 = `state(posted)`). The composer posts ONE batch covering
///   `[posted+1 .. sync_block]` anchored at `state(posted)`, but the prover
///   streams it as `max_window`-sized chunks. When the batch span exceeds
///   `max_window` the SETTLING chunk's local `parent_state_root` is a LATER
///   chunk's parent (`state(posted + k·max_window)`), NOT `state(posted)` — so
///   OD-5 MUST compare against the telescoped `batch_anchor_root` (the FIRST
///   chunk's re-executed parent, threaded through the window loop), never the
///   settling chunk's local parent. Both are RE-EXECUTED facts (`vw.*`), never a
///   composer claim;
/// - telescoping: every adjacent delta chains (`e_k.newState == e_{k+1}.currentState`);
/// - single-rollup: one `rollupId` across the chain;
/// - interior boundaries: each interior is legitimate iff it is one of
///   (a) a recomputable placeholder (`interim_interior_root` — telescopes,
///   never a real root; used for intra-tx / wide-batch boundaries that have no
///   real on-chain root), (b) a PROVEN RE-EXECUTED root — the no-op leading
///   immediate's `newState` (= `state(sync_block-1)`, the re-executed parent
///   of the Sync block) and the collapsed inbound-deferred boundaries (every
///   inbound delivery lands in the ONE Sync block, so the deferred chain
///   telescopes through `state(sync_block) == final_state_root` after the first
///   consume). For a single-block settling window `state(sync_block-1) ==
///   parent_state_root`, so BOTH legitimate proven boundaries are exactly
///   `vw.parent_state_root` / `vw.final_state_root` — which are also the chain
///   endpoints `r0`/`rn`. This is why a boundary that equals an endpoint is NOT
///   a replay-re-arm hazard when that endpoint is a PROVEN re-executed root: the
///   no-op marker + final-collapse structure REQUIRES it (L1's `_applyStateDeltas`
///   sets `config.stateRoot = newState` per consume, so each later deferred
///   entry's `currentState` MUST be the live root `state(sync_block)`), or
///   (c) a PROVEN per-tx pair-end root (the validator's per-tx roots mapped onto
///   the Sync block's pair-end positions, both re-derived) at a strictly-
///   increasing position. ONLY a boundary that is NEITHER a placeholder NOR any
///   re-executed root (parent / final / per-tx) is the genuine replay-re-arm
///   hazard — a composer fabricating an interior decoupled from re-execution —
///   and is refused. The N:N inbound-outcome gate independently binds the
///   collapsed deferred entries to the sealed deliveries, so a degenerate
///   collapse cannot smuggle a phantom delivery past this gate.
/// - (#10) no SYSTEM tx in the settled Sync block may have REVERTED.
///
/// `batch_anchor_root` is the RE-EXECUTED `state(posted)` — the parent root of
/// the FIRST validated chunk since the last settlement (the window loop threads
/// it; for a single-chunk batch it equals `vw.parent_state_root`). The
/// `interim_interior_root(r0, k)` placeholders fold `r0 == batch_anchor_root`,
/// so they recompute correctly under a wide multi-chunk batch.
fn verify_settlement_chain(
    pb: &eez_control_rpc::v1::PostBatch,
    vw: &VerifiedWindow,
    batch_anchor_root: B256,
    sync_block_rlp: &[u8],
) -> eyre::Result<()> {
    use eez_evm::settlement::interim_interior_root;

    let batch = decode_postbatch(&pb.abi_calldata)
        .map_err(|e| eyre::eyre!("decode postBatch calldata: {e}"))?;
    let entries = &batch.inner.entries;
    if entries.is_empty() {
        eyre::bail!("settlement batch has no entries");
    }
    let mut chain = Vec::with_capacity(entries.len());
    for (i, e) in entries.iter().enumerate() {
        match e.stateDeltas.as_slice() {
            [d] => chain.push(d),
            ds => eyre::bail!(
                "entry {i} carries {} StateDeltas, expected exactly 1 (Position B)",
                ds.len()
            ),
        }
    }
    let first = chain[0];
    let last = chain[chain.len() - 1];
    let r0 = first.currentState;
    let rn = last.newState;

    if rn != vw.final_state_root {
        eyre::bail!(
            "endpoint: last.newState {rn} != re-executed final_state_root {}",
            vw.final_state_root
        );
    }
    if r0 != batch_anchor_root {
        eyre::bail!(
            "parent-anchor (OD-5): first.currentState {r0} != re-executed batch-anchor root {batch_anchor_root} (= state(posted), the FIRST chunk's re-executed parent)"
        );
    }
    if !chain.windows(2).all(|w| w[0].newState == w[1].currentState) {
        eyre::bail!("telescoping: a delta's newState != the next delta's currentState");
    }
    if !chain.iter().all(|d| d.rollupId == first.rollupId) {
        eyre::bail!("settlement chain spans multiple rollups");
    }

    // Re-derive the Sync block's SYSTEM-tx flags from its OWN RLP, then map the
    // validator's per-tx roots onto the pair-end positions (both re-derived,
    // never composer claims). A position without a root (count drift) collapses
    // the whole map to None → real interiors refused.
    let system_flags = system_tx_flags_from_rlp(sync_block_rlp);
    let pair_end_roots: Option<Vec<B256>> = vw.sync_per_tx_roots.as_ref().and_then(|per_tx| {
        pair_end_positions(&system_flags)
            .iter()
            .map(|&i| per_tx.get(i).copied())
            .collect()
    });

    // Interior boundaries (1 <= k <= N-1). `k` is the SEMANTIC boundary index
    // (it feeds `interim_interior_root(r0, k)`), not just a cursor.
    //
    // A boundary is legitimate if it is a PROVEN re-executed root. The leading
    // no-op immediate's `newState` is `state(sync_block-1)`: for a single-block
    // settling window that's `vw.parent_state_root`; for a multi-block settling
    // chunk it's the SECOND-TO-LAST block's re-executed root, exposed only via
    // `vw.per_block_roots`. The collapsed inbound-deferred boundaries are
    // `state(sync_block)` = `vw.final_state_root` (every inbound delivery lands
    // in the one Sync block, so the chain telescopes through the final root
    // after the first consume). All are re-execution FACTS, not composer claims
    // — and the no-op + final-collapse boundaries legitimately equal a chain
    // ENDPOINT, which is exactly why the prior "equals an endpoint → replay
    // re-arm" bail false-rejected an honest multi-inbound batch. The genuine
    // hazard (a fabricated interior decoupled from re-execution) is still
    // refused: it matches NO re-executed root (parent / final / per-block / the
    // Sync block's per-tx roots).
    let proven_root = |b: B256| -> bool {
        b == vw.parent_state_root
            || b == vw.final_state_root
            || vw
                .per_block_roots
                .as_deref()
                .is_some_and(|roots| roots.contains(&b))
            || vw
                .sync_per_tx_roots
                .as_deref()
                .is_some_and(|roots| roots.contains(&b))
    };
    let mut last_pos: Option<usize> = None;
    #[allow(clippy::needless_range_loop)]
    for k in 1..chain.len() {
        let boundary = chain[k].currentState;
        if boundary == interim_interior_root(r0, k) {
            continue; // recomputable placeholder — telescopes, never a real root
        }
        // A boundary equal to a PROVEN re-executed root (parent / final / per-tx)
        // is legitimate telescoping — including the no-op-immediate boundary and
        // the final-collapse boundaries that equal an endpoint. Only an interior
        // that is NEITHER a placeholder NOR a re-executed root is the replay-re-arm
        // hazard.
        if proven_root(boundary) {
            // The no-op-immediate boundary (= parent / `state(sync_block-1)`) and
            // the inbound final-collapse boundaries (= `final_state_root`, the same
            // root every deferred entry telescopes onto) pass freely — they are
            // endpoints / inter-block roots, NOT positioned interior pair-ends, and
            // the collapse legitimately repeats `final_state_root`. ONLY a genuine
            // INTERMEDIATE pair-end root (a distinct per-tx root that is neither
            // endpoint, e.g. an N-pair flash-loan chain) takes the strict-order
            // advance so its interiors can't be reordered.
            let is_endpoint_root = boundary == r0 || boundary == rn;
            if !is_endpoint_root {
                if let Some(roots) = pair_end_roots.as_deref() {
                    if let Some(pos) = roots.iter().position(|r| *r == boundary) {
                        if last_pos.is_some_and(|lp| pos <= lp) {
                            eyre::bail!(
                                "interior boundary {k} matches pair-end root #{pos} OUT OF ORDER"
                            );
                        }
                        last_pos = Some(pos);
                    }
                }
            }
            continue;
        }
        if boundary == r0 || boundary == rn {
            // Reachable only if an endpoint is NOT a proven re-executed root — i.e.
            // the endpoint gates above were bypassed (they enforce r0 ==
            // batch_anchor_root and rn == final_state_root, and for a single-chunk
            // batch batch_anchor_root == parent_state_root). Defense in depth.
            eyre::bail!(
                "interior boundary {k} equals an endpoint that is not a proven re-executed root — replay re-arm hazard"
            );
        }
        eyre::bail!(
            "interior boundary {k} ({boundary}) is neither a placeholder nor any re-executed root (parent / final / per-tx) — replay re-arm hazard"
        );
    }

    // (#10) No SYSTEM tx in the settled block may have REVERTED — a reverted-but-
    // sealed system tx would pass the calldata-derived gates vacuously.
    if !system_txs_succeeded(&system_flags, vw.sync_tx_statuses.as_deref()) {
        eyre::bail!("a SYSTEM tx in the settled block REVERTED (or per-tx status count drift)");
    }
    Ok(())
}

/// The settlement-chain verdict, split so the prover loop can distinguish a
/// RETREATABLE OD-5 anchor mismatch (its resume cursor is stale relative to the
/// composer's `posted` — an L2 wipe / deep reorg dropped `posted` below
/// `last_accepted`, so the prover re-executes the batch from the wrong place and
/// its re-executed `state(posted)` anchor is `state(stale)`) from a HARD
/// soundness reject (endpoint / telescope / single-rollup / interior /
/// reverted-system-tx). ONLY the anchor mismatch is healed by retreating +
/// re-verifying; every hard reject keeps the cursor put.
enum SettlementVerdict {
    /// Every gate passed.
    Ok,
    /// The composer's CLAIMED batch anchor `state(posted)` (= `first.currentState`)
    /// does NOT equal the prover's RE-EXECUTED batch anchor. Both roots are carried
    /// for the log; the heal is to retreat the resume cursor toward genesis and
    /// re-derive the anchor from the true `posted`.
    AnchorMismatch { claimed_r0: B256, reexecuted: B256 },
    /// A soundness gate failed for a reason UNRELATED to the resume cursor — never
    /// retreat (re-executing from elsewhere can't make a fabricated chain valid).
    HardReject(eyre::Report),
}

/// The batch's CLAIMED OD-5 anchor — `first.currentState` = `state(posted)`. A
/// decode / shape failure returns `Err`, which the classifier funnels to the
/// inner gate's authoritative `HardReject` (the resume cursor is not at fault).
fn batch_first_current_state(pb: &eez_control_rpc::v1::PostBatch) -> eyre::Result<B256> {
    let batch = decode_postbatch(&pb.abi_calldata)
        .map_err(|e| eyre::eyre!("decode postBatch calldata: {e}"))?;
    let first = batch
        .inner
        .entries
        .first()
        .ok_or_else(|| eyre::eyre!("settlement batch has no entries"))?;
    match first.stateDeltas.as_slice() {
        [d] => Ok(d.currentState),
        ds => eyre::bail!(
            "entry 0 carries {} StateDeltas, expected exactly 1 (Position B)",
            ds.len()
        ),
    }
}

/// In driven mode the composer dictates exactly one posted window. Failed
/// minimal Sync blocks remain canonical L2 blocks and their historical
/// `PostBatch` sidecars can still be replayed from the control ring; they are
/// not the current directive. Treat only the directive `to_block` as settling and
/// strip older sidecars so the window validates across them like ordinary empty
/// Sync blocks.
fn driven_effective_settlement(
    event: &mut eez_control_rpc::v1::ControlEvent,
    driven: bool,
    target_to_block: Option<u64>,
) -> bool {
    if event.composition.is_none() {
        return false;
    }
    if driven && Some(event.block_number) != target_to_block {
        warn!(
            block_number = event.block_number,
            ?target_to_block,
            "driven: ignoring non-target PostBatch sidecar in replayed control event",
        );
        event.composition = None;
        return false;
    }
    true
}

/// Classify the settlement chain into the 3-way verdict the prover loop acts on.
/// The OD-5 anchor is pre-checked against the RE-EXECUTED `batch_anchor_root`: a
/// pure `first.currentState != batch_anchor_root` is the (retreatable)
/// `AnchorMismatch` — the soundness-critical discrimination is a single
/// compiler-checked branch, not a string match on an opaque error. When the
/// anchor AGREES, the full `verify_settlement_chain` runs and any failure is a
/// `HardReject` (so an endpoint / telescope / interior / single-rollup failure is
/// NEVER mistaken for a stale-cursor retreat). SOUNDNESS: a fabricated batch
/// whose anchor differs gets EXACTLY ONE retreat; re-verified from the true
/// posted (genesis in v1) it either telescopes from the real re-executed roots
/// (it was honest, just resumed stale) or fails again at `last_accepted == 0`,
/// where the self-disarm makes it a terminal reject — no false attestation either
/// way. The retreat only changes WHERE re-execution starts, never WHAT is checked.
fn classify_settlement_chain(
    pb: &eez_control_rpc::v1::PostBatch,
    vw: &VerifiedWindow,
    batch_anchor_root: B256,
    sync_block_rlp: &[u8],
) -> SettlementVerdict {
    if let Ok(r0) = batch_first_current_state(pb) {
        if r0 != batch_anchor_root {
            return SettlementVerdict::AnchorMismatch {
                claimed_r0: r0,
                reexecuted: batch_anchor_root,
            };
        }
    }
    match verify_settlement_chain(pb, vw, batch_anchor_root, sync_block_rlp) {
        Ok(()) => SettlementVerdict::Ok,
        Err(e) => SettlementVerdict::HardReject(e),
    }
}

/// How the resume cursor moves on a settling window that PASSED every gate, given
/// the located CONFIRMED posted height `derived_h` (the cache height of the claimed
/// `current_state`) and the current `last_accepted`. Pure so the advance/retain/
/// retreat boundaries are unit-tested independently of the async loop.
#[derive(Debug, PartialEq, Eq)]
enum CursorMove {
    /// Posted ADVANCED to h (h > last_accepted): advance + reset the telescope.
    Advance(u64),
    /// Posted UNCHANGED (h == last_accepted): a stale re-post — retain the anchor,
    /// do not advance (the stuck-state catch-up depends on this).
    Retain,
    /// Posted RETREATED to a cached h (h < last_accepted): precise retreat.
    Retreat(u64),
    /// Unlocatable on a passing window — unreachable (an OD-5-passing claim is
    /// always cache-resident); hold defensively rather than advance.
    HoldDefensive,
}

fn settling_cursor_move(derived_h: Option<u64>, last_accepted: u64) -> CursorMove {
    match derived_h {
        Some(h) if h > last_accepted => CursorMove::Advance(h),
        Some(h) if h == last_accepted => CursorMove::Retain,
        Some(h) => CursorMove::Retreat(h),
        None => CursorMove::HoldDefensive,
    }
}

/// How the resume cursor RE-ANCHORS on an OD-5 anchor mismatch (a rejected window
/// carrying `anchor_mismatch = Some(claimed_r0)`), given the cache height of the
/// claimed root, the current `last_accepted`, and the retreat budget. Pure for
/// the same reason — the precise-retreat / advance / bounded-fallback split is the
/// soundness-critical part.
#[derive(Debug, PartialEq, Eq)]
enum ReanchorMove {
    /// Cached BELOW last_accepted: precise retreat to h (replay [h+1..]).
    Retreat(u64),
    /// Cached at/above last_accepted: posted advanced; advance to h.
    Advance(u64),
    /// Unlocatable, budget available: bounded blunt retreat to genesis.
    RetreatGenesis,
    /// Unlocatable, already at genesis: terminal reject (self-disarmed).
    TerminalAtGenesis,
    /// Unlocatable, budget exhausted: terminal reject.
    BudgetExhausted,
}

fn reanchor_move(
    cached_h: Option<u64>,
    last_accepted: u64,
    consecutive_retreats: u32,
    max_retreats: u32,
) -> ReanchorMove {
    match cached_h {
        Some(h) if h < last_accepted => ReanchorMove::Retreat(h),
        Some(h) => ReanchorMove::Advance(h),
        None => {
            if last_accepted > 0 && consecutive_retreats < max_retreats {
                ReanchorMove::RetreatGenesis
            } else if last_accepted == 0 {
                ReanchorMove::TerminalAtGenesis
            } else {
                ReanchorMove::BudgetExhausted
            }
        }
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    // A validator needs BOTH halves — fail loud rather than silently downgrading
    // to a non-validating observer.
    match (args.validator_bin.is_some(), args.chain_config.is_some()) {
        (true, true) | (false, false) => {}
        _ => eyre::bail!(
            "--validator-bin and --chain-config (EEZ_VALIDATOR_BIN / EEZ_CHAIN_CONFIG) must be set together, or neither"
        ),
    }
    // Attesting mode: a signer key turns the daemon from "validate + log" into
    // "validate + SIGN + submit". The gate vkey is the attester's OWN address, so
    // the hash the prover recomputes-and-signs can't drift from the key it signs
    // with.
    let signer = match args.signer_key {
        Some(key) => match EcdsaProofSigner::from_private_key(key) {
            Ok(s) => Some(s),
            Err(e) => eyre::bail!("invalid --signer-key / EEZ_PROOF_SIGNER_KEY: {e:?}"),
        },
        None => None,
    };
    let vkey = signer
        .as_ref()
        .map_or(args.vkey, |s| vkey_from_address(s.address()));
    let vkey_configured = vkey != B256::ZERO;
    let proof_sink_url = args
        .proof_sink_url
        .clone()
        .unwrap_or_else(|| args.control_addr.clone());

    // Durable-backfill endpoint. The arg defaults to the L2 RPC, so a normally
    // deployed prover RECOVERS replay gaps out of the box; an EXPLICIT empty
    // value (`--l2-rpc-url ""` / `EEZ_L2_RPC_URL=`) normalizes to None — the
    // fail-loud, no-backfill opt-out (observer mode is unchanged).
    let l2_rpc_url: Option<String> = args.l2_rpc_url.clone().filter(|u| !u.trim().is_empty());

    let validating = args.validator_bin.is_some() && args.chain_config.is_some();
    // Composer-driven dispatch (Phase 3): when set, the prover takes its verify
    // range from the composer's ProverDispatch stream (the oldest posted-but-
    // unverified window) instead of self-picking `from_block = last_accepted+1`.
    // Default off ⇒ the self-pick path is byte-for-byte unchanged. Same parse as
    // eez-node's gate so the two agree.
    let driven = std::env::var("EEZ_COMPOSER_DRIVEN")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    info!(
        control_addr = %args.control_addr,
        validating,
        vkey_configured,
        attesting = signer.is_some(),
        max_window = args.max_window,
        driven,
        "eez-proverd starting",
    );
    if driven && signer.is_none() {
        warn!(
            "EEZ_COMPOSER_DRIVEN is set but no signer key — a driven prover that does not ATTEST \
             cannot advance the composer's verified frontier, so the composer will re-dispatch the \
             same window forever. Set EEZ_PROOF_SIGNER_KEY to attest."
        );
    }
    if let Some(s) = &signer {
        info!(
            attester = %s.address(),
            proof_sink = %proof_sink_url,
            "ATTESTING — will SIGN + submit the publicInputsHash of each fully-verified settling window",
        );
    }
    if !vkey_configured {
        warn!(
            "neither --signer-key nor --vkey is set: settlement publicInputsHash verification is \
             DISABLED; settling windows pass through ungated"
        );
    }
    match &l2_rpc_url {
        Some(url) => info!(
            l2_rpc_url = %url,
            "durable backfill ENABLED — replay gaps are reconstructed from L2 archive state \
             (debug_executionWitness + debug_getRawBlock), never fast-forwarded past",
        ),
        None => warn!(
            "durable backfill DISABLED (--l2-rpc-url empty): a replay gap is fail-loud and the \
             in-flight batch is dropped for retry — the cursor is NEVER advanced past unproven blocks",
        ),
    }

    // Resume cursor = the composer's TRUE SETTLED HEIGHT: advanced ONLY on a
    // COMPLETED SETTLING window (a batch boundary), NEVER per non-settling chunk.
    // So on (re)start/reconnect the prover resumes from `last_accepted+1 =
    // posted+1` — the start of the in-flight BATCH — and re-streams every chunk of
    // it (a gap up to the live tip is durably backfilled, recovering a stuck
    // `posted << tip`). This is REQUIRED for the OD-5 batch anchor (GAP-2): a
    // restart mid-batch must re-derive `state(posted)` by RE-EXECUTING the batch's
    // FIRST chunk, which only happens if resume lands on the batch boundary. A
    // per-chunk cursor would resume mid-batch, where the first re-executed parent
    // is `state(posted + k·max_window) != state(posted)`, and OD-5 would falsely
    // reject the eventual settlement forever. In-session, non-settling chunk
    // contiguity uses the separate `stream_tip` (below), so the per-chunk
    // empty-window gap check still works without advancing the settled cursor.
    //
    // INVARIANT REPAIR (anchor-at-confirmed-posted): the above assumes `last_accepted
    // == posted` (the composer's L1-CONFIRMED settled height). The OLD code advanced
    // `last_accepted = window_end` on EVERY settling window — OPTIMISTICALLY, assuming
    // the attestation would settle. It does not always: an attestation only settles
    // if it matches a composer deferred-post still IN-FLIGHT. After an L2 wipe the
    // composer's `posted` = 0 and it re-posts a GROWING `[1..tip]` batch anchored at
    // `state(0)` every slot; a prover that re-executes the whole backlog (minutes)
    // attests a height whose deferred-post timed out long ago, so it NEVER settles —
    // yet the OLD code advanced `last_accepted` past `posted`, and the NEXT
    // genesis-anchored re-post then mismatched the now-stale re-executed anchor → OD-5
    // rejects every settlement forever (a re-drift LOOP).
    //
    // THE FIX: `last_accepted` tracks the CONFIRMED posted = the height `H` whose
    // RE-EXECUTED `state(H)` equals the batch's CLAIMED `current_state` (= state(posted)),
    // located from `root_to_height` (a process-lifetime map of the prover's OWN
    // re-executed roots → height). On a settling window we advance ONLY when `H`
    // strictly exceeds `last_accepted` (a genuinely new, re-executed posted), RETAIN
    // the telescope anchor when `H` is unchanged (a stale re-post — stay connected,
    // no re-backfill, catch up to the live tip), and RETREAT precisely to a cached
    // lower `H` on a wipe/reorg. An `H` the prover never re-executed (fabricated, or
    // below this session's horizon) falls to the bounded retreat-to-genesis +
    // self-disarm. Soundness is unchanged: the cache is only a resume HINT — OD-5 +
    // every gate still re-verify `current_state` against the re-executed roots, so a
    // wrong `H` cannot make a bad batch attest (it fails OD-5 → reject → no advance).
    let mut last_accepted: u64 = 0;
    let mut consecutive_failures: u32 = 0;
    // Bounded budget of UNLOCATABLE-anchor retreats (a fabricated/below-horizon
    // current_state → blunt retreat-to-genesis). Cache-located precise retreats
    // (a real wipe/reorg) reset this — they are not flapping. The `last_accepted == 0`
    // self-disarm is the terminal stop; reset on every genuine settlement.
    let mut consecutive_retreats: u32 = 0;
    const MAX_CONSECUTIVE_RETREATS: u32 = 4;
    // Resume-cursor cache: each RE-EXECUTED state root → its block height, so a
    // settling batch's claimed `current_state` (= state(posted)) can be LOCATED to
    // drive `last_accepted` to the composer's true confirmed posted (never past it).
    // PROCESS-LIFETIME (survives reconnects) so the stuck-state catch-up + the
    // re-anchor cost at most ONE backfill, never one per re-post. `or_insert` keeps
    // the LOWEST height for a repeated root (conservative posted on a no-op repeat).
    // Genesis (state(0)) self-registers as the parent of the first replayed window
    // (the prover always resumes from `last_accepted+1`, = 1 on a fresh start / a
    // retreat-to-0). Only a HINT — OD-5 re-proves every anchor against re-execution.
    let mut root_to_height: HashMap<B256, u64> = HashMap::new();

    // Composer-driven dispatch (Phase 3) state. Unused on the self-pick path.
    // `driven_rerequest` bounds the OD-5 anchor-mismatch re-request to ONE per
    // directive boundary: the composer re-emits the IDENTICAL window on a
    // non-attested re-request, so the counter is reset ONLY when a genuinely new
    // boundary (`from_block`) arrives — else the cap never engages (risk: an
    // unbounded retry). `prover_epoch` is log-correlation only (the composer
    // derives WHAT to dispatch from its own ledger, never a prover-asserted height).
    let mut driven_rerequest: u32 = 0;
    let mut last_vr_from: Option<u64> = None;
    let prover_epoch: u64 = 0;

    loop {
        if consecutive_failures > 0 {
            let exp = consecutive_failures.min(6);
            let delay = Duration::from_millis(500u64 << exp).min(Duration::from_secs(30));
            warn!(
                attempt = consecutive_failures,
                ?delay,
                "reconnecting to the composer"
            );
            tokio::time::sleep(delay).await;
        }

        // Phase 3: in driven mode RECEIVE one directive (the oldest posted-but-
        // unverified window) before subscribing. A fresh dispatch connection per
        // iteration keeps the prover stateless; on any failure we `continue` and
        // the next iteration re-receives the SAME oldest-unverified window. Reset
        // the bounded re-request counter ONLY on a NEW directive boundary (the
        // composer re-emits the same `from_block` on a non-attested re-request).
        let vr: Option<VerifyRange> = if driven {
            match dispatch_one(&args.control_addr, prover_epoch).await {
                Some(d) => {
                    if last_vr_from != Some(d.from_block) {
                        driven_rerequest = 0;
                        last_vr_from = Some(d.from_block);
                    }
                    info!(
                        from_block = d.from_block,
                        to_block = d.to_block,
                        rollup_id = d.rollup_id,
                        "driven: received verify directive",
                    );
                    Some(d)
                }
                None => {
                    consecutive_failures += 1;
                    continue;
                }
            }
        } else {
            None
        };

        let mut control = match ControlFeedClient::connect(args.control_addr.clone()).await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "control feed connect failed");
                consecutive_failures += 1;
                continue;
            }
        };

        // Phase 3: driven mode takes from_block from the directive (= posted+1,
        // the OD-5 anchor block + 1); self-pick resumes from the settled cursor.
        let from_block = match &vr {
            Some(d) => d.from_block,
            None => last_accepted + 1,
        };
        let mut stream = match control.subscribe(SubscribeRequest { from_block }).await {
            Ok(s) => s.into_inner(),
            Err(e) => {
                warn!(error = %e, "subscribe failed");
                consecutive_failures += 1;
                continue;
            }
        };
        consecutive_failures = 0;
        info!(from_block, "subscribed to the composer control feed");

        // Fresh window per subscribe — a reconnect re-accumulates from the replay.
        let mut window: Vec<ControlEvent> = Vec::new();
        // In-session streaming tip for the per-chunk empty-window gap check: the
        // highest block number proved in THIS connection. Distinct from the
        // settled `last_accepted` (which only moves at batch boundaries) so a
        // non-settling chunk close advances the contiguity anchor WITHOUT
        // fast-forwarding the resume cursor. Seeded to `from_block - 1` so the
        // FIRST event (block `from_block`) passes the gap check directly and only
        // a TRUE ring-eviction (event > from_block) triggers backfill.
        //
        // Self-pick: `from_block = last_accepted + 1`, so `from_block - 1 ==
        // last_accepted` — IDENTICAL to the old seed, zero behavior change.
        // Driven (Phase 3): `from_block = posted + 1` (the directive). Seeding to
        // `posted` means the first event (posted+1) needs NO backfill — the whole
        // posted batch is self-contained from its boundary and the OD-5 anchor is
        // re-derived from block `from_block`'s own witness. WITHOUT this, a fresh
        // driven start (last_accepted=0) with from_block>1 would backfill
        // [1..posted] = re-verify from genesis, exactly the [1..tip] replay the
        // composer-driven inversion exists to ELIMINATE.
        let mut stream_tip: u64 = from_block.saturating_sub(1);
        // OD-5 batch anchor (GAP-2): the RE-EXECUTED `state(posted)` — the parent
        // root of the FIRST validated chunk since the last settlement. The composer
        // posts ONE batch `[posted+1 .. sync_block]` anchored at `state(posted)`,
        // but the prover streams it as `max_window`-sized chunks; a batch wider than
        // `max_window` settles on a LATER chunk whose local `parent_state_root` is
        // `state(posted + k·max_window) != state(posted)`. We TELESCOPE: capture the
        // first chunk's re-executed parent (re-execution, never the composer's claim
        // — that's the value under test), verify chunk-to-chunk root continuity as
        // chunks close, and feed THIS anchor (not the settling chunk's local parent)
        // into the OD-5 check. `None` = the next chunk starts a fresh batch. A
        // reconnect always replays from the batch boundary (`posted+1`), so it is
        // RE-DERIVED here from the re-streamed first chunk — never carried stale
        // across a restart.
        let mut batch_anchor_root: Option<B256> = None;
        // The previous closed chunk's re-executed final root — the next chunk's
        // expected re-executed parent (state-root telescoping across chunks, which
        // the block-hash contiguity guard does NOT cover). Reset with the anchor.
        let mut prev_chunk_final_root: Option<B256> = None;
        // The previous closed chunk's LAST block hash — the next chunk's first
        // event's expected `parent_hash`. The intra-window contiguity guard only
        // checks `parent_hash == tip.block_hash` for the 2nd+ event of a window
        // (the first event has no in-window predecessor); WITHOUT this the splice
        // of two chunks is bound only by the state-root telescope (which a crafted
        // witness pair could satisfy while presenting a different block). Carry the
        // hash across the chunk boundary and assert contiguity, fail-closed. The
        // state-root telescope (`prev_chunk_final_root`) and this hash check are
        // complementary; both must hold. Empty across the FIRST chunk (None).
        let mut prev_chunk_last_hash: Option<Vec<u8>> = None;

        // Storm guard: the (window_start, window_end) of the last window we ran
        // through native-validate. A re-verification churn in the composer's
        // ledger can re-send the IDENTICAL settling window thousands of times per
        // second; each runs validate_window, which SPAWNS a native-validate
        // subprocess (~1k spawns/s OOM-killed the prover live). We throttle
        // identical consecutive re-sends below so a churn degrades to a slow
        // re-verify rather than a crash. Defence-in-depth — the real fix is in the
        // composer's `mark_attested` (resolve the dispatched-widest window), which
        // removes the churn at its source.
        let mut last_validated_window: Option<(u64, u64)> = None;
        // Archive backfill is fed through the same per-event path as live replay.
        // On a large gap we queue at most one max_window chunk ahead of the
        // already-received event, letting the normal close/validate path advance
        // stream_tip before that event is retried.
        let mut pending_events: VecDeque<ControlEvent> = VecDeque::new();

        loop {
            let next_event = if let Some(event) = pending_events.pop_front() {
                Ok(Some(event))
            } else {
                stream.message().await
            };
            match next_event {
                Ok(Some(mut event)) => {
                    // Phase 3 (driven): never verify past the directive's to_block. By
                    // construction to_block is the settling block (is_sync) and the window
                    // closes + breaks there, so this fires only if that block carried NO
                    // composition (a composer bug) — fail-closed: refuse + re-request.
                    if let Some(d) = &vr {
                        if event.block_number > d.to_block {
                            error!(
                                got = event.block_number,
                                to_block = d.to_block,
                                "driven: streamed past the directive to_block without a settling composition — refusing + re-requesting",
                            );
                            consecutive_failures += 1;
                            break;
                        }
                    }
                    // Cross-chunk block-HASH continuity: the FIRST block of a
                    // fresh window must chain by HASH from the PREVIOUS closed
                    // chunk's last block. The intra-window guard below only binds
                    // the 2nd+ event of a window (the first has no in-window
                    // predecessor), and the per-chunk telescope binds only the
                    // re-executed STATE ROOTS — neither closes the block-hash
                    // splice at a chunk boundary. Validate the first incoming
                    // block's `parent_hash` (the backfill path seeds blocks
                    // [expected..got-1] BEFORE this event whose first is reth-
                    // archive-authoritative and chains internally; its parent is
                    // the boundary block, so checking the FIRST block to enter the
                    // window — backfill's first or `event` — closes the splice).
                    // Fail-closed: a mismatch drops the in-flight window.
                    if window.is_empty() {
                        if let Some(prev_hash) = prev_chunk_last_hash.as_deref() {
                            // No backfill (event extends the boundary directly):
                            // the event itself is the window's first block, so its
                            // parent must be the previous chunk's last hash. With
                            // backfill the first reconstructed block carries the
                            // boundary parent; we validate that block below right
                            // after it is fetched.
                            let first_needs_backfill = event.block_number != stream_tip + 1;
                            if !first_needs_backfill && event.parent_hash != prev_hash {
                                warn!(
                                    block_number = event.block_number,
                                    "cross-chunk block-hash break: first event's parent_hash != previous chunk's last block hash; dropping the window"
                                );
                                window.clear();
                                consecutive_failures += 1;
                                break;
                            }
                        }
                    }
                    // First event of a fresh window MUST chain from the streaming
                    // tip (`stream_tip` = the highest block proved in THIS
                    // connection, seeded to the settled cursor at subscribe);
                    // otherwise the server's bounded ring evicted
                    // [stream_tip+1 .. event-1] (a disconnect outlasting the
                    // ring horizon, or a stuck prover whose posted << tip). The
                    // missing blocks are DURABLY BACKFILLED from the L2 archive
                    // (debug_executionWitness + debug_getRawBlock) and pushed into
                    // the window BEFORE the live event, so the contiguity guard
                    // below passes and `stream_tip` stays anchored at the TRUE
                    // proved tip. NEVER fast-forward past unproven blocks — that
                    // decouples the anchor from the composer's `posted` and breaks
                    // the OD-5 parent-anchor check forever.
                    if window.is_empty() && event.block_number != stream_tip + 1 {
                        let expected = stream_tip + 1;
                        let got = event.block_number;
                        match &l2_rpc_url {
                            // Backfill DISABLED: fail loud, drop the batch, retry on
                            // the next replay. The cursor is NOT advanced.
                            None => {
                                error!(
                                    expected,
                                    got,
                                    "DATA_LOSS: replay gap and --l2-rpc-url is unset; cannot backfill \
                                     [{expected}..={}] — dropping the batch (retry on replay); cursor NOT advanced",
                                    got.saturating_sub(1),
                                );
                                window.clear();
                                consecutive_failures += 1;
                                break;
                            }
                            // Backfill ENABLED: reconstruct at most one validation chunk
                            // in order from the L2 archive. The recovered events are put
                            // in front of this already-received event so the existing
                            // per-event max_window path validates the gap in bounded
                            // chunks instead of one enormous native-validate window.
                            Some(url) if got > expected => {
                                let chunk =
                                    u64::try_from(args.max_window.max(1)).unwrap_or(u64::MAX);
                                let backfill_got = got.min(expected.saturating_add(chunk));
                                warn!(
                                    expected,
                                    got,
                                    backfill_to = backfill_got - 1,
                                    "replay gap — durably backfilling bounded chunk [{expected}..={}] from the L2 archive",
                                    backfill_got - 1,
                                );
                                let t_backfill = Instant::now();
                                let recovered = match backfill_gap_events(
                                    url,
                                    expected,
                                    backfill_got,
                                    prev_chunk_last_hash.as_deref(),
                                    args.backfill_concurrency,
                                )
                                .await
                                {
                                    Ok(events) => events,
                                    Err(e) => {
                                        error!(
                                            error = %e,
                                            "backfill FAILED — dropping the batch; cursor NOT advanced",
                                        );
                                        window.clear();
                                        consecutive_failures += 1;
                                        break;
                                    }
                                };
                                let backfill_ms = t_backfill.elapsed().as_millis();
                                let count = u64::try_from(recovered.len()).unwrap_or(u64::MAX);
                                info!(
                                    backfilled = count,
                                    backfill_ms,
                                    per_block_ms = backfill_ms / u128::from(count.max(1)),
                                    from = expected,
                                    to = backfill_got - 1,
                                    replay_gap_to = got - 1,
                                    concurrency = args.backfill_concurrency.clamp(1, 64),
                                    "✓ replay gap chunk backfilled from the L2 archive (debug_executionWitness per block)",
                                );
                                pending_events.push_front(event);
                                for ev in recovered.into_iter().rev() {
                                    pending_events.push_front(ev);
                                }
                                continue;
                            }
                            // `got < expected` (a stale replay below the cursor): the
                            // server replayed a block we already proved. Not a gap;
                            // the contiguity guard / strictly-increasing numbers drop
                            // it. (`got == expected` can't reach here.)
                            Some(_) => {}
                        }
                    }
                    // Intra-window contiguity: the event must extend the tip.
                    if let Some(tip) = window.last() {
                        if event.parent_hash != tip.block_hash
                            || event.block_number != tip.block_number + 1
                        {
                            warn!(
                                block_number = event.block_number,
                                "window contiguity break; dropping the in-flight window"
                            );
                            window.clear();
                        }
                    }
                    let is_sync = driven_effective_settlement(
                        &mut event,
                        driven,
                        vr.as_ref().map(|d| d.to_block),
                    );
                    window.push(event);

                    if is_sync || window.len() >= args.max_window {
                        // Window is non-empty after the push.
                        let window_end = window
                            .last()
                            .expect("window non-empty after push")
                            .block_number;
                        let window_start = window.first().map_or(window_end, |e| e.block_number);
                        // Storm guard (see `last_validated_window` decl): if this is
                        // an IDENTICAL re-send of the window we just validated, pace
                        // it so a composer-side re-verification churn can't spawn
                        // native-validate fast enough to OOM-kill us. Every window is
                        // still fully validated + attested — just rate-limited.
                        if last_validated_window == Some((window_start, window_end)) {
                            warn!(
                                from = window_start,
                                to = window_end,
                                "identical settling window re-sent — throttling native-validate (re-verification storm guard)",
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        }
                        last_validated_window = Some((window_start, window_end));
                        let mut window_ok = true;
                        let mut verified: Option<VerifiedWindow> = None;
                        // The publicInputsHash to sign IF every gate passes (attesting).
                        let mut attest_hash: Option<B256> = None;
                        // Set ONLY when the settlement chain fails because the
                        // composer's claimed `state(posted)` anchor != the re-executed
                        // anchor (a stale resume cursor) — carries the claimed root so
                        // the rejection branch can retreat + re-verify. `None` for a
                        // clean window or any hard soundness reject.
                        let mut anchor_mismatch: Option<B256> = None;
                        // The CONFIRMED posted height located for a settling window:
                        // the cache height of the batch's claimed `current_state`.
                        // Drives the resume-cursor advance/retain/retreat decision in
                        // the `is_sync` block. `None` for a non-settling window (and,
                        // defensively, an unlocatable claim — unreachable past OD-5).
                        let mut derived_h: Option<u64> = None;

                        if let (Some(vbin), Some(cfg)) = (&args.validator_bin, &args.chain_config) {
                            match validate_window(&window, vbin, cfg, &args.work_dir).await {
                                Ok(vw) => verified = Some(vw),
                                Err(e) => {
                                    error!(error = %e, "window stateless re-execution FAILED");
                                    window_ok = false;
                                }
                            }
                        } else {
                            info!(
                                blocks = window.len(),
                                settling = is_sync,
                                "window closed (observer: set --validator-bin to validate)"
                            );
                        }

                        // Resume-cursor cache fill (anchor-at-confirmed-posted): map
                        // every RE-EXECUTED root in this window to its height. Placed
                        // BEFORE the OD-5 logic so the load-bearing invariant holds —
                        // an OD-5-PASSING claim `R0 == batch_anchor_root` is therefore
                        // ALWAYS cache-resident (it equals this window's
                        // `parent_state_root`, registered right here). `parent_state_root`
                        // = state(window_start-1) registers genesis -> 0 on the first
                        // replayed window. `or_insert` keeps the lowest height for a
                        // repeated root. Runs for settling AND non-settling windows.
                        if let Some(vw) = &verified {
                            if window_start > 0 {
                                root_to_height
                                    .entry(vw.parent_state_root)
                                    .or_insert(window_start - 1);
                            }
                            if let Some(roots) = &vw.per_block_roots {
                                for (i, r) in roots.iter().enumerate() {
                                    root_to_height.entry(*r).or_insert(window_start + i as u64);
                                }
                            }
                            root_to_height
                                .entry(vw.final_state_root)
                                .or_insert(window_end);
                        }

                        // GAP-2 OD-5 batch anchor — TELESCOPE across chunks. The
                        // composer's batch is anchored at `state(posted)`; the prover
                        // streams it as `max_window` chunks, so the SETTLING chunk's
                        // local re-executed parent is NOT `state(posted)` when the batch
                        // spans > 1 chunk. Capture the FIRST chunk's re-executed parent
                        // as the batch anchor (re-execution, never a composer claim) and
                        // verify each later chunk's re-executed parent == the previous
                        // chunk's re-executed final root (state-root continuity the
                        // block-hash guard doesn't cover). A break here = a non-contiguous
                        // re-execution → fail closed.
                        if let Some(vw) = &verified {
                            match prev_chunk_final_root {
                                None => {
                                    // First chunk of a fresh batch: anchor at its
                                    // re-executed parent = `state(posted)`.
                                    batch_anchor_root = Some(vw.parent_state_root);
                                }
                                Some(prev_final) => {
                                    if vw.parent_state_root != prev_final {
                                        error!(
                                            chunk_parent = %vw.parent_state_root,
                                            prev_chunk_final = %prev_final,
                                            "batch chunk telescoping break: re-executed parent != previous chunk's re-executed final root; REJECTED",
                                        );
                                        window_ok = false;
                                    }
                                }
                            }
                            // Advance the telescope cursor to THIS chunk's re-executed
                            // final root (the next chunk's expected parent).
                            prev_chunk_final_root = Some(vw.final_state_root);
                        }

                        // Settlement publicInputsHash cross-check — the attester
                        // recomputes this byte-for-byte before signing.
                        if let Some(pb) = window.iter().find_map(|e| {
                            e.composition.as_ref().and_then(|c| c.post_batch.as_ref())
                        }) {
                            // Locate the composer's CONFIRMED posted height for the
                            // resume-cursor decision below: the cache height of the
                            // batch's claimed `current_state` (= state(posted)). A
                            // `pb` is present only on a settling Sync block, so this
                            // is exactly the is_sync case. A decode miss → None →
                            // fail-closed (no advance). This is ONLY the resume signal;
                            // the OD-5 soundness check still binds R0 to the re-executed
                            // anchor independently.
                            derived_h = batch_first_current_state(pb)
                                .ok()
                                .and_then(|r0| root_to_height.get(&r0).copied());
                            if vkey_configured {
                                match verify_settlement_public_inputs(pb, vkey) {
                                    Ok(h) => {
                                        info!(
                                            public_inputs_hash = %h,
                                            "✓ settlement publicInputsHash recomputed = composer claim",
                                        );
                                        attest_hash = Some(h);
                                    }
                                    Err(e) => {
                                        error!(error = %e, "settlement publicInputsHash REJECTED");
                                        window_ok = false;
                                    }
                                }
                            } else {
                                warn!(
                                    "settling window but --vkey unset; publicInputsHash NOT verified"
                                );
                            }

                            // (2b) Settlement-chain gate — checks the composer's
                            // claimed StateDelta chain against the RE-EXECUTED roots
                            // (endpoints + telescoping + single-rollup + interiors +
                            // reverted-system-tx #10). Validator mode only.
                            if let Some(vw) = &verified {
                                // The Sync block (window's last event) re-derives the
                                // system flags + pair-end positions.
                                let sync_block_rlp =
                                    window.last().map_or(&[][..], |e| e.block.as_slice());
                                // OD-5 anchors at the TELESCOPED batch anchor
                                // (`state(posted)` = the FIRST chunk's re-executed
                                // parent), NOT the settling chunk's local parent. The
                                // capture above set it on this batch's first chunk; its
                                // absence means a settling window with no first-chunk
                                // re-execution (validator returned no parent root) — fail
                                // closed rather than silently anchoring at the local parent.
                                match batch_anchor_root {
                                    Some(anchor) => {
                                        match classify_settlement_chain(
                                            pb,
                                            vw,
                                            anchor,
                                            sync_block_rlp,
                                        ) {
                                            SettlementVerdict::Ok => info!(
                                                %anchor,
                                                "✓ settlement chain telescopes R0(=state(posted))→…→R_N to the re-executed roots",
                                            ),
                                            SettlementVerdict::AnchorMismatch {
                                                claimed_r0,
                                                reexecuted,
                                            } => {
                                                error!(
                                                    %claimed_r0,
                                                    %reexecuted,
                                                    "settlement chain anchor mismatch (OD-5): claimed state(posted) != re-executed batch anchor — the resume cursor is stale vs the composer's posted (L2 wipe / deep reorg); will retreat + re-verify",
                                                );
                                                window_ok = false;
                                                anchor_mismatch = Some(claimed_r0);
                                            }
                                            SettlementVerdict::HardReject(e) => {
                                                error!(error = %e, "settlement chain REJECTED");
                                                window_ok = false;
                                            }
                                        }
                                    }
                                    None => {
                                        error!(
                                            "settlement chain REJECTED: no re-executed batch anchor (state(posted)) captured for this batch",
                                        );
                                        window_ok = false;
                                    }
                                }
                            }

                            // (2c) Inbound outcome gate — the X==Y / false-success /
                            // forged-hash soundness close, generalized to MULTI-DELIVERY
                            // (GAP-3): re-derive ALL inbound calls sealed in this window's
                            // blocks (`executeIncomingCrossChainCall`) and require a TOTAL
                            // BIJECTION to the settling batch's deferred inbound entries +
                            // failed lookups — every sealed inbound consumes a DISTINCT
                            // carrier (right SHAPE, returnData, on-chain hash H), in the
                            // canonical consumption order, with NO phantom carrier and NO
                            // unmatched delivery. Catches a composer that delivers X on L2
                            // but settles Y on L1, settles a delivery L2 never ran, or
                            // double-claims a carrier. Handles K=0 (a deferred entry with
                            // no sealed inbound → cardinality mismatch → reject) and
                            // duplicate identical calls (positional pairing). Validator/
                            // vkey mode only. (eez0 seals only SUCCESS inbounds today —
                            // failures poison-evicted; the failure half is dormant + ready.)
                            if vkey_configured {
                                let inbound_calls: Vec<_> = window
                                    .iter()
                                    .flat_map(|e| extract_inbounds(&e.block))
                                    .collect();
                                match decode_postbatch(&pb.abi_calldata) {
                                    Ok(batch) => {
                                        match multi_inbound_outcome_gate(&batch, &inbound_calls) {
                                            Ok(()) => info!(
                                                sealed_inbounds = inbound_calls.len(),
                                                "✓ inbound outcome gate — L1 batch bijects to the re-derived L2 inbound deliveries (shape/hash/returnData)",
                                            ),
                                            Err(e) => {
                                                error!(
                                                    sealed_inbounds = inbound_calls.len(),
                                                    error = %e,
                                                    "inbound outcome MISMATCH — no bijection between L2 deliveries and the L1 batch; REJECTED",
                                                );
                                                window_ok = false;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(error = %e, "inbound gate: decode_postbatch failed; REJECTED");
                                        window_ok = false;
                                    }
                                }
                            }

                            // (2d) Outbound authorization gate (A3) — HARD. Authorize
                            // every outbound L2->L1 settlement entry against its
                            // paired, SIGNED Sync-block user tx (the non-system txs of
                            // the window's last block), the SAME shared
                            // `eez_evm::outbound_gate` check the deriver (A4) runs — so
                            // the prover + the follower can't drift. No re-executed
                            // log / guest commitment is involved (the outbound user tx
                            // reverts in plain re-execution; see the gate module). The
                            // user-tx EXTRACTION here is proven equal to A4's DA
                            // pairing by `outbound_user_tx_extraction_matches_da_pairing`,
                            // so A4's e2e_value_outbound validation transfers here.
                            // A phantom/tampered outbound (no backing signed user tx)
                            // => reject the window (fail closed): the prover refuses to
                            // attest, so L1 will not settle it.
                            if vkey_configured {
                                match decode_postbatch(&pb.abi_calldata) {
                                    Ok(batch) => {
                                        let has_outbound = batch.inner.entries.iter().any(|e| {
                                            e.proxyEntryHash == B256::ZERO
                                                && !e.l2ToL1Calls.is_empty()
                                        });
                                        // Only an outbound-bearing settling batch needs the gate;
                                        // an inbound-only / empty batch has nothing to authorize.
                                        if has_outbound {
                                            let l2_rollup_id = batch
                                                .inner
                                                .entries
                                                .first()
                                                .and_then(|e| e.stateDeltas.first())
                                                .map(|d| d.rollupId.to::<u64>());
                                            let sync_block_rlp = window
                                                .last()
                                                .map_or(&[][..], |e| e.block.as_slice());
                                            let user_txs =
                                                outbound_user_txs_from_block(sync_block_rlp);
                                            match l2_rollup_id {
                                                Some(rid) => match verify_outbound_authorized(
                                                    &batch, &user_txs, rid,
                                                ) {
                                                    Ok(()) => info!(
                                                        "✓ outbound authorization gate — every outbound entry backed by a signed user tx",
                                                    ),
                                                    Err(e) => {
                                                        error!(
                                                            error = %e,
                                                            "A3 outbound gate REJECTED: an outbound settlement entry is not authorized by a signed user tx (phantom/tampered withdrawal)",
                                                        );
                                                        window_ok = false;
                                                    }
                                                },
                                                // An outbound-bearing settling batch MUST carry
                                                // the settlement StateDelta on entry[0] (same
                                                // anchor the 2b/2c gates bind to); its absence
                                                // means a malformed batch — fail closed.
                                                None => {
                                                    error!(
                                                        "A3 outbound gate REJECTED: outbound batch entry[0] carries no settlement StateDelta — cannot bind the L2 rollup id",
                                                    );
                                                    window_ok = false;
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(error = %e, "A3 outbound gate REJECTED: decode_postbatch failed");
                                        window_ok = false;
                                    }
                                }
                            }
                        }

                        // Fail-closed: when enforcing (a validator and/or a vkey),
                        // NEVER advance the cursor past a rejected window — reconnect
                        // to replay + re-validate it rather than skip it forever.
                        // A pure observer has no attestation to protect, so it
                        // advances and moves on. (A persistently-bad window will
                        // loop under the reconnect backoff — loud, not silent.)
                        if (validating || vkey_configured) && !window_ok {
                            // Phase 3 (driven): NO self-pick reanchor/telescope. On an
                            // OD-5 anchor mismatch do ONE bounded re-request (the composer
                            // re-dictates the oldest-unverified window), then HARD reject —
                            // never retreat-to-genesis. Soundness identical: the gate set is
                            // unchanged; a transient stale anchor heals on the re-dictate, a
                            // fabricated/wrong anchor fails OD-5 again and is hard-rejected at
                            // cap 1 (loud, fail-closed — no false attestation). The composer
                            // keeps re-dispatching the same window (it never attested), so a
                            // genuine composer bug surfaces as a loud livelock, not a skip.
                            if driven {
                                match anchor_mismatch {
                                    Some(claimed_r0) if driven_rerequest == 0 => {
                                        driven_rerequest = 1;
                                        warn!(
                                            window_end,
                                            %claimed_r0,
                                            from_block = ?vr.as_ref().map(|d| d.from_block),
                                            to_block = ?vr.as_ref().map(|d| d.to_block),
                                            "driven: OD-5 anchor mismatch on directive — re-requesting ONCE (composer re-dictates)",
                                        );
                                    }
                                    Some(claimed_r0) => {
                                        error!(
                                            window_end,
                                            %claimed_r0,
                                            "driven: OD-5 anchor mismatch PERSISTED after one re-request — HARD reject, refusing to attest",
                                        );
                                    }
                                    None => {
                                        error!(
                                            window_end,
                                            "driven: window rejected by a gate (not an anchor mismatch) — refusing to attest; re-requesting",
                                        );
                                    }
                                }
                                consecutive_failures += 1;
                                break;
                            }
                            // Self-heal a STALE resume cursor on an OD-5 anchor mismatch
                            // (NOT a hard reject): the composer's claimed `current_state`
                            // (= state(posted)) != the prover's re-executed anchor. RE-ANCHOR
                            // the resume cursor to the CONFIRMED posted by LOCATING the
                            // claimed root in `root_to_height` (the prover's own re-executed
                            // roots). SOUNDNESS: this changes only WHERE re-execution starts,
                            // never WHAT is verified — the full settlement chain re-runs over
                            // the re-executed roots, so a wrong target self-corrects (it fails
                            // OD-5 again → reject) and a fabricated root that matches NO
                            // re-executed height falls to the bounded retreat-to-genesis +
                            // self-disarm.
                            if let Some(claimed_r0) = anchor_mismatch {
                                let cached_h = root_to_height.get(&claimed_r0).copied();
                                match reanchor_move(
                                    cached_h,
                                    last_accepted,
                                    consecutive_retreats,
                                    MAX_CONSECUTIVE_RETREATS,
                                ) {
                                    // Cached BELOW last_accepted — posted RETREATED (a
                                    // wipe / deep reorg the prover already re-executed).
                                    // PRECISE retreat: replay only [H+1..], re-anchor at
                                    // the re-executed state(H) — no [1..tip] backfill.
                                    ReanchorMove::Retreat(h) => {
                                        warn!(
                                            window_end,
                                            last_accepted,
                                            %claimed_r0,
                                            retreat_to = h,
                                            "OD-5 anchor mismatch: claimed posted is a CACHED lower height — precise retreat (replay [H+1..], no full backfill)",
                                        );
                                        last_accepted = h;
                                        consecutive_retreats = 0;
                                        consecutive_failures += 1;
                                        break;
                                    }
                                    // Cached at/above last_accepted — posted ADVANCED while
                                    // the telescope was still anchored at the OLD posted (the
                                    // single stuck->steady transition: the live window
                                    // settled, posted moved, but batch_anchor was still the
                                    // old root so OD-5 mismatched once). ADVANCE to H + replay
                                    // the thin [H+1..] window and re-anchor at state(H).
                                    ReanchorMove::Advance(h) => {
                                        info!(
                                            window_end,
                                            last_accepted,
                                            %claimed_r0,
                                            advance_to = h,
                                            "OD-5 anchor mismatch: claimed posted is a CACHED higher height — posted advanced; advancing the cursor and re-anchoring",
                                        );
                                        last_accepted = h;
                                        consecutive_retreats = 0;
                                        consecutive_failures += 1;
                                        break;
                                    }
                                    // UNLOCATABLE — fabricated, or the true posted is below
                                    // this session's re-execution horizon. Bounded blunt
                                    // retreat-to-genesis + self-disarm (re-populates the cache
                                    // from block 1 on the reconnect).
                                    ReanchorMove::RetreatGenesis => {
                                        consecutive_retreats += 1;
                                        warn!(
                                            window_end,
                                            last_accepted,
                                            %claimed_r0,
                                            consecutive_retreats,
                                            "OD-5 anchor mismatch, claimed posted UNLOCATABLE in the re-executed cache — bounded retreat to genesis + re-verify from block 1",
                                        );
                                        last_accepted = 0;
                                        consecutive_failures += 1;
                                        break;
                                    }
                                    ReanchorMove::TerminalAtGenesis => {
                                        error!(
                                            window_end,
                                            %claimed_r0,
                                            "OD-5 anchor mismatch already at genesis (last_accepted=0): the composer's claimed batch anchor does not match the re-derived genesis chain — refusing to attest (a fabricated anchor, or the prover's genesis/chain-config does not match this chain)",
                                        );
                                    }
                                    ReanchorMove::BudgetExhausted => {
                                        error!(
                                            window_end,
                                            consecutive_retreats,
                                            "OD-5 anchor mismatch but the retreat budget is exhausted — refusing further retreats and staying loudly rejected (the composer may be flapping chains)",
                                        );
                                    }
                                }
                            }
                            error!(
                                window_end,
                                "refusing to advance past a rejected window; reconnecting to replay"
                            );
                            consecutive_failures += 1;
                            break;
                        }

                        // ATTEST (A.3c): every gate passed — SIGN the publicInputsHash
                        // and return it via ProofSink. The composer fills batch.proofs[]
                        // with the 65-byte signature and posts to L1. Attesting mode only.
                        // (A submit failure is logged but still advances — the composer
                        // can re-request via the feed replay; a not-advance retry is a
                        // later refinement.)
                        if let (Some(signer), Some(hash)) = (&signer, attest_hash) {
                            // Phase 3 (driven): the prover signs its OWN recomputed hash, but
                            // it MUST equal the directive's publicInputsHash — that is the key
                            // the composer's window is recorded under (mark_attested matches the
                            // SUBMITTED hash). A mismatch ⇒ the attestation matches no window ⇒
                            // the verified frontier never advances (looks like a livelock).
                            // ERROR-level: the composer's ledger hint disagrees with abi_calldata.
                            if driven {
                                if let Some(d) = &vr {
                                    if d.public_inputs_hash.as_slice() != hash.as_slice() {
                                        error!(
                                            recomputed = %hash,
                                            "driven: recomputed publicInputsHash != directive hint — attestation will NOT match the composer's window; the frontier cannot advance (signing the recomputed hash regardless)",
                                        );
                                    }
                                }
                            }
                            match signer.sign_prehash(hash) {
                                Ok(sig) => {
                                    let proof = SlotProof {
                                        l1_slot_anchor: window_start,
                                        public_inputs_hash: hash.to_vec(),
                                        post_batch_proof: sig.to_vec(),
                                    };
                                    match submit_slot_proof(&proof_sink_url, proof).await {
                                        Ok(true) => info!(
                                            window_start,
                                            %hash,
                                            "✓ ATTESTED — signed publicInputsHash + ProofSink accepted",
                                        ),
                                        Ok(false) => warn!(
                                            window_start,
                                            "ProofSink did NOT accept the attestation"
                                        ),
                                        Err(e) => warn!(error = %e, "ProofSink submit failed"),
                                    }
                                }
                                Err(e) => error!(error = ?e, "signing the publicInputsHash failed"),
                            }
                        }

                        // Always advance the in-session streaming tip (the per-chunk
                        // contiguity anchor). Advance the SETTLED resume cursor ONLY at
                        // a batch boundary (a settling window) — a non-settling chunk
                        // leaves `last_accepted` at `posted`, so a restart mid-batch
                        // re-streams from `posted+1` and re-derives the OD-5 batch anchor
                        // (`state(posted)`) by re-executing the batch's first chunk.
                        stream_tip = window_end;
                        // Carry this chunk's LAST block hash as the next chunk's
                        // expected first-block parent. L2 blocks chain by hash
                        // CONTINUOUSLY — even across a settling (batch) boundary the
                        // next batch's first block (`posted+1`) chains from this
                        // settling block — so this is NOT reset with the per-BATCH
                        // anchors below; only a reconnect (which re-declares it
                        // `None`) clears it, and that first post-reconnect chunk is
                        // re-anchored by `stream_tip` / backfill instead.
                        if let Some(last) = window.last() {
                            prev_chunk_last_hash = Some(last.block_hash.clone());
                        }
                        if is_sync {
                            // Phase 3 (driven): the composer dictated this frontier. The
                            // directive is satisfied iff this settling block IS its to_block;
                            // advance last_accepted = to_block, reset the telescope, and break
                            // to receive the next directive. A settling composition before
                            // to_block cannot occur within one posted batch (it settles only at
                            // sync_height) — guard fail-closed.
                            if driven {
                                let target = vr.as_ref().map(|d| d.to_block);
                                if Some(window_end) == target {
                                    info!(
                                        window_end,
                                        "driven: directive settled + attested — advancing the cursor; awaiting the next directive",
                                    );
                                    last_accepted = window_end;
                                    consecutive_retreats = 0;
                                    // batch_anchor_root / prev_chunk_final_root are re-declared
                                    // fresh on the next outer iteration (the break leaves this
                                    // subscribe scope), so resetting them here would be dead.
                                    break;
                                }
                                error!(
                                    window_end,
                                    ?target,
                                    "driven: settling composition at a block != directive to_block (impossible for one posted batch) — refusing + re-requesting",
                                );
                                consecutive_failures += 1;
                                break;
                            }
                            // ANCHOR-AT-CONFIRMED-POSTED. `last_accepted` tracks the
                            // CONFIRMED posted = height(claimed current_state), located
                            // in `derived_h`. We reach here only when window_ok stayed
                            // true (OD-5 + every gate passed), so the claimed R0 ==
                            // the re-executed batch anchor, which the accumulation step
                            // registered — hence `derived_h` is Some in the real arms.
                            match settling_cursor_move(derived_h, last_accepted) {
                                // POSTED ADVANCED — a genuinely new, re-executed
                                // settlement (steady state, or the live window finally
                                // landing on L1). Advance + RESET the telescope so the
                                // next batch re-anchors from its first chunk; genuine
                                // progress re-arms the unlocatable-retreat budget.
                                CursorMove::Advance(h) => {
                                    last_accepted = h;
                                    consecutive_retreats = 0;
                                    batch_anchor_root = None;
                                    prev_chunk_final_root = None;
                                }
                                // POSTED UNCHANGED — a stale / timed-out re-post of the
                                // SAME batch (the stuck state). Do NOT advance, do NOT
                                // reset the telescope: the threaded anchor stays =
                                // state(posted), so the next growing re-post telescopes
                                // forward against the SAME anchor and OD-5 keeps PASSING.
                                // No reconnect, no re-backfill — the stream stays
                                // connected and catches up to the live tip, where the
                                // in-flight deferred-post finally settles.
                                CursorMove::Retain => {
                                    info!(
                                        window_end,
                                        last_accepted,
                                        "settling window with UNCHANGED posted (claimed current_state == state(last_accepted)) — stale re-post; NOT advancing, RETAINING the telescope anchor to stay connected and avoid a re-backfill",
                                    );
                                }
                                // POSTED RETREATED but the root IS cached — a wipe / deep
                                // reorg the prover re-executed. Precise retreat (replay
                                // [H+1..], no full backfill). Mostly belt-and-suspenders:
                                // a real retreat usually trips OD-5 first and is handled
                                // in the reject branch above.
                                CursorMove::Retreat(h) => {
                                    warn!(
                                        window_end,
                                        last_accepted,
                                        retreat_to = h,
                                        "settling window carries a LOWER cached posted — precise retreat (replay [H+1..])",
                                    );
                                    last_accepted = h;
                                    consecutive_retreats = 0;
                                    // The telescope anchors are re-declared None by the
                                    // reconnect this `break` triggers, so they are not
                                    // reset here (it would be a dead write).
                                    consecutive_failures += 1;
                                    break;
                                }
                                // UNLOCATABLE — unreachable here (an OD-5-passing R0 is
                                // always cache-resident). Defensive fail-closed.
                                CursorMove::HoldDefensive => {
                                    error!(
                                        window_end,
                                        "settling window passed all gates but its claimed current_state is not in the re-executed cache — NOT advancing (defensive fail-closed)",
                                    );
                                }
                            }
                        }
                        window.clear();
                    }
                }
                Ok(None) => {
                    warn!("feed closed by the composer; reconnecting");
                    consecutive_failures += 1;
                    break;
                }
                Err(status) => {
                    warn!(error = %status, "control feed stream error; reconnecting");
                    consecutive_failures += 1;
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{U256, address};
    use eez_evm::EvmBatch;
    use eez_evm::entries::encode_postbatch;
    use eez_evm::types::RollupIdWithProofSystemsSol;

    /// A minimal finalized PostBatch — one PS, one rollup, TIMELESS — mirroring
    /// eez-evm's `carrier_batch` test helper, with the publicInputsHash the
    /// composer would claim for the given `vkey`.
    fn carrier_post_batch(vkey: B256) -> eez_control_rpc::v1::PostBatch {
        let mut batch = EvmBatch::default();
        batch.inner.blockNumber = 0; // timeless
        batch.inner.proofSystems = vec![address!("00000000000000000000000000000000000000aa")];
        batch.inner.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: U256::from(1),
            proofSystemIndex: vec![0],
        }];
        let claimed = public_inputs_hashes(&batch, vkey, None).unwrap()[0];
        eez_control_rpc::v1::PostBatch {
            abi_calldata: encode_postbatch(&batch),
            public_inputs_hash: claimed.to_vec(),
            l1_block_hash: Vec::new(),
            ..Default::default()
        }
    }

    fn control_event_with_postbatch(block_number: u64) -> eez_control_rpc::v1::ControlEvent {
        eez_control_rpc::v1::ControlEvent {
            block_number,
            composition: Some(eez_control_rpc::v1::Composition {
                post_batch: Some(eez_control_rpc::v1::PostBatch::default()),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn driven_settlement_ignores_non_target_postbatch_sidecar() {
        let mut event = control_event_with_postbatch(10);
        assert!(!driven_effective_settlement(&mut event, true, Some(20)));
        assert!(
            event.composition.is_none(),
            "stale non-target sidecar must be stripped before validation"
        );
    }

    #[test]
    fn driven_settlement_accepts_target_postbatch_sidecar() {
        let mut event = control_event_with_postbatch(20);
        assert!(driven_effective_settlement(&mut event, true, Some(20)));
        assert!(event.composition.is_some());
    }

    #[test]
    fn recompute_matches_composer_claim() {
        let vkey = B256::repeat_byte(0x42);
        let pb = carrier_post_batch(vkey);
        // The prover reconstructs the batch from abi_calldata and recomputes the
        // hash byte-for-byte — it MUST match the composer's claim.
        let got = verify_settlement_public_inputs(&pb, vkey).expect("must verify");
        assert_eq!(got.to_vec(), pb.public_inputs_hash);
    }

    #[test]
    fn wrong_vkey_fails_closed() {
        let vkey = B256::repeat_byte(0x42);
        let pb = carrier_post_batch(vkey);
        // A prover holding a DIFFERENT vkey recomputes a different hash → refused.
        assert!(verify_settlement_public_inputs(&pb, B256::repeat_byte(0x99)).is_err());
    }

    #[test]
    fn tampered_claim_fails_closed() {
        let vkey = B256::repeat_byte(0x42);
        let mut pb = carrier_post_batch(vkey);
        // Flip the composer's claimed hash → the recompute disagrees → refused.
        pb.public_inputs_hash = B256::repeat_byte(0x01).to_vec();
        assert!(verify_settlement_public_inputs(&pb, vkey).is_err());
    }

    #[test]
    fn inbound_outcome_gate_binds_shape_hash_and_bytes() {
        use alloy_primitives::{Bytes, I256};
        use eez_evm::entries::DecodedInbound;
        use eez_evm::types::{ExecutionEntrySol, StateDeltaSol};
        use eez_protocol::RollupId;

        let target = address!("00000000000000000000000000000000000000bb");
        let source = address!("00000000000000000000000000000000000000cc");
        let value = U256::ZERO;
        let data = Bytes::from(vec![0x12, 0x34]);
        let ret = Bytes::from(vec![0xab, 0xcd]); // the proven Y
        let rollup = RollupId(1);

        // H the user's proxy computes on-chain (settled_rollup, …, MAINNET=0).
        let h = eez_evm::cross_chain_call_hash(rollup, target, value, &data, source, RollupId(0));

        let entry = |proxy: B256, ret_data: Bytes, deltas: Vec<StateDeltaSol>| ExecutionEntrySol {
            stateDeltas: deltas,
            proxyEntryHash: proxy,
            destinationRollupId: U256::from(1),
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::ZERO,
            returnData: ret_data,
            rollingHash: B256::ZERO,
        };
        let delta = || {
            vec![StateDeltaSol {
                rollupId: U256::from(1),
                currentState: B256::ZERO,
                newState: B256::ZERO,
                etherDelta: I256::ZERO,
            }]
        };
        // eez0's REAL settling layout: a leading-immediate entry (proxyEntryHash 0,
        // empty returnData, carrying the settlement StateDelta) PREPENDED before the
        // deferred inbound entry (proxyEntryHash H, returnData Y). The gate must find
        // the deferred entry BY H, not at entries[0] — a verbatim based port reading
        // entries.first() here would read the immediate entry and REJECT honest inbound.
        let immediate = || entry(B256::ZERO, Bytes::new(), delta());
        let deferred = || entry(h, ret.clone(), Vec::new());
        let mut batch = EvmBatch::default();
        batch.inner.entries = vec![immediate(), deferred()];

        // The REAL L2-sealed call: success, returns Y, keyed on (target,value,data,source).
        let d = DecodedInbound {
            target,
            value,
            data: data.clone(),
            source,
            return_data: ret.clone(),
            success: true,
        };

        // The 1:1 case of the bijection (a single sealed inbound). Honest delivery:
        // the deferred entry (entries[1]) binds H + returns Y, no failed lookup → OK.
        // (Located by the positional bijection over `proxyEntryHash != 0` entries —
        // the immediate at entries[0] is proxyEntryHash 0 and excluded.)
        assert!(multi_inbound_outcome_gate(&batch, std::slice::from_ref(&d)).is_ok());

        // Forged hash: composer keys the delivery on a hash the user never consumes → REFUSE.
        let mut forged = batch.clone();
        forged.inner.entries[1].proxyEntryHash = B256::repeat_byte(0x99);
        assert!(multi_inbound_outcome_gate(&forged, std::slice::from_ref(&d)).is_err());

        // Wrong bytes: delivers X on L2 but settles a different Y' on L1 → REFUSE.
        let mut wrong_bytes = batch.clone();
        wrong_bytes.inner.entries[1].returnData = Bytes::from(vec![0xff]);
        assert!(multi_inbound_outcome_gate(&wrong_bytes, std::slice::from_ref(&d)).is_err());

        // No settlement StateDelta on the immediate entry → cannot bind H → REFUSE.
        let mut no_delta = batch.clone();
        no_delta.inner.entries[0].stateDeltas = Vec::new();
        assert!(multi_inbound_outcome_gate(&no_delta, std::slice::from_ref(&d)).is_err());
    }

    // ── GAP-3 multi-delivery bijection helpers + tests ───────────────────

    /// One inbound deferred entry (proxyEntryHash != 0 + the proven returnData).
    #[cfg(test)]
    fn mi_deferred_entry(
        proxy: B256,
        ret_data: alloy_primitives::Bytes,
    ) -> eez_evm::types::ExecutionEntrySol {
        eez_evm::types::ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: proxy,
            destinationRollupId: U256::from(1),
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::ZERO,
            returnData: ret_data,
            rollingHash: B256::ZERO,
        }
    }

    /// The leading-immediate entry carrying the settlement StateDelta (anchors H).
    #[cfg(test)]
    fn mi_immediate_entry(rollup: u64) -> eez_evm::types::ExecutionEntrySol {
        use alloy_primitives::I256;
        let mut e = mi_deferred_entry(B256::ZERO, alloy_primitives::Bytes::new());
        e.stateDeltas = vec![eez_evm::types::StateDeltaSol {
            rollupId: U256::from(rollup),
            currentState: B256::ZERO,
            newState: B256::ZERO,
            etherDelta: I256::ZERO,
        }];
        e
    }

    /// A `DecodedInbound` with distinct bytes per `tag`, success by default.
    #[cfg(test)]
    fn mi_sealed(tag: u8, success: bool) -> eez_evm::entries::DecodedInbound {
        eez_evm::entries::DecodedInbound {
            target: address!("00000000000000000000000000000000000000bb"),
            value: U256::ZERO,
            data: alloy_primitives::Bytes::from(vec![tag]),
            source: address!("00000000000000000000000000000000000000cc"),
            return_data: alloy_primitives::Bytes::from(vec![0xa0 | tag]),
            success,
        }
    }

    #[cfg(test)]
    fn mi_hash(d: &eez_evm::entries::DecodedInbound, rollup: u64) -> B256 {
        eez_evm::cross_chain_call_hash(
            eez_protocol::RollupId(rollup),
            d.target,
            d.value,
            &d.data,
            d.source,
            eez_protocol::RollupId(0),
        )
    }

    #[test]
    fn multi_inbound_two_distinct_inbounds_bijection_accepts() {
        // GAP-3: a window sealing TWO distinct inbound deliveries; the batch carries
        // two deferred entries in the SAME consumption order. The N:N bijection
        // matches each positionally (hash + bytes) and accepts.
        let s0 = mi_sealed(1, true);
        let s1 = mi_sealed(2, true);
        let (h0, h1) = (mi_hash(&s0, 1), mi_hash(&s1, 1));
        let mut batch = EvmBatch::default();
        batch.inner.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h0, s0.return_data.clone()),
            mi_deferred_entry(h1, s1.return_data.clone()),
        ];
        multi_inbound_outcome_gate(&batch, &[s0.clone(), s1.clone()])
            .expect("two distinct inbounds biject to two deferred entries in order");

        // Cross-wired returnData (entry #0 carries s1's bytes) → per-pair bytes
        // mismatch → REFUSE (the X-on-L2 / Y-on-L1 equivocation, multi-call form).
        let mut swapped = batch.clone();
        swapped.inner.entries[1].returnData = s1.return_data.clone();
        swapped.inner.entries[2].returnData = s0.return_data.clone();
        assert!(multi_inbound_outcome_gate(&swapped, &[s0, s1]).is_err());
    }

    #[test]
    fn multi_inbound_duplicate_identical_calls_map_to_distinct_entries() {
        // Two LEGITIMATELY-identical inbounds (same target/value/data/source ⇒ same
        // H) must consume TWO distinct deferred entries, in order — not collapse to
        // one by hash-set membership.
        let s = mi_sealed(7, true);
        let h = mi_hash(&s, 1);
        let mut batch = EvmBatch::default();
        batch.inner.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h, s.return_data.clone()),
            mi_deferred_entry(h, s.return_data.clone()),
        ];
        multi_inbound_outcome_gate(&batch, &[s.clone(), s.clone()])
            .expect("two identical inbounds biject to two identical-hash deferred entries");

        // Only ONE deferred entry for two identical sealed inbounds → cardinality
        // mismatch (an unmatched delivery) → REFUSE (no hash-set collapse).
        let mut one_entry = EvmBatch::default();
        one_entry.inner.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h, s.return_data.clone()),
        ];
        assert!(multi_inbound_outcome_gate(&one_entry, &[s.clone(), s]).is_err());
    }

    #[test]
    fn multi_inbound_phantom_entry_rejects() {
        // PHANTOM: the batch settles a deferred inbound entry the L2 NEVER ran (zero
        // sealed inbounds). Cardinality 0 sealed != 1 deferred → REFUSE.
        let s = mi_sealed(1, true);
        let h = mi_hash(&s, 1);
        let mut batch = EvmBatch::default();
        batch.inner.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h, s.return_data.clone()),
        ];
        assert!(
            multi_inbound_outcome_gate(&batch, &[]).is_err(),
            "a deferred entry with no sealed inbound is a phantom delivery"
        );
        // And a CLEAN inbound-free batch (no deferred entries, no sealed) → OK.
        let mut empty = EvmBatch::default();
        empty.inner.entries = vec![mi_immediate_entry(1)];
        multi_inbound_outcome_gate(&empty, &[]).expect("no inbounds, no deferred entries → OK");
    }

    #[test]
    fn multi_inbound_unmatched_delivery_rejects() {
        // UNMATCHED: the L2 sealed an inbound the batch does NOT settle (the batch
        // carries no deferred entry for it). 1 sealed != 0 deferred → REFUSE.
        let s = mi_sealed(1, true);
        let mut batch = EvmBatch::default();
        batch.inner.entries = vec![mi_immediate_entry(1)];
        assert!(multi_inbound_outcome_gate(&batch, &[s]).is_err());
    }

    #[test]
    fn multi_inbound_double_match_rejects() {
        // DOUBLE-MATCH attempt: two sealed inbounds, but the batch carries one
        // deferred entry whose hash matches BOTH (identical calls). Positional
        // cardinality (2 sealed vs 1 deferred) catches it — a single entry can't be
        // consumed twice. (Same shape as the duplicate-identical single-entry case;
        // asserted here from the double-match framing.)
        let s = mi_sealed(3, true);
        let h = mi_hash(&s, 1);
        let mut batch = EvmBatch::default();
        batch.inner.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h, s.return_data.clone()),
        ];
        assert!(
            multi_inbound_outcome_gate(&batch, &[s.clone(), s]).is_err(),
            "one deferred entry cannot back two sealed inbounds (no double-match)",
        );
    }

    #[test]
    fn multi_inbound_phantom_failed_lookup_rejects() {
        // The FAILURE half (dormant but rigorous): a `failed` LookupCall with no
        // sealed failure inbound is a phantom revert (L1 would revert the user's
        // consume on a call L2 never failed) → REFUSE.
        let s = mi_sealed(1, true);
        let h = mi_hash(&s, 1);
        let mut batch = EvmBatch::default();
        batch.inner.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h, s.return_data.clone()),
        ];
        // A leftover failed lookup with no backing sealed failure.
        batch.inner.l1ToL2lookupCalls = vec![eez_evm::types::LookupCallSol {
            crossChainCallHash: B256::repeat_byte(0xfe),
            destinationRollupId: U256::from(1),
            returnData: alloy_primitives::Bytes::from(vec![0xde]),
            failed: true,
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::ZERO,
            rollingHash: B256::ZERO,
            expectedStateRoots: Vec::new(),
        }];
        assert!(
            multi_inbound_outcome_gate(&batch, std::slice::from_ref(&s)).is_err(),
            "a failed lookup with no sealed failure inbound is a phantom revert"
        );
    }

    #[test]
    fn outbound_gate_authorizes_via_signed_user_tx() {
        use alloy_consensus::TxLegacy;
        use alloy_eips::eip2718::Encodable2718 as _;
        use alloy_network::TxSignerSync as _;
        use alloy_primitives::{Address, Bytes, TxKind};
        use alloy_signer_local::PrivateKeySigner;
        use eez_evm::types::{ExecutionEntrySol, L2ToL1CallSol};
        use reth_ethereum_primitives::{Transaction, TransactionSigned};

        let signer = PrivateKeySigner::random();
        let source = signer.address();
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let proxy = eez_evm::outbound_gate::compute_cross_chain_proxy_address(target, 0);
        let value = U256::from(7u64);
        let data = vec![0x12u8, 0x34];
        let l2 = 1u64;

        let sign = |to: Address, value: U256, input: Vec<u8>, who: &PrivateKeySigner| -> Bytes {
            let mut tx = TxLegacy {
                chain_id: Some(1u64),
                nonce: 0,
                gas_price: 1,
                gas_limit: 21_000,
                to: TxKind::Call(to),
                value,
                input: input.into(),
            };
            let sig = who.sign_transaction_sync(&mut tx).expect("sign");
            let signed = TransactionSigned::new_unhashed(Transaction::Legacy(tx), sig);
            let mut b = Vec::new();
            signed.encode_2718(&mut b);
            Bytes::from(b)
        };
        let mk = |proxy_hash: B256, calls: Vec<L2ToL1CallSol>| ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: proxy_hash,
            destinationRollupId: U256::from(l2),
            callCount: U256::from(calls.len() as u64),
            l2ToL1Calls: calls,
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        };
        let call = || L2ToL1CallSol {
            targetAddress: target,
            value,
            data: Bytes::from(data.clone()),
            sourceAddress: source,
            sourceRollupId: U256::from(l2),
            revertSpan: U256::ZERO,
        };
        // Layout: a leading anchor (proxyEntryHash 0, EMPTY calls — the wrapper's
        // filter drops it) + the OUTBOUND settlement entry (proxyEntryHash 0, one
        // call) + an inbound deferred (proxyEntryHash != 0 — dropped). Only the
        // middle entry is gated, paired with `user_txs[0]`.
        let mut batch = EvmBatch::default();
        batch.inner.entries = vec![
            mk(B256::ZERO, Vec::new()),
            mk(B256::ZERO, vec![call()]),
            mk(B256::repeat_byte(0x99), Vec::new()),
        ];

        let good = sign(proxy, value, data.clone(), &signer);
        // Authorized by the genuine signed user tx → PASS.
        assert!(verify_outbound_authorized(&batch, std::slice::from_ref(&good), l2).is_ok());
        // No paired user tx → PHANTOM withdrawal → REJECT.
        assert!(verify_outbound_authorized(&batch, &[], l2).is_err());
        // A DIFFERENT EOA signed it → not the claimed source → REJECT.
        let other = PrivateKeySigner::random();
        let wrong = sign(proxy, value, data.clone(), &other);
        assert!(verify_outbound_authorized(&batch, std::slice::from_ref(&wrong), l2).is_err());
        // Tampered claim: entry says value 999 but the user signed 7 → REJECT.
        let mut tampered = batch.clone();
        tampered.inner.entries[1].l2ToL1Calls[0].value = U256::from(999u64);
        assert!(verify_outbound_authorized(&tampered, std::slice::from_ref(&good), l2).is_err());
    }

    /// A3<->A4 EXTRACTION EQUIVALENCE: the outbound user txs
    /// `outbound_user_txs_from_block` pulls from a sealed Sync block (the A3 path)
    /// are EXACTLY the outbound user txs the deriver pairs from DA (the A4 path).
    /// Built through the SAME shared builder both sides use
    /// (`build_cross_chain_sync_pairs` -> `interleave_sync_block_txs`), this proves
    /// A3's RLP extraction == A4's DA pairing — so A4's end-to-end validation
    /// (e2e_value_outbound) transfers to A3, which is why BOTH gates can run HARD
    /// without a separate prover-stack run. Also locks in the EEZL2_ADDR (NOT
    /// stale CCM_ADDRESS=0xeeee) system-tx filter: a wrong address would leak the
    /// load tx into the result and fail this assertion.
    #[test]
    fn outbound_user_tx_extraction_matches_da_pairing() {
        use alloy_consensus::{Header, TxEip1559};
        use alloy_eips::eip2718::{Decodable2718 as _, Encodable2718 as _};
        use alloy_network::TxSignerSync as _;
        use alloy_primitives::{Bytes, TxKind, b256};
        use alloy_rlp::Encodable as _;
        use alloy_signer_local::PrivateKeySigner;
        use eez_evm::system_tx::{
            SystemTxContext, build_cross_chain_sync_pairs, interleave_sync_block_txs,
        };
        use eez_evm::types::{ExecutionEntrySol, L2ToL1CallSol};
        use reth_ethereum_primitives::{Block, BlockBody, Transaction, TransactionSigned};

        // SYSTEM signer = the key for SYSTEM_ADDRESS (anvil#0), so the load tx
        // recovers to SYSTEM_ADDRESS and the EEZL2_ADDR filter excludes it.
        let system_signer = PrivateKeySigner::from_bytes(&b256!(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        ))
        .unwrap();
        assert_eq!(
            system_signer.address(),
            eez_evm::SYSTEM_ADDRESS,
            "anvil#0 must be SYSTEM_ADDRESS"
        );

        let cfg = SystemTxContext {
            system_signer,
            ccm_l2_address: eez_evm::outbound_gate::EEZL2_ADDR,
            l2_chain_id: 1,
            l2_gas_price: 1,
            l2_gas_limit: 2_000_000,
            this_rollup_id: 1,
        };

        // The outbound user tx (an EOA -> the L2 outbound proxy for `target`).
        let user_key = PrivateKeySigner::random();
        let source = user_key.address();
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let proxy = eez_evm::outbound_gate::compute_cross_chain_proxy_address(target, 0);
        let value = U256::from(123u64);
        let data = vec![0xabu8, 0xcd, 0xef];

        // PRODUCTION tx type: the composer's outbound user tx is EIP-1559
        // (common/mod.rs::send_outbound_set_value). Exercising it here proves the
        // RLP extraction + recover/value/input/to all handle the real tx envelope,
        // not just legacy — the bit a synthetic legacy-only test would miss.
        let user_tx: Bytes = {
            let mut tx = TxEip1559 {
                chain_id: 1u64,
                nonce: 0,
                gas_limit: 100_000,
                max_fee_per_gas: 1,
                max_priority_fee_per_gas: 0,
                to: TxKind::Call(proxy),
                value,
                access_list: Default::default(),
                input: data.clone().into(),
            };
            let sig = user_key.sign_transaction_sync(&mut tx).unwrap();
            let signed = TransactionSigned::new_unhashed(Transaction::Eip1559(tx), sig);
            let mut b = Vec::new();
            signed.encode_2718(&mut b);
            Bytes::from(b)
        };

        // The L1 outbound settlement entry (proxyEntryHash 0, the L2->L1 call).
        let entry = ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: B256::ZERO,
            destinationRollupId: U256::from(1),
            callCount: U256::from(1u8),
            l2ToL1Calls: vec![L2ToL1CallSol {
                targetAddress: target,
                value,
                data: Bytes::from(data.clone()),
                sourceAddress: source,
                sourceRollupId: U256::from(1u64),
                revertSpan: U256::ZERO,
            }],
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        };

        // THE shared builder -> the canonical Sync-block tx list [load(sys), user].
        let pairs =
            build_cross_chain_sync_pairs(&[(entry, user_tx.clone())], &[], &cfg, 0).unwrap();
        let sync_txs = interleave_sync_block_txs(&pairs);
        assert!(sync_txs.len() >= 2, "at least load + user");

        // Seal them into a Block RLP (what A3 reads from the window's last event).
        let txs: Vec<TransactionSigned> = sync_txs
            .iter()
            .map(|b| TransactionSigned::decode_2718(&mut b.as_ref()).unwrap())
            .collect();
        let block = Block {
            header: Header::default(),
            body: BlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: None,
            },
        };
        let mut rlp = Vec::new();
        block.encode(&mut rlp);

        // A3 extraction == the single outbound user tx A4 pairs from DA.
        let extracted = outbound_user_txs_from_block(&rlp);
        assert_eq!(
            extracted,
            vec![user_tx],
            "A3 RLP extraction must equal the DA-paired outbound user tx"
        );
    }

    /// K>=2: TWO outbound immediates in ONE Sync slot. Exercises (P1.3) the
    /// builder's multi-entry interleave (`[load0,user0,load1,user1]`, two-phase
    /// SYSTEM_ADDRESS nonces) and the A3 extraction+gate over MULTIPLE entries
    /// (each user tx pairs positionally with its entry, all binds hold), and
    /// (P1.4) the composition determinism that A2b/A4 rely on: the SAME shared
    /// builder fed the SAME inputs yields byte-identical Sync-block txs.
    #[test]
    fn outbound_gate_k2_multiple_immediates_one_slot() {
        use alloy_consensus::{Header, TxLegacy};
        use alloy_eips::eip2718::{Decodable2718 as _, Encodable2718 as _};
        use alloy_network::TxSignerSync as _;
        use alloy_primitives::{Address, Bytes, TxKind, b256};
        use alloy_rlp::Encodable as _;
        use alloy_signer_local::PrivateKeySigner;
        use eez_evm::system_tx::{
            SystemTxContext, build_cross_chain_sync_pairs, interleave_sync_block_txs,
        };
        use eez_evm::types::{ExecutionEntrySol, L2ToL1CallSol};
        use reth_ethereum_primitives::{Block, BlockBody, Transaction, TransactionSigned};

        let system_signer = PrivateKeySigner::from_bytes(&b256!(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        ))
        .unwrap();
        let cfg = SystemTxContext {
            system_signer,
            ccm_l2_address: eez_evm::outbound_gate::EEZL2_ADDR,
            l2_chain_id: 1,
            l2_gas_price: 1,
            l2_gas_limit: 2_000_000,
            this_rollup_id: 1,
        };

        // Build one (outbound entry, signed user tx) for a distinct (target, EOA).
        let mk_pair = |target: Address, value_wei: u64, data: Vec<u8>| {
            let user_key = PrivateKeySigner::random();
            let source = user_key.address();
            let proxy = eez_evm::outbound_gate::compute_cross_chain_proxy_address(target, 0);
            let value = U256::from(value_wei);
            let user_tx: Bytes = {
                let mut tx = TxLegacy {
                    chain_id: Some(1u64),
                    nonce: 0,
                    gas_price: 1,
                    gas_limit: 100_000,
                    to: TxKind::Call(proxy),
                    value,
                    input: data.clone().into(),
                };
                let sig = user_key.sign_transaction_sync(&mut tx).unwrap();
                let signed = TransactionSigned::new_unhashed(Transaction::Legacy(tx), sig);
                let mut b = Vec::new();
                signed.encode_2718(&mut b);
                Bytes::from(b)
            };
            let entry = ExecutionEntrySol {
                stateDeltas: Vec::new(),
                proxyEntryHash: B256::ZERO,
                destinationRollupId: U256::from(1),
                callCount: U256::from(1u8),
                l2ToL1Calls: vec![L2ToL1CallSol {
                    targetAddress: target,
                    value,
                    data: Bytes::from(data),
                    sourceAddress: source,
                    sourceRollupId: U256::from(1u64),
                    revertSpan: U256::ZERO,
                }],
                expectedL1ToL2Calls: Vec::new(),
                expectedLookups: Vec::new(),
                returnData: Bytes::new(),
                rollingHash: B256::ZERO,
            };
            (entry, user_tx)
        };

        let p0 = mk_pair(
            address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9"),
            111,
            vec![0x01, 0x02],
        );
        let p1 = mk_pair(
            address!("00000000000000000000000000000000000000bb"),
            222,
            vec![0x03, 0x04, 0x05],
        );
        let outbound = vec![p0.clone(), p1.clone()];

        // P1.4 — composition determinism: same inputs -> byte-identical Sync txs.
        let pairs_a = build_cross_chain_sync_pairs(&outbound, &[], &cfg, 0).unwrap();
        let pairs_b = build_cross_chain_sync_pairs(&outbound, &[], &cfg, 0).unwrap();
        let sync_a = interleave_sync_block_txs(&pairs_a);
        let sync_b = interleave_sync_block_txs(&pairs_b);
        assert_eq!(sync_a, sync_b, "shared builder must be deterministic");
        assert_eq!(sync_a.len(), 4, "K=2 -> [load0,user0,load1,user1]");

        // Seal into a Block RLP (what A3 reads from the window's last event).
        let txs: Vec<TransactionSigned> = sync_a
            .iter()
            .map(|b| TransactionSigned::decode_2718(&mut b.as_ref()).unwrap())
            .collect();
        let block = Block {
            header: Header::default(),
            body: BlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: None,
            },
        };
        let mut rlp = Vec::new();
        block.encode(&mut rlp);

        // P1.3 — A3 extraction recovers BOTH outbound user txs, IN ORDER.
        let extracted = outbound_user_txs_from_block(&rlp);
        assert_eq!(
            extracted,
            vec![p0.1.clone(), p1.1.clone()],
            "K=2 extraction must equal the two DA-paired user txs, in slot order"
        );

        // And the gate authorizes BOTH (each entry paired positionally with its tx).
        let entries = vec![p0.0.clone(), p1.0.clone()];
        assert!(
            eez_evm::outbound_gate::verify_outbound_authorized(&entries, &extracted, 1).is_ok(),
            "both K=2 outbound immediates must be authorized by their paired user txs"
        );

        // Swapping the two user txs breaks the per-entry binds -> reject (proves the
        // pairing is positional, not set-membership).
        let swapped = vec![p1.1.clone(), p0.1.clone()];
        assert!(
            eez_evm::outbound_gate::verify_outbound_authorized(&entries, &swapped, 1).is_err(),
            "mispaired (swapped) user txs must be rejected"
        );
    }

    #[test]
    fn real_captured_fixture_verifies() {
        // Validate the gate against a REAL settling PostBatch captured from a live
        // composer run (block 13, embedded in tests/fixtures/) — confirms the
        // recompute matches real composer output, not just synthetic carriers.
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/postbatch-13.json"
        );
        let raw = std::fs::read_to_string(path).expect("embedded fixture postbatch-13.json");
        let j: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let dec = |k: &str| hex::decode(j[k].as_str().unwrap().trim_start_matches("0x")).unwrap();
        let pb = eez_control_rpc::v1::PostBatch {
            abi_calldata: dec("abi_calldata"),
            public_inputs_hash: dec("public_inputs_hash"),
            l1_block_hash: dec("l1_block_hash"), // 0x → empty → timeless
            ..Default::default()
        };
        // MockEcdsaProver vkey = bytes32(uint160(authorizedSigner = hardhat #0)).
        let vkey: B256 = "0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266"
            .parse()
            .unwrap();
        let got = verify_settlement_public_inputs(&pb, vkey)
            .expect("real captured PostBatch must verify against the mock prover vkey");
        assert_eq!(got.to_vec(), pb.public_inputs_hash);
    }

    #[test]
    fn malformed_calldata_fails_closed() {
        // Garbage abi_calldata can't decode → refused, not panicked.
        let pb = eez_control_rpc::v1::PostBatch {
            abi_calldata: vec![0xde, 0xad, 0xbe, 0xef],
            public_inputs_hash: B256::ZERO.to_vec(),
            ..Default::default()
        };
        assert!(verify_settlement_public_inputs(&pb, B256::ZERO).is_err());
    }

    // ── settlement-chain gate (step 2b) ──────────────────────────────────

    /// Decode the embedded real fixture's batch (block 13) — its entry +
    /// StateDelta are the well-formed template the chain helper clones.
    fn fixture_batch() -> EvmBatch {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/postbatch-13.json"
        );
        let raw = std::fs::read_to_string(path).expect("embedded fixture postbatch-13.json");
        let j: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let calldata =
            hex::decode(j["abi_calldata"].as_str().unwrap().trim_start_matches("0x")).unwrap();
        decode_postbatch(&calldata).unwrap()
    }

    /// A PostBatch whose decoded batch carries `deltas` as the Position-B chain
    /// (one delta per entry). Clones the fixture entry/delta (ExecutionEntrySol
    /// has no Default) and overrides only the rollupId + the two roots.
    fn post_batch_with_chain(deltas: &[(u64, B256, B256)]) -> eez_control_rpc::v1::PostBatch {
        let template = fixture_batch().inner.entries[0].clone();
        let template_delta = template.stateDeltas[0].clone();
        let mut batch = EvmBatch::default();
        batch.inner.entries = deltas
            .iter()
            .map(|&(rid, cur, new)| {
                let mut e = template.clone();
                let mut d = template_delta.clone();
                d.rollupId = U256::from(rid);
                d.currentState = cur;
                d.newState = new;
                e.stateDeltas = vec![d];
                e
            })
            .collect();
        eez_control_rpc::v1::PostBatch {
            abi_calldata: encode_postbatch(&batch),
            ..Default::default()
        }
    }

    fn verified(parent: B256, final_root: B256) -> VerifiedWindow {
        VerifiedWindow {
            parent_state_root: parent,
            final_state_root: final_root,
            per_block_roots: None,
            sync_per_tx_roots: None,
            sync_tx_statuses: None,
        }
    }

    #[test]
    fn settlement_chain_endpoints_anchor_ok() {
        let (a, b) = (B256::repeat_byte(0xa0), B256::repeat_byte(0xb0));
        let pb = post_batch_with_chain(&[(1, a, b)]);
        // Single-chunk batch: the batch anchor == the chunk's re-executed parent A.
        verify_settlement_chain(&pb, &verified(a, b), a, b"").expect("R0==anchor, R_N==final → OK");
    }

    #[test]
    fn settlement_chain_endpoint_mismatch_fails_closed() {
        let (a, b, c) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0xc0),
        );
        let pb = post_batch_with_chain(&[(1, a, b)]);
        // The re-executed final root is C, not the claimed B → endpoint refused.
        assert!(verify_settlement_chain(&pb, &verified(a, c), a, b"").is_err());
    }

    #[test]
    fn settlement_chain_parent_anchor_mismatch_fails_closed() {
        let (a, b, x) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0x11),
        );
        let pb = post_batch_with_chain(&[(1, a, b)]);
        // The re-executed batch anchor is X, not the claimed R0=A → OD-5 refused.
        assert!(verify_settlement_chain(&pb, &verified(x, b), x, b"").is_err());
    }

    #[test]
    fn settlement_chain_telescoping_break_fails_closed() {
        let (a, b, c, d) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0xc0),
            B256::repeat_byte(0xd0),
        );
        // e0: A->B, e1: C->D (C != B → the chain doesn't telescope). Endpoints OK.
        let pb = post_batch_with_chain(&[(1, a, b), (1, c, d)]);
        assert!(verify_settlement_chain(&pb, &verified(a, d), a, b"").is_err());
    }

    #[test]
    fn settlement_chain_multi_inbound_interior_collapse_ok() {
        // BLOCKER #3: a multi-INBOUND settling batch. The composer chains
        //   [immediate(r0->pre_sync) | inbound1(pre_sync->rn) | inbound2(rn->rn) | inbound3(rn->rn)]
        // For a single-block settling window pre_sync == r0 == parent_state_root,
        // and rn == final_state_root. So the interior boundaries are exactly
        //   boundary1 = r0 (= parent_state_root, the no-op-immediate's newState),
        //   boundary2 = rn (= final_state_root, the collapsed deferred boundary),
        //   boundary3 = rn (= final_state_root).
        // All three legitimately EQUAL an endpoint AND are proven re-executed
        // roots — the pre-fix "equals an endpoint → replay re-arm" bail
        // false-rejected this honest batch. With proven-root recognition it
        // passes.
        let (r0, rn) = (B256::repeat_byte(0xa0), B256::repeat_byte(0xb0));
        let pb = post_batch_with_chain(&[
            (1, r0, r0), // leading no-op immediate (pre_sync == r0, single-block window)
            (1, r0, rn), // inbound1: telescopes to the final root
            (1, rn, rn), // inbound2: collapsed onto the final root
            (1, rn, rn), // inbound3: collapsed onto the final root
        ]);
        // Single-block settling window: parent_state_root == r0, final == rn.
        verify_settlement_chain(&pb, &verified(r0, rn), r0, b"")
            .expect("multi-inbound collapse: interiors equal proven endpoints → OK");
    }

    #[test]
    fn settlement_chain_multi_inbound_multiblock_interior_ok() {
        // A MULTI-BLOCK settling window: the no-op-immediate's newState is
        // state(sync_block-1) = an intermediate re-executed root M that is
        // NEITHER r0 NOR rn but IS the validator's per-tx root. The collapsed
        // deferred boundaries are still rn (final_state_root). Both proven.
        let (r0, m, rn) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0x4d),
            B256::repeat_byte(0xb0),
        );
        let pb = post_batch_with_chain(&[
            (1, r0, m),  // immediate: r0 -> state(sync_block-1) = M
            (1, m, rn),  // inbound1: M -> final
            (1, rn, rn), // inbound2: collapsed onto final
        ]);
        // In a MULTI-BLOCK settling chunk M = state(sync_block-1) is the
        // SECOND-TO-LAST block's re-executed root, exposed only via per-block
        // roots (NOT a Sync-block per-tx root, NOT the window's top-level
        // parent). The validator emits `state_root` per block; the prover
        // collects them into `per_block_roots`.
        let vw = VerifiedWindow {
            parent_state_root: r0,
            final_state_root: rn,
            per_block_roots: Some(vec![r0, m, rn]), // [parent.., B_{m-1}=M, sync=rn]
            sync_per_tx_roots: None,
            sync_tx_statuses: None,
        };
        verify_settlement_chain(&pb, &vw, r0, b"")
            .expect("multi-block multi-inbound: M is a proven per-block root, rn is final → OK");
    }

    #[test]
    fn settlement_chain_fabricated_interior_endpoint_rejects() {
        // The replay-re-arm guard MUST still reject a REAL-looking interior that
        // equals an endpoint but is NOT a proven re-executed root. Construct a
        // window whose proven roots are r0 and rn, but craft an interior = X that
        // happens to equal rn while the validator did NOT re-execute that boundary
        // as a real intermediate root. We force the hazard by making rn a value
        // the validator's re-execution does NOT corroborate as final.
        let (r0, rn, x) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0xcc),
        );
        // Chain telescopes r0 -> X -> rn with an interior X that is NOT r0/rn and
        // NOT any proven root (no per-tx roots emitted) → refused.
        let pb = post_batch_with_chain(&[(1, r0, x), (1, x, rn)]);
        let vw = VerifiedWindow {
            parent_state_root: r0,
            final_state_root: rn,
            per_block_roots: Some(vec![r0, rn]), // re-execution does NOT contain X
            sync_per_tx_roots: None,             // validator corroborates NO interior
            sync_tx_statuses: None,
        };
        assert!(
            verify_settlement_chain(&pb, &vw, r0, b"").is_err(),
            "a fabricated interior X (no proven root, no placeholder) must be refused"
        );
    }

    #[test]
    fn settlement_chain_interior_equals_endpoint_unproven_rejects() {
        // The pointed case in the mandate: a REAL interior root that COINCIDES
        // with an endpoint but is NOT corroborated by re-execution must still be
        // rejected. Here the validator's PROVEN final root is C (not B); the
        // composer claims rn=B and crafts interior=B (= its own claimed endpoint).
        // Because B != final_state_root (C), B is NOT a proven root → the endpoint
        // gate already rejects rn=B != C; even if it slipped past, interior B is
        // not proven → refused. Either way: fail-closed.
        let (r0, b, c) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0xc0),
        );
        let pb = post_batch_with_chain(&[(1, r0, b), (1, b, b)]);
        let vw = VerifiedWindow {
            parent_state_root: r0,
            final_state_root: c, // re-execution says the final root is C, not B
            per_block_roots: Some(vec![r0, c]), // B is NOT a re-executed root
            sync_per_tx_roots: None,
            sync_tx_statuses: None,
        };
        assert!(
            verify_settlement_chain(&pb, &vw, r0, b"").is_err(),
            "interior B coinciding with a CLAIMED-but-unproven endpoint must be refused"
        );
    }

    #[test]
    fn settlement_chain_multi_rollup_fails_closed() {
        let (a, b, d) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0xd0),
        );
        // Telescopes A->B->D but entry 1 settles a DIFFERENT rollup.
        let pb = post_batch_with_chain(&[(1, a, b), (2, b, d)]);
        assert!(verify_settlement_chain(&pb, &verified(a, d), a, b"").is_err());
    }

    // --- classify_settlement_chain: the retreatable/hard/ok 3-way split that the
    // prover loop keys its self-healing cursor retreat on. The discrimination must
    // be EXACT: ONLY a `state(posted)` anchor mismatch is retreatable; every other
    // failure (endpoint / telescope / single-rollup / interior) is a hard reject
    // that must keep the cursor put, and a clean window is Ok.

    #[test]
    fn classify_anchor_mismatch_is_retreatable() {
        let (a, b, x) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0x11),
        );
        let pb = post_batch_with_chain(&[(1, a, b)]);
        // Claimed R0 = A, but the RE-EXECUTED batch anchor is X (the resume cursor
        // is stale vs posted). This is the RETREATABLE OD-5 mismatch — and it must
        // carry BOTH roots so the loop can log + heal. The re-executed final (B)
        // still matches the claim, so the ONLY discrepancy is the anchor.
        assert!(
            matches!(
                classify_settlement_chain(&pb, &verified(x, b), x, b""),
                SettlementVerdict::AnchorMismatch { claimed_r0, reexecuted }
                    if claimed_r0 == a && reexecuted == x
            ),
            "claimed R0=A vs re-executed anchor=X must classify as a retreatable AnchorMismatch carrying both roots",
        );
    }

    #[test]
    fn classify_endpoint_mismatch_is_hard_reject() {
        let (a, b, c) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0xc0),
        );
        let pb = post_batch_with_chain(&[(1, a, b)]);
        // Anchor AGREES (A == A) but the re-executed final root is C != claimed B.
        // An endpoint failure must NEVER be mistaken for a retreatable anchor
        // mismatch — re-executing from elsewhere can't make a bad endpoint valid.
        assert!(
            matches!(
                classify_settlement_chain(&pb, &verified(a, c), a, b""),
                SettlementVerdict::HardReject(_)
            ),
            "an endpoint mismatch with a matching anchor must be a HardReject, not retreatable",
        );
    }

    #[test]
    fn classify_telescoping_break_is_hard_reject() {
        let (a, b, c, d) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0xb0),
            B256::repeat_byte(0xc0),
            B256::repeat_byte(0xd0),
        );
        // Anchor agrees (A == A), endpoints agree (D == D), but the chain does not
        // telescope (entry1.currentState C != entry0.newState B) → a hard reject.
        let pb = post_batch_with_chain(&[(1, a, b), (1, c, d)]);
        assert!(
            matches!(
                classify_settlement_chain(&pb, &verified(a, d), a, b""),
                SettlementVerdict::HardReject(_)
            ),
            "a telescoping break with a matching anchor must be a HardReject, not retreatable",
        );
    }

    #[test]
    fn classify_clean_window_is_ok() {
        let (a, b) = (B256::repeat_byte(0xa0), B256::repeat_byte(0xb0));
        let pb = post_batch_with_chain(&[(1, a, b)]);
        // R0 == anchor (A) and R_N == final (B): the happy path is Ok.
        assert!(matches!(
            classify_settlement_chain(&pb, &verified(a, b), a, b""),
            SettlementVerdict::Ok
        ));
    }

    // --- anchor-at-confirmed-posted: the resume-cursor decision boundaries. These
    // pin the soundness-critical advance/retain/retreat split that the async loop
    // drives `last_accepted` off, independent of the stream.

    #[test]
    fn cursor_move_advances_only_on_strict_increase() {
        // Steady state / the live window landing: located posted strictly above the
        // current cursor advances it (and the caller resets the telescope).
        assert_eq!(
            settling_cursor_move(Some(100), 50),
            CursorMove::Advance(100)
        );
        assert_eq!(settling_cursor_move(Some(1), 0), CursorMove::Advance(1));
    }

    #[test]
    fn cursor_move_retains_on_unchanged_posted() {
        // THE STUCK-STATE INVARIANT: a stale re-post claims the SAME posted; the
        // cursor must NOT advance (so the telescope anchor is retained and the
        // prover stays connected, catching up without a re-backfill).
        assert_eq!(settling_cursor_move(Some(0), 0), CursorMove::Retain);
        assert_eq!(settling_cursor_move(Some(16735), 16735), CursorMove::Retain);
    }

    #[test]
    fn cursor_move_retreats_on_lower_cached_posted() {
        assert_eq!(
            settling_cursor_move(Some(3000), 16735),
            CursorMove::Retreat(3000)
        );
    }

    #[test]
    fn cursor_move_holds_defensively_when_unlocatable() {
        // Unreachable past OD-5, but must NOT advance if it ever happens.
        assert_eq!(settling_cursor_move(None, 16735), CursorMove::HoldDefensive);
    }

    #[test]
    fn stuck_state_pins_cursor_at_genesis_then_advances_on_settle() {
        // Simulate the recovery: the composer re-posts a GROWING [1..tip] batch all
        // anchored at genesis (posted=0) while the prover catches up — every settling
        // window claims current_state=genesis => derived_h=0 => Retain, so the cursor
        // stays pinned at 0 and the telescope anchor is never reset (no re-drift, no
        // re-backfill). Only when the live window settles and the composer's posted
        // finally moves (derived_h jumps to the live height) does the cursor advance.
        let mut last_accepted = 0u64;
        for _stale_repost in 0..5 {
            match settling_cursor_move(Some(0), last_accepted) {
                CursorMove::Retain => {} // pinned — correct
                other => panic!("stale re-post must Retain, got {other:?}"),
            }
            assert_eq!(
                last_accepted, 0,
                "cursor must stay pinned through the stuck state"
            );
        }
        // The live window settles: posted advances to the live height.
        match settling_cursor_move(Some(18000), last_accepted) {
            CursorMove::Advance(h) => last_accepted = h,
            other => panic!("the settled live window must Advance, got {other:?}"),
        }
        assert_eq!(
            last_accepted, 18000,
            "cursor advances exactly once, to the confirmed posted"
        );
    }

    #[test]
    fn reanchor_precise_retreat_below_cursor() {
        // A wipe/reorg whose posted the prover already re-executed: retreat to the
        // CACHED height (replay [H+1..]), NOT a blunt genesis backfill.
        assert_eq!(
            reanchor_move(Some(3000), 16735, 0, 4),
            ReanchorMove::Retreat(3000)
        );
    }

    #[test]
    fn reanchor_advance_above_cursor_is_the_stuck_to_steady_transition() {
        // The live window settled, posted moved genesis->H, but the telescope was
        // still genesis so OD-5 mismatched once: advance to the cached higher H.
        assert_eq!(
            reanchor_move(Some(18000), 0, 0, 4),
            ReanchorMove::Advance(18000)
        );
    }

    #[test]
    fn reanchor_unlocatable_falls_to_bounded_genesis_then_self_disarms() {
        // Fabricated / below-horizon root: bounded retreat-to-genesis while budget
        // remains and the cursor is non-genesis ...
        assert_eq!(
            reanchor_move(None, 16735, 0, 4),
            ReanchorMove::RetreatGenesis
        );
        assert_eq!(
            reanchor_move(None, 16735, 3, 4),
            ReanchorMove::RetreatGenesis
        );
        // ... terminal self-disarm once already at genesis (no infinite loop) ...
        assert_eq!(
            reanchor_move(None, 0, 0, 4),
            ReanchorMove::TerminalAtGenesis
        );
        // ... and terminal once the budget is exhausted at a non-genesis cursor.
        assert_eq!(
            reanchor_move(None, 16735, 4, 4),
            ReanchorMove::BudgetExhausted
        );
    }

    #[test]
    fn settlement_chain_wide_batch_anchor_ok() {
        // GAP-2: a batch SPANNING > max_window. The composer anchors R0 at
        // `state(posted)` = A, the settling chunk's LOCAL re-executed parent is a
        // MID-batch root M (= state(posted + k·max_window)) ≠ A. With the OLD code
        // (OD-5 vs the settling chunk's local parent M) this FALSELY rejects; with
        // the telescoped batch anchor (A) it passes. The on-chain chain telescopes
        // A -> M -> Rn (the interior boundary M is a real re-executed pair-end root,
        // exercised by the wide-batch interior test below).
        let (a, m, rn) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0x4d),
            B256::repeat_byte(0xb0),
        );
        let pb = post_batch_with_chain(&[(1, a, m), (1, m, rn)]);
        // The SETTLING chunk re-executes parent = M (its local parent), final = Rn.
        let settling = verified(m, rn);
        // OLD behavior (anchor = the settling chunk's local parent M) would reject:
        assert!(
            verify_settlement_chain(&pb, &settling, m, b"").is_err(),
            "anchoring at the settling chunk's local parent M must reject R0=A (the pre-GAP-2 false reject)"
        );
        // The interior boundary M telescopes to R0=A via interim_interior_root, so
        // pass it WITHOUT per-tx roots; the only difference under test is the anchor.
        let interim = eez_evm::settlement::interim_interior_root(a, 1);
        let pb2 = post_batch_with_chain(&[(1, a, interim), (1, interim, rn)]);
        // GAP-2: anchoring at the TELESCOPED batch anchor A accepts the wide batch.
        verify_settlement_chain(&pb2, &settling, a, b"")
            .expect("telescoped batch anchor A accepts a batch spanning > max_window");
    }

    #[test]
    fn real_fixture_settlement_chain_verifies() {
        // The captured fixture (block 13) is timeless + empty: R0 == R_N, one entry.
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
        let raw =
            std::fs::read_to_string(format!("{dir}/postbatch-13.json")).expect("fixture json");
        let j: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let dec = |k: &str| hex::decode(j[k].as_str().unwrap().trim_start_matches("0x")).unwrap();
        let pb = eez_control_rpc::v1::PostBatch {
            abi_calldata: dec("abi_calldata"),
            ..Default::default()
        };
        let r0 = B256::from_slice(&dec("current_state"));
        let rn = B256::from_slice(&dec("new_state"));
        let block_rlp = std::fs::read(format!("{dir}/block-13.rlp")).expect("block-13.rlp");
        // Single-chunk fixture: the batch anchor == the chunk's re-executed parent r0.
        verify_settlement_chain(&pb, &verified(r0, rn), r0, &block_rlp)
            .expect("real fixture chain must verify against its re-executed roots");
    }

    // ── pure helpers (RLP-derived gates, step 2b-3) ──────────────────────

    #[test]
    fn pair_end_positions_classifies_pairs() {
        // user(false) always ends; a system(true) ends a pair only if NOT
        // followed by a user (a standalone inbound delivery).
        assert_eq!(pair_end_positions(&[]), Vec::<usize>::new());
        assert_eq!(pair_end_positions(&[false]), vec![0]); // lone user
        assert_eq!(pair_end_positions(&[true, false]), vec![1]); // system+user = one pair
        assert_eq!(pair_end_positions(&[true]), vec![0]); // standalone system delivery
        assert_eq!(pair_end_positions(&[true, false, false]), vec![1, 2]);
    }

    #[test]
    fn reverted_system_tx_gate() {
        assert!(system_txs_succeeded(&[true], None)); // no statuses → SKIP (true)
        assert!(system_txs_succeeded(&[true, false], Some(&[true, true]))); // all ok
        assert!(!system_txs_succeeded(&[true, false], Some(&[false, true]))); // SYSTEM reverted
        assert!(system_txs_succeeded(&[false], Some(&[false]))); // a USER revert is fine
        assert!(!system_txs_succeeded(&[true, true], Some(&[true]))); // status count drift
    }

    #[test]
    fn fixture_block_has_no_system_txs() {
        let block_rlp = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/block-13.rlp"
        ))
        .expect("block-13.rlp");
        // The captured settling block (13) carries no transactions.
        assert!(system_tx_flags_from_rlp(&block_rlp).is_empty());
    }
}
