//! Settlement gates + native-validate window validation — the proven prover
//! core, copied VERBATIM from e10aec0 + the edu fixes (effect-prefix root gate,
//! ordering coverage). PURE re-execution + gate logic; the `Prove` server
//! (`main.rs`) drives these. Never sign a window these gates reject.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::control_rpc::v1::ExecutionWitness;
use alloy_primitives::{Address, B256};
use eez_protocol::entries::decode_postbatch;
use eez_protocol::public_inputs::public_inputs_hashes;
use eez_protocol::settlement::{is_system_tx, pair_end_positions};
use tracing::{info, warn};

/// One staged window block the server hands `validate_window`: the composer's
/// claimed block hash, the consensus RLP, and the augmented witness. (Replaces
/// the old feed `ControlEvent` — same fields the staging path reads.)
#[derive(Debug)]
pub struct StagedBlock {
    pub number: u64,
    pub hash: Vec<u8>,
    pub rlp: Vec<u8>,
    pub witness: Option<ExecutionWitness>,
}

pub fn witness_to_json(w: &ExecutionWitness) -> String {
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

#[derive(Debug)]
pub struct VerifiedWindow {
    pub parent_state_root: B256,
    pub final_state_root: B256,
    /// Every window block's PROVEN post-state root (re-executed `state_root`),
    /// in block order. Recognizes inter-block settlement boundaries the two
    /// top-level roots miss — chiefly the no-op leading-immediate entry's
    /// `newState = state(sync_block-1)` when the settling chunk carries more
    /// than just the Sync block. `None` if the validator omits the field (an
    /// older binary) — the interior gate then degrades to parent/final/per-tx
    /// recognition + the placeholder path (fail-closed, never falsely accepts).
    pub per_block_roots: Option<Vec<B256>>,
    /// The Sync (last) block's per-tx re-executed roots (`pair_roots`), for the
    /// interior-boundaries gate. `None` if the validator omits them.
    pub sync_per_tx_roots: Option<Vec<B256>>,
    /// The Sync block's per-tx re-executed receipt statuses, for the
    /// reverted-system-tx (#10) gate. `None` if the validator omits them.
    pub sync_tx_statuses: Option<Vec<bool>>,
    /// The Sync block's re-executed outbound `CrossChainCallExecuted` topic1
    /// hashes, filtered by native-validate to the EEZL2 emitter.
    pub sync_outbound_call_hashes: Option<Vec<B256>>,
}

/// Wall-clock cap on the native-validate subprocess (env
/// `EEZ_VALIDATOR_TIMEOUT_SECS`, default 300s). Generous — it catches a HANG,
/// not slow-but-progressing validation.
fn validator_timeout() -> Duration {
    std::env::var("EEZ_VALIDATOR_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map_or(Duration::from_secs(300), Duration::from_secs)
}

/// Stage the window's blocks + witnesses, run `native-validate --dir`, check
/// each verified block hash matches the composer-claimed one, and return the
/// re-executed roots/per-tx data. Errors = the validator rejected the window
/// (a real attester would refuse to sign).
pub async fn validate_window(
    window: &[StagedBlock],
    validator_bin: &str,
    chain_config: &str,
    work_dir: &str,
) -> eyre::Result<VerifiedWindow> {
    let from = window.first().map_or(0, |e| e.number);
    let to = window.last().map_or(0, |e| e.number);

    let dir = Path::new(work_dir).join(format!("{from}-{to}"));
    tokio::fs::create_dir_all(&dir).await?;
    // Timing: split STAGING (writing block+witness files; grows with witness
    // size) from the native-validate SUBPROCESS (the suspected bottleneck), so
    // the per-window trace shows where the wall-clock goes.
    let t_stage = Instant::now();
    let mut staged_bytes: usize = 0;
    for event in window {
        let n = event.number;
        let witness = event
            .witness
            .as_ref()
            .ok_or_else(|| eyre::eyre!("window block #{n} carries no witness"))?;
        let wjson = witness_to_json(witness);
        staged_bytes += event.rlp.len() + wjson.len();
        tokio::fs::write(dir.join(format!("block-{n}.rlp")), &event.rlp).await?;
        tokio::fs::write(dir.join(format!("witness-{n}.json")), wjson).await?;
    }
    let stage_ms = t_stage.elapsed().as_millis();

    let t_exec = Instant::now();
    // Bound the subprocess: proving sits on the block-production path, so a hung
    // validator must not wedge the composer. `kill_on_drop` means the timeout
    // dropping the future SIGKILLs the child.
    let child = tokio::process::Command::new(validator_bin)
        .arg(chain_config)
        .arg("--dir")
        .arg(&dir)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| eyre::eyre!("spawn {validator_bin}: {e}"))?;
    let timeout = validator_timeout();
    let out = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .map_err(|_| {
            eyre::eyre!("native-validate timed out after {timeout:?} for window {from}-{to}")
        })?
        .map_err(|e| eyre::eyre!("native-validate wait {validator_bin}: {e}"))?;
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
        let claimed = format!("0x{}", hex::encode(&event.hash));
        if validated != claimed {
            eyre::bail!(
                "window block #{} hash mismatch: validator {validated} != claimed {claimed}",
                event.number
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
    // Enforce the invariant at the boundary: a settling window MUST carry per-tx
    // statuses, or the reverted-system-tx gate can't run. Fail-closed here (with
    // a clear message) so downstream can assume presence; the gate also refuses
    // on `None` as a backstop.
    if sync_tx_statuses.is_none() {
        eyre::bail!(
            "validator emitted no tx_statuses for the settling block (window {from}-{to}) — \
             cannot run the reverted-system-tx gate"
        );
    }
    let sync_outbound_call_hashes = last_block["outbound_call_hashes"].as_array().map(|arr| {
        arr.iter()
            .filter_map(serde_json::Value::as_str)
            .filter_map(|s| s.parse::<B256>().ok())
            .collect::<Vec<_>>()
    });
    // Every proved window is a posted settlement (composer-controlled) — the
    // old feed carried non-settling interior blocks; here they never occur.
    let settling = true;
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
        sync_outbound_call_hashes,
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
pub fn verify_settlement_public_inputs(
    pb: &crate::control_rpc::v1::PostBatch,
    vkey: B256,
) -> eyre::Result<B256> {
    let batch = decode_postbatch(&pb.abi_calldata)
        .map_err(|e| eyre::eyre!("decode postBatch calldata: {e}"))?;

    let n_ps = batch.proofSystems.len();
    if n_ps != 1 {
        eyre::bail!("settlement has {n_ps} proof systems; this gate verifies a single PS only");
    }
    if batch.blockNumber != 0 {
        eyre::bail!(
            "settlement batch blockNumber={} is BOUND; only timeless (0) is verifiable without an L1 oracle",
            batch.blockNumber
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

    // The composer-controlled wire lets the composer SKIP pre-claiming the hash
    // (empty) — the prover computes + signs its own. When a claim IS present it
    // must match (defensive cross-check, as in the pull model).
    match pb.public_inputs_hash.len() {
        0 => {}
        32 => {
            let claimed = B256::from_slice(&pb.public_inputs_hash);
            if recomputed != claimed {
                eyre::bail!(
                    "publicInputsHash mismatch: recomputed {recomputed} != composer-claimed {claimed}"
                );
            }
        }
        n => eyre::bail!("composer publicInputsHash must be 0 or 32 bytes, got {n}"),
    }
    Ok(recomputed)
}

/// Per-tx SYSTEM flags of a block, re-derived from its OWN RLP (signer ==
/// `SYSTEM_ADDRESS` && target == the EEZL2 predeploy). Shared by the pair-end
/// classification + the reverted-system-tx (#10) gate. Undecodable RLP is an
/// ERROR (fail-closed): an empty tx set would pass both gates vacuously.
///
/// The target is `ccm_l2_address` — the deployment's on-chain EEZL2 predeploy
/// (runtime config, `EEZ_CCM_L2_ADDRESS`), NOT the stale `eez_protocol::CCM_ADDRESS`
/// (0xeeee..). Using 0xeeee misclassifies every system tx as a user tx, silently
/// defeating the interior-boundary + reverted-system-tx gates that re-derive
/// system flags from this block RLP.
pub fn system_tx_flags_from_rlp(
    block_rlp: &[u8],
    ccm_l2_address: Address,
) -> eyre::Result<Vec<bool>> {
    use alloy_consensus::Transaction as _;
    use alloy_rlp::Decodable as _;
    use reth_primitives_traits::SignerRecoverable as _;

    let block = reth_ethereum_primitives::Block::decode(&mut &block_rlp[..])
        .map_err(|e| eyre::eyre!("decode sync block RLP for system-tx flags: {e}"))?;
    Ok(block
        .body
        .transactions
        .iter()
        .map(|tx| {
            tx.recover_signer()
                .is_ok_and(|signer| is_system_tx(signer, tx.to(), ccm_l2_address))
        })
        .collect())
}

/// Pair-end classification over per-tx system flags: position `i` ends a pair
/// iff tx `i` is a USER tx, or a SYSTEM tx NOT followed by a user tx (a
/// standalone inbound delivery).
/// (#10) `true` iff no SYSTEM tx in the block REVERTED, per the validator's
/// re-executed receipt statuses. A reverted-but-sealed system tx passes every
/// calldata-derived gate vacuously (the inbound result is read from CALLDATA),
/// so it must be caught here. Fail-closed: `statuses = None` REFUSES the window
/// (the current validator always emits statuses; a missing list would otherwise
/// silently disable a mandatory gate), and a count shorter than the tx count
/// refuses too (drift).
pub fn system_txs_succeeded(system_flags: &[bool], statuses: Option<&[bool]>) -> bool {
    let Some(statuses) = statuses else {
        warn!(
            "validator emitted no per-tx statuses — REFUSING the window (mandatory gate cannot run)"
        );
        return false;
    };
    if statuses.len() != system_flags.len() {
        warn!(
            flags = system_flags.len(),
            statuses = statuses.len(),
            "per-tx status count != the block's tx count — refusing (count drift)"
        );
        return false;
    }
    system_flags
        .iter()
        .zip(statuses)
        .all(|(sys, ok)| !*sys || *ok)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettlementKind {
    Anchor,
    Outbound,
    Inbound,
}

pub fn classify_settlement_entry(e: &eez_protocol::abi::ExecutionEntrySol) -> SettlementKind {
    if e.proxyEntryHash != B256::ZERO {
        SettlementKind::Inbound
    } else if !e.l2ToL1Calls.is_empty() {
        SettlementKind::Outbound
    } else {
        SettlementKind::Anchor
    }
}

pub fn settlement_effects_from_sync(
    system_flags: &[bool],
    per_tx_roots: &[B256],
) -> Option<Vec<(SettlementKind, B256)>> {
    pair_end_positions(system_flags)
        .into_iter()
        .map(|p| {
            let kind = if system_flags[p] {
                SettlementKind::Inbound
            } else {
                SettlementKind::Outbound
            };
            per_tx_roots.get(p).copied().map(|root| (kind, root))
        })
        .collect()
}

pub fn verify_effect_prefix_roots(
    entries: &[(SettlementKind, B256, B256)],
    system_flags: &[bool],
    per_tx_roots: Option<&[B256]>,
    batch_anchor_root: B256,
) -> eyre::Result<()> {
    let effect_entries: Vec<&(SettlementKind, B256, B256)> = entries
        .iter()
        .filter(|(kind, _, _)| *kind != SettlementKind::Anchor)
        .collect();
    let effects = match per_tx_roots {
        Some(per_tx) => settlement_effects_from_sync(system_flags, per_tx).ok_or_else(|| {
            eyre::eyre!("effect-prefix: a pair-end position lacks a re-executed per-tx root")
        })?,
        None => {
            if !effect_entries.is_empty() {
                eyre::bail!(
                    "effect-prefix: batch has value-bearing entries but validator emitted no per-tx roots"
                );
            }
            Vec::new()
        }
    };

    if effect_entries.len() != effects.len() {
        eyre::bail!(
            "effect-prefix: {} value-bearing entries but {} re-executed effects",
            effect_entries.len(),
            effects.len(),
        );
    }

    let mut previous_root = match entries.first() {
        Some((SettlementKind::Anchor, _, new)) => *new,
        _ => batch_anchor_root,
    };
    for (idx, ((entry_kind, current_root, new_root), (effect_kind, effect_root))) in
        effect_entries.iter().zip(effects).enumerate()
    {
        if entry_kind != &effect_kind {
            eyre::bail!(
                "effect-prefix: entry {idx} kind {entry_kind:?} != re-executed effect kind {effect_kind:?}"
            );
        }
        if *current_root != previous_root {
            eyre::bail!(
                "effect-prefix: entry {idx} currentState {current_root} != previous prefix root {previous_root}"
            );
        }
        if *new_root != effect_root {
            eyre::bail!(
                "effect-prefix: entry {idx} newState {new_root} != exact effect prefix root {effect_root}"
            );
        }
        previous_root = effect_root;
    }

    Ok(())
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
pub fn extract_inbounds(
    block_rlp: &[u8],
) -> eyre::Result<Vec<eez_protocol::entries::DecodedInbound>> {
    use alloy_consensus::Transaction as _;
    use alloy_rlp::Decodable as _;

    // Fail-closed: an undecodable block must REJECT, not read as zero inbounds.
    let block = reth_ethereum_primitives::Block::decode(&mut &block_rlp[..])
        .map_err(|e| eyre::eyre!("decode block RLP for inbound extraction: {e}"))?;
    Ok(block
        .body
        .transactions
        .iter()
        .filter_map(|tx| eez_protocol::entries::decode_inbound(tx.input()))
        .collect())
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
/// `build_inbound_system_txs` (`eez-protocol/src/system_tx.rs`) seals the
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
/// `pending_in`), and caps each bundle at `MAX_USER_TXS_PER_BUNDLE = 10` so a
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
pub fn multi_inbound_outcome_gate(
    batch: &eez_protocol::EvmBatch,
    sealed: &[eez_protocol::entries::DecodedInbound],
) -> Result<(), String> {
    use eez_protocol::RollupId;
    let entries = &batch.entries;
    let lookups = &batch.l1ToL2lookupCalls;

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

    let hash_of = |d: &eez_protocol::entries::DecodedInbound| -> B256 {
        eez_protocol::cross_chain_call_hash(
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
    let deferred: Vec<&eez_protocol::abi::ExecutionEntrySol> = entries
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

/// Outbound authorization gate (A3): every outbound L2->L1 settlement entry
/// must match a `CrossChainCallExecuted` hash observed during stateless
/// re-execution of the Sync block. Thin wrapper over the SHARED
/// `eez_protocol::outbound_gate::verify_outbound_authorized` — the SAME check the
/// deriver (A4) runs against its local replay receipts, so the prover and the
/// follower cannot drift.
pub fn verify_outbound_authorized(
    batch: &eez_protocol::EvmBatch,
    observed_call_hashes: &[B256],
    l2_rollup_id: u64,
) -> eyre::Result<()> {
    // The outbound immediates only (proxyEntryHash == 0, non-empty calls), in DA
    // order — the SAME partition the deriver pairs (deriver.rs reconcile).
    let outbound: Vec<eez_protocol::abi::ExecutionEntrySol> = batch
        .entries
        .iter()
        .filter(|e| e.proxyEntryHash == B256::ZERO && !e.l2ToL1Calls.is_empty())
        .cloned()
        .collect();
    eez_protocol::outbound_gate::verify_outbound_authorized(
        &outbound,
        observed_call_hashes,
        l2_rollup_id,
    )
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
pub fn verify_settlement_chain(
    pb: &crate::control_rpc::v1::PostBatch,
    vw: &VerifiedWindow,
    batch_anchor_root: B256,
    sync_block_rlp: &[u8],
    ccm_l2_address: Address,
) -> eyre::Result<()> {
    use eez_protocol::settlement::interim_interior_root;

    let batch = decode_postbatch(&pb.abi_calldata)
        .map_err(|e| eyre::eyre!("decode postBatch calldata: {e}"))?;
    let entries = &batch.entries;
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
    let system_flags = system_tx_flags_from_rlp(sync_block_rlp, ccm_l2_address)?;
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

    let classified: Vec<(SettlementKind, B256, B256)> = chain
        .iter()
        .zip(entries.iter())
        .map(|(d, e)| (classify_settlement_entry(e), d.currentState, d.newState))
        .collect();
    verify_effect_prefix_roots(
        &classified,
        &system_flags,
        vw.sync_per_tx_roots.as_deref(),
        batch_anchor_root,
    )?;
    Ok(())
}
#[cfg(test)]
#[allow(
    clippy::field_reassign_with_default,
    reason = "mirror upstream shape; EvmBatch is now a plain Sol alias"
)]
mod tests {
    use super::*;
    use alloy_primitives::{U256, address};
    use eez_protocol::EvmBatch;
    use eez_protocol::abi::RollupIdWithProofSystemsSol;
    use eez_protocol::entries::encode_postbatch;

    /// The EEZL2 predeploy the fixture's system txs target (= the edu deployment's
    /// address). Threaded into the settlement-chain gate's system-tx classification.
    const TEST_CCM_L2: Address = address!("0x4200000000000000000000000000000000000007");

    /// A minimal finalized PostBatch — one PS, one rollup, TIMELESS — mirroring
    /// eez-protocol's `carrier_batch` test helper, with the publicInputsHash the
    /// composer would claim for the given `vkey`.
    fn carrier_post_batch(vkey: B256) -> crate::control_rpc::v1::PostBatch {
        let mut batch = EvmBatch::default();
        batch.blockNumber = 0; // timeless
        batch.proofSystems = vec![address!("00000000000000000000000000000000000000aa")];
        batch.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: U256::from(1),
            proofSystemIndex: vec![0],
        }];
        let claimed = public_inputs_hashes(&batch, vkey, None).unwrap()[0];
        crate::control_rpc::v1::PostBatch {
            abi_calldata: encode_postbatch(&batch),
            public_inputs_hash: claimed.to_vec(),
            l1_block_hash: Vec::new(),
        }
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
        use eez_protocol::RollupId;
        use eez_protocol::abi::{ExecutionEntrySol, StateDeltaSol};
        use eez_protocol::entries::DecodedInbound;

        let target = address!("00000000000000000000000000000000000000bb");
        let source = address!("00000000000000000000000000000000000000cc");
        let value = U256::ZERO;
        let data = Bytes::from(vec![0x12, 0x34]);
        let ret = Bytes::from(vec![0xab, 0xcd]); // the proven Y
        let rollup = RollupId(1);

        // H the user's proxy computes on-chain (settled_rollup, …, MAINNET=0).
        let h =
            eez_protocol::cross_chain_call_hash(rollup, target, value, &data, source, RollupId(0));

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
        batch.entries = vec![immediate(), deferred()];

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
        forged.entries[1].proxyEntryHash = B256::repeat_byte(0x99);
        assert!(multi_inbound_outcome_gate(&forged, std::slice::from_ref(&d)).is_err());

        // Wrong bytes: delivers X on L2 but settles a different Y' on L1 → REFUSE.
        let mut wrong_bytes = batch.clone();
        wrong_bytes.entries[1].returnData = Bytes::from(vec![0xff]);
        assert!(multi_inbound_outcome_gate(&wrong_bytes, std::slice::from_ref(&d)).is_err());

        // No settlement StateDelta on the immediate entry → cannot bind H → REFUSE.
        let mut no_delta = batch.clone();
        no_delta.entries[0].stateDeltas = Vec::new();
        assert!(multi_inbound_outcome_gate(&no_delta, std::slice::from_ref(&d)).is_err());
    }

    // ── GAP-3 multi-delivery bijection helpers + tests ───────────────────

    /// One inbound deferred entry (proxyEntryHash != 0 + the proven returnData).
    #[cfg(test)]
    fn mi_deferred_entry(
        proxy: B256,
        ret_data: alloy_primitives::Bytes,
    ) -> eez_protocol::abi::ExecutionEntrySol {
        eez_protocol::abi::ExecutionEntrySol {
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
    fn mi_immediate_entry(rollup: u64) -> eez_protocol::abi::ExecutionEntrySol {
        use alloy_primitives::I256;
        let mut e = mi_deferred_entry(B256::ZERO, alloy_primitives::Bytes::new());
        e.stateDeltas = vec![eez_protocol::abi::StateDeltaSol {
            rollupId: U256::from(rollup),
            currentState: B256::ZERO,
            newState: B256::ZERO,
            etherDelta: I256::ZERO,
        }];
        e
    }

    /// A `DecodedInbound` with distinct bytes per `tag`, success by default.
    #[cfg(test)]
    fn mi_sealed(tag: u8, success: bool) -> eez_protocol::entries::DecodedInbound {
        eez_protocol::entries::DecodedInbound {
            target: address!("00000000000000000000000000000000000000bb"),
            value: U256::ZERO,
            data: alloy_primitives::Bytes::from(vec![tag]),
            source: address!("00000000000000000000000000000000000000cc"),
            return_data: alloy_primitives::Bytes::from(vec![0xa0 | tag]),
            success,
        }
    }

    #[cfg(test)]
    fn mi_hash(d: &eez_protocol::entries::DecodedInbound, rollup: u64) -> B256 {
        eez_protocol::cross_chain_call_hash(
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
        batch.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h0, s0.return_data.clone()),
            mi_deferred_entry(h1, s1.return_data.clone()),
        ];
        multi_inbound_outcome_gate(&batch, &[s0.clone(), s1.clone()])
            .expect("two distinct inbounds biject to two deferred entries in order");

        // Cross-wired returnData (entry #0 carries s1's bytes) → per-pair bytes
        // mismatch → REFUSE (the X-on-L2 / Y-on-L1 equivocation, multi-call form).
        let mut swapped = batch.clone();
        swapped.entries[1].returnData = s1.return_data.clone();
        swapped.entries[2].returnData = s0.return_data.clone();
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
        batch.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h, s.return_data.clone()),
            mi_deferred_entry(h, s.return_data.clone()),
        ];
        multi_inbound_outcome_gate(&batch, &[s.clone(), s.clone()])
            .expect("two identical inbounds biject to two identical-hash deferred entries");

        // Only ONE deferred entry for two identical sealed inbounds → cardinality
        // mismatch (an unmatched delivery) → REFUSE (no hash-set collapse).
        let mut one_entry = EvmBatch::default();
        one_entry.entries = vec![
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
        batch.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h, s.return_data.clone()),
        ];
        assert!(
            multi_inbound_outcome_gate(&batch, &[]).is_err(),
            "a deferred entry with no sealed inbound is a phantom delivery"
        );
        // And a CLEAN inbound-free batch (no deferred entries, no sealed) → OK.
        let mut empty = EvmBatch::default();
        empty.entries = vec![mi_immediate_entry(1)];
        multi_inbound_outcome_gate(&empty, &[]).expect("no inbounds, no deferred entries → OK");
    }

    #[test]
    fn multi_inbound_unmatched_delivery_rejects() {
        // UNMATCHED: the L2 sealed an inbound the batch does NOT settle (the batch
        // carries no deferred entry for it). 1 sealed != 0 deferred → REFUSE.
        let s = mi_sealed(1, true);
        let mut batch = EvmBatch::default();
        batch.entries = vec![mi_immediate_entry(1)];
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
        batch.entries = vec![
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
        batch.entries = vec![
            mi_immediate_entry(1),
            mi_deferred_entry(h, s.return_data.clone()),
        ];
        // A leftover failed lookup with no backing sealed failure.
        batch.l1ToL2lookupCalls = vec![eez_protocol::abi::LookupCallSol {
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

    fn outbound_entry(
        source: alloy_primitives::Address,
        target: alloy_primitives::Address,
        value: U256,
        data: alloy_primitives::Bytes,
    ) -> eez_protocol::abi::ExecutionEntrySol {
        use eez_protocol::abi::{ExecutionEntrySol, L2ToL1CallSol};

        ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: B256::ZERO,
            destinationRollupId: U256::from(1),
            callCount: U256::from(1u8),
            l2ToL1Calls: vec![L2ToL1CallSol {
                targetAddress: target,
                value,
                data,
                sourceAddress: source,
                sourceRollupId: U256::from(1u64),
                revertSpan: U256::ZERO,
            }],
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            returnData: alloy_primitives::Bytes::new(),
            rollingHash: B256::ZERO,
        }
    }

    fn observed_for(entry: &eez_protocol::abi::ExecutionEntrySol) -> B256 {
        let call = entry.l2ToL1Calls.first().expect("outbound call");
        eez_protocol::cross_chain_call_hash(
            eez_protocol::RollupId(0),
            call.targetAddress,
            call.value,
            &call.data,
            call.sourceAddress,
            eez_protocol::RollupId(1),
        )
    }

    #[test]
    fn outbound_gate_authorizes_via_observed_hash() {
        let wrapper = address!("cccccccccccccccccccccccccccccccccccccccc");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let entry = outbound_entry(
            wrapper,
            target,
            U256::from(7u64),
            alloy_primitives::Bytes::from(vec![0x12u8, 0x34]),
        );
        let mut batch = EvmBatch::default();
        batch.entries = vec![entry.clone()];

        assert!(verify_outbound_authorized(&batch, &[observed_for(&entry)], 1).is_ok());
        assert!(verify_outbound_authorized(&batch, &[], 1).is_err());

        let mut tampered = batch.clone();
        tampered.entries[0].l2ToL1Calls[0].value = U256::from(999u64);
        assert!(verify_outbound_authorized(&tampered, &[observed_for(&entry)], 1).is_err());
    }

    #[test]
    fn outbound_gate_k2_requires_one_observed_hash_per_entry() {
        let source = address!("cccccccccccccccccccccccccccccccccccccccc");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let entry0 = outbound_entry(
            source,
            target,
            U256::from(111u64),
            alloy_primitives::Bytes::from(vec![0x01, 0x02]),
        );
        let entry1 = outbound_entry(
            source,
            target,
            U256::from(222u64),
            alloy_primitives::Bytes::from(vec![0x03, 0x04]),
        );
        let mut batch = EvmBatch::default();
        batch.entries = vec![entry0.clone(), entry1.clone()];

        let observed = vec![observed_for(&entry0), observed_for(&entry1)];
        assert!(verify_outbound_authorized(&batch, &observed, 1).is_ok());
        assert!(verify_outbound_authorized(&batch, &observed[..1], 1).is_err());
        assert!(
            verify_outbound_authorized(&batch, &[observed[1], observed[0]], 1).is_ok(),
            "observed hashes are a multiset; receipt order need not match entry order"
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
        let pb = crate::control_rpc::v1::PostBatch {
            abi_calldata: dec("abi_calldata"),
            public_inputs_hash: dec("public_inputs_hash"),
            l1_block_hash: dec("l1_block_hash"), // 0x → empty → timeless
        };
        // Mock prover vkey = bytes32(uint160(authorizedSigner = hardhat #0)).
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
        let pb = crate::control_rpc::v1::PostBatch {
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
    fn post_batch_with_chain(deltas: &[(u64, B256, B256)]) -> crate::control_rpc::v1::PostBatch {
        let template = fixture_batch().entries[0].clone();
        let template_delta = template.stateDeltas[0].clone();
        let mut batch = EvmBatch::default();
        batch.entries = deltas
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
        crate::control_rpc::v1::PostBatch {
            abi_calldata: encode_postbatch(&batch),
            ..Default::default()
        }
    }

    /// A decodable, empty (0-tx) sync-block RLP — the realistic stand-in for the
    /// settling block in chain-logic tests (they inject roots synthetically; the
    /// block itself carries no system txs). The committed empty fixture, so the
    /// fail-closed RLP decode in `system_tx_flags_from_rlp` sees a valid block.
    fn empty_block_rlp() -> Vec<u8> {
        std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/block-13.rlp"
        ))
        .expect("block-13.rlp fixture")
    }

    fn verified(parent: B256, final_root: B256) -> VerifiedWindow {
        VerifiedWindow {
            parent_state_root: parent,
            final_state_root: final_root,
            per_block_roots: None,
            sync_per_tx_roots: None,
            // Empty sync block → 0 txs → 0 statuses. A real settling window always
            // carries statuses; `None` is rejected at the validate_window boundary,
            // so it is not a valid VerifiedWindow input here.
            sync_tx_statuses: Some(vec![]),
            sync_outbound_call_hashes: None,
        }
    }

    #[test]
    fn settlement_chain_endpoints_anchor_ok() {
        let (a, b) = (B256::repeat_byte(0xa0), B256::repeat_byte(0xb0));
        let pb = post_batch_with_chain(&[(1, a, b)]);
        // Single-chunk batch: the batch anchor == the chunk's re-executed parent A.
        verify_settlement_chain(&pb, &verified(a, b), a, &empty_block_rlp(), TEST_CCM_L2)
            .expect("R0==anchor, R_N==final → OK");
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
        assert!(
            verify_settlement_chain(&pb, &verified(a, c), a, &empty_block_rlp(), TEST_CCM_L2)
                .is_err()
        );
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
        assert!(
            verify_settlement_chain(&pb, &verified(x, b), x, &empty_block_rlp(), TEST_CCM_L2)
                .is_err()
        );
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
        assert!(
            verify_settlement_chain(&pb, &verified(a, d), a, &empty_block_rlp(), TEST_CCM_L2)
                .is_err()
        );
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
        verify_settlement_chain(&pb, &verified(r0, rn), r0, &empty_block_rlp(), TEST_CCM_L2)
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
            // Empty sync block → 0 txs → 0 statuses. A real settling window always
            // carries statuses; `None` is rejected at the validate_window boundary,
            // so it is not a valid VerifiedWindow input here.
            sync_tx_statuses: Some(vec![]),
            sync_outbound_call_hashes: None,
        };
        verify_settlement_chain(&pb, &vw, r0, &empty_block_rlp(), TEST_CCM_L2)
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
            // Empty sync block → 0 txs → 0 statuses. A real settling window always
            // carries statuses; `None` is rejected at the validate_window boundary,
            // so it is not a valid VerifiedWindow input here.
            sync_tx_statuses: Some(vec![]),
            sync_outbound_call_hashes: None,
        };
        assert!(
            verify_settlement_chain(&pb, &vw, r0, &empty_block_rlp(), TEST_CCM_L2).is_err(),
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
            // Empty sync block → 0 txs → 0 statuses. A real settling window always
            // carries statuses; `None` is rejected at the validate_window boundary,
            // so it is not a valid VerifiedWindow input here.
            sync_tx_statuses: Some(vec![]),
            sync_outbound_call_hashes: None,
        };
        assert!(
            verify_settlement_chain(&pb, &vw, r0, &empty_block_rlp(), TEST_CCM_L2).is_err(),
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
        assert!(
            verify_settlement_chain(&pb, &verified(a, d), a, &empty_block_rlp(), TEST_CCM_L2)
                .is_err()
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
            verify_settlement_chain(&pb, &settling, m, &empty_block_rlp(), TEST_CCM_L2).is_err(),
            "anchoring at the settling chunk's local parent M must reject R0=A (the pre-GAP-2 false reject)"
        );
        // The interior boundary M telescopes to R0=A via interim_interior_root, so
        // pass it WITHOUT per-tx roots; the only difference under test is the anchor.
        let interim = eez_protocol::settlement::interim_interior_root(a, 1);
        let pb2 = post_batch_with_chain(&[(1, a, interim), (1, interim, rn)]);
        // GAP-2: anchoring at the TELESCOPED batch anchor A accepts the wide batch.
        verify_settlement_chain(&pb2, &settling, a, &empty_block_rlp(), TEST_CCM_L2)
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
        let pb = crate::control_rpc::v1::PostBatch {
            abi_calldata: dec("abi_calldata"),
            ..Default::default()
        };
        let r0 = B256::from_slice(&dec("current_state"));
        let rn = B256::from_slice(&dec("new_state"));
        let block_rlp = std::fs::read(format!("{dir}/block-13.rlp")).expect("block-13.rlp");
        // Single-chunk fixture: the batch anchor == the chunk's re-executed parent r0.
        verify_settlement_chain(&pb, &verified(r0, rn), r0, &block_rlp, TEST_CCM_L2)
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
        assert!(!system_txs_succeeded(&[true], None)); // no statuses → REFUSE (fail-closed)
        assert!(system_txs_succeeded(&[true, false], Some(&[true, true]))); // all ok
        assert!(!system_txs_succeeded(&[true, false], Some(&[false, true]))); // SYSTEM reverted
        assert!(system_txs_succeeded(&[false], Some(&[false]))); // a USER revert is fine
        assert!(!system_txs_succeeded(&[true, true], Some(&[true]))); // status count drift
    }

    #[test]
    fn effect_prefix_roots_accept_exact_chain() {
        let (a, pre, outbound, inbound) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0x10),
            B256::repeat_byte(0x20),
            B256::repeat_byte(0x30),
        );
        let entries = vec![
            (SettlementKind::Anchor, a, pre),
            (SettlementKind::Outbound, pre, outbound),
            (SettlementKind::Inbound, outbound, inbound),
        ];
        let system_flags = [true, false, true];
        let per_tx_roots = [B256::repeat_byte(0x01), outbound, inbound];

        verify_effect_prefix_roots(&entries, &system_flags, Some(&per_tx_roots), a)
            .expect("exact outbound/inbound prefix chain must pass");
    }

    #[test]
    fn effect_prefix_roots_reject_jump_to_final_root() {
        let (a, pre, outbound, final_root) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0x10),
            B256::repeat_byte(0x20),
            B256::repeat_byte(0x30),
        );
        let entries = vec![
            (SettlementKind::Anchor, a, pre),
            (SettlementKind::Outbound, pre, final_root),
            (SettlementKind::Inbound, final_root, final_root),
        ];
        let system_flags = [true, false, true];
        let per_tx_roots = [B256::repeat_byte(0x01), outbound, final_root];

        assert!(
            verify_effect_prefix_roots(&entries, &system_flags, Some(&per_tx_roots), a).is_err(),
            "outbound entry must not settle against a later/final root"
        );
    }

    #[test]
    fn effect_prefix_roots_reject_reordered_effect_kinds() {
        let (a, pre, outbound, inbound) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0x10),
            B256::repeat_byte(0x20),
            B256::repeat_byte(0x30),
        );
        let entries = vec![
            (SettlementKind::Anchor, a, pre),
            (SettlementKind::Inbound, pre, inbound),
            (SettlementKind::Outbound, inbound, outbound),
        ];
        let system_flags = [true, false, true];
        let per_tx_roots = [B256::repeat_byte(0x01), outbound, inbound];

        assert!(
            verify_effect_prefix_roots(&entries, &system_flags, Some(&per_tx_roots), a).is_err(),
            "settlement entries must match the re-executed effect kind order"
        );
    }

    #[test]
    fn effect_prefix_roots_reject_reordered_prefix_roots() {
        let (a, pre, outbound, inbound) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0x10),
            B256::repeat_byte(0x20),
            B256::repeat_byte(0x30),
        );
        let entries = vec![
            (SettlementKind::Anchor, a, pre),
            (SettlementKind::Outbound, pre, inbound),
            (SettlementKind::Inbound, inbound, outbound),
        ];
        let system_flags = [true, false, true];
        let per_tx_roots = [B256::repeat_byte(0x01), outbound, inbound];

        assert!(
            verify_effect_prefix_roots(&entries, &system_flags, Some(&per_tx_roots), a).is_err(),
            "each settlement entry must use its exact re-executed prefix root, not a later root"
        );
    }

    #[test]
    fn effect_prefix_roots_require_native_per_tx_roots_for_effects() {
        let (a, pre, outbound) = (
            B256::repeat_byte(0xa0),
            B256::repeat_byte(0x10),
            B256::repeat_byte(0x20),
        );
        let entries = vec![
            (SettlementKind::Anchor, a, pre),
            (SettlementKind::Outbound, pre, outbound),
        ];

        assert!(
            verify_effect_prefix_roots(&entries, &[], None, a).is_err(),
            "effect-bearing settlement requires re-executed per-tx roots"
        );
        verify_effect_prefix_roots(&[(SettlementKind::Anchor, a, pre)], &[], None, a)
            .expect("anchor-only settlement does not need per-tx roots");
    }

    #[test]
    fn fixture_block_has_no_system_txs() {
        let block_rlp = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/block-13.rlp"
        ))
        .expect("block-13.rlp");
        // The captured settling block (13) decodes and carries no transactions.
        assert!(
            system_tx_flags_from_rlp(&block_rlp, TEST_CCM_L2)
                .expect("valid fixture block decodes")
                .is_empty()
        );
    }
}
