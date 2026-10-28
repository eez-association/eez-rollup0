//! Canonical unsigned L2 system transactions, shared by Composer, Deriver, and
//! proof verification. Equal entries, live sources and execution positions produce identical
//! EIP-2718 bytes without private material. Sync pairs preserve per-entry ordering.

use crate::native_source::SourceEntry;
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::SolCall;
use eez_primitives::{EEZL2_ADDRESS, EezTxEnvelope, SystemTransaction};

use crate::RollupId;
use crate::abi::{ExecutionEntrySol, L2ExecutionEntrySol, loadExecutionTableCall};
use crate::entries::{
    InboundSidecar, OutboundEntry, build_l2_incoming_entry, build_l2_outbound_entry,
    encode_execute_incoming,
};

/// The transaction body determines boundaries; request metadata never does.
/// Native loads and their immediately following ordinary transaction form one
/// group, regardless of receipt status. Inbound deliveries are one-transaction
/// groups. Unpaired ordinary transactions do not end a group.
/// Keep this cursor with the executed prefix and restore it with speculative
/// state. A proposed transition takes effect only when its transaction commits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionGroupState {
    awaiting_user: bool,
}

impl ExecutionGroupState {
    /// Propose the next cursor and whether this transaction completes a group.
    /// This classifies already decoded envelopes, not source/effect authority.
    pub fn after(self, tx: &EezTxEnvelope) -> Result<(Self, bool), &'static str> {
        match tx {
            EezTxEnvelope::Ethereum(_) => Ok(self.after_ordinary()),
            EezTxEnvelope::System(_) if self.awaiting_user => {
                Err("native table load must be followed immediately by its user transaction")
            }
            EezTxEnvelope::System(native) if native.to == EEZL2_ADDRESS => {
                if native.input.starts_with(&loadExecutionTableCall::SELECTOR) {
                    Ok((
                        Self {
                            awaiting_user: true,
                        },
                        false,
                    ))
                } else if native
                    .input
                    .starts_with(&crate::entries::EXECUTE_INCOMING_SELECTOR)
                {
                    Ok((Self::default(), true))
                } else {
                    Err("unsupported native group selector")
                }
            }
            EezTxEnvelope::System(_) => Err("unsupported native group destination"),
        }
    }

    /// The ordinary branch, also used for unsigned advisory executions. It
    /// neither validates a signature nor authorizes the paired origin identity.
    pub const fn after_ordinary(self) -> (Self, bool) {
        (
            Self {
                awaiting_user: false,
            },
            self.awaiting_user,
        )
    }

    /// A published block or effect checkpoint cannot end after an unpaired load.
    pub fn ensure_complete(self) -> Result<(), &'static str> {
        if self.awaiting_user {
            Err("block ends inside an outbound load/user group")
        } else {
            Ok(())
        }
    }
}

/// Public reconstruction inputs shared by Composer, Deriver, and proof validation.
#[derive(Clone, Debug)]
pub struct SystemTxContext {
    /// Must equal the reserved EEZL2 predeploy; encoding rejects other targets.
    pub eezl2_address: Address,
    /// Binds the reconstructed transaction to its L2 chain for replay protection.
    pub l2_chain_id: u64,
    /// This rollup's id — entries whose `destinationRollupId` doesn't
    /// match are skipped (they belong to a different L2).
    pub this_rollup_id: u64,
}

/// Reconstruct inbound delivery plans from checked sidecars. Foreign entries are
/// skipped. Source ordinals refer to the complete candidate before filtering.
/// Gas is resolved only when the preceding transactions have executed.
///
/// # Errors
/// Reject invalid targets, sidecars and source-index overflow.
pub fn build_inbound_system_txs(
    entries: &[InboundSidecar],
    cfg: &SystemTxContext,
    source: SourceEntry,
) -> Result<Vec<NativeTransactionPlan>, String> {
    let mut source = source;
    let mut out: Vec<NativeTransactionPlan> = Vec::new();
    for entry in entries {
        let incoming = entry.incoming_entry();
        if incoming.l2_rollup_id.0 != cfg.this_rollup_id {
            continue;
        }
        let l2_entry =
            build_l2_incoming_entry(incoming.clone()).map_err(|error| error.to_string())?;
        let calldata = encode_execute_incoming(l2_entry);
        let raw = encode_system_tx(
            source.hash(),
            cfg.eezl2_address,
            calldata,
            incoming.value,
            cfg.l2_chain_id,
        )?;
        source = source.offset(1)?;
        out.push(raw);
    }
    Ok(out)
}

/// Reconstruct one table load per outbound group. Each load must immediately
/// precede its signed user transaction. Rollup0 currently rejects nested tables.
///
/// # Errors
/// Reject invalid targets and source-index overflow.
pub fn build_outbound_load_table_txs(
    entries: &[L2ExecutionEntrySol],
    cfg: &SystemTxContext,
    source: SourceEntry,
) -> Result<Vec<NativeTransactionPlan>, String> {
    let mut source = source;
    let mut out: Vec<NativeTransactionPlan> = Vec::new();
    for entry in entries {
        // One entry per `loadExecutionTable`: the SyncPair pairs each load
        // with its consuming user tx, so a single-element table is correct
        // (the self-clean only matters across pairs, not within one). This is
        // the canonical encoding boundary for an already-validated lean L2
        // entry.
        let calldata = loadExecutionTableCall {
            _entries: vec![entry.clone()],
            _staticEntries: Vec::new(),
        }
        .abi_encode();
        let raw = encode_system_tx(
            source.hash(),
            cfg.eezl2_address,
            calldata,
            U256::ZERO, // loadExecutionTable carries no value
            cfg.l2_chain_id,
        )?;
        source = source.offset(1)?;
        out.push(raw);
    }
    Ok(out)
}

/// A locally reconstructed native call, never a received envelope. Source identity
/// is derived from authenticated context; gas is fixed by its execution position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTransactionPlan {
    chain_id: u64,
    source_hash: B256,
    value: U256,
    input: Bytes,
}

impl NativeTransactionPlan {
    /// Encode at the actual execution position. Never normalize received bytes.
    pub fn at_position(&self, block_gas_limit: u64, gas_used: u64) -> Result<Bytes, String> {
        if gas_used > block_gas_limit {
            return Err("native position exceeds block gas limit".into());
        }
        Ok(SystemTransaction {
            chain_id: self.chain_id,
            source_hash: self.source_hash,
            gas_limit: eez_primitives::system_tx_gas_limit(block_gas_limit, gas_used),
            to: EEZL2_ADDRESS,
            value: self.value,
            input: self.input.clone(),
        }
        .encoded_2718()
        .into())
    }
}

/// Local reconstruction input: native gas is resolved after preceding execution;
/// signed user bytes remain exact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlannedTransaction {
    /// Reconstructed native call, with position-dependent gas still unresolved.
    Native(NativeTransactionPlan),
    /// Exact signed user transaction bytes, never normalized.
    User(Bytes),
}
impl PlannedTransaction {
    /// Materialize a local plan after its preceding transactions have executed.
    pub fn at_position(&self, block_gas_limit: u64, gas_used: u64) -> Result<Bytes, String> {
        match self {
            Self::Native(plan) => plan.at_position(block_gas_limit, gas_used),
            Self::User(raw) => Ok(raw.clone()),
        }
    }
}

/// A Sync-block tx pair: a system tx and its OPTIONAL immediately-following
/// user tx.
///
/// - Inbound delivery: `user_tx = None` — the `executeIncomingCrossChainCall`
///   system tx self-loads + consumes in one tx; no user tx follows.
/// - Outbound L2→L1: `user_tx = Some(_)` — the `loadExecutionTable` system tx
///   stages the deferred entry, immediately consumed by the user tx's
///   `executeCrossChainCall` (the SyncPair `[load_i | user_i]`).
#[derive(Clone, Debug)]
pub struct SyncPair {
    /// The system tx (an unsigned native transaction).
    pub system_tx: NativeTransactionPlan,
    /// The user tx that consumes `system_tx`'s effect in the SAME block,
    /// immediately after it. `None` for a self-contained system tx.
    pub user_tx: Option<Bytes>,
}

/// The canonical Sync-block tx order: each pair's system tx immediately
/// followed by its user tx (if any) — `[s_1, u_1?, s_2, u_2?, …]`.
///
/// INTERLEAVED, not system-first: GIVEN the per-outbound-load failure-isolation
/// choice (see `build_outbound_load_table_txs`), each `loadExecutionTable` wipes
/// + resets the cursor in `_loadExecutionTable`, so each load's user tx must run
/// before the next load. Both `build_sync_block` (the composer) and the
/// deriver's reconstruction build their tx list through THIS fn, so the order
/// is identical by construction (no system-first vs interleaved drift → no
/// `to_block` root divergence). For inbound (all `user_tx == None`) it reduces
/// to `[system_txs…]`, byte-identical to the prior system-first layout.
#[must_use]
pub fn interleave_sync_block_txs(pairs: &[SyncPair]) -> Vec<PlannedTransaction> {
    let mut out = Vec::with_capacity(pairs.len() * 2);
    for pair in pairs {
        out.push(PlannedTransaction::Native(pair.system_tx.clone()));
        if let Some(user_tx) = &pair.user_tx {
            out.push(PlannedTransaction::User(user_tx.clone()));
        }
    }
    out
}

/// Reject the entry shapes the Sync-block lowering cannot represent.
///
/// N>=2 multi-call is NOT yet supported. An entry with multiple l2ToL1Calls
/// cannot be represented by the one-call outbound lowering or an
/// [`InboundSidecar`]. Fail LOUD until multi-call lands (design
/// parked; no doc yet). Today the composer only ever produces single-call
/// entries, so this never fires on the happy path; it is the safe boundary for
/// the parked feature.
///
/// The composer calls this per entry at accept time and
/// [`build_cross_chain_sync_pairs`] calls it over the whole drained set, so
/// both sides gate on the exact same predicate.
///
/// `dir` names the direction (`"outbound"` / `"inbound"`) in the error.
///
/// # Errors
/// Returns a `String` error naming the offending shape.
pub fn check_entry_shape(entry: &ExecutionEntrySol, dir: &str) -> Result<(), String> {
    if entry.l2ToL1Calls.len() > 1 {
        return Err(format!(
            "N>=2 multi-call {dir} entry not yet supported \
             (l2ToL1Calls={}); multi-call support is parked",
            entry.l2ToL1Calls.len(),
        ));
    }
    if !entry.expectedL1ToL2Calls.is_empty() {
        return Err(format!(
            "nested {dir} entry materialization is not supported"
        ));
    }
    if !entry.success {
        return Err(format!("unsuccessful {dir} entry is not supported"));
    }
    if entry
        .l2ToL1Calls
        .iter()
        .any(|call| call.isStatic || call.revertNextNCalls != 0 || call.gas != 0)
    {
        return Err(format!(
            "{dir} entry uses static, revert-span, or explicit-gas semantics that are not supported"
        ));
    }
    Ok(())
}

/// Reconstruct one outbound `[load, user]` group at its original source ordinal.
///
/// # Errors
/// Reject unsupported entry shapes and invalid targets.
pub fn build_outbound_pair(
    entry: &ExecutionEntrySol,
    user_tx: &Bytes,
    cfg: &SystemTxContext,
    source: SourceEntry,
) -> Result<Vec<SyncPair>, String> {
    check_entry_shape(entry, "outbound")?;
    let call = entry
        .l2ToL1Calls
        .first()
        .ok_or_else(|| "outbound entry must contain exactly one l2ToL1Call; found 0".to_string())?;
    let l2_entry = build_l2_outbound_entry(OutboundEntry {
        target: call.targetAddress,
        source: call.sourceAddress,
        value: call.value,
        data: call.data.clone(),
        l2_rollup_id: RollupId(cfg.this_rollup_id),
        return_data: entry.returnData.clone(),
        success: entry.success,
    })
    .map_err(|error| error.to_string())?;
    let loads = build_outbound_load_table_txs(std::slice::from_ref(&l2_entry), cfg, source)?;
    Ok(loads
        .into_iter()
        .map(|load| SyncPair {
            system_tx: load,
            user_tx: Some(user_tx.clone()),
        })
        .collect())
}

/// Canonical full-candidate order: all outbound `[load, user]` groups, then
/// inbound deliveries. Source ordinals advance once per group, not per tx.
/// Derivation must assign these before selecting L1-consumed groups.
///
/// # Errors
/// Reject unsupported entry shapes, invalid targets and source-index overflow.
pub fn build_cross_chain_sync_pairs(
    outbound: &[(ExecutionEntrySol, Bytes)],
    inbound: &[ExecutionEntrySol],
    cfg: &SystemTxContext,
    source: SourceEntry,
) -> Result<Vec<SyncPair>, String> {
    let mut source = source;
    let mut pairs: Vec<SyncPair> = Vec::with_capacity(outbound.len() + inbound.len());

    // Validate even foreign inbound entries, which the delivery builder skips.
    let inbound = inbound
        .iter()
        .map(|entry| {
            check_entry_shape(entry, "inbound")?;
            InboundSidecar::try_from(entry).map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;

    // Each user must consume its load before the next load resets the execution table.
    for (entry, user_tx) in outbound {
        let built = build_outbound_pair(entry, user_tx, cfg, source)?;
        source = source.offset(built.len() as u64)?;
        pairs.extend(built);
    }

    // Inbound groups follow all outbound groups in the full source sequence.
    let deliveries = build_inbound_system_txs(&inbound, cfg, source)?;
    for d in deliveries {
        pairs.push(SyncPair {
            system_tx: d,
            user_tx: None,
        });
    }

    Ok(pairs)
}

/// Use the unsigned native envelope so every role reconstructs identical bytes.
/// Restrict calls to EEZL2; its incoming-call check requires `value` to match
/// the value encoded in the calldata.
fn encode_system_tx(
    source_hash: B256,
    to: Address,
    calldata: Vec<u8>,
    value: U256,
    chain_id: u64,
) -> Result<NativeTransactionPlan, String> {
    if to != EEZL2_ADDRESS {
        return Err("native system transactions must target the EEZL2 predeploy".to_string());
    }
    Ok(NativeTransactionPlan {
        chain_id,
        source_hash,
        value,
        input: calldata.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvmBatch;
    use crate::abi::{L2ToL1CallSol, RollupIdWithProofSystemsSol};
    use crate::entries::{IncomingEntry, decode_postbatch, encode_postbatch};
    use alloy_primitives::address;
    use eez_primitives::EezTxEnvelope as TransactionSigned;

    fn ctx() -> SystemTxContext {
        SystemTxContext {
            eezl2_address: address!("4200000000000000000000000000000000000007"),
            l2_chain_id: 1,
            this_rollup_id: 1,
        }
    }

    fn inbound_entry() -> ExecutionEntrySol {
        InboundSidecar::new(IncomingEntry {
            target: address!("00000000000000000000000000000000000000bb"),
            source: address!("00000000000000000000000000000000000000cc"),
            value: U256::ZERO,
            data: Bytes::from(vec![0x12, 0x34]),
            source_rollup_id: RollupId::MAINNET,
            l2_rollup_id: RollupId(1),
            return_data: Bytes::from(vec![0xab, 0xcd]),
            success: true,
        })
        .unwrap()
        .as_abi_entry()
        .clone()
    }

    fn inbound_sidecars(entries: &[ExecutionEntrySol]) -> Vec<InboundSidecar> {
        entries
            .iter()
            .map(|entry| InboundSidecar::try_from(entry).unwrap())
            .collect()
    }

    fn round_tripped(entries: &[ExecutionEntrySol]) -> Vec<ExecutionEntrySol> {
        let mut batch = EvmBatch {
            entries: entries.to_vec(),
            ..Default::default()
        };
        batch.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: 1,
            proofSystemIndexes: vec![0],
        }];
        let calldata = encode_postbatch(&batch);
        decode_postbatch(&calldata)
            .expect("postBatch round-trips")
            .entries
    }

    /// One outbound L2→L1 immediate entry (`proxyEntryHash == 0`, the L2→L1
    /// call in `l2ToL1Calls[0]`), the shape `build_outbound_load_table_txs`
    /// lowers to a `loadExecutionTable` system tx.
    fn outbound_entry() -> ExecutionEntrySol {
        ExecutionEntrySol {
            rollupUpdates: Vec::new(),
            proxyEntryHash: B256::ZERO, // outbound immediate
            l2ToL1Calls: vec![L2ToL1CallSol {
                revertNextNCalls: 0,
                isStatic: false,
                gas: 0,
                sourceAddress: address!("00000000000000000000000000000000000000ee"),
                sourceRollupId: 1, // L2 source for an L2→L1 outbound
                targetAddress: address!("00000000000000000000000000000000000000dd"),
                value: U256::ZERO,
                data: Bytes::from(vec![0x55, 0x66]),
            }],
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: B256::ZERO,
            destinationRollupId: 1,
            success: true,
            returnData: Bytes::from(vec![0x77]),
        }
    }

    fn source(index: u64) -> SourceEntry {
        crate::native_source::LiveSourceContext::new(1, B256::repeat_byte(0x99))
            .unwrap()
            .entry(index)
    }
    fn source_of(plan: &NativeTransactionPlan) -> B256 {
        use alloy_eips::eip2718::Decodable2718 as _;
        let raw = plan.at_position(30_000_000, 0).unwrap();
        let TransactionSigned::System(tx) = TransactionSigned::decode_2718_exact(&raw).unwrap()
        else {
            panic!("expected native")
        };
        tx.source_hash
    }

    /// THE A2b property: the canonical builder lays a MIXED slot out in
    /// deriver-canonical order (all outbound load+user pairs, THEN inbound
    /// deliveries) with strict two-phase SYSTEM_ADDRESS sources — independent of
    /// the order the (outbound, inbound) args are constructed. K=1/M=1.
    #[test]
    fn cross_chain_sync_pairs_canonical_order_and_two_phase_sources() {
        let cfg = ctx();
        let n = 5u64;
        let user_tx = Bytes::from(vec![0xde, 0xad]);
        let pairs = build_cross_chain_sync_pairs(
            &[(outbound_entry(), user_tx.clone())],
            &[inbound_entry()],
            &cfg,
            source(n),
        )
        .unwrap();

        assert_eq!(pairs.len(), 2, "K=1 outbound + M=1 inbound → 2 pairs");
        // Outbound first, paired with its user tx; inbound second, self-contained.
        assert_eq!(
            pairs[0].user_tx,
            Some(user_tx),
            "pair[0] = outbound load + its user tx",
        );
        assert_eq!(
            pairs[1].user_tx, None,
            "pair[1] = inbound delivery, no user tx"
        );
        // Two-phase sources: outbound load = N, inbound delivery = N+1.
        assert_eq!(
            source_of(&pairs[0].system_tx),
            source(n).hash(),
            "outbound load source_index = N"
        );
        assert_eq!(
            source_of(&pairs[1].system_tx),
            source(n + 1).hash(),
            "inbound delivery source_index = N+1 (strictly AFTER all outbound loads)",
        );
    }

    /// N>=2 multi-call is rejected LOUD, not silently truncated. An entry with
    /// Two `l2ToL1Calls` would lower to only call[0] today (the
    /// outbound `.first()` + build_inbound_system_txs read only [0]); the guard
    /// turns that root-diverging footgun into a clear error naming the
    /// offending call count for the parked multi-call feature.
    #[test]
    fn cross_chain_sync_pairs_rejects_multicall_entries() {
        let cfg = ctx();
        let user = Bytes::from(vec![0x01]);

        // Outbound entry with two calls → rejected.
        let mut multi_out = outbound_entry();
        let extra = multi_out.l2ToL1Calls[0].clone();
        multi_out.l2ToL1Calls.push(extra);
        let err = build_cross_chain_sync_pairs(&[(multi_out, user.clone())], &[], &cfg, source(0))
            .expect_err("N>=2 outbound must be rejected, not silently truncated");
        assert!(err.contains("multi-call outbound"), "err: {err}");
        assert!(
            err.contains("multi-call support is parked"),
            "error must state the feature is parked: {err}",
        );
        assert!(
            err.contains("l2ToL1Calls=2"),
            "error must name the offending call count: {err}",
        );

        // Inbound entry with two calls → rejected.
        let mut multi_in = inbound_entry();
        let extra_in = multi_in.l2ToL1Calls[0].clone();
        multi_in.l2ToL1Calls.push(extra_in);
        let err = build_cross_chain_sync_pairs(&[], &[multi_in], &cfg, source(0))
            .expect_err("N>=2 inbound must be rejected");
        assert!(err.contains("multi-call inbound"), "err: {err}");

        // A single call still builds fine — the
        // guard is a no-op on the only shape the composer produces today.
        build_cross_chain_sync_pairs(
            &[(outbound_entry(), user)],
            &[inbound_entry()],
            &cfg,
            source(0),
        )
        .expect("single-call still builds through the guard");
    }

    #[test]
    fn cross_chain_sync_pairs_rejects_empty_outbound_entry() {
        let cfg = ctx();
        let mut outbound = outbound_entry();
        outbound.l2ToL1Calls.clear();

        let err = build_cross_chain_sync_pairs(
            &[(outbound, Bytes::from_static(&[0x01]))],
            &[],
            &cfg,
            source(0),
        )
        .expect_err("an outbound entry without a call must not drop its user transaction");

        assert!(
            err.contains("exactly one l2ToL1Call"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cross_chain_sync_pairs_rejects_unsuccessful_entries() {
        let cfg = ctx();

        let mut outbound = outbound_entry();
        outbound.success = false;
        let err = build_cross_chain_sync_pairs(
            &[(outbound, Bytes::from_static(&[0x01]))],
            &[],
            &cfg,
            source(0),
        )
        .expect_err("unsuccessful outbound entries are outside the supported profile");
        assert!(err.contains("unsuccessful outbound"), "err: {err}");

        let mut inbound = inbound_entry();
        inbound.success = false;
        let err = build_cross_chain_sync_pairs(&[], &[inbound], &cfg, source(0))
            .expect_err("unsuccessful inbound entries are outside the supported profile");
        assert!(err.contains("unsuccessful inbound"), "err: {err}");
    }

    /// Single-direction degenerates EXACTLY to the per-direction builds, so
    /// e2e_outbound / e2e_inbound stay byte-identical through the refactor.
    #[test]
    fn cross_chain_sync_pairs_degenerate_to_single_direction() {
        let cfg = ctx();
        let n = 3u64;
        let user = Bytes::from(vec![0x01]);

        // Outbound-only == build_outbound_load_table_txs at the same source_index.
        let out_pairs =
            build_cross_chain_sync_pairs(&[(outbound_entry(), user.clone())], &[], &cfg, source(n))
                .unwrap();
        let l2e = build_l2_outbound_entry(OutboundEntry {
            target: outbound_entry().l2ToL1Calls[0].targetAddress,
            source: outbound_entry().l2ToL1Calls[0].sourceAddress,
            value: U256::ZERO,
            data: outbound_entry().l2ToL1Calls[0].data.clone(),
            l2_rollup_id: RollupId(1),
            return_data: outbound_entry().returnData.clone(),
            success: true,
        })
        .unwrap();
        let direct_out =
            build_outbound_load_table_txs(std::slice::from_ref(&l2e), &cfg, source(n)).unwrap();
        assert_eq!(out_pairs.len(), 1);
        assert_eq!(
            out_pairs[0].system_tx, direct_out[0],
            "outbound-only byte-identical"
        );

        // Inbound-only == build_inbound_system_txs at the same source_index.
        let in_pairs =
            build_cross_chain_sync_pairs(&[], &[inbound_entry()], &cfg, source(n)).unwrap();
        let direct_in =
            build_inbound_system_txs(&inbound_sidecars(&[inbound_entry()]), &cfg, source(n))
                .unwrap();
        assert_eq!(in_pairs.len(), 1);
        assert_eq!(
            in_pairs[0].system_tx, direct_in[0],
            "inbound-only byte-identical"
        );
        assert!(in_pairs[0].user_tx.is_none());
    }

    /// Composer-emit == deriver-rebuild for the canonical mixed builder across
    /// the postBatch encode/decode round-trip (the byte-equality both sides rely
    /// on, now for BOTH directions in one call).
    #[test]
    fn cross_chain_sync_pairs_emit_equals_rebuild() {
        let cfg = ctx();
        let n = 9u64;
        let user = Bytes::from(vec![0x01, 0x02]);
        let emitted = build_cross_chain_sync_pairs(
            &[(outbound_entry(), user.clone())],
            &[inbound_entry()],
            &cfg,
            source(n),
        )
        .unwrap();
        let out_rt = round_tripped(&[outbound_entry()])[0].clone();
        let in_rt = round_tripped(&[inbound_entry()])[0].clone();
        let rebuilt =
            build_cross_chain_sync_pairs(&[(out_rt, user)], &[in_rt], &cfg, source(n)).unwrap();
        assert_eq!(emitted.len(), rebuilt.len());
        for (e, r) in emitted.iter().zip(&rebuilt) {
            assert_eq!(
                e.system_tx, r.system_tx,
                "system tx byte-identical across round-trip"
            );
            assert_eq!(e.user_tx, r.user_tx);
        }
    }

    /// A mid-chain resume rebuilds only the tail, which must be byte-equal to the
    /// composer's suffix: offset the source_index by txs the prefix EMITS, not entry count.
    #[test]
    fn resumed_tail_matches_full_build_when_offset_by_prefix_emitted_txs() {
        let cfg = ctx();
        let n = 11u64;
        let (u0, u1) = (Bytes::from(vec![0xa0]), Bytes::from(vec![0xa1]));
        let o0 = outbound_entry();
        let mut o1 = outbound_entry();
        o1.l2ToL1Calls[0].data = Bytes::from(vec![0x99, 0x88]);
        // An inbound entry for ANOTHER rollup: passes the shape gate, emits no
        // system tx on this L2.
        let mut foreign = InboundSidecar::try_from(&inbound_entry())
            .unwrap()
            .incoming_entry();
        foreign.l2_rollup_id = RollupId(2);
        let foreign = InboundSidecar::new(foreign).unwrap().as_abi_entry().clone();
        let in1 = inbound_entry();

        // Composer: the whole claimed chain, sources N.. in emit order.
        let full = build_cross_chain_sync_pairs(
            &[(o0.clone(), u0.clone()), (o1.clone(), u1.clone())],
            &[foreign.clone(), in1.clone()],
            &cfg,
            source(n),
        )
        .unwrap();
        assert_eq!(
            full.len(),
            3,
            "2 outbound loads + 1 delivery (the foreign entry emits none)"
        );
        assert_eq!(
            full.iter()
                .map(|p| source_of(&p.system_tx))
                .collect::<Vec<_>>(),
            vec![source(n).hash(), source(n + 1).hash(), source(n + 2).hash()],
        );

        // Deriver resume: a competing batch already settled the prefix
        // (1 outbound + the foreign inbound); only the rest ran under this batch.
        let (skip_out, skip_in) = (1usize, 1usize);
        let prefix = build_cross_chain_sync_pairs(
            &[(o0, u0)],
            std::slice::from_ref(&foreign),
            &cfg,
            source(n),
        )
        .unwrap();
        assert_eq!(
            prefix.len(),
            1,
            "the foreign entry contributes no system tx"
        );
        assert_ne!(
            prefix.len(),
            skip_out + skip_in,
            "the emitted count must differ from the entry count here, else the \
             offset assertion below is vacuous",
        );

        let tail_out = [(o1, u1)];
        let tail_in = [in1];
        let tail_at = |start: u64| {
            build_cross_chain_sync_pairs(&tail_out, &tail_in, &cfg, source(start)).unwrap()
        };

        let tail = tail_at(n + prefix.len() as u64);
        assert_eq!(tail.len(), full.len() - prefix.len());
        for (t, f) in tail.iter().zip(&full[prefix.len()..]) {
            assert_eq!(
                t.system_tx, f.system_tx,
                "rebuilt tail system tx must be byte-equal to the committed one",
            );
            assert_eq!(t.user_tx, f.user_tx);
        }

        // Negative control: no offset, or the entry-count offset, re-sources the
        // tail into different bytes and derivation halts.
        for wrong in [0u64, (skip_out + skip_in) as u64] {
            assert_ne!(
                tail_at(n + wrong)[0].system_tx,
                full[prefix.len()].system_tx,
                "offset {wrong} must NOT reproduce the committed tail",
            );
        }
    }

    /// Reconstructing inbound transactions from posted entries must preserve the
    /// composer's native bytes, or the follower derives a different block root.
    #[test]
    fn composer_emit_equals_deriver_rebuild_byte_identical() {
        let cfg = ctx();
        let source_index = 7u64;
        let entries = vec![inbound_entry()];

        let emitted =
            build_inbound_system_txs(&inbound_sidecars(&entries), &cfg, source(source_index))
                .unwrap();
        assert_eq!(emitted.len(), 1, "one inbound entry → one system tx");

        let rebuilt = build_inbound_system_txs(
            &inbound_sidecars(&round_tripped(&entries)),
            &cfg,
            source(source_index),
        )
        .unwrap();
        assert_eq!(
            emitted, rebuilt,
            "composer-emit must equal deriver-rebuild byte-for-byte (Phase-C invariant)",
        );
    }

    /// Canonical EIP-2718 bytes for the native unsigned system transaction
    /// represented by `inbound_entry()`. The prefix pins type `0x45` and
    /// the RLP body pins chain ID, source hash, gas limit, EEZL2 target, value, and calldata.
    #[test]
    fn inbound_system_transaction_golden_vector() {
        const EXPECTED: &str = concat!(
            "45f9038401a067fb5da1784078579c726c542563b4577c50fb90652c54566d49b9f170784d37840100000094420000000000000000000000000000000000000780b90344c3fb5f3d00000000000000000000000000",
            "000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000",
            "000320000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000",
            "0000000000000000000000000000000000002047d8d2c330b8e207c17da548b89a3058b10faad241a6b82f88302af38b",
            "ff8fb700000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000",
            "000000000000000000000000000000000002407b6cdaf67ec74ff399bdbcf0e0d2a68fb4aa7b638a87d382baef85c6ba",
            "8e5d3c000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000",
            "000000000000000000000000000000000002600000000000000000000000000000000000000000000000000000000000",
            "000001000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000",
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "000000000000000000000000000000000000cc0000000000000000000000000000000000000000000000000000000000",
            "00000000000000000000000000000000000000000000000000000000000000000000bb00000000000000000000000000",
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "000100000000000000000000000000000000000000000000000000000000000000000212340000000000000000000000",
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000000000002abcd0000000000000000000000",
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "000000",
        );

        let entries = [inbound_entry()];
        let built =
            build_inbound_system_txs(&inbound_sidecars(&entries), &ctx(), source(7)).unwrap();
        assert_eq!(built.len(), 1);
        assert_eq!(
            built[0].at_position(30_000_000, 0).unwrap().as_ref(),
            alloy_primitives::hex::decode(EXPECTED).unwrap(),
            "canonical native inbound system transaction bytes drifted",
        );
    }

    /// Guards against a vacuous pass: the signed bytes MUST vary with the source_index (so the
    /// equality above is non-trivial), and emit==rebuild must still hold at that source_index.
    #[test]
    fn byte_identity_is_non_vacuous_and_holds_across_sources() {
        let cfg = ctx();
        let entries = vec![inbound_entry()];
        let at0 = build_inbound_system_txs(&inbound_sidecars(&entries), &cfg, source(0)).unwrap();
        let at99 = build_inbound_system_txs(&inbound_sidecars(&entries), &cfg, source(99)).unwrap();
        assert_ne!(
            at0, at99,
            "different source_index must change the native bytes"
        );
        assert_eq!(
            build_inbound_system_txs(
                &inbound_sidecars(&round_tripped(&entries)),
                &cfg,
                source(99)
            )
            .unwrap(),
            at99,
            "emit==rebuild at the second source_index too",
        );
    }

    /// A2.1b: the outbound `loadExecutionTable` system tx is byte-identical
    /// whether built from the composer's in-memory entry or from the entry
    /// decoded back out of the `loadExecutionTable` calldata (the deriver's
    /// rebuild) — the Q4 shared-constructor / P-1 byte-equality. One tx per
    /// entry (the SyncPair per-pair load).
    #[test]
    fn outbound_load_table_emit_equals_deriver_rebuild() {
        use crate::entries::{OutboundEntry, build_l2_outbound_entry};

        let cfg = ctx();
        let source_index = 7u64;
        let entry = build_l2_outbound_entry(OutboundEntry {
            target: address!("00000000000000000000000000000000000000cc"),
            source: address!("00000000000000000000000000000000000000dd"),
            value: U256::ZERO,
            data: Bytes::from(vec![0x55, 0x24, 0x10, 0x77, 0x07]),
            l2_rollup_id: RollupId(cfg.this_rollup_id),
            return_data: Bytes::new(),
            success: true,
        })
        .unwrap();

        let emitted =
            build_outbound_load_table_txs(std::slice::from_ref(&entry), &cfg, source(source_index))
                .unwrap();
        assert_eq!(
            emitted.len(),
            1,
            "one outbound entry → one loadExecutionTable tx"
        );

        // Deriver rebuild: decode the entry back out of the loadExecutionTable
        // calldata and re-emit — must be byte-identical.
        let calldata = loadExecutionTableCall {
            _entries: vec![entry.clone()],
            _staticEntries: Vec::new(),
        }
        .abi_encode();
        let decoded =
            loadExecutionTableCall::abi_decode(&calldata).expect("loadExecutionTable round-trips");
        let rebuilt =
            build_outbound_load_table_txs(&decoded._entries, &cfg, source(source_index)).unwrap();
        assert_eq!(
            emitted, rebuilt,
            "composer-emit must equal deriver-rebuild byte-for-byte"
        );

        // Non-vacuous: the native bytes vary with the source_index.
        let at99 = build_outbound_load_table_txs(&[entry], &cfg, source(99)).unwrap();
        assert_ne!(
            emitted, at99,
            "different source_index must change the native bytes"
        );
    }

    /// A2.2: the canonical Sync-block order interleaves each system tx with its
    /// optional user tx — `[s, u?, …]` — so an outbound `[load | user]` pair
    /// stays adjacent (the load's self-clean can't wipe a not-yet-consumed
    /// entry), while inbound (user `None`) reduces to `[system_txs]` exactly as
    /// the prior system-first layout.
    #[test]
    fn interleave_sync_block_txs_orders_pairs() {
        let s1 = NativeTransactionPlan {
            chain_id: 1,
            source_hash: source(0x11).hash(),
            value: U256::ZERO,
            input: Bytes::new(),
        };
        let u1 = Bytes::from(vec![0xA1]);
        let s2 = NativeTransactionPlan {
            chain_id: 1,
            source_hash: source(0x22).hash(),
            value: U256::ZERO,
            input: Bytes::new(),
        };
        let s3 = NativeTransactionPlan {
            chain_id: 1,
            source_hash: source(0x33).hash(),
            value: U256::ZERO,
            input: Bytes::new(),
        };
        let u3 = Bytes::from(vec![0xA3]);

        let out = interleave_sync_block_txs(&[
            SyncPair {
                system_tx: s1.clone(),
                user_tx: Some(u1.clone()),
            },
            SyncPair {
                system_tx: s2.clone(),
                user_tx: None,
            },
            SyncPair {
                system_tx: s3.clone(),
                user_tx: Some(u3.clone()),
            },
        ]);
        assert_eq!(
            out,
            vec![
                PlannedTransaction::Native(s1.clone()),
                PlannedTransaction::User(u1),
                PlannedTransaction::Native(s2.clone()),
                PlannedTransaction::Native(s3),
                PlannedTransaction::User(u3)
            ],
            "[s1,u1, s2, s3,u3] — each system tx then its optional user tx, in order",
        );

        // All-inbound (every user_tx None) == [system_txs] — behavior-preserving.
        let sys = [s1, s2];
        assert_eq!(
            interleave_sync_block_txs(&[
                SyncPair {
                    system_tx: sys[0].clone(),
                    user_tx: None
                },
                SyncPair {
                    system_tx: sys[1].clone(),
                    user_tx: None
                },
            ]),
            sys.into_iter()
                .map(PlannedTransaction::Native)
                .collect::<Vec<_>>(),
            "inbound (no user txs) reduces to the system-only order",
        );

        assert!(interleave_sync_block_txs(&[]).is_empty());
    }
}
