//! Single source of truth for building signed L2 inbound system txs.
//!
//! Given the cross-chain entries that will land in a `postBatch` —
//! whether observed locally by the composer's `simulate_and_resolve`
//! or read from L1's `BatchPosted` event by the deriver — produce
//! the **same** signed `executeIncomingCrossChainCall(...)` system
//! txs that should appear at the head of the L2 Sync block.
//!
//! Composer and deriver agree mechanically: both call
//! [`build_inbound_system_txs`] with the same `entries`, so the signed
//! txs are byte-identical (`Rollup-1.md §5`: system txs precede user
//! txs).
//!
//! Txs are signed legacy txs from the SYSTEM_ADDRESS key — works
//! against vanilla reth without a custom tx type, and both processes
//! sign with the same key so the sigs match. (Type-0x7E system txs,
//! per `Rollup-1.md §5.3`, would drop the deriver's need for the key;
//! a follow-up.)
//!
//! Nonce: both sides read the SYSTEM_ADDRESS nonce from local L2 state
//! at the same parent block. Reth derives it deterministically from
//! applied history, so equal histories give equal nonce → signature →
//! tx hash.

use alloy_consensus::TxLegacy;
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::SolCall;
use reth_ethereum_primitives::{Transaction, TransactionSigned};

use crate::RollupId;
use crate::abi::{ExecutionEntrySol, L2ExecutionEntrySol, loadExecutionTableCall};
use crate::entries::{
    IncomingEntry, OutboundEntry, build_l2_incoming_entry, build_l2_outbound_entry,
    encode_execute_incoming,
};

/// Per-follower configuration the system-tx builder needs. The
/// composer and deriver each build one from their own startup config.
#[derive(Clone, Debug)]
pub struct SystemTxContext {
    /// SYSTEM_ADDRESS signer for this L2. Must match the L2's
    /// `EEZL2.SYSTEM_ADDRESS` immutable, otherwise
    /// `executeIncomingCrossChainCall`'s `onlySystemAddress` modifier
    /// reverts.
    pub system_signer: PrivateKeySigner,
    /// Address of the `EEZL2` contract on this L2.
    pub eezl2_address: Address,
    /// EIP-155 chain id of this L2.
    pub l2_chain_id: u64,
    /// Legacy `gasPrice` for the signed system tx. Dev/devnet uses
    /// 1 gwei (above the dev-mode 0 basefee).
    pub l2_gas_price: u128,
    /// Per-tx gas budget. Matches
    /// `crosschain-evm-composer::EXECUTE_INCOMING_GAS_LIMIT` (~2M)
    /// from the reference impl.
    pub l2_gas_limit: u64,
    /// This rollup's id — entries whose `destinationRollupId` doesn't
    /// match are skipped (they belong to a different L2).
    pub this_rollup_id: u64,
}

/// Build signed L2 inbound system txs from a postBatch's entries.
///
/// For each entry whose `destinationRollupId == cfg.this_rollup_id`,
/// reconstructs the outer cross-chain call from
/// `entry.L2ToL1Calls[0]` and produces one signed legacy tx invoking
/// `EEZL2.executeIncomingCrossChainCall(...)`. Entries for other
/// rollups are skipped.
///
/// `starting_nonce` is the SYSTEM_ADDRESS account nonce at the L2
/// parent block. The function advances locally by one per emitted
/// tx — callers don't need to thread nonces themselves.
///
/// # Errors
///
/// Returns a `String` error if a signature operation fails (signer
/// chain-id disagreement, malformed key, etc.).
pub fn build_inbound_system_txs(
    entries: &[ExecutionEntrySol],
    cfg: &SystemTxContext,
    starting_nonce: u64,
) -> Result<Vec<Bytes>, String> {
    let mut nonce = starting_nonce;
    let mut out: Vec<Bytes> = Vec::new();
    for entry in entries {
        if entry.destinationRollupId != cfg.this_rollup_id {
            continue;
        }
        if entry.l2ToL1Calls.is_empty() {
            continue;
        }
        if !entry.success
            || entry.l2ToL1Calls.len() != 1
            || !entry.expectedL1ToL2Calls.is_empty()
            || entry.l2ToL1Calls[0].isStatic
            || entry.l2ToL1Calls[0].revertNextNCalls != 0
            || entry.l2ToL1Calls[0].gas != 0
        {
            return Err(
                "inbound system transaction uses an unsupported execution shape".to_owned(),
            );
        }
        let outer = &entry.l2ToL1Calls[0];
        let source_rollup = outer.sourceRollupId;
        // Build the lean L2 mirror, then encode
        // `executeIncomingCrossChainCall(...)`. NOTE: inbound runtime semantics
        // (success / return_data byte-identity vs the composer's emit) are
        // validated in the prover phase — outbound-first does not exercise
        // this delivery path at runtime.
        let l2_entry = build_l2_incoming_entry(IncomingEntry {
            target: outer.targetAddress,
            source: outer.sourceAddress,
            value: outer.value,
            data: outer.data.clone(),
            source_rollup_id: RollupId(source_rollup),
            l2_rollup_id: RollupId(cfg.this_rollup_id),
            return_data: entry.returnData.clone(),
            success: entry.success,
        })
        .map_err(|error| error.to_string())?;
        let calldata = encode_execute_incoming(
            outer.targetAddress,
            outer.value,
            outer.data.clone(),
            outer.sourceAddress,
            RollupId(source_rollup),
            l2_entry,
        );
        let raw = sign_legacy_system_tx(
            &cfg.system_signer,
            nonce,
            cfg.eezl2_address,
            calldata,
            outer.value,
            cfg.l2_chain_id,
            cfg.l2_gas_price,
            cfg.l2_gas_limit,
        )?;
        nonce = nonce.checked_add(1).ok_or_else(|| {
            "SYSTEM_ADDRESS nonce overflow in build_inbound_system_txs".to_string()
        })?;
        out.push(raw);
    }
    Ok(out)
}

/// Build SYNC-block `loadExecutionTable` system txs for OUTBOUND L2→L1
/// deferred entries — ONE tx per entry, mirroring [`build_inbound_system_txs`]'s
/// one-tx-per-entry shape. Each tx stages a single entry that the
/// immediately-following user tx's `EEZL2.executeCrossChainCall` consumes,
/// so the SyncPair block layout is `[load_1 | user_1 | load_2 | user_2 | …]`.
/// Per-outbound-load is a deliberate FAILURE-ISOLATION choice, NOT forced: a
/// single `loadExecutionTable([all entries])` followed by N user txs also works
/// (`entryIndex` is persistent storage advanced by `_consumeAndExecute`). But
/// `_loadExecutionTable` replaces the table and resets `entryIndex` to zero, so
/// one load per entry isolates a reverting/desync'd withdrawal — it can't
/// cascade-desync the cursor for the rest. Given that choice, each load's user
/// tx must run before the next load wipes the table → the interleaved order.
///
/// `starting_nonce` is the SYSTEM_ADDRESS account nonce at the L2 parent
/// block; advanced by one per emitted tx (callers don't thread nonces).
///
/// # Errors
///
/// Returns a `String` error if a signature operation fails or the
/// SYSTEM_ADDRESS nonce overflows.
pub fn build_outbound_load_table_txs(
    entries: &[L2ExecutionEntrySol],
    cfg: &SystemTxContext,
    starting_nonce: u64,
) -> Result<Vec<Bytes>, String> {
    let mut nonce = starting_nonce;
    let mut out: Vec<Bytes> = Vec::new();
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
        let raw = sign_legacy_system_tx(
            &cfg.system_signer,
            nonce,
            cfg.eezl2_address,
            calldata,
            U256::ZERO, // loadExecutionTable carries no value
            cfg.l2_chain_id,
            cfg.l2_gas_price,
            cfg.l2_gas_limit,
        )?;
        nonce = nonce.checked_add(1).ok_or_else(|| {
            "SYSTEM_ADDRESS nonce overflow in build_outbound_load_table_txs".to_string()
        })?;
        out.push(raw);
    }
    Ok(out)
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
    /// The system tx (a signed SYSTEM_ADDRESS tx).
    pub system_tx: Bytes,
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
pub fn interleave_sync_block_txs(pairs: &[SyncPair]) -> Vec<Bytes> {
    let mut out: Vec<Bytes> = Vec::with_capacity(pairs.len() * 2);
    for pair in pairs {
        out.push(pair.system_tx.clone());
        if let Some(user_tx) = &pair.user_tx {
            out.push(user_tx.clone());
        }
    }
    out
}

/// THE canonical Sync-block `SyncPair` builder — the SINGLE source of truth
/// both the composer (to commit the Sync block) and the deriver (to
/// reconstruct it) call, so the two are byte-identical BY CONSTRUCTION: no
/// drain-order-vs-two-phase nonce drift, no interleaved-vs-system-first order
/// drift. (Replaces the two sides independently calling
/// `build_outbound_load_table_txs` / `build_inbound_system_txs` in their own
/// order with their own nonce sequencing — the source of the mixed-batch
/// fork.)
///
/// Canonical order (given the per-outbound-load failure-isolation choice +
/// `_loadExecutionTable`'s table replacement + cursor reset, which
/// `executeIncomingCrossChainCall` also triggers):
/// ALL outbound load+user pairs FIRST, THEN all inbound deliveries —
/// `[load_0,user_0, …, load_{K-1},user_{K-1}, deliver_0, …, deliver_{M-1}]`.
/// SYSTEM_ADDRESS nonces run strictly in that order from `starting_nonce`:
/// outbound loads `N..N+K-1`, inbound deliveries `N+K..` (the deriver's
/// canonical two-phase order, `deriver.rs`). For the value-free A2b target
/// K=1/M=1 the list is `[load(N), user, deliver(N+1)]`.
///
/// `outbound`: each `(L1-shape outbound ExecutionEntrySol, its consuming user
/// tx)` in canonical (entry) order; the L2→L1 call is rebuilt from
/// `entry.l2ToL1Calls[0]` (the same lowering both sides apply). `inbound`: the
/// L1-shape inbound deferred entries (`build_inbound_system_txs` reads
/// `l2ToL1Calls[0]`; entries for other rollups / empty are skipped).
///
/// Single-direction degenerates EXACTLY: outbound-only → `[load,user,…]`;
/// inbound-only → `[deliver,…]` (all `user_tx == None`) — byte-identical to
/// the pre-refactor per-direction builds.
///
/// # Errors
/// Rejects unsupported entry shapes, signing failures, and SYSTEM_ADDRESS
/// nonce overflow.
pub fn build_cross_chain_sync_pairs(
    outbound: &[(ExecutionEntrySol, Bytes)],
    inbound: &[ExecutionEntrySol],
    cfg: &SystemTxContext,
    starting_nonce: u64,
) -> Result<Vec<SyncPair>, String> {
    let mut nonce = starting_nonce;
    let mut pairs: Vec<SyncPair> = Vec::with_capacity(outbound.len() + inbound.len());

    // N>=2 multi-call is NOT yet supported. An entry with multiple l2ToL1Calls
    // would be SILENTLY TRUNCATED to call[0] below (the
    // outbound `.first()` and build_inbound_system_txs both read only [0]),
    // diverging the Sync-block root with no error — the #1 footgun called out in
    // docs/multicall-design.md. Fail LOUD until multi-call lands. Today the
    // composer only ever produces single-call entries, so this never fires on
    // the happy path; it is the safe boundary for the parked feature.
    let reject_multicall = |entry: &ExecutionEntrySol, dir: &str| -> Result<(), String> {
        if entry.l2ToL1Calls.len() > 1 {
            return Err(format!(
                "N>=2 multi-call {dir} entry not yet supported \
                 (l2ToL1Calls={})",
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
    };
    for (entry, _) in outbound {
        reject_multicall(entry, "outbound")?;
    }
    for entry in inbound {
        reject_multicall(entry, "inbound")?;
    }

    // ── PHASE 1 — outbound: each loadExecutionTable immediately paired with
    // its consuming user tx (the self-clean requires consume-before-next-load).
    for (entry, user_tx) in outbound {
        let call = entry.l2ToL1Calls.first().ok_or_else(|| {
            "outbound entry must contain exactly one l2ToL1Call; found 0".to_string()
        })?;
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
        let loads = build_outbound_load_table_txs(std::slice::from_ref(&l2_entry), cfg, nonce)?;
        nonce = nonce
            .checked_add(loads.len() as u64)
            .ok_or_else(|| "SYSTEM_ADDRESS nonce overflow (outbound loads)".to_string())?;
        for load in loads {
            pairs.push(SyncPair {
                system_tx: load,
                user_tx: Some(user_tx.clone()),
            });
        }
    }

    // ── PHASE 2 — inbound deliveries, continuing the SYSTEM_ADDRESS nonce
    // after ALL outbound loads (build_inbound_system_txs advances internally).
    let deliveries = build_inbound_system_txs(inbound, cfg, nonce)?;
    for d in deliveries {
        pairs.push(SyncPair {
            system_tx: d,
            user_tx: None,
        });
    }

    Ok(pairs)
}

/// Sign a single legacy L2 tx from SYSTEM_ADDRESS with an explicit
/// `value`.
///
/// `EEZL2.executeIncomingCrossChainCall` enforces strict
/// `msg.value == value` equality in `executeIncomingCrossChainCall` — pass the same
/// value here as is embedded in the calldata.
///
/// # Errors
///
/// Returns a `String` error if `sign_transaction_sync` fails.
#[expect(
    clippy::too_many_arguments,
    reason = "wrapping in a struct would just move the arity to the constructor; \
              every field is load-bearing and the function is private to this module"
)]
fn sign_legacy_system_tx(
    signer: &PrivateKeySigner,
    nonce: u64,
    to: Address,
    calldata: Vec<u8>,
    value: U256,
    chain_id: u64,
    gas_price: u128,
    gas_limit: u64,
) -> Result<Bytes, String> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price,
        gas_limit,
        to: TxKind::Call(to),
        value,
        input: calldata.into(),
    };
    let sig = signer
        .sign_transaction_sync(&mut tx)
        .map_err(|e| format!("sign_transaction_sync: {e}"))?;
    let signed = TransactionSigned::new_unhashed(Transaction::Legacy(tx), sig);
    let mut buf = Vec::with_capacity(512);
    signed.encode_2718(&mut buf);
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvmBatch;
    use crate::abi::{L2ToL1CallSol, RollupIdWithProofSystemsSol, StateUpdateSol};
    use crate::entries::{decode_postbatch, encode_postbatch};
    use alloy_primitives::{B256, I256, address};

    fn ctx() -> SystemTxContext {
        SystemTxContext {
            system_signer: PrivateKeySigner::from_bytes(&B256::with_last_byte(1)).unwrap(),
            eezl2_address: address!("4200000000000000000000000000000000000007"),
            l2_chain_id: 1,
            l2_gas_price: 1_000_000_000,
            l2_gas_limit: 2_000_000,
            this_rollup_id: 1,
        }
    }

    /// One inbound L1→L2 deferred entry (the call lives in `l2ToL1Calls[0]`,
    /// `destinationRollupId == this_rollup_id`), the shape `build_inbound_system_txs`
    /// lowers to an `executeIncomingCrossChainCall` system tx.
    fn inbound_entry() -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateUpdates: vec![StateUpdateSol {
                rollupId: 1,
                currentState: B256::ZERO,
                newState: B256::repeat_byte(0x11),
                etherDelta: I256::ZERO,
            }],
            proxyEntryHash: B256::repeat_byte(0xab),
            l2ToL1Calls: vec![L2ToL1CallSol {
                revertNextNCalls: 0,
                isStatic: false,
                gas: 0,
                sourceAddress: address!("00000000000000000000000000000000000000cc"),
                sourceRollupId: 0, // MAINNET source for an L1→L2 inbound
                targetAddress: address!("00000000000000000000000000000000000000bb"),
                value: U256::ZERO,
                data: Bytes::from(vec![0x12, 0x34]),
            }],
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: B256::ZERO,
            destinationRollupId: 1,
            success: true,
            returnData: Bytes::from(vec![0xab, 0xcd]),
        }
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
            stateUpdates: Vec::new(),
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

    /// Decode a signed legacy system tx's nonce.
    fn nonce_of(raw: &Bytes) -> u64 {
        use alloy_consensus::Transaction as _;
        use alloy_eips::eip2718::Decodable2718 as _;
        let mut s: &[u8] = raw.as_ref();
        TransactionSigned::decode_2718(&mut s)
            .expect("decode system tx")
            .nonce()
    }

    /// THE A2b property: the canonical builder lays a MIXED slot out in
    /// deriver-canonical order (all outbound load+user pairs, THEN inbound
    /// deliveries) with strict two-phase SYSTEM_ADDRESS nonces — independent of
    /// the order the (outbound, inbound) args are constructed. K=1/M=1.
    #[test]
    fn cross_chain_sync_pairs_canonical_order_and_two_phase_nonces() {
        let cfg = ctx();
        let n = 5u64;
        let user_tx = Bytes::from(vec![0xde, 0xad]);
        let pairs = build_cross_chain_sync_pairs(
            &[(outbound_entry(), user_tx.clone())],
            &[inbound_entry()],
            &cfg,
            n,
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
        // Two-phase nonces: outbound load = N, inbound delivery = N+1.
        assert_eq!(nonce_of(&pairs[0].system_tx), n, "outbound load nonce = N");
        assert_eq!(
            nonce_of(&pairs[1].system_tx),
            n + 1,
            "inbound delivery nonce = N+1 (strictly AFTER all outbound loads)",
        );
    }

    /// N>=2 multi-call is rejected LOUD, not silently truncated. An entry with
    /// Two `l2ToL1Calls` would lower to only call[0] today (the
    /// outbound `.first()` + build_inbound_system_txs read only [0]); the guard
    /// turns that root-diverging footgun into a clear error naming the
    /// offending call count.
    #[test]
    fn cross_chain_sync_pairs_rejects_multicall_entries() {
        let cfg = ctx();
        let user = Bytes::from(vec![0x01]);

        // Outbound entry with two calls → rejected.
        let mut multi_out = outbound_entry();
        let extra = multi_out.l2ToL1Calls[0].clone();
        multi_out.l2ToL1Calls.push(extra);
        let err = build_cross_chain_sync_pairs(&[(multi_out, user.clone())], &[], &cfg, 0)
            .expect_err("N>=2 outbound must be rejected, not silently truncated");
        assert!(err.contains("multi-call outbound"), "err: {err}");
        assert!(
            err.contains("l2ToL1Calls=2"),
            "error must name the offending call count: {err}",
        );

        // Inbound entry with two calls → rejected.
        let mut multi_in = inbound_entry();
        let extra_in = multi_in.l2ToL1Calls[0].clone();
        multi_in.l2ToL1Calls.push(extra_in);
        let err = build_cross_chain_sync_pairs(&[], &[multi_in], &cfg, 0)
            .expect_err("N>=2 inbound must be rejected");
        assert!(err.contains("multi-call inbound"), "err: {err}");

        // A single call still builds fine — the
        // guard is a no-op on the only shape the composer produces today.
        build_cross_chain_sync_pairs(&[(outbound_entry(), user)], &[inbound_entry()], &cfg, 0)
            .expect("single-call still builds through the guard");
    }

    #[test]
    fn cross_chain_sync_pairs_rejects_empty_outbound_entry() {
        let cfg = ctx();
        let mut outbound = outbound_entry();
        outbound.l2ToL1Calls.clear();

        let err =
            build_cross_chain_sync_pairs(&[(outbound, Bytes::from_static(&[0x01]))], &[], &cfg, 0)
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
        let err =
            build_cross_chain_sync_pairs(&[(outbound, Bytes::from_static(&[0x01]))], &[], &cfg, 0)
                .expect_err("unsuccessful outbound entries are outside the supported profile");
        assert!(err.contains("unsuccessful outbound"), "err: {err}");

        let mut inbound = inbound_entry();
        inbound.success = false;
        let err = build_cross_chain_sync_pairs(&[], &[inbound], &cfg, 0)
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

        // Outbound-only == build_outbound_load_table_txs at the same nonce.
        let out_pairs =
            build_cross_chain_sync_pairs(&[(outbound_entry(), user.clone())], &[], &cfg, n)
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
            build_outbound_load_table_txs(std::slice::from_ref(&l2e), &cfg, n).unwrap();
        assert_eq!(out_pairs.len(), 1);
        assert_eq!(
            out_pairs[0].system_tx, direct_out[0],
            "outbound-only byte-identical"
        );

        // Inbound-only == build_inbound_system_txs at the same nonce.
        let in_pairs = build_cross_chain_sync_pairs(&[], &[inbound_entry()], &cfg, n).unwrap();
        let direct_in = build_inbound_system_txs(&[inbound_entry()], &cfg, n).unwrap();
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
            n,
        )
        .unwrap();
        let out_rt = round_tripped(&[outbound_entry()])[0].clone();
        let in_rt = round_tripped(&[inbound_entry()])[0].clone();
        let rebuilt = build_cross_chain_sync_pairs(&[(out_rt, user)], &[in_rt], &cfg, n).unwrap();
        assert_eq!(emitted.len(), rebuilt.len());
        for (e, r) in emitted.iter().zip(&rebuilt) {
            assert_eq!(
                e.system_tx, r.system_tx,
                "system tx byte-identical across round-trip"
            );
            assert_eq!(e.user_tx, r.user_tx);
        }
    }

    /// Phase-C invariant (the migration plan's ★ HIGHEST-RISK item): the composer
    /// EMITS the inbound system tx from its in-memory entries; the standalone deriver
    /// REBUILDS it from the L1 `postBatch` (encode → on-chain → decode). The shared
    /// `build_inbound_system_txs` MUST produce BYTE-IDENTICAL signed txs across that
    /// encode/decode round-trip — otherwise the derived L2 block forks from the
    /// composer's and the next postBatch root mismatches: a SILENT soundness failure
    /// (based has no deriver, so this equality was never exercised upstream).
    ///
    /// (eez0 keeps `TxLegacy` — fixed `gasPrice`, no `base_fee` — so the plan's
    /// `max_fee = base_fee*2` asymmetry trap does not apply; the surface is the
    /// entry encode/decode preservation + nonce agreement, which this pins.)
    #[test]
    fn composer_emit_equals_deriver_rebuild_byte_identical() {
        let cfg = ctx();
        let nonce = 7u64;
        let entries = vec![inbound_entry()];

        let emitted = build_inbound_system_txs(&entries, &cfg, nonce).unwrap();
        assert_eq!(emitted.len(), 1, "one inbound entry → one system tx");

        let rebuilt = build_inbound_system_txs(&round_tripped(&entries), &cfg, nonce).unwrap();
        assert_eq!(
            emitted, rebuilt,
            "composer-emit must equal deriver-rebuild byte-for-byte (Phase-C invariant)",
        );
    }

    /// Guards against a vacuous pass: the signed bytes MUST vary with the nonce (so the
    /// equality above is non-trivial), and emit==rebuild must still hold at that nonce.
    #[test]
    fn byte_identity_is_non_vacuous_and_holds_across_nonces() {
        let cfg = ctx();
        let entries = vec![inbound_entry()];
        let at0 = build_inbound_system_txs(&entries, &cfg, 0).unwrap();
        let at99 = build_inbound_system_txs(&entries, &cfg, 99).unwrap();
        assert_ne!(at0, at99, "different nonce must change the signed bytes");
        assert_eq!(
            build_inbound_system_txs(&round_tripped(&entries), &cfg, 99).unwrap(),
            at99,
            "emit==rebuild at the second nonce too",
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
        let nonce = 7u64;
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
            build_outbound_load_table_txs(std::slice::from_ref(&entry), &cfg, nonce).unwrap();
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
        let rebuilt = build_outbound_load_table_txs(&decoded._entries, &cfg, nonce).unwrap();
        assert_eq!(
            emitted, rebuilt,
            "composer-emit must equal deriver-rebuild byte-for-byte"
        );

        // Non-vacuous: the signed bytes vary with the nonce.
        let at99 = build_outbound_load_table_txs(&[entry], &cfg, 99).unwrap();
        assert_ne!(
            emitted, at99,
            "different nonce must change the signed bytes"
        );
    }

    /// A2.2: the canonical Sync-block order interleaves each system tx with its
    /// optional user tx — `[s, u?, …]` — so an outbound `[load | user]` pair
    /// stays adjacent (the load's self-clean can't wipe a not-yet-consumed
    /// entry), while inbound (user `None`) reduces to `[system_txs]` exactly as
    /// the prior system-first layout.
    #[test]
    fn interleave_sync_block_txs_orders_pairs() {
        let s1 = Bytes::from(vec![0x11]);
        let u1 = Bytes::from(vec![0xA1]);
        let s2 = Bytes::from(vec![0x22]);
        let s3 = Bytes::from(vec![0x33]);
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
            vec![s1, u1, s2, s3, u3],
            "[s1,u1, s2, s3,u3] — each system tx then its optional user tx, in order",
        );

        // All-inbound (every user_tx None) == [system_txs] — behavior-preserving.
        let sys = [Bytes::from(vec![1]), Bytes::from(vec![2])];
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
            sys.to_vec(),
            "inbound (no user txs) reduces to the system-only order",
        );

        assert!(interleave_sync_block_txs(&[]).is_empty());
    }
}
