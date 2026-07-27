//! Outbound L2->L1 authorization gate — the soundness check the DERIVER runs
//! (over the reconciled batch + the Sync block it re-executed) when re-deriving
//! the L2 chain from L1. Designed to be the SHARED check a future out-of-process
//! prover would also run (so the two can never drift), but in THIS repo only the
//! deriver calls it — the composer builds these entries from its own drained,
//! signed mempool txs, and the mock prover runs no gate. The deriver is the
//! canonical-chain authority, so this is the enforcement point today.
//!
//! An outbound settlement entry (an immediate — `proxyEntryHash == 0` — carrying
//! a non-empty `l2ToL1Calls`) claims an L2->L1 call L1 will EXECUTE, paying the
//! rollup's escrowed ether. The gate proves the L2 ACTUALLY originated it by
//! recomputing the entry's cross-chain call hash and matching it against the
//! `CrossChainCallExecuted` events the Sync block emitted when the deriver
//! re-executed it (`EEZL2.executeCrossChainCall`, `EEZL2.sol:200`).
//!
//! `EEZL2` computes that hash from the ACTUAL call: `sourceAddress` is the
//! proxy's IMMEDIATE caller at ANY depth — so a WRAPPER contract that internally
//! calls the proxy is a first-class source (an L2 DeFi contract swapping on an L1
//! pool) — `targetRollupId` is the proxy's `originalRollupId` (MAINNET=0 for an
//! L1 target), and `sourceRollupId` is forced to this L2's `ROLLUP_ID`. Matching
//! that single hash therefore binds ALL of: source (incl. a wrapper), target,
//! value, data, and target-rollup==MAINNET, in one comparison. It is exactly
//! what the outbound consume path enforces on re-execution: `_consumeAndExecute`
//! reverts `ExecutionNotFound` unless a loaded entry's `proxyEntryHash` equals
//! this hash (`EEZL2.sol:405`,`:408`; the inbound analog is `EntryHashMismatch`
//! at `:265`) — and what a real zk-prover of the L2 STF proves — so the bind
//! is UNFORGEABLE: a composer cannot make a phantom settlement entry match an
//! event, because emitting that event requires a real signed DA tx that, when
//! deterministically replayed, actually makes that exact call. Anti-phantom
//! safety thus rests on "a composer can't forge what deterministic execution of
//! the signed DA produces" — a superset of the prior gate's "can't forge a
//! signature", and one that (unlike the prior gate) covers contract-initiated
//! withdrawals. See `docs/OUTBOUND-VIA-WRAPPER-GATE.md`.
//!
//! (The prior gate bound each entry positionally to the top-level signed
//! Sync-block user tx's `to`/`signer`/`value`/`data`. Those binds only hold for
//! a DIRECT EOA->proxy call: a wrapper makes `sourceAddress` the wrapper and
//! `tx.to` the wrapper, so every bind misfired and the deriver rejected the
//! withdrawal by design. The trace binding here is depth-agnostic.)
//!
//! NOTE on the L2 execution + ether model (verified against EEZL2.sol @5c51e02):
//! the outbound user tx SUCCEEDS in plain re-execution. `executeCrossChainCall`
//! burns `msg.value` to `SYSTEM_ADDRESS` (`EEZL2.sol:192-194`) and then consumes
//! the loaded LEAN settlement entry (`callCount == 0`, empty `incomingCalls` →
//! `_processNCalls(0)` is a no-op, so `_rollingHash` stays 0 and matches
//! `entry.rollingHash`). There is NO L1 delivery on L2 — the L2 leg is the burn
//! + a no-op settlement record; the real delivery is the L1 immediate entry. So
//! the L2 ether debit IS the `SYSTEM_ADDRESS` burn (NOT a `StateDelta` — EEZL2
//! has no state deltas / ether accounting); the L1 debit is the settlement
//! entry's `etherDelta` (`-value`); conservation is cross-chain.

use std::collections::HashMap;

use crate::RollupId;
use alloy_primitives::{B256, U256};

use crate::abi::ExecutionEntrySol;
use crate::action::cross_chain_call_hash;

/// `RollupId(0)` — MAINNET. An L2->L1 outbound's L1 target lives on mainnet, so
/// the `targetRollupId` field of its call hash is 0 (the L2 proxy's
/// `originalRollupId`) — the value `EEZL2.executeCrossChainCall` recomputes.
const MAINNET_ROLLUP_ID: RollupId = RollupId(0);

/// Verify every OUTBOUND settlement entry was actually originated on L2, by
/// matching its recomputed cross-chain call hash against the
/// `CrossChainCallExecuted` events the re-executed Sync block emitted.
///
/// `outbound_entries` MUST be the outbound immediates only (`proxyEntryHash ==
/// 0`, non-empty `l2ToL1Calls`) that L1 settled, in DA order.
/// `observed_call_hashes` is the multiset of `crossChainCallHash` (topic1) from
/// every `CrossChainCallExecuted` log the Sync block emitted (outbound
/// consumptions; inbound delivery emits a different event, so it is naturally
/// excluded). `l2_rollup_id` is this L2's own id, which each entry's
/// `sourceRollupId` must equal.
///
/// # Errors
/// The first outbound entry that is malformed (non-zero `proxyEntryHash`, empty
/// `l2ToL1Calls`, or `sourceRollupId != l2_rollup_id`) or whose recomputed call
/// hash has no matching emitted event — a phantom / tampered / redirected /
/// non-mainnet-target withdrawal.
pub fn verify_outbound_authorized(
    outbound_entries: &[ExecutionEntrySol],
    observed_call_hashes: &[B256],
    l2_rollup_id: u64,
) -> Result<(), String> {
    // Multiset of hashes actually consumed on L2 this Sync block. Each entry
    // claims one — two identical calls need two events.
    let mut available: HashMap<B256, usize> = HashMap::new();
    for h in observed_call_hashes {
        *available.entry(*h).or_insert(0) += 1;
    }

    for (i, entry) in outbound_entries.iter().enumerate() {
        // Defensive: callers pass outbound immediates only. A non-outbound entry
        // here is a pairing bug, not a phantom — surface it loudly.
        if entry.proxyEntryHash != B256::ZERO {
            return Err(format!(
                "outbound gate misused: entry {i} has non-zero proxyEntryHash (not an outbound immediate)"
            ));
        }
        let Some(call) = entry.l2ToL1Calls.first() else {
            return Err(format!(
                "outbound gate misused: entry {i} has empty l2ToL1Calls (not an outbound immediate)"
            ));
        };
        // N>=2 multi-call outbound is parked + rejected upstream
        // (system_tx::reject_multicall); one call per immediate here.

        // source rollup id == this L2 — the L1 delivery builds the source proxy
        // from (sourceAddress, sourceRollupId) (EEZ.sol:958), so a wrong id
        // would settle against a different source identity.
        if call.sourceRollupId != U256::from(l2_rollup_id) {
            return Err(format!(
                "outbound entry {i}: sourceRollupId {} != this L2 {l2_rollup_id}",
                call.sourceRollupId,
            ));
        }

        // Recompute the hash EEZL2 computes for this call and require a matching
        // emitted event. `targetRollupId = MAINNET(0)` binds the target to L1;
        // `sourceRollupId = this L2` is what EEZL2 forces into the hash. A match
        // binds source (the immediate caller, so wrapper-friendly) / target /
        // value / data / mainnet-target all at once, at any call depth — the
        // same hash `EntryHashMismatch` enforces during replay.
        let expected = cross_chain_call_hash(
            MAINNET_ROLLUP_ID,
            call.targetAddress,
            call.value,
            &call.data,
            call.sourceAddress,
            RollupId(l2_rollup_id),
        );
        match available.get_mut(&expected) {
            Some(n) if *n > 0 => *n -= 1,
            _ => {
                return Err(format!(
                    "outbound entry {i} (target {:#x}, value {}, source {:#x}) has NO matching \
                     CrossChainCallExecuted event in the re-executed Sync block (hash {expected:#x}) \
                     — phantom / tampered / redirected / non-mainnet-target withdrawal",
                    call.targetAddress, call.value, call.sourceAddress,
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::L2ToL1CallSol;
    use alloy_primitives::{Address, Bytes, address};

    fn entry(calls: Vec<L2ToL1CallSol>) -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: B256::ZERO, // outbound immediate
            destinationRollupId: U256::from(1),
            callCount: U256::from(calls.len() as u64),
            l2ToL1Calls: calls,
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        }
    }

    /// The observed `CrossChainCallExecuted` topic1 EEZL2 would emit for `call`
    /// on this L2 — `sourceRollupId` forced to `l2_rollup_id`, `targetRollupId`
    /// = MAINNET(0). The gate recomputes the identical hash.
    fn observed(call: &L2ToL1CallSol, l2_rollup_id: u64) -> B256 {
        cross_chain_call_hash(
            RollupId(0),
            call.targetAddress,
            call.value,
            &call.data,
            call.sourceAddress,
            RollupId(l2_rollup_id),
        )
    }

    fn call(source: Address, target: Address, value: u64, data: &[u8]) -> L2ToL1CallSol {
        L2ToL1CallSol {
            targetAddress: target,
            value: U256::from(value),
            data: Bytes::from(data.to_vec()),
            sourceAddress: source,
            sourceRollupId: U256::from(1u64),
            revertSpan: U256::ZERO,
        }
    }

    #[test]
    fn gate_accepts_authorized_and_rejects_phantom() {
        let source = address!("00000000000000000000000000000000000000aa");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let c = call(source, target, 7, &[0x12, 0x34]);
        let entries = vec![entry(vec![c.clone()])];

        // Authorized — a matching event was observed.
        assert!(verify_outbound_authorized(&entries, &[observed(&c, 1)], 1).is_ok());

        // Phantom — the settlement entry has no matching observed event.
        assert!(verify_outbound_authorized(&entries, &[], 1).is_err());
    }

    /// THE outbound-via-wrapper property: the `sourceAddress` is a CONTRACT
    /// (a wrapper that internally called the proxy), not an EOA. The gate accepts
    /// it as long as a matching `CrossChainCallExecuted` event was emitted —
    /// there is no EOA / `tx.to == proxy` assumption. The prior gate rejected
    /// this by design.
    #[test]
    fn gate_accepts_contract_source_wrapper() {
        let wrapper = address!("cccccccccccccccccccccccccccccccccccccccc");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let c = call(wrapper, target, 42, &[0xab]);
        let entries = vec![entry(vec![c.clone()])];
        assert!(
            verify_outbound_authorized(&entries, &[observed(&c, 1)], 1).is_ok(),
            "a contract-initiated (wrapper) outbound must be accepted"
        );
    }

    #[test]
    fn gate_rejects_tampered_value_data_or_target() {
        let source = address!("00000000000000000000000000000000000000aa");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let c = call(source, target, 7, &[0x12, 0x34]);
        let good = observed(&c, 1); // the real event

        // Tampered value: the settlement entry claims value 999 → different hash.
        let mut tv = c.clone();
        tv.value = U256::from(999u64);
        assert!(verify_outbound_authorized(&[entry(vec![tv])], &[good], 1).is_err());

        // Tampered data.
        let mut td = c.clone();
        td.data = Bytes::from(vec![0xff]);
        assert!(verify_outbound_authorized(&[entry(vec![td])], &[good], 1).is_err());

        // Redirected target.
        let mut tt = c.clone();
        tt.targetAddress = address!("00000000000000000000000000000000000000bb");
        assert!(verify_outbound_authorized(&[entry(vec![tt])], &[good], 1).is_err());
    }

    /// The MAINNET-target bind survives: the gate recomputes with
    /// `targetRollupId = MAINNET(0)`, so an event produced by a NON-mainnet-target
    /// proxy (an L2->L2 outbound, `originalRollupId != 0`) hashes differently and
    /// is rejected even though target/value/data/source match.
    #[test]
    fn gate_enforces_mainnet_target_rollup() {
        let source = address!("00000000000000000000000000000000000000aa");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let c = call(source, target, 7, &[0x12, 0x34]);

        // An event whose hash used targetRollupId = 5 (a non-mainnet target).
        let non_mainnet_event = cross_chain_call_hash(
            RollupId(5),
            c.targetAddress,
            c.value,
            &c.data,
            c.sourceAddress,
            RollupId(1),
        );
        assert_ne!(non_mainnet_event, observed(&c, 1));
        assert!(
            verify_outbound_authorized(&[entry(vec![c])], &[non_mainnet_event], 1).is_err(),
            "a withdrawal whose only event targets a non-mainnet proxy must be rejected"
        );
    }

    #[test]
    fn gate_rejects_wrong_source_rollup_id() {
        let source = address!("00000000000000000000000000000000000000aa");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let c = call(source, target, 7, &[0x12, 0x34]);
        // Gate run for L2 rollup 2, but the entry's call claims sourceRollupId 1.
        assert!(
            verify_outbound_authorized(&[entry(vec![c.clone()])], &[observed(&c, 2)], 2).is_err()
        );
    }

    /// Multiset semantics: two IDENTICAL outbound entries need two matching
    /// events. One event authorizes only the first; the second is a phantom.
    #[test]
    fn gate_requires_one_event_per_entry() {
        let source = address!("00000000000000000000000000000000000000aa");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let c = call(source, target, 7, &[0x12, 0x34]);
        let two = vec![entry(vec![c.clone()]), entry(vec![c.clone()])];

        // Two events → both authorized.
        assert!(verify_outbound_authorized(&two, &[observed(&c, 1), observed(&c, 1)], 1).is_ok());
        // One event → the second entry is unmatched.
        assert!(verify_outbound_authorized(&two, &[observed(&c, 1)], 1).is_err());
    }

    #[test]
    fn gate_rejects_malformed_entries() {
        let source = address!("00000000000000000000000000000000000000aa");
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let c = call(source, target, 7, &[0x12, 0x34]);

        // Non-zero proxyEntryHash = not an outbound immediate.
        let mut bad = entry(vec![c.clone()]);
        bad.proxyEntryHash = B256::repeat_byte(0x11);
        assert!(verify_outbound_authorized(&[bad], &[observed(&c, 1)], 1).is_err());

        // Empty l2ToL1Calls = not an outbound immediate.
        assert!(verify_outbound_authorized(&[entry(vec![])], &[], 1).is_err());
    }
}
