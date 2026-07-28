//! Outbound L2->L1 authorization gate: anti-phantom-payout check on withdrawals.

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
