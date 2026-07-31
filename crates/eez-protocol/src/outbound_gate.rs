//! Authorization for mutable L2 calls represented by outbound settlement entries.
//!
//! For each claimed call, the gate recomputes the gas-aware key used by
//! `EEZL2.executeCrossChainCall` and consumes one matching event observation
//! from the re-executed Sync block. The hash binds the immediate L2 caller,
//! source and target rollups, target, value, calldata, and manager-entry gas.
//! Treating observations as a multiset preserves duplicate calls while
//! preventing one event from authorizing more than one entry.

use std::collections::HashMap;

use crate::RollupId;
use alloy_primitives::B256;

use crate::abi::ExecutionEntrySol;
use crate::action::{CallHashInput, CallMode, l2_outbound_call_hash};

/// `RollupId(0)` — MAINNET. An L2->L1 outbound's L1 target lives on mainnet, so
/// the `targetRollupId` field of its call hash is 0 (the L2 proxy's
/// `originalRollupId`) — the value `EEZL2.executeCrossChainCall` recomputes.
const MAINNET_ROLLUP_ID: RollupId = RollupId(0);

/// The supported EEZL2 deployment disables `USE_GAS_LEFT`.
const SUPPORTED_CALL_GAS: u64 = 0;

/// Canonically decoded evidence from an `EEZL2.CrossChainCallExecuted` log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboundCallObservation {
    call_hash: B256,
    call_gas: u64,
}

impl OutboundCallObservation {
    /// Construct an observation from a canonically decoded `EEZL2` event.
    #[must_use]
    pub const fn new(call_hash: B256, call_gas: u64) -> Self {
        Self {
            call_hash,
            call_gas,
        }
    }

    /// Hash emitted by `EEZL2` for the executed call.
    #[must_use]
    pub const fn call_hash(&self) -> B256 {
        self.call_hash
    }

    /// Manager-entry gas value folded into the emitted hash.
    ///
    /// This is not the destination-call forwarding gas limit.
    #[must_use]
    pub const fn call_gas(&self) -> u64 {
        self.call_gas
    }
}

/// Verify every OUTBOUND settlement entry was actually originated on L2, by
/// matching its recomputed cross-chain call hash against the
/// `CrossChainCallExecuted` events the re-executed Sync block emitted.
///
/// `outbound_entries` MUST be the outbound immediates only (`proxyEntryHash ==
/// 0`, non-empty `l2ToL1Calls`) that L1 settled, in DA order.
/// `observed_calls` contains the canonically decoded
/// `CrossChainCallExecuted` logs from the Sync block. `l2_rollup_id` is this
/// L2's own id, which each entry's `sourceRollupId` must equal.
///
/// # Errors
/// The first outbound entry that is malformed (non-zero `proxyEntryHash`, empty
/// `l2ToL1Calls`, or `sourceRollupId != l2_rollup_id`) or whose recomputed call
/// hash has no matching emitted event — a phantom / tampered / redirected /
/// non-mainnet-target withdrawal.
pub fn verify_outbound_authorized(
    outbound_entries: &[ExecutionEntrySol],
    observed_calls: &[OutboundCallObservation],
    l2_rollup_id: u64,
) -> Result<(), String> {
    // Multiset of hashes actually consumed on L2 this Sync block. Each entry
    // claims one — two identical calls need two events.
    let mut available: HashMap<B256, usize> = HashMap::new();
    for observation in observed_calls {
        if observation.call_gas() != SUPPORTED_CALL_GAS {
            return Err(format!(
                "outbound event uses callGas {}; this deployment requires callGas 0",
                observation.call_gas(),
            ));
        }
        *available.entry(observation.call_hash()).or_insert(0) += 1;
    }

    for (i, entry) in outbound_entries.iter().enumerate() {
        // Defensive: callers pass outbound immediates only. A non-outbound entry
        // here is a pairing bug, not a phantom — surface it loudly.
        if entry.proxyEntryHash != B256::ZERO {
            return Err(format!(
                "outbound gate misused: entry {i} has non-zero proxyEntryHash (not an outbound immediate)"
            ));
        }
        let [call] = entry.l2ToL1Calls.as_slice() else {
            return Err(format!(
                "outbound entry {i} must contain exactly one L2-to-L1 call"
            ));
        };
        if !entry.success || !entry.expectedL1ToL2Calls.is_empty() {
            return Err(format!(
                "outbound entry {i} uses an unsuccessful or nested execution shape"
            ));
        }
        if call.isStatic || call.revertNextNCalls != 0 || call.gas != 0 {
            return Err(format!(
                "outbound entry {i} uses unsupported static, revert-span, or explicit-gas semantics"
            ));
        }

        // source rollup id == this L2 — the L1 delivery builds the source proxy
        // from (sourceAddress, sourceRollupId) (EEZ.sol:958), so a wrong id
        // would settle against a different source identity.
        if call.sourceRollupId != l2_rollup_id {
            return Err(format!(
                "outbound entry {i}: sourceRollupId {} != this L2 {l2_rollup_id}",
                call.sourceRollupId,
            ));
        }

        // Recompute the key EEZL2 uses to find the loaded entry. MAINNET binds
        // the destination to L1; the configured rollup binds the L2 source.
        let expected = l2_outbound_call_hash(
            CallHashInput {
                call_mode: CallMode::from_is_static(call.isStatic),
                source_address: call.sourceAddress,
                source_rollup_id: RollupId(l2_rollup_id),
                target_address: call.targetAddress,
                target_rollup_id: MAINNET_ROLLUP_ID,
                value: call.value,
                data: &call.data,
            },
            SUPPORTED_CALL_GAS,
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
    use alloy_primitives::{Address, Bytes, U256, address};

    fn entry(calls: Vec<L2ToL1CallSol>) -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateUpdates: Vec::new(),
            proxyEntryHash: B256::ZERO, // outbound immediate
            l2ToL1Calls: calls,
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: B256::ZERO,
            destinationRollupId: 1,
            success: true,
            returnData: Bytes::new(),
        }
    }

    /// The observed `CrossChainCallExecuted` topic1 EEZL2 would emit for `call`
    /// on this L2 — `sourceRollupId` forced to `l2_rollup_id`, `targetRollupId`
    /// = MAINNET(0). The gate recomputes the identical hash.
    fn observed(call: &L2ToL1CallSol, l2_rollup_id: u64) -> OutboundCallObservation {
        OutboundCallObservation::new(
            l2_outbound_call_hash(
                CallHashInput {
                    call_mode: CallMode::Mutable,
                    source_address: call.sourceAddress,
                    source_rollup_id: RollupId(l2_rollup_id),
                    target_address: call.targetAddress,
                    target_rollup_id: RollupId::MAINNET,
                    value: call.value,
                    data: &call.data,
                },
                SUPPORTED_CALL_GAS,
            ),
            SUPPORTED_CALL_GAS,
        )
    }

    fn call(source: Address, target: Address, value: u64, data: &[u8]) -> L2ToL1CallSol {
        L2ToL1CallSol {
            revertNextNCalls: 0,
            isStatic: false,
            gas: 0,
            sourceAddress: source,
            sourceRollupId: 1,
            targetAddress: target,
            value: U256::from(value),
            data: Bytes::from(data.to_vec()),
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
        let non_mainnet_event = OutboundCallObservation::new(
            l2_outbound_call_hash(
                CallHashInput {
                    call_mode: CallMode::Mutable,
                    source_address: c.sourceAddress,
                    source_rollup_id: RollupId(1),
                    target_address: c.targetAddress,
                    target_rollup_id: RollupId(5),
                    value: c.value,
                    data: &c.data,
                },
                SUPPORTED_CALL_GAS,
            ),
            SUPPORTED_CALL_GAS,
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

    #[test]
    fn gate_rejects_nonzero_call_gas() {
        let c = call(
            address!("00000000000000000000000000000000000000aa"),
            address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9"),
            7,
            &[0x12, 0x34],
        );
        let call_gas = 1;
        let call_hash = l2_outbound_call_hash(
            CallHashInput {
                call_mode: CallMode::Mutable,
                source_address: c.sourceAddress,
                source_rollup_id: RollupId(1),
                target_address: c.targetAddress,
                target_rollup_id: RollupId::MAINNET,
                value: c.value,
                data: &c.data,
            },
            call_gas,
        );
        let observation = OutboundCallObservation::new(call_hash, call_gas);

        assert!(verify_outbound_authorized(&[entry(vec![c])], &[observation], 1).is_err());
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

        let mut failed = entry(vec![c.clone()]);
        failed.success = false;
        assert!(verify_outbound_authorized(&[failed], &[observed(&c, 1)], 1).is_err());

        for unsupported in [
            L2ToL1CallSol {
                isStatic: true,
                ..c.clone()
            },
            L2ToL1CallSol {
                revertNextNCalls: 1,
                ..c.clone()
            },
            L2ToL1CallSol { gas: 1, ..c },
        ] {
            assert!(verify_outbound_authorized(&[entry(vec![unsupported])], &[], 1).is_err());
        }
    }
}
