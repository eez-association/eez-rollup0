//! L1 post budget arithmetic: postBatch gas pins and projections, blob bytes, block-gas fit.

use alloy_primitives::Bytes;

/// Stand-in for each proof when sizing before the attesters run. Sized for an
/// ECDSA proof (65 B), the only kind the prover client accepts.
pub(super) const MAX_PROOF_BYTES: usize = 128;

/// Ceiling on a postBatch's gas limit (`EEZ_MAX_POSTBATCH_GAS` overrides): the
/// EIP-7825 per-tx cap, above which no tx is valid at any block gas limit.
pub(super) const DEFAULT_MAX_POSTBATCH_GAS: u64 = 16_777_216;

/// The belt prices bytes the drain cannot see (ABI framing, proof, DA shape), so
/// the drain must stop earlier or the same set requeues forever. Gap seen: 230k.
const POSTBATCH_DRAIN_MARGIN: u64 = 600_000;

/// Execution for a batch with just the leading immediate entry, calldata aside.
/// Measured 139k, 10% slack; pinned by `contracts/test/PostBatchGasPins.t.sol`.
const POSTBATCH_BASE_GAS_PIN: u64 = 160_000;

/// One more entry costs EEZ.sol hashing, a state SSTORE, and a CREATE2 proxy
/// deploy for a new sender. Same pin test; measured 334k worst case, 10% slack.
const POSTBATCH_ENTRY_GAS_PIN: u64 = 370_000;

/// Each `ECDSAProofSystem` past the first costs a manager vkey read and its
/// verify call. Same pin test; measured 12,264 worst case (sixteen), 10% slack.
const POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN: u64 = 14_000;

/// Execution the extra ECDSA proofs of a batch carrying `proof_systems` add,
/// calldata aside.
pub(super) const fn attestation_gas(proof_systems: u64) -> u64 {
    POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN.saturating_mul(proof_systems.saturating_sub(1))
}

/// Below this the drain of a batch carrying `proof_systems` proofs admits
/// nothing and evicts the first held tx as poison.
pub(super) const fn min_viable_postbatch_gas(proof_systems: u64) -> u64 {
    projected_postbatch_gas(2, 0, 0)
        .saturating_add(attestation_gas(proof_systems))
        .saturating_add(POSTBATCH_DRAIN_MARGIN)
}

/// EIP-7623 calldata floor — below it the tx is invalid and dies at simulation.
pub(super) fn calldata_floor_gas(calldata: &[u8]) -> u64 {
    let nonzero = calldata.iter().filter(|byte| **byte != 0).count() as u64;
    let zero = calldata.len() as u64 - nonzero;
    21_000 + 10 * (zero + 4 * nonzero)
}

/// Exact standard-rate calldata gas: 4 per zero byte, 16 per non-zero byte.
pub(super) fn calldata_gas(bytes: &[u8]) -> u64 {
    let nonzero = bytes.iter().filter(|byte| **byte != 0).count() as u64;
    let zero = bytes.len() as u64 - nonzero;
    4 * zero + 16 * nonzero
}

/// Whole-postBatch gas on EIP-7623's standard branch: tx base, calldata, pins,
/// probed target calls. The floor branch is checked in `sign_post_batch_tx`.
pub(super) const fn projected_postbatch_gas(
    entry_count: u64,
    calldata_gas: u64,
    target_gas: u64,
) -> u64 {
    21_000_u64
        .saturating_add(POSTBATCH_BASE_GAS_PIN)
        .saturating_add(POSTBATCH_ENTRY_GAS_PIN.saturating_mul(entry_count))
        .saturating_add(calldata_gas)
        .saturating_add(target_gas)
}

/// What one accepted held tx adds to the postBatch projection, per term.
#[derive(Debug, Clone, Copy)]
pub(super) struct TxL1Gas {
    /// `batch.entries` rows it contributes, each costing [`POSTBATCH_ENTRY_GAS_PIN`].
    pub(super) entries: u64,
    /// Standard-rate gas for every byte it puts in the postBatch calldata.
    pub(super) calldata_gas: u64,
    /// Probed cost of the target call EEZ.sol runs inline (outbound only).
    pub(super) target_gas: u64,
}

/// L1 gas one accepted tx adds. `da_entries`/`da_tx` are its DA copies; only
/// outbound pays `target_gas` inline.
fn projected_tx_l1_gas(
    batch_entries: &[eez_protocol::abi::ExecutionEntrySol],
    da_entries: &[eez_protocol::abi::ExecutionEntrySol],
    da_tx: &[u8],
    target_gas: u64,
) -> TxL1Gas {
    use alloy_sol_types::SolValue as _;
    let encoded = |entries: &[eez_protocol::abi::ExecutionEntrySol]| -> u64 {
        entries
            .iter()
            .map(|entry| calldata_gas(&entry.abi_encode()))
            .sum()
    };
    TxL1Gas {
        entries: batch_entries.len() as u64,
        calldata_gas: encoded(batch_entries)
            .saturating_add(encoded(da_entries))
            .saturating_add(calldata_gas(da_tx)),
        target_gas,
    }
}

/// Running gas and blob-byte projections, so the drain stops before building
/// a batch that exceeds the L1 transaction gas cap or blob capacity.
#[derive(Debug, Clone, Copy)]
pub(super) struct PostBatchGasBudget {
    pub(super) cap: u64,
    /// Running total, seeded with the leading immediate entry every postBatch
    /// carries. The projection is linear, so costs just add.
    pub(super) projected: u64,
    /// `None` when the batch carries its DA in calldata.
    pub(super) blob: Option<BlobBytes>,
    /// Attestation gas for the configured attesters, priced into every
    /// postBatch projection.
    pub(super) attestation: u64,
}

/// Blob stream bytes: the post's capacity, the bytes reserved so far, and
/// the bytes of an empty interval, the least any later slot reserves.
#[derive(Debug, Clone, Copy)]
pub(super) struct BlobBytes {
    pub(super) limit: usize,
    pub(super) used: usize,
    pub(super) floor: usize,
}

impl PostBatchGasBudget {
    /// A budget for a batch that may carry a proof from each of
    /// `proof_systems` attesters.
    pub(super) fn new(max_gas: u64, proof_systems: u64) -> Self {
        let attestation = attestation_gas(proof_systems);
        Self {
            cap: max_gas.saturating_sub(POSTBATCH_DRAIN_MARGIN),
            projected: projected_postbatch_gas(1, 0, 0).saturating_add(attestation),
            blob: None,
            attestation,
        }
    }

    /// The cost of `cost` as a postBatch of its own, for the cut event.
    pub(super) fn standalone_cost(&self, cost: TxL1Gas) -> u64 {
        projected_postbatch_gas(cost.entries, cost.calldata_gas, cost.target_gas)
            .saturating_add(self.attestation)
    }

    /// L1 gas one accepted tx adds. Its DA copies cost calldata gas only when
    /// the batch carries its DA in calldata, not in blobs.
    pub(super) fn tx_cost(
        &self,
        batch_entries: &[eez_protocol::abi::ExecutionEntrySol],
        da_entries: &[eez_protocol::abi::ExecutionEntrySol],
        da_tx: &[u8],
        target_gas: u64,
    ) -> TxL1Gas {
        if self.blob.is_some() {
            projected_tx_l1_gas(batch_entries, &[], &[], target_gas)
        } else {
            projected_tx_l1_gas(batch_entries, da_entries, da_tx, target_gas)
        }
    }

    /// Charge the blob bytes one held tx adds; only `Accept` charges them.
    pub(super) fn try_accept_da(
        &mut self,
        rollup_id: u64,
        entries: &[eez_protocol::abi::ExecutionEntrySol],
        user: Option<&Bytes>,
    ) -> Result<BlockGasFit, String> {
        let Some(blob) = &mut self.blob else {
            return Ok(BlockGasFit::Accept);
        };
        // A full range cannot admit another held tx; defer it to a later slot.
        if blob.used >= blob.limit {
            return Ok(BlockGasFit::Defer);
        }
        let actions = entries
            .iter()
            .map(|entry| {
                eez_protocol::entries::manifest::action_from_entry(
                    entry,
                    eez_protocol::RollupId(rollup_id),
                )
                .map_err(|e| format!("DA action for drain: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Outbound users travel in blob DA; inbound users are already on L1.
        let block = eez_payload_codec::SpanBlock {
            extra_data: eez_driver::BUILDER_EXTRA_DATA.to_vec(),
            transactions: user.into_iter().map(|user| user.to_vec()).collect(),
            ..Default::default()
        };
        // One envelope per candidate conservatively covers aggregate prefix growth.
        let contribution = blob_stream_len(rollup_id, &[block], &actions)?;
        let next = blob.used.saturating_add(contribution);
        Ok(if next <= blob.limit {
            blob.used = next;
            BlockGasFit::Accept
        } else if blob.floor.saturating_add(contribution) > blob.limit {
            // Not even a range holding one empty interval could take it.
            BlockGasFit::Unfittable
        } else {
            BlockGasFit::Defer
        })
    }

    /// Charge `cost` if it fits. `false` leaves the projection untouched, so the
    /// caller can defer this tx and stop draining.
    pub(super) fn try_accept(&mut self, cost: TxL1Gas) -> bool {
        let next = self
            .projected
            .saturating_add(POSTBATCH_ENTRY_GAS_PIN.saturating_mul(cost.entries))
            .saturating_add(cost.calldata_gas)
            .saturating_add(cost.target_gas);
        if next > self.cap {
            return false;
        }
        self.projected = next;
        true
    }
}

/// What the drain can do with a pair wanting `declared` gas on a Sync block
/// that already holds `gas_used`, or with a tx against the batch's blob bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BlockGasFit {
    /// Room in this block — accept.
    Accept,
    /// No room here, but an emptier block would take it: hold for a later slot.
    Defer,
    /// Over the whole block limit, so no slot can ever take it.
    Unfittable,
}

/// Whether a pair fits the L2 Sync block. Sums DECLARED limits, which is what
/// the builder refuses on; over-estimating only defers early, never overflows.
pub(super) const fn block_gas_fit(
    gas_used: u64,
    declared: u64,
    block_gas_limit: u64,
) -> BlockGasFit {
    if gas_used.saturating_add(declared) <= block_gas_limit {
        BlockGasFit::Accept
    } else if declared > block_gas_limit {
        BlockGasFit::Unfittable
    } else {
        BlockGasFit::Defer
    }
}

/// Declared gas limit of a raw tx. An undecodable tx counts as the whole block
/// so it is refused, never read as free space (`invariant 7`).
pub(super) fn declared_gas_limit(raw: &Bytes) -> u64 {
    use alloy_consensus::Transaction as _;
    use alloy_eips::eip2718::Decodable2718 as _;
    reth_ethereum_primitives::TransactionSigned::decode_2718(&mut raw.as_ref())
        .map_or(u64::MAX, |tx| tx.gas_limit())
}

/// The declared limit and active cap when an envelope can never fit.
pub(super) const fn gas_limit_violation(declared: u64, cap: Option<u64>) -> Option<(u64, u64)> {
    match cap {
        Some(cap) if declared > cap => Some((declared, cap)),
        _ => None,
    }
}

/// Bytes `blocks` and `actions` take in a blob stream, its closing
/// CloseBlobStream byte included.
pub(super) fn blob_stream_len(
    rollup_id: u64,
    blocks: &[eez_payload_codec::SpanBlock],
    actions: &[eez_payload_codec::Action],
) -> Result<usize, String> {
    eez_payload_codec::encode_container(rollup_id, blocks, actions)
        .map(|stream| stream.len().saturating_add(1))
        .map_err(|e| format!("encode blob DA: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::tests::signed_user_tx;
    use alloy_primitives::{Address, U256};
    use eez_driver::BUILDER_GAS_LIMIT;

    #[test]
    fn eip7623_floor_matches_hand_computed_vector() {
        // 21000 + 10 * (zeros + 4*nonzeros).
        assert_eq!(calldata_floor_gas(&[]), 21_000);
        // 3 zeros (3 tokens) + 2 non-zeros (8 tokens) = 11 tokens = 110 gas.
        assert_eq!(calldata_floor_gas(&[0, 0xab, 0, 0xcd, 0]), 21_110);
        // A realistic batch's floor alone outgrows a multi-million fixed limit.
        assert!(calldata_floor_gas(&vec![0xff; 200 * 1024]) > 4_000_000);
    }

    /// K = 6 with a terminal metadata change to exercise both run encodings.
    fn drain_budget_range(with_traffic: bool) -> Vec<eez_payload_codec::SpanBlock> {
        let mut blocks = vec![
            eez_payload_codec::SpanBlock {
                beneficiary: [0x11; 20],
                extra_data: vec![0x22; 4],
                transactions: if with_traffic {
                    vec![vec![0x42; 200]]
                } else {
                    Vec::new()
                },
            };
            5
        ];
        blocks.push(eez_payload_codec::SpanBlock {
            beneficiary: [0x33; 20],
            extra_data: eez_driver::BUILDER_EXTRA_DATA.to_vec(),
            transactions: Vec::new(),
        });
        blocks
    }

    const ONE_EMPTY_BLOB: BlobBytes = BlobBytes {
        limit: eez_payload_codec::LOGICAL_BYTES_PER_BLOB,
        used: 0,
        floor: 0,
    };

    fn blob_budget(blob: BlobBytes) -> PostBatchGasBudget {
        PostBatchGasBudget {
            blob: Some(blob),
            ..PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1)
        }
    }

    #[test]
    fn blob_budget_defers_behind_interval_traffic_and_evicts_only_the_oversized() {
        let floor = blob_stream_len(1, &drain_budget_range(false), &[]).unwrap();
        let base = blob_stream_len(1, &drain_budget_range(true), &[]).unwrap();
        let limit = 4096;
        let fit = |used, size| {
            blob_budget(BlobBytes { limit, used, floor })
                .try_accept_da(1, &[], Some(&Bytes::from(vec![0x42; size])))
                .unwrap()
        };
        assert_eq!(fit(floor, 3500), BlockGasFit::Accept);
        assert_eq!(fit(base, 3500), BlockGasFit::Defer);
        assert_eq!(fit(floor, limit), BlockGasFit::Unfittable);
    }

    #[test]
    fn blob_budget_matches_single_transaction_capacity() {
        let capacity = eez_payload_codec::LOGICAL_BYTES_PER_BLOB;
        for length in [capacity - 100, capacity - 40, capacity, capacity + 1] {
            let user = Bytes::from(vec![0x42; length]);
            let blocks = [eez_payload_codec::SpanBlock {
                transactions: vec![user.to_vec()],
                extra_data: eez_driver::BUILDER_EXTRA_DATA.to_vec(),
                ..Default::default()
            }];
            let fits = eez_payload_codec::encode_blob_container(1, &blocks, &[])
                .unwrap()
                .len()
                <= 1;
            let mut budget = blob_budget(ONE_EMPTY_BLOB);
            assert_eq!(
                budget.try_accept_da(1, &[], Some(&user)).unwrap(),
                if fits {
                    BlockGasFit::Accept
                } else {
                    BlockGasFit::Unfittable
                },
                "{length} bytes"
            );
        }
    }

    #[test]
    fn blob_budget_projection_covers_the_real_stream() {
        for outbound in [false, true] {
            let action = eez_payload_codec::Action {
                source_rollup_id: u64::from(outbound),
                target_rollup_id: u64::from(!outbound),
                success: true,
                data: vec![0x42; 100],
                return_data: vec![0x24; 100],
                ..Default::default()
            };
            let entry = eez_protocol::entries::manifest::entry_from_action(
                &action,
                eez_protocol::RollupId(1),
            )
            .unwrap();
            let user = Bytes::from(vec![0x42; 100]);
            // Seeded with committed traffic, so an unseeded base under-counts.
            let mut blocks = drain_budget_range(true);
            let mut budget = blob_budget(BlobBytes {
                used: blob_stream_len(1, &blocks, &[]).unwrap(),
                ..ONE_EMPTY_BLOB
            });
            let mut actions = Vec::new();
            while budget
                .try_accept_da(1, std::slice::from_ref(&entry), outbound.then_some(&user))
                .unwrap()
                == BlockGasFit::Accept
            {
                actions.push(
                    eez_protocol::entries::manifest::action_from_entry(
                        &entry,
                        eez_protocol::RollupId(1),
                    )
                    .unwrap(),
                );
                if outbound {
                    blocks.last_mut().unwrap().transactions.push(user.to_vec());
                }
                let bytes = blob_stream_len(1, &blocks, &actions).unwrap();
                assert!(bytes <= budget.blob.unwrap().used);
            }
            assert!(
                actions.len() > 128,
                "exercise multibyte count and operations-length prefixes"
            );
            assert_eq!(
                eez_payload_codec::encode_blob_container(1, &blocks, &actions)
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    /// The forge pin test re-declares the constants as literals, so a change
    /// here would leave it measuring the old value in silence. Bind them.
    #[test]
    fn solidity_pin_test_mirrors_the_rust_gas_pins() {
        let sol = include_str!("../../../../contracts/test/PostBatchGasPins.t.sol");
        for (name, rust) in [
            ("POSTBATCH_BASE_GAS_PIN", POSTBATCH_BASE_GAS_PIN),
            ("POSTBATCH_ENTRY_GAS_PIN", POSTBATCH_ENTRY_GAS_PIN),
            (
                "POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN",
                POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN,
            ),
        ] {
            let decl = format!("uint256 private constant {name} = ");
            let rest = sol
                .split(&decl)
                .nth(1)
                .unwrap_or_else(|| panic!("{name} is not declared in PostBatchGasPins.t.sol"));
            let literal: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '_')
                .collect();
            let sol_value: u64 = literal
                .replace('_', "")
                .parse()
                .unwrap_or_else(|e| panic!("{name} literal {literal:?}: {e}"));
            assert_eq!(
                sol_value, rust,
                "{name} drifted: Solidity pins {sol_value}, Rust projects {rust}",
            );
        }
    }

    /// One attester adds no attestation gas to the floor.
    #[test]
    fn a_single_attester_keeps_the_single_proof_floor() {
        assert_eq!(
            min_viable_postbatch_gas(1),
            projected_postbatch_gas(2, 0, 0) + POSTBATCH_DRAIN_MARGIN
        );
        assert!(min_viable_postbatch_gas(7) > min_viable_postbatch_gas(1));
    }

    /// The pre-prove gate sizes the batch with a `MAX_PROOF_BYTES` stand-in for
    /// the proof it does not have yet. That is only safe if the stand-in is a
    /// worst case: longer than any real proof, and priced at the dearer
    /// non-zero-byte rate. Both halves are pinned here — if a proof system ever
    /// returns something bigger, this fails before the gate turns optimistic and
    /// the signing-side ceiling has to catch it instead.
    #[test]
    fn proof_placeholder_is_a_worst_case() {
        /// r‖s‖v — the attestation `ECDSAProofSystem` verifies.
        const ECDSA_PROOF_BYTES: usize = 65;
        const { assert!(MAX_PROOF_BYTES >= ECDSA_PROOF_BYTES) };
        // Non-zero bytes are 4 EIP-7623 tokens, zeros are 1, so an all-0xff
        // stand-in prices above any real proof of the same length.
        let placeholder = [0xffu8; MAX_PROOF_BYTES];
        assert!(
            calldata_floor_gas(&placeholder) >= calldata_floor_gas(&[0x11u8; ECDSA_PROOF_BYTES])
        );
        assert!(calldata_floor_gas(&placeholder) >= calldata_floor_gas(&[0u8; MAX_PROOF_BYTES]));
        // Monotonic in length, so a shorter real proof can never cost more.
        assert!(calldata_floor_gas(&[0xffu8; 96]) <= calldata_floor_gas(&placeholder));
    }

    /// Gas one outbound target call used on L1, measured on a kurtosis devnet
    /// (the same run measured ~336k marginal per outbound entry in total).
    const MEASURED_TARGET_GAS: u64 = 47_763;

    /// An outbound settlement entry of realistic size: one L1 call with a
    /// 4-byte selector plus two words of arguments.
    fn outbound_entry() -> eez_protocol::abi::ExecutionEntrySol {
        eez_protocol::abi::ExecutionEntrySol {
            l2ToL1Calls: vec![eez_protocol::abi::L2ToL1CallSol {
                revertNextNCalls: 0,
                isStatic: false,
                gas: 0,
                sourceAddress: Address::repeat_byte(0x11),
                sourceRollupId: 1,
                targetAddress: Address::repeat_byte(0x22),
                value: U256::ZERO,
                data: Bytes::from(vec![0x55u8; 68]),
            }],
            destinationRollupId: 1,
            success: true,
            ..Default::default()
        }
    }

    #[test]
    fn projection_charges_the_pins_the_calldata_and_the_probed_target() {
        // The cheapest postBatch: tx base + base pin + one leading entry.
        assert_eq!(
            projected_postbatch_gas(1, 0, 0),
            21_000 + POSTBATCH_BASE_GAS_PIN + POSTBATCH_ENTRY_GAS_PIN
        );
        // Entries are linear in the pin; calldata and probed target gas add raw.
        assert_eq!(
            projected_postbatch_gas(4, 500, 100),
            21_000 + POSTBATCH_BASE_GAS_PIN + 4 * POSTBATCH_ENTRY_GAS_PIN + 600
        );
        // Standard EIP-7623 rates, well under the 10-per-token floor the
        // signing path refuses on.
        assert_eq!(calldata_gas(&[]), 0);
        assert_eq!(calldata_gas(&[0, 0xab, 0, 0xcd, 0]), 3 * 4 + 2 * 16);
        // The bug this replaces: at a 50-tx bundle cap the real cost passes the
        // EIP-7825 per-tx cap, so no gas limit makes the batch postable.
        assert!(
            projected_postbatch_gas(51, 0, 50 * MEASURED_TARGET_GAS) > DEFAULT_MAX_POSTBATCH_GAS,
            "50 outbound entries must be recognised as unpostable",
        );
    }

    /// Every proof past the first is a verify call the batch pays for, so the
    /// drain starts that much further into its budget.
    #[test]
    fn drain_budget_charges_every_attester_past_the_first() {
        let single = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1);
        let seven = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 7);
        assert_eq!(
            seven.projected - single.projected,
            6 * POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN
        );
        assert_eq!(attestation_gas(0), 0);
        assert_eq!(attestation_gas(1), 0);
    }

    #[test]
    fn blob_budget_charges_no_calldata_for_da_copies() {
        let entries = [outbound_entry()];
        let raw_tx = vec![0x77u8; 180];
        let calldata = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1);
        let blob = blob_budget(ONE_EMPTY_BLOB);
        let in_calldata = calldata.tx_cost(&entries, &entries, &raw_tx, MEASURED_TARGET_GAS);
        let in_blobs = blob.tx_cost(&entries, &entries, &raw_tx, MEASURED_TARGET_GAS);
        let batch_only = projected_tx_l1_gas(&entries, &[], &[], MEASURED_TARGET_GAS);
        assert_eq!(in_blobs.calldata_gas, batch_only.calldata_gas);
        assert!(in_calldata.calldata_gas > batch_only.calldata_gas + calldata_gas(&raw_tx));
    }

    #[test]
    fn drain_budget_cuts_the_accepted_prefix_below_the_cap() {
        let entries = [outbound_entry()];
        let raw_tx = vec![0x77u8; 180];
        let cost = projected_tx_l1_gas(&entries, &entries, &raw_tx, MEASURED_TARGET_GAS);
        // Every term is charged: one entry, the inline call, and the calldata for
        // the entry in `batch.entries` plus its DA copy and the raw tx.
        assert_eq!(cost.entries, 1);
        assert_eq!(cost.target_gas, MEASURED_TARGET_GAS);
        let encoded = {
            use alloy_sol_types::SolValue as _;
            calldata_gas(&entries[0].abi_encode())
        };
        assert_eq!(cost.calldata_gas, 2 * encoded + calldata_gas(&raw_tx));

        let mut budget = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1);
        // The drain leaves the belt's margin below the cap, or the belt refuses
        // what the drain accepted and the same set requeues forever.
        assert_eq!(
            budget.cap,
            DEFAULT_MAX_POSTBATCH_GAS - POSTBATCH_DRAIN_MARGIN
        );
        let per_tx = POSTBATCH_ENTRY_GAS_PIN + cost.calldata_gas + cost.target_gas;
        let expected = (budget.cap - budget.projected) / per_tx;
        let mut accepted = 0u64;
        // The drain never offers more than the per-bundle cap.
        while accepted < 50 && budget.try_accept(cost) {
            accepted += 1;
        }
        assert_eq!(accepted, expected);
        // The cut is what makes the batch postable at all.
        assert!(accepted < 50);
        assert!(budget.projected <= DEFAULT_MAX_POSTBATCH_GAS);
        // A rejected tx leaves the projection untouched, so the accepted prefix
        // is exactly what the batch will carry.
        let projected = budget.projected;
        assert!(!budget.try_accept(cost));
        assert_eq!(budget.projected, projected);
        // The drain and the prepare-time belt agree on the same entries: the
        // leading immediate plus one per accepted tx.
        assert_eq!(
            projected,
            projected_postbatch_gas(
                accepted + 1,
                accepted * cost.calldata_gas,
                accepted * MEASURED_TARGET_GAS,
            )
        );
    }

    /// Two outbound pairs near the EIP-7825 cap overflow a 30M block, so the
    /// second must be refused before `build_sync_block` hard-errors on it.
    #[test]
    fn block_gas_fit_refuses_the_second_fat_outbound_pair() {
        // The early gate checks the signed user's declared gas. The actual
        // load is metered before the user is admitted by append_and_execute.
        const USER_DECLARED: u64 = 16_700_000;
        const USER_BURNED: u64 = 13_300_000;
        const LOAD_BURNED: u64 = 100_000;
        let pair = USER_DECLARED;

        // Pair 1 fits an empty block.
        assert_eq!(
            block_gas_fit(0, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Accept
        );
        // Pair 2 sees the burn of pair 1 and does not.
        let after_one = LOAD_BURNED + USER_BURNED;
        assert_eq!(
            block_gas_fit(after_one, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Defer,
            "the second fat pair must be deferred, not built",
        );
        // Ground truth for the deferral: the builder itself would refuse pair 2's
        // user tx, since its declared limit exceeds what the block has left.
        assert!(USER_DECLARED > BUILDER_GAS_LIMIT - after_one - LOAD_BURNED);

        // Exact boundary: equal fits, one over defers.
        assert_eq!(
            block_gas_fit(BUILDER_GAS_LIMIT - pair, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Accept,
        );
        assert_eq!(
            block_gas_fit(BUILDER_GAS_LIMIT - pair + 1, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Defer,
        );

        // Head of line: a pair over the whole block is unfittable at any prefix,
        // so it is evicted rather than deferred forever.
        assert_eq!(
            block_gas_fit(0, BUILDER_GAS_LIMIT + 1, BUILDER_GAS_LIMIT),
            BlockGasFit::Unfittable,
        );
        assert_eq!(
            block_gas_fit(after_one, BUILDER_GAS_LIMIT + 1, BUILDER_GAS_LIMIT),
            BlockGasFit::Unfittable,
        );
        // An undecodable tx counts as the whole block, so it can never be
        // mistaken for free space.
        assert_eq!(
            declared_gas_limit(&Bytes::from_static(b"not a tx")),
            u64::MAX
        );
        assert_eq!(
            block_gas_fit(0, u64::MAX, BUILDER_GAS_LIMIT),
            BlockGasFit::Unfittable,
        );
    }

    #[test]
    fn eip_7825_user_cap_is_independent_of_sync_block_fit() {
        // An invalid user limit can still fit in the aggregate Sync-block budget.
        const EIP_7825_TX_GAS_LIMIT: u64 = alloy_eips::eip7825::MAX_TX_GAS_LIMIT_OSAKA;
        let invalid_user_limit = EIP_7825_TX_GAS_LIMIT + 1;
        let pair_limit = invalid_user_limit;

        assert_eq!(
            block_gas_fit(0, pair_limit, BUILDER_GAS_LIMIT),
            BlockGasFit::Accept,
            "block capacity alone must not be treated as user-transaction validity",
        );
        assert!(invalid_user_limit > EIP_7825_TX_GAS_LIMIT);
        assert!(pair_limit < BUILDER_GAS_LIMIT);
    }

    #[test]
    fn gas_limit_violation_uses_the_decoded_eip_7825_boundary_only_when_active() {
        const EIP_7825_TX_GAS_LIMIT: u64 = alloy_eips::eip7825::MAX_TX_GAS_LIMIT_OSAKA;
        let at_cap = declared_gas_limit(&signed_user_tx(EIP_7825_TX_GAS_LIMIT));
        let over_cap = declared_gas_limit(&signed_user_tx(EIP_7825_TX_GAS_LIMIT + 1));

        assert_eq!(
            gas_limit_violation(at_cap, Some(EIP_7825_TX_GAS_LIMIT)),
            None
        );
        assert_eq!(
            gas_limit_violation(over_cap, Some(EIP_7825_TX_GAS_LIMIT)),
            Some((EIP_7825_TX_GAS_LIMIT + 1, EIP_7825_TX_GAS_LIMIT)),
        );
        assert_eq!(gas_limit_violation(over_cap, None), None);
    }
}
