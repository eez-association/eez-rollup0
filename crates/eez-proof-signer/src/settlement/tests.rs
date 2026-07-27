//! Shared fixtures and builders for the settlement gate test submodules.

use std::num::NonZeroU64;

use alloy_consensus::{SignableTransaction as _, Transaction as _};
use alloy_primitives::{Address, B256, Bytes, I256, Signature, U256, address, b256};
use alloy_sol_types::{SolCall as _, SolValue as _};
use eez_evm::EvmBatch;
use eez_evm::entries::{
    EXECUTE_INCOMING_SELECTOR, IncomingEntry, build_l2_incoming_entry, encode_execute_incoming,
    encode_postbatch,
};
use eez_evm::public_inputs::public_inputs_hashes;
use eez_evm::types::{
    ExecutionEntrySol, ExpectedL1ToL2CallSol, ExpectedLookupSol, ExpectedOutgoingCrossChainCallSol,
    L2ExpectedLookupSol, L2LookupCallSol, L2ToL1CallSol, LookupCallSol,
    RollupIdWithProofSystemsSol, StateDeltaSol,
};
use eez_evm::{RollupId, SYSTEM_ADDRESS, cross_chain_call_hash};
use reth_ethereum_primitives_stateless::{BlockBody, TransactionSigned};
use reth_primitives_traits_stateless::{BlockBody as _, SignerRecoverable as _};

use crate::attest::NonZeroProofSystemVkey;
use crate::testkit::{
    SYSTEM_INBOUND_SELECTOR_TX, SYSTEM_PRIVATE_KEY, SYSTEM_TX, checkpoint,
    system_transaction_context, test_proof_system_vkey,
};
use crate::validate::{OutboundEventObservation, SettlementBlockEvidence, ValidatedBlock};

use super::{
    AuthorizedInboundEffects, AuthorizedOutboundEffects, BlockInspectionError, BoundEffectSequence,
    CanonicalPostBatch, ClaimedEntryShape, DaPayloadError, EffectPrefixError, EthereumBlock,
    InboundCandidate, InboundEffectError, InboundObservationError, ObservedEffectKind,
    OutboundEffectError, PostBatchDecodeError, PublicInputError, SettlingBlockObservations,
    StateDeltaChainError, SystemTransactionKey,
    authorize_inbound_effects as authorize_inbound_effects_for_rollup,
    authorize_outbound_effects as authorize_outbound_effects_for_rollup, bind_effects_to_execution,
    decode_canonical_post_batch, encode_da_payload, inspect_inbound_candidate,
    inspect_settling_block, inspect_validated_settling_block, recompute_public_input_hash,
    verify_da_payload_for_test as verify_da_payload, verify_no_intermediate_system_transactions,
    verify_state_delta_chain, verify_validated_intermediate_blocks,
};

const EXPECTED_PROOF_SYSTEM: Address = address!("00000000000000000000000000000000000000aa");

fn system_transactions() -> super::SystemTransactionReconstructor {
    SystemTransactionKey::new(SYSTEM_PRIVATE_KEY)
        .unwrap()
        .into_reconstructor(1, NonZeroU64::new(1).unwrap())
}

fn build_inbound_transactions(
    entries: &[ExecutionEntrySol],
    context: &eez_evm::system_tx::SystemTxContext,
    starting_nonce: u64,
) -> Vec<TransactionSigned> {
    eez_evm::system_tx::build_inbound_system_txs(entries, context, starting_nonce)
        .unwrap()
        .into_iter()
        .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
        .collect()
}

fn verify_anchor_only_da_payload<'a>(
    da_payload: &[u8],
    blocks: impl IntoIterator<Item = (u64, &'a [u8])>,
) -> Result<(), DaPayloadError> {
    let blocks = blocks.into_iter().collect::<Vec<_>>();
    let (settling, intermediates) = blocks.split_last().expect("at least one block");
    verify_da_payload(
        da_payload,
        intermediates.iter().copied(),
        *settling,
        &AuthorizedOutboundEffects::default(),
        &AuthorizedInboundEffects::default(),
        &system_transactions(),
    )
}

// Fixed signed transaction fixtures. All except `USER_INBOUND_SELECTOR_TX`
// use the well-known Anvil key whose address is `eez_evm::SYSTEM_ADDRESS`.
// Transaction classification is derived from these bytes; no signer is
// injected.
const EIP1559_SYSTEM_TX: &str = "02f862018001018252089442000000000000000000000000000000000000078080c080a01a1ff6a847a249f83cde3536899f858e52db7ab221c7d452d1164a484859f3f3a02c507280e556114e9c646082a07a51c58d23338ff511d18c5805d90ddba8d196";
const SYSTEM_SIGNER_OTHER_TARGET_TX: &str = "f85f02018252089400000000000000000000000000000000000000aa808025a0961dcce4a5ba76fdf8652d8717ccb28e00743fc448c29e131a64a14523791af0a023c6c8ab26b39a97709e43bd9f36da40b0e8a1259d20b0a7ae38eb973e4df201";
const CREATE_TX: &str = "f84b010182cf0880800026a042168e79fe83854ac3f35cce0ec5cd01d422dee84a077986bb1ab3d1773ba5b9a0456e4766eef810187b80eef3d7a935018f8aa5a469910e6179fbf23d9a29b495";
const USER_INBOUND_SELECTOR_TX: &str = "f8648001830186a09442000000000000000000000000000000000000078084eb49424626a09ffe9b2bb85f6d6a32ea19f2643e9e640b97d3189bfa0668ec4eb708e1ce6131a02778884f1c1e1e96cac6cea89f51182eec19bdb33d98e8f56297bdf7328e0c28";

fn expected_rollup_id() -> NonZeroU64 {
    NonZeroU64::new(1).unwrap()
}

fn carrier_batch() -> CanonicalPostBatch {
    let mut batch = EvmBatch::empty();
    batch.inner.proofSystems = vec![EXPECTED_PROOF_SYSTEM];
    batch.inner.rollupIdsWithProofSystems = vec![rollup_row(1)];
    CanonicalPostBatch::from_decoded_for_test(batch)
}

fn rollup_row(rollup_id: u64) -> RollupIdWithProofSystemsSol {
    RollupIdWithProofSystemsSol {
        rollupId: U256::from(rollup_id),
        proofSystemIndex: vec![0],
    }
}

fn lookup(destination_rollup_id: u64) -> LookupCallSol {
    LookupCallSol {
        crossChainCallHash: B256::ZERO,
        destinationRollupId: U256::from(destination_rollup_id),
        returnData: Bytes::new(),
        failed: false,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        rollingHash: B256::ZERO,
        expectedStateRoots: Vec::new(),
    }
}

fn state_entry(rollup_id: U256, current: B256, new: B256) -> ExecutionEntrySol {
    ExecutionEntrySol {
        stateDeltas: vec![StateDeltaSol {
            rollupId: rollup_id,
            currentState: current,
            newState: new,
            etherDelta: I256::ZERO,
        }],
        proxyEntryHash: B256::ZERO,
        destinationRollupId: rollup_id,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        returnData: Bytes::new(),
        rollingHash: B256::ZERO,
    }
}

fn state_chain(roots: &[B256]) -> CanonicalPostBatch {
    let mut batch = EvmBatch::empty();
    batch.inner.entries = roots
        .windows(2)
        .map(|pair| state_entry(U256::from(1), pair[0], pair[1]))
        .collect();
    CanonicalPostBatch::from_decoded_for_test(batch)
}

fn effect_batch(roots: &[B256], kinds: &[ClaimedEntryShape]) -> CanonicalPostBatch {
    assert_eq!(roots.len(), kinds.len() + 2);
    let mut batch = state_chain(roots);
    for (entry, kind) in batch.inner.entries.iter_mut().skip(1).zip(kinds) {
        match kind {
            ClaimedEntryShape::Outbound => {
                let call = l2_to_l1_call();
                entry.stateDeltas[0].etherDelta = -I256::try_from(call.value).unwrap();
                entry.l2ToL1Calls.push(call);
                entry.callCount = U256::from(1);
                // Independent rolling-hash oracle for one successful call
                // with empty return data.
                entry.rollingHash =
                    b256!("68676dacdc339269dad7302dad8697771c8c23d92fa956992dc881fce33e0764");
            }
            ClaimedEntryShape::Inbound => entry.proxyEntryHash = B256::repeat_byte(0x11),
            ClaimedEntryShape::Anchor | ClaimedEntryShape::Invalid => {
                panic!("unsupported test effect kind {kind:?}")
            }
        }
    }
    batch
}

fn settling_with_effect_candidates(
    effect_candidate_positions: Vec<usize>,
) -> SettlingBlockObservations {
    let transactions = effect_candidate_positions
        .iter()
        .max()
        .map_or(0, |position| position + 1);
    let observations =
        SettlingBlockObservations::for_test(vec![false; transactions], Vec::new(), Vec::new());
    assert_eq!(
        observations.effect_candidate_positions(),
        effect_candidate_positions
    );
    observations
}

fn settling_with_system_flags(system_sender_flags: Vec<bool>) -> SettlingBlockObservations {
    SettlingBlockObservations::for_test(system_sender_flags, Vec::new(), Vec::new())
}

fn settling_with_outbound_pairs(count: usize) -> SettlingBlockObservations {
    settling_with_system_flags(
        std::iter::repeat_n([true, false], count)
            .flatten()
            .collect(),
    )
}

fn invalid_inbound_candidate(transaction_index: usize) -> InboundCandidate {
    InboundCandidate {
        transaction_index,
        inspection: Err(InboundObservationError::RevertedTransaction),
    }
}

fn strict_inbound_calldata(value: U256, success: bool) -> Vec<u8> {
    let target = address!("00000000000000000000000000000000000000aa");
    let source = address!("00000000000000000000000000000000000000bb");
    let data = Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]);
    let entry = build_l2_incoming_entry(IncomingEntry {
        target,
        source,
        value,
        data: data.clone(),
        source_rollup_id: RollupId(0),
        l2_rollup_id: RollupId(1),
        return_data: Bytes::from_static(&[0x01, 0x02]),
        success,
    });
    encode_execute_incoming(target, value, data, source, RollupId(0), entry)
}

fn observed_inbound_candidate(
    transaction_index: usize,
    value: U256,
    success: bool,
) -> InboundCandidate {
    let calldata = strict_inbound_calldata(value, success);
    InboundCandidate {
        transaction_index,
        inspection: inspect_inbound_candidate(value, &calldata, true, expected_rollup_id()),
    }
}

fn bindable_inbound_batch(settling: &SettlingBlockObservations) -> CanonicalPostBatch {
    let observations = settling
        .inbound_candidates()
        .iter()
        .map(|candidate| candidate.inspection.as_ref().unwrap())
        .collect::<Vec<_>>();
    let roots = vec![B256::ZERO; observations.len() + 2];
    let kinds = vec![ClaimedEntryShape::Inbound; observations.len()];
    let mut batch = effect_batch(&roots, &kinds);
    for (entry, observation) in batch.inner.entries.iter_mut().skip(1).zip(observations) {
        entry.proxyEntryHash = observation.recomputed_call_hash;
        entry.returnData = observation.return_data.clone();
        entry.stateDeltas[0].etherDelta = I256::try_from(observation.value).unwrap();
    }
    batch
}

fn effect_plan<'batch, 'settling>(
    batch: &'batch CanonicalPostBatch,
    settling: &'settling SettlingBlockObservations,
) -> BoundEffectSequence<'batch, 'settling> {
    let checkpoints = settling
        .effect_candidate_positions()
        .into_iter()
        .map(|transaction_index| checkpoint(transaction_index, B256::ZERO))
        .collect::<Vec<_>>();
    let verified_state_chain = verified_state_chain_for_test(batch);
    bind_effects_to_execution(&verified_state_chain, B256::ZERO, &checkpoints, settling).unwrap()
}

/// Test shorthand for the fixture's fixed rollup identity.
fn verify_effect_prefix<'batch, 'settling>(
    batch: &'batch CanonicalPostBatch,
    pre_settling_root: B256,
    transaction_state_checkpoints: &[crate::validate::TransactionStateCheckpoint],
    settling: &'settling SettlingBlockObservations,
) -> Result<BoundEffectSequence<'batch, 'settling>, EffectPrefixError> {
    let verified_state_chain = verified_state_chain_for_test(batch);
    bind_effects_to_execution(
        &verified_state_chain,
        pre_settling_root,
        transaction_state_checkpoints,
        settling,
    )
}

/// Build the state-chain capability required by effect-binding unit tests.
///
/// Tests that exercise malformed chains call `verify_state_delta_chain`
/// directly instead of bypassing this prerequisite.
fn verified_state_chain_for_test(
    batch: &CanonicalPostBatch,
) -> super::state_chain::VerifiedStateDeltaChain<'_> {
    let entries = &batch.inner.entries;
    let window_pre_state_root = entries[0].stateDeltas[0].currentState;
    let window_post_state_root = entries[entries.len() - 1].stateDeltas[0].newState;
    verify_state_delta_chain(
        batch,
        expected_rollup_id(),
        window_pre_state_root,
        window_post_state_root,
    )
    .expect("effect-binding fixture must have a valid state-delta chain")
}

/// Test shorthand for the fixture's fixed rollup identity.
fn verify_inbound_effect_entries<'settling>(
    bound_effects: &BoundEffectSequence<'_, 'settling>,
) -> Result<AuthorizedInboundEffects<'settling>, InboundEffectError> {
    authorize_inbound_effects_for_rollup(bound_effects, expected_rollup_id())
}

/// Test shorthand for the fixture's fixed rollup identity.
fn authorize_outbound_effects(
    bound_effects: &BoundEffectSequence<'_, '_>,
) -> Result<AuthorizedOutboundEffects, OutboundEffectError> {
    authorize_outbound_effects_for_rollup(bound_effects, expected_rollup_id())
}

fn l2_to_l1_call() -> L2ToL1CallSol {
    L2ToL1CallSol {
        targetAddress: address!("00000000000000000000000000000000000000aa"),
        value: U256::ZERO,
        data: Bytes::from_static(&[0xde, 0xad]),
        sourceAddress: address!("00000000000000000000000000000000000000bb"),
        sourceRollupId: U256::from(1),
        revertSpan: U256::ZERO,
    }
}

fn observed_outbound_call(
    transaction_index: usize,
    receipt_log_index: usize,
    call: &L2ToL1CallSol,
) -> OutboundEventObservation {
    OutboundEventObservation::for_test(
        transaction_index,
        receipt_log_index,
        Some(cross_chain_call_hash(
            RollupId::MAINNET,
            call.targetAddress,
            call.value,
            &call.data,
            call.sourceAddress,
            RollupId(1),
        )),
    )
}

fn expected_call() -> ExpectedL1ToL2CallSol {
    ExpectedL1ToL2CallSol {
        crossChainCallHash: B256::ZERO,
        callCount: U256::ZERO,
        returnData: Bytes::new(),
    }
}

fn expected_lookup() -> ExpectedLookupSol {
    ExpectedLookupSol {
        crossChainCallHash: B256::ZERO,
        returnData: Bytes::new(),
        failed: false,
        l2ToL1CallNumber: 0,
        lastL1ToL2CallConsumed: 0,
        executingLookupIndex: 0,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        callCount: U256::ZERO,
        rollingHash: B256::ZERO,
    }
}

fn l2_expected_outgoing_call() -> ExpectedOutgoingCrossChainCallSol {
    ExpectedOutgoingCrossChainCallSol {
        crossChainCallHash: B256::ZERO,
        callCount: U256::ZERO,
        returnData: Bytes::new(),
    }
}

fn l2_expected_lookup() -> L2ExpectedLookupSol {
    L2ExpectedLookupSol {
        crossChainCallHash: B256::ZERO,
        returnData: Bytes::new(),
        failed: false,
        callNumber: 0,
        lastOutgoingCallConsumed: 0,
        executingLookupIndex: 0,
        incomingCalls: Vec::new(),
        expectedOutgoingCalls: Vec::new(),
        callCount: U256::ZERO,
        rollingHash: B256::ZERO,
    }
}

fn l2_lookup_call() -> L2LookupCallSol {
    L2LookupCallSol {
        crossChainCallHash: B256::ZERO,
        returnData: Bytes::new(),
        failed: false,
        incomingCalls: Vec::new(),
        expectedOutgoingCalls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        rollingHash: B256::ZERO,
    }
}

fn transaction(encoded: &str) -> TransactionSigned {
    alloy_rlp::decode_exact(hex::decode(encoded).unwrap()).unwrap()
}

fn user_transaction(nonce: u64) -> TransactionSigned {
    alloy_consensus::TxLegacy {
        nonce,
        input: vec![u8::try_from(nonce).unwrap()].into(),
        ..Default::default()
    }
    .into_signed(Signature::test_signature())
    .into()
}

fn block_rlp(transactions: Vec<TransactionSigned>) -> Vec<u8> {
    let body = BlockBody {
        transactions,
        ..Default::default()
    };
    alloy_rlp::encode(EthereumBlock::new(Default::default(), body))
}

fn validated_block(number: u64, rlp: Vec<u8>, system_senders: Vec<bool>) -> ValidatedBlock {
    ValidatedBlock::for_test(
        number,
        rlp,
        SettlementBlockEvidence::for_test(system_senders, Vec::new()),
    )
}

fn block_and_payload_transactions(transactions: Vec<TransactionSigned>) -> (Vec<u8>, Vec<Vec<u8>>) {
    let body = BlockBody {
        transactions,
        ..Default::default()
    };
    let payload_transactions = body.encoded_2718_transactions_iter().collect();
    let block = EthereumBlock::new(Default::default(), body);
    (alloy_rlp::encode(block), payload_transactions)
}

fn recorded_calldata() -> Vec<u8> {
    hex::decode(
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/postbatch-13-calldata.hex"
        ))
        .trim()
        .trim_start_matches("0x"),
    )
    .unwrap()
}

fn recorded_batch() -> CanonicalPostBatch {
    decode_canonical_post_batch(recorded_calldata()).unwrap()
}

mod blocks;
mod da;
mod inbound;
mod outbound;
mod post_batch;
mod state;
