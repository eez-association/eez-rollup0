//! Shared fixtures and the in-process tonic `Prove` harness for the service
//! test submodules.

mod attestation;
mod golden;
mod pipeline;
mod runtime;

use alloy_consensus::{SignableTransaction as _, Transaction as _};
use alloy_primitives::{B256, Bytes, I256, Signature, U256, address, b256};
use alloy_sol_types::SolValue as _;
use eez_control_rpc::v1::prover_client::ProverClient;
use eez_control_rpc::v1::{
    BlockWitness, ExecutionWitness, PostBatch, ProveChunk, ProveHeader, ProveResponse, prove_chunk,
};
use eez_protocol::abi::{ExecutionEntrySol, L2ToL1CallSol, StateUpdateSol};
use reth_primitives_traits::BlockBody as _;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status};

use super::rpc::WorkerGuard;
use super::settlement_job::{
    PipelineError, SettlementInput, SettlementPipelineError, run_settlement, validate_and_settle,
};
use super::*;
use crate::cancel::CancellationToken;
use crate::testkit::{
    SYSTEM_PRIVATE_KEY, TEST_SYSTEM_ADDRESS, checkpoint, system_transaction_context,
    test_proof_system_vkey,
};
use crate::validate::Validator;
use crate::validate::testing::backend_output_for;
use crate::window::AdmittedBlock;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn expected_rollup_id(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn test_proof_system() -> Address {
    address!("00000000000000000000000000000000000000aa")
}

fn test_attester() -> crate::attest::Attester {
    // Anvil account #1. Test-only and intentionally public.
    let private_key = b256!("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d");
    crate::attest::Attester::new(
        private_key,
        test_proof_system_vkey(),
        test_proof_system(),
        TEST_SYSTEM_ADDRESS,
    )
    .unwrap()
}

fn test_system_transaction_reconstructor(
    rollup_id: NonZeroU64,
) -> settlement::SystemTransactionReconstructor {
    test_system_transaction_key().into_reconstructor(1, rollup_id)
}

fn test_system_transaction_key() -> settlement::SystemTransactionKey {
    settlement::SystemTransactionKey::new(SYSTEM_PRIVATE_KEY, TEST_SYSTEM_ADDRESS).unwrap()
}

#[test]
fn service_state_rejects_a_validator_for_another_system_identity() {
    let other_address = address!("6f4c950442e1Af093BcfF730381E63Ae9171b87a");
    let other_key = settlement::SystemTransactionKey::new(
        b256!("0000000000000000000000000000000000000000000000000000000000000042"),
        other_address,
    )
    .unwrap();

    let error = ServiceState::new(
        Validator::stub(Vec::new()),
        expected_rollup_id(1),
        test_attester(),
        other_key,
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "validator and system-transaction key use different L2 system addresses"
    );
}

#[test]
fn service_state_rejects_an_attester_bound_to_another_system_identity() {
    let attester = crate::attest::Attester::new(
        b256!("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"),
        test_proof_system_vkey(),
        test_proof_system(),
        Address::repeat_byte(0xbb),
    )
    .unwrap();

    let error = ServiceState::new(
        Validator::stub(Vec::new()),
        expected_rollup_id(1),
        attester,
        test_system_transaction_key(),
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "attester and system-transaction key use different L2 system addresses"
    );
}

fn anchor_batch() -> eez_protocol::EvmBatch {
    anchor_batch_for(1)
}

fn anchor_batch_for(rollup_id: u64) -> eez_protocol::EvmBatch {
    let mut batch = eez_protocol::EvmBatch::default();
    batch.entries.push(ExecutionEntrySol {
        stateUpdates: vec![StateUpdateSol {
            rollupId: rollup_id,
            currentState: B256::ZERO,
            newState: B256::ZERO,
            etherDelta: I256::ZERO,
        }],
        proxyEntryHash: B256::ZERO,
        destinationRollupId: rollup_id,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        success: true,
        returnData: Bytes::new(),
        rollingHash: B256::ZERO,
    });
    batch.immediateEntryCount = U256::from(1);
    batch.proofSystems = vec![test_proof_system()];
    batch.rollupIdsWithProofSystems = vec![eez_protocol::abi::RollupIdWithProofSystemsSol {
        rollupId: rollup_id,
        proofSystemIndexes: vec![0],
    }];
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut batch).unwrap();
    batch
}

fn outbound_batch(
    anchor_root: B256,
    pre_settling_root: B256,
    final_root: B256,
) -> eez_protocol::EvmBatch {
    let mut batch = anchor_batch();
    let anchor = &mut batch.entries[0];
    anchor.stateUpdates[0].currentState = anchor_root;
    anchor.stateUpdates[0].newState = pre_settling_root;

    let mut effect = anchor.clone();
    effect.stateUpdates[0].currentState = pre_settling_root;
    effect.stateUpdates[0].newState = final_root;
    effect.l2ToL1Calls.push(l2_to_l1_call());
    batch.entries.push(effect);
    batch.immediateEntryCount = U256::from(2);
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut batch).unwrap();
    batch
}

fn l2_to_l1_call() -> L2ToL1CallSol {
    L2ToL1CallSol {
        revertNextNCalls: 0,
        isStatic: false,
        gas: 0,
        sourceAddress: Address::ZERO,
        sourceRollupId: 1,
        targetAddress: Address::ZERO,
        value: U256::ZERO,
        data: Bytes::new(),
    }
}

/// Canonical one-pair Sync block and the batch that commits to it.
fn canonical_outbound_case() -> (eez_protocol::EvmBatch, Vec<u8>, Vec<u8>, B256) {
    outbound_case(U256::ZERO)
}

fn outbound_case(value: U256) -> (eez_protocol::EvmBatch, Vec<u8>, Vec<u8>, B256) {
    let mut batch = outbound_batch(B256::ZERO, B256::ZERO, B256::ZERO);
    batch.entries[1].l2ToL1Calls[0].value = value;
    batch.entries[1].stateUpdates[0].etherDelta = -I256::try_from(value).unwrap();
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut batch).unwrap();
    let call = &batch.entries[1].l2ToL1Calls[0];
    let call_hash = eez_protocol::l2_outbound_call_hash(
        eez_protocol::CallHashInput {
            call_mode: eez_protocol::CallMode::Mutable,
            source_address: call.sourceAddress,
            source_rollup_id: eez_protocol::RollupId(1),
            target_address: call.targetAddress,
            target_rollup_id: eez_protocol::RollupId::MAINNET,
            value: call.value,
            data: &call.data,
        },
        0,
    );
    let user_body: reth_ethereum_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions: vec![non_system_transaction()],
        ..Default::default()
    };
    let user = user_body.encoded_2718_transactions_iter().next().unwrap();
    let mut sidecar = batch.entries[1].clone();
    sidecar.stateUpdates.clear();
    sidecar.rollingHash = B256::ZERO;
    let pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
        &[(sidecar.clone(), Bytes::from(user.clone()))],
        &[],
        &system_transaction_context(),
        0,
    )
    .unwrap();
    let transactions = eez_protocol::system_tx::interleave_sync_block_txs(&pairs)
        .into_iter()
        .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
        .collect();
    let body: reth_ethereum_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions,
        ..Default::default()
    };
    let block = reth_ethereum_primitives::Block::new(Default::default(), body);
    batch.callData =
        settlement::encode_da_payload(&[vec![user.clone()]], &[sidecar.abi_encode()]).into();
    (batch, alloy_rlp::encode(block), user, call_hash)
}

fn outbound_backend_output() -> validate::BackendWindowOutput {
    let inputs = [AdmittedBlock::test(5, 0x04, 0x05)];
    let mut backend_output = backend_output_for(&inputs);
    backend_output.blocks[0].set_transaction_results_for_test(vec![true, true]);
    backend_output.blocks[0].transaction_state_checkpoints = vec![checkpoint(1, B256::ZERO)];
    backend_output.blocks[0]
        .settlement_evidence
        .set_system_sender_flags_for_test(vec![true, false]);
    backend_output
}

fn outbound_evidence(call_hash: B256) -> validate::SettlementBlockEvidence {
    validate::SettlementBlockEvidence::for_test(
        vec![true, false],
        vec![validate::OutboundEventObservation::decoded_for_test(
            1, 0, call_hash, 0,
        )],
    )
}

fn mixed_outbound_inbound_case() -> (eez_protocol::EvmBatch, Vec<u8>, B256) {
    let (mut batch, _outbound_block, user, outbound_call_hash) = canonical_outbound_case();
    let value = U256::from(9);
    let (_unused_nonce_zero_tx, inbound_call_hash, return_data, inbound_sidecar) =
        strict_inbound_transaction(value);

    let mut inbound_entry = batch.entries[0].clone();
    inbound_entry.stateUpdates[0].etherDelta = I256::try_from(value).unwrap();
    inbound_entry.proxyEntryHash = inbound_call_hash;
    inbound_entry.returnData = return_data;
    batch.entries.push(inbound_entry);
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut batch).unwrap();

    let mut outbound_sidecar = batch.entries[1].clone();
    outbound_sidecar.stateUpdates.clear();
    outbound_sidecar.rollingHash = B256::ZERO;
    let pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
        &[(outbound_sidecar.clone(), Bytes::from(user.clone()))],
        std::slice::from_ref(&inbound_sidecar),
        &system_transaction_context(),
        0,
    )
    .unwrap();
    let transactions = eez_protocol::system_tx::interleave_sync_block_txs(&pairs)
        .into_iter()
        .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
        .collect();
    let body: reth_ethereum_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions,
        ..Default::default()
    };
    let block = reth_ethereum_primitives::Block::new(Default::default(), body);
    batch.callData = settlement::encode_da_payload(
        &[vec![user]],
        &[outbound_sidecar.abi_encode(), inbound_sidecar.abi_encode()],
    )
    .into();

    (batch, alloy_rlp::encode(block), outbound_call_hash)
}

fn mixed_backend_output() -> validate::BackendWindowOutput {
    let inputs = [AdmittedBlock::test(5, 0x04, 0x05)];
    let mut backend_output = backend_output_for(&inputs);
    backend_output.blocks[0].set_transaction_results_for_test(vec![true, true, true]);
    backend_output.blocks[0].transaction_state_checkpoints =
        vec![checkpoint(1, B256::ZERO), checkpoint(2, B256::ZERO)];
    backend_output
}

fn mixed_evidence(outbound_call_hash: B256) -> validate::SettlementBlockEvidence {
    validate::SettlementBlockEvidence::for_test(
        vec![true, false, true],
        vec![validate::OutboundEventObservation::decoded_for_test(
            1,
            0,
            outbound_call_hash,
            0,
        )],
    )
}

fn single_block_settlement_window(
    batch: eez_protocol::EvmBatch,
    block_rlp: Vec<u8>,
) -> Vec<ProveChunk> {
    let mut block = block_chunk(5, 0x04, 0x05);
    block_mut(&mut block).rlp = block_rlp;
    let mut window = vec![header_chunk(5, 5), block];
    replace_post_batch(&mut window, public_input_post_batch_for(batch));
    window
}

fn header_chunk(from: u64, to: u64) -> ProveChunk {
    let block_count = to
        .checked_sub(from)
        .and_then(|distance| distance.checked_add(1))
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or_default();
    ProveChunk {
        kind: Some(prove_chunk::Kind::Header(ProveHeader {
            rollup_id: 1,
            from_block: from,
            to_block: to,
            post_batch: Some(public_input_post_batch_for_empty_blocks(
                anchor_batch(),
                block_count,
            )),
        })),
    }
}

fn public_input_post_batch() -> PostBatch {
    public_input_post_batch_for_empty_blocks(anchor_batch(), 3)
}

fn public_input_post_batch_for(batch: eez_protocol::EvmBatch) -> PostBatch {
    PostBatch {
        abi_calldata: eez_protocol::entries::encode_postbatch(&batch),
        ..PostBatch::default()
    }
}

fn public_input_post_batch_for_empty_blocks(
    mut batch: eez_protocol::EvmBatch,
    block_count: usize,
) -> PostBatch {
    batch.callData = settlement::encode_da_payload(&vec![Vec::new(); block_count], &[]).into();
    public_input_post_batch_for(batch)
}

fn recompute_test_public_inputs_hash(batch: &eez_protocol::EvmBatch) -> B256 {
    settlement::recompute_public_input_hash(
        batch,
        test_proof_system_vkey(),
        expected_rollup_id(1),
        test_proof_system(),
    )
    .unwrap()
}

#[track_caller]
fn assert_attestation(response: &ProveResponse, expected_hash: B256, expected_signer: Address) {
    assert_eq!(response.public_inputs_hash, expected_hash.as_slice());
    assert_eq!(response.signature.len(), 65);
    let signature = Signature::try_from(response.signature.as_slice()).unwrap();
    assert_eq!(
        signature
            .recover_address_from_prehash(&expected_hash)
            .unwrap(),
        expected_signer
    );
}

fn replace_post_batch(window: &mut [ProveChunk], post_batch: PostBatch) {
    header_mut(&mut window[0]).post_batch = Some(post_batch);
}

#[track_caller]
fn header_mut(chunk: &mut ProveChunk) -> &mut ProveHeader {
    let Some(prove_chunk::Kind::Header(header)) = &mut chunk.kind else {
        unreachable!("test chunk must be a header");
    };
    header
}

#[track_caller]
fn block_mut(chunk: &mut ProveChunk) -> &mut BlockWitness {
    let Some(prove_chunk::Kind::Block(block)) = &mut chunk.kind else {
        unreachable!("test chunk must be a block");
    };
    block
}

fn da_payload_for_window(window: &[ProveChunk]) -> Vec<u8> {
    let blocks = window
        .iter()
        .skip(1)
        .map(|chunk| {
            let Some(prove_chunk::Kind::Block(block)) = &chunk.kind else {
                panic!("test window contains a non-block chunk after its header");
            };
            let block =
                alloy_rlp::decode_exact::<reth_ethereum_primitives::Block>(&block.rlp).unwrap();
            block.body.encoded_2718_transactions_iter().collect()
        })
        .collect::<Vec<_>>();
    settlement::encode_da_payload(&blocks, &[])
}

fn replace_batch_bound_to_window(window: &mut [ProveChunk], mut batch: eez_protocol::EvmBatch) {
    batch.callData = da_payload_for_window(window).into();
    replace_post_batch(window, public_input_post_batch_for(batch));
}

fn block_chunk(number: u64, parent: u8, hash: u8) -> ProveChunk {
    let block = reth_ethereum_primitives::Block::new(
        Default::default(),
        reth_ethereum_primitives::BlockBody::default(),
    );
    ProveChunk {
        kind: Some(prove_chunk::Kind::Block(BlockWitness {
            number,
            hash: vec![hash; 32],
            parent_hash: vec![parent; 32],
            rlp: alloy_rlp::encode(block),
            witness: Some(ExecutionWitness {
                state: vec![vec![number as u8]],
                ..ExecutionWitness::default()
            }),
        })),
    }
}

/// A chained happy-path window: header(5..=7) + blocks 5, 6, 7.
fn happy_window() -> Vec<ProveChunk> {
    vec![
        header_chunk(5, 7),
        block_chunk(5, 0x04, 0x05),
        block_chunk(6, 0x05, 0x06),
        block_chunk(7, 0x06, 0x07),
    ]
}

fn stateless_window() -> Vec<ProveChunk> {
    let consensus_header = alloy_consensus::Header {
        number: 5,
        parent_hash: alloy_primitives::B256::repeat_byte(0x04),
        ..Default::default()
    };
    let hash = consensus_header.hash_slow();
    let consensus_block = alloy_consensus::Block::<
        alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844>,
    >::new(consensus_header, Default::default());
    let mut chunk = block_chunk(5, 0x04, 0x05);
    let block = block_mut(&mut chunk);
    block.hash = hash.to_vec();
    block.rlp = alloy_rlp::encode(consensus_block);
    vec![header_chunk(5, 5), chunk]
}

fn stateless_transaction_window() -> Vec<ProveChunk> {
    let header = alloy_consensus::Header {
        number: 5,
        parent_hash: B256::repeat_byte(0x04),
        ..Default::default()
    };
    let hash = header.hash_slow();
    let body: reth_ethereum_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions: vec![non_system_transaction()],
        ..Default::default()
    };
    let block = reth_ethereum_primitives::Block::new(header, body);
    let chunk = ProveChunk {
        kind: Some(prove_chunk::Kind::Block(BlockWitness {
            number: 5,
            hash: hash.to_vec(),
            parent_hash: vec![0x04; 32],
            rlp: alloy_rlp::encode(block),
            witness: Some(ExecutionWitness::default()),
        })),
    };
    vec![header_chunk(5, 5), chunk]
}

type TestTransaction = alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844>;

fn single_non_system_transaction_window() -> Vec<ProveChunk> {
    single_transaction_window(non_system_transaction())
}

fn non_system_transaction() -> TestTransaction {
    alloy_consensus::TxLegacy::default()
        .into_signed(alloy_primitives::Signature::test_signature())
        .into()
}

fn single_system_transaction_window() -> Vec<ProveChunk> {
    single_transaction_window(system_transaction())
}

fn system_transaction() -> TestTransaction {
    alloy_rlp::decode_exact(hex::decode(crate::testkit::SYSTEM_TX).unwrap()).unwrap()
}

fn strict_inbound_transaction(value: U256) -> (TestTransaction, B256, Bytes, ExecutionEntrySol) {
    let target = alloy_primitives::address!("00000000000000000000000000000000000000aa");
    let source = alloy_primitives::address!("00000000000000000000000000000000000000bb");
    let data = Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]);
    let return_data = Bytes::from_static(&[0x01, 0x02]);
    let entry =
        eez_protocol::entries::build_l2_incoming_entry(eez_protocol::entries::IncomingEntry {
            target,
            source,
            value,
            data: data.clone(),
            source_rollup_id: eez_protocol::RollupId(0),
            l2_rollup_id: eez_protocol::RollupId(1),
            return_data: return_data.clone(),
            success: true,
        })
        .unwrap();
    let call_hash = entry.proxyEntryHash;
    let sidecar = ExecutionEntrySol {
        stateUpdates: Vec::new(),
        proxyEntryHash: call_hash,
        l2ToL1Calls: vec![L2ToL1CallSol {
            revertNextNCalls: 0,
            isStatic: false,
            gas: 0,
            sourceAddress: source,
            sourceRollupId: 0,
            targetAddress: target,
            value,
            data: data.clone(),
        }],
        expectedL1ToL2Calls: Vec::new(),
        rollingHash: entry.rollingHash,
        destinationRollupId: 1,
        success: true,
        returnData: return_data.clone(),
    };
    let input = eez_protocol::entries::encode_execute_incoming(
        target,
        value,
        data,
        source,
        eez_protocol::RollupId(0),
        entry,
    );
    let context = system_transaction_context();
    let raw = eez_protocol::system_tx::build_inbound_system_txs(
        std::slice::from_ref(&sidecar),
        &context,
        0,
    )
    .unwrap()
    .remove(0);
    let transaction: TestTransaction = alloy_rlp::decode_exact(raw.as_ref()).unwrap();
    assert_eq!(transaction.input(), input.as_slice());
    (transaction, call_hash, return_data, sidecar)
}

fn single_transaction_window(transaction: TestTransaction) -> Vec<ProveChunk> {
    vec![
        header_chunk(5, 5),
        transaction_block_chunk(5, 0x04, 0x05, transaction),
    ]
}

fn two_block_transaction_window(
    first: TestTransaction,
    settling: TestTransaction,
) -> Vec<ProveChunk> {
    vec![
        header_chunk(5, 6),
        transaction_block_chunk(5, 0x04, 0x05, first),
        transaction_block_chunk(6, 0x05, 0x06, settling),
    ]
}

fn transaction_block_chunk(
    number: u64,
    parent: u8,
    hash: u8,
    transaction: TestTransaction,
) -> ProveChunk {
    transactions_block_chunk(number, parent, hash, vec![transaction])
}

fn transactions_block_chunk(
    number: u64,
    parent: u8,
    hash: u8,
    transactions: Vec<TestTransaction>,
) -> ProveChunk {
    let body: reth_ethereum_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions,
        ..Default::default()
    };
    let consensus_block = reth_ethereum_primitives::Block::new(Default::default(), body);
    let mut chunk = block_chunk(number, parent, hash);
    block_mut(&mut chunk).rlp = alloy_rlp::encode(consensus_block);
    chunk
}

/// The admitted-block shape of [`happy_window`], for stub backend-output construction.
fn happy_block_inputs() -> Vec<AdmittedBlock> {
    [(5u64, 0x04u8, 0x05u8), (6, 0x05, 0x06), (7, 0x06, 0x07)]
        .into_iter()
        .map(|(number, parent_hash, hash)| AdmittedBlock::test(number, parent_hash, hash))
        .collect()
}

fn limits() -> ServiceLimits {
    limits_with(
        16,
        1024 * 1024,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
}

fn limits_with_checkpoint_limit(max_transaction_state_checkpoints: usize) -> ServiceLimits {
    ServiceLimits::new(ServiceLimitsParams {
        max_window_blocks: nz(16),
        max_window_bytes: nz(1024 * 1024),
        max_window_witness_items: nz(1024),
        max_transaction_state_checkpoints,
        stream_idle_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(30),
    })
    .unwrap()
}

fn limits_with(
    max_blocks: usize,
    max_bytes: usize,
    idle_timeout: Duration,
    request_timeout: Duration,
) -> ServiceLimits {
    ServiceLimits::new(ServiceLimitsParams {
        max_window_blocks: nz(max_blocks),
        max_window_bytes: nz(max_bytes),
        max_window_witness_items: nz(1024),
        max_transaction_state_checkpoints: 8,
        stream_idle_timeout: idle_timeout,
        request_timeout,
    })
    .unwrap()
}

/// One in-process server with shared client setup and drop-triggered shutdown.
struct TestServer {
    endpoint: String,
    _shutdown: oneshot::Sender<()>,
}

impl TestServer {
    async fn new(state: Arc<ServiceState>) -> Self {
        Self::with_limits(state, limits()).await
    }

    async fn with_limits(state: Arc<ServiceState>, limits: ServiceLimits) -> Self {
        Self::with_service(ProveSvc::new(state, limits)).await
    }

    async fn with_service(svc: ProveSvc) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc.into_server())
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .expect("test Prove server failed");
        });
        Self {
            endpoint: format!("http://{addr}"),
            _shutdown: shutdown,
        }
    }

    async fn client(&self) -> ProverClient<tonic::transport::Channel> {
        ProverClient::connect(self.endpoint.clone()).await.unwrap()
    }

    async fn prove(&self, chunks: Vec<ProveChunk>) -> Status {
        self.client()
            .await
            .prove(tokio_stream::iter(chunks))
            .await
            .expect_err("request must be rejected")
    }

    async fn attest(&self, chunks: Vec<ProveChunk>) -> ProveResponse {
        self.client()
            .await
            .prove(tokio_stream::iter(chunks))
            .await
            .expect("valid window must be attested")
            .into_inner()
    }
}

fn unused_validator() -> Arc<ServiceState> {
    inner(Validator::stub(Vec::new()))
}

fn one_accepting_validator() -> Arc<ServiceState> {
    inner(Validator::stub(vec![Ok(backend_output_for(
        &happy_block_inputs(),
    ))]))
}

fn one_accepting_single_block_validator() -> Arc<ServiceState> {
    inner(Validator::stub(vec![Ok(backend_output_for(&[
        AdmittedBlock::test(5, 0x04, 0x05),
    ]))]))
}

fn single_block_validator_with_execution_evidence(
    receipt_successes: Vec<bool>,
    system_sender_flags: Vec<bool>,
) -> Arc<ServiceState> {
    let mut backend_output = backend_output_for(&[AdmittedBlock::test(5, 0x04, 0x05)]);
    backend_output.blocks[0].set_transaction_results_for_test(receipt_successes);
    backend_output.blocks[0]
        .settlement_evidence
        .set_system_sender_flags_for_test(system_sender_flags);
    inner(Validator::stub(vec![Ok(backend_output)]))
}

fn two_block_validator_with_execution_evidence(
    first_receipt_successes: Vec<bool>,
    first_system_sender_flags: Vec<bool>,
    settling_receipt_successes: Vec<bool>,
    settling_system_sender_flags: Vec<bool>,
) -> Arc<ServiceState> {
    let mut backend_output = backend_output_for(&[
        AdmittedBlock::test(5, 0x04, 0x05),
        AdmittedBlock::test(6, 0x05, 0x06),
    ]);
    backend_output.blocks[0].set_transaction_results_for_test(first_receipt_successes);
    backend_output.blocks[0]
        .settlement_evidence
        .set_system_sender_flags_for_test(first_system_sender_flags);
    backend_output.blocks[1].set_transaction_results_for_test(settling_receipt_successes);
    backend_output.blocks[1]
        .settlement_evidence
        .set_system_sender_flags_for_test(settling_system_sender_flags);
    inner(Validator::stub(vec![Ok(backend_output)]))
}

fn inner(validator: Validator) -> Arc<ServiceState> {
    inner_with_rollup(validator, expected_rollup_id(1))
}

fn inner_with_rollup(validator: Validator, expected_rollup_id: NonZeroU64) -> Arc<ServiceState> {
    Arc::new(
        ServiceState::new(
            validator,
            expected_rollup_id,
            test_attester(),
            test_system_transaction_key(),
        )
        .unwrap(),
    )
}
