//! Test-support stub backend shared with the service tests.

use std::collections::VecDeque;
use std::sync::{Mutex, mpsc};

use reth_primitives_traits::SignerRecoverable as _;
use tokio::sync::oneshot;

use super::*;

pub(crate) fn settling_block_for_test(
    mut block: ValidatedBlock,
    receipt_successes: Vec<bool>,
    checkpoints: Vec<StateCheckpoint>,
) -> ValidatedBlock {
    block.receipt_successes = receipt_successes;
    block.transaction_state_checkpoints = checkpoints;
    block
}

impl SettlementBlockEvidence {
    /// Derive minimal evidence for tests that use a canned backend result.
    ///
    /// Undecodable RLP yields no senders; every caller exact-decodes the same
    /// RLP itself and rejects such a block, so no manufactured facts cross the
    /// checked boundary.
    pub(crate) fn from_rlp_for_test(rlp: &[u8]) -> Self {
        let system_sender_flags = alloy_rlp::decode_exact::<EthereumBlock>(rlp)
            .map(|block| {
                block
                    .body
                    .transactions
                    .iter()
                    .map(|transaction| {
                        transaction
                            .recover_signer()
                            .is_ok_and(|signer| signer == crate::testkit::TEST_SYSTEM_ADDRESS)
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            system_sender_flags,
            observed_outbound_events: Vec::new(),
        }
    }
}

impl BackendBlockOutput {
    /// Replace synthetic receipts to exercise checked-block coverage validation.
    pub(crate) fn set_transaction_results_for_test(&mut self, receipt_successes: Vec<bool>) {
        self.receipt_successes = receipt_successes;
    }
}

/// Raw canned output deliberately kept outside production's checked backend API.
#[derive(Debug)]
pub(crate) struct TestBackendWindowOutput {
    pub blocks: Vec<BackendBlockOutput>,
}

/// Synthetic test metadata need not match the header (many fixtures use a
/// byte-grid identity). Evidence still crosses the production shape checker.
pub(crate) fn check_test_output(
    admitted: &AdmittedBlock,
    output: BackendBlockOutput,
    pre_state_root: B256,
    allow_checkpoints: bool,
) -> Result<Arc<ValidatedBlock>, ValidationError> {
    let decoded = alloy_rlp::decode_exact::<EthereumBlock>(admitted.rlp()).map_err(|error| {
        ValidationError::InvalidBackendOutput(format!(
            "block {} RLP does not decode exactly: {error}",
            admitted.declared_number
        ))
    })?;
    check_backend_block_output(
        admitted.declared_number,
        admitted.claimed_hash,
        &decoded,
        &output,
        allow_checkpoints,
    )
    .map_err(|error| ValidationError::InvalidBackendOutput(error.to_string()))?;
    Ok(Arc::new(ValidatedBlock {
        number: admitted.declared_number,
        hash: output.computed_hash,
        parent_hash: admitted.claimed_parent_hash,
        pre_state_root,
        post_state_root: output.post_state_root,
        rlp: admitted.rlp.clone(),
        decoded,
        receipt_successes: output.receipt_successes,
        transaction_state_checkpoints: output.transaction_state_checkpoints,
        settlement_evidence: output.settlement_evidence,
    }))
}

#[derive(Debug)]
enum StubAction {
    Respond(Result<TestBackendWindowOutput, String>),
    Block {
        started: oneshot::Sender<()>,
        release: mpsc::Receiver<()>,
        response: Result<TestBackendWindowOutput, String>,
    },
    Panic,
}

/// Canned per-call actions, served in order; a call past the end fails loudly.
#[derive(Debug)]
struct StubBackend {
    actions: Mutex<VecDeque<StubAction>>,
    expected_l2_system_address: alloy_primitives::Address,
}

impl StubBackend {
    fn next_response(&self) -> Result<TestBackendWindowOutput, ValidationError> {
        let action = self.actions.lock().unwrap().pop_front().ok_or_else(|| {
            ValidationError::InternalInvariant(
                "stub validator ran out of canned actions".to_owned(),
            )
        })?;
        let response = match action {
            StubAction::Respond(response) => response,
            StubAction::Block {
                started,
                release,
                response,
            } => {
                let _ = started.send(());
                release.recv().map_err(|_| {
                    ValidationError::InternalInvariant(
                        "blocking stub release sender was dropped".to_owned(),
                    )
                })?;
                response
            }
            StubAction::Panic => panic!("stub validator panicked"),
        };
        match response {
            Ok(output) => Ok(output),
            Err(reason) => Err(ValidationError::Rejected(reason)),
        }
    }
}

#[async_trait::async_trait]
impl ValidationBackend for StubBackend {
    fn label(&self) -> &'static str {
        "stub"
    }

    fn chain_id(&self) -> u64 {
        1
    }

    fn expected_l2_system_address(&self) -> alloy_primitives::Address {
        self.expected_l2_system_address
    }

    fn validate_blocks(
        &self,
        blocks: &[AdmittedBlock],
        _witnesses: &mut [ExecutionWitness],
        cancellation: &CancellationToken,
    ) -> Result<BackendWindowOutput, ValidationError> {
        if cancellation.is_cancelled() {
            return Err(ValidationError::Cancelled);
        }
        let output = self.next_response()?;
        if output.blocks.len() != blocks.len() {
            return Err(ValidationError::InvalidBackendOutput(format!(
                "backend returned {} blocks for {} admitted blocks",
                output.blocks.len(),
                blocks.len()
            )));
        }
        let checked = blocks
            .iter()
            .zip(output.blocks)
            .enumerate()
            .map(|(index, (admitted, output))| {
                check_test_output(admitted, output, B256::ZERO, index + 1 == blocks.len())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BackendWindowOutput { blocks: checked })
    }

    fn remaining_test_actions(&self) -> Option<usize> {
        Some(self.actions.lock().unwrap().len())
    }
}

impl Validator {
    /// A stub backend serving `responses` in order.
    pub(crate) fn stub(responses: Vec<Result<TestBackendWindowOutput, String>>) -> Self {
        Self::from_backend(StubBackend {
            actions: Mutex::new(responses.into_iter().map(StubAction::Respond).collect()),
            expected_l2_system_address: crate::testkit::TEST_SYSTEM_ADDRESS,
        })
    }

    /// A one-shot stub with caller-supplied settlement evidence for each block.
    pub(crate) fn stub_with_settlement_evidence(
        mut output: TestBackendWindowOutput,
        evidence: Vec<SettlementBlockEvidence>,
    ) -> Self {
        assert_eq!(
            output.blocks.len(),
            evidence.len(),
            "test settlement evidence must cover every backend block output",
        );
        for (block, settlement_evidence) in output.blocks.iter_mut().zip(evidence) {
            block.settlement_evidence = settlement_evidence;
        }
        Self::stub(vec![Ok(output)])
    }

    /// A stub that blocks one validation until `release` is signalled.
    pub(crate) fn blocking_stub(
        response: Result<TestBackendWindowOutput, String>,
    ) -> (Self, oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let validator = Self::from_backend(StubBackend {
            actions: Mutex::new(
                [StubAction::Block {
                    started: started_tx,
                    release: release_rx,
                    response,
                }]
                .into(),
            ),
            expected_l2_system_address: crate::testkit::TEST_SYSTEM_ADDRESS,
        });
        (validator, started_rx, release_tx)
    }

    /// A stub whose next validation panics.
    pub(crate) fn panicking_stub() -> Self {
        Self::from_backend(StubBackend {
            actions: Mutex::new([StubAction::Panic].into()),
            expected_l2_system_address: crate::testkit::TEST_SYSTEM_ADDRESS,
        })
    }

    /// Number of canned actions remaining.
    pub(crate) fn stub_remaining(&self) -> usize {
        self.backend
            .remaining_test_actions()
            .expect("stub_remaining called on a production backend")
    }
}

/// Minimal backend output matching the admitted hashes and transaction counts.
///
/// Valid block RLP receives one successful status per transaction. Malformed
/// RLP receives an empty status list rather than a fabricated count; the test
/// backend rejects such a block when it constructs checked evidence.
pub(crate) fn backend_output_for(blocks: &[AdmittedBlock]) -> TestBackendWindowOutput {
    TestBackendWindowOutput {
        blocks: blocks
            .iter()
            .map(|block| {
                let decoded_transaction_count =
                    alloy_rlp::decode_exact::<EthereumBlock>(block.rlp())
                        .map(|block| block.body.transactions.len())
                        .unwrap_or_default();
                BackendBlockOutput {
                    computed_hash: block.claimed_hash,
                    receipt_successes: vec![true; decoded_transaction_count],
                    transaction_state_checkpoints: Vec::new(),
                    post_state_root: B256::ZERO,
                    settlement_evidence: SettlementBlockEvidence::from_rlp_for_test(&block.rlp),
                }
            })
            .collect(),
    }
}
