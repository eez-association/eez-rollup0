//! Test-support stub backend shared with the service tests.

use std::collections::VecDeque;
use std::sync::Mutex;

use reth_primitives_traits::SignerRecoverable as _;

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

/// Synthetic test metadata need not match the header (many fixtures use a
/// byte-grid identity). Evidence still crosses the production shape checker.
pub(crate) fn check_test_output(
    admitted: &AdmittedBlock,
    output: BackendBlockOutput,
    pre_state_root: B256,
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

#[derive(Debug, Default)]
struct StubState {
    responses: VecDeque<Result<Vec<BackendBlockOutput>, String>>,
    pending: VecDeque<BackendBlockOutput>,
}

/// Canned execution facts, deliberately independent of execution correctness.
#[derive(Debug)]
pub(crate) struct StubBackend {
    state: Mutex<StubState>,
    expected_l2_system_address: alloy_primitives::Address,
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
    fn validate_anchor(&self, _anchor: BlockAnchor) -> Result<(), ValidationError> {
        Ok(())
    }
    async fn validate_next(
        &self,
        parent: BlockAnchor,
        block: &AdmittedBlock,
        _witness: ExecutionWitness,
        _timeout: Duration,
    ) -> Result<(Arc<ValidatedBlock>, bool), ValidationError> {
        block.check_parent(parent)?;
        let output = {
            let mut state = self.state.lock().unwrap();
            if state.pending.is_empty() {
                let output = state
                    .responses
                    .pop_front()
                    .ok_or_else(|| {
                        ValidationError::InvalidBackendOutput("missing canned block output".into())
                    })?
                    .map_err(ValidationError::Rejected)?;
                state.pending = output.into();
            }
            state.pending.pop_front().ok_or_else(|| {
                ValidationError::InvalidBackendOutput("empty canned output".into())
            })?
        };
        Ok((check_test_output(block, output, parent.state_root)?, false))
    }
    fn remaining_test_actions(&self) -> Option<usize> {
        let state = self.state.lock().unwrap();
        Some(state.responses.len() + usize::from(!state.pending.is_empty()))
    }
}

impl StubBackend {
    pub(crate) fn new(responses: Vec<Result<Vec<BackendBlockOutput>, String>>) -> Self {
        Self {
            state: Mutex::new(StubState {
                responses: responses.into(),
                ..Default::default()
            }),
            expected_l2_system_address: crate::testkit::TEST_SYSTEM_ADDRESS,
        }
    }
    pub(crate) fn with_settlement_evidence(
        mut output: Vec<BackendBlockOutput>,
        evidence: Vec<SettlementBlockEvidence>,
    ) -> Self {
        assert_eq!(output.len(), evidence.len());
        for (block, evidence) in output.iter_mut().zip(evidence) {
            block.settlement_evidence = evidence;
        }
        Self::new(vec![Ok(output)])
    }
    /// Apply the production evidence checker to canned outputs for pure shape tests.
    pub(crate) fn validate(
        &self,
        blocks: &[AdmittedBlock],
    ) -> Result<ValidatedWindow, ValidationError> {
        if blocks.is_empty() {
            return Err(ValidationError::Rejected(
                "refusing to validate an empty window".into(),
            ));
        }
        let output = self
            .state
            .lock()
            .unwrap()
            .responses
            .pop_front()
            .ok_or_else(|| {
                ValidationError::InternalInvariant(
                    "stub validator ran out of canned actions".into(),
                )
            })?
            .map_err(ValidationError::Rejected)?;
        if output.len() != blocks.len() {
            return Err(ValidationError::InvalidBackendOutput(
                "backend block count does not match admitted window".into(),
            ));
        }
        let mut checked = blocks
            .iter()
            .zip(output)
            .map(|(block, output)| check_test_output(block, output, B256::ZERO))
            .collect::<Result<Vec<_>, _>>()?;
        let terminal = checked.pop().unwrap();
        Ok(ValidatedWindow::from_validated_prefix(
            blocks[0].claimed_parent_hash,
            checked,
            terminal,
        ))
    }
}

/// Minimal backend output matching the admitted hashes and transaction counts.
///
/// Valid block RLP receives one successful status per transaction. Malformed
/// RLP receives an empty status list rather than a fabricated count; the test
/// backend rejects such a block when it constructs checked evidence.
pub(crate) fn backend_output_for(blocks: &[AdmittedBlock]) -> Vec<BackendBlockOutput> {
    blocks
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let decoded_transaction_count = alloy_rlp::decode_exact::<EthereumBlock>(block.rlp())
                .map(|block| block.body.transactions.len())
                .unwrap_or_default();
            let transaction_state_checkpoints = (index + 1 == blocks.len()
                && decoded_transaction_count == 0)
                .then(|| crate::testkit::pre_execution_checkpoint(block.claimed_hash))
                .into_iter()
                .collect();
            BackendBlockOutput {
                computed_hash: block.claimed_hash,
                receipt_successes: vec![true; decoded_transaction_count],
                transaction_state_checkpoints,
                post_state_root: B256::ZERO,
                settlement_evidence: SettlementBlockEvidence::from_rlp_for_test(&block.rlp),
            }
        })
        .collect()
}
