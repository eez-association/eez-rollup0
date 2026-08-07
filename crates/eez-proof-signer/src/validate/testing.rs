//! Test-support stub backend shared with the service tests.

use std::collections::VecDeque;
use std::sync::{Mutex, mpsc};

use reth_primitives_traits::SignerRecoverable as _;
use tokio::sync::oneshot;

use super::*;

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
    /// Replace the synthetic transaction results while keeping the backend's
    /// decoded transaction count associated with them.
    pub(crate) fn set_transaction_results_for_test(&mut self, receipt_successes: Vec<bool>) {
        self.decoded_transaction_count = receipt_successes.len();
        self.receipt_successes = receipt_successes;
    }
}

#[derive(Debug)]
enum StubAction {
    Respond(Result<BackendWindowOutput, String>),
    Block {
        started: oneshot::Sender<()>,
        release: mpsc::Receiver<()>,
        response: Result<BackendWindowOutput, String>,
    },
    Panic,
}

/// Canned per-call actions, served in order; a call past the end fails loudly.
#[derive(Debug)]
pub(crate) struct StubValidator {
    actions: Mutex<VecDeque<StubAction>>,
    pub(super) expected_l2_system_address: alloy_primitives::Address,
}

impl StubValidator {
    pub(super) fn next_response(&self) -> eyre::Result<BackendWindowOutput> {
        let action = self
            .actions
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| eyre::eyre!("stub validator ran out of canned actions"))?;
        let response = match action {
            StubAction::Respond(response) => response,
            StubAction::Block {
                started,
                release,
                response,
            } => {
                let _ = started.send(());
                release
                    .recv()
                    .map_err(|_| eyre::eyre!("blocking stub release sender was dropped"))?;
                response
            }
            StubAction::Panic => panic!("stub validator panicked"),
        };
        match response {
            Ok(output) => Ok(output),
            Err(reason) => Err(eyre::eyre!("{reason}")),
        }
    }
}

impl Validator {
    /// A stub backend serving `responses` in order.
    pub(crate) fn stub(responses: Vec<Result<BackendWindowOutput, String>>) -> Self {
        Self::Stub(StubValidator {
            actions: Mutex::new(responses.into_iter().map(StubAction::Respond).collect()),
            expected_l2_system_address: crate::testkit::TEST_SYSTEM_ADDRESS,
        })
    }

    /// A one-shot stub with caller-supplied settlement evidence for each block.
    pub(crate) fn stub_with_settlement_evidence(
        mut output: BackendWindowOutput,
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
        response: Result<BackendWindowOutput, String>,
    ) -> (Self, oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let validator = Self::Stub(StubValidator {
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
        Self::Stub(StubValidator {
            actions: Mutex::new([StubAction::Panic].into()),
            expected_l2_system_address: crate::testkit::TEST_SYSTEM_ADDRESS,
        })
    }

    /// Number of canned actions remaining.
    pub(crate) fn stub_remaining(&self) -> usize {
        match self {
            Self::Stub(stub) => stub.actions.lock().unwrap().len(),
            Self::Stateless(_) => panic!("stub_remaining called on the stateless backend"),
        }
    }
}

/// Minimal backend output matching the admitted hashes and transaction counts.
///
/// Valid block RLP receives one successful status per transaction. Malformed
/// RLP receives an empty status list rather than a fabricated count; the shared
/// backend check rejects such a block at its own exact decode anyway.
pub(crate) fn backend_output_for(blocks: &[AdmittedBlock]) -> BackendWindowOutput {
    BackendWindowOutput {
        pre_state_root: B256::ZERO,
        blocks: blocks
            .iter()
            .map(|block| {
                let decoded_transaction_count = decode_transaction_count(block).unwrap_or_default();
                BackendBlockOutput {
                    decoded_number: block.declared_number,
                    decoded_parent_hash: block.claimed_parent_hash,
                    computed_hash: block.claimed_hash,
                    decoded_transaction_count,
                    receipt_successes: vec![true; decoded_transaction_count],
                    transaction_state_checkpoints: Vec::new(),
                    post_state_root: B256::ZERO,
                    settlement_evidence: SettlementBlockEvidence::from_rlp_for_test(&block.rlp),
                }
            })
            .collect(),
    }
}
