use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use alloy_primitives::{U256, keccak256};
use alloy_provider::{ProviderBuilder, mock::Asserter};
use async_trait::async_trait;
use eez_protocol::EcdsaProofSigner;
use eez_prover::{ActionableProverFailure, RetryableProverError};

use super::*;

/// How a scripted attester answers.
#[derive(Debug, Clone, Copy)]
enum Answer {
    /// A valid proof over the digest of the batch it was asked to prove.
    Sign,
    /// A proof over some other digest.
    SignElsewhere,
    /// A valid signature in its malleable high-s form.
    SignHighS,
    /// A valid signature with the 0/1 recovery byte `ECDSA.recover` rejects.
    SignRawRecovery,
    /// A proof of the wrong length.
    Truncated,
    /// A failure returned by the prover.
    Fail(fn() -> ProverError),
    /// A prover that panics.
    Panic,
}

/// A prover that answers once after `delay`, recording the request it saw.
#[derive(Debug)]
struct ScriptedAttester {
    key: EcdsaProofSigner,
    delay: Duration,
    answer: Answer,
    seen: Mutex<Option<ProvingContext>>,
    finished: AtomicBool,
}

impl ScriptedAttester {
    fn new(key_byte: u8, delay_ms: u64, answer: Answer) -> Arc<Self> {
        Arc::new(Self {
            key: EcdsaProofSigner::from_private_key(B256::repeat_byte(key_byte)).unwrap(),
            delay: Duration::from_millis(delay_ms),
            answer,
            seen: Mutex::new(None),
            finished: AtomicBool::new(false),
        })
    }

    fn sign(&self, digest: B256) -> Bytes {
        self.key.sign_prehash(digest).unwrap()
    }
}

#[async_trait]
impl Prover for ScriptedAttester {
    async fn prove(&self, ctx: ProvingContext) -> Result<Bytes, ProverError> {
        let digest = public_inputs_hashes(&ctx.batch, vkey_of(self.key.address())).unwrap()[0];
        *self.seen.lock().unwrap() = Some(ctx);
        tokio::time::sleep(self.delay).await;
        self.finished.store(true, Ordering::SeqCst);
        match self.answer {
            Answer::Sign => Ok(self.sign(digest)),
            Answer::SignElsewhere => Ok(self.sign(keccak256(digest))),
            Answer::SignHighS => {
                let signature = Signature::try_from(self.sign(digest).as_ref()).unwrap();
                let high =
                    Signature::new(signature.r(), SECP256K1N - signature.s(), !signature.v());
                Ok(Bytes::copy_from_slice(&high.as_bytes()))
            }
            Answer::SignRawRecovery => {
                let mut proof = self.sign(digest).to_vec();
                proof[64] -= 27;
                Ok(Bytes::from(proof))
            }
            Answer::Truncated => Ok(self.sign(digest).slice(..64)),
            Answer::Fail(error) => Err(error()),
            Answer::Panic => panic!("attester crashed"),
        }
    }

    fn vkey(&self) -> B256 {
        B256::ZERO
    }
}

/// The secp256k1 group order.
const SECP256K1N: U256 = U256::from_be_slice(&[
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36, 0x41, 0x41,
]);

/// The membership-ticket vkey the devnet registers for an ECDSA signer.
fn vkey_of(attester: Address) -> B256 {
    attester.into_word()
}

fn rejected() -> ProverError {
    ProverError::Backend("settlement validation rejected".to_owned())
}

fn unavailable() -> ProverError {
    ProverError::Retryable {
        kind: RetryableProverError::Unavailable,
        message: "connect refused".to_owned(),
    }
}

fn poisoned() -> ProverError {
    ProverError::Actionable {
        failure: ActionableProverFailure::Inbound {
            entry_index: 1,
            entry_hash: B256::repeat_byte(0x11),
        },
        message: "reverted delivery".to_owned(),
    }
}

fn other_poison() -> ProverError {
    ProverError::Actionable {
        failure: ActionableProverFailure::Inbound {
            entry_index: 2,
            entry_hash: B256::repeat_byte(0x22),
        },
        message: "reverted delivery".to_owned(),
    }
}

const SIGN: Answer = Answer::Sign;
const REJECTED: Answer = Answer::Fail(rejected);
const UNAVAILABLE: Answer = Answer::Fail(unavailable);
const POISONED: Answer = Answer::Fail(poisoned);
const OTHER_POISON: Answer = Answer::Fail(other_poison);

fn proof_system(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

/// Attesters with proof systems 0x01, 0x02, ... in the given (delay, answer)
/// order, given to the quorum in reverse.
fn quorum(
    attesters: &[(u64, Answer)],
    grace_ms: u64,
) -> (AttestationQuorum, Vec<Arc<ScriptedAttester>>) {
    let provers: Vec<_> = attesters
        .iter()
        .enumerate()
        .map(|(i, &(delay, answer))| {
            ScriptedAttester::new(u8::try_from(i + 1).unwrap(), delay, answer)
        })
        .collect();
    let members = provers
        .iter()
        .enumerate()
        .rev()
        .map(|(i, prover)| QuorumMember {
            proof_system: proof_system(u8::try_from(i + 1).unwrap()),
            attester: prover.key.address(),
            prover: Arc::clone(prover) as Arc<dyn Prover>,
        })
        .collect();
    (
        AttestationQuorum::new(members, Duration::from_millis(grace_ms)).unwrap(),
        provers,
    )
}

/// Every member registered, with `threshold`.
fn registered(provers: &[Arc<ScriptedAttester>], threshold: usize) -> QuorumRegistration {
    QuorumRegistration::new(
        threshold,
        provers
            .iter()
            .map(|p| Some(vkey_of(p.key.address())))
            .collect(),
    )
    .unwrap()
}

fn context() -> ProvingContext {
    ProvingContext {
        rollup_id: 7,
        ..Default::default()
    }
}

/// A 2 s proof budget, so retryable attesters keep retrying well past every
/// scripted delay.
fn timing() -> RollupTiming {
    RollupTiming::new(12_000, 2_000, 2_000, 100)
}

async fn attest_with(
    quorum: &AttestationQuorum,
    registration: QuorumRegistration,
) -> Result<Vec<Attestation>, ProverError> {
    AttestationRound::Quorum {
        quorum,
        registration,
    }
    .attest(&context(), timing(), BundleTarget::NextBlock)
    .await
}

async fn attest(
    quorum: &AttestationQuorum,
    provers: &[Arc<ScriptedAttester>],
    threshold: usize,
) -> Result<Vec<Attestation>, ProverError> {
    attest_with(quorum, registered(provers, threshold)).await
}

fn systems(attestations: &[Attestation]) -> Vec<Address> {
    attestations.iter().map(|a| a.proof_system).collect()
}

/// `batch` as the composer settles it with `attestations`.
fn settled(attestations: Vec<Attestation>) -> EvmBatch {
    let mut batch = context().batch;
    assign_proof_systems(
        &mut batch,
        7,
        attestations.iter().map(|a| a.proof_system).collect(),
    );
    batch.proofs = attestations.into_iter().map(|a| a.proof).collect();
    batch
}

#[test]
fn new_sorts_members_and_rejects_ambiguous_sets() {
    let (quorum, _) = quorum(&[(0, SIGN), (0, SIGN), (0, SIGN)], 0);
    assert_eq!(
        quorum.proof_systems().collect::<Vec<_>>(),
        vec![proof_system(1), proof_system(2), proof_system(3)],
        "members are ordered as the registry requires batch.proofSystems"
    );

    let prover = ScriptedAttester::new(1, 0, SIGN) as Arc<dyn Prover>;
    let member = |ps, attester| QuorumMember {
        proof_system: ps,
        attester,
        prover: Arc::clone(&prover),
    };
    let signer = Address::repeat_byte(0xee);
    assert_eq!(
        AttestationQuorum::new(Vec::new(), Duration::ZERO).unwrap_err(),
        QuorumConfigError::Empty
    );
    assert_eq!(
        AttestationQuorum::new(
            (1..=17).map(|b| member(proof_system(b), signer)).collect(),
            Duration::ZERO
        )
        .unwrap_err(),
        QuorumConfigError::TooMany { count: 17 }
    );
    assert_eq!(
        AttestationQuorum::new(
            vec![
                member(proof_system(1), signer),
                member(Address::ZERO, signer)
            ],
            Duration::ZERO
        )
        .unwrap_err(),
        QuorumConfigError::ZeroProofSystem { index: 1 }
    );
    assert_eq!(
        AttestationQuorum::new(vec![member(proof_system(1), Address::ZERO)], Duration::ZERO)
            .unwrap_err(),
        QuorumConfigError::ZeroAttester { index: 0 }
    );
    assert_eq!(
        AttestationQuorum::new(
            vec![
                member(proof_system(2), signer),
                member(proof_system(2), signer)
            ],
            Duration::ZERO
        )
        .unwrap_err(),
        QuorumConfigError::DuplicateProofSystem {
            proof_system: proof_system(2)
        }
    );
}

#[tokio::test(start_paused = true)]
async fn each_attester_proves_a_batch_naming_only_its_proof_system() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, SIGN)], 0);

    attest(&quorum, &provers, 2).await.unwrap();

    for (i, prover) in provers.iter().enumerate() {
        let seen = prover.seen.lock().unwrap().clone().unwrap();
        assert_eq!(
            seen.batch.proofSystems,
            vec![proof_system(u8::try_from(i + 1).unwrap())]
        );
        assert_eq!(seen.batch.rollupIdsWithProofSystems.len(), 1);
        assert_eq!(seen.batch.rollupIdsWithProofSystems[0].rollupId, 7);
        assert_eq!(
            seen.batch.rollupIdsWithProofSystems[0].proofSystemIndexes,
            vec![0]
        );
    }
}

/// Each proof, signed over its single-attester batch, is valid in the batch
/// that lists every attester: the contract's per-proof-system digest folds
/// only that proof system's vkey.
#[tokio::test(start_paused = true)]
async fn every_proof_verifies_against_the_settled_batch() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, SIGN), (0, SIGN)], 0);

    let batch = settled(attest(&quorum, &provers, 3).await.unwrap());

    assert_eq!(batch.proofSystems.len(), 3);
    for (index, prover) in provers.iter().enumerate() {
        let digests = public_inputs_hashes(&batch, vkey_of(prover.key.address())).unwrap();
        verify_proof(&batch.proofs[index], digests[index], prover.key.address()).unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn attesters_answering_within_the_grace_period_are_all_included() {
    let (quorum, provers) = quorum(&[(300, SIGN), (10, SIGN), (50, SIGN), (20, SIGN)], 100);

    let attestations = attest(&quorum, &provers, 2).await.unwrap();

    assert_eq!(
        systems(&attestations),
        vec![proof_system(2), proof_system(3), proof_system(4)],
        "the threshold is met at 20 ms; 0x03 answers within the grace period, 0x01 after it"
    );
}

#[tokio::test(start_paused = true)]
async fn the_grace_period_runs_from_the_threshold_not_from_each_arrival() {
    // Met at 10 ms, so the window closes at 110 ms however many arrive in it.
    let (quorum, provers) = quorum(&[(10, SIGN), (60, SIGN), (105, SIGN), (115, SIGN)], 100);

    let started = Instant::now();
    let attestations = attest(&quorum, &provers, 1).await.unwrap();

    assert_eq!(
        systems(&attestations),
        vec![proof_system(1), proof_system(2), proof_system(3)]
    );
    assert_eq!(started.elapsed(), Duration::from_millis(110));
}

#[tokio::test(start_paused = true)]
async fn attesters_still_running_after_the_grace_period_are_cancelled() {
    // The slow attester answers well within the 2 s proof budget, so only
    // cancellation keeps it from finishing.
    let (quorum, provers) = quorum(&[(10, SIGN), (1_000, SIGN)], 50);

    let attestations = attest(&quorum, &provers, 1).await.unwrap();
    tokio::time::sleep(Duration::from_secs(10)).await;

    assert_eq!(systems(&attestations), vec![proof_system(1)]);
    assert!(
        !provers[1].finished.load(Ordering::SeqCst),
        "the slow attester's call is aborted, not left running"
    );
}

#[tokio::test(start_paused = true)]
async fn a_slow_attester_does_not_delay_a_met_threshold_beyond_the_grace_period() {
    let (quorum, provers) = quorum(&[(10, SIGN), (20, SIGN), (60_000, SIGN)], 100);

    let started = Instant::now();
    let attestations = attest(&quorum, &provers, 2).await.unwrap();

    assert_eq!(
        systems(&attestations),
        vec![proof_system(1), proof_system(2)]
    );
    assert!(started.elapsed() <= Duration::from_millis(120));
}

#[tokio::test(start_paused = true)]
async fn a_lost_quorum_is_reported_without_waiting_for_slow_attesters() {
    // 2 of 3: two refusals lose the quorum, and the slow attester alone can
    // neither meet it nor confirm an eviction.
    let (quorum, provers) = quorum(&[(10, REJECTED), (10, REJECTED), (60_000, SIGN)], 0);

    let started = Instant::now();
    let error = attest(&quorum, &provers, 2).await.unwrap_err();

    assert!(matches!(error, ProverError::Backend(_)), "{error:?}");
    assert_eq!(started.elapsed(), Duration::from_millis(10));
}

#[tokio::test(start_paused = true)]
async fn invalid_proofs_are_not_counted() {
    for answer in [
        Answer::SignElsewhere,
        Answer::SignHighS,
        Answer::SignRawRecovery,
        Answer::Truncated,
    ] {
        let (quorum, provers) = quorum(&[(0, SIGN), (0, answer), (0, SIGN)], 0);

        let attestations = attest(&quorum, &provers, 2).await.unwrap();

        assert_eq!(
            systems(&attestations),
            vec![proof_system(1), proof_system(3)],
            "{answer:?} must not reach the batch, where it would revert every proof"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_proof_from_another_key_is_not_counted() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, SIGN)], 0);
    let mut members = quorum.members.clone();
    members[1].attester = Address::repeat_byte(0xee);
    let quorum = AttestationQuorum::new(members, Duration::ZERO).unwrap();

    let attestations = attest_with(&quorum, registered(&provers, 1)).await.unwrap();

    assert_eq!(systems(&attestations), vec![proof_system(1)]);
}

#[tokio::test(start_paused = true)]
async fn attesters_sitting_out_are_not_asked_and_do_not_count_towards_blocking() {
    let (quorum, provers) = quorum(&[(0, POISONED), (0, SIGN), (0, SIGN)], 0);
    let mut vkeys = registered(&provers, 2).vkeys;
    vkeys[2] = None;
    let registration = QuorumRegistration::new(2, vkeys).unwrap();

    // Two usable, both needed: one report is enough to block, so it evicts.
    let error = attest_with(&quorum, registration).await.unwrap_err();

    assert!(error.actionable_failure().is_some(), "{error:?}");
    assert!(provers[2].seen.lock().unwrap().is_none());
}

#[test]
fn a_threshold_beyond_the_usable_attesters_is_refused() {
    assert_eq!(
        QuorumRegistration::new(2, vec![None, Some(B256::repeat_byte(1))]).unwrap_err(),
        RegistrationError::Unreachable {
            threshold: 2,
            usable: 1,
            configured: 2,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn an_unreachable_threshold_reports_every_failure() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, REJECTED), (0, REJECTED)], 0);

    let error = attest(&quorum, &provers, 2).await.unwrap_err();

    let ProverError::Backend(message) = error else {
        panic!("expected a backend error, got {error:?}");
    };
    assert!(
        message.contains("1 of 3 attesters proved the window, 2 required"),
        "{message}"
    );
    assert!(
        message.contains(&proof_system(2).to_string())
            && message.contains(&proof_system(3).to_string())
    );
}

#[tokio::test(start_paused = true)]
async fn only_retryable_failures_keep_the_window_retryable() {
    let (quorum, provers) = quorum(&[(0, UNAVAILABLE), (0, UNAVAILABLE), (0, SIGN)], 0);

    let error = attest(&quorum, &provers, 2).await.unwrap_err();

    assert_eq!(
        error.retryable_kind(),
        Some(RetryableProverError::Unavailable),
        "{error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn one_non_retryable_failure_makes_the_window_a_backend_error() {
    let (quorum, provers) = quorum(&[(0, UNAVAILABLE), (0, REJECTED), (0, SIGN)], 0);

    let error = attest(&quorum, &provers, 2).await.unwrap_err();

    assert!(matches!(error, ProverError::Backend(_)), "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn a_panicking_attester_counts_as_that_attesters_failure() {
    let (quorum, provers) = quorum(&[(0, Answer::Panic), (0, SIGN), (0, SIGN)], 0);

    let attestations = attest(&quorum, &provers, 2).await.unwrap();
    assert_eq!(
        systems(&attestations),
        vec![proof_system(2), proof_system(3)]
    );

    let error = attest(&quorum, &provers, 3).await.unwrap_err();
    let ProverError::Backend(message) = error else {
        panic!("expected a backend error, got {error:?}");
    };
    assert!(
        message.contains(&format!(
            "{}: prover backend: the attester panicked",
            proof_system(1)
        )),
        "{message}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_candidate_reported_by_a_blocking_set_is_evicted() {
    // 2 of 3: two attesters are enough to block the quorum, so two must agree.
    let (quorum, provers) = quorum(&[(0, POISONED), (10, POISONED), (0, SIGN)], 0);

    let error = attest(&quorum, &provers, 2).await.unwrap_err();

    assert_eq!(
        error.actionable_failure(),
        Some(ActionableProverFailure::Inbound {
            entry_index: 1,
            entry_hash: B256::repeat_byte(0x11),
        })
    );
}

#[tokio::test(start_paused = true)]
async fn one_attester_alone_cannot_evict_a_candidate() {
    let (quorum, provers) = quorum(&[(0, POISONED), (0, REJECTED), (0, SIGN)], 0);

    let error = attest(&quorum, &provers, 2).await.unwrap_err();

    assert_eq!(error.actionable_failure(), None, "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn disagreeing_attesters_evict_nothing() {
    let (quorum, provers) = quorum(&[(0, POISONED), (0, OTHER_POISON), (0, SIGN)], 0);

    let error = attest(&quorum, &provers, 2).await.unwrap_err();

    assert_eq!(error.actionable_failure(), None, "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn the_verdict_waits_for_attesters_that_could_still_confirm_an_eviction() {
    // 3 of 4: once 0x01 and 0x02 fail the quorum is lost, but eviction needs
    // two matching reports and 0x03 is still running.
    let (quorum, provers) = quorum(
        &[(0, POISONED), (0, REJECTED), (100, POISONED), (0, SIGN)],
        0,
    );

    let error = attest(&quorum, &provers, 3).await.unwrap_err();

    assert!(error.actionable_failure().is_some(), "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn the_verdict_waits_for_a_first_report_when_the_first_failure_is_not_one() {
    // 2 of 2: one report blocks, so the refusal that loses the quorum must not
    // settle the verdict while 0x02 may still name the candidate.
    let (quorum, provers) = quorum(&[(0, REJECTED), (100, POISONED)], 0);

    let error = attest(&quorum, &provers, 2).await.unwrap_err();

    assert!(error.actionable_failure().is_some(), "{error:?}");
}

fn word(value: u64) -> String {
    format!("{:#066x}", U256::from(value))
}

fn manager() -> Address {
    Address::repeat_byte(0x99)
}

/// Answers the batched reads in their order: `threshold()`, then each member's
/// `verificationKey()` and `signer()`.
fn answering(threshold: u64, members: &[(Answered, Answered)]) -> RootProvider {
    let asserter = Asserter::new();
    asserter.push_success(&word(threshold));
    for (vkey, signer) in members {
        for answer in [vkey, signer] {
            match answer {
                Answered::Word(value) => asserter.push_success(value),
                Answered::Revert => asserter.push_failure_msg("execution reverted"),
            }
        }
    }
    ProviderBuilder::default().connect_mocked_client(asserter)
}

enum Answered {
    Word(B256),
    Revert,
}

/// `prover`'s registration as the manager and its proof system report it.
fn answers_for(prover: &ScriptedAttester) -> (Answered, Answered) {
    (
        Answered::Word(vkey_of(prover.key.address())),
        Answered::Word(prover.key.address().into_word()),
    )
}

#[tokio::test]
async fn registration_reads_the_threshold_vkeys_and_signers() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, SIGN)], 0);
    let provider = answering(2, &[answers_for(&provers[0]), answers_for(&provers[1])]);

    let registration = quorum.registration_at(&provider, manager()).await.unwrap();

    assert_eq!(registration, registered(&provers, 2));
}

#[tokio::test]
async fn a_member_whose_proof_system_changed_signer_sits_out() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, SIGN)], 0);
    let provider = answering(
        1,
        &[
            (
                Answered::Word(vkey_of(provers[0].key.address())),
                Answered::Word(Address::repeat_byte(0xee).into_word()),
            ),
            answers_for(&provers[1]),
        ],
    );

    let registration = quorum.registration_at(&provider, manager()).await.unwrap();

    let mut expected = registered(&provers, 1).vkeys;
    expected[0] = None;
    assert_eq!(registration, QuorumRegistration::new(1, expected).unwrap());
}

/// A proof system the manager registers but whose `signer()` cannot be read,
/// such as one that is not an `ECDSAProofSystem`, costs the batch only that
/// member.
#[tokio::test]
async fn a_member_whose_registration_cannot_be_read_sits_out() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, SIGN), (0, SIGN)], 0);
    let provider = answering(
        2,
        &[
            (
                Answered::Word(vkey_of(provers[0].key.address())),
                Answered::Revert,
            ),
            (Answered::Revert, Answered::Revert),
            answers_for(&provers[2]),
        ],
    );

    let error = quorum
        .registration_at(&provider, manager())
        .await
        .unwrap_err();

    assert_eq!(
        error,
        RegistrationError::Unreachable {
            threshold: 2,
            usable: 1,
            configured: 3,
        }
    );

    let provider = answering(
        1,
        &[
            (
                Answered::Word(vkey_of(provers[0].key.address())),
                Answered::Revert,
            ),
            (Answered::Revert, Answered::Revert),
            answers_for(&provers[2]),
        ],
    );
    let registration = quorum.registration_at(&provider, manager()).await.unwrap();
    let mut expected = registered(&provers, 1).vkeys;
    expected[0] = None;
    expected[1] = None;
    assert_eq!(registration, QuorumRegistration::new(1, expected).unwrap());
}

#[tokio::test]
async fn an_unregistered_member_sits_out() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, SIGN)], 0);
    let provider = answering(
        1,
        &[
            answers_for(&provers[0]),
            (Answered::Word(B256::ZERO), Answered::Revert),
        ],
    );

    let registration = quorum.registration_at(&provider, manager()).await.unwrap();

    let mut expected = registered(&provers, 1).vkeys;
    expected[1] = None;
    assert_eq!(registration, QuorumRegistration::new(1, expected).unwrap());
}

#[tokio::test]
async fn a_failed_threshold_read_is_reported() {
    let (quorum, provers) = quorum(&[(0, SIGN), (0, SIGN)], 0);
    let asserter = Asserter::new();
    asserter.push_failure_msg("connection reset");
    for prover in &provers {
        asserter.push_success(&vkey_of(prover.key.address()));
        asserter.push_success(&prover.key.address().into_word());
    }
    let provider = ProviderBuilder::default().connect_mocked_client(asserter);

    let error = quorum
        .registration_at(&provider, manager())
        .await
        .unwrap_err();

    let RegistrationError::Read { contract, call, .. } = error else {
        panic!("expected a read error, got {error:?}");
    };
    assert_eq!((contract, call.as_str()), (manager(), "threshold()"));
}

/// One attester keeps the pre-quorum flow: L1 judges its proof, so the
/// composer neither reads the registration nor checks the proof itself.
#[tokio::test(start_paused = true)]
async fn a_lone_attester_proof_goes_to_l1_unchecked() {
    let (quorum, _) = quorum(&[(0, Answer::SignElsewhere)], 0);
    // Any read would fail on the empty mock.
    let provider = ProviderBuilder::default().connect_mocked_client(Asserter::new());

    let round = quorum
        .round(&provider, Address::repeat_byte(0x98), 7)
        .await
        .unwrap();
    let attestations = round
        .attest(&context(), timing(), BundleTarget::NextBlock)
        .await
        .unwrap();

    assert_eq!(systems(&attestations), vec![proof_system(1)]);
}
