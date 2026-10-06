//! M-of-N attestation: collect proofs for one settlement window from several
//! independent attesters.
//!
//! Each attester is one registered proof system with its own signer. The L1
//! rollup manager accepts a batch once it carries proofs from at least
//! `threshold` distinct proof systems (`Rollup.checkProofSystemsAndGetVkeys`),
//! so decentralisation is enforced on-chain: the composer only decides how fast
//! it collects, never how many distinct signers suffice.
//!
//! An attester's digest is `keccak256(sharedPublicInput || acc)`, where `acc`
//! folds only `(rollupId, vkey)` of that proof system. It does not depend on
//! which other proof systems the batch lists or at which index, so each
//! attester is asked to prove a copy of the batch naming only its own proof
//! system, and its proof stays valid in the final batch that lists every
//! attester that answered.
//!
//! `EEZ.sol` reverts the whole batch on one invalid proof, so every proof is
//! checked here against the digest the composer computes itself before it is
//! counted, and an attester whose registration cannot be read sits the batch
//! out: one faulty or hostile attester must not be able to stall settlement
//! for the others.

use std::{
    cmp::Reverse,
    panic::AssertUnwindSafe,
    sync::{Arc, OnceLock},
    time::Duration,
};

use alloy_eips::BlockNumberOrTag;
use alloy_network::{Ethereum, Network, TransactionBuilder};
use alloy_primitives::{Address, B256, Bytes, Signature};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_client::BatchRequest;
use alloy_sol_types::SolCall;
use eez_driver::RollupTiming;
use eez_l1::BundleTarget;
use eez_protocol::{
    EvmBatch, abi::RollupIdWithProofSystemsSol, public_inputs::public_inputs_hashes,
};
use eez_prover::{Prover, ProverError, ProvingContext};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use tokio::time::{Instant, timeout_at};
use tracing::{Level, event};

use crate::prover_retry::prove_with_retry;

/// Upper bound on the attester set. Every proof system is a verify call the
/// batch pays for, and the gas floor the composer accepts is sized for this
/// many.
pub const MAX_ATTESTERS: usize = 16;

/// How long one batch's read of the manager's attestation settings may take,
/// including the one-time lookup of the manager itself.
const REGISTRATION_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// `r || s || v`, the proof `ECDSAProofSystem` verifies.
const ECDSA_PROOF_BYTES: usize = 65;

/// How long the composer keeps collecting after the threshold is met, so
/// attesters that answer shortly after the fastest ones are still recorded
/// on-chain instead of the same fastest subset being included every time.
pub const DEFAULT_ATTESTATION_GRACE: Duration = Duration::from_millis(250);

/// One registered attester: the proof system its proof is verified by, the key
/// that signs it, and the prover that produces it.
#[derive(Debug, Clone)]
pub struct QuorumMember {
    /// The `ECDSAProofSystem` contract this attester's proof is checked against.
    pub proof_system: Address,
    /// The signer that proof system accepts.
    pub attester: Address,
    /// The prover that returns this attester's proof.
    pub prover: Arc<dyn Prover>,
}

/// A proof and the proof system that verifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attestation {
    /// The proof system the proof belongs to.
    pub proof_system: Address,
    /// The proof bytes `IProofSystem.verify` accepts.
    pub proof: Bytes,
}

/// A malformed attester set.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QuorumConfigError {
    /// No attester was configured.
    #[error("at least one attester is required")]
    Empty,
    /// More attesters than [`MAX_ATTESTERS`].
    #[error("{count} attesters configured, at most {MAX_ATTESTERS} are supported")]
    TooMany {
        /// Number configured.
        count: usize,
    },
    /// A proof system address is zero.
    #[error("attester {index} has the zero proof-system address")]
    ZeroProofSystem {
        /// Position in the configured list.
        index: usize,
    },
    /// An attester address is zero.
    #[error("attester {index} has the zero signer address")]
    ZeroAttester {
        /// Position in the configured list.
        index: usize,
    },
    /// Two attesters share a proof system, which the rollup manager counts once.
    #[error("proof system {proof_system} is configured more than once")]
    DuplicateProofSystem {
        /// The repeated proof system.
        proof_system: Address,
    },
}

/// Why the manager's attestation settings for the next batch are unusable.
/// None of these is a candidate's fault, so the composer requeues the window
/// rather than charging its transactions.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RegistrationError {
    /// A read the batch cannot do without failed.
    #[error("read {call} from {contract}: {message}")]
    Read {
        /// The contract read.
        contract: Address,
        /// The view called.
        call: String,
        /// The transport or decoding failure.
        message: String,
    },
    /// The registry has no manager for the rollup.
    #[error("rollup {rollup_id} has no manager in registry {registry}")]
    NoManager {
        /// The settled rollup.
        rollup_id: u64,
        /// The registry read.
        registry: Address,
    },
    /// The L1 node did not answer the reads in time.
    #[error(
        "the L1 node did not answer the attestation settings reads within {REGISTRATION_READ_TIMEOUT:?}"
    )]
    TimedOut,
    /// Fewer attesters are usable than the manager requires.
    #[error(
        "attestation threshold {threshold} is not reachable: {usable} of the {configured} configured attesters are usable"
    )]
    Unreachable {
        /// Distinct proof systems the manager requires.
        threshold: usize,
        /// Configured attesters this batch can ask.
        usable: usize,
        /// Configured attesters.
        configured: usize,
    },
}

/// The attesters of one rollup and how their proofs are collected.
#[derive(Debug, Clone)]
pub struct AttestationQuorum {
    /// Sorted by strictly ascending proof system, the order the registry
    /// requires for `batch.proofSystems`.
    members: Vec<QuorumMember>,
    grace: Duration,
    /// The rollup's manager contract, looked up once it is registered.
    manager: OnceLock<Address>,
}

impl AttestationQuorum {
    /// Build an attester set. Members may be given in any order; they are
    /// sorted by proof system.
    ///
    /// # Errors
    ///
    /// Returns [`QuorumConfigError`] for an empty or oversized set, a zero
    /// address, or a proof system configured twice.
    pub fn new(mut members: Vec<QuorumMember>, grace: Duration) -> Result<Self, QuorumConfigError> {
        if members.is_empty() {
            return Err(QuorumConfigError::Empty);
        }
        if members.len() > MAX_ATTESTERS {
            return Err(QuorumConfigError::TooMany {
                count: members.len(),
            });
        }
        if let Some(index) = members.iter().position(|m| m.proof_system == Address::ZERO) {
            return Err(QuorumConfigError::ZeroProofSystem { index });
        }
        if let Some(index) = members.iter().position(|m| m.attester == Address::ZERO) {
            return Err(QuorumConfigError::ZeroAttester { index });
        }
        members.sort_by_key(|m| m.proof_system);
        if let Some(pair) = members
            .windows(2)
            .find(|w| w[0].proof_system == w[1].proof_system)
        {
            return Err(QuorumConfigError::DuplicateProofSystem {
                proof_system: pair[0].proof_system,
            });
        }
        Ok(Self {
            members,
            grace,
            manager: OnceLock::new(),
        })
    }

    /// The configured proof systems, strictly ascending.
    pub fn proof_systems(&self) -> impl Iterator<Item = Address> + '_ {
        self.members.iter().map(|m| m.proof_system)
    }

    /// Number of configured attesters.
    #[must_use]
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Always false: an attester set is never empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// The attesters to ask for the next batch.
    ///
    /// A lone attester keeps the pre-quorum flow and reads nothing. Otherwise
    /// the manager's threshold, and the verification key of every configured
    /// proof system and the signer it accepts, are read for every batch: the
    /// owner can change the threshold or (de)register a proof system at any
    /// time, and a batch built against a stale view reverts on-chain.
    ///
    /// # Errors
    ///
    /// [`RegistrationError`] if the manager or its threshold cannot be read in
    /// time, or if too few attesters are usable to meet the threshold. An
    /// attester whose own registration cannot be read only sits the batch out.
    pub(crate) async fn round(
        &self,
        provider: &RootProvider,
        registry: Address,
        rollup_id: u64,
    ) -> Result<AttestationRound<'_>, RegistrationError> {
        if let [member] = self.members.as_slice() {
            return Ok(AttestationRound::Alone(member));
        }
        let deadline = Instant::now() + REGISTRATION_READ_TIMEOUT;
        let registration = timeout_at(deadline, async {
            let manager = match self.manager.get() {
                Some(manager) => *manager,
                None => {
                    let manager = rollup_manager(provider, registry, rollup_id).await?;
                    *self.manager.get_or_init(|| manager)
                }
            };
            self.registration_at(provider, manager).await
        })
        .await
        .map_err(|_| RegistrationError::TimedOut)??;
        Ok(AttestationRound::Quorum {
            quorum: self,
            registration,
        })
    }

    /// Reads the threshold and every member's registration in one JSON-RPC
    /// batch.
    async fn registration_at(
        &self,
        provider: &RootProvider,
        manager: Address,
    ) -> Result<QuorumRegistration, RegistrationError> {
        let mut batch = BatchRequest::new(provider.client());
        let threshold = batch
            .add_call(
                "eth_call",
                &view(manager, &IRollupAttestationReader::thresholdCall {}),
            )
            .map_err(|error| read_error(manager, "threshold()", &error))?;
        let mut member_reads = Vec::with_capacity(self.members.len());
        for member in &self.members {
            let vkey = batch.add_call(
                "eth_call",
                &view(
                    manager,
                    &IRollupAttestationReader::verificationKeyCall {
                        proofSystem: member.proof_system,
                    },
                ),
            );
            let signer = batch.add_call(
                "eth_call",
                &view(member.proof_system, &IEcdsaProofSystemReader::signerCall {}),
            );
            member_reads.push((vkey, signer));
        }
        batch
            .send()
            .await
            .map_err(|error| read_error(manager, "the attestation settings", &error))?;

        let threshold = decode::<IRollupAttestationReader::thresholdCall>(threshold.await)
            .map_err(|message| RegistrationError::Read {
                contract: manager,
                call: "threshold()".to_owned(),
                message,
            })?;
        // The manager accepts any batch once `threshold` proofs verify; zero
        // still needs one proof for the batch to be attested at all.
        let threshold = usize::try_from(threshold).unwrap_or(usize::MAX).max(1);
        let mut vkeys = Vec::with_capacity(self.members.len());
        for (member, (vkey, signer)) in self.members.iter().zip(member_reads) {
            let vkey = match vkey {
                Ok(vkey) => decode::<IRollupAttestationReader::verificationKeyCall>(vkey.await),
                Err(error) => Err(error.to_string()),
            };
            let signer = match signer {
                Ok(signer) => decode::<IEcdsaProofSystemReader::signerCall>(signer.await),
                Err(error) => Err(error.to_string()),
            };
            vkeys.push(usable_vkey(member, manager, vkey, signer));
        }
        QuorumRegistration::new(threshold, vkeys)
    }
}

/// The attesters asked for one batch.
#[derive(Debug)]
pub(crate) enum AttestationRound<'a> {
    /// The only configured attester. The composer has no other attester to
    /// protect from it, so its proof goes to L1 unchecked and L1 is the judge.
    Alone(&'a QuorumMember),
    /// Several attesters and the manager's settings for this batch.
    Quorum {
        /// The configured attesters.
        quorum: &'a AttestationQuorum,
        /// Built by [`AttestationQuorum::round`] for exactly these members.
        registration: QuorumRegistration,
    },
}

impl AttestationRound<'_> {
    /// Collect the proofs `ctx` settles with.
    ///
    /// A quorum collects at least the registered threshold of verified proofs
    /// from the usable attesters, each retried independently within the
    /// Composer proof budget. Once the threshold is met, collection continues
    /// for the grace period (never past the attempts' own budget) and returns
    /// every verified proof received, sorted by proof system.
    ///
    /// # Errors
    ///
    /// When the threshold can no longer be met: an actionable failure if
    /// enough usable attesters to block the quorum on their own
    /// (`usable - threshold + 1`) report the same one; otherwise a retryable
    /// error if every failure was retryable, else a backend error naming each
    /// failure.
    pub(crate) async fn attest(
        &self,
        ctx: &ProvingContext,
        timing: RollupTiming,
        target: BundleTarget,
    ) -> Result<Vec<Attestation>, ProverError> {
        match self {
            Self::Alone(member) => {
                let request = single_attester_context(ctx, member.proof_system);
                let proof =
                    prove_with_retry(member.prover.as_ref(), request, timing, target).await?;
                Ok(vec![Attestation {
                    proof_system: member.proof_system,
                    proof,
                }])
            }
            Self::Quorum {
                quorum,
                registration,
            } => {
                quorum
                    .collect_proofs(ctx, registration, timing, target)
                    .await
            }
        }
    }
}

impl AttestationQuorum {
    async fn collect_proofs(
        &self,
        ctx: &ProvingContext,
        registration: &QuorumRegistration,
        timing: RollupTiming,
        target: BundleTarget,
    ) -> Result<Vec<Attestation>, ProverError> {
        let threshold = registration.threshold;
        let usable = registration.usable();
        let mut pending: FuturesUnordered<_> = self
            .members
            .iter()
            .zip(&registration.vkeys)
            .filter_map(|(member, vkey)| vkey.map(|vkey| (member, vkey)))
            .map(|(member, vkey)| async move {
                let request = single_attester_context(ctx, member.proof_system);
                let proof = AssertUnwindSafe(prove_verified(member, vkey, request, timing, target))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(ProverError::Backend("the attester panicked".to_owned()))
                    });
                (member.proof_system, proof)
            })
            .collect();

        let mut tally = Tally::new(usable, threshold);
        // The verdict ends collection as soon as the threshold is out of reach,
        // so nothing is awaited once every attester has answered.
        while tally.attestations.len() < threshold {
            if let Some(error) = tally.verdict(pending.len()) {
                return Err(error);
            }
            if let Some((proof_system, proof)) = pending.next().await {
                tally.record(proof_system, proof);
            }
        }
        let deadline = Instant::now() + self.grace;
        while let Ok(Some((proof_system, proof))) = timeout_at(deadline, pending.next()).await {
            tally.record(proof_system, proof);
        }
        // Attesters still running are cancelled here, so none outlives its window.
        drop(pending);

        let mut attestations = tally.attestations;
        attestations.sort_by_key(|a| a.proof_system);
        event!(
            name: "eez.composer.attestation.quorum_reached",
            Level::INFO,
            event_name = "eez.composer.attestation.quorum_reached",
            threshold,
            usable,
            attesters = self.members.len(),
            proofs = attestations.len(),
            "attestation quorum reached",
        );
        Ok(attestations)
    }
}

/// The manager's attestation settings for one batch, valid by construction:
/// one entry per quorum member and a threshold the usable members can meet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuorumRegistration {
    /// Distinct proof systems a batch must carry.
    threshold: usize,
    /// Parallel to the quorum's members: the verification key the manager
    /// registers for a usable member, `None` for one that sits this batch out.
    vkeys: Vec<Option<B256>>,
}

impl QuorumRegistration {
    fn new(threshold: usize, vkeys: Vec<Option<B256>>) -> Result<Self, RegistrationError> {
        let registration = Self { threshold, vkeys };
        let usable = registration.usable();
        if threshold > usable {
            return Err(RegistrationError::Unreachable {
                threshold,
                usable,
                configured: registration.vkeys.len(),
            });
        }
        Ok(registration)
    }

    fn usable(&self) -> usize {
        self.vkeys.iter().flatten().count()
    }
}

alloy_sol_types::sol! {
    /// The attestation settings of a rollup's manager contract (`Rollup.sol`).
    #[sol(rpc)]
    interface IRollupAttestationReader {
        function threshold() external view returns (uint256);
        function verificationKey(address proofSystem) external view returns (bytes32);
    }

    /// The signer an `ECDSAProofSystem` accepts.
    #[sol(rpc)]
    interface IEcdsaProofSystemReader {
        function signer() external view returns (address);
    }
}

/// The `eth_call` parameters of a view on `contract` at the latest block.
fn view(
    contract: Address,
    call: &impl SolCall,
) -> (<Ethereum as Network>::TransactionRequest, BlockNumberOrTag) {
    (
        <Ethereum as Network>::TransactionRequest::default()
            .with_to(contract)
            .with_input(call.abi_encode()),
        BlockNumberOrTag::Latest,
    )
}

fn decode<C: SolCall>(output: Result<Bytes, impl std::fmt::Display>) -> Result<C::Return, String> {
    let output = output.map_err(|error| error.to_string())?;
    C::abi_decode_returns(&output).map_err(|error| error.to_string())
}

fn read_error(contract: Address, call: &str, error: &impl std::fmt::Display) -> RegistrationError {
    RegistrationError::Read {
        contract,
        call: call.to_owned(),
        message: error.to_string(),
    }
}

/// The verification key `member` is asked under, or `None` when it sits this
/// batch out: unreadable, unregistered, or accepting another signer.
fn usable_vkey(
    member: &QuorumMember,
    manager: Address,
    vkey: Result<B256, String>,
    signer: Result<Address, String>,
) -> Option<B256> {
    let warn = |reason: &str, error: Option<&str>| {
        event!(
            name: "eez.composer.attestation.member_skipped",
            Level::WARN,
            event_name = "eez.composer.attestation.member_skipped",
            proof_system = %member.proof_system,
            attester = %member.attester,
            %manager,
            reason,
            error = error.unwrap_or_default(),
            "an attester is not asked for this batch",
        );
    };
    let vkey = match vkey {
        Ok(vkey) => vkey,
        Err(error) => {
            warn("its verification key cannot be read", Some(&error));
            return None;
        }
    };
    if vkey.is_zero() {
        warn(
            "its proof system is not registered on the rollup manager",
            None,
        );
        return None;
    }
    match signer {
        Ok(signer) if signer == member.attester => Some(vkey),
        // A proof from the configured key would revert the batch on-chain, so
        // the member sits out until its config matches.
        Ok(_) => {
            warn("its proof system accepts another signer", None);
            None
        }
        Err(error) => {
            warn("its proof system's signer cannot be read", Some(&error));
            None
        }
    }
}

/// The rollup manager contract registered for `rollup_id` in the EEZ registry.
async fn rollup_manager(
    provider: &RootProvider,
    registry: Address,
    rollup_id: u64,
) -> Result<Address, RegistrationError> {
    let manager = crate::composer::IEEZReader::new(registry, provider)
        .rollups(rollup_id)
        .call()
        .await
        .map_err(|error| read_error(registry, &format!("rollups({rollup_id})"), &error))?
        .rollupContract;
    if manager == Address::ZERO {
        return Err(RegistrationError::NoManager {
            rollup_id,
            registry,
        });
    }
    Ok(manager)
}

/// `member`'s proof for `request`, counted only if it verifies.
async fn prove_verified(
    member: &QuorumMember,
    vkey: B256,
    request: ProvingContext,
    timing: RollupTiming,
    target: BundleTarget,
) -> Result<Bytes, ProverError> {
    // The digest this attester must sign, computed by the same routine the
    // contract mirrors.
    let expected = public_inputs_hashes(&request.batch, vkey)
        .map_err(|error| ProverError::Backend(format!("public inputs: {error}")))?
        .first()
        .copied()
        .ok_or_else(|| ProverError::Backend("no public inputs hash".to_owned()))?;
    let proof = prove_with_retry(member.prover.as_ref(), request, timing, target).await?;
    verify_proof(&proof, expected, member.attester)?;
    Ok(proof)
}

/// Accept `proof` only if `ECDSAProofSystem` will: 65 bytes, `v` of 27 or 28,
/// low `s` (OpenZeppelin `ECDSA.recover` rejects the malleable high half), and
/// recovering to `attester` over the digest the composer computed.
fn verify_proof(proof: &[u8], expected: B256, attester: Address) -> Result<(), ProverError> {
    let invalid = |reason: String| ProverError::Backend(format!("invalid attestation: {reason}"));
    if proof.len() != ECDSA_PROOF_BYTES {
        return Err(invalid(format!(
            "{} bytes, expected {ECDSA_PROOF_BYTES}",
            proof.len()
        )));
    }
    let v = proof[ECDSA_PROOF_BYTES - 1];
    if v != 27 && v != 28 {
        return Err(invalid(format!("recovery byte {v}, expected 27 or 28")));
    }
    let signature = Signature::try_from(proof).map_err(|error| invalid(error.to_string()))?;
    if signature.normalize_s().is_some() {
        return Err(invalid("high-s signature".to_owned()));
    }
    let signer = signature
        .recover_address_from_prehash(&expected)
        .map_err(|error| invalid(error.to_string()))?;
    if signer != attester {
        return Err(invalid(format!(
            "recovers to {signer}, not attester {attester}, over digest {expected}"
        )));
    }
    Ok(())
}

/// Make `proof_systems`, strictly ascending, the ones `batch` settles
/// `rollup_id` with.
pub(crate) fn assign_proof_systems(
    batch: &mut EvmBatch,
    rollup_id: u64,
    proof_systems: Vec<Address>,
) {
    batch.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
        rollupId: rollup_id,
        proofSystemIndexes: (0..proof_systems.len() as u64).collect(),
    }];
    batch.proofSystems = proof_systems;
}

/// The batch as one attester is asked to prove it: naming only that
/// attester's proof system, the shape a single-proof-system signer validates.
fn single_attester_context(ctx: &ProvingContext, proof_system: Address) -> ProvingContext {
    let mut request = ctx.clone();
    assign_proof_systems(&mut request.batch, ctx.rollup_id, vec![proof_system]);
    request
}

/// Proofs and failures collected so far for one window.
struct Tally {
    /// Attesters asked for this window.
    usable: usize,
    threshold: usize,
    attestations: Vec<Attestation>,
    failures: Vec<(Address, ProverError)>,
}

impl Tally {
    fn new(usable: usize, threshold: usize) -> Self {
        Self {
            usable,
            threshold,
            attestations: Vec::with_capacity(usable),
            failures: Vec::new(),
        }
    }

    fn record(&mut self, proof_system: Address, proof: Result<Bytes, ProverError>) {
        match proof {
            Ok(proof) => self.attestations.push(Attestation {
                proof_system,
                proof,
            }),
            Err(error) => {
                event!(
                    name: "eez.composer.attestation.attester_failed",
                    Level::WARN,
                    event_name = "eez.composer.attestation.attester_failed",
                    %proof_system,
                    error = %error,
                    "an attester did not return a valid proof for this window",
                );
                self.failures.push((proof_system, error));
            }
        }
    }

    /// The error to report once the threshold is out of reach, or `None` while
    /// `pending` attesters could still meet it or still decide an eviction.
    /// Below the threshold with nothing pending, always an error.
    fn verdict(&self, pending: usize) -> Option<ProverError> {
        if self.attestations.len() + pending >= self.threshold {
            return None;
        }

        // An eviction censors a candidate, so it needs as many attesters as it
        // takes to block the quorum: fewer than that cannot outvote the rest.
        let blocking = self.usable - self.threshold + 1;
        // The most reported failure; on a tie, the one reported first.
        let leading = self
            .failures
            .iter()
            .enumerate()
            .filter_map(|(index, (_, e))| e.actionable_failure().map(|failure| (index, failure)))
            .map(|(index, failure)| {
                let count = self
                    .failures
                    .iter()
                    .filter(|(_, e)| e.actionable_failure() == Some(failure))
                    .count();
                (failure, count, index)
            })
            .max_by_key(|&(_, count, index)| (count, Reverse(index)));
        if let Some((failure, count, _)) = leading
            && count >= blocking
        {
            return Some(ProverError::Actionable {
                failure,
                message: format!(
                    "{count} of {} attesters reported this candidate, enough to block the quorum",
                    self.usable
                ),
            });
        }
        if leading.map_or(0, |(_, count, _)| count) + pending >= blocking {
            return None;
        }

        let summary = self
            .failures
            .iter()
            .map(|(ps, e)| format!("{ps}: {e}"))
            .collect::<Vec<_>>()
            .join("; ");
        let message = format!(
            "{} of {} attesters proved the window, {} required: {summary}",
            self.attestations.len(),
            self.usable,
            self.threshold
        );
        if self
            .failures
            .iter()
            .all(|(_, e)| e.retryable_kind().is_some())
            && let Some(kind) = self.failures.first().and_then(|(_, e)| e.retryable_kind())
        {
            return Some(ProverError::Retryable { kind, message });
        }
        Some(ProverError::Backend(message))
    }
}

#[cfg(test)]
mod tests;
