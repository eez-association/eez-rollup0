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
//! counted: one faulty or hostile attester must not be able to stall
//! settlement for the others.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes, Signature};
use alloy_provider::RootProvider;
use eez_driver::RollupTiming;
use eez_l1::BundleTarget;
use eez_protocol::EvmBatch;
use eez_protocol::abi::RollupIdWithProofSystemsSol;
use eez_protocol::public_inputs::public_inputs_hashes;
use eez_prover::{ActionableProverFailure, Prover, ProverError, ProvingContext};
use tokio::task::{Id, JoinSet};
use tokio::time::{Instant, timeout, timeout_at};
use tracing::{Level, event};

use crate::prover_retry::prove_with_retry;

/// Upper bound on the attester set. Every proof system is a verify call the
/// batch pays for, and the gas floor the composer accepts is sized for this
/// many.
pub const MAX_ATTESTERS: usize = 16;

/// How long one batch's read of the manager's attestation settings may take.
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

    /// The manager's attestation settings for the next batch: its threshold,
    /// the verification key of every configured proof system and the signer
    /// that proof system accepts, read concurrently.
    ///
    /// Read for every batch: the owner can change the threshold or
    /// (de)register a proof system at any time, and a batch built against a
    /// stale view reverts on-chain.
    ///
    /// # Errors
    ///
    /// A message if the manager cannot be read in time, or if the attesters it
    /// registers cannot meet its threshold. Neither is a candidate's fault, so
    /// the composer requeues the window rather than charging its transactions.
    pub(crate) async fn registration(
        &self,
        provider: &RootProvider,
        registry: Address,
        rollup_id: u64,
    ) -> Result<QuorumRegistration, String> {
        let manager = match self.manager.get() {
            Some(manager) => *manager,
            None => {
                let manager = rollup_manager(provider, registry, rollup_id).await?;
                *self.manager.get_or_init(|| manager)
            }
        };
        self.registration_at(provider, manager).await
    }

    async fn registration_at(
        &self,
        provider: &RootProvider,
        manager: Address,
    ) -> Result<QuorumRegistration, String> {
        let mut reads = JoinSet::new();
        for (index, proof_system) in self.proof_systems().enumerate() {
            let provider = provider.clone();
            reads.spawn(async move {
                let vkey = IRollupAttestationReader::new(manager, &provider)
                    .verificationKey(proof_system)
                    .call()
                    .await
                    .map_err(|error| {
                        format!("read verificationKey({proof_system}) from {manager}: {error}")
                    })?;
                if vkey.is_zero() {
                    return Ok((index, vkey, Address::ZERO));
                }
                let signer = IEcdsaProofSystemReader::new(proof_system, &provider)
                    .signer()
                    .call()
                    .await
                    .map_err(|error| {
                        format!("read signer() from proof system {proof_system}: {error}")
                    })?;
                Ok::<_, String>((index, vkey, signer))
            });
        }
        let vkeys = async {
            let mut vkeys = vec![B256::ZERO; self.members.len()];
            while let Some(read) = reads.join_next().await {
                let (index, vkey, signer) =
                    read.map_err(|error| format!("registration read task: {error}"))??;
                let member = &self.members[index];
                if vkey.is_zero() {
                    event!(
                        name: "eez.composer.attestation.member_unregistered",
                        Level::WARN,
                        event_name = "eez.composer.attestation.member_unregistered",
                        proof_system = %member.proof_system,
                        %manager,
                        "proof system is not registered on the rollup manager; its attester is not asked",
                    );
                    continue;
                }
                if signer != member.attester {
                    // A proof from the configured key would revert the batch
                    // on-chain, so the member sits out until its config matches.
                    event!(
                        name: "eez.composer.attestation.signer_mismatch",
                        Level::WARN,
                        event_name = "eez.composer.attestation.signer_mismatch",
                        proof_system = %member.proof_system,
                        %signer,
                        attester = %member.attester,
                        "proof system accepts another signer; its attester is not asked",
                    );
                    continue;
                }
                vkeys[index] = vkey;
            }
            Ok::<_, String>(vkeys)
        };
        let (threshold, vkeys) = timeout(REGISTRATION_READ_TIMEOUT, async {
            tokio::join!(attestation_threshold(provider, manager), vkeys)
        })
        .await
        .map_err(|_| {
            format!("rollup manager {manager} did not answer within {REGISTRATION_READ_TIMEOUT:?}")
        })?;
        let registration = QuorumRegistration {
            threshold: threshold?,
            vkeys: vkeys?,
        };
        registration.ensure_reachable(self.members.len())?;
        Ok(registration)
    }

    /// Collect at least the registered threshold of verified proofs for `ctx`
    /// from the attesters the manager registers, each retried independently
    /// within the Composer proof budget.
    ///
    /// Once the threshold is met, collection continues for the grace period
    /// (never past the attempts' own budget) and returns every verified proof
    /// received, sorted by proof system.
    ///
    /// # Errors
    ///
    /// When the threshold can no longer be met: an actionable failure if
    /// enough registered attesters to block the quorum on their own
    /// (`registered - threshold + 1`) report the same one; otherwise a
    /// retryable error if every failure was retryable, else a backend error
    /// naming each failure.
    pub(crate) async fn attest(
        &self,
        ctx: &ProvingContext,
        registration: Option<&QuorumRegistration>,
        timing: RollupTiming,
        target: BundleTarget,
    ) -> Result<Vec<Attestation>, ProverError> {
        let Some(registration) = registration else {
            return self.attest_alone(ctx, timing, target).await;
        };
        registration
            .ensure_reachable(self.members.len())
            .map_err(ProverError::Backend)?;
        let threshold = registration.threshold;
        let registered = registration.registered();

        let mut pending = JoinSet::new();
        let mut tasks: Vec<(Id, Address)> = Vec::with_capacity(registered);
        for (member, &vkey) in self.members.iter().zip(&registration.vkeys) {
            if vkey.is_zero() {
                continue;
            }
            let prover = Arc::clone(&member.prover);
            let attester = member.attester;
            let request = single_attester_context(ctx, member.proof_system);
            let task = pending.spawn(async move {
                // The digest this attester must sign, computed by the same
                // routine the contract mirrors.
                let expected = public_inputs_hashes(&request.batch, vkey)
                    .map_err(|error| ProverError::Backend(format!("public inputs: {error}")))?
                    .first()
                    .copied()
                    .ok_or_else(|| ProverError::Backend("no public inputs hash".to_owned()))?;
                let proof = prove_with_retry(prover.as_ref(), request, timing, target).await?;
                verify_proof(&proof, expected, attester)?;
                Ok(proof)
            });
            tasks.push((task.id(), member.proof_system));
        }
        let proof_system_of = |id: Id| {
            tasks
                .iter()
                .find(|(task, _)| *task == id)
                .map_or(Address::ZERO, |(_, proof_system)| *proof_system)
        };

        let mut tally = Tally::new(registered, threshold);
        let mut grace_deadline: Option<Instant> = None;
        loop {
            let next = match grace_deadline {
                Some(deadline) => match timeout_at(deadline, pending.join_next_with_id()).await {
                    Ok(next) => next,
                    Err(_) => break,
                },
                None => pending.join_next_with_id().await,
            };
            let Some(joined) = next else {
                break;
            };
            let (proof_system, result) = match joined {
                Ok((id, result)) => (proof_system_of(id), result),
                Err(error) => (
                    proof_system_of(error.id()),
                    Err(ProverError::Backend(format!(
                        "attester task failed: {error}"
                    ))),
                ),
            };
            match result {
                Ok(proof) => tally.attestations.push(Attestation {
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
                    tally.failures.push((proof_system, error));
                }
            }

            if tally.attestations.len() >= threshold {
                grace_deadline.get_or_insert_with(|| Instant::now() + self.grace);
            } else if let Some(error) = tally.verdict(pending.len()) {
                return Err(error);
            }
        }

        if tally.attestations.len() < threshold {
            return Err(tally.verdict(0).unwrap_or_else(|| {
                ProverError::Backend("attestation quorum was not reached".to_owned())
            }));
        }

        pending.abort_all();
        let mut attestations = tally.attestations;
        attestations.sort_by_key(|a| a.proof_system);
        event!(
            name: "eez.composer.attestation.quorum_reached",
            Level::INFO,
            event_name = "eez.composer.attestation.quorum_reached",
            threshold,
            registered,
            attesters = self.members.len(),
            proofs = attestations.len(),
            "attestation quorum reached",
        );
        Ok(attestations)
    }

    /// A lone attester's proof, as before quorums: the composer has no other
    /// attester to protect from it, so the proof goes to L1 unchecked and L1
    /// is the judge.
    async fn attest_alone(
        &self,
        ctx: &ProvingContext,
        timing: RollupTiming,
        target: BundleTarget,
    ) -> Result<Vec<Attestation>, ProverError> {
        let [member] = self.members.as_slice() else {
            return Err(ProverError::Backend(format!(
                "{} attesters are configured, so a registration is required",
                self.members.len()
            )));
        };
        let request = single_attester_context(ctx, member.proof_system);
        let proof = prove_with_retry(member.prover.as_ref(), request, timing, target).await?;
        Ok(vec![Attestation {
            proof_system: member.proof_system,
            proof,
        }])
    }
}

/// The manager's attestation settings for one batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuorumRegistration {
    /// Distinct proof systems a batch must carry.
    threshold: usize,
    /// Parallel to the quorum's members; zero for a proof system the manager
    /// does not register or that no longer accepts the configured attester.
    vkeys: Vec<B256>,
}

impl QuorumRegistration {
    fn registered(&self) -> usize {
        self.vkeys.iter().filter(|v| !v.is_zero()).count()
    }

    fn ensure_reachable(&self, configured: usize) -> Result<(), String> {
        let registered = self.registered();
        if self.threshold > registered {
            return Err(format!(
                "attestation threshold {} is not reachable: the manager registers {registered} of the {configured} configured attesters",
                self.threshold
            ));
        }
        Ok(())
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

/// The rollup manager contract registered for `rollup_id` in the EEZ registry.
async fn rollup_manager(
    provider: &RootProvider,
    registry: Address,
    rollup_id: u64,
) -> Result<Address, String> {
    let manager = crate::composer::IEEZReader::new(registry, provider)
        .rollups(rollup_id)
        .call()
        .await
        .map_err(|error| format!("read rollups({rollup_id}) from registry {registry}: {error}"))?
        .rollupContract;
    if manager == Address::ZERO {
        return Err(format!(
            "rollup {rollup_id} has no manager in registry {registry}"
        ));
    }
    Ok(manager)
}

/// The number of distinct proof systems the manager currently requires.
async fn attestation_threshold(provider: &RootProvider, manager: Address) -> Result<usize, String> {
    let threshold = IRollupAttestationReader::new(manager, provider)
        .threshold()
        .call()
        .await
        .map_err(|error| format!("read threshold() from rollup manager {manager}: {error}"))?;
    // The manager accepts any batch once `threshold` proofs verify; zero still
    // needs one proof for the batch to be attested at all.
    Ok(usize::try_from(threshold).unwrap_or(usize::MAX).max(1))
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
    attesters: usize,
    threshold: usize,
    attestations: Vec<Attestation>,
    failures: Vec<(Address, ProverError)>,
}

impl Tally {
    fn new(attesters: usize, threshold: usize) -> Self {
        Self {
            attesters,
            threshold,
            attestations: Vec::with_capacity(attesters),
            failures: Vec::new(),
        }
    }

    /// The error to report once the threshold is out of reach, or `None` while
    /// `pending` attesters could still meet it or still decide an eviction.
    fn verdict(&self, pending: usize) -> Option<ProverError> {
        if self.attestations.len() + pending >= self.threshold {
            return None;
        }

        // An eviction censors a candidate, so it needs as many attesters as it
        // takes to block the quorum: fewer than that cannot outvote the rest.
        let blocking = self.attesters - self.threshold + 1;
        let mut leading: Option<(ActionableProverFailure, usize)> = None;
        for failure in self
            .failures
            .iter()
            .filter_map(|(_, e)| e.actionable_failure())
        {
            let count = self
                .failures
                .iter()
                .filter(|(_, e)| e.actionable_failure() == Some(failure))
                .count();
            if leading.is_none_or(|(_, best)| count > best) {
                leading = Some((failure, count));
            }
        }
        if let Some((failure, count)) = leading
            && count >= blocking
        {
            return Some(ProverError::Actionable {
                failure,
                message: format!(
                    "{count} of {} attesters reported this candidate, enough to block the quorum",
                    self.attesters
                ),
            });
        }
        if leading.map_or(0, |(_, count)| count) + pending >= blocking {
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
            self.attesters,
            self.threshold
        );
        if let Some(kind) = self
            .failures
            .iter()
            .map(|(_, e)| e.retryable_kind())
            .collect::<Option<Vec<_>>>()
            .and_then(|kinds| kinds.first().copied())
        {
            return Some(ProverError::Retryable { kind, message });
        }
        Some(ProverError::Backend(message))
    }
}

#[cfg(test)]
mod tests;
