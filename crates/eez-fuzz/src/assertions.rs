//! Output oracles for the composer fuzzer — *what* makes a `Composition` correct.
//!
//! # The core principle: settled state is the only ground truth
//!
//! Cross-chain composition is OPTIMISTIC. During source simulation the composer
//! *synthesizes* what a cross-chain call returns and feeds it back so execution
//! continues; the composition then carries an execution table the settlement
//! side must honor. So the return the composer reports is a CLAIM — and under the
//! mock prover (which does not bind execution) a self-consistent table with a
//! fabricated return + a matching rolling hash will ratify. The one thing that
//! cannot be faked is the destination contract's state after the table is
//! actually replayed. **Returns are claims; settled state is fact. Assert the fact.**
//!
//! # Oracles implemented here ([`World::assert_executes_and_ratifies`])
//!
//! 1. **Ratify** — replay the composition's own production payloads
//!    (`EEZL2.executeIncomingCrossChainCall` / `loadExecutionTable`) against the
//!    real frozen bytecode. No-revert proves the rolling hash + call counts are
//!    internally consistent AND exercises the overlay push/pop pairing in
//!    `SessionInspector` (a broken pairing surfaces here as a revert /
//!    `RollingHashMismatch`). The contracts are the oracle: they re-check exactly
//!    what the composer committed to.
//! 2. **Settled state** — read the destination's slot-0 after the replay and
//!    assert it equals the value the input INTENDED to set. This is the ground
//!    truth a mock prover cannot fake, and the only check that catches "the
//!    cross-chain call never actually executed" (plus wrong-target,
//!    force-reverted, mis-classified-as-failed). The expected value comes from the
//!    generator's own `set(x) -> x` model (handed back WITH the input), so the
//!    oracle is not circular with the composer.
//!
//! # Complementary oracles (elsewhere, by design)
//!
//! - **Determinism** — `compose` is read-only against the frozen world, so the
//!   same input must yield a byte-identical `Composition`; any nondeterminism
//!   (e.g. leaked HashMap order) is a bug. Lives as `compose_is_deterministic`.
//! - **Fixture invariants** — the world serves the deployed bytecode and the
//!   proxy registered. Baked into `boot_*` via `assert_world_has_code`, so they
//!   run on every boot rather than in one dedicated test.
//! - **No-panic** — the universal one: `compose` *errors* (EmptyCalls, decode, …)
//!   are valid rejections; a panic is the finding. Enforced by the fuzz target.
//!
//! # What these oracles do NOT reach (keeping scope honest)
//!
//! - **Liveness / strategy** (the issue-#20 DDoS): a griefed bundle correctly
//!   reverts — no safety invariant is violated. Needs a multi-tx sequence + an
//!   "honest effect lands despite an adversarial sibling" predicate.
//! - **Proof binding**: under the mock prover, ratification validates
//!   execution-table correctness, not signature soundness. Needs the real prover.
//! - **L1-side settled effect**: the source batch needs the rollup's ECDSA proof
//!   over `SIGNER` (no key in the fixture), so the L1 side is a structural check;
//!   the L2 replay is what exercises the overlay pairing + settled state today.
//!
//! # Cost hierarchy (cheap first, run broadly)
//!
//! no-panic / determinism (free, every input) < ratify (one replay) < settled
//! state (one replay + a storage read; the decisive correctness check).

use alloy_primitives::{Address, TxKind, U256};
use eez_evm::EvmProtocol;
use eez_protocol::Composition;
use reth_provider::test_utils::MockEthProvider;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::StateProviderFactory;
use revm::context::ContextTr;
use revm::context::TxEnv;
use revm::context::result::ExecutionResult;
use revm::database::CacheDB;
use revm::state::AccountInfo;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use crate::{SYSTEM_ADDR, World};

impl World {
    /// Execute + ratify oracle: replay the
    /// composition's OWN production payloads against the real frozen bytecode
    /// and assert they apply without revert.
    ///
    /// - L2 target (the real overlay oracle): the arriving system tx runs
    ///   `EEZL2.executeIncomingCrossChainCall`, which loads the table and
    ///   checks the rolling hash + call counts against the real bytecode — a
    ///   no-revert here ratifies the target. A broken overlay push/pop
    ///   pairing in `SessionInspector` makes the source sim record wrong
    ///   inner state/return-data, which surfaces here as a revert or
    ///   `RollingHashMismatch`.
    /// - L1 source: the composer emits the batch WITHOUT proofs — the
    ///   downstream poster signs it (the rollup's ECDSA proof over `SIGNER`)
    ///   before submitting, and `postVerifyAndExecuteOrSaveExecutionsFromBatch`
    ///   reverts `InvalidProofSystemConfig` on an unsigned batch
    ///   (`EEZ.sol:435`). That signature is downstream of the composer's
    ///   overlay logic (and `SIGNER` has no key here), so the L1 side is a
    ///   structural check; the L2 replay above is the ratification that
    ///   exercises the overlay pairing.
    /// - SETTLED state: when `expected_settled` is `Some`, after replaying the
    ///   inbound we read `value_l2`'s slot-0 storage and assert it equals the
    ///   value the generator INTENDED to set. This is the ground truth a mock
    ///   prover can't fake — the composer's return data / rolling hash can look
    ///   right while the destination contract never actually changed. The
    ///   predicted value comes from the generator's own `set(x) -> x` model, so
    ///   the oracle is not circular with the composer.
    pub fn assert_executes_and_ratifies(
        &self,
        comp: &Composition<EvmProtocol>,
        expected_settled: Option<U256>,
    ) {
        for target in &comp.targets {
            if let Some(inbound) = &target.inbound_payload {
                let (r, settled) = replay_inbound(
                    &self.l2_provider,
                    self.eezl2,
                    inbound.clone(),
                    target.inbound_value,
                    self.value_l2,
                );
                assert!(r.is_success(), "L2 inbound execute+ratify reverted: {r:?}");
                if let Some(expected) = expected_settled {
                    assert_eq!(
                        settled, expected,
                        "settled {} slot-0 = {settled}, expected {expected} — the cross-chain \
                         call did not really run (return data can look right past a mock prover)",
                        self.value_l2,
                    );
                }
            } else {
                let r = replay_once(
                    &self.l2_provider,
                    SYSTEM_ADDR,
                    self.eezl2,
                    target.load_table_payload.clone(),
                    U256::ZERO,
                );
                assert!(r.is_success(), "L2 loadExecutionTable reverted: {r:?}");
            }
        }

        assert!(
            comp.source.entry_payload.len() > 4,
            "composer must emit a non-empty source entry-chain payload",
        );
    }
}

/// Replay the inbound system tx (`executeIncomingCrossChainCall`) against the
/// frozen L2 state and read `settle_target`'s slot-0 storage from the committed
/// DB — the actual destination-contract effect, not the composer's claim.
pub fn replay_inbound(
    provider: &MockEthProvider,
    eezl2: Address,
    inbound: Vec<u8>,
    value: U256,
    settle_target: Address,
) -> (ExecutionResult, U256) {
    let sp = provider.latest().expect("latest state");
    let mut db = CacheDB::new(StateProviderDatabase::new(sp));
    db.insert_account_info(
        SYSTEM_ADDR,
        AccountInfo {
            balance: U256::from(10u128).pow(U256::from(24u8)),
            nonce: 0,
            ..Default::default()
        },
    );
    let mut evm = Context::mainnet().with_db(db).build_mainnet();
    let result = evm
        .transact_commit(TxEnv {
            caller: SYSTEM_ADDR,
            kind: TxKind::Call(eezl2),
            data: inbound.into(),
            gas_limit: 16_000_000,
            nonce: 0,
            chain_id: Some(1),
            value,
            ..Default::default()
        })
        .expect("evm transact");
    let settled = evm
        .db()
        .cache
        .accounts
        .get(&settle_target)
        .and_then(|a| a.storage.get(&U256::ZERO).copied())
        .unwrap_or(U256::ZERO);
    (result, settled)
}

/// Apply one call tx against a frozen provider's state in revm and return the
/// result. The caller is overlaid as a funded nonce-0 EOA so system/deployer
/// senders need no prior funding or nonce bookkeeping.
pub fn replay_once(
    provider: &MockEthProvider,
    caller: Address,
    to: Address,
    data: Vec<u8>,
    value: U256,
) -> ExecutionResult {
    let sp = provider.latest().expect("latest state");
    let mut db = CacheDB::new(StateProviderDatabase::new(sp));
    db.insert_account_info(
        caller,
        AccountInfo {
            balance: U256::from(10u128).pow(U256::from(24u8)),
            nonce: 0,
            ..Default::default()
        },
    );
    let mut evm = Context::mainnet().with_db(db).build_mainnet();
    evm.transact_commit(TxEnv {
        caller,
        kind: TxKind::Call(to),
        data: data.into(),
        gas_limit: 16_000_000,
        nonce: 0,
        chain_id: Some(1),
        value,
        ..Default::default()
    })
    .expect("evm transact")
}
