//! Native execution-group framing shared by executors and checkpoint selection.
use crate::{EEZL2_ADDRESS, EezTxEnvelope};

/// EEZL2 table-load selector, pinned against the protocol ABI by its tests.
pub const LOAD_EXECUTION_TABLE_SELECTOR: [u8; 4] = [0xbf, 0x2e, 0xeb, 0xd3];
/// EEZL2 inbound-delivery selector, pinned against the protocol ABI by its tests.
pub const EXECUTE_INCOMING_SELECTOR: [u8; 4] = [0xc3, 0xfb, 0x5f, 0x3d];

/// The transaction body determines boundaries; request metadata never does.
/// Native loads and their immediately following ordinary transaction form one
/// group, regardless of receipt status. Inbound deliveries are one-transaction
/// groups. Unpaired ordinary transactions do not end a group.
/// Keep this cursor with the executed prefix and restore it with speculative
/// state. A proposed transition takes effect only when its transaction commits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionGroupState {
    awaiting_user: bool,
}

impl ExecutionGroupState {
    /// Propose the next cursor and whether this transaction completes a group.
    /// This classifies already decoded envelopes, not source/effect authority.
    pub fn after(self, tx: &EezTxEnvelope) -> Result<(Self, bool), &'static str> {
        match tx {
            EezTxEnvelope::Ethereum(_) => Ok(self.after_ordinary()),
            EezTxEnvelope::System(_) if self.awaiting_user => {
                Err("native table load must be followed immediately by its user transaction")
            }
            EezTxEnvelope::System(native) if native.to == EEZL2_ADDRESS => {
                if native.input.starts_with(&LOAD_EXECUTION_TABLE_SELECTOR) {
                    Ok((
                        Self {
                            awaiting_user: true,
                        },
                        false,
                    ))
                } else if native.input.starts_with(&EXECUTE_INCOMING_SELECTOR) {
                    Ok((Self::default(), true))
                } else {
                    Err("unsupported native group selector")
                }
            }
            EezTxEnvelope::System(_) => Err("unsupported native group destination"),
        }
    }

    /// The ordinary branch, also used for unsigned advisory executions. It
    /// neither validates a signature nor authorizes the paired origin identity.
    pub const fn after_ordinary(self) -> (Self, bool) {
        (
            Self {
                awaiting_user: false,
            },
            self.awaiting_user,
        )
    }

    /// A published block or effect checkpoint cannot end after an unpaired load.
    pub fn ensure_complete(self) -> Result<(), &'static str> {
        if self.awaiting_user {
            Err("block ends inside an outbound load/user group")
        } else {
            Ok(())
        }
    }
}

/// Complete native-group endpoints. Ordinary transactions outside a load/user
/// group do not authorize cross-chain effects and do not create effect checkpoints.
pub fn execution_group_end_positions(
    transactions: &[EezTxEnvelope],
) -> Result<Vec<usize>, &'static str> {
    let mut state = ExecutionGroupState::default();
    let mut positions = Vec::new();
    for (index, transaction) in transactions.iter().enumerate() {
        let (next, completes) = state.after(transaction)?;
        state = next;
        if completes {
            positions.push(index);
        }
    }
    state.ensure_complete()?;
    Ok(positions)
}
