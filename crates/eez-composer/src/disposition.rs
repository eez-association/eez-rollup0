//! Recovery's decision on a batch's transactions once L1 decides.
//!
//! The optimistic ledger keeps a [`BatchTx`] per transaction, never its body.
//! Recovery decides a [`TxDisposition`] for each and hands the composer a
//! [`BatchDisposition`]; the composer holds the bodies and carries the
//! decision out on its held pool.

use alloy_primitives::{Address, B256, TxHash};

use crate::ingress::Direction;

/// A batch's transaction as the ledger keeps it: everything recovery decides
/// on, without the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchTx {
    /// Hash of the signed envelope.
    pub hash: TxHash,
    /// Recovered sender.
    pub sender: Address,
    /// Cross-chain axis; with `sender` it names the tx's nonce chain.
    pub direction: Direction,
    /// The tx's nonce in that chain.
    pub nonce: u64,
}

/// What becomes of one transaction of a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxDisposition {
    /// Its nonce is spent: free its reservation; it never returns to the pool.
    Release,
    /// Back to the front of the pool with no attempt counted.
    Requeue,
    /// Back to the front of the pool with one failed attempt counted; at
    /// [`MAX_BUNDLE_ATTEMPTS`](crate::composer::MAX_BUNDLE_ATTEMPTS) it and its
    /// nonce suffix are evicted instead.
    Charge,
    /// The ledger no longer holds it: its body goes and its nonce reservation
    /// stays held.
    Forget,
}

/// Recovery's decision on the transactions of one batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BatchDisposition {
    /// Hash of the batch's postBatch L1 tx.
    pub batch_id: B256,
    /// Height of the Sync block the txs come back from. The composer refuses
    /// them there, so reth cannot mine one as a plain L2 tx, and reports what
    /// became of them.
    pub refuse_at: Option<u64>,
    /// Each tx's fate, in batch order.
    pub txs: Vec<(TxHash, TxDisposition)>,
    /// The ledger still holds the released txs: an L1 reorg can return them to
    /// the pool until L1 finality, so the composer keeps their bodies.
    pub keep_released: bool,
}
