//! Source-chain nonce reads and the stale-nonce split shared by the drain and recovery.

use std::collections::HashMap;

use alloy_primitives::Address;
use alloy_provider::Provider as _;
use reth_storage_api::StateProvider;
use tracing::{Level, event};

use super::CrossChainExecCtx;
use crate::held_pool::HeldTx;
use crate::ingress::Direction;

/// Read L1 nonces for inbound senders at `at`.
pub(super) async fn inbound_source_nonces_for_drain(
    ctx: &CrossChainExecCtx,
    rollup_id: u64,
    drained: &[HeldTx],
    at: alloy_eips::BlockId,
) -> HashMap<(Address, Direction), u64> {
    let mut source_nonces = HashMap::new();
    for tx in drained {
        let key = (tx.sender, tx.direction);
        if tx.direction != Direction::Inbound || source_nonces.contains_key(&key) {
            continue;
        }
        match ctx
            .l1_provider
            .get_transaction_count(tx.sender)
            .block_id(at)
            .await
        {
            Ok(nonce) => {
                source_nonces.insert(key, nonce);
            }
            Err(err) => event!(
                name: "eez.composer.cc_compose.source_nonce_check_failed",
                Level::WARN,
                rollup_id,
                sender = %tx.sender,
                direction = ?tx.direction,
                error = %err,
                "canonical source-chain nonce read failed",
            ),
        }
    }
    source_nonces
}

/// Read outbound nonces from the Sync block's parent state.
pub(super) fn outbound_source_nonces_for_drain(
    parent_state: &dyn StateProvider,
    rollup_id: u64,
    drained: &[HeldTx],
) -> HashMap<(Address, Direction), u64> {
    let mut source_nonces = HashMap::new();
    for tx in drained {
        let key = (tx.sender, tx.direction);
        if tx.direction != Direction::Outbound || source_nonces.contains_key(&key) {
            continue;
        }
        match parent_state.account_nonce(&tx.sender) {
            Ok(nonce) => {
                source_nonces.insert(key, nonce.unwrap_or(0));
            }
            Err(err) => event!(
                name: "eez.composer.cc_compose.source_nonce_check_failed",
                Level::WARN,
                rollup_id,
                sender = %tx.sender,
                direction = ?tx.direction,
                error = %err,
                "L2 source nonce read failed",
            ),
        }
    }
    source_nonces
}

pub(super) fn partition_stale(
    drained: Vec<HeldTx>,
    source_nonces: &HashMap<(Address, Direction), u64>,
) -> (Vec<HeldTx>, Vec<HeldTx>) {
    drained.into_iter().partition(|tx| {
        source_nonces
            .get(&(tx.sender, tx.direction))
            .is_none_or(|source_nonce| *source_nonce <= tx.nonce)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::tests::held;
    use alloy_primitives::TxHash;

    #[test]
    fn stale_partition_is_canonical_directional_and_ordered() {
        let sender = Address::repeat_byte(0xd);
        let other = Address::repeat_byte(0xe);
        let drained = vec![
            held(sender, Direction::Inbound, 1, 1),
            held(sender, Direction::Inbound, 3, 3),
            held(sender, Direction::Outbound, 2, 2),
            held(sender, Direction::Outbound, 4, 4),
            held(other, Direction::Inbound, 0, 5),
            held(sender, Direction::Inbound, 2, 6),
        ];
        let source_nonces = HashMap::from([
            ((sender, Direction::Inbound), 3),
            ((sender, Direction::Outbound), 3),
        ]);

        let (fresh, stale) = partition_stale(drained, &source_nonces);

        assert_eq!(
            fresh.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![
                TxHash::repeat_byte(3),
                TxHash::repeat_byte(4),
                TxHash::repeat_byte(5),
            ]
        );
        assert_eq!(
            stale.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![
                TxHash::repeat_byte(1),
                TxHash::repeat_byte(2),
                TxHash::repeat_byte(6),
            ]
        );
    }

    #[test]
    fn stale_partition_fails_open_without_a_source_nonce() {
        let tx = held(Address::repeat_byte(0xf), Direction::Outbound, 9, 1);
        let (fresh, stale) = partition_stale(vec![tx.clone()], &HashMap::new());
        assert_eq!(fresh[0].hash, tx.hash);
        assert!(stale.is_empty());

        let (fresh, stale) = partition_stale(Vec::new(), &HashMap::new());
        assert!(fresh.is_empty());
        assert!(stale.is_empty());
    }
}
