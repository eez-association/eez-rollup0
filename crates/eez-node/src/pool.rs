//! L2 pool that refuses SYSTEM_ADDRESS txs. Real ones ride in Sync blocks over
//! the engine API, so this covers RPC and reth's reorg re-injection alike.

use std::{any::Any, time::SystemTime};

use alloy_eips::{eip7840::BlobParams, merge::EPOCH_SLOTS};
use alloy_primitives::Address;
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::ConfigureEvm;
use reth_node_api::{NodePrimitives, PrimitivesTy};
use reth_node_builder::{
    BuilderContext,
    components::{PoolBuilder, TxPoolBuilder, create_blob_store_with_cache},
    node::{FullNodeTypes, NodeTypes},
};
use reth_primitives_traits::SealedBlock;
use reth_transaction_pool::{
    CoinbaseTipOrdering, EthPooledTransaction, Pool, PoolTransaction, TransactionOrigin,
    TransactionValidationOutcome, TransactionValidationTaskExecutor, TransactionValidator,
    blobstore::DiskFileBlobStore,
    error::{InvalidPoolTransactionError, PoolTransactionError},
    validate::EthTransactionValidator,
};
use tracing::{Level, event};

/// reth's Ethereum pool with [`SystemAddressGate`] around its validator.
pub type EezTransactionPool<Client, S, Evm> = Pool<
    TransactionValidationTaskExecutor<
        SystemAddressGate<EthTransactionValidator<Client, EthPooledTransaction, Evm>>,
    >,
    CoinbaseTipOrdering<EthPooledTransaction>,
    S,
>;

#[derive(Debug, thiserror::Error)]
#[error("SYSTEM_ADDRESS transactions are not accepted via the mempool")]
struct SystemAddressRejected;

impl PoolTransactionError for SystemAddressRejected {
    fn is_bad_transaction(&self) -> bool {
        // Never valid from anyone, so the peer that sent it earns a penalty.
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Rejects L2 SYSTEM_ADDRESS txs; a reorg re-injects them and a Live block
/// carrying one fail-closes emission. `None` = off (node builds no system txs).
#[derive(Debug)]
pub struct SystemAddressGate<V> {
    inner: V,
    system_address: Option<Address>,
}

impl<V> SystemAddressGate<V> {
    pub const fn new(inner: V, system_address: Option<Address>) -> Self {
        Self {
            inner,
            system_address,
        }
    }
}

impl<V> SystemAddressGate<V>
where
    V: TransactionValidator,
{
    /// The pool tx carries its recovered sender, so this is a compare, not an
    /// ECDSA recovery.
    fn is_system_sender(&self, tx: &V::Transaction) -> bool {
        self.system_address == Some(tx.sender())
    }

    fn reject(tx: V::Transaction) -> TransactionValidationOutcome<V::Transaction> {
        event!(
            name: "eez.node.pool.system_tx_rejected",
            Level::WARN,
            tx_hash = %tx.hash(),
            sender = %tx.sender(),
            "rejected a SYSTEM_ADDRESS transaction offered to the L2 pool",
        );
        TransactionValidationOutcome::Invalid(
            tx,
            InvalidPoolTransactionError::other(SystemAddressRejected),
        )
    }
}

impl<V> TransactionValidator for SystemAddressGate<V>
where
    V: TransactionValidator,
{
    type Transaction = V::Transaction;
    type Block = V::Block;

    async fn validate_transaction(
        &self,
        origin: TransactionOrigin,
        transaction: Self::Transaction,
    ) -> TransactionValidationOutcome<Self::Transaction> {
        if self.is_system_sender(&transaction) {
            return Self::reject(transaction);
        }
        self.inner.validate_transaction(origin, transaction).await
    }

    async fn validate_transactions(
        &self,
        transactions: impl IntoIterator<Item = (TransactionOrigin, Self::Transaction), IntoIter: Send>
        + Send,
    ) -> Vec<TransactionValidationOutcome<Self::Transaction>> {
        // One batch keeps the inner validator on a single state provider;
        // `slots` puts the results back in input order.
        let mut slots = Vec::new();
        let mut forwarded = Vec::new();
        for (origin, tx) in transactions {
            if self.is_system_sender(&tx) {
                slots.push(Some(Self::reject(tx)));
            } else {
                slots.push(None);
                forwarded.push((origin, tx));
            }
        }
        let mut inner = self
            .inner
            .validate_transactions(forwarded)
            .await
            .into_iter();
        // One outcome per input, in input order. A short inner result would
        // silently drop txs, so panic instead (invariant 7).
        slots
            .into_iter()
            .map(|slot| {
                slot.unwrap_or_else(|| {
                    inner
                        .next()
                        .expect("inner validator returned fewer outcomes than transactions")
                })
            })
            .collect()
    }

    fn on_new_head_block(&self, new_tip_block: &SealedBlock<Self::Block>) {
        self.inner.on_new_head_block(new_tip_block);
    }
}

/// Copy of [`reth_node_ethereum::node::EthereumPoolBuilder`] that wraps the
/// Ethereum validator in [`SystemAddressGate`].
#[derive(Debug, Default, Clone, Copy)]
pub struct EezPoolBuilder {
    system_address: Option<Address>,
}

impl EezPoolBuilder {
    pub const fn new(system_address: Option<Address>) -> Self {
        Self { system_address }
    }
}

impl<Types, Node, Evm> PoolBuilder<Node, Evm> for EezPoolBuilder
where
    Types: NodeTypes<
            ChainSpec: EthereumHardforks,
            Primitives: NodePrimitives<SignedTx = TransactionSigned>,
        >,
    Node: FullNodeTypes<Types = Types>,
    Evm: ConfigureEvm<Primitives = PrimitivesTy<Types>> + Clone + 'static,
{
    type Pool = EezTransactionPool<Node::Provider, DiskFileBlobStore, Evm>;

    async fn build_pool(
        self,
        ctx: &BuilderContext<Node>,
        evm_config: Evm,
    ) -> eyre::Result<Self::Pool> {
        let pool_config = ctx.pool_config();

        let blobs_disabled = ctx.config().txpool.disable_blobs_support
            || ctx.config().txpool.blobpool_max_count == 0;

        let blob_cache_size = if let Some(blob_cache_size) = pool_config.blob_cache_size {
            Some(blob_cache_size)
        } else {
            let current_timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs();
            let blob_params = ctx
                .chain_spec()
                .blob_params_at_timestamp(current_timestamp)
                .unwrap_or_else(BlobParams::cancun);
            Some((blob_params.target_blob_count * EPOCH_SLOTS * 2) as u32)
        };

        let blob_store = create_blob_store_with_cache(ctx, blob_cache_size)?;

        let eth_validator =
            TransactionValidationTaskExecutor::eth_builder(ctx.provider().clone(), evm_config)
                .set_eip4844(!blobs_disabled)
                .kzg_settings(ctx.kzg_settings()?)
                .with_max_tx_input_bytes(ctx.config().txpool.max_tx_input_bytes)
                .with_local_transactions_config(pool_config.local_transactions_config.clone())
                .set_tx_fee_cap(ctx.config().rpc.rpc_tx_fee_cap)
                .with_max_tx_gas_limit(ctx.config().txpool.max_tx_gas_limit)
                .with_minimum_priority_fee(ctx.config().txpool.minimum_priority_fee)
                .with_additional_tasks(ctx.config().txpool.additional_validation_tasks)
                .build_with_tasks(ctx.task_executor().clone(), blob_store.clone());

        if eth_validator.validator().eip4844() {
            // KZG setup is slow, so warm it off the first-block path.
            let kzg_settings = eth_validator.validator().kzg_settings().clone();
            ctx.task_executor().spawn_blocking_task(async move {
                let _ = kzg_settings.get();
            });
        }

        let system_address = self.system_address;
        let validator = eth_validator.map(|inner| SystemAddressGate::new(inner, system_address));

        let transaction_pool = TxPoolBuilder::new(ctx)
            .with_validator(validator)
            .build_and_spawn_maintenance_task(blob_store, pool_config)?;

        event!(
            name: "eez.node.pool.ready",
            Level::INFO,
            system_address = ?system_address,
            "L2 transaction pool initialized with the SYSTEM_ADDRESS gate",
        );

        Ok(transaction_pool)
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{TxLegacy, transaction::Recovered};
    use alloy_primitives::{Signature, TxKind, U256, address};
    use reth_ethereum_primitives::Transaction;
    use reth_transaction_pool::validate::ValidTransaction;

    use super::*;

    const SYSTEM: Address = address!("1111111111111111111111111111111111111111");
    const USER: Address = address!("2222222222222222222222222222222222222222");

    #[derive(Debug)]
    struct AcceptAll;

    impl TransactionValidator for AcceptAll {
        type Transaction = EthPooledTransaction;
        type Block = reth_ethereum_primitives::Block;

        async fn validate_transaction(
            &self,
            _origin: TransactionOrigin,
            transaction: Self::Transaction,
        ) -> TransactionValidationOutcome<Self::Transaction> {
            TransactionValidationOutcome::Valid {
                balance: U256::ZERO,
                state_nonce: 0,
                bytecode_hash: None,
                transaction: ValidTransaction::Valid(transaction),
                propagate: false,
                authorities: None,
            }
        }
    }

    fn pooled_tx(sender: Address, nonce: u64) -> EthPooledTransaction {
        let tx = Transaction::Legacy(TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 1,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Default::default(),
        });
        // The sender is asserted, so the signature never has to be valid.
        let signed =
            TransactionSigned::new_unhashed(tx, Signature::new(U256::ONE, U256::ONE, false));
        EthPooledTransaction::new(Recovered::new_unchecked(signed, sender), 100)
    }

    #[tokio::test]
    async fn rejects_system_sender_and_admits_others() {
        let gate = SystemAddressGate::new(AcceptAll, Some(SYSTEM));

        let out = gate
            .validate_transaction(TransactionOrigin::External, pooled_tx(SYSTEM, 0))
            .await;
        assert!(matches!(
            out.as_invalid(),
            Some(InvalidPoolTransactionError::Other(_))
        ));

        let out = gate
            .validate_transaction(TransactionOrigin::Local, pooled_tx(USER, 0))
            .await;
        assert!(out.is_valid());
    }

    #[tokio::test]
    async fn batch_keeps_input_order() {
        let gate = SystemAddressGate::new(AcceptAll, Some(SYSTEM));
        let batch = vec![
            (TransactionOrigin::External, pooled_tx(USER, 0)),
            (TransactionOrigin::External, pooled_tx(SYSTEM, 1)),
            (TransactionOrigin::External, pooled_tx(USER, 2)),
        ];
        let hashes: Vec<_> = batch.iter().map(|(_, tx)| *tx.hash()).collect();

        let out = gate.validate_transactions(batch).await;

        assert_eq!(
            out.iter()
                .map(TransactionValidationOutcome::tx_hash)
                .collect::<Vec<_>>(),
            hashes
        );
        assert!(out[0].is_valid());
        assert!(out[1].is_invalid());
        assert!(out[2].is_valid());
    }

    #[tokio::test]
    async fn unset_system_address_is_a_pass_through() {
        let gate = SystemAddressGate::new(AcceptAll, None);
        let out = gate
            .validate_transaction(TransactionOrigin::External, pooled_tx(SYSTEM, 0))
            .await;
        assert!(out.is_valid());
    }
}
