//! Ethereum pooled transactions adapted to EEZ block primitives. Native system
//! envelopes have no conversion into the public pooled wire format.
use alloy_consensus::{
    Typed2718,
    transaction::{Recovered, TxHashRef},
};
use alloy_eips::{
    eip2930::AccessList,
    eip4844::{BlobTransactionValidationError, env_settings::KzgSettings},
    eip7594::BlobTransactionSidecarVariant,
    eip7702::SignedAuthorization,
};
use alloy_primitives::{Address, B256, Bytes, TxHash, TxKind, U256};
use eez_primitives::EezTxEnvelope;
use reth_primitives_traits::{InMemorySize, SignedTransaction};
use reth_transaction_pool::{
    EthBlobTransactionSidecar, EthPoolTransaction, EthPooledTransaction, PoolTransaction,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EezPooledTransaction(EthPooledTransaction<EezTxEnvelope>);
impl Typed2718 for EezPooledTransaction {
    fn ty(&self) -> u8 {
        self.0.ty()
    }
}
impl InMemorySize for EezPooledTransaction {
    fn size(&self) -> usize {
        self.0.size()
    }
}
impl PoolTransaction for EezPooledTransaction {
    type TryFromConsensusError = alloy_consensus::error::ValueError<EezTxEnvelope>;
    type Consensus = EezTxEnvelope;
    type Pooled = <EthPooledTransaction as PoolTransaction>::Pooled;
    fn clone_into_consensus(&self) -> Recovered<Self::Consensus> {
        self.0.transaction.clone()
    }
    fn consensus_ref(&self) -> Recovered<&Self::Consensus> {
        Recovered::new_unchecked(&self.0.transaction, self.sender())
    }
    fn into_consensus(self) -> Recovered<Self::Consensus> {
        self.0.transaction
    }
    fn from_pooled(tx: Recovered<Self::Pooled>) -> Self {
        let tx = EthPooledTransaction::from_pooled(tx);
        Self(EthPooledTransaction {
            transaction: tx.transaction.map(EezTxEnvelope::Ethereum),
            cost: tx.cost,
            encoded_length: tx.encoded_length,
            blob_sidecar: tx.blob_sidecar,
        })
    }
    fn hash(&self) -> &TxHash {
        self.0.transaction.tx_hash()
    }
    fn sender(&self) -> Address {
        self.0.transaction.signer()
    }
    fn sender_ref(&self) -> &Address {
        self.0.transaction.signer_ref()
    }
    fn cost(&self) -> &U256 {
        &self.0.cost
    }
    fn encoded_length(&self) -> usize {
        self.0.encoded_length
    }
}
impl EthPoolTransaction for EezPooledTransaction {
    fn take_blob(&mut self) -> EthBlobTransactionSidecar {
        if self.is_eip4844() {
            std::mem::replace(&mut self.0.blob_sidecar, EthBlobTransactionSidecar::Missing)
        } else {
            EthBlobTransactionSidecar::None
        }
    }
    fn try_into_pooled_eip4844(
        self,
        sidecar: Arc<BlobTransactionSidecarVariant>,
    ) -> Option<Recovered<Self::Pooled>> {
        let (tx, signer) = self.into_consensus().into_parts();
        let EezTxEnvelope::Ethereum(tx) = tx else {
            return None;
        };
        tx.try_into_pooled_eip4844(Arc::unwrap_or_clone(sidecar))
            .ok()
            .map(|tx| tx.with_signer(signer))
    }
    fn try_from_eip4844(
        tx: Recovered<Self::Consensus>,
        sidecar: BlobTransactionSidecarVariant,
    ) -> Option<Self> {
        let (tx, signer) = tx.into_parts();
        let EezTxEnvelope::Ethereum(tx) = tx else {
            return None;
        };
        tx.try_into_pooled_eip4844(sidecar)
            .ok()
            .map(|tx| Self::from_pooled(tx.with_signer(signer)))
    }
    fn validate_blob(
        &self,
        sidecar: &BlobTransactionSidecarVariant,
        settings: &KzgSettings,
    ) -> Result<(), BlobTransactionValidationError> {
        if let EezTxEnvelope::Ethereum(tx) = self.0.transaction.inner()
            && let Some(tx) = tx.as_eip4844()
        {
            return tx.tx().validate_blob(sidecar, settings);
        }
        Err(BlobTransactionValidationError::NotBlobTransaction(
            self.ty(),
        ))
    }
}
impl alloy_consensus::Transaction for EezPooledTransaction {
    fn chain_id(&self) -> Option<alloy_primitives::ChainId> {
        self.0.chain_id()
    }

    fn nonce(&self) -> u64 {
        self.0.nonce()
    }

    fn gas_limit(&self) -> u64 {
        self.0.gas_limit()
    }

    fn gas_price(&self) -> Option<u128> {
        self.0.gas_price()
    }

    fn max_fee_per_gas(&self) -> u128 {
        self.0.max_fee_per_gas()
    }

    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.0.max_priority_fee_per_gas()
    }

    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        self.0.max_fee_per_blob_gas()
    }

    fn priority_fee_or_price(&self) -> u128 {
        self.0.priority_fee_or_price()
    }

    fn effective_gas_price(&self, base_fee: Option<u64>) -> u128 {
        self.0.effective_gas_price(base_fee)
    }

    fn is_dynamic_fee(&self) -> bool {
        self.0.is_dynamic_fee()
    }

    fn kind(&self) -> TxKind {
        self.0.kind()
    }

    fn is_create(&self) -> bool {
        self.0.is_create()
    }

    fn value(&self) -> U256 {
        self.0.value()
    }

    fn input(&self) -> &Bytes {
        self.0.input()
    }

    fn access_list(&self) -> Option<&AccessList> {
        self.0.access_list()
    }

    fn blob_versioned_hashes(&self) -> Option<&[B256]> {
        self.0.blob_versioned_hashes()
    }

    fn authorization_list(&self) -> Option<&[SignedAuthorization]> {
        self.0.authorization_list()
    }
}
