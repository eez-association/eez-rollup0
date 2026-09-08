//! EEZ's unsigned system transactions and otherwise unchanged Ethereum primitives.

use alloy_consensus::{
    Transaction, TransactionEnvelope, Typed2718,
    transaction::{SignerRecoverable, TxHashRef},
};
use alloy_eips::eip2718::{Decodable2718, Eip2718Error, Encodable2718};
use alloy_primitives::{Address, B256, Bytes, Sealable, Sealed, TxKind, U256, address};
use alloy_rlp::{Decodable, Encodable};
use reth_primitives_traits::InMemorySize;

/// Reserved EEZ type. This is not the OP deposit wire format.
pub const SYSTEM_TX_TYPE: u8 = 0x76;
/// Codeless EEZ privilege domain, distinct from Ethereum's block-level system caller.
pub const SYSTEM_ADDRESS: Address = address!("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076");
/// The L2 execution manager is a protocol predeploy.
pub const EEZL2_ADDRESS: Address = address!("4200000000000000000000000000000000000007");

/// Canonical wire body: chain ID, nonce, gas price, gas limit, destination, value, calldata.
/// Signatures and an arbitrary sender are deliberately absent. Authorization is
/// established by derivation; this type must never enter a public transaction pool.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    alloy_rlp::RlpEncodable,
    alloy_rlp::RlpDecodable,
)]
#[serde(rename_all = "camelCase")]
pub struct SystemTransaction {
    #[serde(with = "alloy_serde::quantity")]
    pub chain_id: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub nonce: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub gas_price: u128,
    #[serde(rename = "gas", with = "alloy_serde::quantity")]
    pub gas_limit: u64,
    pub to: Address,
    pub value: U256,
    pub input: Bytes,
}

impl Typed2718 for SystemTransaction {
    fn ty(&self) -> u8 {
        SYSTEM_TX_TYPE
    }
}
impl Encodable2718 for SystemTransaction {
    fn encode_2718_len(&self) -> usize {
        1 + self.length()
    }
    fn encode_2718(&self, out: &mut dyn alloy_rlp::BufMut) {
        out.put_u8(SYSTEM_TX_TYPE);
        self.encode(out);
    }
}
impl Decodable2718 for SystemTransaction {
    fn typed_decode(ty: u8, buf: &mut &[u8]) -> Result<Self, Eip2718Error> {
        if ty != SYSTEM_TX_TYPE {
            return Err(Eip2718Error::UnexpectedType(ty));
        }
        let tx = Self::decode(buf)?;
        if tx.to != EEZL2_ADDRESS {
            return Err(alloy_rlp::Error::Custom("system transaction must target EEZL2").into());
        }
        Ok(tx)
    }
    fn fallback_decode(_: &mut &[u8]) -> Result<Self, Eip2718Error> {
        Err(Eip2718Error::UnexpectedType(0))
    }
}
impl Sealable for SystemTransaction {
    fn hash_slow(&self) -> B256 {
        self.trie_hash()
    }
}
impl SignerRecoverable for SystemTransaction {
    fn recover_signer(&self) -> Result<Address, alloy_consensus::crypto::RecoveryError> {
        Ok(SYSTEM_ADDRESS)
    }
    fn recover_signer_unchecked(&self) -> Result<Address, alloy_consensus::crypto::RecoveryError> {
        self.recover_signer()
    }
}
impl Transaction for SystemTransaction {
    fn chain_id(&self) -> Option<u64> {
        Some(self.chain_id)
    }
    fn nonce(&self) -> u64 {
        self.nonce
    }
    fn gas_limit(&self) -> u64 {
        self.gas_limit
    }
    fn gas_price(&self) -> Option<u128> {
        Some(self.gas_price)
    }
    fn max_fee_per_gas(&self) -> u128 {
        self.gas_price
    }
    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        None
    }
    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        None
    }
    fn priority_fee_or_price(&self) -> u128 {
        self.gas_price
    }
    fn effective_gas_price(&self, _: Option<u64>) -> u128 {
        self.gas_price
    }
    fn is_dynamic_fee(&self) -> bool {
        false
    }
    fn kind(&self) -> TxKind {
        TxKind::Call(self.to)
    }
    fn is_create(&self) -> bool {
        false
    }
    fn value(&self) -> U256 {
        self.value
    }
    fn input(&self) -> &Bytes {
        &self.input
    }
    fn access_list(&self) -> Option<&alloy_eips::eip2930::AccessList> {
        None
    }
    fn blob_versioned_hashes(&self) -> Option<&[B256]> {
        None
    }
    fn authorization_list(&self) -> Option<&[alloy_eips::eip7702::SignedAuthorization]> {
        None
    }
}

#[derive(Clone, Debug, TransactionEnvelope)]
#[envelope(tx_type_name = EezTxType)]
pub enum EezTxEnvelope {
    #[envelope(flatten)]
    Ethereum(reth_ethereum_primitives::TransactionSigned),
    #[envelope(ty = 118)]
    System(Sealed<SystemTransaction>),
}
impl From<reth_ethereum_primitives::TransactionSigned> for EezTxEnvelope {
    fn from(tx: reth_ethereum_primitives::TransactionSigned) -> Self {
        Self::Ethereum(tx)
    }
}
impl From<SystemTransaction> for EezTxEnvelope {
    fn from(tx: SystemTransaction) -> Self {
        Self::System(Sealed::new(tx))
    }
}
impl SignerRecoverable for EezTxEnvelope {
    fn recover_signer(&self) -> Result<Address, alloy_consensus::crypto::RecoveryError> {
        match self {
            Self::Ethereum(tx) => tx.recover_signer(),
            Self::System(tx) => tx.recover_signer(),
        }
    }
    fn recover_signer_unchecked(&self) -> Result<Address, alloy_consensus::crypto::RecoveryError> {
        match self {
            Self::Ethereum(tx) => tx.recover_signer_unchecked(),
            Self::System(tx) => tx.recover_signer_unchecked(),
        }
    }
}
impl TxHashRef for EezTxEnvelope {
    fn tx_hash(&self) -> &B256 {
        match self {
            Self::Ethereum(tx) => tx.tx_hash(),
            Self::System(tx) => tx.hash_ref(),
        }
    }
}
impl InMemorySize for EezTxEnvelope {
    fn size(&self) -> usize {
        match self {
            Self::Ethereum(tx) => tx.size(),
            Self::System(tx) => std::mem::size_of::<Self>() + tx.input.len(),
        }
    }
}
impl reth_codecs::Compact for EezTxEnvelope {
    fn to_compact<B: alloy_rlp::BufMut + AsMut<[u8]>>(&self, buf: &mut B) -> usize {
        self.encode_2718(buf);
        self.encode_2718_len()
    }
    fn from_compact(buf: &[u8], len: usize) -> (Self, &[u8]) {
        let (data, rest) = buf.split_at(len);
        (
            Self::decode_2718_exact(data).expect("invalid stored EEZ transaction"),
            rest,
        )
    }
}

pub type Block = alloy_consensus::Block<EezTxEnvelope>;
pub type BlockBody = alloy_consensus::BlockBody<EezTxEnvelope>;
pub type Receipt = alloy_consensus::EthereumReceipt<EezTxType>;
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EezPrimitives;
impl reth_primitives_traits::NodePrimitives for EezPrimitives {
    type Block = Block;
    type BlockHeader = alloy_consensus::Header;
    type BlockBody = BlockBody;
    type SignedTx = EezTxEnvelope;
    type Receipt = Receipt;
}

impl InMemorySize for EezTxType {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl reth_codecs::Compact for EezTxType {
    fn to_compact<B: alloy_rlp::BufMut + AsMut<[u8]>>(&self, buf: &mut B) -> usize {
        buf.put_u8((*self).into());
        1
    }
    fn from_compact(buf: &[u8], _: usize) -> (Self, &[u8]) {
        (
            Self::try_from(buf[0]).expect("invalid stored EEZ type"),
            &buf[1..],
        )
    }
}
impl alloy_evm::FromRecoveredTx<EezTxEnvelope> for revm::context::TxEnv {
    /// Native envelopes have no signature: use the protocol's fixed caller and
    /// Ethereum's legacy execution type to retain normal nonce, fee, balance,
    /// and revert rules. The envelope and receipt still carry `0x76`. Ordinary
    /// Ethereum transactions use the upstream conversion unchanged.
    fn from_recovered_tx(tx: &EezTxEnvelope, sender: Address) -> Self {
        match tx {
            EezTxEnvelope::Ethereum(tx) => Self::from_recovered_tx(tx, sender),
            EezTxEnvelope::System(tx) => Self {
                caller: SYSTEM_ADDRESS,
                gas_limit: tx.gas_limit,
                gas_price: tx.gas_price,
                kind: TxKind::Call(tx.to),
                value: tx.value,
                data: tx.input.clone(),
                nonce: tx.nonce,
                chain_id: Some(tx.chain_id),
                // Execution uses ordinary fee, nonce and balance rules. The
                // envelope and receipt retain the reserved EEZ type.
                tx_type: 0,
                ..Default::default()
            },
        }
    }
}
impl alloy_evm::FromTxWithEncoded<EezTxEnvelope> for revm::context::TxEnv {
    fn from_encoded_tx(tx: &EezTxEnvelope, sender: Address, _: Bytes) -> Self {
        <Self as alloy_evm::FromRecoveredTx<EezTxEnvelope>>::from_recovered_tx(tx, sender)
    }
}

pub mod engine;

/// Public transaction gossip retains Ethereum's pooled envelope. Conversion from
/// a block is fallible so reorg reinjection cannot put native system txs in a pool.
impl TryFrom<EezTxEnvelope>
    for alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844WithSidecar>
{
    type Error = alloy_consensus::error::ValueError<EezTxEnvelope>;
    fn try_from(tx: EezTxEnvelope) -> Result<Self, Self::Error> {
        match tx {
            EezTxEnvelope::Ethereum(tx) => {
                Self::try_from(tx).map_err(|e| e.map(EezTxEnvelope::Ethereum))
            }
            tx => Err(alloy_consensus::error::ValueError::new(
                tx,
                "native system transactions cannot enter the pool",
            )),
        }
    }
}
impl From<alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844WithSidecar>>
    for EezTxEnvelope
{
    fn from(
        tx: alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844WithSidecar>,
    ) -> Self {
        Self::Ethereum(tx.into())
    }
}

impl reth_rpc_traits::SignableTxRequest<EezTxEnvelope> for alloy_rpc_types_eth::TransactionRequest {
    async fn try_build_and_sign(
        self,
        signer: impl alloy_network::TxSigner<alloy_primitives::Signature> + Send,
    ) -> Result<EezTxEnvelope, reth_rpc_traits::SignTxRequestError> {
        <Self as reth_rpc_traits::SignableTxRequest<reth_ethereum_primitives::TransactionSigned>>::try_build_and_sign(self, signer).await.map(EezTxEnvelope::Ethereum)
    }
}
impl reth_rpc_traits::TryIntoSimTx<EezTxEnvelope> for alloy_rpc_types_eth::TransactionRequest {
    fn try_into_sim_tx(self) -> Result<EezTxEnvelope, alloy_consensus::error::ValueError<Self>> {
        self.build_typed_simulate_transaction()
            .map(EezTxEnvelope::Ethereum)
    }
}

impl reth_codecs::Compress for EezTxEnvelope {
    type Compressed = Vec<u8>;
    fn compress_to_buf<B: alloy_rlp::BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        self.encode_2718(buf);
    }
}
impl reth_codecs::Decompress for EezTxEnvelope {
    fn decompress(value: &[u8]) -> Result<Self, reth_codecs::DecompressError> {
        Self::decode_2718_exact(value).map_err(reth_codecs::DecompressError::new)
    }
}

#[cfg(test)]
mod tests;

impl<T> From<alloy_consensus::Signed<T>> for EezTxEnvelope
where
    reth_ethereum_primitives::TransactionSigned: From<alloy_consensus::Signed<T>>,
{
    fn from(tx: alloy_consensus::Signed<T>) -> Self {
        Self::Ethereum(tx.into())
    }
}

impl Default for EezTxType {
    fn default() -> Self {
        Self::Ethereum(alloy_consensus::TxType::Legacy)
    }
}
