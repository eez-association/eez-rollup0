use crate::{EezEvmConfig, EezEvmTx};
use alloy_consensus::transaction::Recovered;
use alloy_rpc_types_eth::TransactionRequest;
use evm2::ethereum::RecoveredTxEnvelope;
use reth_evm::{EvmEnvFor, TxEnvFor};
use reth_rpc_convert::{EthTxEnvError, TryIntoTxEnv, transaction::TxEnvConverter};

/// Builds call and estimate environments from RPC requests. Requests are always
/// Ethereum transactions; native `0x76` transactions never come from RPC.
#[derive(Debug, Clone, Copy, Default)]
pub struct EezTxEnvConverter;

impl TxEnvConverter<TransactionRequest, EezEvmConfig> for EezTxEnvConverter {
    type Error = EthTxEnvError;

    fn convert_tx_env(
        &self,
        tx_req: TransactionRequest,
        evm_env: &EvmEnvFor<EezEvmConfig>,
    ) -> Result<TxEnvFor<EezEvmConfig>, Self::Error> {
        let tx: RecoveredTxEnvelope = tx_req.try_into_tx_env(evm_env)?;
        let signer = tx.signer();
        Ok(Recovered::new_unchecked(
            EezEvmTx::Ethereum(tx.into_inner()),
            signer,
        ))
    }
}
