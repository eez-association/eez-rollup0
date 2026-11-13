//! The EEZ evm2 type family: Ethereum transaction handling plus native `0x76`
//! deposits and genesis-configured fee collection.
//!
//! Ethereum transactions run evm2's standard handlers with [`EezTxHooks`], which
//! credit the burned base fee to the fee collector. Native transactions run
//! [`native`], built from the same public handler pieces.

use alloy_consensus::{Transaction, Typed2718, transaction::Recovered};
use alloy_eips::{eip2930::AccessList, eip7702::SignedAuthorization};
use alloy_primitives::{Address, B256, Bytes, ChainId, TxKind, U256};
use eez_primitives::{
    EEZL2_ADDRESS, EezTxEnvelope, SYSTEM_ADDRESS, SYSTEM_TX_GAS_LIMIT, SYSTEM_TX_TYPE,
    SystemTransaction,
};
use evm2::{
    BaseEvmConfigSelector, Evm, EvmFeatures, EvmTypesHost, SpecId, TxResult,
    env::TxEnvExt,
    ethereum::{
        self as eth, TxEnvelope, default_settle_gas, eip1559, eip2930, eip4844, eip7702, legacy,
    },
    handler::{GasSettlement, TxHandlerHooks},
    interpreter::GasTracker,
    registry::{HandlerError, HandlerResult, TxRegistry, TxRequest, handler},
};
use std::fmt;

/// EEZ evm2 type family: evm2's Ethereum types with [`EezEvmTx`] as the
/// transaction and the fee collector carried per EVM.
#[derive(Debug, Clone, Copy)]
pub struct EezEvmTypes(());

impl EvmTypesHost for EezEvmTypes {
    type ConfigSelector = BaseEvmConfigSelector;
    type SpecId = SpecId;
    type Tx = EezEvmTx;
    type EvmExt = EezEvmExt;
    type MessageExt = ();
    type MessageResultExt = ();
    type TxEnvExt = ();
    type TxResultExt = ();
    type BlockEnvExt = ();
    type Host<'a> = Evm<'a, Self>;
}

/// Per-EVM EEZ state, set by [`EezEvmFactory`] when it creates an EVM.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EezEvmExt {
    /// Receives the base fee Ethereum burns. Absent (Ethereum L1 simulation), the
    /// base fee is burned.
    pub fee_collector: Option<Address>,
}

/// Transaction executed by the EEZ EVM.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EezEvmTx {
    /// An Ethereum transaction.
    Ethereum(TxEnvelope),
    /// A native system transaction (`0x76`). It keeps its fields so the handler
    /// re-validates the protocol sender and target.
    System(SystemTransaction),
}

impl EezEvmTx {
    const fn ethereum(&self) -> Option<&TxEnvelope> {
        match self {
            Self::Ethereum(tx) => Some(tx),
            Self::System(_) => None,
        }
    }

    const fn system(&self) -> Option<&SystemTransaction> {
        match self {
            Self::System(tx) => Some(tx),
            Self::Ethereum(_) => None,
        }
    }
}

impl From<EezTxEnvelope> for EezEvmTx {
    fn from(tx: EezTxEnvelope) -> Self {
        match tx {
            EezTxEnvelope::Ethereum(tx) => Self::Ethereum(tx.into()),
            EezTxEnvelope::System(tx) => Self::System(tx.into_inner()),
        }
    }
}

impl reth_evm::FromRecoveredTx<EezTxEnvelope> for EezEvmTx {
    fn from_recovered_tx(tx: Recovered<EezTxEnvelope>) -> Self {
        tx.into_inner().into()
    }
}

impl reth_evm::FromTxWithEncoded<EezTxEnvelope> for EezEvmTx {}

/// Ethereum's signed envelope, for the env-deriving `EthEvmConfig`.
type TransactionSigned = alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844>;

impl reth_evm::FromRecoveredTx<TransactionSigned> for EezEvmTx {
    fn from_recovered_tx(tx: Recovered<TransactionSigned>) -> Self {
        Self::Ethereum(tx.into_inner().into())
    }
}

impl reth_evm::FromTxWithEncoded<TransactionSigned> for EezEvmTx {}

impl reth_evm::TransactionEnvMut for EezEvmTx {
    /// Native transactions have a protocol-fixed gas budget.
    fn set_gas_limit(&mut self, gas_limit: u64) {
        if let Self::Ethereum(tx) = self {
            tx.set_gas_limit(gas_limit);
        }
    }

    /// Native transactions carry no access list.
    fn set_access_list(&mut self, access_list: AccessList) {
        if let Self::Ethereum(tx) = self {
            tx.set_access_list(access_list);
        }
    }
}

impl Typed2718 for EezEvmTx {
    fn ty(&self) -> u8 {
        match self {
            Self::Ethereum(tx) => tx.ty(),
            Self::System(_) => SYSTEM_TX_TYPE,
        }
    }
}

macro_rules! delegate {
    ($self:ident, $tx:ident => $expr:expr) => {
        match $self {
            Self::Ethereum($tx) => $expr,
            Self::System($tx) => $expr,
        }
    };
}

impl Transaction for EezEvmTx {
    fn chain_id(&self) -> Option<ChainId> {
        delegate!(self, tx => tx.chain_id())
    }
    fn nonce(&self) -> u64 {
        delegate!(self, tx => tx.nonce())
    }
    fn gas_limit(&self) -> u64 {
        match self {
            Self::Ethereum(tx) => tx.gas_limit(),
            Self::System(_) => SYSTEM_TX_GAS_LIMIT,
        }
    }
    fn gas_price(&self) -> Option<u128> {
        delegate!(self, tx => tx.gas_price())
    }
    fn max_fee_per_gas(&self) -> u128 {
        delegate!(self, tx => tx.max_fee_per_gas())
    }
    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        delegate!(self, tx => tx.max_priority_fee_per_gas())
    }
    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        delegate!(self, tx => tx.max_fee_per_blob_gas())
    }
    fn priority_fee_or_price(&self) -> u128 {
        delegate!(self, tx => tx.priority_fee_or_price())
    }
    fn effective_gas_price(&self, base_fee: Option<u64>) -> u128 {
        delegate!(self, tx => tx.effective_gas_price(base_fee))
    }
    fn is_dynamic_fee(&self) -> bool {
        delegate!(self, tx => tx.is_dynamic_fee())
    }
    fn kind(&self) -> TxKind {
        delegate!(self, tx => tx.kind())
    }
    fn is_create(&self) -> bool {
        delegate!(self, tx => tx.is_create())
    }
    fn value(&self) -> U256 {
        delegate!(self, tx => tx.value())
    }
    fn input(&self) -> &Bytes {
        delegate!(self, tx => tx.input())
    }
    fn access_list(&self) -> Option<&AccessList> {
        delegate!(self, tx => tx.access_list())
    }
    fn blob_versioned_hashes(&self) -> Option<&[B256]> {
        delegate!(self, tx => tx.blob_versioned_hashes())
    }
    fn authorization_list(&self) -> Option<&[SignedAuthorization]> {
        delegate!(self, tx => tx.authorization_list())
    }
}

/// Rejections specific to EEZ execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EezEvmError {
    /// A native transaction not from the protocol sender to the EEZL2 predeploy.
    InvalidNativeTransaction,
    /// The native deposit would overflow the system balance.
    NativeDepositOverflow,
    /// The fee credit would overflow the collector balance.
    FeeCollectorOverflow,
}

impl fmt::Display for EezEvmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidNativeTransaction => "invalid native system caller or target",
            Self::NativeDepositOverflow => "native deposit balance overflow",
            Self::FeeCollectorOverflow => "fee collector balance overflow",
        })
    }
}

impl std::error::Error for EezEvmError {}

/// Hooks for Ethereum transactions: credit the base fee Ethereum burns to the
/// fee collector.
#[derive(Debug, Clone, Copy, Default)]
pub struct EezTxHooks;

impl TxHandlerHooks<EezEvmTypes> for EezTxHooks {
    /// After Ethereum's refund and beneficiary reward, credit
    /// `min(basefee, effective price) × gas used`. The minimum also covers
    /// simulations with base-fee checks off. Only from London, only with fee
    /// charging on, and only a non-zero amount: a zero credit would touch the
    /// collector and change state.
    fn settle_transaction(
        host: &mut Evm<'_, EezEvmTypes>,
        _envelope: &EezEvmTx,
        gas: GasSettlement<EezEvmTypes>,
    ) -> HandlerResult<TxResult<EezEvmTypes>> {
        let gas_price = gas.gas_price;
        let result = default_settle_gas(host, gas)?;
        let Some(collector) = host.ext().fee_collector else {
            return Ok(result);
        };
        if !host.spec_id().enables(SpecId::LONDON) || !host.feature(EvmFeatures::FEE_CHARGE) {
            return Ok(result);
        }
        let fee = host.block().basefee.min(gas_price) * U256::from(result.tx_gas_used());
        if !fee.is_zero() {
            let mut account = host.state_mut().account(&collector)?;
            let balance = account
                .balance()
                .checked_add(fee)
                .ok_or_else(|| HandlerError::external(EezEvmError::FeeCollectorOverflow))?;
            account.set_balance(balance);
        }
        Ok(result)
    }
}

/// The transaction registry for `spec_id`: evm2's Ethereum handlers at their
/// forks with [`EezTxHooks`], and the native handler at every fork.
pub fn eez_tx_registry(spec_id: SpecId) -> TxRegistry<EezEvmTypes, TxResult<EezEvmTypes>> {
    type T = EezEvmTypes;
    type H = EezTxHooks;

    let mut registry = TxRegistry::new()
        .with_handler(
            0,
            |tx: &EezEvmTx| tx.ethereum().and_then(TxEnvelope::as_legacy),
            handler(
                legacy::prepare_with_hooks::<T, H>,
                legacy::execute_prepared::<T, H>,
            ),
        )
        .with_handler(
            SYSTEM_TX_TYPE,
            EezEvmTx::system,
            handler(native::prepare, native::execute),
        );
    if spec_id.enables(SpecId::BERLIN) {
        registry.register(
            1,
            |tx: &EezEvmTx| tx.ethereum().and_then(TxEnvelope::as_eip2930),
            handler(
                eip2930::prepare_with_hooks::<T, H>,
                eip2930::execute_prepared::<T, H>,
            ),
        );
    }
    if spec_id.enables(SpecId::LONDON) {
        registry.register(
            2,
            |tx: &EezEvmTx| tx.ethereum().and_then(TxEnvelope::as_eip1559),
            handler(
                eip1559::prepare_with_hooks::<T, H>,
                eip1559::execute_prepared::<T, H>,
            ),
        );
    }
    if spec_id.enables(SpecId::CANCUN) {
        registry.register(
            3,
            |tx: &EezEvmTx| tx.ethereum().and_then(TxEnvelope::as_eip4844),
            handler(
                eip4844::prepare_with_hooks::<T, H>,
                eip4844::execute_prepared::<T, H>,
            ),
        );
    }
    if spec_id.enables(SpecId::PRAGUE) {
        registry.register(
            4,
            |tx: &EezEvmTx| tx.ethereum().and_then(TxEnvelope::as_eip7702),
            handler(
                eip7702::prepare_with_hooks::<T, H>,
                eip7702::execute_prepared::<T, H>,
            ),
        );
    }
    registry
}

/// The native `0x76` handler: Ethereum's legacy handler without gas-price
/// validation or the upfront fee charge, around a minted deposit.
///
/// The mint precedes the sender's balance check and the frame checkpoint, so a
/// failed call keeps it; the handler restores the pre-mint balance. Any
/// validation error discards the whole transaction state, mint included.
mod native {
    use super::{
        EEZL2_ADDRESS, EezEvmError, EezEvmTypes, GasSettlement, GasTracker, HandlerError,
        HandlerResult, SYSTEM_ADDRESS, SYSTEM_TX_GAS_LIMIT, SystemTransaction, TxEnvExt, TxKind,
        TxRequest, TxResult, U256, default_settle_gas, eth,
    };

    pub(super) struct Prepared {
        intrinsic: u64,
        floor_gas: u64,
        sender_nonce: u64,
        previous_balance: U256,
    }

    pub(super) fn prepare(
        req: &mut TxRequest<'_, '_, EezEvmTypes, SystemTransaction>,
    ) -> HandlerResult<Prepared> {
        let caller = req.tx.signer();
        let tx = req.tx.inner();
        if caller != SYSTEM_ADDRESS || tx.to != EEZL2_ADDRESS {
            return Err(HandlerError::external(
                EezEvmError::InvalidNativeTransaction,
            ));
        }
        let to = TxKind::Call(tx.to);
        let version = req.host.version();
        eth::validate_chain_id(version, Some(tx.chain_id), true)?;
        eth::validate_tx_gas_limit_cap(version, SYSTEM_TX_GAS_LIMIT)?;
        eth::validate_block_gas_limit(version, SYSTEM_TX_GAS_LIMIT, req.host.block().gas_limit)?;
        eth::validate_nonce_not_overflow(tx.nonce)?;
        let (intrinsic, floor_gas) =
            eth::intrinsic_and_floor_gas(version, caller, to, &tx.input, 0, 0, tx.value);
        eth::validate_intrinsic_gas(SYSTEM_TX_GAS_LIMIT, intrinsic, 0)?;
        eth::validate_floor_gas(SYSTEM_TX_GAS_LIMIT, floor_gas)?;
        eth::validate_execution_gas_limit_cap(version, SYSTEM_TX_GAS_LIMIT, intrinsic, floor_gas)?;

        let previous_balance = {
            let mut system = req.host.state_mut().account(&SYSTEM_ADDRESS)?;
            let previous = system.balance();
            if !tx.value.is_zero() {
                let funded = previous
                    .checked_add(tx.value)
                    .ok_or_else(|| HandlerError::external(EezEvmError::NativeDepositOverflow))?;
                system.set_balance(funded);
            }
            previous
        };
        let sender_nonce =
            eth::validate_sender_and_bump_nonce(req.host, caller, tx.nonce, tx.value)?;
        eth::warm_base_accounts(req.host, to);
        Ok(Prepared {
            intrinsic,
            floor_gas,
            sender_nonce,
            previous_balance,
        })
    }

    pub(super) fn execute(
        req: TxRequest<'_, '_, EezEvmTypes, SystemTransaction>,
        prepared: Prepared,
    ) -> HandlerResult<TxResult<EezEvmTypes>> {
        let Prepared {
            intrinsic,
            floor_gas,
            sender_nonce,
            previous_balance,
        } = prepared;
        let caller = req.tx.signer();
        let tx = req.tx.inner();
        let (execution_gas_limit, reservoir) =
            eth::initial_gas_and_reservoir(req.host.version(), SYSTEM_TX_GAS_LIMIT, intrinsic, 0);
        *req.host.tx_env_mut() = TxEnvExt {
            origin: caller,
            gas_price: U256::ZERO,
            chain_id: U256::from(req.host.version().chain_id),
            ..TxEnvExt::default()
        };
        let mut tx_gas =
            GasTracker::new_with_execution_gas_and_reservoir(execution_gas_limit, reservoir);
        let frame = eth::prepare_initial_frame(
            req.host,
            caller,
            sender_nonce,
            TxKind::Call(tx.to),
            &tx.input,
            tx.value,
            &mut tx_gas,
        )?;
        let result = eth::execute_initial_frame(
            req.host,
            frame,
            &mut tx_gas,
            execution_gas_limit,
            reservoir,
        )?;
        if !result.is_success() && !tx.value.is_zero() {
            // The frame rolled back the transfer but not the mint before it.
            req.host
                .state_mut()
                .account(&SYSTEM_ADDRESS)?
                .set_balance(previous_balance);
        }
        default_settle_gas(
            req.host,
            GasSettlement {
                caller,
                gas_price: U256::ZERO,
                gas_limit: SYSTEM_TX_GAS_LIMIT,
                floor_gas,
                initial_state_gas: 0,
                state_refund: 0,
                result,
            },
        )
    }
}

/// Creates EEZ EVMs: the registry above and the configured fee collector.
#[derive(Debug, Default, Clone, Copy)]
pub struct EezEvmFactory {
    fee_collector: Option<Address>,
}

impl EezEvmFactory {
    pub const fn new(fee_collector: Option<Address>) -> Self {
        Self { fee_collector }
    }
}

impl reth_evm_ethereum::EvmFactory for EezEvmFactory {
    type Types = EezEvmTypes;
    type SpecId = SpecId;

    fn spec_id(&self, spec: SpecId) -> SpecId {
        spec
    }

    fn tx_registry(&self, spec: SpecId) -> TxRegistry<EezEvmTypes, TxResult<EezEvmTypes>> {
        eez_tx_registry(spec)
    }

    fn configure_evm(&self, evm: &mut Evm<'_, EezEvmTypes>) {
        evm.ext_mut().fee_collector = self.fee_collector;
    }
}
