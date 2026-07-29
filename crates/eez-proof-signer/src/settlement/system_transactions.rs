//! System-transaction key validation and canonical Sync reconstruction.
//!
//! This module validates operator-provided keys once, keeps secret values out
//! of debug output, and builds the fixed context used to reconstruct omitted
//! system transactions.

use std::fmt;
use std::num::NonZeroU64;

use alloy_primitives::{Address, B256, Bytes};
use alloy_signer_local::PrivateKeySigner;
use eez_protocol::SYSTEM_ADDRESS;
use eez_protocol::abi::ExecutionEntrySol;
use thiserror::Error;

use crate::EEZL2_ADDRESS;

// These builder values participate in the signed transaction bytes and must
// match the values used to produce the omitted system transactions.
const SYSTEM_TRANSACTION_GAS_PRICE: u128 = 1_000_000_000;
const SYSTEM_TRANSACTION_GAS_LIMIT: u64 = 2_000_000;

/// System private key used to reconstruct signed Sync-block transactions
/// omitted from DA.
///
/// The key must be the deployment key for [`SYSTEM_ADDRESS`]. System
/// transactions present in Composer-supplied block RLP but omitted from batch
/// DA are accepted only when they byte-match the sequence independently
/// reconstructed with this key.
pub(crate) struct SystemTransactionKey(PrivateKeySigner);

impl fmt::Debug for SystemTransactionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemTransactionKey")
            .field("address", &SYSTEM_ADDRESS)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum SystemTransactionConfigError {
    #[error("L2 system key is not a valid secp256k1 private key")]
    InvalidPrivateKey,
    #[error("L2 system key derives address {actual}; expected {expected}")]
    WrongAddress { expected: Address, actual: Address },
}

impl SystemTransactionKey {
    /// Parse once into the canonical signer and pin it to the reserved
    /// system address; no secret representation other than the signer is kept.
    pub(crate) fn new(private_key: B256) -> Result<Self, SystemTransactionConfigError> {
        let signer = PrivateKeySigner::from_bytes(&private_key)
            .map_err(|_| SystemTransactionConfigError::InvalidPrivateKey)?;
        let actual = signer.address();
        if actual != SYSTEM_ADDRESS {
            return Err(SystemTransactionConfigError::WrongAddress {
                expected: SYSTEM_ADDRESS,
                actual,
            });
        }
        Ok(Self(signer))
    }

    /// Complete the operator-configured reconstruction context with chain identity.
    pub(crate) fn into_reconstructor(
        self,
        l2_chain_id: u64,
        expected_rollup_id: NonZeroU64,
    ) -> SystemTransactionReconstructor {
        SystemTransactionReconstructor {
            context: eez_protocol::system_tx::SystemTxContext {
                system_signer: self.0,
                ccm_l2_address: EEZL2_ADDRESS,
                l2_chain_id,
                l2_gas_price: SYSTEM_TRANSACTION_GAS_PRICE,
                l2_gas_limit: SYSTEM_TRANSACTION_GAS_LIMIT,
                this_rollup_id: expected_rollup_id.get(),
            },
        }
    }
}

/// Configuration for reconstructing and byte-checking omitted Sync-block
/// system transactions.
///
/// The transaction target is fixed to [`EEZL2_ADDRESS`]. Code identity at that
/// address is a deployment invariant and is not checked here.
pub(crate) struct SystemTransactionReconstructor {
    /// Startup-checked operator and fixed protocol reconstruction parameters.
    context: eez_protocol::system_tx::SystemTxContext,
}

impl SystemTransactionReconstructor {
    /// Rebuild the canonical signed Sync pairs from authorized effect projections.
    pub(super) fn reconstruct_sync_pairs(
        &self,
        outbound: &[(ExecutionEntrySol, Bytes)],
        inbound: &[ExecutionEntrySol],
        starting_nonce: u64,
    ) -> Result<Vec<eez_protocol::system_tx::SyncPair>, String> {
        eez_protocol::system_tx::build_cross_chain_sync_pairs(
            outbound,
            inbound,
            &self.context,
            starting_nonce,
        )
    }
}

impl fmt::Debug for SystemTransactionReconstructor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemTransactionReconstructor")
            .field("system_address", &self.context.system_signer.address())
            .field("ccm_l2_address", &self.context.ccm_l2_address)
            .field("l2_chain_id", &self.context.l2_chain_id)
            .field("l2_gas_price", &self.context.l2_gas_price)
            .field("l2_gas_limit", &self.context.l2_gas_limit)
            .field("rollup_id", &self.context.this_rollup_id)
            .finish()
    }
}
