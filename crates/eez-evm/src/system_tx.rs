//! Single source of truth for building signed L2 inbound system txs.
//!
//! Given the cross-chain entries that will land in a `postBatch` —
//! whether observed locally by the composer's `simulate_and_resolve`
//! or read from L1's `BatchPosted` event by the deriver — produce
//! the **same** signed `executeIncomingCrossChainCall(...)` system
//! txs that should appear at the head of the L2 Sync block.
//!
//! Composer and deriver agree mechanically: both call
//! [`build_inbound_system_txs`] with the same `entries`, so the signed
//! txs are byte-identical (`Rollup-1.md §5`: system txs precede user
//! txs).
//!
//! Txs are signed legacy txs from the SYSTEM_ADDRESS key — works
//! against vanilla reth without a custom tx type, and both processes
//! sign with the same key so the sigs match. (Type-0x7E system txs,
//! per `Rollup-1.md §5.3`, would drop the deriver's need for the key;
//! a follow-up.)
//!
//! Nonce: both sides read the SYSTEM_ADDRESS nonce from local L2 state
//! at the same parent block. Reth derives it deterministically from
//! applied history, so equal histories give equal nonce → signature →
//! tx hash.

use alloy_consensus::TxLegacy;
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use reth_ethereum_primitives::{Transaction, TransactionSigned};

use crate::dialect::encode_execute_incoming_cross_chain_call;
use crate::types::ExecutionEntrySol;

/// Per-follower configuration the system-tx builder needs. The
/// composer and deriver each build one from their own startup config.
#[derive(Clone, Debug)]
pub struct SystemTxContext {
    /// SYSTEM_ADDRESS signer for this L2. Must match the L2's
    /// `EEZL2.SYSTEM_ADDRESS` immutable, otherwise
    /// `executeIncomingCrossChainCall`'s `onlySystemAddress` modifier
    /// reverts.
    pub system_signer: PrivateKeySigner,
    /// On-L2 address of the `EEZL2` contract (CCM-L2 predeploy).
    pub ccm_l2_address: Address,
    /// EIP-155 chain id of this L2.
    pub l2_chain_id: u64,
    /// Legacy `gasPrice` for the signed system tx. Dev/devnet uses
    /// 1 gwei (above the dev-mode 0 basefee).
    pub l2_gas_price: u128,
    /// Per-tx gas budget. Matches
    /// `crosschain-evm-composer::EXECUTE_INCOMING_GAS_LIMIT` (~2M)
    /// from the reference impl.
    pub l2_gas_limit: u64,
    /// This rollup's id — entries whose `destinationRollupId` doesn't
    /// match are skipped (they belong to a different L2).
    pub this_rollup_id: u64,
}

/// Build signed L2 inbound system txs from a postBatch's entries.
///
/// For each entry whose `destinationRollupId == cfg.this_rollup_id`,
/// reconstructs the outer cross-chain call from
/// `entry.L2ToL1Calls[0]` and produces one signed legacy tx invoking
/// `EEZL2.executeIncomingCrossChainCall(...)`. Entries for other
/// rollups are skipped.
///
/// `starting_nonce` is the SYSTEM_ADDRESS account nonce at the L2
/// parent block. The function advances locally by one per emitted
/// tx — callers don't need to thread nonces themselves.
///
/// # Errors
///
/// Returns a `String` error if a signature operation fails (signer
/// chain-id disagreement, malformed key, etc.).
pub fn build_inbound_system_txs(
    entries: &[ExecutionEntrySol],
    cfg: &SystemTxContext,
    starting_nonce: u64,
) -> Result<Vec<Bytes>, String> {
    let mut nonce = starting_nonce;
    let mut out: Vec<Bytes> = Vec::new();
    for entry in entries {
        // `destinationRollupId` is `uint256` on-chain; rollup ids fit
        // in u64 by construction (`eez_protocol::RollupId`).
        let dest_rollup = u64::try_from(entry.destinationRollupId).unwrap_or(u64::MAX);
        if dest_rollup != cfg.this_rollup_id {
            continue;
        }
        if entry.L2ToL1Calls.is_empty() {
            continue;
        }
        let outer = &entry.L2ToL1Calls[0];
        let source_rollup = u64::try_from(outer.sourceRollupId).unwrap_or(u64::MAX);
        let calldata = encode_execute_incoming_cross_chain_call(
            outer.targetAddress,
            outer.value,
            &outer.data,
            outer.sourceAddress,
            source_rollup,
            vec![entry.clone()],
            Vec::new(),
        );
        let raw = sign_legacy_system_tx(
            &cfg.system_signer,
            nonce,
            cfg.ccm_l2_address,
            calldata,
            outer.value,
            cfg.l2_chain_id,
            cfg.l2_gas_price,
            cfg.l2_gas_limit,
        )?;
        nonce = nonce.checked_add(1).ok_or_else(|| {
            "SYSTEM_ADDRESS nonce overflow in build_inbound_system_txs".to_string()
        })?;
        out.push(raw);
    }
    Ok(out)
}

/// Sign a single legacy L2 tx from SYSTEM_ADDRESS with an explicit
/// `value`.
///
/// `EEZL2.executeIncomingCrossChainCall` enforces strict
/// `msg.value == value` equality (`EEZL2.sol:194`) — pass the same
/// value here as is embedded in the calldata.
///
/// # Errors
///
/// Returns a `String` error if `sign_transaction_sync` fails.
#[expect(
    clippy::too_many_arguments,
    reason = "wrapping in a struct would just move the arity to the constructor; \
              every field is load-bearing and the function is private to this module"
)]
fn sign_legacy_system_tx(
    signer: &PrivateKeySigner,
    nonce: u64,
    to: Address,
    calldata: Vec<u8>,
    value: U256,
    chain_id: u64,
    gas_price: u128,
    gas_limit: u64,
) -> Result<Bytes, String> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price,
        gas_limit,
        to: TxKind::Call(to),
        value,
        input: calldata.into(),
    };
    let sig = signer
        .sign_transaction_sync(&mut tx)
        .map_err(|e| format!("sign_transaction_sync: {e}"))?;
    let signed = TransactionSigned::new_unhashed(Transaction::Legacy(tx), sig);
    let mut buf = Vec::with_capacity(512);
    signed.encode_2718(&mut buf);
    Ok(Bytes::from(buf))
}
