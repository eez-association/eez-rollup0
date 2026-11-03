//! Consensus balance cleanup after complete groups and at block finalization.
use alloy_evm::Database;
use alloy_primitives::{Address, U256};
use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS};
use revm::state::{Account, EvmState};

/// Protocol inventory and burn-receipt accounts. FeeVault is deliberately absent.
pub const PROTOCOL_BALANCE_ACCOUNTS: [Address; 2] = [EEZL2_ADDRESS, SYSTEM_ADDRESS];

/// Clear residual protocol ETH in a provisional state delta, retaining nonce,
/// code, storage and deletion flags. This performs no commit, call, log, gas
/// charge or refund. The caller must commit or discard the entire delta together.
///
/// Used at block finalization and after complete cross-chain groups. Looking
/// up an untouched account through the same DB includes it in execution witnesses.
/// Missing/zero accounts stay untouched. A database error leaves committed state
/// unchanged; discard the provisional delta on error.
pub fn clear_protocol_balances<DB: Database>(
    db: &mut DB,
    state: &mut EvmState,
) -> Result<(), DB::Error> {
    for address in PROTOCOL_BALANCE_ACCOUNTS {
        if let Some(account) = state.get_mut(&address) {
            if !account.info.balance.is_zero() {
                account.info.balance = U256::ZERO;
                account.mark_touch();
            }
        } else if let Some(info) = db.basic(address)?
            && !info.balance.is_zero()
        {
            let mut account = Account::from(info);
            account.info.balance = U256::ZERO;
            account.mark_touch();
            state.insert(address, account);
        }
    }
    Ok(())
}
