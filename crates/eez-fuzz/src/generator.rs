//! Structure-aware `raw_tx` generator.
//!
//! The address space is restricted by CONSTRUCTION: the fuzzer picks an *index*
//! into the live trigger dict, never a raw 20-byte address — so a mutation swaps
//! which trigger/method/signer fires (the 256-bit-address-EQ-no-gradient problem
//! never bites), and every input dispatches into the cross-chain path instead of
//! no-op'ing. See `docs/FUZZ_TESTING.md`.

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_eips::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use arbitrary::Arbitrary;

/// A 4-byte selector + count of 32-byte static (uint256-shaped) args for one
/// trigger method. Declarative ABI table; dynamic args are out of scope.
#[derive(Clone, Debug)]
pub struct CallSpec {
    pub selector: [u8; 4],
    pub static_args: usize,
    /// Whether the trigger method accepts `msg.value`. Sending value to a
    /// non-payable method reverts before the proxy call fires (→ `EmptyCalls`),
    /// so the generator only attaches value when this is set.
    pub payable: bool,
}

impl CallSpec {
    /// A non-payable method from a canonical signature, e.g.
    /// `"setViaProxy(uint256)"`.
    pub fn from_sig(sig: &str, static_args: usize) -> Self {
        let mut selector = [0u8; 4];
        selector.copy_from_slice(&keccak256(sig.as_bytes())[..4]);
        Self {
            selector,
            static_args,
            payable: false,
        }
    }
}

/// One trigger contract (reaches a proxy when called) + its callable methods.
#[derive(Clone, Debug)]
pub struct Trigger {
    pub address: Address,
    pub calls: Vec<CallSpec>,
}

/// Runtime dictionary the world fixture hands the generator: live triggers,
/// fixture signing keys, chain id.
#[derive(Debug)]
pub struct Dict {
    pub chain_id: u64,
    pub triggers: Vec<Trigger>,
    pub keys: Vec<PrivateKeySigner>,
}

/// Structure-aware fuzz input: indices into the dict + typed leaves. Fixed
/// width keeps libFuzzer mutations byte-stable (one byte → one choice).
#[derive(Debug, Arbitrary)]
pub struct FuzzTx {
    pub trigger_sel: u16,
    pub method_sel: u8,
    pub signer_sel: u8,
    pub nonce: u64,
    pub value: u64,
    pub args: [u128; 4],
}

impl FuzzTx {
    /// Resolve indices against `dict` and return the signed EIP-2718 `raw_tx`
    /// bytes (what `simulate_source_tx` decodes) PLUS the predicted settled
    /// value — the generator's own `set(x) -> x` model: `setViaProxy(v)` drives
    /// `Value.value = v`, so the predicted slot-0 effect is the first arg `v`.
    /// Handing the answer back with the input keeps the oracle independent of
    /// the composer (no separate reference needed for the controlled world).
    pub fn resolve_and_sign(&self, dict: &Dict) -> (Vec<u8>, U256) {
        let trig = &dict.triggers[(self.trigger_sel as usize) % dict.triggers.len()];
        let call = &trig.calls[(self.method_sel as usize) % trig.calls.len()];
        let signer = &dict.keys[(self.signer_sel as usize) % dict.keys.len()];

        let mut input = Vec::with_capacity(4 + 32 * call.static_args);
        input.extend_from_slice(&call.selector);
        for i in 0..call.static_args {
            let word = U256::from(self.args[i % self.args.len()]);
            input.extend_from_slice(&word.to_be_bytes::<32>());
        }
        let tx_value = if call.payable { U256::from(self.value) } else { U256::ZERO };
        let raw = sign_call(signer, dict.chain_id, self.nonce, trig.address, tx_value, input.into());
        let predicted = U256::from(self.args[0]);
        (raw, predicted)
    }
}

/// Sign a zero-fee EIP-1559 call tx (no balance needed; source-sim disables
/// the nonce check) and return its EIP-2718 wire bytes.
pub fn sign_call(
    signer: &PrivateKeySigner,
    chain_id: u64,
    nonce: u64,
    to: Address,
    value: U256,
    input: Bytes,
) -> Vec<u8> {
    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit: 5_000_000,
        max_fee_per_gas: 0,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(to),
        value,
        access_list: Default::default(),
        input,
    };
    let sig = signer.sign_transaction_sync(&mut tx).expect("sign tx");
    TxEnvelope::from(tx.into_signed(sig)).encoded_2718()
}
