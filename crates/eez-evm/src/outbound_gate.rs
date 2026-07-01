//! Outbound L2->L1 authorization gate — the SHARED soundness check both the
//! prover (A3, over the batch + its DA tx-list) and the deriver (A4, over the
//! reconciled batch + the same DA tx-list) run, so the two can NEVER drift.
//!
//! An outbound settlement entry (an immediate — `proxyEntryHash == 0` — carrying
//! non-empty `l2ToL1Calls`) claims an L2->L1 call L1 will EXECUTE, paying the
//! rollup's escrowed ether. The gate proves the L2 ACTUALLY authorized it by
//! cross-checking the entry against its paired, SIGNED L2 user tx — the one the
//! composer drained from the mempool and committed to DA (the `i`-th outbound
//! immediate pairs with the `i`-th Sync-block user tx; composer
//! drain==splice==DA order guarantees the match). A composer cannot forge a
//! user's ECDSA signature, so a phantom withdrawal (an entry with no real user
//! tx behind it) cannot satisfy all four binds:
//!
//!   1. `recover(user_tx) == call.sourceAddress`  — the EOA that signed it
//!   2. `user_tx.value      == call.value`        — the ether moved
//!   3. `user_tx.input      == call.data`         — the exact calldata
//!   4. `user_tx.to         == createProxyAddr(call.targetAddress, MAINNET)` —
//!      the deterministic CREATE2 cross-chain proxy for the L1 target, so the
//!      withdrawal cannot be redirected to a different target.
//!
//! NOTE on the discarded log model: the canonical outbound L2 block does NOT
//! emit `CrossChainCallExecuted` — the user's proxy call REVERTS in plain
//! re-execution (`executeCrossChainCall` -> `_consumeAndExecute` cannot complete
//! the L1 delivery without a cross-chain dispatcher), rolling back the
//! `emit` at `EEZL2.sol:200`. Composer and deriver both execute the committed
//! block plain, agree (so the follower converges), and the L2 ether debit is
//! booked by the settlement `StateDelta`, not the reverted burn. A
//! re-executed-log gate would therefore reject every legitimate withdrawal;
//! the signed-tx bind above is the correct, reproducible, log-free check.

use alloy_consensus::Transaction as _;
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::{Address, B256, Bytes, U256, address, hex, keccak256};
use reth_ethereum_primitives::TransactionSigned;
use reth_primitives_traits::SignedTransaction as _;

use crate::types::ExecutionEntrySol;

/// Canonical L2 cross-chain manager (EEZL2) predeploy — the CREATE2 deployer of
/// every L2 cross-chain proxy (`EEZBase.computeCrossChainProxyAddress` uses
/// `address(this)` = this address as both the deployer and a constructor arg).
pub const EEZL2_ADDR: Address = address!("4200000000000000000000000000000000000007");

/// `RollupId(0)` — MAINNET. An L2->L1 outbound's L1 target lives on mainnet, so
/// its proxy's `originalRollupId` (the salt + ctor-arg rollup id) is 0.
const MAINNET_ROLLUP_ID: u64 = 0;

/// `type(CrossChainProxy).creationCode` — the EXACT creation bytecode the
/// foundry build emits for `sync-rollups-protocol/src/base/CrossChainProxy.sol`
/// (1111 bytes). Concatenated with the abi-encoded ctor args it is the CREATE2
/// init code whose hash determines the proxy address. Pinned with the contracts
/// submodule; the `create2_matches_onchain_proxy` test recomputes a known
/// proxy address to catch any drift after a contract/compiler bump.
const PROXY_CREATION_CODE: &[u8] = &hex!(
    "60e03461009557601f61045738819003918201601f19168301916001600160401b038311\
        848410176100995780849260609460405283398101031261009557610047816100ad565b\
        906040610056602083016100ad565b9101519160805260a05260c0526040516103959081\
        6100c28239608051818181610140015281816102ac015261032f015260a05181505060c0\
        51815050f35b5f80fd5b634e487b7160e01b5f52604160045260245ffd5b519060016001\
        60a01b03821682036100955756fe60806040526004361061023e575f3560e01c8063532f\
        08391461002b57639f149e1b0361023e57610096565b6040366003190112610092576004\
        356001600160a01b0381168103610092576024359067ffffffffffffffff821161009257\
        366023830112156100925781600401359167ffffffffffffffff83116100925736602484\
        83010111610092576024019061013d565b5f80fd5b34610092575f366003190112610092\
        573330036100b2575f805d005b61023e565b634e487b7160e01b5f52604160045260245f\
        fd5b90601f8019910116810190811067ffffffffffffffff8211176100ed57604052565b\
        6100b7565b67ffffffffffffffff81116100ed57601f01601f191660200190565b3d1561\
        0138573d9061011f826100f2565b9161012d60405193846100cb565b82523d5f60208401\
        3e565b606090565b337f0000000000000000000000000000000000000000000000000000\
        0000000000006001600160a01b0316036100b257825f9392849360405192839283378101\
        848152039134905af161018e61010e565b901561019c57602081519101f35b6020815191\
        01fd5b6001600160a01b0390911681526040602082018190528101829052606091805f84\
        8401375f828201840152601f01601f1916010190565b6020818303126100925780519067\
        ffffffffffffffff8211610092570181601f820112156100925780519061020f826100f2\
        565b9261021d60405194856100cb565b8284526020838301011161009257815f92602080\
        93018386015e8301015290565b5f806040516020810190639f149e1b60e01b8252600481\
        526102616024826100cb565b519082305af161026f61010e565b50610304575f80604051\
        6020810190633d526d1760e11b82526102a88161029a3633602484016101a4565b03601f\
        1981018352826100cb565b51907f00000000000000000000000000000000000000000000\
        000000000000000000005afa6102d561010e565b90805b6102ea575b1561019c57602081\
        519101f35b90806020806102fe935183010191016101db565b906102dd565b5f80604051\
        6020810190639af5325960e01b825261032a8161029a3633602484016101a4565b519034\
        7f00000000000000000000000000000000000000000000000000000000000000005af161\
        035861010e565b90806102d856fea2646970667358221220ccb81427bb41bbb8a276a72f\
        0ded5edb9066871bffff6541a80c99966e5f18ae64736f6c63430008220033"
);

/// Recompute the deterministic CREATE2 cross-chain proxy address for
/// `original_addr` on `original_rollup_id`, the byte-for-byte mirror of
/// `EEZBase.computeCrossChainProxyAddress` (`EEZBase.sol:158-169`):
///
/// ```text
/// salt         = keccak256(abi.encodePacked(uint256 originalRollupId, address originalAddress))
/// bytecodeHash = keccak256(creationCode ++ abi.encode(EEZL2, originalAddress, originalRollupId))
/// proxy        = CREATE2(EEZL2, salt, bytecodeHash)
/// ```
#[must_use]
pub fn compute_cross_chain_proxy_address(
    original_addr: Address,
    original_rollup_id: u64,
) -> Address {
    let rollup_word = U256::from(original_rollup_id).to_be_bytes::<32>();

    // salt = keccak256(abi.encodePacked(uint256 rollupId, address addr)) — 52 bytes.
    let mut salt_pre = Vec::with_capacity(32 + 20);
    salt_pre.extend_from_slice(&rollup_word);
    salt_pre.extend_from_slice(original_addr.as_slice());
    let salt = keccak256(&salt_pre);

    // bytecodeHash = keccak256(creationCode ++ abi.encode(EEZL2, addr, rollupId)).
    // abi.encode of (address, address, uint256) = three left-padded 32-byte words.
    let mut init = Vec::with_capacity(PROXY_CREATION_CODE.len() + 96);
    init.extend_from_slice(PROXY_CREATION_CODE);
    init.extend_from_slice(EEZL2_ADDR.into_word().as_slice());
    init.extend_from_slice(original_addr.into_word().as_slice());
    init.extend_from_slice(&rollup_word);
    let bytecode_hash = keccak256(&init);

    EEZL2_ADDR.create2(salt, bytecode_hash)
}

/// Verify every OUTBOUND settlement entry is authorized by its paired, SIGNED
/// Sync-block user tx. `outbound_entries` MUST be the outbound immediates only
/// (`proxyEntryHash == 0`, non-empty `l2ToL1Calls`) in DA order, and `user_txs`
/// the Sync-block's user txs they pair with positionally (`outbound_entries[i]`
/// <-> `user_txs[i]`). `l2_rollup_id` is this L2's own id, which each entry's
/// `sourceRollupId` must equal.
///
/// # Errors
/// The first outbound entry whose paired user tx is missing, undecodable,
/// unrecoverable, or fails any of the four binds (signer / value / data / proxy
/// target) — a phantom or tampered withdrawal.
pub fn verify_outbound_authorized(
    outbound_entries: &[ExecutionEntrySol],
    user_txs: &[Bytes],
    l2_rollup_id: u64,
) -> Result<(), String> {
    for (i, entry) in outbound_entries.iter().enumerate() {
        // Defensive: callers pass outbound immediates only. A non-outbound entry
        // here is a pairing bug, not a phantom — surface it loudly.
        if entry.proxyEntryHash != B256::ZERO {
            return Err(format!(
                "outbound gate misused: entry {i} has non-zero proxyEntryHash (not an outbound immediate)"
            ));
        }
        let Some(call) = entry.l2ToL1Calls.first() else {
            return Err(format!(
                "outbound gate misused: entry {i} has empty l2ToL1Calls (not an outbound immediate)"
            ));
        };
        // N>=2 multi-call outbound is parked + rejected upstream
        // (system_tx::reject_multicall); one call per immediate here.

        let raw = user_txs.get(i).ok_or_else(|| {
            format!(
                "outbound entry {i} (target {:#x}, value {}) has NO paired user tx \
                 ({} outbound entries vs {} user txs) — phantom withdrawal",
                call.targetAddress,
                call.value,
                outbound_entries.len(),
                user_txs.len(),
            )
        })?;

        let mut slice: &[u8] = raw.as_ref();
        let tx = TransactionSigned::decode_2718(&mut slice)
            .map_err(|e| format!("outbound entry {i}: paired user tx is undecodable: {e}"))?;
        let value = tx.value();
        let data = tx.input().clone();
        let to = tx.to();
        let signer = tx
            .try_into_recovered()
            .map_err(|_| format!("outbound entry {i}: paired user tx signer unrecoverable"))?
            .signer();

        // 1. signer == claimed source EOA (the bind a composer cannot forge)
        if signer != call.sourceAddress {
            return Err(format!(
                "outbound entry {i}: paired user tx signer {signer:#x} != claimed source \
                 {:#x} — phantom withdrawal",
                call.sourceAddress,
            ));
        }
        // 2. source rollup id == this L2
        if call.sourceRollupId != U256::from(l2_rollup_id) {
            return Err(format!(
                "outbound entry {i}: sourceRollupId {} != this L2 {l2_rollup_id}",
                call.sourceRollupId,
            ));
        }
        // 3. value moved matches
        if value != call.value {
            return Err(format!(
                "outbound entry {i}: paired user tx value {value} != claimed {} — tampered withdrawal",
                call.value,
            ));
        }
        // 4. calldata matches
        if data.as_ref() != call.data.as_ref() {
            return Err(format!(
                "outbound entry {i}: paired user tx calldata != claimed data — tampered withdrawal"
            ));
        }
        // 5. tx.to is the deterministic proxy for the CLAIMED L1 target ON
        //    MAINNET. This is the single check that binds BOTH the target address
        //    AND the target rollup: `compute_cross_chain_proxy_address(target,
        //    MAINNET=0)` is the CREATE2 address of the proxy EEZL2 deploys for
        //    `(target, originalRollupId=0)`, and CREATE2 is collision-resistant,
        //    so `tx.to == expected_proxy` PROVES the proxy maps to (target,
        //    rollup 0) — no separate `targetRollupId` field is needed or trusted.
        //    A redirected target (different address) OR a non-mainnet target
        //    (an L2->L2 outbound, whose proxy has originalRollupId != 0, not built
        //    today) yields a different proxy address and is rejected here. This
        //    ENFORCES the "target rollup = 0" invariant the prover would otherwise
        //    have to merely ASSUME (l2-to-l1-extension-plan.md:323).
        let expected_proxy =
            compute_cross_chain_proxy_address(call.targetAddress, MAINNET_ROLLUP_ID);
        if to != Some(expected_proxy) {
            return Err(format!(
                "outbound entry {i}: paired user tx to {to:?} != cross-chain proxy {expected_proxy:#x} \
                 for target {:#x} on MAINNET — redirected / non-mainnet-target / phantom withdrawal",
                call.targetAddress,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::L2ToL1CallSol;
    use alloy_consensus::TxLegacy;
    use alloy_primitives::TxKind;
    use alloy_signer_local::PrivateKeySigner;

    fn entry(proxy: B256, calls: Vec<L2ToL1CallSol>) -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: proxy,
            destinationRollupId: U256::from(1),
            callCount: U256::from(calls.len() as u64),
            l2ToL1Calls: calls,
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        }
    }

    /// The CREATE2 helper reproduces the on-chain proxy address EXACTLY. Known
    /// vector from `e2e_value_outbound`: target `0xdc64…f6c9`, rollupId 0 ->
    /// outbound proxy `0xe983…758e` (logged by the test). Guards against a
    /// `creationCode` drift after a contracts/compiler bump.
    #[test]
    fn create2_matches_onchain_proxy() {
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let got = compute_cross_chain_proxy_address(target, 0);
        assert_eq!(got, address!("e9833139709fa68c07023207df8da3be705f758e"));
    }

    /// Sign a legacy tx to `to` with `value`/`input` from `signer`, 2718-encoded.
    fn signed_tx(signer: &PrivateKeySigner, to: Address, value: U256, input: Vec<u8>) -> Bytes {
        use alloy_eips::eip2718::Encodable2718 as _;
        use alloy_network::TxSignerSync as _;
        use reth_ethereum_primitives::Transaction;
        let mut tx = TxLegacy {
            chain_id: Some(1u64),
            nonce: 0,
            gas_price: 1,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value,
            input: input.into(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).expect("sign");
        let signed = TransactionSigned::new_unhashed(Transaction::Legacy(tx), sig);
        let mut buf = Vec::new();
        signed.encode_2718(&mut buf);
        Bytes::from(buf)
    }

    #[test]
    fn gate_accepts_authorized_and_rejects_phantom_or_tampered() {
        let signer = PrivateKeySigner::random();
        let source = signer.address();
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let proxy = compute_cross_chain_proxy_address(target, 0);
        let value = U256::from(7u64);
        let data = vec![0x12u8, 0x34];

        let call = || L2ToL1CallSol {
            targetAddress: target,
            value,
            data: Bytes::from(data.clone()),
            sourceAddress: source,
            sourceRollupId: U256::from(1u64),
            revertSpan: U256::ZERO,
        };
        let entries = vec![entry(B256::ZERO, vec![call()])];
        let good = signed_tx(&signer, proxy, value, data.clone());

        // Authorized — every bind holds.
        assert!(verify_outbound_authorized(&entries, std::slice::from_ref(&good), 1).is_ok());

        // Phantom — no paired user tx.
        assert!(verify_outbound_authorized(&entries, &[], 1).is_err());

        // Wrong signer — a DIFFERENT EOA signed it.
        let other = PrivateKeySigner::random();
        let wrong_signer = signed_tx(&other, proxy, value, data.clone());
        assert!(verify_outbound_authorized(&entries, &[wrong_signer], 1).is_err());

        // Tampered value.
        let mut tv = entries.clone();
        tv[0].l2ToL1Calls[0].value = U256::from(999u64);
        assert!(verify_outbound_authorized(&tv, std::slice::from_ref(&good), 1).is_err());

        // Tampered data.
        let mut td = entries.clone();
        td[0].l2ToL1Calls[0].data = Bytes::from(vec![0xff]);
        assert!(verify_outbound_authorized(&td, std::slice::from_ref(&good), 1).is_err());

        // Redirected target — user signed a tx to the proxy of a DIFFERENT target.
        let other_target = address!("00000000000000000000000000000000000000bb");
        let mut tt = entries.clone();
        tt[0].l2ToL1Calls[0].targetAddress = other_target;
        // (proxy in `good` still points at `target`, not `other_target`.)
        assert!(verify_outbound_authorized(&tt, std::slice::from_ref(&good), 1).is_err());

        // Wrong source rollup id.
        assert!(verify_outbound_authorized(&entries, std::slice::from_ref(&good), 2).is_err());
    }

    /// The CREATE2 proxy-target bind ENFORCES "target rollup = MAINNET(0)" — it is
    /// not merely assumed (l2-to-l1-extension-plan.md:323). A withdrawal whose
    /// user tx called the proxy for a NON-mainnet target (an L2->L2 outbound, the
    /// proxy created with `originalRollupId != 0`) hits a DIFFERENT CREATE2
    /// address than `compute_cross_chain_proxy_address(target, 0)`, so the gate
    /// rejects it even though target address / value / data / signer all match.
    #[test]
    fn gate_enforces_mainnet_target_rollup() {
        let signer = PrivateKeySigner::random();
        let source = signer.address();
        let target = address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9");
        let value = U256::from(7u64);
        let data = vec![0x12u8, 0x34];

        // Distinct rollups => distinct proxy addresses for the SAME target.
        let mainnet_proxy = compute_cross_chain_proxy_address(target, 0);
        let l2l2_proxy = compute_cross_chain_proxy_address(target, 5);
        assert_ne!(
            mainnet_proxy, l2l2_proxy,
            "originalRollupId must change the CREATE2 proxy address"
        );

        let call = || L2ToL1CallSol {
            targetAddress: target,
            value,
            data: Bytes::from(data.clone()),
            sourceAddress: source,
            sourceRollupId: U256::from(1u64),
            revertSpan: U256::ZERO,
        };
        let entries = vec![entry(B256::ZERO, vec![call()])];

        // A user tx to the MAINNET proxy is accepted; the SAME tx re-pointed at the
        // rollup-5 proxy (a non-mainnet target) is rejected — the gate binds the
        // target rollup to 0 with no `targetRollupId` field to trust.
        let ok = signed_tx(&signer, mainnet_proxy, value, data.clone());
        assert!(verify_outbound_authorized(&entries, std::slice::from_ref(&ok), 1).is_ok());

        let non_mainnet = signed_tx(&signer, l2l2_proxy, value, data.clone());
        assert!(
            verify_outbound_authorized(&entries, std::slice::from_ref(&non_mainnet), 1).is_err(),
            "a withdrawal to a non-mainnet-target proxy must be rejected"
        );
    }
}
