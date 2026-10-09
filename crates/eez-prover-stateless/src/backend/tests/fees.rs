use super::*;
use alloy_consensus::{BlockBody, TxReceipt as _, constants::EMPTY_OMMER_ROOT_HASH, proofs};
use alloy_primitives::{TxKind, keccak256};
use reth_trie_common::{HashBuilder, Nibbles, TrieAccount, proof::ProofRetainer};

#[test]
fn collector_credits_are_committed_by_stateless_replay_and_transaction_checkpoints() {
    let collector = Address::repeat_byte(0xc0);
    let beneficiary = Address::repeat_byte(0xb0);
    let recipient = Address::repeat_byte(0xa0);
    let mut chain_config = fixture_chain_config();
    // London fee accounting, without later pre-execution system contracts.
    chain_config.shanghai_time = None;
    chain_config.cancun_time = None;
    chain_config.prague_time = None;
    chain_config.osaka_time = None;
    chain_config.extra_fields.insert(
        "feeCollector".into(),
        serde_json::to_value(collector).unwrap(),
    );
    let backend = Backend::new(chain_config, TEST_SYSTEM_ADDRESS);
    let gas_used = 21_000;
    let transaction: TransactionSigned = TxLegacy {
        chain_id: Some(1),
        gas_limit: gas_used,
        gas_price: 10,
        to: TxKind::Call(recipient),
        value: U256::from(13),
        ..Default::default()
    }
    .into_signed(Signature::test_signature())
    .into();
    let sender = transaction.recover_signer().unwrap();
    let receipt = EthereumReceipt {
        tx_type: eez_primitives::EezTxType::Ethereum(alloy_consensus::TxType::Legacy),
        success: true,
        cumulative_gas_used: gas_used,
        logs: vec![],
    };

    for initial_collector_balance in [None, Some(11u64)] {
        let mut accounts = vec![(
            keccak256(sender),
            TrieAccount {
                balance: U256::from(1_000_000),
                ..Default::default()
            },
        )];
        if let Some(balance) = initial_collector_balance {
            accounts.push((
                keccak256(collector),
                TrieAccount {
                    balance: U256::from(balance),
                    ..Default::default()
                },
            ));
        }
        accounts.sort_unstable_by_key(|(key, _)| *key);
        let targets = [sender, collector, beneficiary, recipient]
            .map(|address| Nibbles::unpack(keccak256(address)))
            .to_vec();
        let mut trie = HashBuilder::default().with_proof_retainer(ProofRetainer::new(targets));
        for (key, account) in accounts {
            trie.add_leaf(Nibbles::unpack(key), &alloy_rlp::encode(account));
        }
        let parent = Header {
            state_root: trie.root(),
            beneficiary,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            gas_limit: 30_000_000,
            // At the target, the next block retains this base fee.
            gas_used: 15_000_000,
            base_fee_per_gas: Some(7),
            ..Default::default()
        };
        let witness = ExecutionWitness {
            state: trie.take_proof_nodes().into_inner().into_values().collect(),
            headers: vec![alloy_rlp::encode(&parent).into()],
            ..Default::default()
        };
        // Independent accounting: sender pays 210,000; tip is 63,000;
        // base fee is 147,000; the recipient receives the transferred 13.
        let post_accounts = [
            (
                sender,
                TrieAccount {
                    nonce: 1,
                    balance: U256::from(1_000_000 - 210_000 - 13),
                    ..Default::default()
                },
            ),
            (
                recipient,
                TrieAccount {
                    balance: U256::from(13),
                    ..Default::default()
                },
            ),
            (
                beneficiary,
                TrieAccount {
                    balance: U256::from(63_000),
                    ..Default::default()
                },
            ),
        ];
        let burned_root =
            reth_trie_common::root::state_root_unhashed(post_accounts.into_iter().chain(
                initial_collector_balance.map(|balance| {
                    (
                        collector,
                        TrieAccount {
                            balance: U256::from(balance),
                            ..Default::default()
                        },
                    )
                }),
            ));
        let collected_root =
            reth_trie_common::root::state_root_unhashed(post_accounts.into_iter().chain([(
                collector,
                TrieAccount {
                    balance: U256::from(initial_collector_balance.unwrap_or_default() + 147_000),
                    ..Default::default()
                },
            )]));
        assert_ne!(collected_root, burned_root);
        let block = Block {
            header: Header {
                parent_hash: parent.hash_slow(),
                number: 1,
                timestamp: 1,
                state_root: collected_root,
                gas_used,
                transactions_root: proofs::calculate_transaction_root(std::slice::from_ref(
                    &transaction,
                )),
                receipts_root: proofs::calculate_receipt_root(&[receipt.with_bloom_ref()]),
                ..parent
            },
            body: BlockBody {
                transactions: vec![transaction.clone()],
                ..Default::default()
            },
        };
        let recovered = RecoveredBlock::try_recover(block.clone()).unwrap();
        let ordinary = stateless_validation_recovered(
            recovered.clone(),
            witness.clone(),
            backend.chain_spec.clone(),
            backend.evm_config.clone(),
        )
        .unwrap();
        let detailed = stateless_validation_recovered_with_state_checkpoints(
            recovered,
            witness.clone(),
            backend.chain_spec.clone(),
            backend.evm_config.clone(),
            &[stateless_reth::CheckpointAt::Transaction(0)],
        )
        .unwrap();
        assert_eq!(detailed.validation, ordinary);
        assert_eq!(ordinary.post_state_root, collected_root);
        assert_eq!(
            detailed.checkpoints.checkpoints[0].state_root,
            collected_root
        );
        assert_eq!(
            detailed.checkpoints.checkpoints[0].block_hash,
            block.header.hash_slow()
        );

        let admitted = admitted_block_with_witness(
            1,
            block.header.hash_slow(),
            block.header.parent_hash,
            alloy_rlp::encode(&block),
            witness.clone(),
        );
        assert_eq!(
            backend.validate(vec![admitted]).unwrap().blocks[0].post_state_root,
            collected_root
        );

        let mut burned = block;
        burned.header.state_root = burned_root;
        let error = stateless_validation_recovered(
            RecoveredBlock::try_recover(burned).unwrap(),
            witness,
            backend.chain_spec.clone(),
            backend.evm_config.clone(),
        )
        .unwrap_err();
        assert!(
            matches!(error, StatelessValidationError::PostStateRootMismatch { got, expected } if got == collected_root && expected == burned_root)
        );
    }
}
