use super::*;
use alloy_eips::eip7002::{
    WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS, WITHDRAWAL_REQUEST_PREDEPLOY_CODE,
};
use alloy_genesis::GenesisAccount;
use alloy_primitives::keccak256;
use eez_composer::local::{build_sync_block, sync_block_candidates};
use reth_primitives_traits::SealedHeader;
use reth_provider::test_utils::create_test_provider_factory_with_chain_spec;
use reth_trie_common::{HashBuilder, Nibbles, TrieAccount, proof::ProofRetainer};

#[test]
fn empty_composer_anchor_matches_both_provers_after_post_block_changes() {
    // Real database, EVM execution and trie roots throughout. TempDatabase owns
    // and removes its database/static-file directories when the provider drops.
    for excess in [0u64, 10] {
        let contract = WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS;
        let code = WITHDRAWAL_REQUEST_PREDEPLOY_CODE.clone();
        let storage = (excess != 0)
            .then_some((B256::ZERO, B256::from(U256::from(excess))))
            .into_iter()
            .collect();
        let genesis = Genesis {
            config: fixture_chain_config(),
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1),
            alloc: [(
                contract,
                GenesisAccount {
                    nonce: Some(1),
                    code: Some(code.clone()),
                    storage: Some(storage),
                    ..Default::default()
                },
            )]
            .into(),
            ..Default::default()
        };
        let stateless = Backend::from_genesis(genesis, TEST_SYSTEM_ADDRESS).unwrap();
        let chain_spec = stateless.chain_spec.clone();
        let provider = create_test_provider_factory_with_chain_spec(chain_spec.clone());
        let parent = SealedHeader::seal_slow(chain_spec.genesis_header().clone());
        assert_eq!(
            reth_db_common::init::init_genesis(&provider).unwrap(),
            parent.hash()
        );
        let provider = reth_provider::providers::BlockchainProvider::new(provider).unwrap();

        // Construct the witness independently from the builder, retaining the
        // actual pre-state account and storage trie nodes.
        let storage_key = Nibbles::unpack(keccak256(B256::ZERO));
        let mut storage_trie =
            HashBuilder::default().with_proof_retainer(ProofRetainer::new(vec![storage_key]));
        if excess != 0 {
            storage_trie.add_leaf(storage_key, &alloy_rlp::encode(U256::from(excess)));
        }
        let account = TrieAccount {
            nonce: 1,
            code_hash: keccak256(&code),
            storage_root: storage_trie.root(),
            ..Default::default()
        };
        let account_key = Nibbles::unpack(keccak256(contract));
        let mut account_trie =
            HashBuilder::default().with_proof_retainer(ProofRetainer::new(vec![account_key]));
        account_trie.add_leaf(account_key, &alloy_rlp::encode(account));
        assert_eq!(parent.state_root, account_trie.root());
        let mut state: Vec<Bytes> = account_trie
            .take_proof_nodes()
            .into_inner()
            .into_values()
            .collect();
        state.extend(storage_trie.take_proof_nodes().into_inner().into_values());
        let witness = ExecutionWitness {
            state,
            codes: vec![code],
            headers: vec![alloy_rlp::encode(parent.header()).into()],
            ..Default::default()
        };

        // With no queued requests, EIP-7002 still reduces excess by its target
        // of two. Assert the independently calculated final root so this test
        // cannot pass by making Composer skip the post-block call too.
        let final_excess = excess.saturating_sub(2);
        let final_storage = (final_excess != 0).then_some((B256::ZERO, U256::from(final_excess)));
        let expected_root = reth_trie_common::root::state_root_unhashed([(
            contract,
            TrieAccount {
                storage_root: reth_trie_common::root::storage_root_unhashed(final_storage),
                ..account
            },
        )]);
        let recipient = Address::repeat_byte(0xf0);
        let built =
            build_sync_block(&provider, &stateless.evm_config, &parent, 1, recipient, &[]).unwrap();
        assert_eq!(built.header.state_root, expected_root);
        assert_eq!(built.header.state_root == parent.state_root, excess == 0);
        assert!(built.block.body().transactions.is_empty());
        assert!(built.block.body().withdrawals.as_ref().unwrap().is_empty());
        assert_eq!(
            built.header.requests_hash,
            Some(alloy_eips::eip7685::EMPTY_REQUESTS_HASH)
        );
        let candidates =
            sync_block_candidates(&provider, &stateless.evm_config, &parent, 1, recipient, &[])
                .unwrap();
        assert_eq!(candidates.anchor, built.header.hash());
        assert!(candidates.per_effect.is_empty());

        let block = built.block.into_sealed_block().into_block();
        let input = admitted_block_with_witness(
            1,
            built.header.hash(),
            parent.hash(),
            alloy_rlp::encode(&block),
            witness.clone(),
        );
        let stateful = eez_prover_stateful::Backend::new(provider, chain_spec, TEST_SYSTEM_ADDRESS);
        let backends: [&dyn ValidationBackend; 2] = [&stateless, &stateful];
        for backend in backends {
            let output = backend
                .validate_blocks(
                    std::slice::from_ref(&input),
                    &mut [witness.clone()],
                    &CancellationToken::default(),
                )
                .unwrap_or_else(|error| {
                    panic!("{} rejected excess {excess}: {error}", backend.label())
                });
            assert_eq!(
                output.blocks[0].transaction_state_checkpoints,
                [StateCheckpoint {
                    at: CheckpointAt::PreExecution,
                    state_root: expected_root,
                    block_hash: candidates.anchor,
                }],
                "{} disagrees with Composer for excess {excess}",
                backend.label()
            );

            // The final checkpoint must only be returned after validating the
            // recomputed root; copying an unverified terminal hash is unsafe.
            let mut invalid = block.clone();
            invalid.header.state_root = B256::repeat_byte(0xee);
            let invalid = admitted_block_with_witness(
                1,
                invalid.header.hash_slow(),
                parent.hash(),
                alloy_rlp::encode(&invalid),
                witness.clone(),
            );
            assert!(
                matches!(
                    backend.validate_blocks(
                        &[invalid],
                        &mut [witness.clone()],
                        &CancellationToken::default(),
                    ),
                    Err(ValidationError::Rejected(_))
                ),
                "{} accepted an invalid state root",
                backend.label()
            );
        }
    }
}
