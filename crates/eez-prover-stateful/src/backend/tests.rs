use std::ops::RangeBounds;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_consensus::constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_ROOT_HASH};
use alloy_consensus::{BlockBody, Header, SignableTransaction as _, TxLegacy};
use alloy_eips::{BlockNumHash, BlockNumberOrTag};
use alloy_primitives::{Bytes, Signature, TxKind, U256};
use eez_proof_signer::window::testing::admitted_block_from_consensus_rlp;
use reth_chainspec::ChainInfo;
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
use reth_storage_api::errors::provider::ProviderResult;
use reth_storage_api::{BlockIdReader, StateProviderBox};

use super::*;

fn admitted(number: u64, parent_hash: B256, state_root: B256) -> AdmittedBlock {
    admitted_block(Block {
        header: Header {
            parent_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            state_root,
            transactions_root: EMPTY_ROOT_HASH,
            receipts_root: EMPTY_ROOT_HASH,
            number,
            gas_limit: 30_000_000,
            timestamp: number,
            ..Default::default()
        },
        body: BlockBody::default(),
    })
}

fn admitted_block(block: Block) -> AdmittedBlock {
    let number = block.header.number;
    let parent_hash = block.header.parent_hash;
    let hash = block.header.hash_slow();
    admitted_block_from_consensus_rlp(number, hash, parent_hash, alloy_rlp::encode(block))
}

fn provider_with_anchor(number: u64, hash: B256, state_root: B256) -> MockEthProvider {
    let provider = MockEthProvider::new();
    provider.add_header(
        hash,
        Header {
            number,
            state_root,
            gas_limit: 30_000_000,
            ..Default::default()
        },
    );
    provider
}

#[derive(Debug)]
struct CanonicalView {
    height: u64,
    hashes: Vec<(u64, B256)>,
}

impl BlockHashReader for CanonicalView {
    fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
        Ok(self
            .hashes
            .iter()
            .find_map(|(candidate, hash)| (*candidate == number).then_some(*hash)))
    }

    fn canonical_hashes_range(&self, start: u64, end: u64) -> ProviderResult<Vec<B256>> {
        Ok(self
            .hashes
            .iter()
            .filter_map(|(number, hash)| (start..end).contains(number).then_some(*hash))
            .collect())
    }
}

impl BlockNumReader for CanonicalView {
    fn chain_info(&self) -> ProviderResult<ChainInfo> {
        Ok(ChainInfo::default())
    }

    fn best_block_number(&self) -> ProviderResult<u64> {
        Ok(self.height)
    }

    fn last_block_number(&self) -> ProviderResult<u64> {
        Ok(self.height)
    }

    fn block_number(&self, hash: B256) -> ProviderResult<Option<u64>> {
        Ok(self
            .hashes
            .iter()
            .find_map(|(number, candidate)| (*candidate == hash).then_some(*number)))
    }
}

/// Provider whose canonical anchor changes on the post-replay re-check.
struct ReorgingProvider {
    inner: MockEthProvider,
    anchor_hash: B256,
    reorged_anchor_hash: B256,
    anchor_reads: AtomicUsize,
}

impl BlockHashReader for ReorgingProvider {
    fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
        if number == 0 {
            let reads = self.anchor_reads.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(if reads == 0 {
                self.anchor_hash
            } else {
                self.reorged_anchor_hash
            }));
        }
        self.inner.block_hash(number)
    }

    fn canonical_hashes_range(&self, start: u64, end: u64) -> ProviderResult<Vec<B256>> {
        self.inner.canonical_hashes_range(start, end)
    }
}

impl BlockNumReader for ReorgingProvider {
    fn chain_info(&self) -> ProviderResult<ChainInfo> {
        self.inner.chain_info()
    }

    fn best_block_number(&self) -> ProviderResult<u64> {
        self.inner.best_block_number()
    }

    fn last_block_number(&self) -> ProviderResult<u64> {
        self.inner.last_block_number()
    }

    fn block_number(&self, hash: B256) -> ProviderResult<Option<u64>> {
        self.inner.block_number(hash)
    }
}

impl HeaderProvider for ReorgingProvider {
    type Header = Header;

    fn header(&self, block_hash: B256) -> ProviderResult<Option<Self::Header>> {
        self.inner.header(block_hash)
    }

    fn header_by_number(&self, number: u64) -> ProviderResult<Option<Self::Header>> {
        self.inner.header_by_number(number)
    }

    fn headers_range(&self, range: impl RangeBounds<u64>) -> ProviderResult<Vec<Self::Header>> {
        self.inner.headers_range(range)
    }

    fn sealed_header(&self, number: u64) -> ProviderResult<Option<SealedHeader<Self::Header>>> {
        self.inner.sealed_header(number)
    }

    fn sealed_headers_while(
        &self,
        range: impl RangeBounds<u64>,
        predicate: impl FnMut(&SealedHeader<Self::Header>) -> bool,
    ) -> ProviderResult<Vec<SealedHeader<Self::Header>>> {
        self.inner.sealed_headers_while(range, predicate)
    }
}

impl StateProviderFactory for ReorgingProvider {
    fn latest(&self) -> ProviderResult<StateProviderBox> {
        self.inner.latest()
    }

    fn state_by_block_number_or_tag(
        &self,
        number_or_tag: BlockNumberOrTag,
    ) -> ProviderResult<StateProviderBox> {
        self.inner.state_by_block_number_or_tag(number_or_tag)
    }

    fn history_by_block_number(&self, block: u64) -> ProviderResult<StateProviderBox> {
        self.inner.history_by_block_number(block)
    }

    fn history_by_block_hash(&self, block: B256) -> ProviderResult<StateProviderBox> {
        self.inner.history_by_block_hash(block)
    }

    fn state_by_block_hash(&self, block: B256) -> ProviderResult<StateProviderBox> {
        self.inner.state_by_block_hash(block)
    }

    fn pending(&self) -> ProviderResult<StateProviderBox> {
        self.inner.pending()
    }

    fn pending_state_by_hash(&self, block_hash: B256) -> ProviderResult<Option<StateProviderBox>> {
        self.inner.pending_state_by_hash(block_hash)
    }

    fn maybe_pending(&self) -> ProviderResult<Option<StateProviderBox>> {
        self.inner.maybe_pending()
    }
}

impl BlockIdReader for ReorgingProvider {
    fn pending_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
        self.inner.pending_block_num_hash()
    }

    fn safe_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
        self.inner.safe_block_num_hash()
    }

    fn finalized_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
        self.inner.finalized_block_num_hash()
    }
}

#[test]
fn backend_identity_is_fixed_at_construction() {
    let chain_spec = Arc::new(ChainSpec::default());
    let chain_id = chain_spec.chain().id();
    let l2_address = Address::repeat_byte(9);
    let provider: MockEthProvider = MockEthProvider::new();
    let backend = Backend::new(provider, chain_spec, l2_address);

    assert_eq!(backend.chain_id(), chain_id);
    assert_eq!(backend.expected_l2_system_address(), l2_address);
}

#[test]
fn checkpoint_plan_is_not_limited_before_the_provider_is_read() {
    let chain_spec = Arc::new(ChainSpec::default());
    let transaction = TxLegacy::default()
        .into_signed(Signature::test_signature())
        .into();
    let blocks = [admitted_block(Block {
        header: Header {
            number: 1,
            gas_limit: 30_000_000,
            timestamp: 1,
            ..Default::default()
        },
        body: BlockBody {
            transactions: vec![transaction],
            ..Default::default()
        },
    })];
    // This provider has no canonical head. Reaching it proves the complete
    // checkpoint plan was accepted without an operator-configured ceiling.
    let provider: MockEthProvider = MockEthProvider::new();

    let error = validate_blocks(
        &provider,
        &chain_spec,
        &EthEvmConfig::new(Arc::clone(&chain_spec)),
        Address::repeat_byte(9),
        &blocks,
        &CancellationToken::default(),
    )
    .unwrap_err();

    assert!(matches!(error, ValidationError::Unavailable(_)));
}

#[test]
fn execution_errors_preserve_retryability() {
    let invalid_composition = execution_error(BlockExecutionError::Validation(
        reth_evm::block::BlockValidationError::msg("invalid transaction"),
    ));
    assert!(matches!(invalid_composition, ValidationError::Rejected(_)));

    let backend_fault = execution_error(BlockExecutionError::msg("database unavailable"));
    assert!(matches!(backend_fault, ValidationError::Unavailable(_)));
}

#[test]
fn empty_checkpoint_execution_retains_transaction_state() {
    let chain_spec = Arc::new(ChainSpec::default());
    let evm_config = EthEvmConfig::new(Arc::clone(&chain_spec));
    let recipient = Address::repeat_byte(0x22);
    let signed = TxLegacy {
        nonce: 0,
        gas_price: 1,
        gas_limit: 21_000,
        to: TxKind::Call(recipient),
        value: U256::from(1),
        ..Default::default()
    }
    .into_signed(Signature::test_signature());
    let sender = signed.recover_signer().unwrap();
    let block = RecoveredBlock::new_unhashed(
        Block {
            header: Header {
                number: 1,
                gas_limit: 30_000_000,
                timestamp: 1,
                ..Default::default()
            },
            body: BlockBody {
                transactions: vec![signed.into()],
                ..Default::default()
            },
        },
        vec![sender],
    );
    let provider: MockEthProvider = MockEthProvider::new();
    provider.add_account(sender, ExtendedAccount::new(0, U256::from(u64::MAX)));
    let mut state = State::builder()
        .with_database(StateProviderDatabase::new(
            Box::new(provider) as Box<dyn StateProvider + Send>
        ))
        .with_bundle_update()
        .build();

    execute_block(&evm_config, &mut state, &block).unwrap();

    assert_eq!(
        state
            .bundle_state
            .account(&sender)
            .and_then(|account| account.info.as_ref())
            .map(|account| account.nonce),
        Some(1),
        "an empty checkpoint plan must still retain the transaction's sender-nonce update",
    );
}

#[test]
fn follower_behind_the_required_anchor_is_retryable() {
    let chain_spec = Arc::new(ChainSpec::default());
    let provider = provider_with_anchor(0, B256::repeat_byte(1), B256::repeat_byte(2));
    let blocks = [admitted(2, B256::repeat_byte(3), B256::repeat_byte(4))];

    let error = validate_blocks(
        &provider,
        &chain_spec,
        &EthEvmConfig::new(Arc::clone(&chain_spec)),
        Address::repeat_byte(9),
        &blocks,
        &CancellationToken::default(),
    )
    .unwrap_err();

    assert!(matches!(error, ValidationError::Unavailable(_)));
}

#[test]
fn conflicting_local_anchor_is_fatal() {
    let chain_spec = Arc::new(ChainSpec::default());
    let provider = provider_with_anchor(0, B256::repeat_byte(1), B256::repeat_byte(2));
    let blocks = [admitted(1, B256::repeat_byte(3), B256::repeat_byte(4))];

    let error = validate_blocks(
        &provider,
        &chain_spec,
        &EthEvmConfig::new(Arc::clone(&chain_spec)),
        Address::repeat_byte(9),
        &blocks,
        &CancellationToken::default(),
    )
    .unwrap_err();

    assert!(matches!(error, ValidationError::Rejected(_)));
}

#[test]
fn conflicting_known_block_is_fatal() {
    let chain_spec = Arc::new(ChainSpec::default());
    let anchor_hash = B256::repeat_byte(1);
    let provider = provider_with_anchor(0, anchor_hash, B256::repeat_byte(2));
    provider.add_header(
        B256::repeat_byte(5),
        Header {
            number: 1,
            parent_hash: anchor_hash,
            gas_limit: 30_000_000,
            ..Default::default()
        },
    );
    let blocks = [admitted(1, anchor_hash, B256::repeat_byte(4))];

    let error = validate_blocks(
        &provider,
        &chain_spec,
        &EthEvmConfig::new(Arc::clone(&chain_spec)),
        Address::repeat_byte(9),
        &blocks,
        &CancellationToken::default(),
    )
    .unwrap_err();

    assert!(matches!(error, ValidationError::Rejected(_)));
    assert!(error.to_string().contains("request proposes"));
}

#[test]
fn anchor_reorg_during_replay_is_aborted() {
    let anchor_hash = B256::repeat_byte(1);
    let provider = CanonicalView {
        height: 0,
        hashes: vec![(0, B256::repeat_byte(2))],
    };
    let blocks = [admitted(1, anchor_hash, B256::repeat_byte(3))];

    let error = check_canonical_snapshot(&provider, 0, anchor_hash, 0, &blocks).unwrap_err();

    assert!(matches!(error, ValidationError::Aborted(_)));
    assert!(error.to_string().contains("canonical anchor"));
}

#[test]
fn newly_acquired_conflicting_block_is_aborted() {
    let anchor_hash = B256::repeat_byte(1);
    let local_block_hash = B256::repeat_byte(2);
    let blocks = [admitted(1, anchor_hash, B256::repeat_byte(3))];
    assert_ne!(blocks[0].claimed_hash(), local_block_hash);
    let provider = CanonicalView {
        height: 1,
        hashes: vec![(0, anchor_hash), (1, local_block_hash)],
    };

    let error = check_canonical_snapshot(&provider, 0, anchor_hash, 0, &blocks).unwrap_err();

    assert!(matches!(error, ValidationError::Aborted(_)));
    assert!(error.to_string().contains("canonical block 1"));
}

#[test]
fn anchor_reorg_is_aborted_through_the_complete_validation_path() {
    let chain_spec = Arc::new(ChainSpec::default());
    let anchor_hash = B256::repeat_byte(1);
    let anchor_root = B256::repeat_byte(2);
    let terminal_root = B256::repeat_byte(3);
    let inner = provider_with_anchor(0, anchor_hash, anchor_root);
    inner.add_state_root(terminal_root);
    let provider = ReorgingProvider {
        inner,
        anchor_hash,
        reorged_anchor_hash: B256::repeat_byte(4),
        anchor_reads: AtomicUsize::new(0),
    };
    let blocks = [admitted(1, anchor_hash, terminal_root)];

    let error = validate_blocks(
        &provider,
        &chain_spec,
        &EthEvmConfig::new(Arc::clone(&chain_spec)),
        Address::repeat_byte(9),
        &blocks,
        &CancellationToken::default(),
    )
    .unwrap_err();

    assert!(matches!(error, ValidationError::Aborted(_)));
    assert!(error.to_string().contains("canonical anchor"));
}

#[test]
fn invalid_standalone_header_is_fatal() {
    let chain_spec = Arc::new(ChainSpec::default());
    let anchor_hash = B256::repeat_byte(1);
    let anchor_root = B256::repeat_byte(2);
    let provider = provider_with_anchor(0, anchor_hash, anchor_root);
    let blocks = [admitted_block(Block {
        header: Header {
            parent_hash: anchor_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            state_root: B256::repeat_byte(3),
            transactions_root: EMPTY_ROOT_HASH,
            receipts_root: EMPTY_ROOT_HASH,
            number: 1,
            gas_limit: 30_000_000,
            timestamp: 1,
            extra_data: Bytes::from(vec![0; 33]),
            ..Default::default()
        },
        body: BlockBody::default(),
    })];

    let error = validate_blocks(
        &provider,
        &chain_spec,
        &EthEvmConfig::new(Arc::clone(&chain_spec)),
        Address::repeat_byte(9),
        &blocks,
        &CancellationToken::default(),
    )
    .unwrap_err();

    assert!(matches!(error, ValidationError::Rejected(_)));
    assert!(error.to_string().contains("extra data"));
}

#[test]
fn replays_an_empty_terminal_block_from_local_anchor_state() {
    let chain_spec = Arc::new(ChainSpec::default());
    let anchor_hash = B256::repeat_byte(1);
    let anchor_root = B256::repeat_byte(2);
    let terminal_root = B256::repeat_byte(3);
    let provider = provider_with_anchor(0, anchor_hash, anchor_root);
    provider.add_state_root(terminal_root);
    let blocks = [admitted(1, anchor_hash, terminal_root)];

    let output = validate_blocks(
        &provider,
        &chain_spec,
        &EthEvmConfig::new(Arc::clone(&chain_spec)),
        Address::repeat_byte(9),
        &blocks,
        &CancellationToken::default(),
    )
    .unwrap();

    assert_eq!(output.pre_state_root, anchor_root);
    assert_eq!(output.blocks.len(), 1);
    assert_eq!(output.blocks[0].post_state_root, terminal_root);
}

#[test]
fn replayed_state_root_must_match_the_block_header() {
    let chain_spec = Arc::new(ChainSpec::default());
    let anchor_hash = B256::repeat_byte(1);
    let anchor_root = B256::repeat_byte(2);
    let computed_root = B256::repeat_byte(3);
    let claimed_root = B256::repeat_byte(4);
    let provider = provider_with_anchor(0, anchor_hash, anchor_root);
    provider.add_state_root(computed_root);
    let blocks = [admitted(1, anchor_hash, claimed_root)];

    let error = validate_blocks(
        &provider,
        &chain_spec,
        &EthEvmConfig::new(Arc::clone(&chain_spec)),
        Address::repeat_byte(9),
        &blocks,
        &CancellationToken::default(),
    )
    .unwrap_err();

    assert!(matches!(error, ValidationError::Rejected(_)));
    assert!(error.to_string().contains("produced root"));
}
