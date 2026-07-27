use super::*;

fn limits(max_blocks: usize, max_bytes: usize, max_witness_items: usize) -> WindowLimits {
    WindowLimits {
        blocks: max_blocks,
        payload_bytes: max_bytes,
        witness_items: max_witness_items,
    }
}

fn generous_limits() -> WindowLimits {
    limits(16, 1024 * 1024, 1024)
}

fn expected_rollup_id() -> NonZeroU64 {
    NonZeroU64::new(1).unwrap()
}

fn start_checked(
    limits: WindowLimits,
    first: ProveChunk,
) -> Result<WindowAssembler<true>, WindowError> {
    Ok(WindowAssembler::start(limits, first)?
        .verify_rollup_identity(expected_rollup_id())
        .expect("test headers use the expected rollup id"))
}

fn header(from: u64, to: u64) -> ProveChunk {
    ProveChunk {
        kind: Some(prove_chunk::Kind::Header(ProveHeader {
            rollup_id: 1,
            from_block: from,
            to_block: to,
            post_batch: Some(PostBatch::default()),
        })),
    }
}

fn block(number: u64, parent: u8, hash: u8) -> ProveChunk {
    ProveChunk {
        kind: Some(prove_chunk::Kind::Block(BlockWitness {
            number,
            hash: vec![hash; 32],
            parent_hash: vec![parent; 32],
            rlp: vec![number as u8; 4],
            witness: Some(WireExecutionWitness {
                state: vec![vec![number as u8]],
                ..WireExecutionWitness::default()
            }),
        })),
    }
}

fn assemble(chunks: impl IntoIterator<Item = ProveChunk>) -> Result<AdmittedWindow, WindowError> {
    let mut chunks = chunks.into_iter();
    let first = chunks.next().ok_or(WindowError::EmptyStream)?;
    let mut assembler = start_checked(generous_limits(), first)?;
    for chunk in chunks {
        assembler.push(chunk)?;
    }
    assembler.finish()
}

#[test]
fn accepts_a_complete_hash_linked_window_and_preserves_validation_inputs() {
    let input = assemble([
        header(5, 7),
        block(5, 0x99, 0x05),
        block(6, 0x05, 0x06),
        block(7, 0x06, 0x07),
    ])
    .unwrap();
    let (_, blocks) = input.into_parts();
    let blocks = blocks.as_slice();
    assert_eq!(blocks.len(), 3);
    assert_eq!(blocks[0].claimed_parent_hash, B256::repeat_byte(0x99));
    assert_eq!(blocks[2].claimed_hash, B256::repeat_byte(0x07));
    assert_eq!(blocks[2].rlp, vec![7; 4]);
    assert_eq!(blocks[2].witness.state[0].as_ref(), &[7]);
}

#[test]
fn execution_witness_conversion_preserves_every_field() {
    let witness = into_execution_witness(WireExecutionWitness {
        state: vec![vec![0x00, 0xff], vec![0x01]],
        codes: vec![vec![0x02]],
        keys: vec![vec![], vec![0x03]],
        headers: vec![vec![0x04, 0x05]],
    });
    let bytes = |items: &[alloy_primitives::Bytes]| {
        items.iter().map(|item| item.to_vec()).collect::<Vec<_>>()
    };
    assert_eq!(bytes(&witness.state), [vec![0x00, 0xff], vec![0x01]]);
    assert_eq!(bytes(&witness.codes), [vec![0x02]]);
    assert_eq!(bytes(&witness.keys), [vec![], vec![0x03]]);
    assert_eq!(bytes(&witness.headers), [vec![0x04, 0x05]]);
}

#[test]
fn wire_witness_item_count_includes_every_collection() {
    let witness = WireExecutionWitness {
        state: vec![Vec::new(); 2],
        codes: vec![Vec::new()],
        keys: vec![Vec::new(); 2],
        headers: vec![Vec::new()],
    };

    assert_eq!(wire_witness_item_count(&witness), 6);
}

#[test]
fn accepts_a_single_block_window() {
    let input = assemble([header(5, 5), block(5, 0x04, 0x05)]).unwrap();
    let (_, blocks) = input.into_parts();
    assert_eq!(blocks.as_slice().len(), 1);
}

#[test]
fn start_separates_the_rollup_claim_from_the_admitted_header() {
    let assembler = WindowAssembler::start(generous_limits(), header(5, 6)).unwrap();
    let header = assembler.header();

    assert_eq!(assembler.claimed_rollup_id(), 1);
    assert_eq!(
        (header.declared_from_block, header.declared_to_block),
        (5, 6)
    );
}

#[test]
fn block_admission_requires_the_expected_rollup_identity() {
    let mut mismatched_header = header(5, 5);
    let Some(prove_chunk::Kind::Header(wire_header)) = &mut mismatched_header.kind else {
        unreachable!();
    };
    wire_header.rollup_id = 2;

    let unchecked = WindowAssembler::start(generous_limits(), mismatched_header).unwrap();
    assert_eq!(
        unchecked.verify_rollup_identity(expected_rollup_id()).err(),
        Some(RollupIdentityMismatch {
            claimed: 2,
            expected: 1,
        })
    );
}

#[test]
fn discarded_composer_hash_still_counts_toward_the_payload_quota() {
    let mut header_chunk = header(5, 5);
    let Some(prove_chunk::Kind::Header(wire_header)) = &mut header_chunk.kind else {
        unreachable!();
    };
    let post_batch = wire_header.post_batch.as_mut().unwrap();
    post_batch.abi_calldata = vec![0x42; 16];
    post_batch.public_inputs_hash = vec![0x99; 256];
    let encoded_len = prost::Message::encoded_len(&header_chunk);

    assert!(matches!(
        WindowAssembler::start(limits(1, encoded_len - 1, 1), header_chunk.clone()),
        Err(WindowError::PayloadLimit { .. })
    ));

    let accepted = WindowAssembler::start(limits(1, encoded_len, 1), header_chunk).unwrap();
    assert_eq!(
        accepted.header().submitted_post_batch_calldata,
        vec![0x42; 16]
    );
}

#[test]
fn rejects_invalid_or_over_quota_bounds_when_the_header_arrives() {
    for invalid in [header(0, 5), header(6, 5)] {
        assert!(matches!(
            WindowAssembler::start(generous_limits(), invalid),
            Err(WindowError::InvalidBounds { .. })
        ));
    }

    let err = WindowAssembler::start(limits(2, 1024, 10), header(5, 7))
        .err()
        .unwrap();
    assert!(matches!(err, WindowError::BlockLimit { span: 3, .. }));
    assert!(err.is_resource_exhausted());

    WindowAssembler::start(limits(2, 1024, 10), header(5, 6)).unwrap();
}

#[test]
fn a_maximum_u64_span_is_rejected_without_overflow() {
    assert!(matches!(
        WindowAssembler::start(generous_limits(), header(1, u64::MAX)),
        Err(WindowError::BlockLimit { span: u64::MAX, .. })
    ));
}

#[test]
fn a_large_allowed_span_does_not_eagerly_allocate_block_storage() {
    let assembler =
        WindowAssembler::start(limits(1_000_000_000, 1024, 10), header(1, 1_000_000_000)).unwrap();
    assert_eq!(assembler.blocks.capacity(), 0);
}

#[test]
fn rejects_early_eof_and_an_extra_block() {
    let mut incomplete = start_checked(generous_limits(), header(5, 6)).unwrap();
    incomplete.push(block(5, 0x04, 0x05)).unwrap();
    assert!(matches!(
        incomplete.finish(),
        Err(WindowError::Incomplete {
            expected: 2,
            actual: 1,
            ..
        })
    ));

    let mut extra = start_checked(generous_limits(), header(5, 5)).unwrap();
    extra.push(block(5, 0x04, 0x05)).unwrap();
    assert!(matches!(
        extra.push(block(6, 0x05, 0x06)),
        Err(WindowError::ExtraBlock { expected: 1 })
    ));
}

#[test]
fn rejects_a_gap_a_duplicate_and_a_reordering_immediately() {
    for chunks in [
        vec![header(5, 6), block(5, 0x04, 0x05), block(7, 0x06, 0x07)],
        vec![header(5, 6), block(5, 0x04, 0x05), block(5, 0x04, 0x05)],
        vec![header(5, 6), block(6, 0x05, 0x06)],
    ] {
        assert!(matches!(
            assemble(chunks),
            Err(WindowError::WrongBlockNumber { .. })
        ));
    }
}

#[test]
fn rejects_malformed_hash_lengths() {
    let mut short_hash = block(5, 0x04, 0x05);
    let Some(prove_chunk::Kind::Block(wire_block)) = &mut short_hash.kind else {
        unreachable!();
    };
    wire_block.hash.truncate(31);

    let mut short_parent = block(5, 0x04, 0x05);
    let Some(prove_chunk::Kind::Block(wire_block)) = &mut short_parent.kind else {
        unreachable!();
    };
    wire_block.parent_hash.truncate(31);

    for malformed in [short_hash, short_parent] {
        let mut assembler = start_checked(generous_limits(), header(5, 5)).unwrap();
        assert!(assembler.push(malformed).is_err());
    }
}

#[test]
fn rejects_a_hash_chain_break() {
    let mut assembler = start_checked(generous_limits(), header(5, 6)).unwrap();
    assembler.push(block(5, 0x04, 0x05)).unwrap();
    let error = assembler.push(block(6, 0xbb, 0x06)).unwrap_err();
    assert!(matches!(
        error,
        WindowError::ChainBreak {
            number: 6,
            previous: 5,
            expected,
            actual,
        } if expected == B256::repeat_byte(0x05)
            && actual == B256::repeat_byte(0xbb)
    ));
}

#[test]
fn rejects_a_missing_witness() {
    let mut missing = block(5, 0x04, 0x05);
    let Some(prove_chunk::Kind::Block(block)) = &mut missing.kind else {
        unreachable!();
    };
    block.witness = None;

    let mut assembler = start_checked(generous_limits(), header(5, 5)).unwrap();
    assert!(matches!(
        assembler.push(missing),
        Err(WindowError::MissingWitness { number: 5 })
    ));
}

#[test]
fn enforces_the_aggregate_payload_limit() {
    let header = header(5, 5);
    let block = block(5, 0x04, 0x05);
    let exact = header.encoded_len() + block.encoded_len();

    let mut accepted = start_checked(limits(1, exact, 10), header.clone()).unwrap();
    accepted.push(block.clone()).unwrap();
    accepted.finish().unwrap();

    let mut rejected = start_checked(limits(1, exact - 1, 10), header).unwrap();
    let err = rejected.push(block).unwrap_err();
    assert!(matches!(err, WindowError::PayloadLimit { .. }));
    assert!(err.is_resource_exhausted());
}

#[test]
fn enforces_the_aggregate_witness_item_limit() {
    let mut many_items = block(5, 0x04, 0x05);
    let Some(prove_chunk::Kind::Block(block)) = &mut many_items.kind else {
        unreachable!();
    };
    block.witness.as_mut().unwrap().codes = vec![Vec::new(); 3];

    let mut assembler = start_checked(limits(1, 1024, 3), header(5, 5)).unwrap();
    let err = assembler.push(many_items).unwrap_err();
    assert!(matches!(
        err,
        WindowError::WitnessItemLimit { attempted: 4, .. }
    ));
    assert!(err.is_resource_exhausted());
}

#[test]
fn enforces_header_first_and_chunk_kinds() {
    assert!(matches!(assemble([]), Err(WindowError::EmptyStream)));

    assert!(matches!(
        WindowAssembler::start(generous_limits(), block(5, 0x04, 0x05)),
        Err(WindowError::BlockBeforeHeader { number: 5 })
    ));

    assert!(matches!(
        WindowAssembler::start(generous_limits(), ProveChunk { kind: None }),
        Err(WindowError::MissingKind { chunk_index: 0 })
    ));

    let mut duplicate = start_checked(generous_limits(), header(5, 5)).unwrap();
    assert!(matches!(
        duplicate.push(header(5, 5)),
        Err(WindowError::DuplicateHeader)
    ));
}

#[test]
fn rejects_a_header_without_post_batch() {
    let mut header_chunk = header(5, 5);
    let Some(prove_chunk::Kind::Header(wire_header)) = &mut header_chunk.kind else {
        unreachable!();
    };
    wire_header.post_batch = None;

    assert!(matches!(
        WindowAssembler::start(generous_limits(), header_chunk),
        Err(WindowError::MissingPostBatch)
    ));
}

#[test]
fn requires_an_empty_l1_block_hash() {
    for actual in [1, 32] {
        let mut header_chunk = header(5, 5);
        let Some(prove_chunk::Kind::Header(wire_header)) = &mut header_chunk.kind else {
            unreachable!();
        };
        wire_header.post_batch.as_mut().unwrap().l1_block_hash = vec![0; actual];

        let err = WindowAssembler::start(generous_limits(), header_chunk)
            .err()
            .unwrap();
        assert!(matches!(
            err,
            WindowError::NonemptyL1BlockHash { actual: error_len } if error_len == actual
        ));
        assert!(!err.is_resource_exhausted());
    }

    WindowAssembler::start(generous_limits(), header(5, 5)).unwrap();
}

#[test]
fn nonempty_l1_block_hash_precedes_the_block_quota() {
    let mut over_quota = header(5, 7);
    let Some(prove_chunk::Kind::Header(wire_header)) = &mut over_quota.kind else {
        unreachable!();
    };
    wire_header.post_batch.as_mut().unwrap().l1_block_hash = vec![0; 32];

    let err = WindowAssembler::start(limits(2, 1024, 10), over_quota)
        .err()
        .unwrap();
    assert!(matches!(
        err,
        WindowError::NonemptyL1BlockHash { actual: 32 }
    ));
    assert!(!err.is_resource_exhausted());
}
